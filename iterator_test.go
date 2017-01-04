package gorocksdb

import (
	"strconv"
	"testing"

	"github.com/facebookgo/ensure"
)

func TestIterator(t *testing.T) {
	db := newTestDB(t, "TestIterator", nil)
	defer db.Close()

	// insert keys
	givenKeys := [][]byte{[]byte("key1"), []byte("key2"), []byte("key3")}
	wo := NewDefaultWriteOptions()
	for _, k := range givenKeys {
		ensure.Nil(t, db.Put(wo, k, []byte("val")))
	}

	ro := NewDefaultReadOptions()
	iter := db.NewIterator(ro)
	defer iter.Close()
	var actualKeys [][]byte
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		key := make([]byte, 4)
		copy(key, iter.Key().Data())
		actualKeys = append(actualKeys, key)
	}
	ensure.Nil(t, iter.Err())
	ensure.DeepEqual(t, actualKeys, givenKeys)
}

func TestIteratorSeeking(t *testing.T) {
	db := newTestDB(t, "TestIteratorSeeking", nil)
	defer db.Close()

	wo := NewDefaultWriteOptions()
	for i := int64(100); i != 200; i++ {
		var key, val = []byte(strconv.FormatInt(i, 10)), []byte(strconv.FormatInt(i, 16))
		ensure.Nil(t, db.Put(wo, key, val))
	}

	ro := NewDefaultReadOptions()
	iter := db.NewIterator(ro)
	defer iter.Close()

	expect := func(i int64) {
		ensure.True(t, iter.Valid())
		ensure.DeepEqual(t, string(iter.Key().Data()), strconv.FormatInt(i, 10))
		ensure.DeepEqual(t, string(iter.Value().Data()), strconv.FormatInt(i, 16))
	}

	// Next followed by SeekToFirst works as expected.
	for i := 0; i != 2; i++ {
		iter.SeekToFirst()
		expect(100)
		iter.Next()
		expect(101)
		iter.Next()
		expect(102)
	}

	// As does Prev.
	iter.Prev()
	expect(101)
	iter.Next()
	expect(102)

	// Seek followed by Next.
	iter.Seek([]byte("132"))
	expect(132)
	iter.Next()
	expect(133)
	iter.Seek([]byte("122"))
	expect(122)
	iter.Next()
	expect(123)

	// SeekToLast followed by Prev.
	iter.SeekToLast()
	expect(199)
	iter.Prev()
	expect(198)
	iter.Next()
	expect(199)

	// Step beyond last item.
	iter.Next()
	ensure.False(t, iter.Valid())

	iter.SeekToFirst()
	expect(100)
	iter.Next()
	expect(101)
	iter.Prev()
	expect(100)

	// Step before first element.
	iter.Prev()
	ensure.False(t, iter.Valid())

	// Expect the arena grew once (at most two Nexts in a sequence).
	ensure.DeepEqual(t, len(iter.arena), minArenaSize*2)

	// Establish fixture where the arena is too small to hold a key/value
	iter.SeekToFirst()
	iter.arena = make([]byte, 8)
	iter.remainder = nil

	// Next & accessors still work as expected, though arena isn't used.
	iter.Next()
	ensure.DeepEqual(t, len(iter.remainder), 0)
	expect(101)

	// Subsequent iterations do grow the arena each time its drained.
	iter.Next() // Drains.
	ensure.DeepEqual(t, len(iter.arena), 16)
	ensure.DeepEqual(t, len(iter.remainder), 13)
	expect(102)

	iter.Next() // Drains.
	ensure.DeepEqual(t, len(iter.arena), 32)
	ensure.DeepEqual(t, len(iter.remainder), 26)
	expect(103)
	iter.Next()
	ensure.DeepEqual(t, len(iter.arena), 32)
	ensure.DeepEqual(t, len(iter.remainder), 13)
	expect(104)

	iter.Next() // Drains.
	ensure.DeepEqual(t, len(iter.arena), 64)
	ensure.DeepEqual(t, len(iter.remainder), 52)
	expect(105)
}
