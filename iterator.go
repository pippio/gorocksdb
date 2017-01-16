package gorocksdb

/*
#include <stdlib.h>
#include "rocksdb/c.h"

size_t batched_iter_next(rocksdb_iterator_t* cit, size_t arena_len, char* arena);

*/
import "C"
import (
	"bytes"
	"errors"
	"unsafe"
)

const (
	// Min and max arena sizes used for pre-fetching iterator keys & values.
	minArenaSize = 4096
	maxArenaSize = minArenaSize * 1024
)

// Iterator provides a way to seek to specific keys and iterate through
// the keyspace from that point, as well as access the values of those keys.
//
// For example:
//
//      it := db.NewIterator(readOpts)
//      defer it.Close()
//
//      it.Seek([]byte("foo"))
//		for ; it.Valid(); it.Next() {
//          fmt.Printf("Key: %v Value: %v\n", it.Key().Data(), it.Value().Data())
// 		}
//
//      if err := it.Err(); err != nil {
//          return err
//      }
//
type Iterator struct {
	c *C.rocksdb_iterator_t

	// CGO calls are expensive. We amortize this cost by batching iteration and
	// key/value retrieval into a memory arena, populated from C++ and read from
	// Go. For the common case, this greatly reduces the number of required CGO
	// calls in exchange for an extra copy.
	arena, remainder []byte
}

// NewNativeIterator creates a Iterator object.
func NewNativeIterator(c unsafe.Pointer) *Iterator {
	return &Iterator{
		c:     (*C.rocksdb_iterator_t)(c),
		arena: make([]byte, minArenaSize),
	}
}

// Valid returns false only when an Iterator has iterated past either the
// first or the last key in the database.
func (iter *Iterator) Valid() bool {
	if len(iter.remainder) != 0 {
		return true
	}
	return C.rocksdb_iter_valid(iter.c) != 0
}

// ValidForPrefix returns false only when an Iterator has iterated past the
// first or the last key in the database or the specified prefix.
func (iter *Iterator) ValidForPrefix(prefix []byte) bool {
	return iter.Valid() && bytes.HasPrefix(iter.Key().Data(), prefix)
}

// Key returns the key the iterator currently holds.
func (iter *Iterator) Key() *Slice {
	if len(iter.remainder) != 0 {
		return &Slice{
			data:  byteToChar(iter.remainder[4:]),
			size:  parseLenPrefix(iter.remainder),
			freed: true,
		}
	}

	var cLen C.size_t
	cKey := C.rocksdb_iter_key(iter.c, &cLen)
	if cKey == nil {
		return nil
	}
	return &Slice{cKey, cLen, true}
}

// Value returns the value in the database the iterator currently holds.
func (iter *Iterator) Value() *Slice {
	if len(iter.remainder) != 0 {
		var keyLen = parseLenPrefix(iter.remainder)

		return &Slice{
			data:  byteToChar(iter.remainder[8+keyLen:]),
			size:  parseLenPrefix(iter.remainder[4+keyLen:]),
			freed: true,
		}
	}

	var cLen C.size_t
	cVal := C.rocksdb_iter_value(iter.c, &cLen)
	if cVal == nil {
		return nil
	}
	return &Slice{cVal, cLen, true}
}

// Next moves the iterator to the next sequential key in the database.
func (iter *Iterator) Next() {
	if len(iter.remainder) != 0 {
		// Iterate over current key & value.
		iter.remainder = iter.remainder[4+parseLenPrefix(iter.remainder):]
		iter.remainder = iter.remainder[4+parseLenPrefix(iter.remainder):]
	}

	if len(iter.remainder) == 0 {
		if iter.remainder != nil && len(iter.arena) < maxArenaSize {
			// If we full consumed the previously read arena, then double its
			// size up to a bound.
			iter.arena = make([]byte, len(iter.arena)*2)
		}

		var offset = C.batched_iter_next(iter.c, C.size_t(len(iter.arena)),
			byteToChar(iter.arena))
		iter.remainder = iter.arena[:offset]
	}
}

// Prev moves the iterator to the previous sequential key in the database.
func (iter *Iterator) Prev() {
	if len(iter.remainder) != 0 {
		// Seek the underlying iterator to the current key before stepping to Prev.
		iter.Seek(iter.Key().Data())
	}
	C.rocksdb_iter_prev(iter.c)
}

// SeekToFirst moves the iterator to the first key in the database.
func (iter *Iterator) SeekToFirst() {
	C.rocksdb_iter_seek_to_first(iter.c)
	iter.remainder = nil
}

// SeekToLast moves the iterator to the last key in the database.
func (iter *Iterator) SeekToLast() {
	C.rocksdb_iter_seek_to_last(iter.c)
	iter.remainder = nil
}

// Seek moves the iterator to the position greater than or equal to the key.
func (iter *Iterator) Seek(key []byte) {
	cKey := byteToChar(key)
	C.rocksdb_iter_seek(iter.c, cKey, C.size_t(len(key)))
	iter.remainder = nil
}

// SeekForPrev moves the iterator to the last key that less than or equal
// to the target key, in contrast with Seek.
func (iter *Iterator) SeekForPrev(key []byte) {
	cKey := byteToChar(key)
	C.rocksdb_iter_seek_for_prev(iter.c, cKey, C.size_t(len(key)))
}

// Err returns nil if no errors happened during iteration, or the actual
// error otherwise.
func (iter *Iterator) Err() error {
	var cErr *C.char
	C.rocksdb_iter_get_error(iter.c, &cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return errors.New(C.GoString(cErr))
	}
	return nil
}

// Close closes the iterator.
func (iter *Iterator) Close() {
	C.rocksdb_iter_destroy(iter.c)
	iter.c = nil
	iter.remainder = nil
}

func parseLenPrefix(b []byte) C.size_t {
	return C.size_t(b[0])<<24 | C.size_t(b[1])<<16 | C.size_t(b[2])<<8 | C.size_t(b[3])
}
