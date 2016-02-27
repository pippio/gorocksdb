package gorocksdb

import (
	"testing"

	"github.com/facebookgo/ensure"
)

func TestObserverRetainAndRelease(t *testing.T) {
	fixtures := map[int]*struct{}{0: {}, 1: {}, 2: {}, 3: {}}

	// Use a small initial size to excercise re-allocation.
	liveObservers = make([]observer, 1)

	for i := 0; i != len(fixtures); i++ {
		ensure.True(t, int(retainObserver(fixtures[i])) == i, i)
	}

	// Expect we resized storage to 4 observers.
	ensure.True(t, len(liveObservers) == 4, liveObservers)

	for i := 0; i != len(fixtures); i++ {
		ensure.True(t, liveObservers[i] == fixtures[i], i)
	}

	// Release observers. Expect their storage is reused.
	releaseObserver(2)
	releaseObserver(0)

	ensure.True(t, retainObserver(&struct{}{}) == 0)
	ensure.True(t, retainObserver(&struct{}{}) == 2)

	ensure.True(t, len(liveObservers) == 4, liveObservers)
}
