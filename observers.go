package gorocksdb

import (
	"C"
	"sync"
)

// EnvObserver allows for observation of mutating Env operations.
// Consult |Env| in rocksdb/env.h for further details.
type EnvObserver interface {
	// Invoked just before a new WritableFile is created. Returns a
	// WritableFileObserver which is associated with the result file.
	NewWritableFile(fname string) WritableFileObserver
	// Invoked just before |fname| is deleted.
	DeleteFile(fname string)
	// Invoked just before |dirname| is deleted.
	DeleteDir(dirname string)
	// Invoked just before |src| is renamed to |target|.
	RenameFile(src, target string)
	// Invoked just before |src| is linked to |target|.
	LinkFile(src, target string)
}

// WritableFileObserver allows for observation of mutating WritableFile
// operations. Consult |WritableFile| in rocksdb/env.h for further details.
type WritableFileObserver interface {
	// Inoked when |data| is appended to the file. Note that |data| is owned by
	// RocksDB and must not be referenced after this call.
	Append(data []byte)
	// Invoked just before the file is closed.
	Close()
	// Invoked just before the file is Synced.
	Sync()
	// Invoked just before the file is Fsync'd. Note that this may in turn
	// delegate to sync, and result in a call to the Sync() observer.
	Fsync()
	// Invoked just before a range of the file is synced.
	RangeSync(offset, nbytes int64)
}

type observer interface{}

// Global arrays of live observers. Indexes are passed to the C++ side, while
// the instances themselves are retained here to guard against GC until their
// matching C++ destructor is invoked.
var liveObservers = make([]observer, 16)
var liveObserversMu sync.Mutex

func retainObserver(obv observer) (idx C.int) {
	liveObserversMu.Lock()

	for idx = 0; int(idx) != len(liveObservers); idx++ {
		if liveObservers[idx] == nil {
			break
		}
	}

	// Quadratic resize.
	if int(idx) == len(liveObservers) {
		old := liveObservers
		liveObservers = make([]observer, len(old)*2)
		copy(liveObservers, old)
	}

	liveObservers[idx] = obv
	liveObserversMu.Unlock()
	return
}

func releaseObserver(idx C.int) {
	liveObserversMu.Lock()
	liveObservers[idx] = nil
	liveObserversMu.Unlock()
}

func getEnvObserver(idx C.int) (r EnvObserver) {
	liveObserversMu.Lock()
	r = liveObservers[idx].(EnvObserver)
	liveObserversMu.Unlock()
	return
}

func getWFObserver(idx C.int) (wf WritableFileObserver) {
	liveObserversMu.Lock()
	wf = liveObservers[idx].(WritableFileObserver)
	liveObserversMu.Unlock()
	return
}
