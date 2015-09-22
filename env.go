package gorocksdb

/*
#include "rocksdb/c.h"
#include "gorocksdb.h"
*/
import "C"

import (
	"reflect"
	"unsafe"
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

// Env is a system call environment used by a database.
type Env struct {
	c *C.rocksdb_env_t

	// |handle| is held by C++ classes. Reference here to avoid GC.
	handle *envObserverHandle
}

// NewDefaultEnv creates a default environment.
func NewDefaultEnv() *Env {
	return NewNativeEnv(C.rocksdb_create_default_env())
}

// NewNativeEnv creates a Environment object.
func NewNativeEnv(c *C.rocksdb_env_t) *Env {
	return &Env{c: c}
}

func NewObservedEnv(obv EnvObserver) *Env {
	handle := &envObserverHandle{
		EnvObserver: obv,
		fileHandles: make(map[*wfObserverHandle]struct{}),
	}
	return &Env{
		c:      C.gorocksdb_create_hooked_env(unsafe.Pointer(handle)),
		handle: handle,
	}
}

// SetBackgroundThreads sets the number of background worker threads
// of a specific thread pool for this environment.
// 'LOW' is the default pool.
// Default: 1
func (env *Env) SetBackgroundThreads(n int) {
	C.rocksdb_env_set_background_threads(env.c, C.int(n))
}

// SetHighPriorityBackgroundThreads sets the size of the high priority
// thread pool that can be used to prevent compactions from stalling
// memtable flushes.
func (env *Env) SetHighPriorityBackgroundThreads(n int) {
	C.rocksdb_env_set_high_priority_background_threads(env.c, C.int(n))
}

// Destroy deallocates the Env object.
func (env *Env) Destroy() {
	C.rocksdb_env_destroy(env.c)
	env.c = nil
}

// Concrete type wrapping |EnvObserver|, for passing through C++.
type envObserverHandle struct {
	EnvObserver

	// References to created |WritableFileObserver|'s which must be held for GC.
	fileHandles map[*wfObserverHandle]struct{}
}

// Concrete type wrapping |WritableFileObserver|, for passing through C++.
type wfObserverHandle struct {
	WritableFileObserver

	env *envObserverHandle
}

//export gorocksdb_env_new_writable_file
func gorocksdb_env_new_writable_file(state unsafe.Pointer, fnameRaw *C.char,
	fnameLen C.int) unsafe.Pointer {

	env := (*envObserverHandle)(state)
	wf := &wfObserverHandle{env.NewWritableFile(C.GoStringN(fnameRaw, fnameLen)), env}

	// Reference |wf| until it's deleted from the C++ side.
	env.fileHandles[wf] = struct{}{}

	return unsafe.Pointer(wf)
}

//export gorocksdb_env_delete_file
func gorocksdb_env_delete_file(state unsafe.Pointer, fnameRaw *C.char, fnameLen C.int) {
	env := (*envObserverHandle)(state)
	env.DeleteFile(C.GoStringN(fnameRaw, fnameLen))
}

//export gorocksdb_env_delete_dir
func gorocksdb_env_delete_dir(state unsafe.Pointer, fnameRaw *C.char, fnameLen C.int) {
	env := (*envObserverHandle)(state)
	env.DeleteDir(C.GoStringN(fnameRaw, fnameLen))
}

//export gorocksdb_env_rename_file
func gorocksdb_env_rename_file(state unsafe.Pointer, srcRaw *C.char, srcLen C.int,
	targetRaw *C.char, targetLen C.int) {

	env := (*envObserverHandle)(state)
	env.RenameFile(C.GoStringN(srcRaw, srcLen), C.GoStringN(targetRaw, targetLen))
}

//export gorocksdb_env_link_file
func gorocksdb_env_link_file(state unsafe.Pointer, srcRaw *C.char, srcLen C.int,
	targetRaw *C.char, targetLen C.int) {

	env := (*envObserverHandle)(state)
	env.LinkFile(C.GoStringN(srcRaw, srcLen), C.GoStringN(targetRaw, targetLen))
}

//export gorocksdb_wf_dtor
func gorocksdb_wf_dtor(state unsafe.Pointer) {
	wf := (*wfObserverHandle)(state)
	// Free from |env.fileHandles| so that GC can reclaim the handle & observer.
	delete(wf.env.fileHandles, wf)
}

//export gorocksdb_wf_append
func gorocksdb_wf_append(state unsafe.Pointer, raw *C.char, rawLen C.size_t) {
	wf := (*wfObserverHandle)(state)

	var data []byte

	// Initialize |data| to the underlying |raw| array, without a copy.
	header := (*reflect.SliceHeader)(unsafe.Pointer(&data))
	header.Data = uintptr(unsafe.Pointer(raw))
	header.Len = int(rawLen)
	header.Cap = int(rawLen)

	wf.Append(data)
}

//export gorocksdb_wf_close
func gorocksdb_wf_close(state unsafe.Pointer) {
	(*wfObserverHandle)(state).Close()
}

//export gorocksdb_wf_sync
func gorocksdb_wf_sync(state unsafe.Pointer) {
	(*wfObserverHandle)(state).Sync()
}

//export gorocksdb_wf_fsync
func gorocksdb_wf_fsync(state unsafe.Pointer) {
	(*wfObserverHandle)(state).Fsync()
}

//export gorocksdb_wf_range_sync
func gorocksdb_wf_range_sync(state unsafe.Pointer, offset, nbytes C.off_t) {
	(*wfObserverHandle)(state).RangeSync(int64(offset), int64(nbytes))
}
