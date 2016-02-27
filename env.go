package gorocksdb

/*
#cgo CXXFLAGS: -std=c++11
#include "rocksdb/c.h"
#include "gorocksdb.h"
*/
import "C"

import (
	"reflect"
	"unsafe"
)

// Env is a system call environment used by a database.
type Env struct {
	c *C.rocksdb_env_t
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
	return &Env{c: C.gorocksdb_create_hooked_env(retainObserver(obv))}
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

//export gorocksdb_env_new_writable_file
func gorocksdb_env_new_writable_file(idx C.int, fnameRaw *C.char, fnameLen C.int) C.int {
	wf := getEnvObserver(idx).NewWritableFile(C.GoStringN(fnameRaw, fnameLen))
	return retainObserver(wf)
}

//export gorocksdb_env_delete_file
func gorocksdb_env_delete_file(idx C.int, fnameRaw *C.char, fnameLen C.int) {
	getEnvObserver(idx).DeleteFile(C.GoStringN(fnameRaw, fnameLen))
}

//export gorocksdb_env_delete_dir
func gorocksdb_env_delete_dir(idx C.int, fnameRaw *C.char, fnameLen C.int) {
	getEnvObserver(idx).DeleteDir(C.GoStringN(fnameRaw, fnameLen))
}

//export gorocksdb_env_rename_file
func gorocksdb_env_rename_file(idx C.int, srcRaw *C.char, srcLen C.int, targetRaw *C.char, targetLen C.int) {
	getEnvObserver(idx).RenameFile(C.GoStringN(srcRaw, srcLen), C.GoStringN(targetRaw, targetLen))
}

//export gorocksdb_env_link_file
func gorocksdb_env_link_file(idx C.int, srcRaw *C.char, srcLen C.int, targetRaw *C.char, targetLen C.int) {
	getEnvObserver(idx).LinkFile(C.GoStringN(srcRaw, srcLen), C.GoStringN(targetRaw, targetLen))
}

//export gorocksdb_env_dtor
func gorocksdb_env_dtor(idx C.int) {
	releaseObserver(idx)
}

//export gorocksdb_wf_dtor
func gorocksdb_wf_dtor(idx C.int) {
	releaseObserver(idx)
}

//export gorocksdb_wf_append
func gorocksdb_wf_append(idx C.int, raw *C.char, rawLen C.size_t) {
	var data []byte

	// Initialize |data| to the underlying |raw| array, without a copy.
	header := (*reflect.SliceHeader)(unsafe.Pointer(&data))
	header.Data = uintptr(unsafe.Pointer(raw))
	header.Len = int(rawLen)
	header.Cap = int(rawLen)

	getWFObserver(idx).Append(data)
}

//export gorocksdb_wf_close
func gorocksdb_wf_close(idx C.int) {
	getWFObserver(idx).Close()
}

//export gorocksdb_wf_sync
func gorocksdb_wf_sync(idx C.int) {
	getWFObserver(idx).Sync()
}

//export gorocksdb_wf_fsync
func gorocksdb_wf_fsync(idx C.int) {
	getWFObserver(idx).Fsync()
}

//export gorocksdb_wf_range_sync
func gorocksdb_wf_range_sync(idx C.int, offset, nbytes C.off_t) {
	getWFObserver(idx).RangeSync(int64(offset), int64(nbytes))
}
