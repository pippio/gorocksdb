#include "hooked_writable_file.hpp"

#include <iostream>

extern "C" {
#include "_cgo_export.h"
}

using rocksdb::Slice;
using rocksdb::Status;
using rocksdb::WritableFileWrapper;
using std::unique_ptr;

HookedWritableFile::HookedWritableFile(void* state,
      unique_ptr<WritableFile> delegate)
  : WritableFileWrapper(delegate.get()),
    state_(state),
    delegate_(std::move(delegate)) {
}

HookedWritableFile::~HookedWritableFile() {
  gorocksdb_wf_dtor(state_);
}

Status HookedWritableFile::Append(const Slice& data) {
  gorocksdb_wf_append(state_, (char*)data.data(), data.size());
  return WritableFileWrapper::Append(data);
}

Status HookedWritableFile::Close() {
  gorocksdb_wf_close(state_);
  return WritableFileWrapper::Close();
}

Status HookedWritableFile::Sync() {
  gorocksdb_wf_sync(state_);
  return WritableFileWrapper::Sync();
}

Status HookedWritableFile::Fsync() {
  gorocksdb_wf_fsync(state_);
  return WritableFileWrapper::Fsync();
}

Status HookedWritableFile::RangeSync(off_t offset, off_t nbytes) {
  gorocksdb_wf_range_sync(state_, offset, nbytes);
  return WritableFileWrapper::RangeSync(offset, nbytes);
}
