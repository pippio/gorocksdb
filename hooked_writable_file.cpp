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
  Status status = WritableFileWrapper::Append(data);
  if (status.ok()) {
    gorocksdb_wf_append(state_, (char*)data.data(), data.size());
  }
  return status;
}

Status HookedWritableFile::Close() {
  Status status = WritableFileWrapper::Close();
  if (status.ok()) {
    gorocksdb_wf_close(state_);
  }
  return status;
}

Status HookedWritableFile::Sync() {
  Status status = WritableFileWrapper::Sync();
  if (status.ok()) {
    gorocksdb_wf_sync(state_);
  }
  return status;
}

Status HookedWritableFile::Fsync() {
  Status status = WritableFileWrapper::Fsync();
  if (status.ok()) {
    gorocksdb_wf_fsync(state_);
  }
  return status;
}

Status HookedWritableFile::RangeSync(off_t offset, off_t nbytes) {
  Status status = WritableFileWrapper::RangeSync(offset, nbytes);
  if (status.ok()) {
    gorocksdb_wf_range_sync(state_, offset, nbytes);
  }
  return status;
}
