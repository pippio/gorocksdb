#include "hooked_writable_file.hpp"

#include <iostream>

extern "C" {
#include "_cgo_export.h"
}

using rocksdb::Slice;
using rocksdb::Status;
using rocksdb::WritableFileWrapper;
using std::unique_ptr;

HookedWritableFile::HookedWritableFile(int handle,
      unique_ptr<WritableFile> delegate)
  : WritableFileWrapper(delegate.get()),
    handle_(handle),
    delegate_(std::move(delegate)) {
}

HookedWritableFile::~HookedWritableFile() {
  gorocksdb_wf_dtor(handle_);
}

Status HookedWritableFile::Append(const Slice& data) {
  Status status = WritableFileWrapper::Append(data);
  if (status.ok()) {
    gorocksdb_wf_append(handle_, (char*)data.data(), data.size());
  }
  return status;
}

Status HookedWritableFile::Close() {
  Status status = WritableFileWrapper::Close();
  if (status.ok()) {
    gorocksdb_wf_close(handle_);
  }
  return status;
}

Status HookedWritableFile::Sync() {
  Status status = WritableFileWrapper::Sync();
  if (status.ok()) {
    gorocksdb_wf_sync(handle_);
  }
  return status;
}

Status HookedWritableFile::Fsync() {
  Status status = WritableFileWrapper::Fsync();
  if (status.ok()) {
    gorocksdb_wf_fsync(handle_);
  }
  return status;
}
