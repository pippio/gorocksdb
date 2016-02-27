#ifndef HOOKED_WRITABLE_FILE_HPP
#define HOOKED_WRITABLE_FILE_HPP

#include <rocksdb/env.h>
#include <memory>

// Implementation of rocksdb::WritableFile which notifies cgo hooks of calls to
// designated methods. Actual handling of calls is delegated to
// WritableFileWrapper subclass.
class HookedWritableFile : public rocksdb::WritableFileWrapper {
 public:

  // |handle| is an opaque integer which is passed back into cgo hooks.
  HookedWritableFile(int handle, std::unique_ptr<WritableFile> delegate);

  virtual ~HookedWritableFile() override;

  virtual rocksdb::Status Append(const rocksdb::Slice& data) override;

  virtual rocksdb::Status Close() override;

  virtual rocksdb::Status Sync() override;

  virtual rocksdb::Status Fsync() override;

 protected:

  virtual rocksdb::Status RangeSync(off_t offset, off_t nbytes) override;

 private:

  int handle_;

  // Delegate is exclusively owned by HookedWritableFile.
  std::unique_ptr<rocksdb::WritableFile> delegate_;
};

#endif
