#ifndef HOOKED_ENV_HPP
#define HOOKED_ENV_HPP

#include <rocksdb/env.h>
#include <memory>

// Implementation of rocksdb::Env which notifies cgo hooks of calls to
// designated methods. Actual handling of calls is delegated to EnvWrapper
// subclass.
class HookedEnv : public rocksdb::EnvWrapper {
 public:

  // |state| is an opaque pointer which is passed back into cgo hooks.
  HookedEnv(void* state);

  virtual ~HookedEnv() override;

  virtual rocksdb::Status NewWritableFile(
      const std::string& fname,
      std::unique_ptr<rocksdb::WritableFile>* result,
      const rocksdb::EnvOptions& options) override;

  virtual rocksdb::Status DeleteFile(const std::string& fname) override;

  virtual rocksdb::Status DeleteDir(const std::string& dirname) override;

  virtual rocksdb::Status RenameFile(const std::string& src,
                                     const std::string& target) override;

  virtual rocksdb::Status LinkFile(const std::string& src,
                                   const std::string& target) override;
 private:

  void* state_;
};

#endif // #ifndef HOOKED_ENV_HPP
