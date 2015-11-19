#include "hooked_env.hpp"
#include "hooked_writable_file.hpp"

#include <iostream>

extern "C" {
#include "gorocksdb.h"
#include "_cgo_export.h"
}

using rocksdb::EnvOptions;
using rocksdb::EnvWrapper;
using rocksdb::Status;
using rocksdb::WritableFile;
using std::unique_ptr;

HookedEnv::HookedEnv(void* state)
  : rocksdb::EnvWrapper(rocksdb::Env::Default()),
    state_(state) { }

HookedEnv::~HookedEnv() { }

Status HookedEnv::NewWritableFile(const std::string& fname,
                                  unique_ptr<WritableFile>* result,
                                  const EnvOptions& options) {
  // Call down into wrapper subclass to get an actual delegate.
  Status status = EnvWrapper::NewWritableFile(fname, result, options);

  if (status.ok()) {
    // Wrap |result| with a hooked implementation.
    void* file_state = gorocksdb_env_new_writable_file(state_,
        (char*)(fname.c_str()), fname.length());
    result->reset(new HookedWritableFile(file_state, std::move(*result)));
  }
  return status;
}

Status HookedEnv::DeleteFile(const std::string& fname) {
  Status status = EnvWrapper::DeleteFile(fname);
  if (status.ok()) {
    gorocksdb_env_delete_file(state_, (char*)(fname.c_str()), fname.length());
  }
  return status;
}

Status HookedEnv::DeleteDir(const std::string& dirname) {
  Status status = EnvWrapper::DeleteDir(dirname);
  if (status.ok()) {
    gorocksdb_env_delete_dir(state_, (char*)(dirname.c_str()), dirname.length());
  }
  return status;
}

Status HookedEnv::RenameFile(const std::string& src,
                             const std::string& target) {
  Status result = EnvWrapper::RenameFile(src, target);
  if (result.ok()) {
    gorocksdb_env_rename_file(state_,
        (char*)(src.c_str()),
        src.length(),
        (char*)(target.c_str()),
        target.length());
  }
  return result;
}

Status HookedEnv::LinkFile(const std::string& src,
                           const std::string& target) {
  Status result = EnvWrapper::LinkFile(src, target);
  if (result.ok()) {
    gorocksdb_env_link_file(state_,
        (char*)(src.c_str()),
        src.length(),
        (char*)(target.c_str()),
        target.length());
  }
  return result;
}


extern "C" {

// Note: this definition is copied from github.com/facebook/rocksdb/db/c.cc:339
// We must copy, as this struct is defined in a .cc we don't have access too.
struct rocksdb_env_t {
  rocksdb::Env* rep;
  bool is_default;
};

rocksdb_env_t* gorocksdb_create_hooked_env(void* state) {
  rocksdb_env_t* result = new rocksdb_env_t;
  result->rep = new HookedEnv(state);
  result->is_default = false;
  return result;
}

} // extern "C"
