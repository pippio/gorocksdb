#include "hooked_env.hpp"
#include "hooked_writable_file.hpp"
#include "rocksdb/options.h"

#include <iostream>

extern "C" {
#include "gorocksdb.h"
#include "_cgo_export.h"
}

using rocksdb::Options;
using rocksdb::EnvOptions;
using rocksdb::EnvWrapper;
using rocksdb::Status;
using rocksdb::WritableFile;
using std::unique_ptr;

HookedEnv::HookedEnv(int handle)
  : rocksdb::EnvWrapper(rocksdb::Env::Default()),
    handle_(handle) { }

HookedEnv::~HookedEnv() {
  gorocksdb_env_dtor(handle_);
}

Status HookedEnv::NewWritableFile(const std::string& fname,
                                  unique_ptr<WritableFile>* result,
                                  const EnvOptions& options) {
  // Call down into wrapper subclass to get an actual delegate.
  Status status = EnvWrapper::NewWritableFile(fname, result, options);

  if (status.ok()) {
    // Wrap |result| with a hooked implementation.
    int wf_handle = gorocksdb_env_new_writable_file(handle_,
        (char*)(fname.c_str()), fname.length());
    result->reset(new HookedWritableFile(wf_handle, std::move(*result)));
  }
  return status;
}

Status HookedEnv::DeleteFile(const std::string& fname) {
  Status status = EnvWrapper::DeleteFile(fname);
  if (status.ok()) {
    gorocksdb_env_delete_file(handle_, (char*)(fname.c_str()), fname.length());
  }
  return status;
}

Status HookedEnv::DeleteDir(const std::string& dirname) {
  Status status = EnvWrapper::DeleteDir(dirname);
  if (status.ok()) {
    gorocksdb_env_delete_dir(handle_, (char*)(dirname.c_str()), dirname.length());
  }
  return status;
}

Status HookedEnv::RenameFile(const std::string& src,
                             const std::string& target) {
  Status result = EnvWrapper::RenameFile(src, target);
  if (result.ok()) {
    gorocksdb_env_rename_file(handle_,
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
    gorocksdb_env_link_file(handle_,
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

rocksdb_env_t* gorocksdb_create_hooked_env(int handle) {
  rocksdb_env_t* result = new rocksdb_env_t;
  result->rep = new HookedEnv(handle);
  result->is_default = false;
  return result;
}

// Note: this definition is copied from github.com/facebook/rocksdb/db/c.cc:99
struct rocksdb_options_t { rocksdb::Options rep; };

void gorocksdb_options_set_compaction_priority(rocksdb_options_t *opts, int priority) {
  opts->rep.compaction_pri = rocksdb::CompactionPri(priority);
}

} // extern "C"
