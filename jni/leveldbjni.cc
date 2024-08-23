#include <jni.h>
#include "leveldb/db.h"
#include "leveldb/cache.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/write_batch.h"

extern "C" {
  #include "org_tron_leveldb_DB.h"
  #include "org_tron_leveldb_DBIterator.h"
  #include "org_tron_leveldb_WriteBatch.h"
}

using namespace leveldb;

class JavaComparator : public Comparator {
 private:
  JNIEnv* env_;
  jobject jobj_;
  jclass cls_;
  jstring jname_;
  const char* name_;
  jmethodID compare_mid_;

 public:

  JavaComparator(JNIEnv* env, jobject jobj) : env_(env), jobj_(jobj) {
    cls_ = env_->FindClass("org/tron/leveldb/DBComparator");
    jmethodID mid = env_->GetMethodID(cls_, "name", "()Ljava/lang/String;");
    jname_ = static_cast<jstring>(env_->CallObjectMethod(jobj_, mid));
    name_ = env_->GetStringUTFChars(jname_, nullptr);
    compare_mid_ = env_->GetMethodID(cls_, "compare", "([B[B)I");
  }

  ~JavaComparator() override { 
    env_->DeleteGlobalRef(jobj_);
    env_->ReleaseStringUTFChars(jname_, name_);
  }

  // Three-way comparison.  Returns value:
  //   < 0 iff "a" < "b",
  //   == 0 iff "a" == "b",
  //   > 0 iff "a" > "b"
  int Compare(const Slice& a, const Slice& b) const override {
    jbyteArray jba = env_->NewByteArray(a.size());
    jbyteArray jbb = env_->NewByteArray(b.size());
    env_->SetByteArrayRegion(jba, 0, a.size(),
                             reinterpret_cast<const jbyte*>(a.data()));
    env_->SetByteArrayRegion(jbb, 0, b.size(),
                             reinterpret_cast<const jbyte*>(b.data()));

    return env_->CallIntMethod(jobj_, compare_mid_, jba, jbb);
  }

  // The name of the comparator.  Used to check for comparator
  // mismatches (i.e., a DB created with one comparator is
  // accessed using a different comparator.
  //
  // The client of this package should switch to a new name whenever
  // the comparator implementation changes in a way that will cause
  // the relative ordering of any two keys to change.
  //
  // Names starting with "leveldb." are reserved and should not be used
  // by any clients of this package.
  const char* Name() const override {
    return name_;
  }

  // Advanced functions: these are used to reduce the space requirements
  // for internal data structures like index blocks.

  // If *start < limit, changes *start to a short string in [start,limit).
  // Simple comparator implementations may return with *start unchanged,
  // i.e., an implementation of this method that does nothing is correct.
  void FindShortestSeparator(std::string* start,
                             const Slice& limit) const override {
    jmethodID mid = env_->GetMethodID(cls_, "findShortestSeparator",
                                      "([B[B)[B");
    jbyteArray jstart = env_->NewByteArray(start->size());
    env_->SetByteArrayRegion(jstart, 0, start->size(),
                             reinterpret_cast<const jbyte*>(start->data()));
    jbyteArray jlimit = env_->NewByteArray(limit.size());
    env_->SetByteArrayRegion(jlimit, 0, limit.size(),
                             reinterpret_cast<const jbyte*>(limit.data()));
    jbyteArray jresult = static_cast<jbyteArray>(env_->CallObjectMethod(
                                           jobj_, mid, jstart, jlimit));
    auto l = env_->GetArrayLength(jresult);
    const char* ptr = reinterpret_cast<const char*>(env_->GetByteArrayElements(jresult, NULL));
    *start = std::string(ptr, ptr + l);
    env_->ReleaseByteArrayElements(
        jresult, reinterpret_cast<jbyte*>(const_cast<char*>(ptr)), JNI_ABORT);
  }

  // Changes *key to a short string >= *key.
  // Simple comparator implementations may return with *key unchanged,
  // i.e., an implementation of this method that does nothing is correct.
  void FindShortSuccessor(std::string* key) const override {
    jmethodID mid = env_->GetMethodID(cls_, "findShortSuccessor", "([B)[B");
    jbyteArray jkey = env_->NewByteArray(key->size());
    env_->SetByteArrayRegion(jkey, 0, key->size(),
                             reinterpret_cast<const jbyte*>(key->data()));
    jbyteArray jresult =
        static_cast<jbyteArray>(env_->CallObjectMethod(jobj_, mid, jkey));
    auto l = env_->GetArrayLength(jresult);
    const char* ptr = reinterpret_cast<const char*>(env_->GetByteArrayElements(jresult, NULL));
    *key = std::string(ptr, ptr + l);
    env_->ReleaseByteArrayElements(
        jresult, reinterpret_cast<jbyte*>(const_cast<char*>(ptr)), JNI_ABORT);
  }
};

class JavaLogger : public Logger {
 private:
  JNIEnv* env_;
  jobject jobj_;
  jclass cls_;

 public:
  JavaLogger(JNIEnv* env, jobject jobj) : env_(env), jobj_(jobj) {
    cls_ = env_->GetObjectClass(jobj_);
  }

  ~JavaLogger() override { env_->DeleteGlobalRef(jobj_); }

  void Logv(const char* format, std::va_list ap) override {
    jmethodID mid = env_->GetMethodID(cls_, "log", "(Ljava/lang/String;)V");
    char buffer[1024];
    vsnprintf(buffer, sizeof(buffer), format, ap);
    jstring jstr = env_->NewStringUTF(buffer);
    env_->CallVoidMethod(jobj_, mid, jstr);
  }
};

static bool check(JNIEnv* env, const Status& status) {
  if (status.ok()) {
    return true;
  } else if (status.IsNotFound()) {
    jclass exception_class = env->FindClass("java/io/FileNotFoundException");
    env->ThrowNew(exception_class, status.ToString().c_str());
    return false;
  } else if (status.IsCorruption()) {
    jclass exception_class = env->FindClass("java/io/IOException");
    env->ThrowNew(exception_class, status.ToString().c_str());
    return false;
  } else {
    jclass exception_class = env->FindClass("java/lang/RuntimeException");
    env->ThrowNew(exception_class, status.ToString().c_str());
    return false;
  }
}

JNIEXPORT void JNICALL Java_org_tron_leveldb_DB_init(JNIEnv* env, jobject jdb, jobject jfile, jobject joptions) {
  jclass db_class = env->GetObjectClass(jdb);
  jclass options_class = env->GetObjectClass(joptions);
  Options options;
  options.create_if_missing = env->GetBooleanField(
      joptions, env->GetFieldID(options_class, "createIfMissing", "Z"));
  options.paranoid_checks = env->GetBooleanField(
      joptions, env->GetFieldID(options_class, "paranoidChecks", "Z"));

  jclass compression_type_class =
      env->FindClass("org/tron/leveldb/CompressionType");
  jobject jcompression_type = env->GetObjectField(
      joptions, env->GetFieldID(options_class, "compressionType",
                                "Lorg/tron/leveldb/CompressionType;"));
  
  options.compression = static_cast<CompressionType>(env->GetIntField(
      jcompression_type,
      env->GetFieldID(compression_type_class, "persistentId", "I")));

  options.block_size = env->GetIntField(
      joptions, env->GetFieldID(options_class, "blockSize", "I"));
  options.write_buffer_size = env->GetIntField(
      joptions, env->GetFieldID(options_class, "writeBufferSize", "I"));
  options.block_cache = NewLRUCache(env->GetLongField(
      joptions, env->GetFieldID(options_class, "cacheSize", "J")));
  options.max_open_files = env->GetIntField(
      joptions, env->GetFieldID(options_class, "maxOpenFiles", "I"));


  jobject jcomparator = env->GetObjectField(
      joptions, env->GetFieldID(options_class, "comparator",
                                "Lorg/tron/leveldb/DBComparator;"));

  if (jcomparator != NULL) {
    auto ptr = new JavaComparator(env, env->NewGlobalRef(jcomparator));
    env->SetLongField(jdb, env->GetFieldID(db_class, "nativeComparator", "J"),
                      reinterpret_cast<jlong>(ptr));
    options.comparator = ptr;
  }

  jobject jlogger = env->GetObjectField(
      joptions,
      env->GetFieldID(options_class, "logger", "Lorg/tron/leveldb/Logger;"));

  if (jlogger != NULL) {
    auto ptr = new JavaLogger(env, env->NewGlobalRef(jlogger));
    env->SetLongField(jdb, env->GetFieldID(db_class, "nativeLogger", "J"),
                      reinterpret_cast<jlong>(ptr));
    options.info_log = ptr;
  }

  jclass file_class = env->GetObjectClass(jfile);
  jstring path =
      static_cast<jstring>(env->CallObjectMethod(
      jfile, env->GetMethodID(file_class, "getPath", "()Ljava/lang/String;")));

  DB* db;
  const char* pathUTF = env->GetStringUTFChars(path, nullptr);
  auto status = DB::Open(options, pathUTF, &db);
  env->ReleaseStringUTFChars(path, pathUTF);

  if (status.IsInvalidArgument()) {
    jclass exception_class = env->FindClass("java/io/FileNotFoundException");
    env->ThrowNew(exception_class, status.ToString().c_str());
  } else if (check(env, status)) {
    env->SetLongField(jdb, env->GetFieldID(db_class, "nativeDb", "J"),
                      reinterpret_cast<jlong>(db));
  
  }
}

ReadOptions getReadOptions(JNIEnv* env, jobject jread_options) {
  jclass read_options_class = env->GetObjectClass(jread_options);
  ReadOptions read_options;
  read_options.verify_checksums = env->GetBooleanField(
      jread_options,
      env->GetFieldID(read_options_class, "verifyChecksums", "Z"));
  read_options.fill_cache = env->GetBooleanField(
      jread_options, env->GetFieldID(read_options_class, "fillCache", "Z"));
  return read_options;
}

void throw_not_open(JNIEnv* env) {
  jclass exception_class = env->FindClass("java/lang/IllegalStateException");
  env->ThrowNew(exception_class, "DB is not open");
}

JNIEXPORT jobject JNICALL Java_org_tron_leveldb_DB_iterator(JNIEnv* env, jobject jdb,
                                                            jobject jread_options) {
  jclass db_class = env->GetObjectClass(jdb);
  DB* db = reinterpret_cast<DB*>(
      env->GetLongField(jdb, env->GetFieldID(db_class, "nativeDb", "J")));
  if (!db) {
    throw_not_open(env);
    return NULL;
  }

  ReadOptions read_options = getReadOptions(env, jread_options);
  Iterator* iterator = db->NewIterator(read_options);
  return env->NewObject(env->FindClass("org/tron/leveldb/DBIterator"),
                 env->GetMethodID(env->FindClass("org/tron/leveldb/DBIterator"),
                                  "<init>", "(J)V"),
                 reinterpret_cast<jlong>(iterator));
}

JNIEXPORT void JNICALL Java_org_tron_leveldb_DB_close(JNIEnv* env, jobject jdb) {
  jclass db_class = env->GetObjectClass(jdb);
  DB* db = reinterpret_cast<DB*>(
      env->GetLongField(jdb, env->GetFieldID(db_class, "nativeDb", "J")));
  if (db)
    delete db;
  env->SetLongField(jdb, env->GetFieldID(db_class, "nativeDb", "J"), 0);
}

JNIEXPORT void JNICALL Java_org_tron_leveldb_DB_put___3B_3B(JNIEnv* env, jobject jdb,
                                                            jbyteArray key,
                                                            jbyteArray data) {
  jclass db_class = env->GetObjectClass(jdb);
  DB* db = reinterpret_cast<DB*>(
      env->GetLongField(jdb, env->GetFieldID(db_class, "nativeDb", "J")));

  if (!db) {
    throw_not_open(env);
    return;
  }

  Slice key_slice(reinterpret_cast<const char*>(env->GetByteArrayElements(key, nullptr)),
                  env->GetArrayLength(key));
  Slice data_slice(reinterpret_cast<const char*>(env->GetByteArrayElements(data, nullptr)), 
                   env->GetArrayLength(data));
  db->Put(WriteOptions(), key_slice, data_slice);
  env->ReleaseByteArrayElements(
      key, reinterpret_cast<jbyte*>(const_cast<char*>(key_slice.data())),
      JNI_ABORT);
  env->ReleaseByteArrayElements(
      data, reinterpret_cast<jbyte*>(const_cast<char*>(data_slice.data())),
      JNI_ABORT);
}

JNIEXPORT void JNICALL Java_org_tron_leveldb_DB_put___3B_3BZ(JNIEnv* env, jobject jdb,
                                                             jbyteArray key,
                                                             jbyteArray data,
                                                             jboolean sync) {
  jclass db_class = env->GetObjectClass(jdb);
  DB* db = reinterpret_cast<DB*>(
      env->GetLongField(jdb, env->GetFieldID(db_class, "nativeDb", "J")));
  if (!db) {
    throw_not_open(env);
    return;
  }

  Slice key_slice(
      reinterpret_cast<const char*>(env->GetByteArrayElements(key, nullptr)), 
      env->GetArrayLength(key));
  Slice data_slice(
      reinterpret_cast<const char*>(env->GetByteArrayElements(data, nullptr)), 
      env->GetArrayLength(data));
  WriteOptions options;
  options.sync = sync;
  db->Put(options, key_slice, data_slice);
  env->ReleaseByteArrayElements(
      key, reinterpret_cast<jbyte*>(const_cast<char*>(key_slice.data())),
      JNI_ABORT);
  env->ReleaseByteArrayElements(data, reinterpret_cast<jbyte*>(const_cast<char*>(data_slice.data())),
      JNI_ABORT);
}

JNIEXPORT jbyteArray JNICALL Java_org_tron_leveldb_DB_get(JNIEnv* env,
                                                          jobject jdb,
                                                          jbyteArray key) {
  jclass db_class = env->GetObjectClass(jdb);
  DB* db = reinterpret_cast<DB*>(
      env->GetLongField(jdb, env->GetFieldID(db_class, "nativeDb", "J")));
  if (!db) {
    throw_not_open(env);
    return NULL;
  }

  ReadOptions options;
  if (env->GetBooleanField(
          jdb, env->GetFieldID(db_class, "verifyChecksumsSet", "Z"))) {
    options.verify_checksums = true;
  }
  Slice key_slice(
      reinterpret_cast<const char*>(env->GetByteArrayElements(key, nullptr)), 
      env->GetArrayLength(key));
  std::string value;
  auto status = db->Get(options, key_slice, &value);
  env->ReleaseByteArrayElements(
      key, reinterpret_cast<jbyte*>(const_cast<char*>(key_slice.data())),
      JNI_ABORT);

  if (status.IsNotFound()) {
    return NULL;
  }

  if (check(env, status)) {
    jbyteArray jvalue = env->NewByteArray(value.size());
    env->SetByteArrayRegion(jvalue, 0, value.size(),
                            reinterpret_cast<const jbyte*>(value.data()));
    return jvalue;
  }
  return NULL;
}

JNIEXPORT void JNICALL Java_org_tron_leveldb_DB_delete(JNIEnv* env, jobject jdb,
                                                       jbyteArray key,
                                                       jboolean sync) {
  jclass db_class = env->GetObjectClass(jdb);
  DB* db = reinterpret_cast<DB*>(
      env->GetLongField(jdb, env->GetFieldID(db_class, "nativeDb", "J")));
  if (!db) {
    throw_not_open(env);
    return;
  }

  Slice key_slice(
      reinterpret_cast<const char*>(env->GetByteArrayElements(key, nullptr)), 
      env->GetArrayLength(key));
  WriteOptions options;
  options.sync = sync;
  db->Delete(options, key_slice);
  env->ReleaseByteArrayElements(
      key, reinterpret_cast<jbyte*>(const_cast<char*>(key_slice.data())),
      JNI_ABORT);
}

JNIEXPORT jobject JNICALL Java_org_tron_leveldb_DB_createWriteBatch(JNIEnv* env, 
                                                                    jobject jdb) {
  jclass db_class = env->GetObjectClass(jdb);
  DB* db = reinterpret_cast<DB*>(
      env->GetLongField(jdb, env->GetFieldID(db_class, "nativeDb", "J")));
  if (!db) {
    throw_not_open(env);
    return NULL;
  }

  WriteBatch* batch = new WriteBatch();
  return env->NewObject(
      env->FindClass("org/tron/leveldb/WriteBatch"),
      env->GetMethodID(env->FindClass("org/tron/leveldb/WriteBatch"), "<init>",
                       "(J)V"),
      reinterpret_cast<jlong>(batch));
}

JNIEXPORT void JNICALL Java_org_tron_leveldb_DB_write(JNIEnv* env, jobject jdb,
                                                      jobject jbatch,
                                                      jboolean sync) {
  jclass db_class = env->GetObjectClass(jdb);
  DB* db = reinterpret_cast<DB*>(
      env->GetLongField(jdb, env->GetFieldID(db_class, "nativeDb", "J")));
  if (!db) {
    throw_not_open(env);
    return ;
  }

  jclass batch_class = env->GetObjectClass(jbatch);
  WriteBatch* batch = reinterpret_cast<WriteBatch*>(env->GetLongField(
      jbatch, env->GetFieldID(batch_class, "nativeHandle", "J")));
  WriteOptions options;
  options.sync = sync;
  auto status = db->Write(options, batch);
  check(env, status);
}

JNIEXPORT jstring JNICALL Java_org_tron_leveldb_DB_getProperty(JNIEnv* env,
                                                               jobject jdb,
                                                               jstring jname) {
  jclass db_class = env->GetObjectClass(jdb);
  DB* db = reinterpret_cast<DB*>(
      env->GetLongField(jdb, env->GetFieldID(db_class, "nativeDb", "J")));
  if (!db) {
    throw_not_open(env);
    return NULL;
  }

  const char* name = env->GetStringUTFChars(jname, nullptr);
  std::string value;
  db->GetProperty(name, &value);
  env->ReleaseStringUTFChars(jname, name);
  return env->NewStringUTF(value.c_str());
}

void throw_it_not_open(JNIEnv* env) {
  jclass exception_class = env->FindClass("java/lang/IllegalStateException");
  env->ThrowNew(exception_class, "DB iterator is not open");
}

JNIEXPORT void JNICALL Java_org_tron_leveldb_DBIterator_close(JNIEnv* env,
                                                              jobject jiter) {
  jclass iter_class = env->GetObjectClass(jiter);
  Iterator* iter = reinterpret_cast<Iterator*>(env->GetLongField(
      jiter, env->GetFieldID(iter_class, "nativeHandle", "J")));
  if (iter)
    delete iter;
  env->SetLongField(
      jiter, env->GetFieldID(iter_class, "nativeHandle", "J"), 0);
}

JNIEXPORT void JNICALL Java_org_tron_leveldb_DBIterator_seekToFirst(JNIEnv* env, jobject jiter) {
  jclass iter_class = env->GetObjectClass(jiter);
  Iterator* iter = reinterpret_cast<Iterator*>(env->GetLongField(
      jiter, env->GetFieldID(iter_class, "nativeHandle", "J")));
  if (!iter) {
    throw_it_not_open(env);
    return;
  }
  iter->SeekToFirst();
}

JNIEXPORT jboolean JNICALL Java_org_tron_leveldb_DBIterator_hasNext(JNIEnv* env, jobject jiter) {
  jclass iter_class = env->GetObjectClass(jiter);
  Iterator* iter = reinterpret_cast<Iterator*>(env->GetLongField(
      jiter, env->GetFieldID(iter_class, "nativeHandle", "J")));
  if (!iter) {
    throw_it_not_open(env);
    return 0;
  }
  return iter->Valid();
}

JNIEXPORT jobject JNICALL Java_org_tron_leveldb_DBIterator_next(JNIEnv* env,
                                                                jobject jiter) {
  jclass iter_class = env->GetObjectClass(jiter);
  Iterator* iter = reinterpret_cast<Iterator*>(env->GetLongField(
      jiter, env->GetFieldID(iter_class, "nativeHandle", "J")));
  if (!iter) {
    throw_it_not_open(env);
    return NULL;
  }

  jbyteArray key_array = env->NewByteArray(iter->key().size());
  env->SetByteArrayRegion(key_array, 0, iter->key().size(),
                          reinterpret_cast<const jbyte*>(iter->key().data()));
  jbyteArray value_array = env->NewByteArray(iter->value().size());
  env->SetByteArrayRegion(value_array, 0, iter->value().size(),
                          reinterpret_cast<const jbyte*>(iter->value().data()));
  
  iter->Next();

  return env->NewObject(
      env->FindClass("java/util/AbstractMap$SimpleEntry"),
      env->GetMethodID(env->FindClass("java/util/AbstractMap$SimpleEntry"),
                       "<init>", "(Ljava/lang/Object;Ljava/lang/Object;)V"),
      key_array,
      value_array);
}

JNIEXPORT jobject JNICALL
Java_org_tron_leveldb_DBIterator_peekNext(JNIEnv* env, jobject jiter) {
  jclass iter_class = env->GetObjectClass(jiter);
  Iterator* iter = reinterpret_cast<Iterator*>(env->GetLongField(
      jiter, env->GetFieldID(iter_class, "nativeHandle", "J")));
  if (!iter) {
    throw_it_not_open(env);
    return NULL;
  }

  jbyteArray key_array = env->NewByteArray(iter->key().size());
  env->SetByteArrayRegion(key_array, 0, iter->key().size(),
                          reinterpret_cast<const jbyte*>(iter->key().data()));
  jbyteArray value_array = env->NewByteArray(iter->value().size());
  env->SetByteArrayRegion(value_array, 0, iter->value().size(),
                          reinterpret_cast<const jbyte*>(iter->value().data()));
  return env->NewObject(
      env->FindClass("java/util/AbstractMap$SimpleEntry"),
      env->GetMethodID(env->FindClass("java/util/AbstractMap$SimpleEntry"),
                       "<init>", "(Ljava/lang/Object;Ljava/lang/Object;)V"),
      key_array,
      value_array);
}

JNIEXPORT void JNICALL
Java_org_tron_leveldb_DBIterator_seekToLast(JNIEnv* env, jobject jiter) {
  jclass iter_class = env->GetObjectClass(jiter);
  Iterator* iter = reinterpret_cast<Iterator*>(env->GetLongField(
      jiter, env->GetFieldID(iter_class, "nativeHandle", "J")));
  if (!iter) {
    throw_it_not_open(env);
    return;
  }

  iter->SeekToLast();
}

JNIEXPORT jboolean JNICALL Java_org_tron_leveldb_DBIterator_hasPrev(JNIEnv* env,
                                                                    jobject jiter) {
  jclass iter_class = env->GetObjectClass(jiter);
  Iterator* iter = reinterpret_cast<Iterator*>(env->GetLongField(
      jiter, env->GetFieldID(iter_class, "nativeHandle", "J")));
  if (!iter) {
    throw_it_not_open(env);
    return 0;
  }

  return iter->Valid();
}

JNIEXPORT void JNICALL Java_org_tron_leveldb_DBIterator_prev(JNIEnv* env,
                                                             jobject jiter) {
  jclass iter_class = env->GetObjectClass(jiter);
  Iterator* iter = reinterpret_cast<Iterator*>(env->GetLongField(
      jiter, env->GetFieldID(iter_class, "nativeHandle", "J")));
  if (!iter) {
    throw_it_not_open(env);
    return;
  }

  iter->Prev();
}

JNIEXPORT jobject JNICALL
Java_org_tron_leveldb_DBIterator_peekPrev(JNIEnv* env, jobject jiter) {
  jclass iter_class = env->GetObjectClass(jiter);
  Iterator* iter = reinterpret_cast<Iterator*>(env->GetLongField(
      jiter, env->GetFieldID(iter_class, "nativeHandle", "J")));
  if (!iter) {
    throw_it_not_open(env);
    return NULL;
  }

  jbyteArray key_array = env->NewByteArray(iter->key().size());
  env->SetByteArrayRegion(key_array, 0, iter->key().size(),
                          reinterpret_cast<const jbyte*>(iter->key().data()));
  jbyteArray value_array = env->NewByteArray(iter->value().size());
  env->SetByteArrayRegion(value_array, 0, iter->value().size(),
                          reinterpret_cast<const jbyte*>(iter->value().data()));
  return env->NewObject(
      env->FindClass("java/util/AbstractMap$SimpleEntry"),
      env->GetMethodID(env->FindClass("java/util/AbstractMap$SimpleEntry"),
                       "<init>", "(Ljava/lang/Object;Ljava/lang/Object;)V"),
      key_array, value_array);

}

JNIEXPORT void JNICALL Java_org_tron_leveldb_DBIterator_seek(JNIEnv* env,
                                                             jobject jiter,
                                                             jbyteArray key) {
  jclass iter_class = env->GetObjectClass(jiter);
  Iterator* iter = reinterpret_cast<Iterator*>(env->GetLongField(
      jiter, env->GetFieldID(iter_class, "nativeHandle", "J")));
  if (!iter) {
    throw_it_not_open(env);
    return;
  }

  Slice key_slice(
      reinterpret_cast<const char*>(env->GetByteArrayElements(key, nullptr)), 
      env->GetArrayLength(key));
  iter->Seek(key_slice);
  env->ReleaseByteArrayElements(
      key, reinterpret_cast<jbyte*>(const_cast<char*>(key_slice.data())),
      JNI_ABORT);
}

// implement WriteBatch native methods
JNIEXPORT void JNICALL Java_org_tron_leveldb_WriteBatch_close(JNIEnv* env,
                                                              jobject jbatch) {
  jclass batch_class = env->GetObjectClass(jbatch);
  WriteBatch* batch = reinterpret_cast<WriteBatch*>(env->GetLongField(
      jbatch, env->GetFieldID(batch_class, "nativeHandle", "J")));
  if (batch) delete batch;
  env->SetLongField(jbatch, env->GetFieldID(batch_class, "nativeHandle", "J"),
                    0);
}

void throw_batch_not_open(JNIEnv* env) {
  jclass exception_class = env->FindClass("java/lang/IllegalStateException");
  env->ThrowNew(exception_class, "WriteBatch is not open");
}

JNIEXPORT void JNICALL Java_org_tron_leveldb_WriteBatch_put(JNIEnv* env,
                                                            jobject jbatch,
                                                            jbyteArray key,
                                                            jbyteArray value) {
  jclass batch_class = env->GetObjectClass(jbatch);
  WriteBatch* batch = reinterpret_cast<WriteBatch*>(env->GetLongField(
      jbatch, env->GetFieldID(batch_class, "nativeHandle", "J")));
  if (!batch) {
    throw_batch_not_open(env);
    return;
  }

  Slice key_slice(
      reinterpret_cast<const char*>(env->GetByteArrayElements(key, nullptr)), 
      env->GetArrayLength(key));
  Slice value_slice(
      reinterpret_cast<const char*>(env->GetByteArrayElements(value, nullptr)), 
      env->GetArrayLength(value));
  batch->Put(key_slice, value_slice);
  env->ReleaseByteArrayElements(
      key, reinterpret_cast<jbyte*>(const_cast<char*>(key_slice.data())),
      JNI_ABORT);
  env->ReleaseByteArrayElements(
      value, reinterpret_cast<jbyte*>(const_cast<char*>(value_slice.data())),
      JNI_ABORT);
}

JNIEXPORT void JNICALL Java_org_tron_leveldb_WriteBatch_delete(JNIEnv* env,
                                                               jobject jbatch,
                                                               jbyteArray key) {
  jclass batch_class = env->GetObjectClass(jbatch);
  WriteBatch* batch = reinterpret_cast<WriteBatch*>(env->GetLongField(
      jbatch, env->GetFieldID(batch_class, "nativeHandle", "J")));
  if (!batch) {
    throw_batch_not_open(env);
    return;
  }

  Slice key_slice(
      reinterpret_cast<const char*>(env->GetByteArrayElements(key, nullptr)), 
      env->GetArrayLength(key));
  batch->Delete(key_slice);
  env->ReleaseByteArrayElements(
      key, reinterpret_cast<jbyte*>(const_cast<char*>(key_slice.data())),
      JNI_ABORT);
}
