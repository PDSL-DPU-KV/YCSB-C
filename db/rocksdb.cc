//
//  rocksdb.cc
//  YCSB-C
//

#include "rocksdb.h"

#include "coding.h"

using namespace std;
using namespace rapidjson;

namespace ycsbc {
RocksDB::RocksDB(const char *dbfilename, utils::Properties &props)
    : noResult(0), dbstats_(nullptr), write_op_(nullptr) {
  // 1. Open and read from json config file
  std::string config_file_path =
      props.GetProperty("dboptions", "../config/rocksdb.json");
  std::ostringstream file_out_buffer;
  std::ifstream config_file(config_file_path);
  file_out_buffer << config_file.rdbuf();
  std::string config_json_str(file_out_buffer.str());
  config_file.close();
  cout << config_json_str << endl;
  // 2. Parse json string
  Document doc;
  doc.Parse(config_json_str.c_str());
  assert(doc.IsObject());
  // 3. Set Options
  write_op_ = new rocksdb::WriteOptions();
  rocksdb::Options options;
  options.create_if_missing = true;
  SetOptions(&options, props, doc);
  // 4. Open RocksDB
  rocksdb::Status s = rocksdb::DB::Open(options, dbfilename, &db_);
  if (!s.ok()) {
    cerr << "Can't open rocksdb " << dbfilename << " " << s.ToString() << endl;
    exit(0);
  }
  // 5. Set Remote Compaction
  auto iter_rc_options = doc.FindMember("RemoteCompaction");
  if (iter_rc_options != doc.MemberEnd()) {
    auto rc_options = iter_rc_options->value.GetObject();
    auto iter_use_rc = rc_options.FindMember("use_remote_compaction");
    if (iter_use_rc != rc_options.MemberEnd() && iter_use_rc->value.GetBool()) {
      auto iter = rc_options.FindMember("remote_compaction_use_rdma");
      bool use_rdma =
          iter != rc_options.MemberEnd() ? iter->value.GetBool() : true;

      iter = rc_options.FindMember("remote_compaction_server_ip");
      std::string server_ip = iter != rc_options.MemberEnd()
                                  ? iter->value.GetString()
                                  : "127.0.0.1";

      iter = rc_options.FindMember("remote_compaction_server_port");
      int server_port =
          iter != rc_options.MemberEnd() ? iter->value.GetInt() : 8411;

      SetupPluggableCompaction(server_ip, server_port, use_rdma);
    }
  }
}

void RocksDB::SetOptions(rocksdb::Options *options, utils::Properties &props,
                         rapidjson::Document &doc) {
  // 1. Set rocksdb options with config file
  auto iter = doc.FindMember("write_buffer_size");
  options->write_buffer_size =
      iter != doc.MemberEnd() ? iter->value.GetUint64() : 64 << 20;
  
  iter = doc.FindMember("target_file_size_base");
  options->target_file_size_base =
      iter != doc.MemberEnd() ? iter->value.GetUint64() : 64 << 20;
  
  iter = doc.FindMember("max_write_buffer_number");
  options->max_write_buffer_number =
      iter != doc.MemberEnd() ? iter->value.GetInt() : 2;
  
  iter = doc.FindMember("max_bytes_for_level_base");
  options->max_bytes_for_level_base =
      iter != doc.MemberEnd() ? iter->value.GetUint64() : 256 << 20;
  
  iter = doc.FindMember("wal_bytes_per_sync");
  options->wal_bytes_per_sync =
      iter != doc.MemberEnd() ? iter->value.GetUint64() : 0;

  iter = doc.FindMember("max_background_flushes");
  options->max_background_flushes =
      iter != doc.MemberEnd() ? iter->value.GetInt() : -1;

  iter = doc.FindMember("max_background_compactions");
  options->max_background_compactions =
      iter != doc.MemberEnd() ? iter->value.GetInt() : -1;
  
  iter = doc.FindMember("max_background_jobs");
  options->max_background_jobs =
      iter != doc.MemberEnd() ? iter->value.GetInt() : 2;
  
  iter = doc.FindMember("max_subcompactions");
  options->max_subcompactions =
      iter != doc.MemberEnd() ? iter->value.GetUint() : 1;

  iter = doc.FindMember("compression_type");
  std::string compression_type_str =
      iter != doc.MemberEnd() ? iter->value.GetString() : "no";
  std::unordered_map<std::string, rocksdb::CompressionType>
      compression_type_map = {
          {"no", rocksdb::kNoCompression},
          {"zlib", rocksdb::kZlibCompression},
          {"snappy", rocksdb::kSnappyCompression},
          {"dpu", rocksdb::kDpuCompression},
          {"lz4", rocksdb::kLZ4Compression},
      };
  auto compress_it = compression_type_map.find(compression_type_str);
  options->compression = compress_it != compression_type_map.end()
                             ? compress_it->second
                             : rocksdb::kNoCompression;
  if (options->compression == rocksdb::kDpuCompression) {
    if (!rocksdb::Dpu_Compressdev_Init()) {
      cerr << "Dpu_Compressdev_Init() failed! options->compression set to "
              "kNoCompression."
           << endl;
      options->compression = rocksdb::kNoCompression;
    }
  }

  iter = doc.FindMember("use_direct_io_for_flush_and_compaction");
  options->use_direct_io_for_flush_and_compaction =
      iter != doc.MemberEnd() ? iter->value.GetBool() : false;

  iter = doc.FindMember("use_direct_reads");
  options->use_direct_reads =
      iter != doc.MemberEnd() ? iter->value.GetBool() : false;
  
  iter = doc.FindMember("stats_dump_period_sec");
  options->stats_dump_period_sec =
      iter != doc.MemberEnd() ? iter->value.GetUint() : 600;
  // 2. Set rocksdb write options with config file
  iter = doc.FindMember("WriteOptions");
  if (iter != doc.MemberEnd()) {
    auto write_options = iter->value.GetObject();

    auto iter_write_op = write_options.FindMember("disableWAL");
    write_op_->disableWAL = iter_write_op != write_options.MemberEnd()
                                ? iter_write_op->value.GetBool()
                                : false;

    iter_write_op = write_options.FindMember("sync");
    write_op_->sync = iter_write_op != write_options.MemberEnd()
                          ? iter_write_op->value.GetBool()
                          : false;
  }
  // 3. Set statistics or not
  bool statistics = utils::StrToBool(props["dbstatistics"]);
  if (statistics) {
    dbstats_ = rocksdb::CreateDBStatistics();
    options->statistics = dbstats_;
  }
}

// Wire up all compaction requests through our pluggable service
void RocksDB::SetupPluggableCompaction(const std::string &server_ip,
                                       const int server_port,
                                       const bool use_rdma) {
  // create a service object
  std::unique_ptr<rocksdb::PluggableCompactionService> service;
  service.reset(rocksdb::NewGenericPluggableCompactionService(
      db_->GetName(), server_ip, server_port, use_rdma));
  // Setup our local DB to invoke a custom service. This will ensure that
  // all compaction requests will flow through the service object. The
  // service object forwards the compaction requests to the clone.
  db_->RegisterPluggableCompactionService(std::move(service));
}

void RocksDB::CleanupPluggableCompaction() {
  // UnRegister
  db_->UnRegisterPluggableCompactionService();
}

int RocksDB::Read(const std::string &table, const std::string &key,
                  const std::vector<std::string> *fields,
                  std::vector<KVPair> &result) {
  string value;
  rocksdb::Status s = db_->Get(rocksdb::ReadOptions(), key, &value);
  if (s.ok()) {
    // printf("value:%lu\n",value.size());
    DeSerializeValues(value, result);
    /* printf("get:key:%lu-%s\n",key.size(),key.data());
    for( auto kv : result) {
        printf("get field:key:%lu-%s
    value:%lu-%s\n",kv.first.size(),kv.first.data(),kv.second.size(),kv.second.data());
    } */
    return DB::kOK;
  }
  if (s.IsNotFound()) {
    noResult++;
    // cerr<<"read not found:"<<noResult<<endl;
    return DB::kOK;
  } else {
    cerr << "read error" << endl;
    exit(0);
  }
}

int RocksDB::Scan(const std::string &table, const std::string &key, int len,
                  const std::vector<std::string> *fields,
                  std::vector<std::vector<KVPair>> &result) {
  auto it = db_->NewIterator(rocksdb::ReadOptions());
  it->Seek(key);
  std::string val;
  std::string k;
  // printf("len:%d\n",len);
  for (int i = 0; i < len && it->Valid(); i++) {
    k = it->key().ToString();
    val = it->value().ToString();
    // printf("i:%d key:%lu value:%lu\n",i,k.size(),val.size());
    it->Next();
  }
  delete it;
  return DB::kOK;
}

int RocksDB::Insert(const std::string &table, const std::string &key,
                    std::vector<KVPair> &values) {
  rocksdb::Status s;
  string value;
  SerializeValues(values, value);
  /* printf("put:key:%lu-%s\n",key.size(),key.data());
  for( auto kv : values) {
      printf("put field:key:%lu-%s
  value:%lu-%s\n",kv.first.size(),kv.first.data(),kv.second.size(),kv.second.data());
  } */

  s = db_->Put(*write_op_, key, value);
  if (!s.ok()) {
    cerr << "insert error\n" << endl;
    exit(0);
  }

  return DB::kOK;
}

int RocksDB::Update(const std::string &table, const std::string &key,
                    std::vector<KVPair> &values) {
  return Insert(table, key, values);
}

int RocksDB::Delete(const std::string &table, const std::string &key) {
  rocksdb::Status s;

  s = db_->Delete(*write_op_, key);
  if (!s.ok()) {
    cerr << "Delete error\n" << endl;
    exit(0);
  }
  return DB::kOK;
}

void RocksDB::PrintStats() {
  if (noResult) cout << "read not found:" << noResult << endl;
  string stats;
  if (db_->GetProperty("rocksdb.stats", &stats)) {
    cout << stats << endl;
  }
  if (db_->GetProperty("rocksdb.aggregated-table-properties", &stats)) {
    cout << stats <<endl;
  }
  if (dbstats_.get() != nullptr) {
    fprintf(stdout, "STATISTICS:\n%s\n", dbstats_->ToString().c_str());
  }
}

RocksDB::~RocksDB() {
  printf("wait delete remote_compaction...\n");
  CleanupPluggableCompaction();
  auto compression_type = db_->GetOptions().compression;
  printf("wait delete db...\n");
  delete db_;
  delete write_op_;
  if (compression_type == rocksdb::kDpuCompression) {
    printf("wait delete Dpu_Compressdev...\n");
    rocksdb::Dpu_Compressdev_Destroy();
  }
  printf("all clear!\n");
  /*if (cache_.get() != nullptr) {
       this will leak, but we're shutting down so nobody cares
      cache_->DisownData();
  }*/
}

void RocksDB::SerializeValues(std::vector<KVPair> &kvs, std::string &value) {
  value.clear();
  PutFixed64(&value, kvs.size());
  for (unsigned int i = 0; i < kvs.size(); i++) {
    PutFixed64(&value, kvs[i].first.size());
    value.append(kvs[i].first);
    PutFixed64(&value, kvs[i].second.size());
    value.append(kvs[i].second);
  }
}

void RocksDB::DeSerializeValues(std::string &value, std::vector<KVPair> &kvs) {
  uint64_t offset = 0;
  uint64_t kv_num = 0;
  uint64_t key_size = 0;
  uint64_t value_size = 0;

  kv_num = DecodeFixed64(value.c_str());
  offset += 8;
  for (unsigned int i = 0; i < kv_num; i++) {
    ycsbc::DB::KVPair pair;
    key_size = DecodeFixed64(value.c_str() + offset);
    offset += 8;

    pair.first.assign(value.c_str() + offset, key_size);
    offset += key_size;

    value_size = DecodeFixed64(value.c_str() + offset);
    offset += 8;

    pair.second.assign(value.c_str() + offset, value_size);
    offset += value_size;
    kvs.push_back(pair);
  }
}

bool RocksDB::HaveBalancedDistribution() {
  // return true;
  return db_->HaveBalancedDistribution();
}
}  // namespace ycsbc
