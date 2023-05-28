//
//  rocksdb.cc
//  YCSB-C
//

#include "rocksdb.h"

#include "coding.h"
#include "rocksdb/cache.h"
#include "rocksdb/options.h"
#include "rocksdb/statistics.h"

using namespace std;
using namespace rapidjson;

namespace ycsbc {
RocksDB::RocksDB(const char *dbfilename, utils::Properties &props)
    : noResult(0), dbstats_(nullptr), write_op_(nullptr), read_op_(nullptr) {
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
  read_op_ = new rocksdb::ReadOptions();
  rocksdb::Options options;
  options.create_if_missing = true;
  SetOptions(&options, props, doc);
  // 4. Open RocksDB
  rocksdb::Status s = rocksdb::DB::Open(options, dbfilename, &db_);
  if (!s.ok()) {
    cerr << "Can't open rocksdb " << dbfilename << " " << s.ToString() << endl;
    exit(0);
  }
}

void RocksDB::SetOptions(rocksdb::Options *options, utils::Properties &props,
                         rapidjson::Document &doc) {
  // 1. Set rocksdb options with config file
  auto iter = doc.FindMember("write_buffer_size");
  options->write_buffer_size =
      iter != doc.MemberEnd() ? iter->value.GetUint64() : 64 << 20;

  iter = doc.FindMember("max_write_buffer_number");
  options->max_write_buffer_number =
      iter != doc.MemberEnd() ? iter->value.GetInt() : 2;

  iter = doc.FindMember("max_background_flushes");
  options->max_background_flushes =
      iter != doc.MemberEnd() ? iter->value.GetInt() : -1;

  iter = doc.FindMember("max_background_compactions");
  options->max_background_compactions =
      iter != doc.MemberEnd() ? iter->value.GetInt() : -1;

  iter = doc.FindMember("subcompactions");
  options->max_subcompactions =
      iter != doc.MemberEnd() ? iter->value.GetInt() : 1;

  iter = doc.FindMember("compression_type");
  std::string compression_type_str =
      iter != doc.MemberEnd() ? iter->value.GetString() : "no";
  std::unordered_map<std::string, rocksdb::CompressionType>
      compression_type_map = {
          {"no", rocksdb::kNoCompression},
          {"snappy", rocksdb::kSnappyCompression},
          {"lz4", rocksdb::kLZ4Compression},
          {"zstd", rocksdb::kZSTD},
          {"zlib", rocksdb::kZlibCompression},
      };
  auto compress_it = compression_type_map.find(compression_type_str);
  options->compression = compress_it != compression_type_map.end()
                             ? compress_it->second
                             : rocksdb::kNoCompression;

  iter = doc.FindMember("num_levels");
  options->num_levels = iter != doc.MemberEnd() ? iter->value.GetInt() : 7;

  iter = doc.FindMember("min_level_to_compress");
  auto ml = iter != doc.MemberEnd() ? iter->value.GetInt() : 7;
  if (ml >= 0) {
    options->compression_per_level.resize(options->num_levels);
    for (int i = 0; i < ml; i++) {
      options->compression_per_level[i] = rocksdb::kNoCompression;
    }
    for (int i = ml; i <= options->num_levels; i++) {
      options->compression_per_level[i] = options->compression;
    }
  }

  iter = doc.FindMember("use_direct_reads");
  options->use_direct_reads =
      iter != doc.MemberEnd() ? iter->value.GetBool() : false;

  iter = doc.FindMember("use_direct_io_for_flush_and_compaction");
  options->use_direct_io_for_flush_and_compaction =
      iter != doc.MemberEnd() ? iter->value.GetBool() : false;

  iter = doc.FindMember("report_bg_io_stats");
  options->report_bg_io_stats =
      iter != doc.MemberEnd() ? iter->value.GetBool() : false;

  iter = doc.FindMember("disable_auto_compactions");
  options->disable_auto_compactions =
      iter != doc.MemberEnd() ? iter->value.GetBool() : false;

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

  // 3. Set cache options with config file
  iter = doc.FindMember("CacheOptions");
  if (iter != doc.MemberEnd()) {
    auto cache_options = iter->value.GetObject();
    auto iter_cache_op = cache_options.FindMember("cache_size");
    auto cache_size = iter_cache_op != cache_options.MemberEnd()
                          ? iter_cache_op->value.GetInt()
                          : 8 << 20;
    iter_cache_op = cache_options.FindMember("cache_numshardbits");
    auto cache_numshardbits = iter_cache_op != cache_options.MemberEnd()
                                  ? iter_cache_op->value.GetInt()
                                  : -1;
    iter_cache_op = cache_options.FindMember("cache_high_pri_pool_ratio");
    auto cache_high_pri_pool_ratio = iter_cache_op != cache_options.MemberEnd()
                                  ? iter_cache_op->value.GetDouble()
                                  : 0.0; 
    std::shared_ptr<rocksdb::MemoryAllocator> allocator;
    rocksdb::LRUCacheOptions opts(static_cast<size_t>(cache_size), cache_numshardbits,
          false /*strict_capacity_limit*/, cache_high_pri_pool_ratio,
          allocator, rocksdb::kDefaultToAdaptiveMutex);
    cache_ = rocksdb::NewLRUCache(opts);
  }

  // 4. Set rocksdb block based table options
  iter = doc.FindMember("TableOptions");
  if (iter != doc.MemberEnd()) {
    rocksdb::BlockBasedTableOptions block_based_options;

    auto table_options = iter->value.GetObject();
    auto op_iter = table_options.FindMember("bloom_bits");
    block_based_options.filter_policy.reset(
        rocksdb::NewBloomFilterPolicy(op_iter->value.GetDouble()));

    block_based_options.block_cache = cache_;
    options->table_factory.reset(
        NewBlockBasedTableFactory(block_based_options));
  }

  // . Set rocksdb statistics
  iter = doc.FindMember("statistics");
  if (iter != doc.MemberEnd() && iter->value.GetBool()) {
    dbstats_ = rocksdb::CreateDBStatistics();
  }
  iter = doc.FindMember("stats_level");
  auto sl = iter != doc.MemberEnd() ? iter->value.GetInt() : 3;
  if (dbstats_) {
    dbstats_->set_stats_level(static_cast<rocksdb::StatsLevel>(sl));
  }
  options->statistics = dbstats_;
}

int RocksDB::Read(const std::string &table, const std::string &key,
                  const std::vector<std::string> *fields,
                  std::vector<KVPair> &result) {
  string value;
  rocksdb::Status s = db_->Get(*read_op_, key, &value);
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
  auto it = db_->NewIterator(*read_op_);
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
  // printf("put:key:%lu-%s\n",key.size(),key.data());
  // for( auto kv : values) {
  //     printf("put field:key:%lu-%s
  //     value:%lu-%s\n",kv.first.size(),kv.first.data(),kv.second.size(),kv.second.data());
  // }

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
    cout << stats << endl;
  }
  if (dbstats_.get() != nullptr) {
    fprintf(stdout, "STATISTICS:\n%s\n", dbstats_->ToString().c_str());
  }
}

RocksDB::~RocksDB() {
  printf("wait delete db...\n");
  delete db_;
  delete write_op_;
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
