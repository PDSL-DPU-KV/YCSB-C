#ifndef YCSB_C_ROCKS_DB_H_
#define YCSB_C_ROCKS_DB_H_

#include <rapidjson/document.h>
#include <rocksdb/cache.h>
#include <rocksdb/db.h>
#include <rocksdb/dpu_deflate_tool.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/options.h>
#include <rocksdb/pluggable_compaction.h>
#include <rocksdb/table.h>

#include <iostream>
#include <mutex>
#include <sstream>
#include <string>

#include "core_workload.h"
#include "db.h"
#include "properties.h"

using std::cout;
using std::endl;

namespace ycsbc {
class RocksDB : public DB {
 public:
  RocksDB(const char *dbfilename, utils::Properties &props);

  int Read(const std::string &table, const std::string &key,
           const std::vector<std::string> *fields, std::vector<KVPair> &result);

  int Scan(const std::string &table, const std::string &key, int len,
           const std::vector<std::string> *fields,
           std::vector<std::vector<KVPair>> &result);

  int Update(const std::string &table, const std::string &key,
             std::vector<KVPair> &values);

  int Insert(const std::string &table, const std::string &key,
             std::vector<KVPair> &values);

  int Delete(const std::string &table, const std::string &key);

  void PrintStats();

  ~RocksDB();

 private:
  rocksdb::DB *db_;
  unsigned noResult;
  // std::shared_ptr<rocksdb::Cache> cache_;
  std::shared_ptr<rocksdb::Statistics> dbstats_;
  rocksdb::WriteOptions *write_op_;

  void SetOptions(rocksdb::Options *options, utils::Properties &props,
                  rapidjson::Document &doc);
  void SetupPluggableCompaction(const std::string &server_ip,
                                const int server_port, const bool use_rdma);
  void CleanupPluggableCompaction();
  void SerializeValues(std::vector<KVPair> &kvs, std::string &value);
  void DeSerializeValues(std::string &value, std::vector<KVPair> &kvs);
};

}  // namespace ycsbc

#endif  // YCSB_C_ROCKS_DB_H_
