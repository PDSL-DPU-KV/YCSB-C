//
//  basic_db.cc
//  YCSB-C
//
//  Created by Jinglei Ren on 12/17/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#include "db_factory.h"

#include <string>

#include "basic_db.h"
#include "rocksdb.h"

using namespace std;
using ycsbc::DB;
using ycsbc::DBFactory;

std::string GetPathForMultiple(std::string base_name, size_t id) {
  if (!base_name.empty()) {
#ifndef OS_WIN
    if (base_name.back() != '/') {
      base_name += '/';
    }
#else
    if (base_name.back() != '\\') { 
      base_name += '\\';
    }
#endif
  }
  return base_name + std::to_string(id);
}

DB* DBFactory::CreateDB(utils::Properties& props, int id) {
  if (props["dbname"] == "basic") {
    return new BasicDB;
  } else if (props["dbname"] == "rocksdb") {
    rocksdb::init_mylog_file();
    std::string dbpath = props.GetProperty("dbpath", "/tmp/ycsbc-rocksdb-test");
    return new RocksDB(GetPathForMultiple(dbpath, id).c_str(), props);
  } else
    return NULL;
}
