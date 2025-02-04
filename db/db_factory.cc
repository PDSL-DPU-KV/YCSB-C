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

DB* DBFactory::CreateDB(utils::Properties& props) {
  if (props["dbname"] == "basic") {
    return new BasicDB;
  } else if (props["dbname"] == "rocksdb") {
    std::string dbpath = props.GetProperty("dbpath", "/tmp/ycsbc-rocksdb-test");
    return new RocksDB(dbpath.c_str(), props);
  } else
    return NULL;
}
