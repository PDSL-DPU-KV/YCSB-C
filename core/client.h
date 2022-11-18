//
//  client.h
//  YCSB-C
//
//  Created by Jinglei Ren on 12/10/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_CLIENT_H_
#define YCSB_C_CLIENT_H_

#include <hdr/hdr_histogram.h>
#include <string>

#include "core_workload.h"
#include "db.h"
#include "timer.h"
#include "utils.h"

extern uint64_t ops_cnt[ycsbc::Operation::READMODIFYWRITE + 1];   //操作个数
extern uint64_t ops_time[ycsbc::Operation::READMODIFYWRITE + 1];  //微秒
extern struct hdr_histogram* histograms[ycsbc::Operation::READMODIFYWRITE + 1];
extern struct hdr_histogram* overall_histogram;

namespace ycsbc {

class Client {
 public:
  Client(DB &db, CoreWorkload &wl) : db_(db), workload_(wl) {}

  virtual bool DoInsert();
  virtual bool DoTransaction();

  virtual ~Client() {}

 protected:
  virtual int TransactionRead();
  virtual int TransactionReadModifyWrite();
  virtual int TransactionScan();
  virtual int TransactionUpdate();
  virtual int TransactionInsert();

  DB &db_;
  CoreWorkload &workload_;
};

inline bool Client::DoInsert() {
  std::string key = workload_.NextSequenceKey();
  std::vector<DB::KVPair> pairs;
  workload_.BuildValues(pairs);
  return (db_.Insert(workload_.NextTable(), key, pairs) == DB::kOK);
}

inline bool Client::DoTransaction() {
  int status = -1;
  uint64_t start_time = get_now_micros();
  uint64_t elapsed = 0;

  switch (workload_.NextOperation()) {
    case READ:
      status = TransactionRead();
      elapsed = get_now_micros() - start_time;
      if (elapsed > 3600000000) {
        printf("too large tx_xtime\n");
      } else {
        hdr_record_value_atomic(overall_histogram, elapsed);
        hdr_record_value_atomic(histograms[ycsbc::Operation::READ], elapsed);
      }
      ops_time[READ] += elapsed;
      ops_cnt[READ]++;
      break;
    case UPDATE:
      status = TransactionUpdate();
      elapsed = get_now_micros() - start_time;
      if (elapsed > 3600000000) {
        printf("too large tx_xtime\n");
      } else {
        hdr_record_value_atomic(overall_histogram, elapsed);
        hdr_record_value_atomic(histograms[ycsbc::Operation::UPDATE], elapsed);
      }
      ops_time[UPDATE] += elapsed;
      ops_cnt[UPDATE]++;
      break;
    case INSERT:
      status = TransactionInsert();
      elapsed = get_now_micros() - start_time;
      if (elapsed > 3600000000) {
        printf("too large tx_xtime\n");
      } else {
        hdr_record_value_atomic(overall_histogram, elapsed);
        hdr_record_value_atomic(histograms[ycsbc::Operation::INSERT], elapsed);
      }
      ops_time[INSERT] += elapsed;
      ops_cnt[INSERT]++;
      break;
    case SCAN:
      status = TransactionScan();
      elapsed = get_now_micros() - start_time;
      if (elapsed > 3600000000) {
        printf("too large tx_xtime\n");
      } else {
        hdr_record_value_atomic(overall_histogram, elapsed);
        hdr_record_value_atomic(histograms[ycsbc::Operation::SCAN], elapsed);
      }
      ops_time[SCAN] += elapsed;
      ops_cnt[SCAN]++;
      break;
    case READMODIFYWRITE:
      status = TransactionReadModifyWrite();
      elapsed = get_now_micros() - start_time;
      if (elapsed > 3600000000) {
        printf("too large tx_xtime\n");
      } else {
        hdr_record_value_atomic(overall_histogram, elapsed);
        hdr_record_value_atomic(histograms[ycsbc::Operation::READMODIFYWRITE], elapsed);
      }
      ops_time[READMODIFYWRITE] += elapsed;
      ops_cnt[READMODIFYWRITE]++;
      break;
    default:
      throw utils::Exception("Operation request is not recognized!");
  }
  assert(status >= 0);
  return (status == DB::kOK);
}

inline int Client::TransactionRead() {
  const std::string &table = workload_.NextTable();
  const std::string &key = workload_.NextTransactionKey();
  std::vector<DB::KVPair> result;
  if (!workload_.read_all_fields()) {
    std::vector<std::string> fields;
    fields.push_back("field" + workload_.NextFieldName());
    return db_.Read(table, key, &fields, result);
  } else {
    return db_.Read(table, key, NULL, result);
  }
}

inline int Client::TransactionReadModifyWrite() {
  const std::string &table = workload_.NextTable();
  const std::string &key = workload_.NextTransactionKey();
  std::vector<DB::KVPair> result;

  if (!workload_.read_all_fields()) {
    std::vector<std::string> fields;
    fields.push_back("field" + workload_.NextFieldName());
    db_.Read(table, key, &fields, result);
  } else {
    db_.Read(table, key, NULL, result);
  }

  std::vector<DB::KVPair> values;
  if (workload_.write_all_fields()) {
    workload_.BuildValues(values);
  } else {
    workload_.BuildUpdate(values);
  }
  return db_.Update(table, key, values);
}

inline int Client::TransactionScan() {
  const std::string &table = workload_.NextTable();
  const std::string &key = workload_.NextTransactionKey();
  int len = workload_.NextScanLength();
  std::vector<std::vector<DB::KVPair>> result;
  if (!workload_.read_all_fields()) {
    std::vector<std::string> fields;
    fields.push_back("field" + workload_.NextFieldName());
    return db_.Scan(table, key, len, &fields, result);
  } else {
    return db_.Scan(table, key, len, NULL, result);
  }
}

inline int Client::TransactionUpdate() {
  const std::string &table = workload_.NextTable();
  const std::string &key = workload_.NextTransactionKey();
  std::vector<DB::KVPair> values;
  if (workload_.write_all_fields()) {
    workload_.BuildValues(values);
  } else {
    workload_.BuildUpdate(values);
  }
  return db_.Update(table, key, values);
}

inline int Client::TransactionInsert() {
  const std::string &table = workload_.NextTable();
  const std::string &key = workload_.NextSequenceKey();
  std::vector<DB::KVPair> values;
  workload_.BuildValues(values);
  return db_.Insert(table, key, values);
}

}  // namespace ycsbc

#endif  // YCSB_C_CLIENT_H_
