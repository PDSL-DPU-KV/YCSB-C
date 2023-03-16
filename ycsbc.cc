//
//  ycsbc.cc
//  YCSB-C
//
//  Created by Jinglei Ren on 12/19/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#include <hdr/hdr_histogram.h>
#include <unistd.h>

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <future>
#include <iostream>
#include <string>
#include <vector>

#include "client.h"
#include "core_workload.h"
#include "db_factory.h"
#include "rocksdb/perf_level.h"
#include "rocksdb/perf_context.h"
#include "timer.h"
#include "utils.h"

using namespace std;

////statistics
uint64_t ops_cnt[ycsbc::Operation::READMODIFYWRITE + 1] = {0};   //操作个数
uint64_t ops_time[ycsbc::Operation::READMODIFYWRITE + 1] = {0};  //微秒
struct hdr_histogram* histograms[ycsbc::Operation::READMODIFYWRITE + 1];
struct hdr_histogram* overall_histogram;
////

void UsageMessage(const char *command);
bool StrStartWith(const char *str, const char *pre);
string ParseCommandLine(int argc, const char *argv[], utils::Properties &props);
void Init(utils::Properties &props);
void PrintInfo(utils::Properties &props);

int DelegateClient(ycsbc::DB *db, ycsbc::CoreWorkload *wl, const int num_ops,
                   bool is_loading, int perf_level) {
  db->Init();
  ycsbc::Client client(*db, *wl);
  int oks = 0;
  int next_report_ = 0;

  SetPerfLevel(static_cast<rocksdb::PerfLevel>(perf_level));
  rocksdb::get_perf_context()->EnablePerLevelPerfContext();

  for (int i = 0; i < num_ops; ++i) {
    if (i >= next_report_) {
      if (next_report_ < 1000)
        next_report_ += 100;
      else if (next_report_ < 5000)
        next_report_ += 500;
      else if (next_report_ < 10000)
        next_report_ += 1000;
      else if (next_report_ < 50000)
        next_report_ += 5000;
      else if (next_report_ < 100000)
        next_report_ += 10000;
      else if (next_report_ < 500000)
        next_report_ += 50000;
      else
        next_report_ += 100000;
      fprintf(stderr, "... finished %d ops%30s\r", i, "");
      fflush(stderr);
    }
    if (is_loading) {
      oks += client.DoInsert();
    } else {
      oks += client.DoTransaction();
    }
  }

  if (perf_level > rocksdb::PerfLevel::kDisable) {
    fprintf(stderr, "perf context: %s\n", rocksdb::get_perf_context()->ToString().c_str());
  }
  db->Close();
  return oks;
}

int main(const int argc, const char *argv[]) {
  utils::Properties props;
  Init(props);
  string file_name = ParseCommandLine(argc, argv, props);

  ycsbc::DB *db = ycsbc::DBFactory::CreateDB(props);
  if (!db) {
    cout << "Unknown database name " << props["dbname"] << endl;
    exit(0);
  }

  PrintInfo(props);

  ycsbc::CoreWorkload wl;
  wl.Init(props);

  const int num_threads = stoi(props.GetProperty("threadcount", "1"));
  const bool load = utils::StrToBool(props.GetProperty("load", "false"));
  const bool run = utils::StrToBool(props.GetProperty("run", "false"));
  const bool print_stats = utils::StrToBool(props["dbstatistics"]);
  const int perf_level = stoi(props["perflevel"]);
  const bool wait_for_balance = utils::StrToBool(props["dbwaitforbalance"]);

  // Initialise the histogram
  int res = hdr_init(1, INT64_C(3600000000), 3, &overall_histogram);
  if ((res != 0) || (overall_histogram == NULL)) {
    printf("Failed to init overall histogram!\n");
    exit(0);
  }
  for (int i = 0; i < ycsbc::Operation::READMODIFYWRITE + 1; ++i) {
    res = hdr_init(1, INT64_C(3600000000), 3, &histograms[i]);
    if ((res != 0) || (histograms[i] == NULL)) {
      printf("Failed to init histogram %d!\n", i);
      exit(0);
    }
  }

  vector<future<int>> actual_ops;
  int total_ops = 0;
  int sum = 0;
  utils::Timer<double> timer;

  if (load) {
    uint64_t load_start = get_now_micros();
    total_ops = stoi(props[ycsbc::CoreWorkload::RECORD_COUNT_PROPERTY]);
    for (int i = 0; i < num_threads; ++i) {
      actual_ops.emplace_back(async(launch::async, DelegateClient, db, &wl,
                                    total_ops / num_threads, true, perf_level));
    }
    assert((int)actual_ops.size() == num_threads);

    sum = 0;
    for (auto &n : actual_ops) {
      assert(n.valid());
      sum += n.get();
    }
    uint64_t load_end = get_now_micros();
    uint64_t use_time = load_end - load_start;
    printf("********** load result **********\n");
    printf("loading records:%d  use time:%.3f s  IOPS:%.2f iops\n", sum,
           1.0 * use_time * 1e-6, 1.0 * sum * 1e6 / use_time);
    printf("*********************************\n");
  }

  if (run) {
    // Peforms transactions
    actual_ops.clear();
    total_ops = stoi(props[ycsbc::CoreWorkload::OPERATION_COUNT_PROPERTY]);
    uint64_t run_start = get_now_micros();
    for (int i = 0; i < num_threads; ++i) {
      actual_ops.emplace_back(async(launch::async, DelegateClient, db, &wl,
                                    total_ops / num_threads, false, perf_level));
    }
    assert((int)actual_ops.size() == num_threads);

    sum = 0;
    for (auto &n : actual_ops) {
      assert(n.valid());
      sum += n.get();
    }
    uint64_t run_end = get_now_micros();
    uint64_t use_time = run_end - run_start;

    printf("********** run result **********\n");
    printf("all opeartion records:%d  use time:%.3f s  IOPS:%.2f iops\n\n", sum,
           1.0 * use_time * 1e-6, 1.0 * sum * 1e6 / use_time);
    printf("average: %lf, 95th: %ld, 99th: %ld, 99.9th: %ld, 99.99th: %ld\n",
            hdr_mean(overall_histogram), 
            hdr_value_at_percentile(overall_histogram, 95),
            hdr_value_at_percentile(overall_histogram, 99),
            hdr_value_at_percentile(overall_histogram, 99.9),
            hdr_value_at_percentile(overall_histogram, 99.99));
    if (ops_cnt[ycsbc::INSERT]) {
      printf("insert ops:%7lu  use time:%7.3f s  IOPS:%7.2f iops\n",
             ops_cnt[ycsbc::INSERT], 1.0 * ops_time[ycsbc::INSERT] * 1e-6,
             1.0 * ops_cnt[ycsbc::INSERT] * 1e6 / ops_time[ycsbc::INSERT]);
      printf("average: %lf, 95th: %ld, 99th: %ld, 99.9th: %ld, 99.99th: %ld\n",
            hdr_mean(histograms[ycsbc::Operation::INSERT]), 
            hdr_value_at_percentile(histograms[ycsbc::Operation::INSERT], 95),
            hdr_value_at_percentile(histograms[ycsbc::Operation::INSERT], 99),
            hdr_value_at_percentile(histograms[ycsbc::Operation::INSERT], 99.9),
            hdr_value_at_percentile(histograms[ycsbc::Operation::INSERT], 99.99));
    }
    if (ops_cnt[ycsbc::READ]) {
      printf("read ops  :%7lu  use time:%7.3f s  IOPS:%7.2f iops\n",
             ops_cnt[ycsbc::READ], 1.0 * ops_time[ycsbc::READ] * 1e-6,
             1.0 * ops_cnt[ycsbc::READ] * 1e6 / ops_time[ycsbc::READ]);
      printf("average: %lf, 95th: %ld, 99th: %ld, 99.9th: %ld, 99.99th: %ld\n",
            hdr_mean(histograms[ycsbc::Operation::READ]), 
            hdr_value_at_percentile(histograms[ycsbc::Operation::READ], 95),
            hdr_value_at_percentile(histograms[ycsbc::Operation::READ], 99),
            hdr_value_at_percentile(histograms[ycsbc::Operation::READ], 99.9),
            hdr_value_at_percentile(histograms[ycsbc::Operation::READ], 99.99));
    }
    if (ops_cnt[ycsbc::UPDATE]) {
      printf("update ops:%7lu  use time:%7.3f s  IOPS:%7.2f iops\n",
             ops_cnt[ycsbc::UPDATE], 1.0 * ops_time[ycsbc::UPDATE] * 1e-6,
             1.0 * ops_cnt[ycsbc::UPDATE] * 1e6 / ops_time[ycsbc::UPDATE]);
      printf("average: %lf, 95th: %ld, 99th: %ld, 99.9th: %ld, 99.99th: %ld\n",
            hdr_mean(histograms[ycsbc::Operation::UPDATE]), 
            hdr_value_at_percentile(histograms[ycsbc::Operation::UPDATE], 95),
            hdr_value_at_percentile(histograms[ycsbc::Operation::UPDATE], 99),
            hdr_value_at_percentile(histograms[ycsbc::Operation::UPDATE], 99.9),
            hdr_value_at_percentile(histograms[ycsbc::Operation::UPDATE], 99.99));
    }
    if (ops_cnt[ycsbc::SCAN]) {
      printf("scan ops  :%7lu  use time:%7.3f s  IOPS:%7.2f iops\n",
             ops_cnt[ycsbc::SCAN], 1.0 * ops_time[ycsbc::SCAN] * 1e-6,
             1.0 * ops_cnt[ycsbc::SCAN] * 1e6 / ops_time[ycsbc::SCAN]);
      printf("average: %lf, 95th: %ld, 99th: %ld, 99.9th: %ld, 99.99th: %ld\n",
            hdr_mean(histograms[ycsbc::Operation::SCAN]), 
            hdr_value_at_percentile(histograms[ycsbc::Operation::SCAN], 95),
            hdr_value_at_percentile(histograms[ycsbc::Operation::SCAN], 99),
            hdr_value_at_percentile(histograms[ycsbc::Operation::SCAN], 99.9),
            hdr_value_at_percentile(histograms[ycsbc::Operation::SCAN], 99.99));
    }
    if (ops_cnt[ycsbc::READMODIFYWRITE]) {
      printf("rmw ops   :%7lu  use time:%7.3f s  IOPS:%7.2f iops\n",
             ops_cnt[ycsbc::READMODIFYWRITE],
             1.0 * ops_time[ycsbc::READMODIFYWRITE] * 1e-6,
             1.0 * ops_cnt[ycsbc::READMODIFYWRITE] * 1e6 /
                 ops_time[ycsbc::READMODIFYWRITE]);
      printf("average: %lf, 95th: %ld, 99th: %ld, 99.9th: %ld, 99.99th: %ld\n",
            hdr_mean(histograms[ycsbc::Operation::READMODIFYWRITE]), 
            hdr_value_at_percentile(histograms[ycsbc::Operation::READMODIFYWRITE], 95),
            hdr_value_at_percentile(histograms[ycsbc::Operation::READMODIFYWRITE], 99),
            hdr_value_at_percentile(histograms[ycsbc::Operation::READMODIFYWRITE], 99.9),
            hdr_value_at_percentile(histograms[ycsbc::Operation::READMODIFYWRITE], 99.99));
    }
    printf("********************************\n");
  }
  if (print_stats) {
    printf("-------------- db statistics --------------\n");
    db->PrintStats();
    printf("-------------------------------------------\n");
  }
  if (wait_for_balance) {
    uint64_t sleep_time = 0;
    while (!db->HaveBalancedDistribution()) {
      sleep(10);
      sleep_time += 10;
    }
    printf("Wait balance:%lu s\n", sleep_time);

    printf("-------------- db statistics --------------\n");
    db->PrintStats();
    printf("-------------------------------------------\n");
  }
  delete db;
  return 0;
}

string ParseCommandLine(int argc, const char *argv[],
                        utils::Properties &props) {
  int argindex = 1;
  string filename;
  while (argindex < argc && StrStartWith(argv[argindex], "-")) {
    if (strcmp(argv[argindex], "-threads") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("threadcount", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-db") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("dbname", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-P") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      filename.assign(argv[argindex]);
      ifstream input(argv[argindex]);
      try {
        props.Load(input);
      } catch (const string &message) {
        cout << message << endl;
        exit(0);
      }
      input.close();
      argindex++;
    } else if (strcmp(argv[argindex], "-dbpath") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("dbpath", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-load") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("load", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-run") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("run", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-dboptions") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("dboptions", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-dbstatistics") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("dbstatistics", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-perflevel") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("perflevel", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-dbwaitforbalance") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("dbwaitforbalance", argv[argindex]);
      argindex++;
    } else {
      cout << "Unknown option '" << argv[argindex] << "'" << endl;
      exit(0);
    }
  }

  if (argindex == 1 || argindex != argc) {
    UsageMessage(argv[0]);
    exit(0);
  }

  return filename;
}

void UsageMessage(const char *command) {
  cout << "Usage: " << command << " [options]" << endl;
  cout << "Options:" << endl;
  cout << "  -threads n: execute using n threads (default: 1)" << endl;
  cout << "  -db dbname: specify the name of the DB to use (default: basic)"
       << endl;
  cout << "  -P propertyfile: load properties from the given file. Multiple "
          "files can"
       << endl;
  cout << "                   be specified, and will be processed in the order "
          "specified"
       << endl;
}

inline bool StrStartWith(const char *str, const char *pre) {
  return strncmp(str, pre, strlen(pre)) == 0;
}

void Init(utils::Properties &props) {
  props.SetProperty("dbname", "basic");
  props.SetProperty("dbpath", "");
  props.SetProperty("load", "false");
  props.SetProperty("run", "false");
  props.SetProperty("threadcount", "1");
  props.SetProperty("dboption", "0");
  props.SetProperty("dbstatistics", "false");
  props.SetProperty("perflevel", "3");
  props.SetProperty("dbwaitforbalance", "false");
}

void PrintInfo(utils::Properties &props) {
  printf("---- dbname:%s  dbpath:%s ----\n", props["dbname"].c_str(),
         props["dbpath"].c_str());
  printf("%s", props.DebugString().c_str());
  printf("----------------------------------------\n");
  fflush(stdout);
}
