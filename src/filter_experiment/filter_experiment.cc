// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <endian.h>
#include <time.h>
#include <cinttypes>
#include <cstdio>
#include <thread>
#include <atomic>

#include <iostream>
#include <fstream>
#include <random>
#include <string>
#include <vector>

#include "pebblesdb/db.h"
#include "pebblesdb/filter_policy.h"
#include "pebblesdb/slice.h"
#include "pebblesdb/options.h"

static std::atomic<uint64_t> read_count;

void close(leveldb::DB* db, leveldb::Options* level_options) {
    delete db;
    delete level_options->filter_policy;
}

void benchPointQuery(leveldb::DB* db, uint64_t key_count, uint64_t key_gap, uint64_t query_count) {
    std::mt19937_64 e(1);
    std::uniform_int_distribution<unsigned long long> dist(0, (key_count * key_gap));

    std::vector<uint64_t> query_keys;

    for (uint64_t i = 0; i < query_count; i++) {
	uint64_t r = dist(e);
	query_keys.push_back(r);
    }

    struct timespec ts_start;
    struct timespec ts_end;
    uint64_t elapsed;

    printf("point query\n");
    clock_gettime(CLOCK_MONOTONIC, &ts_start);

    for (uint64_t i = 0; i < query_count; i++) {
        uint64_t key = query_keys[i];
        key = htobe64(key);

        leveldb::Slice s_key(reinterpret_cast<const char*>(&key), sizeof(key));
        std::string s_value;
        uint64_t value;

        leveldb::Status status = db->Get(leveldb::ReadOptions(), s_key, &s_value);
        
        if (status.ok()) {
            assert(s_value.size() >= sizeof(uint64_t));
            value = *reinterpret_cast<const uint64_t*>(s_value.data());
            (void)value;
        }
    }

    clock_gettime(CLOCK_MONOTONIC, &ts_end);
    elapsed = static_cast<uint64_t>(ts_end.tv_sec) * 1000000000UL +
	static_cast<uint64_t>(ts_end.tv_nsec) -
	static_cast<uint64_t>(ts_start.tv_sec) * 1000000000UL +
	static_cast<uint64_t>(ts_start.tv_nsec);

    std::cout << "elapsed:    " << (static_cast<double>(elapsed) / 1000000000.) << "\n";
    std::cout << "throughput: " << (static_cast<double>(query_count) / (static_cast<double>(elapsed) / 1000000000.)) << "\n";
}

void warmup(leveldb::DB* db, uint64_t key_count, uint64_t key_gap, uint64_t query_count) {
    struct timespec ts_start;
    struct timespec ts_end;
    uint64_t elapsed;

    std::cout << "warming up\n";
    clock_gettime(CLOCK_MONOTONIC, &ts_start);

    for (uint64_t i = 0; i < query_count; i++) {
	uint64_t key = key_count * key_gap / query_count * i + 1;
	key = htobe64(key);

	leveldb::Slice s_key(reinterpret_cast<const char*>(&key), sizeof(key));
	std::string s_value;
	uint64_t value;

	leveldb::Status status = db->Get(leveldb::ReadOptions(), s_key, &s_value);

	if (status.ok()) {
	    assert(s_value.size() >= sizeof(uint64_t));
	    value = *reinterpret_cast<const uint64_t*>(s_value.data());
	    (void)value;
	}
    }
    
    clock_gettime(CLOCK_MONOTONIC, &ts_end);
    elapsed = static_cast<uint64_t>(ts_end.tv_sec) * 1000000000UL +
	static_cast<uint64_t>(ts_end.tv_nsec) -
	static_cast<uint64_t>(ts_start.tv_sec) * 1000000000UL +
	static_cast<uint64_t>(ts_start.tv_nsec);

    std::cout << "elapsed:    " << (static_cast<double>(elapsed) / 1000000000.) << "\n";
    std::cout << "throughput: " << (static_cast<double>(query_count) / (static_cast<double>(elapsed) / 1000000000.)) << "\n";
}

void init(const std::string& db_path, leveldb::DB** db, leveldb::Options* options,
	  uint64_t key_count, uint64_t value_size) {
    const std::string kKeyPath = "/home/bx1/trash/test/pebblesdb/src/filter-experiment/poisson_timestamps.csv";
    char value_buf[value_size];
    memset(value_buf, 0, value_size);

    options->compression = leveldb::CompressionType::kSnappyCompression;

    leveldb::Status status = leveldb::DB::Open(*options, db_path, db);
    if (!status.ok()) {
	std::cout << "creating new DB\n";
	options->create_if_missing = true;
	status = leveldb::DB::Open(*options, db_path, db);
    assert(status.ok());

	std::cout << "loading timestamp keys\n";
	std::ifstream keyFile(kKeyPath);
	std::vector<uint64_t> keys;

	uint64_t key = 0;
	for (uint64_t i = 0; i < key_count; i++) {
	    keyFile >> key;
	    keys.push_back(key);
	}

	std::cout << "inserting keys\n";
	for (uint64_t i = 0; i < key_count; i++) {
	    key = keys[i];
	    key = htobe64(key);
	    leveldb::Slice s_key(reinterpret_cast<const char*>(&key), sizeof(key));
	    leveldb::Slice s_value(value_buf, value_size);

	    status = (*db)->Put(leveldb::WriteOptions(), s_key, s_value);
	    if (!status.ok()) {
		    std::cout << status.ToString().c_str() << "\n";
		    assert(false);
	    }

	    if (i % (key_count / 10000) == 0)
		std::cout << i << "/" << key_count << " [" << ((i + 0.0)/(key_count + 0.0) * 100.) << "]\n";
	}

	//std::cout << "compacting\n";
	//leveldb::CompactRangeOptions compact_range_options;
	//(*db)->CompactRange(compact_range_options, NULL, NULL);
    }
}

void printIO() {
    FILE* fp = fopen("/sys/block/sda/sda1/stat", "r");
    if (fp == NULL) {
	    printf("Error: empty fp\n");
	return;
    }
    char buf[4096];
    if (fgets(buf, sizeof(buf), fp) != NULL)
	    printf("%s", buf);
    fclose(fp);
    printf("\n");
}

int main(int argc, const char* argv[]) {
    if (argc < 3) {
	std::cout << "Usage:\n";
	std::cout << "arg 1: path to datafiles\n";
	std::cout << "arg 2: filter type\n";
	std::cout << "\t0: Bloom filter\n";
	//std::cout << "\t2: SuRF\n";
	return -1;
    }

    std::string db_path = std::string(argv[1]); 
    int filter_type = atoi(argv[2]);
    //uint64_t scan_length = 1;
    //uint64_t range_size = 100000;

    const std::string kKeyPath = "poisson_timestamps.csv";
    const uint64_t kValueSize = 1000;
    const uint64_t kKeyRange = 10000000000000;
    
    // 2GB config
    const uint64_t kKeyCount = 2000000;
    const uint64_t kWarmupSampleGap = 100;

    //warmup
    const uint64_t kKeyGap = 100000;

    const uint64_t kWarmupQueryCount = 100000;
    const uint64_t kQueryCount = 50000;


    //=========================================================================
    
    leveldb::DB* db;
    leveldb::Options options;
    

    if (filter_type == 0)
	options.filter_policy = leveldb::NewBloomFilterPolicy(10);

    if (options.filter_policy == nullptr)
	std::cout << "Filter DISABLED\n";
    else
	std::cout << "Using " << options.filter_policy->Name() << "\n";
    
    init(db_path, &db, &options, kKeyCount, kValueSize);


    //=========================================================================
    
    uint64_t current_read_count = read_count;

    warmup(db, kKeyCount, kKeyGap, kWarmupQueryCount);
    std::cout << "read_count = " << (static_cast<double>(read_count - current_read_count) / kWarmupQueryCount) << " per op\n\n";

    printIO();
    benchPointQuery(db, kKeyCount, kKeyGap, kQueryCount);
    //benchOpenRangeQuery(db, kKeyCount, kKeyGap, kQueryCount, scan_length);
    //benchClosedRangeQuery(db, kKeyCount, kKeyGap, kQueryCount, range_size);
    printIO();

    std::cout << "read_count = " << (static_cast<double>(read_count - current_read_count) / kWarmupQueryCount) << " per op\n\n";

    //printFreeMem();

    close(db, &options);

    return 0;
}