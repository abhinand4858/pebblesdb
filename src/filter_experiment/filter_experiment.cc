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
#include "pebblesdb/table.h"


void close(leveldb::DB* db, leveldb::Options* level_options) {
    delete db;
    delete level_options->filter_policy;
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

void benchPointQuery(leveldb::DB* db, leveldb::Options* options,
		     uint64_t key_range, uint64_t query_count) {
    //std::random_device rd;
    //std::mt19937_64 e(rd());
    std::mt19937_64 e(2017);
    std::uniform_int_distribution<unsigned long long> dist(0, key_range);

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

// void benchOpenRangeQuery(leveldb::DB* db, leveldb::Options* options, uint64_t key_range,
// 			 uint64_t query_count, uint64_t scan_length) {
//     //std::random_device rd;
//     //std::mt19937_64 e(rd());
//     std::mt19937_64 e(2017);
//     std::uniform_int_distribution<unsigned long long> dist(0, key_range);

//     std::vector<uint64_t> query_keys;

//     for (uint64_t i = 0; i < query_count; i++) {
//         uint64_t r = dist(e);
//         query_keys.push_back(r);
//     }

//     struct timespec ts_start;
//     struct timespec ts_end;
//     uint64_t elapsed;

//     printf("open range query\n");
//     leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());

//     clock_gettime(CLOCK_MONOTONIC, &ts_start);

//     for (uint64_t i = 0; i < query_count; i++) {
//         uint64_t key = query_keys[i];
//         key = htobe64(key);
//         leveldb::Slice s_key(reinterpret_cast<const char*>(&key), sizeof(key));
        
//         std::string s_value;
//         uint64_t value;

//         uint64_t j = 0;
//         for (it->Seek(s_key); it->Valid() && j < scan_length; it->Next(), j++) {
//             uint64_t found_key = *reinterpret_cast<const uint64_t*>(it->key().data());
//             assert(it->value().size() >= sizeof(uint64_t));
//             value = *reinterpret_cast<const uint64_t*>(it->value().data());
//             (void)value;
//             break;
//         }
//     }
    
//     clock_gettime(CLOCK_MONOTONIC, &ts_end);
//     elapsed = static_cast<uint64_t>(ts_end.tv_sec) * 1000000000UL +
// 	static_cast<uint64_t>(ts_end.tv_nsec) -
// 	static_cast<uint64_t>(ts_start.tv_sec) * 1000000000UL +
// 	static_cast<uint64_t>(ts_start.tv_nsec);

//     std::cout << "elapsed:    " << (static_cast<double>(elapsed) / 1000000000.) << "\n";
//     std::cout << "throughput: " << (static_cast<double>(query_count) / (static_cast<double>(elapsed) / 1000000000.)) << "\n";


//     delete it;
// }

// void benchClosedRangeQuery(leveldb::DB* db, leveldb::Options* options, uint64_t key_range,
// 			   uint64_t query_count, uint64_t range_size) {
//     //std::random_device rd;
//     //std::mt19937_64 e(rd());
//     std::mt19937_64 e(2017);
//     std::uniform_int_distribution<unsigned long long> dist(0, key_range);

//     std::vector<uint64_t> query_keys;

//     for (uint64_t i = 0; i < query_count; i++) {
// 	uint64_t r = dist(e);
// 	query_keys.push_back(r);
//     }

//     struct timespec ts_start;
//     struct timespec ts_end;
//     uint64_t elapsed;

//     printf("closed range query\n");

//     clock_gettime(CLOCK_MONOTONIC, &ts_start);

//     for (uint64_t i = 0; i < query_count; i++) {
// 	uint64_t key = query_keys[i];
// 	uint64_t upper_key = key + range_size;
// 	key = htobe64(key);
// 	leveldb::Slice s_key(reinterpret_cast<const char*>(&key), sizeof(key));
// 	upper_key = htobe64(upper_key);
// 	leveldb::Slice s_upper_key(reinterpret_cast<const char*>(&upper_key), sizeof(upper_key));
	
// 	std::string s_value;
// 	uint64_t value;

// 	leveldb::ReadOptions read_options = leveldb::ReadOptions();
// 	//read_options.iterate_upper_bound = &s_upper_key;  NEED TO UPDATE THE UPPER BOUND HERE!!!!!
// 	leveldb::Iterator* it = db->NewIterator(read_options);

// 	uint64_t j = 0;
// 	for (it->Seek(s_key); it->Valid(); it->Next(), j++) {
// 	    uint64_t found_key = *reinterpret_cast<const uint64_t*>(it->key().data());
// 	    assert(it->value().size() >= sizeof(uint64_t));
// 	    value = *reinterpret_cast<const uint64_t*>(it->value().data());
// 	    (void)value;
// 	    break;
// 	}
	
// 	delete it;
//     }
    
//     clock_gettime(CLOCK_MONOTONIC, &ts_end);
//     elapsed = static_cast<uint64_t>(ts_end.tv_sec) * 1000000000UL +
// 	static_cast<uint64_t>(ts_end.tv_nsec) -
// 	static_cast<uint64_t>(ts_start.tv_sec) * 1000000000UL +
// 	static_cast<uint64_t>(ts_start.tv_nsec);

//     //std::cout << "elapsed:    " << (static_cast<double>(elapsed) / 1000000000.) << "\n";
//     std::cout << "throughput: " << (static_cast<double>(query_count) / (static_cast<double>(elapsed) / 1000000000.)) << "\n";

// }

void init(const std::string& db_path, leveldb::DB** db, leveldb::Options* options,
	  uint64_t key_count, uint64_t value_size) {
    const std::string kKeyPath = "/home/bx1/trash/test/pebblesdb/src/filter_experiment/poisson_timestamps.csv";
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

void testScan(leveldb::DB* db, uint64_t key_count) {
    const std::string kKeyPath = "/home/bx1/trash/test/pebblesdb/src/filter_experiment/poisson_timestamps.csv";

    std::cout << "testScan: loading timestamp keys\n";
    std::ifstream keyFile(kKeyPath);
    std::vector<uint64_t> keys;

    uint64_t key = 0;
    for (uint64_t i = 0; i < key_count; i++) {
        keyFile >> key;
        keys.push_back(key);
    }

    struct timespec ts_start;
    struct timespec ts_end;
    uint64_t elapsed;

    clock_gettime(CLOCK_MONOTONIC, &ts_start);

    for (uint64_t i = 0; i < key_count; i++) {
        key = htobe64(keys[i]);

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
    std::cout << "throughput: " << (static_cast<double>(key_count) / (static_cast<double>(elapsed) / 1000000000.)) << "\n";
}

void printIO() {
    FILE* fp = fopen("/sys/block/sda/sda1/stat", "r");
    if (fp == NULL) {
	    printf("Error: empty fp\n");
        printf("%s\n", strerror(errno));
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
        std::cout << "\t1: SuRF\n";
        return -1;
    }

    std::string db_path = std::string(argv[1]); 
    int filter_type = atoi(argv[2]);
    //uint64_t scan_length = 1;
    //uint64_t range_size = 100000;

    const std::string kKeyPath = "poisson_timestamps.csv";
  
    // kWarmupSampleGap = kKeyCount / warmup_query_count;
    //const uint64_t kQueryCount = 50000;

    //const uint64_t kKeyCount = 100000000;
    const uint64_t kKeyCount = 2000000; //2x10^6
    const uint64_t kValueSize = 1000;
    const uint64_t kKeyGap = 100000;

     const uint64_t kWarmupQueryCount = 100000;
    const uint64_t kQueryCount = 50000;

    //=========================================================================
    
    leveldb::DB* db;
    leveldb::Options options;
    
    if (filter_type == 0)
	    options.filter_policy = leveldb::NewBloomFilterPolicy(10);
    else if (filter_type == 1)
	    options.filter_policy = leveldb::NewSuRFPolicy(0, 0, true, 16);

    if (options.filter_policy == nullptr)
	    std::cout << "Filter DISABLED\n";
    else
	    std::cout << "Using " << options.filter_policy->Name() << "\n";
    
    init(db_path, &db, &options, kKeyCount, kValueSize);

    //=========================================================================
    
    //uint64_t current_read_count = read_count;

    //std::cout << options.statistics->ToString() << "\n";

    printIO();
    warmup(db, kKeyCount, kKeyGap, kWarmupQueryCount);
    //std::cout << "read_count = " << (static_cast<double>(read_count - current_read_count) / kWarmupQueryCount) << " per op\n\n";

    printIO();
    benchPointQuery(db, kKeyCount, kKeyGap, kQueryCount);
    //benchOpenRangeQuery(db, kKeyCount, kKeyGap, kQueryCount, scan_length);
    //benchClosedRangeQuery(db, kKeyCount, kKeyGap, kQueryCount, range_size);
    printIO();

    //std::cout << "read_count = " << (static_cast<double>(read_count - current_read_count) / kWarmupQueryCount) << " per op\n\n";

    //printFreeMem();

    close(db, &options);

    return 0;
}