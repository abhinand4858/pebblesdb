// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <endian.h>
#include <time.h>
#include <cinttypes>
#include <climits>
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

#include "pebblesdb/cache.h"

// assume compression ratio = 0.5
void setValueBuffer(char* value_buf, int size,
		    std::mt19937_64 &e,
		    std::uniform_int_distribution<unsigned long long>& dist) {
    memset(value_buf, 0, size);
    int pos = size / 2;
    while (pos < size) {
        uint64_t num = dist(e);
        char* num_bytes = reinterpret_cast<char*>(&num);
        memcpy(value_buf + pos, num_bytes, 8);
        pos += 8;
    }
}

void init(const std::string& key_path, const std::string& db_path, leveldb::DB** db, leveldb::Options* options,
	  uint64_t key_count, uint64_t value_size, int filter_type) {

    std::mt19937_64 e(2017);
    std::uniform_int_distribution<unsigned long long> dist(0, ULLONG_MAX);
    
    char value_buf[value_size];

    if (filter_type == 0)
	    options->filter_policy = leveldb::NewBloomFilterPolicy(14);
    else if (filter_type == 1)
	    options->filter_policy = leveldb::NewSuRFPolicy(0, 0, true, 16);
    else if (filter_type == 2)
	    options->filter_policy = leveldb::NewSuRFPolicy(1, 4, true, 16);
    else if (filter_type == 3)
	    options->filter_policy = leveldb::NewSuRFPolicy(2, 4, true, 16);

    if (options->filter_policy == nullptr)
	    std::cout << "Filter DISABLED\n";
    else
	    std::cout << "Using " << options->filter_policy->Name() << "\n";
    
    options->block_cache = leveldb::NewLRUCache(100 * 1048576); // 100MB cache

    options->max_open_files = -1; // pre-load indexes and filters

    // 2GB config
    //options->write_buffer_size = 2 * 1048576;
    //options->max_bytes_for_level_base = 10 * 1048576;
    //options->target_file_size_base = 2 * 1048576;

    // 100GB config
    //options->write_buffer_size = 64 * 1048576;
    //options->max_bytes_for_level_base = 256 * 1048576;
    //options->target_file_size_base = 64 * 1048576;

    leveldb::Status status = leveldb::DB::Open(*options, db_path, db);
    if (!status.ok()) {
        std::cout << "creating new DB\n";
        options->create_if_missing = true;
        status = leveldb::DB::Open(*options, db_path, db);
        assert(status.ok());

        std::cout << "loading timestamp keys\n";
        std::ifstream keyFile(key_path);
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
            setValueBuffer(value_buf, value_size, e, dist);
            leveldb::Slice s_value(value_buf, value_size);

            status = (*db)->Put(leveldb::WriteOptions(), s_key, s_value);
            if (!status.ok()) {
                std::cout << status.ToString().c_str() << "\n";
                assert(false);
            }

            if (i % (key_count / 100) == 0)
                std::cout << i << "/" << key_count << " [" << ((i + 0.0)/(key_count + 0.0) * 100.) << "]\n";
        }

        //std::cout << "compacting\n";
        //leveldb::CompactRangeOptions compact_range_options;
        //(*db)->CompactRange(compact_range_options, NULL, NULL);
    }
}


void close(leveldb::DB* db, leveldb::Options* level_options) {
    delete db;
    delete level_options->filter_policy;
}


void warmup(const std::string& key_path, leveldb::DB* db, uint64_t key_count, uint64_t key_gap) {
    
    std::ifstream keyFile(key_path);
    std::vector<uint64_t> keys;
    uint64_t key = 0;

    for (uint64_t i = 0; i < key_count; i++) {
        keyFile >> key;
        if (i % key_gap == 0)
            keys.push_back(key);
    }
    
    struct timespec ts_start;
    struct timespec ts_end;
    uint64_t elapsed;

    std::cout << "warming up\n";
    clock_gettime(CLOCK_MONOTONIC, &ts_start);

    for (uint64_t i = 0; i < keys.size(); i++) {
        key = keys[i];
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
    std::cout << "throughput: " << (static_cast<double>(keys.size()) / (static_cast<double>(elapsed) / 1000000000.)) << "\n";
}

void testScan(const std::string& key_path, leveldb::DB* db, uint64_t key_count) {

    std::cout << "testScan: loading timestamp keys\n";
    std::ifstream keyFile(key_path);
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

void benchPointQuery(leveldb::DB* db, leveldb::Options* options,
		     uint64_t key_range, uint64_t query_count) {
    std::mt19937_64 e(2019);
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

void benchOpenRangeQuery(leveldb::DB* db, leveldb::Options* options, uint64_t key_range,
			 uint64_t query_count, uint64_t scan_length) {
    //std::random_device rd;
    //std::mt19937_64 e(rd());
    std::mt19937_64 e(2019);
    std::uniform_int_distribution<unsigned long long> dist(0, key_range);

    std::vector<uint64_t> query_keys;

    for (uint64_t i = 0; i < query_count; i++) {
        uint64_t r = dist(e);
        query_keys.push_back(r);
    }

    struct timespec ts_start;
    struct timespec ts_end;
    uint64_t elapsed;

    printf("open range query\n");
    leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());

    clock_gettime(CLOCK_MONOTONIC, &ts_start);

    for (uint64_t i = 0; i < query_count; i++) {
        uint64_t key = query_keys[i];
        key = htobe64(key);
        leveldb::Slice s_key(reinterpret_cast<const char*>(&key), sizeof(key));
        
        std::string s_value;
        uint64_t value;

        uint64_t j = 0;
        for (it->Seek(s_key); it->Valid() && j < scan_length; it->Next(), j++) {
            uint64_t found_key = *reinterpret_cast<const uint64_t*>(it->key().data());
            assert(it->value().size() >= sizeof(uint64_t));
            value = *reinterpret_cast<const uint64_t*>(it->value().data());
            (void)value;
            break;
        }
    }
    
    clock_gettime(CLOCK_MONOTONIC, &ts_end);
    elapsed = static_cast<uint64_t>(ts_end.tv_sec) * 1000000000UL +
	static_cast<uint64_t>(ts_end.tv_nsec) -
	static_cast<uint64_t>(ts_start.tv_sec) * 1000000000UL +
	static_cast<uint64_t>(ts_start.tv_nsec);

    std::cout << "elapsed:    " << (static_cast<double>(elapsed) / 1000000000.) << "\n";
    std::cout << "throughput: " << (static_cast<double>(query_count) / (static_cast<double>(elapsed) / 1000000000.)) << "\n";


    delete it;
}

void benchClosedRangeQuery(leveldb::DB* db, leveldb::Options* options, uint64_t key_range,
			   uint64_t query_count, uint64_t range_size) {
    //std::random_device rd;
    //std::mt19937_64 e(rd());
    std::mt19937_64 e(2019);
    std::uniform_int_distribution<unsigned long long> dist(0, key_range);

    std::vector<uint64_t> query_keys;

    for (uint64_t i = 0; i < query_count; i++) {
        uint64_t r = dist(e);
        query_keys.push_back(r);
    }

    struct timespec ts_start;
    struct timespec ts_end;
    uint64_t elapsed;

    printf("closed range query\n");

    clock_gettime(CLOCK_MONOTONIC, &ts_start);

    for (uint64_t i = 0; i < query_count; i++) {
        uint64_t key = query_keys[i];
        uint64_t upper_key = key + range_size;
        key = htobe64(key);
        leveldb::Slice s_key(reinterpret_cast<const char*>(&key), sizeof(key));
        upper_key = htobe64(upper_key);
        leveldb::Slice s_upper_key(reinterpret_cast<const char*>(&upper_key), sizeof(upper_key));
        
        std::string s_value;
        uint64_t value;

        leveldb::ReadOptions read_options = leveldb::ReadOptions();
        
        //read_options.iterate_upper_bound = &s_upper_key;  NEED TO UPDATE THE UPPER BOUND HERE!!!!!
        leveldb::Iterator* it = db->NewIterator(read_options);

        uint64_t j = 0;
        for (it->Seek(s_key); it->Valid() && it->key().ToString() < s_upper_key.ToString(); it->Next(), j++) {
            uint64_t found_key = *reinterpret_cast<const uint64_t*>(it->key().data());
            assert(it->value().size() >= sizeof(uint64_t));
            value = *reinterpret_cast<const uint64_t*>(it->value().data());
            (void)value;
            break;
        }
        
        delete it;
    }
    
    clock_gettime(CLOCK_MONOTONIC, &ts_end);
    elapsed = static_cast<uint64_t>(ts_end.tv_sec) * 1000000000UL +
	static_cast<uint64_t>(ts_end.tv_nsec) -
	static_cast<uint64_t>(ts_start.tv_sec) * 1000000000UL +
	static_cast<uint64_t>(ts_start.tv_nsec);

    //std::cout << "elapsed:    " << (static_cast<double>(elapsed) / 1000000000.) << "\n";
    std::cout << "throughput: " << (static_cast<double>(query_count) / (static_cast<double>(elapsed) / 1000000000.)) << "\n";

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

uint64_t getReadIOCount() {
    std::ifstream io_file(std::string("/sys/block/sda/sda1/stat"));
    uint64_t io_count = 0;
    io_file >> io_count;
    return io_count;
}

uint64_t getWriteIOCount() { 
    std::ifstream io_file(std::string("/sys/block/sda/sda1/stat"));
    uint64_t io_count = 0;
    for(int i = 0; i < 5; i++) 
    	io_file >> io_count;
    return io_count;
}


uint64_t getMemFree() {
    std::ifstream mem_file(std::string("/proc/meminfo"));
    std::string str;
    uint64_t free_mem = 0;
    for (int i = 0; i < 4; i++)
	    mem_file >> str;
    mem_file >> free_mem;
    return free_mem;
}

uint64_t getMemAvailable() {
    std::ifstream mem_file(std::string("/proc/meminfo"));
    std::string str;
    uint64_t mem_available = 0;
    for (int i = 0; i < 7; i++)
	    mem_file >> str;
    mem_file >> mem_available;
    return mem_available;
}

int main(int argc, const char* argv[]) {
    if (argc < 3) {
        std::cout << "Usage:\n";
        std::cout << "arg 1: path to datafiles\n";
        std::cout << "arg 2: filter type\n";
        std::cout << "\t0: Bloom filter\n";
        std::cout << "\t1: SuRF\n";
        std::cout << "\t2: SuRF Hash\n";
	    std::cout << "\t3: SuRF Real\n";
        std::cout << "arg 3: query type\n";
        std::cout << "\t0: init\n";
        std::cout << "\t1: point query\n";
        std::cout << "\t2: open range query\n";
        std::cout << "\t3: closed range query\n";
        return -1;
    }

    std::string db_path = std::string(argv[1]); 
    int filter_type = atoi(argv[2]);
    int query_type = atoi(argv[3]);

    uint64_t scan_length = 1;
    uint64_t range_size = 5000000;

    const std::string kKeyPath = "/home/bx1/trash/test/pebblesdb/src/filter_experiment/poisson_timestamps.csv";
    const uint64_t kValueSize = 1000;
    const uint64_t kKeyRange = 10000000000000;
    const uint64_t kQueryCount = 50000;
    
    // 2GB config
    const uint64_t kKeyCount = 2000000;
    const uint64_t kWarmupSampleGap = 100;

        // 100GB config
    //const uint64_t kKeyCount = 100000000;
    //const uint64_t kWarmupSampleGap = kKeyCount / warmup_query_count;

    //=========================================================================
    
    leveldb::DB* db;
    leveldb::Options options;
    
    init(kKeyPath, db_path, &db, &options, kKeyCount, kValueSize, filter_type);

    if(query_type == 0) return 0;

    //=========================================================================
    
    //testScan(db, kKeyCount);


    uint64_t mem_free_before = getMemFree();
    uint64_t mem_available_before = getMemAvailable();

    //printIO();
    
    warmup(kKeyPath, db, kKeyCount, kWarmupSampleGap);
    
    
    uint64_t mem_free_after = getMemFree();
    uint64_t mem_available_after = getMemAvailable();
    std::cout << "Mem Free diff: " << (mem_free_before - mem_free_after) << "\n";
    std::cout << "Mem Aavilable diff: " << (mem_available_before - mem_available_after) << "\n";
    
    //printIO();
    uint64_t read_io_before = getReadIOCount();
    uint64_t write_io_before = getWriteIOCount();
    mem_free_before = getMemFree();
    mem_available_before = getMemAvailable();

     if (query_type == 1)
	    benchPointQuery(db, &options, kKeyRange, kQueryCount);
    else if (query_type == 2)
	    benchOpenRangeQuery(db, &options, kKeyRange, kQueryCount, scan_length);
    else if (query_type == 3)
	    benchClosedRangeQuery(db, &options, kKeyRange, kQueryCount, range_size);

    
    //printIO();
    uint64_t read_io_after = getReadIOCount();
    uint64_t write_io_after = getWriteIOCount();
    mem_free_after = getMemFree();
    mem_available_after = getMemAvailable();

    std::cout << "Read I/O count: " << (read_io_after - read_io_before) << "\n";
    std::cout << "Write I/O count: " << (write_io_after - write_io_before) << "\n";
    std::cout << "Mem Free diff: " << (mem_free_before - mem_free_after) << "\n";
    std::cout << "Mem Aavilable diff: " << (mem_available_before - mem_available_after) << "\n";
    

    close(db, &options);

    return 0;
}