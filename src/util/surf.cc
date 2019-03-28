// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "pebblesdb/filter_policy.h"

#include "pebblesdb/slice.h"
#include "util/hash.h"
#include <stdio.h>
#include "third-party/SuRF/include/surf.hpp"

namespace leveldb {

namespace {

class SuRFPolicy : public FilterPolicy {
public:
    explicit SuRFPolicy(int suffix_type, uint32_t suffix_len,
			bool include_dense, uint32_t sparse_dense_ratio)
	: suffix_len_(suffix_len), include_dense_(include_dense),
	  sparse_dense_ratio_(sparse_dense_ratio) {
	if (suffix_type == 1)
	    suffix_type_ = surf::kHash;
	else if (suffix_type == 2)
	    suffix_type_ = surf::kReal;
	else
	    suffix_type_ = surf::kNone;
    }

    ~SuRFPolicy() {
    }

    virtual const char* Name() const override {
	return "leveldb.BuiltinSuRF";
    }

    virtual void CreateFilter(const Slice* keys, int n,
			      std::string* dst) const override {
		std::vector<std::string> keys_str;
		for (size_t i = 0; i < (size_t)n; i++)
			keys_str.push_back(std::string(keys[i].data(), keys[i].size()));

		surf::SuRF* filter;
		if (suffix_type_ == surf::SuffixType::kHash)
			filter = new surf::SuRF(keys_str, include_dense_, sparse_dense_ratio_,
						suffix_type_, suffix_len_, 0);
		else
			filter = new surf::SuRF(keys_str, include_dense_, sparse_dense_ratio_,
						suffix_type_, 0, suffix_len_);
		uint64_t size = filter->serializedSize();
		char* data = filter->serialize();
		dst->append(data, size);
		filter->destroy();
		delete filter;
    }

    virtual bool KeyMayMatch(const Slice& key,
			     const Slice& filter) const override {
		char* filter_data = const_cast<char*>(filter.data());
		char* data = filter_data;
		surf::SuRF* filter_surf = surf::SuRF::deSerialize(data);
		bool found = filter_surf->lookupKey(std::string(key.data(), key.size()));
		delete filter_surf;
		return found;
    }

private:
    surf::SuffixType suffix_type_;
    surf::level_t suffix_len_;
    bool include_dense_;
    uint32_t sparse_dense_ratio_;
	
};
}

const FilterPolicy* NewSuRFPolicy(int suffix_type,
				  uint32_t suffix_len,
				  bool include_dense,
				  uint32_t sparse_dense_ratio) {
    return new SuRFPolicy(suffix_type, suffix_len, include_dense,
			  sparse_dense_ratio);
}
 
}// namespace leveldb
