// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <atomic>
#include <cstdint>
#include <cstring>
#include <memory>
#include <ostream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/gutil/bits.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/gutil/strings/human_readable.h"
#include "kudu/util/cache.h"
#include "kudu/util/monotime.h"
#include "kudu/util/random.h"
#include "kudu/util/random_util.h"
#include "kudu/util/slice.h"
#include "kudu/util/slru_cache.h"
#include "kudu/util/test_util.h"

DEFINE_int32(num_threads, 16, "The number of threads to access the cache concurrently.");
DEFINE_int32(run_seconds, 1, "The number of seconds to run the benchmark");

using std::atomic;
using std::pair;
using std::string;
using std::thread;
using std::unique_ptr;
using std::vector;

namespace kudu {

// Benchmark a 1GB cache.
static constexpr int kCacheCapacity = 1024 * 1024 * 1024;
static constexpr int kProbationarySegmentCapacity = 204 * 1024 * 1024;
static constexpr int kProtectedSegmentCapacity = kCacheCapacity - kProbationarySegmentCapacity;
static constexpr uint32_t kLookups = 2;
static constexpr uint16_t kMaxMultiplier = 256;

// Use 4kb entries.
static constexpr int kEntrySize = 4 * 1024;

// Test parameterization.
struct BenchSetup {
  enum class Pattern {
    // Zipfian distribution -- a small number of items make up the
    // vast majority of lookups.
    ZIPFIAN,
    // Every item is equally likely to be looked up.
    UNIFORM,
    // A small number of pre-determined items with small values are frequently looked up
    // while random items with large values are looked up less frequently.
    PRE_DETERMINED_FREQUENT_LOOKUPS
  };
  Pattern pattern;

  string ToString(Pattern pattern) const {
    switch (pattern) {
      case Pattern::ZIPFIAN: return "ZIPFIAN";
      case Pattern::UNIFORM: return "UNIFORM";
      case Pattern::PRE_DETERMINED_FREQUENT_LOOKUPS: return "PRE_DETERMINED_FREQUENT_LOOKUPS";
      default: LOG(FATAL) << "unexpected benchmark pattern: " << static_cast<int>(pattern); break;
    }
    return "unknown benchmark pattern";
  }

  // The ratio between the size of the dataset and the cache.
  //
  // A value smaller than 1 will ensure that the whole dataset fits
  // in the cache.
  double dataset_cache_ratio;

  Cache::EvictionPolicy eviction_policy;

  string ToString() const {
    string ret;
    ret += ToString(pattern);
    if (eviction_policy == Cache::EvictionPolicy::SLRU) {
      ret += " SLRU";
    } else {
      ret += " LRU";
    }
    ret += StringPrintf(" ratio=%.2fx n_unique=%d", dataset_cache_ratio, max_key());
    return ret;
  }

  // Return the maximum cache key to be generated for a lookup.
  uint32_t max_key() const {
    if (eviction_policy == Cache::EvictionPolicy::SLRU) {
      return static_cast<int64_t>(
          (kProbationarySegmentCapacity + kProtectedSegmentCapacity) * dataset_cache_ratio)
          / kEntrySize;
    }
    return static_cast<int64_t>(kCacheCapacity * dataset_cache_ratio) / kEntrySize;
  }
};

class CacheBench : public KuduTest,
                   public testing::WithParamInterface<BenchSetup>{
 public:
  void SetUp() override {
    KuduTest::SetUp();
    const BenchSetup& setup = GetParam();
    if (setup.eviction_policy == Cache::EvictionPolicy::SLRU) {
      cache_.reset(NewSLRUCache(kProbationarySegmentCapacity, kProtectedSegmentCapacity,
                                "test-cache", kLookups));
    } else {
      cache_.reset(NewCache(kCacheCapacity, "test-cache"));
    }
  }

  // Run queries against the cache until '*done' becomes true.
  // If 'frequent' is true, the workload is a small set of keys with small values.
  // If 'frequent' is false, the workload is a large set of keys with large values.
  // Returns a pair of the number of cache hits and lookups.
  pair<int64_t, int64_t> DoQueries(const atomic<bool>* done, bool frequent, uint32_t large_number) {
    const BenchSetup& setup = GetParam();
    Random r(GetRandomSeed32());
    int64_t lookups = 0;
    int64_t hits = 0;
    while (!*done) {
      uint32_t int_key;
      switch (setup.pattern) {
        case BenchSetup::Pattern::ZIPFIAN:
          int_key = r.Skewed(Bits::Log2Floor(setup.max_key()));
          break;
        case BenchSetup::Pattern::UNIFORM:
          int_key = r.Uniform(setup.max_key());
          break;
        case BenchSetup::Pattern::PRE_DETERMINED_FREQUENT_LOOKUPS:
          if (frequent) {
            auto small_multiplier = r.Uniform(kMaxMultiplier);
            int_key = large_number * small_multiplier;
          } else {
            // Rare random key with big value.
            int_key = r.Uniform(setup.max_key());
          }
          break;
        default:
          LOG(FATAL) << "Unsupported benchmark pattern" << setup.ToString(setup.pattern);
      }
      char key_buf[sizeof(int_key)];
      memcpy(key_buf, &int_key, sizeof(int_key));
      Slice key_slice(key_buf, arraysize(key_buf));
      auto h(cache_->Lookup(key_slice, Cache::EXPECT_IN_CACHE));
      if (h) {
        ++hits;
      } else {
        int entry_size = kEntrySize;
        if (setup.pattern == BenchSetup::Pattern::PRE_DETERMINED_FREQUENT_LOOKUPS && !frequent) {
          entry_size = 10000 * kEntrySize;
        }
        auto ph(cache_->Allocate(
            key_slice, /* val_len=*/entry_size, /* charge=*/entry_size));
        cache_->Insert(std::move(ph), nullptr);
      }
      ++lookups;
    }
    return {hits, lookups};
  }

  // Starts the given number of threads to concurrently call DoQueries.
  // Returns the aggregated number of cache hits and lookups.
  pair<int64_t, int64_t> RunQueryThreads(int n_threads, int n_seconds, uint32_t large_number) {
    vector<thread> threads(n_threads);
    atomic<bool> done(false);
    atomic<int64_t> total_lookups(0);
    atomic<int64_t> total_hits(0);
    bool frequent;
    for (int i = 0; i < n_threads; i++) {
      frequent = i % 2 == 0;
      threads[i] = thread([&, frequent]() {
          pair<int64_t, int64_t> hits_lookups = DoQueries(&done, frequent, large_number);
          total_hits += hits_lookups.first;
          total_lookups += hits_lookups.second;
        });
    }
    SleepFor(MonoDelta::FromSeconds(n_seconds));
    done = true;
    for (auto& t : threads) {
      t.join();
    }
    return {total_hits, total_lookups};
  }

 protected:
  unique_ptr<Cache> cache_;
};

// Test both distributions, and for each, test both the case where the data
// fits in the cache and where it is a bit larger.
INSTANTIATE_TEST_SUITE_P(Patterns, CacheBench, testing::ValuesIn(std::vector<BenchSetup>{
      {BenchSetup::Pattern::ZIPFIAN, 1.0, Cache::EvictionPolicy::LRU},
      {BenchSetup::Pattern::ZIPFIAN, 1.0, Cache::EvictionPolicy::SLRU},
      {BenchSetup::Pattern::ZIPFIAN, 3.0, Cache::EvictionPolicy::LRU},
      {BenchSetup::Pattern::ZIPFIAN, 3.0, Cache::EvictionPolicy::SLRU},
      {BenchSetup::Pattern::UNIFORM, 1.0, Cache::EvictionPolicy::LRU},
      {BenchSetup::Pattern::UNIFORM, 1.0, Cache::EvictionPolicy::SLRU},
      {BenchSetup::Pattern::UNIFORM, 3.0, Cache::EvictionPolicy::LRU},
      {BenchSetup::Pattern::UNIFORM, 3.0, Cache::EvictionPolicy::SLRU},
      {BenchSetup::Pattern::PRE_DETERMINED_FREQUENT_LOOKUPS, 1.0, Cache::EvictionPolicy::LRU},
      {BenchSetup::Pattern::PRE_DETERMINED_FREQUENT_LOOKUPS, 1.0, Cache::EvictionPolicy::SLRU},
      {BenchSetup::Pattern::PRE_DETERMINED_FREQUENT_LOOKUPS, 3.0, Cache::EvictionPolicy::LRU},
      {BenchSetup::Pattern::PRE_DETERMINED_FREQUENT_LOOKUPS, 3.0, Cache::EvictionPolicy::SLRU}
    }));

TEST_P(CacheBench, RunBench) {
  const BenchSetup& setup = GetParam();

  Random r(GetRandomSeed32());
  uint32_t large_number_max = setup.max_key() / kMaxMultiplier;
  uint32_t large_number = r.Uniform(large_number_max);

  // Run a short warmup phase to try to populate the cache. Otherwise, even if the
  // dataset is smaller than the cache capacity, we would count a bunch of misses
  // during the warm-up phase.
  LOG(INFO) << "Warming up...";
  RunQueryThreads(FLAGS_num_threads, 1, large_number);

  LOG(INFO) << "Running benchmark...";
  pair<int64_t, int64_t> hits_lookups = RunQueryThreads(FLAGS_num_threads, FLAGS_run_seconds,
                                                        large_number);
  int64_t hits = hits_lookups.first;
  int64_t lookups = hits_lookups.second;

  int64_t l_per_sec = lookups / FLAGS_run_seconds;
  double hit_rate = static_cast<double>(hits) / lookups;
  string test_case = setup.ToString();
  LOG(INFO) << test_case << ": " << HumanReadableNum::ToString(l_per_sec) << " lookups/sec";
  LOG(INFO) << test_case << ": " << StringPrintf("%.1f", hit_rate * 100.0) << "% hit rate";
}

} // namespace kudu
