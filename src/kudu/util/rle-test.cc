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

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <ostream>
#include <string>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/gutil/mathlimits.h"
#include "kudu/util/bit-stream-utils.h"
#include "kudu/util/bit-stream-utils.inline.h"
#include "kudu/util/bit-util.h"
#include "kudu/util/faststring.h"
#include "kudu/util/hexdump.h"
#include "kudu/util/rle-encoding.h"
#include "kudu/util/slice.h"
#include "kudu/util/test_util.h"

using std::string;
using std::vector;

namespace kudu {

constexpr const uint8_t kMaxWidth = 64;

class TestRle : public KuduTest {};

TEST(BitArray, TestBool) {
  const int len_bytes = 2;
  faststring buffer(len_bytes);

  BitWriter writer(&buffer);

  // Write alternating 0's and 1's
  for (int i = 0; i < 8; ++i) {
    writer.PutValue(i % 2, 1);
  }
  writer.Flush();
  EXPECT_EQ(buffer[0], 0b10101010);

  // Write 00110011
  for (int i = 0; i < 8; ++i) {
    switch (i) {
      case 0:
      case 1:
      case 4:
      case 5:
        writer.PutValue(0, 1);
        break;
      default:
        writer.PutValue(1, 1);
        break;
    }
  }
  writer.Flush();

  // Validate the exact bit value
  EXPECT_EQ(buffer[0], 0b10101010);
  EXPECT_EQ(buffer[1], 0b11001100);

  // Use the reader and validate
  BitReader reader(buffer.data(), buffer.size());
  for (int i = 0; i < 8; ++i) {
    bool val = false;
    bool result = reader.GetValue(1, &val);
    EXPECT_TRUE(result);
    EXPECT_EQ(val, i % 2);
  }

  for (int i = 0; i < 8; ++i) {
    bool val = false;
    bool result = reader.GetValue(1, &val);
    EXPECT_TRUE(result);
    switch (i) {
      case 0:
      case 1:
      case 4:
      case 5:
        EXPECT_EQ(val, false);
        break;
      default:
        EXPECT_EQ(val, true);
        break;
    }
  }
}

// Writes 'num_vals' values with width 'bit_width' and reads them back.
void TestBitArrayValues(int bit_width, int num_vals) {
  const int kTestLen = BitUtil::Ceil<3>(bit_width * num_vals);
  const uint64_t mod = bit_width == 64? 1 : 1LL << bit_width;

  faststring buffer(kTestLen);
  BitWriter writer(&buffer);
  for (int i = 0; i < num_vals; ++i) {
    writer.PutValue(i % mod, bit_width);
  }
  writer.Flush();
  EXPECT_EQ(writer.bytes_written(), kTestLen);

  BitReader reader(buffer.data(), kTestLen);
  for (int i = 0; i < num_vals; ++i) {
    int64_t val = 0;
    bool result = reader.GetValue(bit_width, &val);
    EXPECT_TRUE(result);
    EXPECT_EQ(val, i % mod);
  }
  EXPECT_EQ(reader.bytes_left(), 0);
}

TEST(BitArray, TestValues) {
  for (int width = 1; width <= kMaxWidth; ++width) {
    TestBitArrayValues(width, 1);
    TestBitArrayValues(width, 2);
    // Don't write too many values
    TestBitArrayValues(width, (width < 12) ? (1 << width) : 4096);
    TestBitArrayValues(width, 1024);
  }
}

// Test some mixed values
TEST(BitArray, TestMixed) {
  const int kTestLenBits = 1024;
  faststring buffer(kTestLenBits / 8);
  bool parity = true;

  BitWriter writer(&buffer);
  for (int i = 0; i < kTestLenBits; ++i) {
    if (i % 2 == 0) {
      writer.PutValue(parity, 1);
      parity = !parity;
    } else {
      writer.PutValue(i, 10);
    }
  }
  writer.Flush();

  parity = true;
  BitReader reader(buffer.data(), buffer.size());
  for (int i = 0; i < kTestLenBits; ++i) {
    bool result;
    if (i % 2 == 0) {
      bool val = false;
      result = reader.GetValue(1, &val);
      EXPECT_EQ(val, parity);
      parity = !parity;
    } else {
      int val;
      result = reader.GetValue(10, &val);
      EXPECT_EQ(val, i);
    }
    EXPECT_TRUE(result);
  }
}

// Validates encoding of values by encoding and decoding them.  If
// expected_encoding != NULL, also validates that the encoded buffer is
// exactly 'expected_encoding'.
// if expected_len is not -1, it will validate the encoded size is correct.
template<typename T, uint8_t BIT_WIDTH>
void ValidateRle(const vector<T>& values,
                 uint8_t* expected_encoding,
                 int expected_len) {
  faststring buffer;
  RleEncoder<T, BIT_WIDTH> encoder(&buffer);

  for (const auto& value : values) {
    encoder.Put(value);
  }
  int encoded_len = encoder.Flush();

  if (expected_len != -1) {
    EXPECT_EQ(encoded_len, expected_len);
  }
  if (expected_encoding != nullptr) {
    EXPECT_EQ(memcmp(buffer.data(), expected_encoding, expected_len), 0)
      << "\n"
      << "Expected: " << HexDump(Slice(expected_encoding, expected_len)) << "\n"
      << "Got:      " << HexDump(Slice(buffer));
  }

  // Verify read
  RleDecoder<T, BIT_WIDTH> decoder(buffer.data(), encoded_len);
  for (const auto& value : values) {
    T val = 0;
    bool result = decoder.Get(&val);
    EXPECT_TRUE(result);
    EXPECT_EQ(value, val);
  }
}

template<typename T, uint8_t BIT_WIDTH>
void ValidateRleUpToWidth(const vector<T>& values,
                          uint8_t* expected_encoding,
                          int expected_len) {
  if constexpr (BIT_WIDTH == 1) {
    ValidateRle<T, 1>(values, expected_encoding, expected_len);
  } else {
    ValidateRleUpToWidth<T, BIT_WIDTH - 1>(values, expected_encoding, expected_len);
    ValidateRle<T, BIT_WIDTH>(values, expected_encoding, expected_len);
  }
}

template<typename T, uint8_t BIT_WIDTH>
void ValidateRleFromWidth(const vector<T>& values) {
  if constexpr (BIT_WIDTH == kMaxWidth) {
    ValidateRle<T, kMaxWidth>(values, nullptr, 2 * (1 + BitUtil::Ceil<3>(kMaxWidth)));
  } else {
    ValidateRle<T, BIT_WIDTH>(values, nullptr, 2 * (1 + BitUtil::Ceil<3>(BIT_WIDTH)));
    ValidateRleFromWidth<T, BIT_WIDTH + 1>(values);
  }
}

template<typename T, uint8_t BIT_WIDTH>
void ValidateRleFromWidthX100(const vector<T>& values) {
  if constexpr (BIT_WIDTH == kMaxWidth) {
    ValidateRle<T, kMaxWidth>(values, nullptr, 1 + BitUtil::Ceil<3>(kMaxWidth * 100));
  } else {
    ValidateRle<T, BIT_WIDTH>(values, nullptr, 1 + BitUtil::Ceil<3>(BIT_WIDTH * 100));
    ValidateRleFromWidthX100<T, BIT_WIDTH + 1>(values);
  }
}

TEST(Rle, SpecificSequences) {
  const int kTestLen = 1024;
  uint8_t expected_buffer[kTestLen];
  vector<uint64_t> values(100);

  // Test 50 0' followed by 50 1's
  for (int i = 0; i < 50; ++i) {
    values[i] = 0;
  }
  for (int i = 50; i < 100; ++i) {
    values[i] = 1;
  }

  // expected_buffer valid for bit width <= 1 byte
  expected_buffer[0] = (50 << 1);
  expected_buffer[1] = 0;
  expected_buffer[2] = (50 << 1);
  expected_buffer[3] = 1;

  ValidateRleUpToWidth<uint64_t, 8>(values, expected_buffer, 4);
  ValidateRleFromWidth<uint64_t, 9>(values);

  // Test 100 0's and 1's alternating
  for (int i = 0; i < 100; ++i) {
    values[i] = i % 2;
  }
  constexpr const auto num_groups = BitUtil::Ceil<3>(100);
  expected_buffer[0] = (num_groups << 1) | 1;
  for (int i = 0; i < 100/8; ++i) {
    expected_buffer[i + 1] = 0b10101010; // 0xaa
  }
  // Values for the last 4 0 and 1's
  expected_buffer[1 + 100/8] = 0b00001010; // 0x0a

  // num_groups and expected_buffer only valid for bit width = 1
  ValidateRle<uint64_t, 1>(values, expected_buffer, num_groups + 1);
  ValidateRleFromWidthX100<uint64_t, 2>(values);
}

// ValidateRle on 'num_vals' values with width 'bit_width'. If 'value' != -1, that value
// is used, otherwise alternating values are used.
template<uint8_t BIT_WIDTH>
void TestRleValues(int num_vals, int value = -1) {
  constexpr const uint64_t mod = BIT_WIDTH == 64 ? 1ULL : 1ULL << BIT_WIDTH;
  vector<uint64_t> values;
  values.reserve(num_vals);
  for (uint64_t v = 0; v < num_vals; ++v) {
    values.push_back((value != -1) ? value : (BIT_WIDTH == 64 ? v : (v % mod)));
  }
  ValidateRle<uint64_t, BIT_WIDTH>(values, nullptr, -1);
}

template<uint8_t BIT_WIDTH>
void TestRleValuesFromWidth() {
  if constexpr (BIT_WIDTH == kMaxWidth) {
    TestRleValues<kMaxWidth>(1);
    TestRleValues<kMaxWidth>(1024);
    TestRleValues<kMaxWidth>(1024, 0);
    TestRleValues<kMaxWidth>(1024, 1);
  } else {
    TestRleValues<BIT_WIDTH>(1);
    TestRleValues<BIT_WIDTH>(1024);
    TestRleValues<BIT_WIDTH>(1024, 0);
    TestRleValues<BIT_WIDTH>(1024, 1);
    TestRleValuesFromWidth<BIT_WIDTH + 1>();
  }
}

TEST(RleTest, Values) {
  TestRleValuesFromWidth<1>();
}

class BitRle : public KuduTest {
 protected:

  template<uint8_t BIT_WIDTH>
  void TestRandomBools(uint32_t seed) {
    if constexpr (BIT_WIDTH == 1) {
      vector<uint64_t> values = FillWithRandomValues(seed + 1);
      ValidateRle<uint64_t, 1>(values, nullptr, -1);
    } else {
      TestRandomBools<BIT_WIDTH - 1>(seed + BIT_WIDTH);
      vector<uint64_t> values = FillWithRandomValues(seed);
      ValidateRle<uint64_t, BIT_WIDTH>(values, nullptr, -1);
    }
  }

  static vector<uint64_t> FillWithRandomValues(uint32_t seed) {
    srand(seed);
    vector<uint64_t> values;
    bool parity = 0;
    for (int i = 0; i < 1000; ++i) {
      int group_size = rand() % 20 + 1; // NOLINT(*)
      if (group_size > 16) {
        group_size = 1;
      }
      for (int i = 0; i < group_size; ++i) {
        values.push_back(parity);
      }
      parity = !parity;
    }
    return values;
  }
};

// Tests all true/false values
TEST_F(BitRle, AllSame) {
  const int kTestLen = 1024;
  vector<bool> values;

  for (int v = 0; v < 2; ++v) {
    values.clear();
    for (int i = 0; i < kTestLen; ++i) {
      values.push_back(v != 0);
    }

    ValidateRle<bool, 1>(values, nullptr, 3);
  }
}

// Test that writes out a repeated group and then a literal
// group but flush before finishing.
TEST_F(BitRle, Flush) {
  vector<bool> values;
  values.reserve(20);
  for (int i = 0; i < 16; ++i) {
    values.push_back(true);
  }
  values.push_back(false);
  ValidateRle<bool, 1>(values, nullptr, -1);
  values.push_back(true);
  ValidateRle<bool, 1>(values, nullptr, -1);
  values.push_back(true);
  ValidateRle<bool, 1>(values, nullptr, -1);
  values.push_back(true);
  ValidateRle<bool, 1>(values, nullptr, -1);
}

// Test some random bool sequences.
TEST_F(BitRle, RandomBools) {
  const int n_iters = AllowSlowTests() ? 20 : 3;
  for (uint32_t it = 0; it < n_iters; ++it) {
    TestRandomBools<kMaxWidth>(it);
  }
}

// Test some random 64-bit sequences.
TEST_F(BitRle, Random64Bit) {
  int iters = 0;
  const int n_iters = AllowSlowTests() ? 1000 : 20;
  while (iters < n_iters) {
    srand(iters++);
    if (iters % 10000 == 0) LOG(ERROR) << "Seed: " << iters;
    vector<uint64_t > values;
    for (int i = 0; i < 1000; ++i) {
      int group_size = rand() % 20 + 1; // NOLINT(*)
      uint64_t cur_value = (static_cast<uint64_t>(rand()) << 32) + static_cast<uint64_t>(rand());
      if (group_size > 16) {
        group_size = 1;
      }
      for (int i = 0; i < group_size; ++i) {
        values.push_back(cur_value);
      }

    }
    ValidateRle<uint64_t, 64>(values, nullptr, -1);
  }
}

// Test a sequence of 1 0's, 2 1's, 3 0's. etc
// e.g. 011000111100000
TEST_F(BitRle, RepeatedPattern) {
  vector<bool> values;
  const int min_run = 1;
  const int max_run = 32;

  for (int i = min_run; i <= max_run; ++i) {
    int v = i % 2;
    for (int j = 0; j < i; ++j) {
      values.push_back(v);
    }
  }

  // And go back down again
  for (int i = max_run; i >= min_run; --i) {
    int v = i % 2;
    for (int j = 0; j < i; ++j) {
      values.push_back(v);
    }
  }

  ValidateRle<bool, 1>(values, nullptr, -1);
}

TEST_F(TestRle, TestBulkPut) {
  size_t run_length;
  bool val = false;

  faststring buffer(1);
  RleEncoder<bool, 1> encoder(&buffer);
  encoder.Put(true, 10);
  encoder.Put(false, 7);
  encoder.Put(true, 5);
  encoder.Put(true, 15);
  encoder.Flush();

  RleDecoder<bool, 1> decoder(buffer.data(), encoder.len());
  run_length = decoder.GetNextRun(&val, MathLimits<size_t>::kMax);
  ASSERT_TRUE(val);
  ASSERT_EQ(10, run_length);

  run_length = decoder.GetNextRun(&val, MathLimits<size_t>::kMax);
  ASSERT_FALSE(val);
  ASSERT_EQ(7, run_length);

  run_length = decoder.GetNextRun(&val, MathLimits<size_t>::kMax);
  ASSERT_TRUE(val);
  ASSERT_EQ(20, run_length);

  ASSERT_EQ(0, decoder.GetNextRun(&val, MathLimits<size_t>::kMax));
}

TEST_F(TestRle, TestGetNextRun) {
  // Repeat the test with different number of items
  for (int num_items = 7; num_items < 200; num_items += 13) {
    // Test different block patterns
    //    1: 01010101 01010101
    //    2: 00110011 00110011
    //    3: 00011100 01110001
    //    ...
    for (int block = 1; block <= 20; ++block) {
      faststring buffer(1);
      RleEncoder<bool, 1> encoder(&buffer);
      for (int j = 0; j < num_items; ++j) {
        encoder.Put(!!(j & 1), block);
      }
      encoder.Flush();

      RleDecoder<bool, 1> decoder(buffer.data(), encoder.len());
      size_t count = num_items * block;
      for (int j = 0; j < num_items; ++j) {
        size_t run_length;
        bool val = false;
        DCHECK_GT(count, 0);
        run_length = decoder.GetNextRun(&val, MathLimits<size_t>::kMax);
        run_length = std::min(run_length, count);

        ASSERT_EQ(!!(j & 1), val);
        ASSERT_EQ(block, run_length);
        count -= run_length;
      }
      DCHECK_EQ(count, 0);
    }
  }
}

// Generate a random bit string which consists of 'num_runs' runs,
// each with a random length between 1 and 100. Returns the number
// of values encoded (i.e the sum run length).
static size_t GenerateRandomBitString(int num_runs, faststring* enc_buf, string* string_rep) {
  RleEncoder<bool, 1> enc(enc_buf);
  size_t num_bits = 0;
  for (int i = 0; i < num_runs; i++) {
    size_t run_length = random() % 100;
    bool value = static_cast<bool>(i & 1);
    enc.Put(value, run_length);
    string_rep->append(run_length, value ? '1' : '0');
    num_bits += run_length;
  }
  enc.Flush();
  return num_bits;
}

TEST_F(TestRle, TestRoundTripRandomSequencesWithRuns) {
  SeedRandom();

  // Test the limiting function of GetNextRun.
  const int kMaxToReadAtOnce = (random() % 20) + 1;

  // Generate a bunch of random bit sequences, and "round-trip" them
  // through the encode/decode sequence.
  for (int rep = 0; rep < 100; rep++) {
    faststring buf;
    string string_rep;
    int num_bits = GenerateRandomBitString(10, &buf, &string_rep);
    RleDecoder<bool, 1> decoder(buf.data(), buf.size());
    string roundtrip_str;
    int rem_to_read = num_bits;
    size_t run_len;
    bool val;
    while (rem_to_read > 0 &&
           (run_len = decoder.GetNextRun(&val, std::min(kMaxToReadAtOnce, rem_to_read))) != 0) {
      ASSERT_LE(run_len, kMaxToReadAtOnce);
      roundtrip_str.append(run_len, val ? '1' : '0');
      rem_to_read -= run_len;
    }

    ASSERT_EQ(string_rep, roundtrip_str);
  }
}
TEST_F(TestRle, TestSkip) {
  faststring buffer(1);
  RleEncoder<bool, 1> encoder(&buffer);

  // 0101010[1] 01010101 01
  //        "A"
  for (int j = 0; j < 18; ++j) {
    encoder.Put(!!(j & 1));
  }

  // 0011[00] 11001100 11001100 11001100 11001100
  //      "B"
  for (int j = 0; j < 19; ++j) {
    encoder.Put(!!(j & 1), 2);
  }

  // 000000000000 11[1111111111] 000000000000 111111111111
  //                   "C"
  // 000000000000 111111111111 0[00000000000] 111111111111
  //                                  "D"
  // 000000000000 111111111111 000000000000 111111111111
  for (int j = 0; j < 12; ++j) {
    encoder.Put(!!(j & 1), 12);
  }
  encoder.Flush();

  bool val = false;
  size_t run_length;
  RleDecoder<bool, 1> decoder(buffer.data(), encoder.len());

  // position before "A"
  ASSERT_EQ(3, decoder.Skip(7));
  run_length = decoder.GetNextRun(&val, MathLimits<size_t>::kMax);
  ASSERT_TRUE(val);
  ASSERT_EQ(1, run_length);

  // position before "B"
  ASSERT_EQ(7, decoder.Skip(14));
  run_length = decoder.GetNextRun(&val, MathLimits<size_t>::kMax);
  ASSERT_FALSE(val);
  ASSERT_EQ(2, run_length);

  // position before "C"
  ASSERT_EQ(18, decoder.Skip(46));
  run_length = decoder.GetNextRun(&val, MathLimits<size_t>::kMax);
  ASSERT_TRUE(val);
  ASSERT_EQ(10, run_length);

  // position before "D"
  ASSERT_EQ(24, decoder.Skip(49));
  run_length = decoder.GetNextRun(&val, MathLimits<size_t>::kMax);
  ASSERT_FALSE(val);
  ASSERT_EQ(11, run_length);

  encoder.Flush();
}

// RLE encoding groups values and decides whether to run-length encode or simply bit-pack
// (literal encoding). This test verifies correctness of the RLE decoding when literal
// encoding is used irrespective of the size of the group and the number of values encoded.
template <typename IntType>
class TestRleLiteralGetNextRun : public KuduTest {
 public:
  void RunTest() {
    constexpr const auto num_bytes_per_val = sizeof(IntType);
    constexpr const auto bit_width = 8 * num_bytes_per_val;

    // Test with number of values that are not necessarily multiple of the group size (8).
    auto max_num_vals = std::min<uint64_t>(1024, std::numeric_limits<IntType>::max());
    for (auto num_vals = 1; num_vals <= max_num_vals; num_vals++) {
      faststring buffer(num_vals * num_bytes_per_val);
      RleEncoder<IntType, bit_width> encoder(&buffer);

      // Use non-repeated pattern of integers so that literal encoding is used.
      for (auto i = 0; i < num_vals; i++) {
        encoder.Put(i, 1);
      }
      encoder.Flush();

      RleDecoder<IntType, bit_width> decoder(buffer.data(), encoder.len());
      IntType val = 0;
      for (auto i = 0; i < num_vals; i++) {
        size_t len = decoder.GetNextRun(&val, num_vals);
        ASSERT_EQ(1, len);
        ASSERT_EQ(i, val);
      }
      // Read one beyond to verify no value is returned.
      ASSERT_EQ(0, decoder.GetNextRun(&val, num_vals));
    }
  }
};

typedef ::testing::Types<int8_t, int16_t, int32_t, int64_t> IntDataTypes;
TYPED_TEST_SUITE(TestRleLiteralGetNextRun, IntDataTypes);

TYPED_TEST(TestRleLiteralGetNextRun, RleGetNextRunIntDataTypes) {
  this->RunTest();
}

} // namespace kudu
