/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <boost/random/uniform_int_distribution.hpp>
#include <folly/Benchmark.h>
#include <folly/init/Init.h>
#include "velox/buffer/Buffer.h"
#include "velox/type/DecimalUtil.h"

using namespace facebook::velox;

namespace {
static void fillDecimals(
    int64_t* decimals,
    const uint64_t* nullsPtr,
    const int64_t* values,
    const int64_t* scales,
    int32_t numValues,
    int32_t targetScale) {
  for (int32_t i = 0; i < numValues; i++) {
    if (!nullsPtr || !bits::isBitNull(nullsPtr, i)) {
      int32_t currentScale = scales[i];
      int64_t value = values[i];
      if (targetScale > currentScale &&
          targetScale - currentScale <= ShortDecimalType::kMaxPrecision) {
        value *= static_cast<int64_t>(
            DecimalUtil::kPowersOfTen[targetScale - currentScale]);
      } else if (
          targetScale < currentScale &&
          currentScale - targetScale <= ShortDecimalType::kMaxPrecision) {
        value /= static_cast<int64_t>(
            DecimalUtil::kPowersOfTen[currentScale - targetScale]);
      } else if (targetScale != currentScale) {
        VELOX_FAIL("Decimal scale out of range");
      }
      decimals[i] = value;
    }
  }
}
} // namespace

int32_t numValues = 100000;
int32_t targetScale = 4;
int64_t* results;
int64_t* resultsSimd;
const uint64_t* nullsPtr;
int64_t* values;
const int64_t* scales;

int64_t rand(std::mt19937& rng) {
  return boost::random::uniform_int_distribution<int64_t>()(rng) %
      DecimalUtil::kPowersOfTen[10];
}

int fillDecimalsBenchmark(int nIters, int nScales, bool isNull, bool isSimd) {
  folly::BenchmarkSuspender suspender;

  if (isSimd) {
    DecimalUtil::fillDecimals(
        resultsSimd, nullptr, values, scales, numValues, targetScale);
  } else {
    fillDecimals(resultsSimd, nullptr, values, scales, numValues, targetScale);
  }

  return nIters * numValues;
}

BENCHMARK_NAMED_PARAM_MULTI(
    fillDecimalsBenchmark,
    fillDecimals_2_false_false,
    2,
    false,
    false);

BENCHMARK_NAMED_PARAM_MULTI(
    fillDecimalsBenchmark,
    fillDecimals_4_false_false,
    4,
    false,
    false);

BENCHMARK_NAMED_PARAM_MULTI(
    fillDecimalsBenchmark,
    fillDecimals_6_false_false,
    6,
    false,
    false);

BENCHMARK_NAMED_PARAM_MULTI(
    fillDecimalsBenchmark,
    fillDecimals_2_true_false,
    2,
    true,
    false);

BENCHMARK_NAMED_PARAM_MULTI(
    fillDecimalsBenchmark,
    fillDecimals_4_true_false,
    4,
    true,
    false);

BENCHMARK_NAMED_PARAM_MULTI(
    fillDecimalsBenchmark,
    fillDecimals_6_true_false,
    6,
    true,
    false);

BENCHMARK_NAMED_PARAM_MULTI(
    fillDecimalsBenchmark,
    fillDecimals_2_false_true,
    2,
    false,
    true);

BENCHMARK_NAMED_PARAM_MULTI(
    fillDecimalsBenchmark,
    fillDecimals_4_false_true,
    4,
    false,
    true);

BENCHMARK_NAMED_PARAM_MULTI(
    fillDecimalsBenchmark,
    fillDecimals_6_false_true,
    6,
    false,
    true);

BENCHMARK_NAMED_PARAM_MULTI(
    fillDecimalsBenchmark,
    fillDecimals_2_true_true,
    2,
    true,
    true);

BENCHMARK_NAMED_PARAM_MULTI(
    fillDecimalsBenchmark,
    fillDecimals_4_true_true,
    4,
    true,
    true);

BENCHMARK_NAMED_PARAM_MULTI(
    fillDecimalsBenchmark,
    fillDecimals_6_true_true,
    6,
    true,
    true);

int32_t main(int32_t argc, char* argv[]) {
  folly::Init init{&argc, &argv};
  memory::MemoryManager::testingSetInstance({});
  auto pool = memory::memoryManager()->addLeafPool();
  std::mt19937 seed{12345};
  auto valuesBufferPtr =
      AlignedBuffer::allocate<int64_t>(numValues, pool.get());
  values = valuesBufferPtr->asMutable<int64_t>();

  auto resultsBufferPtr =
      AlignedBuffer::allocate<int64_t>(numValues, pool.get());
  results = resultsBufferPtr->asMutable<int64_t>();

  auto resultsSimdBufferPtr =
      AlignedBuffer::allocate<int64_t>(numValues, pool.get());
  resultsSimd = resultsSimdBufferPtr->asMutable<int64_t>();

  auto scalesBufferPtr =
      AlignedBuffer::allocate<int64_t>(numValues, pool.get(), 4);
  scales = scalesBufferPtr->as<int64_t>();

  auto numBytes = bits::nbytes(numValues);
  auto nulls = AlignedBuffer::allocate<char>(numBytes, pool.get(), 0);
  nulls->setSize(numBytes);
  auto* nullsPtr = nulls->asMutable<uint64_t>();
  if (false) {
    for (auto i = 0; i < numValues; i++) {
      if (i % 6 == 0) {
        bits::setNull(nullsPtr, 1);
      }
    }
  }

  for (auto i = 0; i < numValues; i++) {
    values[i] = rand(seed);
  }
//  auto resultsBufferPtr =
//      AlignedBuffer::allocate<int64_t>(numValues, pool.get());
//  results = resultsBufferPtr->asMutable<int64_t>();
//
//  auto resultsSimdBufferPtr =
//      AlignedBuffer::allocate<int64_t>(numValues, pool.get());
//  resultsSimd = resultsSimdBufferPtr->asMutable<int64_t>();
//
//  fillDecimals(results, nullptr, values, scales, numValues, targetScale);
//  DecimalUtil::fillDecimals(
//      resultsSimd, nullptr, values, scales, numValues, targetScale);
//  for (auto i = 0; i < numValues; i++) {
//    if (!bits::isBitNull(nullsPtr, i)) {
//      VELOX_CHECK_EQ(results[i], resultsSimd[i]);
//    }
//  }

  folly::runBenchmarks();
  return 0;
}
