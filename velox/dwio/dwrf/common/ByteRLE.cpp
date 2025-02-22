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
#include "velox/dwio/dwrf/common/ByteRLE.h"

#include <algorithm>

#include "velox/dwio/common/exception/Exceptions.h"

namespace facebook::velox::dwrf {

class ByteRleEncoderImpl : public ByteRleEncoder {
 public:
  explicit ByteRleEncoderImpl(std::unique_ptr<BufferedOutputStream> output)
      : outputStream_{std::move(output)},
        numLiterals_{0},
        repeat_{false},
        tailRunLength_{0},
        bufferPosition_{0},
        bufferLength_{0},
        buffer_{nullptr} {}

  uint64_t add(
      const char* data,
      const common::Ranges& ranges,
      const uint64_t* nulls) override;

  uint64_t add(
      const std::function<char(vector_size_t)>& valueAt,
      const common::Ranges& ranges,
      const std::function<bool(vector_size_t)>& isNullAt) override;

  uint64_t addBits(
      const uint64_t* data,
      const common::Ranges& ranges,
      const uint64_t* nulls,
      bool invert) override {
    throw std::runtime_error("addBits is only for bool stream");
  }

  uint64_t addBits(
      const std::function<bool(vector_size_t)>& isNullAt,
      const common::Ranges& ranges,
      const std::function<bool(vector_size_t)>& valueAt,
      bool invert) override {
    throw std::runtime_error("addBits is only for bool stream");
  }

  uint64_t getBufferSize() const override {
    return outputStream_->size();
  }

  uint64_t flush() override;

  void recordPosition(PositionRecorder& recorder, int32_t strideIndex = -1)
      const override;

 protected:
  void writeByte(char c);
  void writeValues();
  void write(char c);

  std::unique_ptr<BufferedOutputStream> outputStream_;
  std::array<char, RLE_MAX_LITERAL_SIZE> literals_;
  int32_t numLiterals_;
  bool repeat_;
  int32_t tailRunLength_;
  int32_t bufferPosition_;
  int32_t bufferLength_;
  char* buffer_;
};

void ByteRleEncoderImpl::writeByte(char c) {
  if (UNLIKELY(bufferPosition_ == bufferLength_)) {
    int32_t addedSize = 0;
    DWIO_ENSURE(
        outputStream_->Next(reinterpret_cast<void**>(&buffer_), &addedSize),
        "Allocation failure");
    bufferPosition_ = 0;
    bufferLength_ = addedSize;
  }
  buffer_[bufferPosition_++] = c;
}

uint64_t ByteRleEncoderImpl::add(
    const char* data,
    const common::Ranges& ranges,
    const uint64_t* nulls) {
  uint64_t count = 0;
  if (nulls) {
    for (auto& pos : ranges) {
      if (!bits::isBitNull(nulls, pos)) {
        write(data[pos]);
        ++count;
      }
    }
  } else {
    for (auto& pos : ranges) {
      write(data[pos]);
      ++count;
    }
  }
  return count;
}

uint64_t ByteRleEncoderImpl::add(
    const std::function<char(vector_size_t)>& valueAt,
    const common::Ranges& ranges,
    const std::function<bool(vector_size_t)>& isNullAt) {
  uint64_t count = 0;
  if (isNullAt) {
    for (auto& pos : ranges) {
      if (!isNullAt(pos)) {
        write(valueAt(pos));
        ++count;
      }
    }
  } else {
    for (auto& pos : ranges) {
      write(valueAt(pos));
      ++count;
    }
  }
  return count;
}

void ByteRleEncoderImpl::writeValues() {
  if (numLiterals_ == 0) {
    return;
  }
  if (repeat_) {
    writeByte(static_cast<char>(numLiterals_ - RLE_MINIMUM_REPEAT));
    writeByte(literals_[0]);
  } else {
    writeByte(static_cast<char>(-numLiterals_));
    for (int32_t i = 0; i < numLiterals_; ++i) {
      writeByte(literals_[i]);
    }
  }
  repeat_ = false;
  tailRunLength_ = 0;
  numLiterals_ = 0;
}

uint64_t ByteRleEncoderImpl::flush() {
  writeValues();
  outputStream_->BackUp(bufferLength_ - bufferPosition_);
  uint64_t dataSize = outputStream_->flush();
  bufferLength_ = bufferPosition_ = 0;
  return dataSize;
}

void ByteRleEncoderImpl::write(char value) {
  if (numLiterals_ == 0) {
    literals_[numLiterals_++] = value;
    tailRunLength_ = 1;
  } else if (repeat_) {
    if (value == literals_[0]) {
      if (++numLiterals_ == RLE_MAXIMUM_REPEAT) {
        writeValues();
      }
    } else {
      writeValues();
      literals_[numLiterals_++] = value;
      tailRunLength_ = 1;
    }
  } else {
    if (value == literals_[numLiterals_ - 1]) {
      tailRunLength_ += 1;
    } else {
      tailRunLength_ = 1;
    }
    if (tailRunLength_ == RLE_MINIMUM_REPEAT) {
      if (numLiterals_ + 1 > RLE_MINIMUM_REPEAT) {
        numLiterals_ -= (RLE_MINIMUM_REPEAT - 1);
        writeValues();
        literals_[0] = value;
      }
      repeat_ = true;
      numLiterals_ = RLE_MINIMUM_REPEAT;
    } else {
      literals_[numLiterals_++] = value;
      if (numLiterals_ == RLE_MAX_LITERAL_SIZE) {
        writeValues();
      }
    }
  }
}

void ByteRleEncoderImpl::recordPosition(
    PositionRecorder& recorder,
    int32_t strideIndex) const {
  outputStream_->recordPosition(
      recorder, bufferLength_, bufferPosition_, strideIndex);
  recorder.add(static_cast<uint64_t>(numLiterals_), strideIndex);
}

std::unique_ptr<ByteRleEncoder> createByteRleEncoder(
    std::unique_ptr<BufferedOutputStream> output) {
  return std::make_unique<ByteRleEncoderImpl>(std::move(output));
}

class BooleanRleEncoderImpl : public ByteRleEncoderImpl {
 public:
  explicit BooleanRleEncoderImpl(std::unique_ptr<BufferedOutputStream> output)
      : ByteRleEncoderImpl{std::move(output)}, bitsRemained{8}, current{0} {}

  uint64_t add(
      const char* data,
      const common::Ranges& ranges,
      const uint64_t* nulls) override;

  uint64_t addBits(
      const uint64_t* data,
      const common::Ranges& ranges,
      const uint64_t* nulls,
      bool invert) override;

  uint64_t addBits(
      const std::function<bool(vector_size_t)>& isNullAt,
      const common::Ranges& ranges,
      const std::function<bool(vector_size_t)>& valueAt,
      bool invert) override;

  uint64_t flush() override {
    if (bitsRemained != 8) {
      writeByte();
    }
    return ByteRleEncoderImpl::flush();
  }

  void recordPosition(PositionRecorder& recorder, int32_t strideIndex = -1)
      const override {
    ByteRleEncoderImpl::recordPosition(recorder, strideIndex);
    recorder.add(static_cast<uint64_t>(8 - bitsRemained), strideIndex);
  }

 private:
  int32_t bitsRemained;
  char current;

  void writeByte() {
    write(current);
    bitsRemained = 8;
    current = 0;
  }

  void writeBool(bool val) {
    --bitsRemained;
    current |= ((val ? 1 : 0) << bitsRemained);
    if (bitsRemained == 0) {
      writeByte();
    }
  }
};

uint64_t BooleanRleEncoderImpl::add(
    const char* data,
    const common::Ranges& ranges,
    const uint64_t* nulls) {
  uint64_t count = 0;
  if (nulls) {
    for (auto& pos : ranges) {
      if (!bits::isBitNull(nulls, pos)) {
        writeBool(!data || data[pos]);
        ++count;
      }
    }
  } else {
    for (auto& pos : ranges) {
      writeBool(!data || data[pos]);
      ++count;
    }
  }
  return count;
}

uint64_t BooleanRleEncoderImpl::addBits(
    const uint64_t* data,
    const common::Ranges& ranges,
    const uint64_t* nulls,
    bool invert) {
  uint64_t count = 0;
  if (nulls) {
    for (auto& pos : ranges) {
      if (!bits::isBitNull(nulls, pos)) {
        bool val = (!data || invert != bits::isBitSet(data, pos));
        writeBool(val);
        ++count;
      }
    }
  } else {
    for (auto& pos : ranges) {
      bool val = (!data || invert != bits::isBitSet(data, pos));
      writeBool(val);
      ++count;
    }
  }
  return count;
}

uint64_t BooleanRleEncoderImpl::addBits(
    const std::function<bool(vector_size_t)>& valueAt,
    const common::Ranges& ranges,
    const std::function<bool(vector_size_t)>& isNullAt,
    bool invert) {
  uint64_t count = 0;
  if (isNullAt) {
    for (auto& pos : ranges) {
      if (!isNullAt(pos)) {
        bool val = (!valueAt || invert != valueAt(pos));
        writeBool(val);
        ++count;
      }
    }
  } else {
    for (auto& pos : ranges) {
      bool val = (!valueAt || invert != valueAt(pos));
      writeBool(val);
      ++count;
    }
  }
  return count;
}

std::unique_ptr<ByteRleEncoder> createBooleanRleEncoder(
    std::unique_ptr<BufferedOutputStream> output) {
  return std::make_unique<BooleanRleEncoderImpl>(std::move(output));
}

void ByteRleDecoder::nextBuffer() {
  VELOX_DCHECK_EQ(pendingSkip_, 0);

  int32_t bufferLength;
  const void* bufferPointer;
  const auto ret = inputStream_->Next(&bufferPointer, &bufferLength);
  VELOX_CHECK(
      ret,
      "bad read in nextBuffer {}, {}",
      encodingKey_.toString(),
      inputStream_->getName());
  bufferStart_ = static_cast<const char*>(bufferPointer);
  bufferEnd_ = bufferStart_ + bufferLength;
}

void ByteRleDecoder::seekToRowGroup(
    dwio::common::PositionProvider& positionProvider) {
  // Move the input stream
  inputStream_->seekToPosition(positionProvider);
  // Force a re-read from the stream
  bufferEnd_ = bufferStart_;
  // Force reading a new header
  remainingValues_ = 0;
  // Skip ahead the given number of records
  pendingSkip_ = positionProvider.next();
}

void ByteRleDecoder::skipBytes(size_t count) {
  if (bufferStart_ < bufferEnd_) {
    const size_t skipSize = std::min(
        static_cast<size_t>(count),
        static_cast<size_t>(bufferEnd_ - bufferStart_));
    bufferStart_ += skipSize;
    count -= skipSize;
  }
  if (count > 0) {
    inputStream_->SkipInt64(count);
  }
}

void ByteRleDecoder::next(
    char* data,
    uint64_t numValues,
    const uint64_t* nulls) {
  skipPending();

  uint64_t position = 0;
  // skip over null values
  while (nulls && position < numValues && bits::isBitNull(nulls, position)) {
    ++position;
  }

  while (position < numValues) {
    // If we are out of values, read more.
    if (remainingValues_ == 0) {
      readHeader();
    }
    // How many do we read out of this block?
    const size_t count =
        std::min(static_cast<size_t>(numValues - position), remainingValues_);
    uint64_t consumed{0};
    if (repeating_) {
      if (nulls) {
        for (uint64_t i = 0; i < count; ++i) {
          if (!bits::isBitNull(nulls, position + i)) {
            data[position + i] = value_;
            ++consumed;
          }
        }
      } else {
        ::memset(data + position, value_, count);
        consumed = count;
      }
    } else {
      if (nulls) {
        for (uint64_t i = 0; i < count; ++i) {
          if (!bits::isBitNull(nulls, position + i)) {
            data[position + i] = readByte();
            ++consumed;
          }
        }
      } else {
        uint64_t i = 0;
        while (i < count) {
          if (bufferStart_ == bufferEnd_) {
            nextBuffer();
          }
          const uint64_t copyBytes = std::min(
              static_cast<uint64_t>(count - i),
              static_cast<uint64_t>(bufferEnd_ - bufferStart_));
          std::copy(
              bufferStart_, bufferStart_ + copyBytes, data + position + i);
          bufferStart_ += copyBytes;
          i += copyBytes;
        }
        consumed = count;
      }
    }
    remainingValues_ -= consumed;
    position += count;
    // skip over any null values
    while (nulls && position < numValues && bits::isBitNull(nulls, position)) {
      ++position;
    }
  }
}

std::unique_ptr<ByteRleDecoder> createByteRleDecoder(
    std::unique_ptr<dwio::common::SeekableInputStream> input,
    const EncodingKey& ek) {
  return std::make_unique<ByteRleDecoder>(std::move(input), ek);
}

void BooleanRleDecoder::seekToRowGroup(
    dwio::common::PositionProvider& positionProvider) {
  ByteRleDecoder::seekToRowGroup(positionProvider);
  const uint64_t consumed = positionProvider.next();
  VELOX_CHECK_LE(
      consumed,
      8,
      "bad position ",
      encodingKey_.toString(),
      ", ",
      inputStream_->getName());
  pendingSkip_ = 8 * pendingSkip_ + consumed;
  remainingBits_ = 0;
}

void BooleanRleDecoder::skipPending() {
  auto numValues = pendingSkip_;
  pendingSkip_ = 0;
  if (numValues <= remainingBits_) {
    remainingBits_ -= numValues;
  } else {
    numValues -= remainingBits_;
    remainingBits_ = 0;
    pendingSkip_ = numValues / 8;
    ByteRleDecoder::skipPending();
    uint64_t bitsToSkip = numValues % 8;
    if (bitsToSkip) {
      ByteRleDecoder::next(
          reinterpret_cast<char*>(&reversedLastByte_), 1, nullptr);
      bits::reverseBits(&reversedLastByte_, 1);
      remainingBits_ = 8 - bitsToSkip;
    }
  }
}

void BooleanRleDecoder::next(
    char* data,
    uint64_t numValues,
    const uint64_t* nulls) {
  skipPending();

  uint64_t nonNulls = numValues;
  if (nulls) {
    nonNulls = bits::countNonNulls(nulls, 0, numValues);
  }

  const uint32_t outputBytes = (numValues + 7) / 8;
  if (nonNulls == 0) {
    ::memset(data, 0, outputBytes);
    return;
  }

  if (remainingBits_ >= nonNulls) {
    // The remaining bits from last round is enough for this round, and we don't
    // need to read new data. Since remainingBits should be less than or equal
    // to 8, therefore nonNulls must be less than 8.
    data[0] =
        reversedLastByte_ >> (8 - remainingBits_) & 0xff >> (8 - nonNulls);
    remainingBits_ -= nonNulls;
  } else {
    // Put the remaining bits, if any, into previousByte.
    uint8_t previousByte{0};
    if (remainingBits_ > 0) {
      previousByte = reversedLastByte_ >> (8 - remainingBits_);
    }

    // We need to read in (nonNulls - remainingBits) values and it must be a
    // positive number if nonNulls is positive
    const uint64_t bytesRead = bits::divRoundUp(nonNulls - remainingBits_, 8);
    ByteRleDecoder::next(data, bytesRead, nullptr);

    bits::reverseBits(reinterpret_cast<uint8_t*>(data), bytesRead);
    reversedLastByte_ = data[bytesRead - 1];

    // Now shift the data in place
    if (remainingBits_ > 0) {
      uint64_t nonNullDWords = nonNulls / 64;
      // Shift 64 bits a time when there're enough data. Note that the data
      // buffer was created 64-bits aligned so there won't be performance
      // degradation shifting it in 64-bit unit.
      for (uint64_t i = 0; i < nonNullDWords; ++i) {
        uint64_t tmp = reinterpret_cast<uint64_t*>(data)[i];
        reinterpret_cast<uint64_t*>(data)[i] =
            previousByte | tmp << remainingBits_; // previousByte is LSB
        previousByte = (tmp >> (64 - remainingBits_)) & 0xff;
      }

      // Shift 8 bits a time for the remaining bits
      const uint64_t nonNullOutputBytes = (nonNulls + 7) / 8;
      for (int32_t i = nonNullDWords * 8; i < nonNullOutputBytes; ++i) {
        uint8_t tmp = data[i]; // already reversed
        data[i] = previousByte | tmp << remainingBits_; // previousByte is LSB
        previousByte = tmp >> (8 - remainingBits_);
      }
    }
    remainingBits_ = bytesRead * 8 + remainingBits_ - nonNulls;
  }

  // Unpack data for nulls.
  if (numValues > nonNulls) {
    bits::scatterBits(nonNulls, numValues, data, nulls, data);
  }

  // Clear the most significant bits in the last byte which will be processed in
  // the next round.
  data[outputBytes - 1] &= 0xff >> (outputBytes * 8 - numValues);
}

std::unique_ptr<BooleanRleDecoder> createBooleanRleDecoder(
    std::unique_ptr<dwio::common::SeekableInputStream> input,
    const EncodingKey& ek) {
  return std::make_unique<BooleanRleDecoder>(std::move(input), ek);
}

} // namespace facebook::velox::dwrf
