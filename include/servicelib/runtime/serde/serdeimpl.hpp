/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 * Licensed under the MIT License. See the
 * [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */
#pragma once

#include <bit>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include <servicelib/runtime/serde/serde.hpp>

namespace servicelib::serde {

class SerdeError : public std::runtime_error {
 public:
  SerdeError(std::string message, std::size_t offset = 0)
      : std::runtime_error(std::move(message)), offset_(offset) {}

  [[nodiscard]] std::size_t offset() const noexcept { return offset_; }

 private:
  std::size_t offset_;
};

struct SerdeLimits {
  std::size_t maxStringBytes = std::numeric_limits<std::size_t>::max();
  std::size_t maxBytes = std::numeric_limits<std::size_t>::max();
  std::size_t maxContainerElements = std::numeric_limits<std::size_t>::max();
  std::size_t maxTotalBytes = std::numeric_limits<std::size_t>::max();
};

namespace detail {

inline constexpr std::size_t kSizeBytes = 8;

inline std::size_t checked_add(std::size_t lhs, std::size_t rhs,
                               std::string_view what) {
  if (rhs > std::numeric_limits<std::size_t>::max() - lhs) {
    throw SerdeError("serde size overflow while encoding " + std::string(what));
  }
  return lhs + rhs;
}

inline void reserve_more(SerdeData& output, std::size_t additional) {
  if (additional > output.max_size() - output.size()) {
    throw SerdeError("serde output size overflow", output.size());
  }
  output.reserve(output.size() + additional);
}

inline void put_u8(SerdeData& output, std::uint8_t value) {
  output.push_back(static_cast<std::byte>(value));
}

inline void put_u16_be(SerdeData& output, std::uint16_t value) {
  output.push_back(static_cast<std::byte>((value >> 8) & 0xffu));
  output.push_back(static_cast<std::byte>(value & 0xffu));
}

inline void put_u32_be(SerdeData& output, std::uint32_t value) {
  output.push_back(static_cast<std::byte>((value >> 24) & 0xffu));
  output.push_back(static_cast<std::byte>((value >> 16) & 0xffu));
  output.push_back(static_cast<std::byte>((value >> 8) & 0xffu));
  output.push_back(static_cast<std::byte>(value & 0xffu));
}

inline void put_u64_be(SerdeData& output, std::uint64_t value) {
  for (int index = 7; index >= 0; --index) {
    output.push_back(static_cast<std::byte>((value >> (index * 8)) & 0xffu));
  }
}

inline void set_u64_be(SerdeData& output, std::size_t offset,
                       std::uint64_t value) {
  if (offset > output.size() || output.size() - offset < kSizeBytes) {
    throw SerdeError("invalid serde frame offset", offset);
  }
  for (int index = 7; index >= 0; --index) {
    output[offset + static_cast<std::size_t>(7 - index)] =
        static_cast<std::byte>((value >> (index * 8)) & 0xffu);
  }
}

inline void put_size(SerdeData& output, std::size_t size) {
  put_u64_be(output, static_cast<std::uint64_t>(size));
}

class Reader final {
 public:
  explicit Reader(SerdeView data, std::size_t maxTotalBytes =
                                      std::numeric_limits<std::size_t>::max())
      : data_(data) {
    if (data.size() > maxTotalBytes) {
      throw SerdeError("serde input exceeds configured limit");
    }
  }

  [[nodiscard]] std::size_t offset() const noexcept { return offset_; }
  [[nodiscard]] std::size_t remaining() const noexcept {
    return data_.size() - offset_;
  }

  SerdeView read(std::size_t size, std::string_view what) {
    if (size > remaining()) {
      throw SerdeError("serde underflow while reading " + std::string(what),
                       offset_);
    }
    const auto result = data_.subspan(offset_, size);
    offset_ += size;
    return result;
  }

  std::uint8_t read_u8() {
    return std::to_integer<std::uint8_t>(read(1, "u8")[0]);
  }

  std::uint16_t read_u16_be() {
    const auto bytes = read(2, "u16");
    return static_cast<std::uint16_t>(
        (std::to_integer<std::uint16_t>(bytes[0]) << 8) |
        std::to_integer<std::uint16_t>(bytes[1]));
  }

  std::uint32_t read_u32_be() {
    const auto bytes = read(4, "u32");
    return (std::to_integer<std::uint32_t>(bytes[0]) << 24) |
           (std::to_integer<std::uint32_t>(bytes[1]) << 16) |
           (std::to_integer<std::uint32_t>(bytes[2]) << 8) |
           std::to_integer<std::uint32_t>(bytes[3]);
  }

  std::uint64_t read_u64_be() {
    const auto bytes = read(8, "u64");
    std::uint64_t value = 0;
    for (const auto byte : bytes) {
      value = (value << 8) | std::to_integer<std::uint64_t>(byte);
    }
    return value;
  }

  std::size_t read_size(std::size_t maximum, std::string_view what) {
    const std::uint64_t encoded = read_u64_be();
    if (encoded > std::numeric_limits<std::size_t>::max()) {
      throw SerdeError("serde " + std::string(what) + " does not fit size_t",
                       offset_ - kSizeBytes);
    }
    const auto size = static_cast<std::size_t>(encoded);
    if (size > maximum) {
      throw SerdeError(
          "serde " + std::string(what) + " exceeds configured limit",
          offset_ - kSizeBytes);
    }
    return size;
  }

 private:
  SerdeView data_;
  std::size_t offset_ = 0;
};

template <typename T, typename SerializerType>
SerdeData serialize_fresh(const SerializerType& serializer, const T& value,
                          std::size_t reserve = 0) {
  SerdeData output;
  output.reserve(reserve);
  serializer.SerializeTo(output, value);
  return output;
}

template <typename T>
std::shared_ptr<const Serde<T>> require_serde(
    std::shared_ptr<const Serde<T>> serde) {
  if (!serde) {
    throw std::invalid_argument("serde must not be null");
  }
  return serde;
}

}  // namespace detail

struct BoolSerde final : Serde<bool> {
  bool IsStub() const noexcept override { return false; }
  SerdeData Serialize(const bool& value) const override {
    return detail::serialize_fresh(*this, value, 1);
  }
  void SerializeTo(SerdeData& output, const bool& value) const override {
    detail::put_u8(output, value ? 1u : 0u);
  }
  bool Deserialize(SerdeView data) const override {
    detail::Reader reader(data);
    return reader.read_u8() != 0;
  }
};

struct Int8Serde final : Serde<std::int8_t> {
  bool IsStub() const noexcept override { return false; }
  SerdeData Serialize(const std::int8_t& value) const override {
    return detail::serialize_fresh(*this, value, 1);
  }
  void SerializeTo(SerdeData& output, const std::int8_t& value) const override {
    detail::put_u8(output, std::bit_cast<std::uint8_t>(value));
  }
  std::int8_t Deserialize(SerdeView data) const override {
    detail::Reader reader(data);
    return std::bit_cast<std::int8_t>(reader.read_u8());
  }
};

struct UInt8Serde final : Serde<std::uint8_t> {
  bool IsStub() const noexcept override { return false; }
  SerdeData Serialize(const std::uint8_t& value) const override {
    return detail::serialize_fresh(*this, value, 1);
  }
  void SerializeTo(SerdeData& output,
                   const std::uint8_t& value) const override {
    detail::put_u8(output, value);
  }
  std::uint8_t Deserialize(SerdeView data) const override {
    detail::Reader reader(data);
    return reader.read_u8();
  }
};

struct Int16Serde final : Serde<std::int16_t> {
  bool IsStub() const noexcept override { return false; }
  SerdeData Serialize(const std::int16_t& value) const override {
    return detail::serialize_fresh(*this, value, 2);
  }
  void SerializeTo(SerdeData& output,
                   const std::int16_t& value) const override {
    detail::put_u16_be(output, std::bit_cast<std::uint16_t>(value) ^ 0x8000u);
  }
  std::int16_t Deserialize(SerdeView data) const override {
    detail::Reader reader(data);
    return std::bit_cast<std::int16_t>(static_cast<std::uint16_t>(
        reader.read_u16_be() ^ static_cast<std::uint16_t>(0x8000u)));
  }
};

struct UInt16Serde final : Serde<std::uint16_t> {
  bool IsStub() const noexcept override { return false; }
  SerdeData Serialize(const std::uint16_t& value) const override {
    return detail::serialize_fresh(*this, value, 2);
  }
  void SerializeTo(SerdeData& output,
                   const std::uint16_t& value) const override {
    detail::put_u16_be(output, value);
  }
  std::uint16_t Deserialize(SerdeView data) const override {
    detail::Reader reader(data);
    return reader.read_u16_be();
  }
};

struct Int32Serde final : Serde<std::int32_t> {
  bool IsStub() const noexcept override { return false; }
  SerdeData Serialize(const std::int32_t& value) const override {
    return detail::serialize_fresh(*this, value, 4);
  }
  void SerializeTo(SerdeData& output,
                   const std::int32_t& value) const override {
    detail::put_u32_be(output,
                       std::bit_cast<std::uint32_t>(value) ^ 0x80000000u);
  }
  std::int32_t Deserialize(SerdeView data) const override {
    detail::Reader reader(data);
    return std::bit_cast<std::int32_t>(reader.read_u32_be() ^ 0x80000000u);
  }
};

struct UInt32Serde final : Serde<std::uint32_t> {
  bool IsStub() const noexcept override { return false; }
  SerdeData Serialize(const std::uint32_t& value) const override {
    return detail::serialize_fresh(*this, value, 4);
  }
  void SerializeTo(SerdeData& output,
                   const std::uint32_t& value) const override {
    detail::put_u32_be(output, value);
  }
  std::uint32_t Deserialize(SerdeView data) const override {
    detail::Reader reader(data);
    return reader.read_u32_be();
  }
};

struct Int64Serde final : Serde<std::int64_t> {
  bool IsStub() const noexcept override { return false; }
  SerdeData Serialize(const std::int64_t& value) const override {
    return detail::serialize_fresh(*this, value, 8);
  }
  void SerializeTo(SerdeData& output,
                   const std::int64_t& value) const override {
    detail::put_u64_be(
        output, std::bit_cast<std::uint64_t>(value) ^ 0x8000000000000000ull);
  }
  std::int64_t Deserialize(SerdeView data) const override {
    detail::Reader reader(data);
    return std::bit_cast<std::int64_t>(reader.read_u64_be() ^
                                       0x8000000000000000ull);
  }
};

struct UInt64Serde final : Serde<std::uint64_t> {
  bool IsStub() const noexcept override { return false; }
  SerdeData Serialize(const std::uint64_t& value) const override {
    return detail::serialize_fresh(*this, value, 8);
  }
  void SerializeTo(SerdeData& output,
                   const std::uint64_t& value) const override {
    detail::put_u64_be(output, value);
  }
  std::uint64_t Deserialize(SerdeView data) const override {
    detail::Reader reader(data);
    return reader.read_u64_be();
  }
};

// Go int and uint use the same fixed 64-bit wire representation.
using IntSerde = Int64Serde;
using UIntSerde = UInt64Serde;

struct RuneSerde final : Serde<char32_t> {
  bool IsStub() const noexcept override { return false; }
  SerdeData Serialize(const char32_t& value) const override {
    return detail::serialize_fresh(*this, value, 4);
  }
  void SerializeTo(SerdeData& output, const char32_t& value) const override {
    detail::put_u32_be(output, static_cast<std::uint32_t>(value));
  }
  char32_t Deserialize(SerdeView data) const override {
    detail::Reader reader(data);
    return static_cast<char32_t>(reader.read_u32_be());
  }
};

struct Float32Serde final : Serde<float> {
  static_assert(sizeof(float) == sizeof(std::uint32_t));
  static_assert(std::numeric_limits<float>::is_iec559);

  bool IsStub() const noexcept override { return false; }
  SerdeData Serialize(const float& value) const override {
    return detail::serialize_fresh(*this, value, 4);
  }
  void SerializeTo(SerdeData& output, const float& value) const override {
    detail::put_u32_be(output, std::bit_cast<std::uint32_t>(value));
  }
  float Deserialize(SerdeView data) const override {
    detail::Reader reader(data);
    return std::bit_cast<float>(reader.read_u32_be());
  }
};

struct Float64Serde final : Serde<double> {
  static_assert(sizeof(double) == sizeof(std::uint64_t));
  static_assert(std::numeric_limits<double>::is_iec559);

  bool IsStub() const noexcept override { return false; }
  SerdeData Serialize(const double& value) const override {
    return detail::serialize_fresh(*this, value, 8);
  }
  void SerializeTo(SerdeData& output, const double& value) const override {
    detail::put_u64_be(output, std::bit_cast<std::uint64_t>(value));
  }
  double Deserialize(SerdeView data) const override {
    detail::Reader reader(data);
    return std::bit_cast<double>(reader.read_u64_be());
  }
};

class StringSerde final : public Serde<std::string> {
 public:
  explicit StringSerde(SerdeLimits limits = {}) : limits_(limits) {}

  bool IsStub() const noexcept override { return false; }
  SerdeData Serialize(const std::string& value) const override {
    return detail::serialize_fresh(
        *this, value,
        detail::checked_add(detail::kSizeBytes, value.size(), "string"));
  }
  void SerializeTo(SerdeData& output, const std::string& value) const override {
    if (value.size() > limits_.maxStringBytes) {
      throw SerdeError("string exceeds configured serde limit");
    }
    detail::reserve_more(output, detail::checked_add(detail::kSizeBytes,
                                                     value.size(), "string"));
    detail::put_size(output, value.size());
    const auto* bytes = reinterpret_cast<const std::byte*>(value.data());
    output.insert(output.end(), bytes, bytes + value.size());
  }
  std::string Deserialize(SerdeView data) const override {
    detail::Reader reader(data, limits_.maxTotalBytes);
    const auto length =
        reader.read_size(limits_.maxStringBytes, "string length");
    const auto bytes = reader.read(length, "string");
    return {reinterpret_cast<const char*>(bytes.data()), bytes.size()};
  }

 private:
  SerdeLimits limits_;
};

template <typename Byte>
  requires(std::same_as<Byte, std::byte> || std::same_as<Byte, std::uint8_t>)
class BasicBytesSerde final : public Serde<std::vector<Byte>> {
 public:
  explicit BasicBytesSerde(SerdeLimits limits = {}) : limits_(limits) {}

  bool IsStub() const noexcept override { return false; }
  SerdeData Serialize(const std::vector<Byte>& value) const override {
    return detail::serialize_fresh(
        *this, value,
        detail::checked_add(detail::kSizeBytes, value.size(), "bytes"));
  }
  void SerializeTo(SerdeData& output,
                   const std::vector<Byte>& value) const override {
    if (value.size() > limits_.maxBytes) {
      throw SerdeError("byte sequence exceeds configured serde limit");
    }
    detail::reserve_more(
        output, detail::checked_add(detail::kSizeBytes, value.size(), "bytes"));
    detail::put_size(output, value.size());
    for (const auto byte : value) {
      if constexpr (std::same_as<Byte, std::byte>) {
        output.push_back(byte);
      } else {
        output.push_back(static_cast<std::byte>(byte));
      }
    }
  }
  std::vector<Byte> Deserialize(SerdeView data) const override {
    detail::Reader reader(data, limits_.maxTotalBytes);
    const auto length = reader.read_size(limits_.maxBytes, "bytes length");
    const auto bytes = reader.read(length, "bytes");
    std::vector<Byte> result;
    result.reserve(length);
    for (const auto byte : bytes) {
      if constexpr (std::same_as<Byte, std::byte>) {
        result.push_back(byte);
      } else {
        result.push_back(std::to_integer<std::uint8_t>(byte));
      }
    }
    return result;
  }

 private:
  SerdeLimits limits_;
};

using BytesSerde = BasicBytesSerde<std::byte>;
using UInt8ArraySerde = BasicBytesSerde<std::uint8_t>;

template <typename T, typename ElementSerde, std::size_t ElementSize>
class FixedSizeArraySerde final : public Serde<std::vector<T>> {
 public:
  explicit FixedSizeArraySerde(SerdeLimits limits = {}) : limits_(limits) {}

  bool IsStub() const noexcept override { return false; }
  SerdeData Serialize(const std::vector<T>& value) const override {
    if (value.size() >
        (std::numeric_limits<std::size_t>::max() - detail::kSizeBytes) /
            ElementSize) {
      throw SerdeError("fixed-size array output size overflow");
    }
    return detail::serialize_fresh(
        *this, value, detail::kSizeBytes + value.size() * ElementSize);
  }
  void SerializeTo(SerdeData& output,
                   const std::vector<T>& value) const override {
    if (value.size() > limits_.maxContainerElements) {
      throw SerdeError("array exceeds configured element limit");
    }
    detail::put_size(output, value.size());
    ElementSerde elementSerde;
    for (const auto& valueItem : value) {
      if constexpr (std::same_as<T, bool>) {
        const bool item = valueItem;
        elementSerde.SerializeTo(output, item);
      } else {
        elementSerde.SerializeTo(output, valueItem);
      }
    }
  }
  std::vector<T> Deserialize(SerdeView data) const override {
    detail::Reader reader(data, limits_.maxTotalBytes);
    const auto count =
        reader.read_size(limits_.maxContainerElements, "array count");
    if (count > reader.remaining() / ElementSize) {
      throw SerdeError("fixed-size array payload is truncated",
                       reader.offset());
    }
    std::vector<T> result;
    result.reserve(count);
    ElementSerde elementSerde;
    for (std::size_t index = 0; index < count; ++index) {
      result.push_back(elementSerde.Deserialize(
          reader.read(ElementSize, "fixed-size array element")));
    }
    return result;
  }

 private:
  SerdeLimits limits_;
};

using BoolArraySerde = FixedSizeArraySerde<bool, BoolSerde, 1>;
using Int16ArraySerde = FixedSizeArraySerde<std::int16_t, Int16Serde, 2>;
using UInt16ArraySerde = FixedSizeArraySerde<std::uint16_t, UInt16Serde, 2>;
using Int32ArraySerde = FixedSizeArraySerde<std::int32_t, Int32Serde, 4>;
using UInt32ArraySerde = FixedSizeArraySerde<std::uint32_t, UInt32Serde, 4>;
using Int64ArraySerde = FixedSizeArraySerde<std::int64_t, Int64Serde, 8>;
using UInt64ArraySerde = FixedSizeArraySerde<std::uint64_t, UInt64Serde, 8>;
using Float32ArraySerde = FixedSizeArraySerde<float, Float32Serde, 4>;
using Float64ArraySerde = FixedSizeArraySerde<double, Float64Serde, 8>;
using IntArraySerde = Int64ArraySerde;
using UIntArraySerde = UInt64ArraySerde;

class StringArraySerde final : public Serde<std::vector<std::string>> {
 public:
  explicit StringArraySerde(SerdeLimits limits = {}) : limits_(limits) {}

  bool IsStub() const noexcept override { return false; }
  SerdeData Serialize(const std::vector<std::string>& value) const override {
    return detail::serialize_fresh(*this, value);
  }
  void SerializeTo(SerdeData& output,
                   const std::vector<std::string>& value) const override {
    if (value.size() > limits_.maxContainerElements) {
      throw SerdeError("string array exceeds configured element limit");
    }
    detail::put_size(output, value.size());
    StringSerde stringSerde(limits_);
    for (const auto& item : value) {
      stringSerde.SerializeTo(output, item);
    }
  }
  std::vector<std::string> Deserialize(SerdeView data) const override {
    detail::Reader reader(data, limits_.maxTotalBytes);
    const auto count =
        reader.read_size(limits_.maxContainerElements, "string array count");
    std::vector<std::string> result;
    result.reserve(count);
    for (std::size_t index = 0; index < count; ++index) {
      const auto length =
          reader.read_size(limits_.maxStringBytes, "string length");
      const auto bytes = reader.read(length, "string array element");
      result.emplace_back(reinterpret_cast<const char*>(bytes.data()),
                          bytes.size());
    }
    return result;
  }

 private:
  SerdeLimits limits_;
};

template <typename T>
class ArraySerde final : public Serde<std::vector<T>> {
 public:
  explicit ArraySerde(std::shared_ptr<const Serde<T>> valueSerde,
                      SerdeLimits limits = {})
      : valueSerde_(detail::require_serde(std::move(valueSerde))),
        limits_(limits) {}

  bool IsStub() const noexcept override { return valueSerde_->IsStub(); }
  SerdeData Serialize(const std::vector<T>& value) const override {
    return detail::serialize_fresh(*this, value);
  }
  void SerializeTo(SerdeData& output,
                   const std::vector<T>& value) const override {
    if (value.size() > limits_.maxContainerElements) {
      throw SerdeError("array exceeds configured element limit");
    }
    detail::put_size(output, value.size());
    for (const auto& item : value) {
      const std::size_t headerOffset = output.size();
      detail::put_u64_be(output, 0);
      const std::size_t valueOffset = output.size();
      valueSerde_->SerializeTo(output, item);
      detail::set_u64_be(output, headerOffset, output.size() - valueOffset);
    }
  }
  std::vector<T> Deserialize(SerdeView data) const override {
    detail::Reader reader(data, limits_.maxTotalBytes);
    const auto count =
        reader.read_size(limits_.maxContainerElements, "array count");
    std::vector<T> result;
    result.reserve(count);
    for (std::size_t index = 0; index < count; ++index) {
      const auto length =
          reader.read_size(limits_.maxTotalBytes, "array element length");
      result.push_back(valueSerde_->Deserialize(
          reader.read(length, "array element payload")));
    }
    return result;
  }

 private:
  std::shared_ptr<const Serde<T>> valueSerde_;
  SerdeLimits limits_;
};

template <typename K, typename V, typename Map = std::unordered_map<K, V>>
class MapSerde final : public Serde<Map> {
 public:
  MapSerde(std::shared_ptr<const Serde<std::vector<K>>> keyArraySerde,
           std::shared_ptr<const Serde<std::vector<V>>> valueArraySerde,
           SerdeLimits limits = {})
      : keyArraySerde_(detail::require_serde(std::move(keyArraySerde))),
        valueArraySerde_(detail::require_serde(std::move(valueArraySerde))),
        limits_(limits) {}

  bool IsStub() const noexcept override {
    return keyArraySerde_->IsStub() || valueArraySerde_->IsStub();
  }
  SerdeData Serialize(const Map& value) const override {
    return detail::serialize_fresh(*this, value);
  }
  void SerializeTo(SerdeData& output, const Map& value) const override {
    if (value.size() > limits_.maxContainerElements) {
      throw SerdeError("map exceeds configured element limit");
    }
    std::vector<K> keys;
    std::vector<V> values;
    keys.reserve(value.size());
    values.reserve(value.size());
    for (const auto& [key, mapValue] : value) {
      keys.push_back(key);
      values.push_back(mapValue);
    }

    const std::size_t keysHeaderOffset = output.size();
    detail::put_u64_be(output, 0);
    const std::size_t keysOffset = output.size();
    keyArraySerde_->SerializeTo(output, keys);
    detail::set_u64_be(output, keysHeaderOffset, output.size() - keysOffset);

    const std::size_t valuesHeaderOffset = output.size();
    detail::put_u64_be(output, 0);
    const std::size_t valuesOffset = output.size();
    valueArraySerde_->SerializeTo(output, values);
    detail::set_u64_be(output, valuesHeaderOffset,
                       output.size() - valuesOffset);
  }
  Map Deserialize(SerdeView data) const override {
    detail::Reader reader(data, limits_.maxTotalBytes);
    const auto keysLength =
        reader.read_size(limits_.maxTotalBytes, "map keys length");
    auto keys = keyArraySerde_->Deserialize(
        reader.read(keysLength, "map keys payload"));
    const auto valuesLength =
        reader.read_size(limits_.maxTotalBytes, "map values length");
    auto values = valueArraySerde_->Deserialize(
        reader.read(valuesLength, "map values payload"));
    if (keys.size() != values.size()) {
      throw SerdeError("map key/value count mismatch", reader.offset());
    }
    if (keys.size() > limits_.maxContainerElements) {
      throw SerdeError("map exceeds configured element limit");
    }
    Map result;
    if constexpr (requires(Map& map, std::size_t size) { map.reserve(size); }) {
      result.reserve(keys.size());
    }
    for (std::size_t index = 0; index < keys.size(); ++index) {
      result.insert_or_assign(std::move(keys[index]), std::move(values[index]));
    }
    return result;
  }

 private:
  std::shared_ptr<const Serde<std::vector<K>>> keyArraySerde_;
  std::shared_ptr<const Serde<std::vector<V>>> valueArraySerde_;
  SerdeLimits limits_;
};

template <typename K, typename V,
          typename KeyValue = servicelib::StreamKeyValue<K, V>>
class StreamKeyValueSerdeImpl final : public StreamKeyValueSerde<KeyValue> {
 public:
  StreamKeyValueSerdeImpl(std::shared_ptr<const Serde<K>> keySerde,
                          std::shared_ptr<const Serde<V>> valueSerde,
                          SerdeLimits limits = {})
      : keySerde_(detail::require_serde(std::move(keySerde))),
        valueSerde_(detail::require_serde(std::move(valueSerde))),
        limits_(limits) {}

  bool IsKeyValue() const noexcept override { return true; }
  const Serializer* KeySerializer() const override { return keySerde_.get(); }
  const Serializer* ValueSerializer() const override {
    return valueSerde_.get();
  }
  SerdeData SerializeKey(const KeyValue& value) const override {
    return keySerde_->Serialize(value.first);
  }
  SerdeData SerializeValue(const KeyValue& value) const override {
    return valueSerde_->Serialize(value.second);
  }
  KeyValue DeserializeKeyValue(SerdeView key, SerdeView value) const override {
    return KeyValue{keySerde_->Deserialize(key),
                    valueSerde_->Deserialize(value)};
  }
  SerdeData Serialize(const KeyValue& value) const override {
    SerdeData output;
    const std::size_t keyHeaderOffset = output.size();
    detail::put_u64_be(output, 0);
    const std::size_t keyOffset = output.size();
    keySerde_->SerializeTo(output, value.first);
    detail::set_u64_be(output, keyHeaderOffset, output.size() - keyOffset);

    const std::size_t valueHeaderOffset = output.size();
    detail::put_u64_be(output, 0);
    const std::size_t valueOffset = output.size();
    valueSerde_->SerializeTo(output, value.second);
    detail::set_u64_be(output, valueHeaderOffset, output.size() - valueOffset);
    return output;
  }
  KeyValue Deserialize(SerdeView data) const override {
    detail::Reader reader(data, limits_.maxTotalBytes);
    const auto keyLength =
        reader.read_size(limits_.maxTotalBytes, "key length");
    auto key = keySerde_->Deserialize(reader.read(keyLength, "key payload"));
    const auto valueLength =
        reader.read_size(limits_.maxTotalBytes, "value length");
    auto value =
        valueSerde_->Deserialize(reader.read(valueLength, "value payload"));
    return KeyValue{std::move(key), std::move(value)};
  }

 private:
  std::shared_ptr<const Serde<K>> keySerde_;
  std::shared_ptr<const Serde<V>> valueSerde_;
  SerdeLimits limits_;
};

template <typename K, typename V,
          typename KeyValue = servicelib::StreamKeyValue<K, V>>
std::unique_ptr<StreamKeyValueSerde<KeyValue>> MakeStreamKeyValueSerde(
    std::shared_ptr<const Serde<K>> keySerde,
    std::shared_ptr<const Serde<V>> valueSerde, SerdeLimits limits = {}) {
  return std::make_unique<StreamKeyValueSerdeImpl<K, V, KeyValue>>(
      std::move(keySerde), std::move(valueSerde), limits);
}

template <typename T>
struct DefaultSerdeFactory;

template <typename T>
std::shared_ptr<const Serde<T>> MakeDefaultSerde(SerdeLimits limits = {}) {
  return DefaultSerdeFactory<T>::Make(limits);
}

namespace detail {

// SFINAE detection of whether DefaultSerdeFactory<T> has been specialized for
// T (the primary template is declared but never defined, so instantiating it
// for an unspecialized T is a substitution failure here, not a hard error).
template <typename T, typename = void>
struct has_default_serde_factory : std::false_type {};

template <typename T>
struct has_default_serde_factory<T, std::void_t<decltype(sizeof(DefaultSerdeFactory<T>))>>
    : std::true_type {};

}  // namespace detail

template <typename T>
inline constexpr bool kHasDefaultSerde = detail::has_default_serde_factory<T>::value;

namespace detail {

// Go: rt.getSerializer(tp) with fallback to serde.MakeStubSerde[T]() —
// resolves a real serde when a DefaultSerdeFactory<T> specialization exists,
// otherwise falls back to a StubSerde<T> (throws only if actually used for
// (de)serialization, never fails to compile or resolve).
template <typename T>
std::shared_ptr<const Serde<T>> resolve_or_stub(SerdeLimits limits) {
  if constexpr (kHasDefaultSerde<T>) {
    return MakeDefaultSerde<T>(limits);
  } else {
    return std::make_shared<StubSerde<T>>();
  }
}

}  // namespace detail

// Go: runtime.MakeSerde[T](env) — always resolves a usable (non-null) serde
// for T, falling back to a StubSerde<T> exactly like Go falls back to
// serde.MakeStubSerde[T]() when no real serializer is registered for T.
template <typename T>
std::unique_ptr<StreamSerde<T>> MakeDefaultStreamSerde(SerdeLimits limits = {}) {
  return MakeStreamSerde(detail::resolve_or_stub<T>(limits));
}

// Go: runtime.MakeKeyValueSerde[K, V](env) — resolves K's and V's serdes
// independently (falling back to StubSerde for either half individually
// rather than failing outright), then combines them into a key-value serde.
template <typename K, typename V, typename KeyValue = servicelib::StreamKeyValue<K, V>>
std::unique_ptr<StreamKeyValueSerde<KeyValue>> MakeDefaultStreamKeyValueSerde(
    SerdeLimits limits = {}) {
  return MakeStreamKeyValueSerde<K, V, KeyValue>(detail::resolve_or_stub<K>(limits),
                                                 detail::resolve_or_stub<V>(limits), limits);
}

#define STREAM_DEFINE_DEFAULT_SERDE(cpp_type, serde_type)             \
  template <>                                                         \
  struct DefaultSerdeFactory<cpp_type> final {                        \
    static std::shared_ptr<const Serde<cpp_type>> Make(SerdeLimits) { \
      return std::make_shared<serde_type>();                          \
    }                                                                 \
  }

STREAM_DEFINE_DEFAULT_SERDE(bool, BoolSerde);
STREAM_DEFINE_DEFAULT_SERDE(std::int8_t, Int8Serde);
STREAM_DEFINE_DEFAULT_SERDE(std::uint8_t, UInt8Serde);
STREAM_DEFINE_DEFAULT_SERDE(std::int16_t, Int16Serde);
STREAM_DEFINE_DEFAULT_SERDE(std::uint16_t, UInt16Serde);
STREAM_DEFINE_DEFAULT_SERDE(std::int32_t, Int32Serde);
STREAM_DEFINE_DEFAULT_SERDE(std::uint32_t, UInt32Serde);
STREAM_DEFINE_DEFAULT_SERDE(std::int64_t, Int64Serde);
STREAM_DEFINE_DEFAULT_SERDE(std::uint64_t, UInt64Serde);
STREAM_DEFINE_DEFAULT_SERDE(float, Float32Serde);
STREAM_DEFINE_DEFAULT_SERDE(double, Float64Serde);
STREAM_DEFINE_DEFAULT_SERDE(char32_t, RuneSerde);

#undef STREAM_DEFINE_DEFAULT_SERDE

template <>
struct DefaultSerdeFactory<std::string> final {
  static std::shared_ptr<const Serde<std::string>> Make(SerdeLimits limits) {
    return std::make_shared<StringSerde>(limits);
  }
};

template <>
struct DefaultSerdeFactory<SerdeData> final {
  static std::shared_ptr<const Serde<SerdeData>> Make(SerdeLimits limits) {
    return std::make_shared<BytesSerde>(limits);
  }
};

template <typename T>
struct DefaultSerdeFactory<std::vector<T>> final {
  static std::shared_ptr<const Serde<std::vector<T>>> Make(SerdeLimits limits) {
    return std::make_shared<ArraySerde<T>>(MakeDefaultSerde<T>(limits), limits);
  }
};

#define STREAM_DEFINE_DEFAULT_ARRAY_SERDE(cpp_type, serde_type)      \
  template <>                                                        \
  struct DefaultSerdeFactory<std::vector<cpp_type>> final {          \
    static std::shared_ptr<const Serde<std::vector<cpp_type>>> Make( \
        SerdeLimits limits) {                                        \
      return std::make_shared<serde_type>(limits);                   \
    }                                                                \
  }

STREAM_DEFINE_DEFAULT_ARRAY_SERDE(bool, BoolArraySerde);
STREAM_DEFINE_DEFAULT_ARRAY_SERDE(std::uint8_t, UInt8ArraySerde);
STREAM_DEFINE_DEFAULT_ARRAY_SERDE(std::int16_t, Int16ArraySerde);
STREAM_DEFINE_DEFAULT_ARRAY_SERDE(std::uint16_t, UInt16ArraySerde);
STREAM_DEFINE_DEFAULT_ARRAY_SERDE(std::int32_t, Int32ArraySerde);
STREAM_DEFINE_DEFAULT_ARRAY_SERDE(std::uint32_t, UInt32ArraySerde);
STREAM_DEFINE_DEFAULT_ARRAY_SERDE(std::int64_t, Int64ArraySerde);
STREAM_DEFINE_DEFAULT_ARRAY_SERDE(std::uint64_t, UInt64ArraySerde);
STREAM_DEFINE_DEFAULT_ARRAY_SERDE(float, Float32ArraySerde);
STREAM_DEFINE_DEFAULT_ARRAY_SERDE(double, Float64ArraySerde);
STREAM_DEFINE_DEFAULT_ARRAY_SERDE(std::string, StringArraySerde);

#undef STREAM_DEFINE_DEFAULT_ARRAY_SERDE

template <typename K, typename V, typename Hash, typename Equal,
          typename Allocator>
struct DefaultSerdeFactory<std::unordered_map<K, V, Hash, Equal, Allocator>>
    final {
  using Map = std::unordered_map<K, V, Hash, Equal, Allocator>;

  static std::shared_ptr<const Serde<Map>> Make(SerdeLimits limits) {
    return std::make_shared<MapSerde<K, V, Map>>(
        MakeDefaultSerde<std::vector<K>>(limits),
        MakeDefaultSerde<std::vector<V>>(limits), limits);
  }
};

}  // namespace servicelib::serde
