/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the
 * [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */
#pragma once

#include <any>
#include <cstddef>
#include <memory>
#include <span>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace servicelib {
template <typename K, typename V>
class StreamKeyValue;
}

namespace servicelib::serde {

using SerdeData = std::vector<std::byte>;
using SerdeView = std::span<const std::byte>;

// Go: Serializer — type-erased serialize/deserialize
class Serializer {
 public:
  virtual ~Serializer() = default;
  virtual SerdeData SerializeObj(const std::any& value) const = 0;
  virtual void SerializeObjTo(SerdeData& output, const std::any& value) const {
    auto encoded = SerializeObj(value);
    output.insert(output.end(), encoded.begin(), encoded.end());
  }
  virtual std::any DeserializeObj(SerdeView data) const = 0;
  virtual bool IsStub() const noexcept = 0;
};

// Go: Serde[T] — typed, implements Serializer
template <typename T>
class Serde : public Serializer {
 public:
  virtual SerdeData Serialize(const T& value) const = 0;
  virtual void SerializeTo(SerdeData& output, const T& value) const {
    auto encoded = Serialize(value);
    output.insert(output.end(), encoded.begin(), encoded.end());
  }
  virtual T Deserialize(SerdeView data) const = 0;

  SerdeData SerializeObj(const std::any& value) const final {
    const auto* typed = std::any_cast<T>(&value);
    if (!typed) {
      throw std::invalid_argument("serde object type mismatch");
    }
    return Serialize(*typed);
  }
  void SerializeObjTo(SerdeData& output, const std::any& value) const final {
    const auto* typed = std::any_cast<T>(&value);
    if (!typed) {
      throw std::invalid_argument("serde object type mismatch");
    }
    SerializeTo(output, *typed);
  }
  std::any DeserializeObj(SerdeView data) const final {
    return std::any{Deserialize(data)};
  }
};

// Go: StreamSerializer
class StreamSerializer {
 public:
  virtual ~StreamSerializer() = default;
  virtual bool IsKeyValue() const noexcept = 0;
};

// Go: StreamSerde[T]
template <typename T>
class StreamSerde : public StreamSerializer {
 public:
  virtual const Serializer* ValueSerializer() const = 0;
  virtual SerdeData Serialize(const T& value) const = 0;
  virtual T Deserialize(SerdeView data) const = 0;
};

// Go: StreamKeyValueSerde[T] — for KeyBy streams
template <typename T>
class StreamKeyValueSerde : public StreamSerde<T> {
 public:
  virtual SerdeData SerializeKey(const T& kv) const = 0;
  virtual SerdeData SerializeValue(const T& kv) const = 0;
  virtual const Serializer* KeySerializer() const = 0;
  virtual T DeserializeKeyValue(SerdeView key, SerdeView value) const = 0;
};

// Go: streamSerde[T] — wraps Serde<T> as StreamSerde<T>
template <typename T>
class StreamSerdeImpl final : public StreamSerde<T> {
  std::shared_ptr<const Serde<T>> ownedSerde_;
  const Serde<T>* serde_;

 public:
  explicit StreamSerdeImpl(const Serde<T>* serde) : serde_(serde) {
    if (!serde_) {
      throw std::invalid_argument("stream serde must not be null");
    }
  }
  explicit StreamSerdeImpl(std::shared_ptr<const Serde<T>> serde)
      : ownedSerde_(std::move(serde)), serde_(ownedSerde_.get()) {
    if (!serde_) {
      throw std::invalid_argument("stream serde must not be null");
    }
  }
  bool IsKeyValue() const noexcept override { return false; }
  const Serializer* ValueSerializer() const override { return serde_; }
  SerdeData Serialize(const T& value) const override {
    return serde_->Serialize(value);
  }
  T Deserialize(SerdeView data) const override {
    return serde_->Deserialize(data);
  }
};

template <typename T>
// Non-owning compatibility overload. The caller must keep serde alive for the
// returned stream serde's complete lifetime; prefer the shared_ptr overload.
std::unique_ptr<StreamSerde<T>> MakeStreamSerde(const Serde<T>* serde) {
  return std::make_unique<StreamSerdeImpl<T>>(serde);
}

template <typename T>
std::unique_ptr<StreamSerde<T>> MakeStreamSerde(
    std::shared_ptr<const Serde<T>> serde) {
  return std::make_unique<StreamSerdeImpl<T>>(std::move(serde));
}

// Go: StubSerde[T] — throws on use; used as placeholder when no serde is
// registered
template <typename T>
class StubSerde final : public Serde<T> {
 public:
  bool IsStub() const noexcept override { return true; }
  SerdeData Serialize(const T&) const override {
    throw std::runtime_error("serde for type is not implemented");
  }
  T Deserialize(SerdeView) const override {
    throw std::runtime_error("serde for type is not implemented");
  }
};

}  // namespace servicelib::serde

// Backward-compat alias used by consumer.hpp
namespace servicelib {
using serde_data = servicelib::serde::SerdeData;
}
