/*
 * streams.hpp
 * C++ streams API
 *
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */
#pragma once

#include <servicelib/runtime/context.hpp>
#include <servicelib/runtime/function.hpp>
#include <servicelib/runtime/payload.hpp>
#include <servicelib/runtime/serde/serde.hpp>
#include <servicelib/runtime/serde/serdeimpl.hpp>

namespace servicelib {

template <typename _Tp>
class StreamConsumer {
  friend struct TopologyPrinter;
  template <typename, typename>
  friend class StreamExecutionEnvironment;
  template <typename>
  friend class StreamConsumer;
  friend class StreamBuilderContext;
  template <typename, typename, typename>
  friend class Stream;
  template <typename>
  friend struct StreamBase::stream_delete;

  StreamConsumer& operator=(const StreamConsumer&) = delete;
  StreamConsumer& operator=(StreamConsumer&&) = delete;

 protected:
  serde::StreamSerde<_Tp>* serde_{nullptr};
  std::unique_ptr<serde::StreamSerde<_Tp>> ownedSerde_;

  StreamConsumer() noexcept = default;

  void copyConsumerSettings(const StreamConsumer<_Tp>& other) {
    serde_     = other.serde_;
  }

  // Go: runtime.MakeSerde[R](env) — called plainly, with no null-check, by
  // root streams (no parent to reuse from) and by the output type of
  // type-changing operators (the type is new to the graph at that point).
  // Always produces a usable (non-null) serde, falling back to StubSerde<_Tp>
  // when no DefaultSerdeFactory<_Tp> specialization is registered.
  void resolveDefaultSerde() {
    ownedSerde_ = serde::MakeDefaultStreamSerde<_Tp>();
    serde_      = ownedSerde_.get();
  }

  // Go: runtime.MakeKeyValueSerde[K, V](env) — used only by KeyBy, whose
  // _Tp is StreamKeyValue<K, V>. Resolves K's and V's serdes independently,
  // always producing a usable (non-null) key-value serde.
  template <typename K, typename V>
  void resolveDefaultKeyValueSerde() {
    ownedSerde_ = serde::MakeDefaultStreamKeyValueSerde<K, V, _Tp>();
    serde_      = ownedSerde_.get();
  }

  virtual const StreamBase& getConsumer() const = 0;
  virtual StreamBase& getConsumer() = 0;
  virtual bool hasConsumer() const noexcept = 0;
  virtual const StreamBase& getBase() const noexcept = 0;
  virtual StreamBase& getBase() noexcept = 0;
  virtual const std::string_view& getType() const = 0;
  virtual std::string getCode() const = 0;
  virtual size_t buildTopology(StreamBuilderContext& ctx, size_t id, StreamBuilderContext::TIdsList* splitConsumerIds,
                               bool skip) = 0;
  virtual void verifyTopology(StreamVerifyContext& ctx) const = 0;
  virtual void printTopology(TopologyPrinter& tp, std::unordered_set<size_t>& visited) const = 0;
  virtual ~StreamConsumer() = default;

 public:
  virtual void consume(MessageContext ctx, Payload<_Tp> payload) = 0;
  virtual const std::string& getName() const noexcept = 0;
  virtual size_t getId() const noexcept = 0;

  serde::StreamSerde<_Tp>* getSerde() const noexcept { return serde_; }
};

template <typename T, typename C>
class Collector final : public NotCopyableOrMovable {
  template <typename, typename, typename>
  friend class Stream;

  C& _collector;
  bool* emitted_{};

 protected:
  explicit Collector(C& collector, bool* emitted = nullptr)
      : _collector(collector), emitted_(emitted) {}

 public:
  void out(MessageContext ctx, T&& v) {
    if (emitted_) *emitted_ = true;
    _collector.produce(ctx, std::forward<T>(v));
  }
  void out(MessageContext ctx, const T& v) {
    if (emitted_) *emitted_ = true;
    _collector.produce(ctx, v);
  }
};

template <typename T>
class Collector<T, void> final : public NotCopyableOrMovable {
  template <typename, typename, typename>
  friend class Stream;

 public:
  void out([[maybe_unused]] MessageContext ctx, [[maybe_unused]] T&& v)      {}
  void out([[maybe_unused]] MessageContext ctx, [[maybe_unused]] const T& v) {}
};

template <typename _Kp, typename _Vp>
class StreamKeyValue final : public std::pair<_Kp, _Vp> {
 public:
  using std::pair<_Kp, _Vp>::pair;

  static StreamKeyValue make(const _Kp& key, const _Vp& value) { return StreamKeyValue(key, value); }

  static StreamKeyValue make(_Kp&& key, _Vp&& value) {
    return StreamKeyValue(std::forward<_Kp>(key), std::forward<_Vp>(value));
  }
};

template <typename _Kp, typename _Vp>
StreamKeyValue<_Kp, _Vp> make_key_value(const _Kp& key, const _Vp& value) {
  return StreamKeyValue<_Kp, _Vp>(key, value);
}

template <typename _Kp, typename _Vp>
StreamKeyValue<_Kp, _Vp> make_key_value(_Kp&& key, _Vp&& value) {
  return StreamKeyValue<_Kp, _Vp>(std::forward<_Kp>(key), std::forward<_Vp>(value));
}

template <typename K, typename V>
using KeyValueType = StreamKeyValue<K, V>;

}  // namespace servicelib
