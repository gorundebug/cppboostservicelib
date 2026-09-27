#pragma once

#include <servicelib/runtime/context.hpp>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cctype>
#include <charconv>
#include <cstdint>
#include <initializer_list>
#include <map>
#include <memory>
#include <mutex>

#include <boost/beast/http/fields.hpp>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

namespace servicelib::http {

struct CaseInsensitiveLess final {
  using is_transparent = void;
  bool operator()(std::string_view left, std::string_view right) const noexcept {
    return std::lexicographical_compare(
        left.begin(), left.end(), right.begin(), right.end(),
        [](unsigned char lhs, unsigned char rhs) {
          return std::tolower(lhs) < std::tolower(rhs);
        });
  }
};

// Incoming HTTP headers retain Beast's owning storage. Header()/lookup() read
// it directly; map-style iteration or mutation materializes a map on demand.
// Native storage is immutable and may be shared by copies. Writers detach,
// preserving value semantics. Views must not outlive their Headers owner or
// a mutation of that owner.
class Headers final {
 public:
  using Map = std::map<std::string, std::string, CaseInsensitiveLess>;
  using key_type = Map::key_type;
  using mapped_type = Map::mapped_type;
  using value_type = Map::value_type;
  using size_type = Map::size_type;
  using iterator = Map::iterator;
  using const_iterator = Map::const_iterator;
  using reverse_iterator = Map::reverse_iterator;
  using const_reverse_iterator = Map::const_reverse_iterator;

  Headers() = default;
  Headers(std::initializer_list<value_type> values) : values_(values) {}
  Headers(Map values) : values_(std::move(values)) {}
  template <typename InputIterator>
  Headers(InputIterator first, InputIterator last) : values_(first, last) {}
  Headers(const Headers&) = default;
  Headers(Headers&&) noexcept = default;
  Headers& operator=(const Headers&) = default;
  Headers& operator=(Headers&&) noexcept = default;
  Headers& operator=(std::initializer_list<value_type> values) {
    Headers replacement(values);
    swap(replacement);
    return *this;
  }

  static Headers FromBeast(boost::beast::http::fields&& fields) {
    Headers result;
    result.native_ = std::make_shared<NativeHeaders>(std::move(fields));
    return result;
  }

  [[nodiscard]] std::optional<std::string_view> lookup(
      std::string_view name) const {
    if (!native_ || detached_) {
      const auto found = values_.find(name);
      if (found == values_.end()) return std::nullopt;
      return found->second;
    }
    const CaseInsensitiveLess less;
    std::optional<std::string_view> result;
    // Preserve the previous map conversion: first spelling, last value for
    // repeated names, including names differing only in case.
    for (const auto& field : native_->fields) {
      const auto key = field.name_string();
      const std::string_view candidate{key.data(), key.size()};
      if (!less(candidate, name) && !less(name, candidate)) {
        const auto value = field.value();
        result = std::string_view{value.data(), value.size()};
      }
    }
    return result;
  }

  [[nodiscard]] bool empty() const noexcept {
    return native_ && !detached_ ? native_->fields.begin() == native_->fields.end()
                   : values_.empty();
  }
  [[nodiscard]] size_type size() const { return map().size(); }
  [[nodiscard]] bool contains(std::string_view name) const {
    return lookup(name).has_value();
  }
  [[nodiscard]] size_type count(std::string_view name) const {
    return contains(name) ? 1 : 0;
  }

  // Explicit access also provides migration for code requiring std::map.
  const Map& map() const {
    if (!native_ || detached_) return values_;
    const auto& source = *native_;
    std::call_once(source.materialized, [&source] {
      source.values = source.toMap();
    });
    return source.values;
  }
  Map& map() {
    if (native_ && !detached_) {
      // Retain the old immutable storage: a key/value passed to a mutator
      // may refer to its fields or its cached const map. It must remain alive
      // throughout the operation, even after detaching the writable map.
      values_ = native_->toMap();
      detached_ = true;
    }
    return values_;
  }

  iterator begin() { return map().begin(); }
  iterator end() { return map().end(); }
  const_iterator begin() const { return map().begin(); }
  const_iterator end() const { return map().end(); }
  const_iterator cbegin() const { return map().cbegin(); }
  const_iterator cend() const { return map().cend(); }
  reverse_iterator rbegin() { return map().rbegin(); }
  reverse_iterator rend() { return map().rend(); }
  const_reverse_iterator rbegin() const { return map().rbegin(); }
  const_reverse_iterator rend() const { return map().rend(); }
  iterator find(std::string_view name) { return map().find(name); }
  const_iterator find(std::string_view name) const { return map().find(name); }
  iterator lower_bound(std::string_view name) { return map().lower_bound(name); }
  const_iterator lower_bound(std::string_view name) const {
    return map().lower_bound(name);
  }
  iterator upper_bound(std::string_view name) { return map().upper_bound(name); }
  const_iterator upper_bound(std::string_view name) const {
    return map().upper_bound(name);
  }
  auto equal_range(std::string_view name) { return map().equal_range(name); }
  auto equal_range(std::string_view name) const { return map().equal_range(name); }
  std::string& operator[](const std::string& name) { return map()[name]; }
  std::string& operator[](std::string&& name) { return map()[std::move(name)]; }
  std::string& at(const std::string& name) { return map().at(name); }
  const std::string& at(const std::string& name) const { return map().at(name); }

  auto insert(value_type value) { return map().insert(std::move(value)); }
  void insert(std::initializer_list<value_type> values) { map().insert(values); }
  template <typename InputIterator>
  void insert(InputIterator first, InputIterator last) { map().insert(first, last); }
  template <typename... Args>
  auto emplace(Args&&... args) { return map().emplace(std::forward<Args>(args)...); }
  template <typename... Args>
  auto try_emplace(Args&&... args) {
    return map().try_emplace(std::forward<Args>(args)...);
  }
  template <typename... Args>
  auto insert_or_assign(Args&&... args) {
    return map().insert_or_assign(std::forward<Args>(args)...);
  }
  size_type erase(std::string_view name) {
    // Own the lookup key before erasing its possible backing map entry.
    const std::string ownedName{name};
    auto& values = map();
    const auto found = values.find(ownedName);
    if (found == values.end()) return 0;
    values.erase(found);
    return 1;
  }
  iterator erase(const_iterator position) {
    const auto name = position->first;
    auto& values = map();
    return values.erase(values.find(name));
  }
  void clear() noexcept {
    native_.reset();
    detached_ = false;
    values_.clear();
  }
  void swap(Headers& other) noexcept {
    values_.swap(other.values_);
    native_.swap(other.native_);
    std::swap(detached_, other.detached_);
  }
  friend void swap(Headers& left, Headers& right) noexcept { left.swap(right); }
  friend bool operator==(const Headers& left, const Headers& right) {
    return left.map() == right.map();
  }

 private:
  struct NativeHeaders final {
    explicit NativeHeaders(boost::beast::http::fields&& value)
        : fields(std::move(value)) {}
    Map toMap() const {
      Map result;
      for (const auto& field : fields) {
        const auto name = field.name_string();
        const auto value = field.value();
        result[std::string(name.data(), name.size())] =
            std::string(value.data(), value.size());
      }
      return result;
    }
    boost::beast::http::fields fields;
    mutable std::once_flag materialized;
    mutable Map values;
  };

  Map values_;
  std::shared_ptr<const NativeHeaders> native_;
  bool detached_{};
};

struct Request final {
  std::string method;
  std::string target;
  std::string path;
  Headers headers;
  std::string body;
  bool keepAlive{true};
};

struct Response final {
  int status{200};
  Headers headers;
  std::string body;
  std::string contentType{"application/json; charset=utf-8"};
  bool keepAlive{true};
};

inline std::optional<std::string_view> Header(const Headers& headers,
                                              std::string_view name) {
  return headers.lookup(name);
}

inline std::string NewStreamId() {
  static std::atomic<std::uint64_t> sequence{};
  const auto now = static_cast<std::uint64_t>(
      std::chrono::steady_clock::now().time_since_epoch().count());
  const auto value = sequence.fetch_add(1, std::memory_order_relaxed);
  std::array<char, 2 * sizeof(std::uint64_t) * 2 + 1> buffer{};
  auto* current = buffer.data();
  auto* end = buffer.data() + buffer.size();
  const auto nowResult = std::to_chars(current, end, now, 16);
  current = nowResult.ptr;
  *current++ = '-';
  const auto sequenceResult = std::to_chars(current, end, value, 16);
  return {buffer.data(), sequenceResult.ptr};
}

inline std::optional<tracing::SpanContext> ParseTraceParent(
    std::string_view value) {
  return tracing::ParseTraceParent(value);
}

inline MessageContext ContextFromHeaders(const Headers& headers,
                                         bool tracingEnabled = true) {
  MessageContext context;
  if (const auto stream = Header(headers, "x-stream-id");
      stream && !stream->empty()) {
    context = std::move(context).withStreamId(std::string(*stream));
  } else {
    context = std::move(context).withStreamId(NewStreamId());
  }

  if (const auto timeout = Header(headers, "x-timeout-ms")) {
    std::int64_t milliseconds{};
    const auto parsed = std::from_chars(
        timeout->data(), timeout->data() + timeout->size(), milliseconds);
    if (parsed.ec == std::errc{} &&
        parsed.ptr == timeout->data() + timeout->size() && milliseconds >= 0) {
      context = std::move(context).withDeadline(
          std::chrono::steady_clock::now() +
          std::chrono::milliseconds(milliseconds));
    }
  }

  if (tracingEnabled) {
    tracing::SpanContext propagation;
    if (const auto parent = Header(headers, "traceparent")) {
      if (auto trace = ParseTraceParent(*parent)) {
        propagation = std::move(*trace);
        if (tracing::SampledTraceParent(*parent)) {
          context = std::move(context).withSampling(true);
        }
      }
    }
    if (const auto traceState = Header(headers, "tracestate"))
      propagation.traceState = std::string(*traceState);
    if (const auto baggage = Header(headers, "baggage"))
      propagation.baggage = std::string(*baggage);
    if (propagation.isValid() || !propagation.traceState.empty() ||
        !propagation.baggage.empty())
      context = std::move(context).withTrace(std::move(propagation));
    if (const auto marker = Header(headers, "x-trace");
        marker && !marker->empty()) {
      context = std::move(context).withSampling(true);
    }
  }
  return context;
}

inline void InjectContext(const MessageContext& context, Headers& headers,
                          bool tracingEnabled = true) {
  if (!context.streamId().empty()) {
    headers["x-stream-id"] = std::string(context.streamId());
  }
  if (context.deadline()) {
    const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::max(*context.deadline() - std::chrono::steady_clock::now(),
                 std::chrono::steady_clock::duration::zero()));
    headers["x-timeout-ms"] = std::to_string(remaining.count());
  }
  if (tracingEnabled) {
    const auto& trace = context.trace();
    if (trace.isValid()) {
      headers["traceparent"] = "00-" + trace.traceId + "-" + trace.spanId +
                               (context.samplingEnabled() ? "-01" : "-00");
      if (!trace.traceState.empty()) headers["tracestate"] = trace.traceState;
    }
    if (!trace.baggage.empty()) headers["baggage"] = trace.baggage;
    if (context.samplingEnabled()) headers["x-trace"] = "1";
  }
}

}  // namespace servicelib::http
