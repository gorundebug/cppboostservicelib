/*
 * streams.hpp
 * C++ streams API
 *
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the
 * [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */
#pragma once

#include <regex>
#include <sstream>

#include <servicelib/runtime/base.hpp>
#include <servicelib/runtime/context.hpp>

namespace servicelib {

template <typename _F, typename _Context = void>
struct StreamFunction;

namespace detail {

struct stream_function_helper final {
  template <typename, typename>
  friend struct servicelib::StreamFunction;

 protected:
  template <typename T>
  static auto& get_function(T& f) {
    return f;
  }

  template <typename T, typename Ctx>
  static auto& get_function(StreamFunction<T, Ctx>& f) {
    return get_function(f.f_);
  }

  template <typename T>
  static auto get_code([[maybe_unused]] T& f, const char* code) {
    return code;
  }

  template <typename T, typename Ctx>
  static auto get_code(StreamFunction<T, Ctx>& f,
                       [[maybe_unused]] const char* code) {
    return get_code(f.f_, f._code);
  }

  template <typename T, typename Ctx>
  static auto get_code(StreamFunction<T, Ctx>& f) {
    return get_code(f.f_, f._code);
  }

  template <typename T>
  static auto get_code([[maybe_unused]] T& f) {
    return static_cast<const char*>(nullptr);
  }
};

template <typename _Tp>
struct is_stream_function : std::false_type {};

template <typename _Tp, typename _Ctx>
struct is_stream_function<StreamFunction<_Tp, _Ctx>> : std::true_type {};

template <typename _Ctx>
struct StreamFunctionContext {
  const _Ctx& context_;
  explicit StreamFunctionContext(const _Ctx& ctx) : context_(ctx) {}
};

template <>
struct StreamFunctionContext<void> {
  template <typename T>
  explicit StreamFunctionContext(const T&) {}
  StreamFunctionContext() = default;
};

}  // namespace detail

template <typename _F, typename _Context>
struct StreamFunction final {
 private:
  friend struct detail::stream_function_helper;
  template <typename, typename>
  friend struct StreamFunction;

  StreamFunction() = delete;
  StreamFunction(const StreamFunction& f) = delete;
  StreamFunction& operator=(const StreamFunction&) = delete;

  void verifyCode() {
    if (code_ == nullptr) {
      throw StreamException(
          "StreamFunction code can't be empty when compile enabled.");
    }
    if (isInternalType()) {
      // TODO: Check isn't full, need to fix
      static const std::regex captureRegex("\\[\\s*(=|&|this\\b)\\s*\\]");
      if (std::regex_search(code_, captureRegex)) {
        throw StreamException(
            std::string(
                "StreamFunction can't capture context by [=], [&], [this]. ") +
            code_ + " You must use another way.");
      }
    }
  }

  _F f_;
  const char* const code_;
  detail::StreamFunctionContext<_Context> context_;

 public:
  StreamFunction(StreamFunction&&) = default;
  StreamFunction& operator=(StreamFunction&&) = default;

  template <typename T, typename _FCtx, typename _Ctx,
            typename = std::enable_if_t<
                !std::is_same_v<std::decay_t<_Ctx>, char*>, void>>
  StreamFunction(StreamFunction<T, _FCtx>&& f, const _Ctx& ctx)
      : f_(std::move(detail::stream_function_helper::get_function(f.f_))),
        code_(f.code_),
        context_(ctx) {}

  template <typename T,
            typename = std::enable_if_t<
                !servicelib::detail::is_stream_function<T>::value, void>>
  StreamFunction(T f, const char* code = nullptr)
      : f_(std::move(detail::stream_function_helper::get_function(f))),
        code_(detail::stream_function_helper::get_code(f, code)) {
#ifdef STREAM_ENABLE_COMPILE
    verifyCode();
#endif
  }

  template <typename T, typename _FCtx,
            typename = std::enable_if_t<std::is_same_v<_FCtx, void>, void>>
  StreamFunction(StreamFunction<T, _FCtx> f)
      : f_(std::move(detail::stream_function_helper::get_function(f))),
        code_(detail::stream_function_helper::get_code(f)) {
#ifdef STREAM_ENABLE_COMPILE
    verifyCode();
#endif
  }

  template <typename... T>
  auto operator()(T&&... t)
      -> decltype(f_(std::forward<T>(std::declval<T>())...)) {
    return f_(std::forward<T>(t)...);
  }

  bool isInternalType() const noexcept {
    return StreamBuilderContext::isInternalType<_F>();
  }

  const char* getCode() const noexcept { return code_; }

  std::string getFunctionCode() const {
    std::stringstream ss;
    if (code_ != nullptr) {
      ss << "StreamFunction(" << code_ << ")";
    }
    return ss.str();
  }

  template <typename F, typename = std::enable_if_t<
                            !detail::is_stream_function<F>::value, void>>
  static auto make_function(F& f, const char* code) {
    return StreamFunction(std::move(f), code);
  }

  template <typename F, typename Ctx>
  static auto make_function(StreamFunction<F, Ctx>& f,
                            [[maybe_unused]] const char* code = nullptr) {
    return make_function(f.f_, f.code_);
  }
};

template <typename T, typename _Ctx>
StreamFunction(StreamFunction<T, _Ctx> f)
    -> StreamFunction<decltype(detail::stream_function_helper::get_function(f)),
                      _Ctx>;

template <typename T, typename _Ctx>
StreamFunction(StreamFunction<T, _Ctx> f, const char*)
    -> StreamFunction<decltype(detail::stream_function_helper::get_function(f)),
                      _Ctx>;

template <typename T>
StreamFunction(T f) -> StreamFunction<T>;

template <typename T>
StreamFunction(T f, const char*) -> StreamFunction<T>;

template <typename _F, typename = std::enable_if_t<
                           !detail::is_stream_function<_F>::value, void>>
inline static auto make_function(_F f, const char* code) {
  return StreamFunction<_F>::make_function(f, code);
}

template <typename _F, typename _FCtx>
inline static auto make_function(StreamFunction<_F, _FCtx>&& f, const char*) {
  return StreamFunction<_F, _FCtx>::make_function(f);
}

template <typename T>
struct StreamType final {
  using type = T;
};

enum class JoinTypeEnum { Inner, Left, Right, Outer };

template <JoinTypeEnum>
struct JoinType final {};

using Inner = JoinType<JoinTypeEnum::Inner>;
using Left = JoinType<JoinTypeEnum::Left>;
using Right = JoinType<JoinTypeEnum::Right>;
using Outer = JoinType<JoinTypeEnum::Outer>;

enum class JoinStrategyEnum { InMemory, RocksDB };

template <JoinStrategyEnum>
struct JoinStrategy final {};

using InMemory_Strategy = JoinStrategy<JoinStrategyEnum::InMemory>;
using RocksDB_Strategy = JoinStrategy<JoinStrategyEnum::RocksDB>;

#ifdef STREAM_ENABLE_COMPILE
#define StreamFunction(func) make_function(func, #func)
#endif

}  // namespace servicelib
