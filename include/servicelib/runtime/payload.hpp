/*
 * payload.hpp
 * C++ streams API
 *
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */
#pragma once

#include <cassert>
#include <concepts>
#include <cstddef>
#include <memory>
#include <type_traits>
#include <utility>

namespace servicelib {

namespace detail {

template <typename T>
inline constexpr bool kStorePayloadInline =
    sizeof(T) <= 2 * sizeof(void*) &&
    alignof(T) <= alignof(std::max_align_t) &&
    std::is_nothrow_copy_constructible_v<T> &&
    std::is_nothrow_move_constructible_v<T> &&
    std::is_nothrow_copy_assignable_v<T> &&
    std::is_nothrow_move_assignable_v<T>;

template <typename T>
using PayloadStorage = std::conditional_t<
    kStorePayloadInline<T>,
    T,
    std::shared_ptr<T>>;

}  // namespace detail

template <typename T>
class Payload {
  static_assert(std::is_object_v<T>);
  static_assert(!std::is_reference_v<T>);
  static_assert(!std::is_volatile_v<T>);

 public:
  using ValueType   = T;
  using StorageType = detail::PayloadStorage<T>;

  Payload(const Payload&) = default;

  Payload(Payload&&) noexcept(
      std::is_nothrow_move_constructible_v<StorageType>) = default;

  Payload& operator=(const Payload&) = default;

  Payload& operator=(Payload&&) noexcept(
      std::is_nothrow_move_assignable_v<StorageType>) = default;

  static Payload make(const T& value)
    requires std::constructible_from<T, const T&>
  {
    return Payload(makeStorage(value));
  }

  static Payload make(T&& value)
    requires std::constructible_from<T, T&&>
  {
    return Payload(makeStorage(std::move(value)));
  }

  template <typename... Args>
    requires std::constructible_from<T, Args...>
  static Payload emplace(Args&&... args) {
    if constexpr (detail::kStorePayloadInline<T>) {
      return Payload(StorageType(std::forward<Args>(args)...));
    } else {
      return Payload(std::make_shared<T>(std::forward<Args>(args)...));
    }
  }

  [[nodiscard]] const T& get() const noexcept {
    if constexpr (detail::kStorePayloadInline<T>) {
      return storage_;
    } else {
      assert(storage_ != nullptr);
      return *storage_;
    }
  }

  [[nodiscard]] T& get() noexcept {
    if constexpr (detail::kStorePayloadInline<T>) {
      return storage_;
    } else {
      assert(storage_ != nullptr);
      return *storage_;
    }
  }

  [[nodiscard]] bool unique() const noexcept {
    if constexpr (detail::kStorePayloadInline<T>) {
      return true;
    } else {
      return storage_.unique();
    }
  }

  [[nodiscard]] const T* operator->() const noexcept { return std::addressof(get()); }
  [[nodiscard]] T*       operator->()       noexcept { return std::addressof(get()); }

  [[nodiscard]] const T& operator*() const noexcept { return get(); }
  [[nodiscard]] T&       operator*()       noexcept { return get(); }

 private:
  explicit Payload(StorageType storage)
      : storage_(std::move(storage)) {}

  static StorageType makeStorage(const T& value)
    requires std::constructible_from<T, const T&>
  {
    if constexpr (detail::kStorePayloadInline<T>) {
      return value;
    } else {
      return std::make_shared<T>(value);
    }
  }

  static StorageType makeStorage(T&& value)
    requires std::constructible_from<T, T&&>
  {
    if constexpr (detail::kStorePayloadInline<T>) {
      return std::move(value);
    } else {
      return std::make_shared<T>(std::move(value));
    }
  }

  StorageType storage_;
};

}  // namespace servicelib
