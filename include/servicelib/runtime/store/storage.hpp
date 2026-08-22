/*
 * storage.hpp
 * C++ streams API — common storage lifecycle contracts.
 *
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 * Licensed under the MIT License. See the
 * [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */
#pragma once

#include <stdexcept>

#include <servicelib/runtime/context.hpp>

namespace servicelib::store {

class StoreAlreadyStartedError final : public std::runtime_error {
 public:
  StoreAlreadyStartedError() : std::runtime_error("store already started") {}
};

class StoreStoppedError final : public std::runtime_error {
 public:
  StoreStoppedError() : std::runtime_error("store stopped") {}
};

class StoreNotStartedError final : public std::runtime_error {
 public:
  StoreNotStartedError() : std::runtime_error("store not started") {}
};

class DuplicateKeyError final : public std::runtime_error {
 public:
  DuplicateKeyError() : std::runtime_error("duplicate key") {}
};

class UnsupportedStoreError final : public std::runtime_error {
 public:
  UnsupportedStoreError() : std::runtime_error("store type is not supported") {}
};

class IStorage {
 public:
  virtual ~IStorage() = default;

  // Context is present for lifecycle API uniformity and is not retained.
  // Storage lifetime is controlled explicitly by stop().
  virtual void start(Context ctx) = 0;

  // Implementations reject new operations, join their timer/callback tasks,
  // and only then return. stop() must not race object destruction.
  virtual void stop(Context ctx) = 0;
};

}  // namespace servicelib::store
