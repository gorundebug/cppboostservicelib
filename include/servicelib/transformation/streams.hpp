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

#ifndef STREAMS_STREAMS_H
#define STREAMS_STREAMS_H

#include <assert.h>
#include <any>
#include <array>
#include <atomic>
#include <chrono>
#include <iostream>
#include <regex>
#include <sstream>
#include <string>
#include <unordered_set>
#include <variant>
// #include <cxxabi.h>

namespace std {

template <>
struct hash<std::vector<std::byte>> {
  std::size_t operator()(const std::vector<std::byte>& bytes) const {
    return std::hash<std::string_view>{}(std::string_view(reinterpret_cast<const char*>(bytes.data()), bytes.size()));
  }
};

template <>
struct equal_to<std::vector<std::byte>> {
  bool operator()(const std::vector<std::byte>& lhs, const std::vector<std::byte>& rhs) const {
    return std::equal_to<std::string_view>{}(std::string_view(reinterpret_cast<const char*>(lhs.data()), lhs.size()),
                                             std::string_view(reinterpret_cast<const char*>(rhs.data()), rhs.size()));
  }
};

}  // namespace std

#include <servicelib/runtime/detail/traits.hpp>
#include <servicelib/runtime/base.hpp>
#include <servicelib/runtime/topology.hpp>
#include <servicelib/runtime/function.hpp>
#include <servicelib/runtime/consumer.hpp>
#include <servicelib/runtime/detail/storage.hpp>
#include <servicelib/runtime/serde/serde.hpp>
#include <servicelib/runtime/serde/serdeimpl.hpp>
#include <servicelib/runtime/config/stream_types.hpp>
#include <servicelib/runtime/stream.hpp>
#include <servicelib/runtime/environment.hpp>
#include <servicelib/runtime/app.hpp>
#include <servicelib/operators/input.inl>
#include <servicelib/operators/cyclelink.inl>

#endif /* STREAMS_STREAMS_H */
