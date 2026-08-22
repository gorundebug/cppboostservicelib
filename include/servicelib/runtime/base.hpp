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

#include <cassert>
#include <cstddef>
#include <functional>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <servicelib/runtime/detail/traits.hpp>

namespace servicelib {
#define STREAM_ENABLE_COMPILE

inline const size_t PROPID_ID = 0;

class IRuntimeEnvironment;

template <typename T>
class Lazy {
 public:
  Lazy(T initializer) : initializer_(std::move(initializer)) {}

  std::invoke_result_t<T> operator()() {
    if (!initialized_) {
      if constexpr (std::is_same_v<std::invoke_result_t<T>, void>) {
        std::invoke(initializer_);
      } else {
        value_ = std::invoke(initializer_);
      }
      initialized_ = true;
    }
    if constexpr (std::is_same_v<std::invoke_result_t<T>, void>) {
      return;
    } else {
      return *value_;
    }
  }

 private:
  T initializer_;
  std::variant<std::monostate, std::invoke_result_t<T>> value_;
  bool initialized_{false};
};

class StreamException final : public std::logic_error {
 public:
  explicit StreamException(const std::string& message)
      : std::logic_error(message) {
    assert(false);
  }
  explicit StreamException(const char* message) : std::logic_error(message) {
    assert(false);
  }

  StreamException(const StreamException& e) : std::logic_error(e) {}
  StreamException& operator=(const StreamException& e) {
    std::logic_error::operator=(e);
    return *this;
  };
};

class NotCopyableOrMovable {
 protected:
  NotCopyableOrMovable() = default;
  ~NotCopyableOrMovable() = default;

 private:
  NotCopyableOrMovable(const NotCopyableOrMovable&) = delete;
  NotCopyableOrMovable(NotCopyableOrMovable&&) = delete;
  NotCopyableOrMovable& operator=(const NotCopyableOrMovable&) = delete;
  NotCopyableOrMovable& operator=(NotCopyableOrMovable&&) = delete;
};

class StreamBuilderContext;
class StreamVerifyContext;
class TopologyPrinter;

class StreamBase : public NotCopyableOrMovable {
  template <typename, typename>
  friend class StreamExecutionEnvironment;
  template <typename>
  friend class StreamConsumer;
  template <typename, typename, typename>
  friend class Stream;
  template <typename, typename, typename, typename>
  friend class InputStream;
  template <typename, typename>
  friend class CycleLinkStream;

 protected:
  struct CommonSettings {
    size_t configId{0};
    std::string name;
  };

 private:
  size_t id_{0};
  CommonSettings settings_;
  IRuntimeEnvironment* env_{nullptr};

 protected:
  StreamBase() {}

  virtual size_t buildTopology(StreamBuilderContext& context, size_t id,
                               std::vector<size_t>* splitConsumerIds,
                               bool skip) = 0;
  virtual void verifyTopology(StreamVerifyContext& context) const = 0;
  virtual void printTopology(TopologyPrinter& printer,
                             std::unordered_set<size_t>& visited) const = 0;

  virtual ~StreamBase() = default;

  void setId(size_t id) noexcept {
    assert(id_ == 0 && id != 0);
    id_ = id;
  }

  void setConfigId(size_t id) noexcept { settings_.configId = id; }

  template <typename Config>
  void setConfigIdentity(const Config& config) {
    settings_.configId = static_cast<size_t>(config.id);
    settings_.name = config.name;
  }

  // Copies name + env to another stream node (used in build() codegen path)
  void copySettings(const StreamBase& other) {
    settings_ = other.settings_;
    env_ = other.env_;
  }

  void setEnv(IRuntimeEnvironment* env) noexcept { env_ = env; }

 public:
  virtual size_t getId() const noexcept { return id_; }
  virtual size_t getConfigId() const noexcept { return settings_.configId; }

  void setName(const char* name) { settings_.name = name; }

  virtual const std::string& getName() const noexcept { return settings_.name; }

  IRuntimeEnvironment* getEnv() const noexcept { return env_; }

 private:
  template <class _Tp>
  struct stream_delete {
    constexpr stream_delete() noexcept = default;

    template <class _Up>
    stream_delete(
        const stream_delete<_Up>&,
        typename std::enable_if<std::is_convertible<_Up*, _Tp*>::value>::type* =
            0) noexcept {}

    void operator()(_Tp* __ptr) const noexcept {
      static_assert(sizeof(_Tp) >= 0, "cannot delete an incomplete type");
      static_assert(!std::is_void<_Tp>::value,
                    "cannot delete an incomplete type");
      delete __ptr;
    }
  };

  template <typename _Tp>
  using unique_ptr = std::unique_ptr<_Tp, stream_delete<_Tp>>;
};

class StreamVerifyContext final : public NotCopyableOrMovable {
  template <typename, typename>
  friend class StreamExecutionEnvironment;
  template <typename, typename, typename>
  friend class Stream;
  template <typename, typename, typename, typename>
  friend class InputStream;
  template <typename, typename>
  friend class CycleLinkStream;

  using TIdSet = typename std::unordered_set<const StreamBase*>;

 private:
  TIdSet idSet_;

 protected:
  StreamVerifyContext() = default;
  ~StreamVerifyContext() = default;

  template <typename T,
            typename = std::enable_if_t<std::is_convertible_v<T*, StreamBase*>>>
  void verify(const T& stream) {
    if (stream.getId() == 0) {
      throw StreamException("Stream id wasn't set");
    }
  }

  void setProcessed(const StreamBase& base) {
    if (idSet_.find(&base) != idSet_.end()) {
      throw StreamException(
          "Verify stream error. Stream is already processed.");
    }
    idSet_.insert(&base);
  }

  bool isProcessed(const StreamBase& base) const noexcept {
    TIdSet::const_iterator it = idSet_.find(&base);
    if (it != idSet_.end()) {
      return true;
    }
    return false;
  }
};

template <typename _Tp>
class StreamConsumer;

class StreamBuilderContext final : public NotCopyableOrMovable {
  template <typename, typename>
  friend class StreamExecutionEnvironment;
  template <typename, typename, typename>
  friend class Stream;
  template <typename, typename, typename, typename>
  friend class InputStream;
  template <typename, typename>
  friend class CycleLinkStream;

 private:
  using TLinkMap = typename std::unordered_map<size_t, const StreamBase*>;

  TLinkMap linkMap_;
  std::stringstream out_;

 public:
  using TIdsList = std::vector<size_t>;

  template <typename T>
  inline static constexpr std::size_t constexpr_strlen(const char* s) {
    return std::string::traits_type::length(s);
  }
  /*
      template<typename T>
      static std::string demangled_type_name() {
          const char * name = typeid(T).name();
          int status = 0;
          std::size_t size = 0;
          char * dname = abi::__cxa_demangle( name, NULL, &size, &status );
          std::string ret;
          if(dname != nullptr){
              ret = dname;
              std::free( dname );
          } else {
              ret = getType<T>();
          }
          return ret;
      }

      template<typename T>
      static const std::string_view getDemangledType() {
          static const std::string type(demangled_type_name<const
     std::remove_reference_t<T>&>()); return type;
      }
  */
  template <typename T>
  static constexpr std::string_view type_name() {
#ifdef __clang__
    std::string_view name = __PRETTY_FUNCTION__;
    const char* s = "[T = const ";
    if (name.find(s) != std::string::npos) {
      name = name.substr(name.find(s) + constexpr_strlen<T>(s));
      name = name.substr(0, name.rfind(" &]"));
    } else {
      s = "[T = ";
      name = name.substr(name.find(s) + constexpr_strlen<T>(s));
      name = name.substr(0, name.rfind("]"));
    }
    return name;
#elif defined(__GNUC__)
    std::string_view name = __PRETTY_FUNCTION__;
    const char* s = "[with T = const ";
    if (name.find(s) != std::string::npos) {
      name = name.substr(name.find(s) + constexpr_strlen<T>(s));
      name = name.substr(0, name.find("&;"));
    } else {
      s = "[with T = ";
      name = name.substr(name.find(s) + constexpr_strlen<T>(s));
      name = name.substr(0, name.rfind(";"));
    }
    return name;
#elif defined(_MSC_VER)
    std::string_view name = __FUNCSIG__;
    const char* s = "type_name<const class";
    if (name.find(s) != std::string::npos) {
      name = name.substr(name.find(s) + constexpr_strlen<T>(s));
      name = name.substr(0, name.rfind("&>(void)"));
    } else {
      const char* s = "type_name<class";
      name = name.substr(name.find(s) + constexpr_strlen<T>(s));
      name = name.substr(0, name.rfind(">(void)"));
    }
    return name;
#endif
  }

  template <typename T>
  static const std::string_view& getType() {
    static const std::string_view type(
        type_name<const std::remove_reference_t<T>&>());
    return type;
  }

  template <typename T>
  static bool isInternalType() {
    static bool b(getType<T>().find("lambda at") != std::string::npos ||
                  getType<T>().find("lambda(") != std::string::npos ||
                  getType<T>().find("<lambda") != std::string::npos);
    return b;
  }

 protected:
  StreamBuilderContext() = default;
  ~StreamBuilderContext() = default;

  auto& getOut() noexcept { return out_; }

  std::string getCode() const { return out_.str(); }

  template <typename T>
  void buildTopology(T&, size_t) {}

  void addLink(size_t id, const StreamBase& consumer) {
    if (linkMap_.find(id) != linkMap_.end()) {
      throw StreamException("Build stream logic error. Link already exists.");
    }
    linkMap_[id] = &consumer;
  }

  const TLinkMap& getLinks() const noexcept { return linkMap_; }
};

}  // namespace servicelib
