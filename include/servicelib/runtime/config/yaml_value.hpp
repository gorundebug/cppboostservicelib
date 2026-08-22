#pragma once

#include <yaml-cpp/yaml.h>

#include <string>
#include <utility>

namespace servicelib::config {

template <typename T>
struct TypeTag final {};

// yaml-cpp boundary adapter for the canonical ServiceLib config parsers.
class YamlValue final {
 public:
  class ConstIterator final {
   public:
    explicit ConstIterator(YAML::const_iterator iterator)
        : iterator_(std::move(iterator)) {}

    ConstIterator& operator++() {
      ++iterator_;
      return *this;
    }
    bool operator!=(const ConstIterator& other) const {
      return iterator_ != other.iterator_;
    }
    YamlValue operator*() const { return YamlValue(iterator_->second); }
    std::string GetName() const {
      return iterator_->first.as<std::string>();
    }

   private:
    YAML::const_iterator iterator_;
  };

  YamlValue() = default;
  explicit YamlValue(YAML::Node node) : node_(std::move(node)) {}

  YamlValue operator[](const std::string& key) const {
    return YamlValue(node_[key]);
  }
  [[nodiscard]] bool IsMissing() const noexcept {
    return !node_.IsDefined();
  }
  [[nodiscard]] bool IsString() const noexcept { return node_.IsScalar(); }
  [[nodiscard]] bool IsObject() const noexcept { return node_.IsMap(); }

  ConstIterator begin() const { return ConstIterator(node_.begin()); }
  ConstIterator end() const { return ConstIterator(node_.end()); }

  template <typename T>
  T As() const {
    if constexpr (requires(const YamlValue& value) {
                    Parse(value, TypeTag<T>{});
                  }) {
      return Parse(*this, TypeTag<T>{});
    } else {
      return node_.as<T>();
    }
  }

  template <typename T>
  T As(T fallback) const {
    return IsMissing() ? std::move(fallback) : As<T>();
  }

  [[nodiscard]] const YAML::Node& node() const noexcept { return node_; }

 private:
  YAML::Node node_;
};

}  // namespace servicelib::config
