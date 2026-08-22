#pragma once

#include <charconv>
#include <cstddef>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>

namespace servicelib::config {

struct CommandLine final {
  std::string configPath{"./config/config.yaml"};
  std::string valuesPath{"./config/overrides.yaml"};
  std::size_t workers{DefaultWorkers()};

  static std::size_t DefaultWorkers() noexcept {
    const auto detected = std::thread::hardware_concurrency();
    return detected == 0 ? 1 : static_cast<std::size_t>(detected);
  }

  static CommandLine Parse(int argc, const char* const* argv) {
    if (argc < 0 || (argc > 0 && argv == nullptr)) {
      throw std::invalid_argument("invalid command line arguments");
    }

    CommandLine result;
    for (int index = 1; index < argc; ++index) {
      const std::string_view argument = RequireArgument(argv[index]);
      if (const auto value = InlineValue(argument, "--config")) {
        result.configPath = RequireValue("--config", *value);
      } else if (argument == "--config") {
        result.configPath = NextValue(argc, argv, index, "--config");
      } else if (const auto value = InlineValue(argument, "--values")) {
        result.valuesPath = RequireValue("--values", *value);
      } else if (argument == "--values") {
        result.valuesPath = NextValue(argc, argv, index, "--values");
      } else if (const auto value = InlineValue(argument, "--workers")) {
        result.workers = ParseWorkers(*value);
      } else if (argument == "--workers") {
        result.workers = ParseWorkers(NextValue(argc, argv, index, "--workers"));
      } else {
        throw std::invalid_argument("unknown command line argument: " +
                                    std::string(argument));
      }
    }
    return result;
  }

 private:
  static std::string_view RequireArgument(const char* argument) {
    if (argument == nullptr) {
      throw std::invalid_argument("null command line argument");
    }
    return argument;
  }

  static std::string_view NextValue(int argc, const char* const* argv,
                                    int& index, std::string_view option) {
    if (++index >= argc) {
      throw std::invalid_argument("missing value for " + std::string(option));
    }
    return RequireArgument(argv[index]);
  }

  static std::string RequireValue(std::string_view option,
                                  std::string_view value) {
    if (value.empty()) {
      throw std::invalid_argument("empty value for " + std::string(option));
    }
    return std::string(value);
  }

  static std::size_t ParseWorkers(std::string_view value) {
    if (value.empty()) {
      throw std::invalid_argument("empty value for --workers");
    }
    std::size_t workers = 0;
    const auto [end, error] =
        std::from_chars(value.data(), value.data() + value.size(), workers);
    if (error != std::errc{} || end != value.data() + value.size() ||
        workers == 0) {
      throw std::invalid_argument(
          "--workers must be a positive base-10 integer");
    }
    return workers;
  }

  struct OptionalView final {
    bool present{false};
    std::string_view value;

    explicit operator bool() const noexcept { return present; }
    const std::string_view& operator*() const noexcept { return value; }
  };

  static OptionalView InlineValue(std::string_view argument,
                                  const char* option) {
    const std::string_view name(option);
    if (!argument.starts_with(name) || argument.size() <= name.size() ||
        argument[name.size()] != '=') {
      return {};
    }
    return {true, argument.substr(name.size() + 1)};
  }
};

}  // namespace servicelib::config
