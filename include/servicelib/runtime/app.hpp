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

#include <servicelib/runtime/environment.hpp>

namespace servicelib {

template <typename TStreamAppImpl, typename TDataTypeFactory>
class StreamApp
    : public StreamExecutionEnvironment<TStreamAppImpl, TDataTypeFactory> {
  class StreamAppRuntime : public NotCopyableOrMovable {
   public:
    virtual void runtimeRelease() = 0;
    virtual void runtimeInit() = 0;
    virtual ~StreamAppRuntime() = default;
  };

  template <typename TRuntime>
  class StreamAppRuntimeImpl final : public StreamAppRuntime {
    TRuntime& runtime_;
    template <typename, typename>
    friend class StreamApp;

   public:
    StreamAppRuntimeImpl(TRuntime& runtime) noexcept : runtime_(runtime) {}
    ~StreamAppRuntimeImpl() override = default;

    void runtimeInit() override { runtime_.runtimeInit(); }

    void runtimeRelease() override { runtime_.runtimeRelease(); }
  };

  std::unique_ptr<StreamAppRuntime> streamAppRuntime_;
  inline static std::atomic<TStreamAppImpl*> instance_{nullptr};
  inline static std::atomic<StreamApp*> baseInstance_{nullptr};

 public:
  using TStreamExecutionEnvironment =
      StreamExecutionEnvironment<TStreamAppImpl, TDataTypeFactory>;

 public:
  template <typename... T>
  static TStreamAppImpl& createStreamApp(T&&... args) {
    static_assert(std::is_invocable<decltype(&TStreamAppImpl::streamsInit),
                                    TStreamAppImpl, T...>::value,
                  "Method streamsInit does not exist in TStreamAppImpl or "
                  "cannot be invoked with the provided arguments");

    static TStreamAppImpl streamApp;

    TStreamAppImpl* expected = nullptr;
    if (std::atomic_compare_exchange_strong_explicit(
            &StreamApp::instance_, &expected, &streamApp,
            std::memory_order_release, std::memory_order_relaxed)) {
      StreamApp::baseInstance_.store(static_cast<StreamApp*>(&streamApp),
                                     std::memory_order_release);
      streamApp.streamsInit(std::forward<T>(args)...);
    } else {
      throw StreamException("streamApp has already been created.");
    }
    return streamApp;
  }

  static TStreamAppImpl& getStreamApp() { return *TStreamAppImpl::instance_; }

  template <typename... T>
  int startApp(T&&... args) {
    static_assert(
        std::is_same_v<int,
                       std::invoke_result_t<decltype(&TStreamAppImpl::start),
                                            TStreamAppImpl, T...>>,
        "Method start does not exist in TStreamAppImpl or cannot be invoked "
        "with the provided arguments");
    if (!streamAppRuntime_) {
      throw StreamException("streamApp wasn't initialized");
    }

    streamAppRuntime_->runtimeInit();
    int ret =
        static_cast<TStreamAppImpl*>(this)->start(std::forward<T>(args)...);
    streamAppRuntime_->runtimeRelease();
    return ret;
  }

 protected:
  StreamApp() = default;
  ~StreamApp() {
    StreamApp* expected = this;
    if (StreamApp::baseInstance_.compare_exchange_strong(
            expected, nullptr, std::memory_order_acq_rel)) {
      StreamApp::instance_.store(nullptr, std::memory_order_release);
    } else if (StreamApp::instance_.load(std::memory_order_acquire) !=
               nullptr) {
      assert(!"Invalid initialization/deinitialization logic for StreamApp.");
    }
  }

  void build() {
    auto& runtime = this->template getExecutionRuntime<false>();
    using TRuntime = std::remove_reference_t<decltype(runtime)>;
    streamAppRuntime_ =
        std::make_unique<StreamAppRuntimeImpl<TRuntime>>(runtime);
  }
};

}  // namespace servicelib
