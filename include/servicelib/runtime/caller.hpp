/*
 * caller.hpp
 * C++ streams API — per-edge dispatch (Caller pattern)
 *
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the
 * [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */
#pragma once

#include <atomic>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>

#include <servicelib/runtime/config/config.hpp>
#include <servicelib/runtime/consumer.hpp>
#include <servicelib/runtime/detail/asio_dispatch.hpp>
#include <servicelib/runtime/environment/environment.hpp>
#include <servicelib/runtime/pool/pool.hpp>

namespace servicelib {

// ──────────────────────────────────────────────────────────────
// ConsumeStatistics — atomic message counter per edge
// Go analog: consumeStatistics / ConsumeStatistics
// ──────────────────────────────────────────────────────────────
class ConsumeStatistics {
 public:
  void inc() noexcept { count_.fetch_add(1, std::memory_order_relaxed); }
  int64_t count() const noexcept {
    return count_.load(std::memory_order_relaxed);
  }

 private:
  std::atomic<int64_t> count_{0};
};

// ──────────────────────────────────────────────────────────────
// CallerBase — type-erased base stored in the per-edge map.
// Holds the fields shared by all Caller<T> implementations, independent of
// T (Go analog: the embedded `caller[T]` struct — source/consumer stay in
// the typed subclass since their type depends on T).
// ──────────────────────────────────────────────────────────────
class CallerBase {
 public:
  struct Params {
    std::string sourceName;
    std::string consumerName;
    std::shared_ptr<tracing::Tracer> tracer;  // nullable: tracing disabled
    bool metricsEnabled{true};
    std::unique_ptr<metrics::Int64Counter>
        messagesCounter;  // never null (Noop default)
  };

  explicit CallerBase(Params params)
      : sourceName_(std::move(params.sourceName)),
        consumerName_(std::move(params.consumerName)),
        tracer_(std::move(params.tracer)),
        metricsEnabled_(params.metricsEnabled),
        messagesCounter_(std::move(params.messagesCounter)) {}

  virtual ~CallerBase() = default;
  virtual bool isAsync() const noexcept = 0;

  ConsumeStatistics& statistics() noexcept { return statistics_; }

 protected:
  void recordMessage() noexcept {
    statistics_.inc();
    if (!metricsEnabled_) return;
    try {
      messagesCounter_->inc();
    } catch (...) {
      // Telemetry must not change edge delivery semantics.
    }
  }

  [[nodiscard]] bool samplingEnabled(
      const MessageContext& context) const noexcept {
    return tracer_ != nullptr && tracing::SamplingEnabled(context);
  }

  [[nodiscard]] tracing::ActiveSpan startCallSpan(
      MessageContext& context, std::string_view type = {},
      std::string_view poolName = {}) const {
    if (!samplingEnabled(context)) {
      return {};
    }
    if (type.empty()) {
      return tracing::StartSpanInPlace(
          context, tracer_.get(), "stream.call",
          {tracing::Attribute::String("from", sourceName_),
           tracing::Attribute::String("to", consumerName_)});
    }
    if (poolName.empty()) {
      return tracing::StartSpanInPlace(
          context, tracer_.get(), "stream.call",
          {tracing::Attribute::String("from", sourceName_),
           tracing::Attribute::String("to", consumerName_),
           tracing::Attribute::String("type", std::string{type})});
    }
    return tracing::StartSpanInPlace(
        context, tracer_.get(), "stream.call",
        {tracing::Attribute::String("from", sourceName_),
         tracing::Attribute::String("to", consumerName_),
         tracing::Attribute::String("type", std::string{type}),
         tracing::Attribute::String("taskpoolname", std::string{poolName})});
  }

  std::string sourceName_;
  std::string consumerName_;
  std::shared_ptr<tracing::Tracer> tracer_;
  bool metricsEnabled_{};
  std::unique_ptr<metrics::Int64Counter> messagesCounter_;

 private:
  ConsumeStatistics statistics_;
};

// ──────────────────────────────────────────────────────────────
// Caller<T> — typed virtual interface
// Go analog: Caller[T] interface = Consumer[T] + IsAsync()
// ──────────────────────────────────────────────────────────────
template <typename T>
class Caller : public CallerBase {
 public:
  using CallerBase::CallerBase;
  virtual void consume(MessageContext ctx, Payload<T> payload) = 0;
};

// ──────────────────────────────────────────────────────────────
// DirectCaller<T> — synchronous dispatch, no thread/pool
// Go analog: directCaller[T]
// ──────────────────────────────────────────────────────────────
template <typename T>
class DirectCaller final : public Caller<T> {
 public:
  DirectCaller(StreamConsumer<T>& consumer, CallerBase::Params params,
               bool async = false)
      : Caller<T>(std::move(params)), consumer_(consumer), async_(async) {}

  void consume(MessageContext ctx, Payload<T> payload) override {
    this->recordMessage();
    const std::string streamId{ctx.streamId()};
    auto job = std::make_shared<Job>(
        Job{std::move(ctx), std::move(payload)});
    bool start = false;
    {
      std::lock_guard lock{mutex_};
      auto& queue = pending_[streamId];
      start = queue.empty();
      queue.push_back(job);
    }
    if (start) dispatch(streamId, std::move(job));
  }

  bool isAsync() const noexcept override { return async_; }

 private:
  struct Job final {
    MessageContext context;
    Payload<T> payload;
  };

  void dispatch(const std::string& streamId, std::shared_ptr<Job> job) {
    auto activeSpan = std::make_shared<tracing::ActiveSpan>(
        this->startCallSpan(job->context));
    auto parent = job->context.retainCompletion();
    auto completion = AsyncCompletionState::make(
        [this, streamId, activeSpan = std::move(activeSpan)] {
          std::shared_ptr<Job> next;
          {
            std::lock_guard lock{mutex_};
            auto found = pending_.find(streamId);
            if (found == pending_.end()) return;
            found->second.pop_front();
            if (found->second.empty()) {
              pending_.erase(found);
              return;
            }
            next = found->second.front();
          }
          dispatch(streamId, std::move(next));
        },
        std::move(parent));
    auto context = std::move(job->context).withCompletion(completion);
    try {
      consumer_.consume(std::move(context), std::move(job->payload));
    } catch (...) {
      completion->release();
      throw;
    }
    completion->release();
  }

  StreamConsumer<T>& consumer_;
  bool async_{};
  std::mutex mutex_;
  std::unordered_map<std::string, std::deque<std::shared_ptr<Job>>> pending_;
};

// ──────────────────────────────────────────────────────────────
// TaskPoolCaller<T> — dispatch via ITaskPool (async, ordered by pool)
// Go analog: taskPoolCaller[T]
// ──────────────────────────────────────────────────────────────
template <typename T>
class TaskPoolCaller final : public Caller<T> {
 public:
  TaskPoolCaller(StreamConsumer<T>& consumer, pool::ITaskPool& pool,
                 log::Logger& logger, CallerBase::Params params)
      : Caller<T>(std::move(params)),
        consumer_(consumer),
        pool_(pool),
        logger_(logger) {}

  void consume(MessageContext ctx, Payload<T> payload) override {
    this->recordMessage();
    ctx = std::move(ctx).withoutCompletion();
    try {
      pool_.addTask(ctx, [this, ctx, p = std::move(payload)]() mutable {
        [[maybe_unused]] auto activeSpan =
            this->startCallSpan(ctx, "taskpool", pool_.getName());
        consumer_.consume(std::move(ctx), std::move(p));
      });
    } catch (const std::exception& e) {
      try {
        logger_.warn(
            "task pool rejected task",
            {log::Field::Str("pool", pool_.getName()), log::Field::Err(e)});
      } catch (...) {
        // Telemetry must not replace the pool's rejection semantics.
      }
    }
  }

  bool isAsync() const noexcept override { return true; }

 private:
  StreamConsumer<T>& consumer_;
  pool::ITaskPool& pool_;
  log::Logger& logger_;
};

// ──────────────────────────────────────────────────────────────
// PriorityTaskPoolCaller<T> — dispatch via IPriorityTaskPool
// Priority comes from MessageContext when explicitly set (including zero),
// otherwise it falls back to the configured default.
// Go analog: priorityTaskPoolCaller[T]
// ──────────────────────────────────────────────────────────────
template <typename T>
class PriorityTaskPoolCaller final : public Caller<T> {
 public:
  PriorityTaskPoolCaller(StreamConsumer<T>& consumer,
                         pool::IPriorityTaskPool& pool, int priority,
                         log::Logger& logger, CallerBase::Params params)
      : Caller<T>(std::move(params)),
        consumer_(consumer),
        pool_(pool),
        priority_(priority),
        logger_(logger) {}

  void consume(MessageContext ctx, Payload<T> payload) override {
    this->recordMessage();
    ctx = std::move(ctx).withoutCompletion();
    const int prio = ctx.hasPriority() ? ctx.priority() : priority_;
    try {
      pool_.addTask(ctx, prio, [this, ctx, p = std::move(payload)]() mutable {
        [[maybe_unused]] auto activeSpan =
            this->startCallSpan(ctx, "prioritytaskpool", pool_.getName());
        consumer_.consume(std::move(ctx), std::move(p));
      });
    } catch (const std::exception& e) {
      try {
        logger_.warn(
            "priority task pool rejected task",
            {log::Field::Str("pool", pool_.getName()), log::Field::Err(e)});
      } catch (...) {
        // Telemetry must not replace the pool's rejection semantics.
      }
    }
  }

  bool isAsync() const noexcept override { return true; }

 private:
  StreamConsumer<T>& consumer_;
  pool::IPriorityTaskPool& pool_;
  int priority_;
  log::Logger& logger_;
};

// ──────────────────────────────────────────────────────────────
// ParallelCaller<T> — post one service-lifetime-tracked Asio task per message
// Go analog: parallelCaller[T] (goroutine per message)
// ──────────────────────────────────────────────────────────────
template <typename T>
class ParallelCaller final : public Caller<T> {
 public:
  ParallelCaller(StreamConsumer<T>& consumer, IRuntimeEnvironment& environment,
                 CallerBase::Params params)
      : Caller<T>(std::move(params)),
        consumer_(consumer),
        environment_(environment) {}

  void consume(MessageContext ctx, Payload<T> payload) override {
    this->recordMessage();
    ctx = std::move(ctx).withoutCompletion();
    environment_.parallel(
        [this, ctx, p = std::move(payload)]() mutable {
          [[maybe_unused]] auto activeSpan =
              this->startCallSpan(ctx, "parallel");
          consumer_.consume(std::move(ctx), std::move(p));
        });
  }

  bool isAsync() const noexcept override { return true; }

 private:
  StreamConsumer<T>& consumer_;
  IRuntimeEnvironment& environment_;
};

// ──────────────────────────────────────────────────────────────
// makeCallerFromEnv<T> — factory selecting dispatch semantics
// from RuntimeConfig link settings (or service default), and wiring up
// metrics/tracing for the edge. Go analog: MakeCaller[T]
// ──────────────────────────────────────────────────────────────
template <typename T, typename Producer>
std::unique_ptr<Caller<T>> makeCallerFromEnv(
    Producer& producer, StreamConsumer<T>& consumer, IRuntimeEnvironment* env,
    config::LinkID link, std::string sourceNameOverride = {}) {
  const config::CallSemanticsGroup* semantics = nullptr;
  const config::ServiceConfig* serviceConfig = nullptr;
  std::shared_ptr<const config::RuntimeConfig> runtimeConfig;
  std::shared_ptr<const config::ServiceConfig> serviceConfigSnapshot;

  if (env) {
    serviceConfigSnapshot = env->getServiceConfigSnapshot();
    serviceConfig = serviceConfigSnapshot.get();
    runtimeConfig = env->getRuntimeConfigSnapshot();
    if (const auto* cfg = runtimeConfig.get()) {
      if (const auto* linkConfig = cfg->GetLink(link.from, link.to)) {
        if (linkConfig->callSemantics.has_value()) {
          semantics = &linkConfig->callSemantics.value();
        }
      }
      if (!semantics && serviceConfig &&
          serviceConfig->defaultCallSemantics.has_value()) {
        semantics = &serviceConfig->defaultCallSemantics.value();
      }
    }
  }

  CallerBase::Params params;
  params.sourceName = producer.getName();
  params.consumerName = consumer.getName();
  if (runtimeConfig) {
    if (const auto source = runtimeConfig->GetStreamConfigByID(link.from)) {
      params.sourceName = source->GetName();
    }
    if (const auto target = runtimeConfig->GetStreamConfigByID(link.to)) {
      params.consumerName = target->GetName();
    }
  }
  if (!sourceNameOverride.empty()) {
    params.sourceName = std::move(sourceNameOverride);
  }

  if (env) {
    params.metricsEnabled = env->getMetrics().enabled();
    metrics::Labels labels{
        {"service", serviceConfig ? serviceConfig->name : std::string()},
        {"from", params.sourceName},
        {"to", params.consumerName},
    };
    auto scope = env->getMetrics().scope("stream", labels);
    params.messagesCounter = scope->counter(
        "messages_total", "Total number of messages processed by stream link");

    if (auto* tr = env->getTracing()) {
      params.tracer =
          tr->tracer(serviceConfig ? serviceConfig->name : std::string());
    }
  } else {
    params.metricsEnabled = false;
    auto scope = metrics::NoopMetrics::instance().scope("", {});
    params.messagesCounter = scope->counter("", "");
  }

  if (!semantics || semantics->functionCall.has_value()) {
    const bool async = semantics && semantics->functionCall->async;
    return std::make_unique<DirectCaller<T>>(consumer, std::move(params),
                                             async);
  }

  if (semantics->taskPool.has_value()) {
    auto* p = env->getTaskPool(semantics->taskPool->poolName);
    if (!p) {
      throw std::runtime_error("task pool not found: " +
                               semantics->taskPool->poolName);
    }
    return std::make_unique<TaskPoolCaller<T>>(consumer, *p, env->getLogger(),
                                               std::move(params));
  }

  if (semantics->priorityTaskPool.has_value()) {
    auto* p = env->getPriorityTaskPool(semantics->priorityTaskPool->poolName);
    if (!p) {
      throw std::runtime_error("priority task pool not found: " +
                               semantics->priorityTaskPool->poolName);
    }
    return std::make_unique<PriorityTaskPoolCaller<T>>(
        consumer, *p, semantics->priorityTaskPool->priority, env->getLogger(),
        std::move(params));
  }

  if (semantics->parallelCall.has_value()) {
    return std::make_unique<ParallelCaller<T>>(consumer, *env,
                                                std::move(params));
  }

  return std::make_unique<DirectCaller<T>>(consumer, std::move(params));
}

}  // namespace servicelib
