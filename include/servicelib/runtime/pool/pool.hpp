/*
 * pool.hpp
 * C++ streams API — task pool interfaces
 *
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the
 * [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */
#pragma once

#include <chrono>
#include <functional>
#include <stdexcept>
#include <string>

#include <servicelib/runtime/context.hpp>

namespace servicelib::pool {

// Go analog: pool.ErrPoolAlreadyStarted
class PoolAlreadyStartedError final : public std::runtime_error {
 public:
  PoolAlreadyStartedError() : std::runtime_error("pool already started") {}
};

// Go analog: pool.ErrPoolStopped
class PoolStoppedError final : public std::runtime_error {
 public:
  PoolStoppedError() : std::runtime_error("pool stopped") {}
};

class PoolNotStartedError final : public std::runtime_error {
 public:
  PoolNotStartedError() : std::runtime_error("pool not started") {}
};

// Go analog: ctx.Err() == context.Canceled, checked at the top of AddTask.
class PoolCancelledError final : public std::runtime_error {
 public:
  PoolCancelledError() : std::runtime_error("request cancelled") {}
};

class PoolSelfStopError final : public std::runtime_error {
 public:
  PoolSelfStopError()
      : std::runtime_error("pool cannot be stopped from its own task") {}
};

// Go analog: pool.DelayPool. DelayPool is service-wide rather than named and
// schedules one independent asynchronous task per call.
class IDelayPool {
 public:
  using Duration = std::chrono::steady_clock::duration;

  virtual ~IDelayPool() = default;

  // start() marks the scheduler as explicitly started. delay() may lazily
  // initialize it on the current userver TaskProcessor before start(), matching
  // Go. The context exists for lifecycle API compatibility: it is neither
  // inspected nor retained.
  virtual void start(Context ctx) = 0;

  // Stops admission and waits for accepted tasks, bounded by the context
  // deadline. After timeout, accepted work continues on shared state that may
  // outlive the IDelayPool object. Calling stop() directly or indirectly from
  // work accounted by this pool is forbidden; the direct case is detected.
  virtual void stop(Context ctx) = 0;

  // Executes fn asynchronously after delay. If ctx has an earlier deadline,
  // that deadline becomes the effective delay. Explicit cancellation or ctx
  // deadline expiry executes fn early; exactly one path is allowed to win.
  // A non-positive effective delay still executes asynchronously.
  virtual void delay(Context ctx, Duration delay, std::function<void()> fn) = 0;
};

// Go analog: pool.TaskPool. Counters/gauges/histogram are pushed live to
// servicelib::metrics instruments wired up at construction (pull-based export
// happens at the metrics backend, e.g. userver's ServerMonitor) — there is
// no snapshot/introspection accessor, matching the Go interface.
class ITaskPool {
 public:
  virtual ~ITaskPool() = default;

  virtual const std::string& getName() const noexcept = 0;
  virtual int getExecutorsCount() const = 0;

  // The start context controls the pool lifecycle. Cancellation before the
  // pool becomes ready fails start(). Cancellation after readiness enters an
  // internal draining phase: tasks already accepted continue to execute,
  // addTask() rejects every new task with PoolStoppedError, and there is no
  // public state value to query. The owner must still call stop() to join the
  // manager/executors and finish lifecycle cleanup.
  virtual void start(Context ctx) = 0;
  virtual void stop(Context ctx) = 0;

  // ctx.deadline(), if reachable, moves the task to the front of the queue
  // once it elapses while the task is still queued (Go analog:
  // context.AfterFunc requeue-to-head in taskpool.go), so it isn't starved
  // by a slow queue. ctx.cancelled() rejects immediately.
  virtual void addTask(Context ctx, std::function<void()> task) = 0;
};

// Go analog: pool.PriorityTaskPool
class IPriorityTaskPool {
 public:
  virtual ~IPriorityTaskPool() = default;

  virtual const std::string& getName() const noexcept = 0;
  virtual int getExecutorsCount() const = 0;

  // Same lifecycle contract as ITaskPool::start().
  virtual void start(Context ctx) = 0;
  virtual void stop(Context ctx) = 0;

  // Lower numeric priorities run first, matching Go's min-heap; equal
  // priorities preserve enqueue order. ctx.deadline(), if reachable, bumps
  // the task to the most urgent
  // priority once it elapses while still queued. ctx.cancelled() rejects
  // immediately.
  virtual void addTask(Context ctx, int priority,
                       std::function<void()> task) = 0;
};

}  // namespace servicelib::pool
