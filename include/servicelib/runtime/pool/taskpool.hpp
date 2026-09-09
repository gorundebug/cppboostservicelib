#pragma once
#include <servicelib/runtime/pool/queuedpool.hpp>
namespace servicelib::pool {
class TaskPoolImpl final : public ITaskPool {
 public:
  TaskPoolImpl(std::string name, IServiceEnvironment& env)
      : pool_(std::move(name), env) {}
  const std::string& getName() const noexcept override {
    return pool_.getName();
  }
  int getExecutorsCount() const override { return pool_.getExecutorsCount(); }
  void start(Context ctx) override { pool_.start(std::move(ctx)); }
  void stop(Context ctx) override { pool_.stop(std::move(ctx)); }
  void addTask(Context ctx, std::function<void()> fn) override {
    pool_.addTask(std::move(ctx), 0, std::move(fn));
  }

 private:
  detail_pool::QueuedPool<false> pool_;
};
inline std::unique_ptr<ITaskPool> makeTaskPool(std::string name,
                                               IServiceEnvironment& env) {
  return std::make_unique<TaskPoolImpl>(std::move(name), env);
}
}  // namespace servicelib::pool
