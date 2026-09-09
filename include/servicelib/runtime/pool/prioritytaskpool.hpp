#pragma once
#include <servicelib/runtime/pool/queuedpool.hpp>
namespace servicelib::pool {
class PriorityTaskPoolImpl final : public IPriorityTaskPool {
 public:
  PriorityTaskPoolImpl(std::string name, IServiceEnvironment& env)
      : pool_(std::move(name), env) {}
  ~PriorityTaskPoolImpl() override {}
  const std::string& getName() const noexcept override {
    return pool_.getName();
  }
  int getExecutorsCount() const override { return pool_.getExecutorsCount(); }
  void start(Context ctx) override { pool_.start(std::move(ctx)); }
  void stop(Context ctx) override { pool_.stop(std::move(ctx)); }
  void addTask(Context ctx, int priority, std::function<void()> fn) override {
    pool_.addTask(std::move(ctx), priority, std::move(fn));
  }

 private:
  detail_pool::QueuedPool<true> pool_;
};
inline std::unique_ptr<IPriorityTaskPool> makePriorityTaskPool(
    std::string name, IServiceEnvironment& env) {
  return std::make_unique<PriorityTaskPoolImpl>(std::move(name), env);
}
}  // namespace servicelib::pool
