#include <gtest/gtest.h>

#include <servicelib/datasink/grpc/streaming_lifecycle.hpp>

#include <atomic>
#include <barrier>
#include <cstddef>
#include <functional>
#include <memory>
#include <thread>
#include <vector>

namespace {

template <typename T>
struct CountedSessionAllocator {
  using value_type = T;
  std::shared_ptr<std::atomic<std::size_t>> outstanding;

  explicit CountedSessionAllocator(
      std::shared_ptr<std::atomic<std::size_t>> counter)
      : outstanding(std::move(counter)) {}

  template <typename U>
  CountedSessionAllocator(const CountedSessionAllocator<U>& other)
      : outstanding(other.outstanding) {}

  T* allocate(std::size_t count) {
    T* result = std::allocator<T>{}.allocate(count);
    outstanding->fetch_add(1);
    return result;
  }

  void deallocate(T* value, std::size_t count) noexcept {
    std::allocator<T>{}.deallocate(value, count);
    outstanding->fetch_sub(1);
  }

  template <typename U>
  bool operator==(const CountedSessionAllocator<U>& other) const noexcept {
    return outstanding == other.outstanding;
  }
};

struct RegistryCell {
  servicelib::datasink::grpc::detail::StreamingRegistry<RegistryCell>::Registration registration;
  std::atomic<unsigned> cancellations{};
  std::function<void()> onCancel;
  void cancel() {
    cancellations.fetch_add(1);
    if (onCancel) onCancel();
  }
};

using Registry = servicelib::datasink::grpc::detail::StreamingRegistry<RegistryCell>;

TEST(GrpcStreamingRegistry, CompletedSessionsReleaseTheirControlBlocks) {
  Registry registry;
  auto outstanding = std::make_shared<std::atomic<std::size_t>>(0);
  for (unsigned index = 0; index < 1000; ++index) {
    auto cell = std::allocate_shared<RegistryCell>(
        CountedSessionAllocator<RegistryCell>{outstanding});
    cell->registration = registry.add(cell);
  }
  // Session objects and their shared allocation must be reclaimed before Stop.
  EXPECT_EQ(outstanding->load(), 0U);
  registry.close();
  EXPECT_EQ(outstanding->load(), 0U);
}

TEST(GrpcStreamingRegistry, CloseCancelsLiveSessionsOnce) {
  Registry registry;
  auto cell = std::make_shared<RegistryCell>();
  cell->registration = registry.add(cell);
  EXPECT_EQ(cell.use_count(), 1);
  registry.close();
  registry.close();
  EXPECT_EQ(cell->cancellations.load(), 1U);
}

TEST(GrpcStreamingRegistry, RegistrationAfterCloseIsCancelled) {
  Registry registry;
  registry.close();
  auto cell = std::make_shared<RegistryCell>();
  cell->registration = registry.add(cell);
  EXPECT_EQ(cell->cancellations.load(), 1U);
}

TEST(GrpcStreamingRegistry, ConcurrentRegistrationAndCloseLoseNoSessions) {
  Registry registry;
  std::vector<std::shared_ptr<RegistryCell>> cells;
  for (unsigned index = 0; index < 200; ++index)
    cells.push_back(std::make_shared<RegistryCell>());
  std::barrier start{3};
  std::jthread first([&] {
    start.arrive_and_wait();
    for (std::size_t index = 0; index < cells.size(); index += 2)
      cells[index]->registration = registry.add(cells[index]);
  });
  std::jthread second([&] {
    start.arrive_and_wait();
    for (std::size_t index = 1; index < cells.size(); index += 2)
      cells[index]->registration = registry.add(cells[index]);
  });
  start.arrive_and_wait();
  registry.close();
  first.join();
  second.join();
  for (const auto& cell : cells) EXPECT_EQ(cell->cancellations.load(), 1U);
}

TEST(GrpcStreamingRegistry, RegistrationCanOutliveRegistry) {
  auto cell = std::make_shared<RegistryCell>();
  {
    Registry registry;
    cell->registration = registry.add(cell);
  }
  EXPECT_EQ(cell->cancellations.load(), 1U);
  cell.reset();
}

TEST(GrpcStreamingRegistry, CancellationDoesNotHoldRegistryMutex) {
  Registry registry;
  auto cell = std::make_shared<RegistryCell>();
  cell->registration = registry.add(cell);
  cell->onCancel = [&registry] { registry.close(); };
  registry.close();
  EXPECT_EQ(cell->cancellations.load(), 1U);
}

TEST(GrpcStreamingRegistry, DuplicateRegistrationPreservesOriginalEntry) {
  Registry registry;
  auto cell = std::make_shared<RegistryCell>();
  cell->registration = registry.add(cell);
  EXPECT_THROW(static_cast<void>(registry.add(cell)), std::logic_error);
  registry.close();
  EXPECT_EQ(cell->cancellations.load(), 1U);
}

TEST(GrpcStreamingRegistry, ConcurrentDestructionAndCloseReleaseRegistrations) {
  for (unsigned round = 0; round < 30; ++round) {
    Registry registry;
    auto outstanding = std::make_shared<std::atomic<std::size_t>>(0);
    std::vector<std::shared_ptr<RegistryCell>> cells;
    std::vector<std::shared_ptr<std::atomic<unsigned>>> cancellations;
    for (unsigned index = 0; index < 128; ++index) {
      auto count = std::make_shared<std::atomic<unsigned>>(0);
      auto cell = std::allocate_shared<RegistryCell>(
          CountedSessionAllocator<RegistryCell>{outstanding});
      cell->onCancel = [count] { count->fetch_add(1); };
      cell->registration = registry.add(cell);
      cancellations.push_back(std::move(count));
      cells.push_back(std::move(cell));
    }

    std::barrier start{3};
    std::jthread first([&] {
      start.arrive_and_wait();
      for (std::size_t index = 0; index < 32; ++index) cells[index].reset();
    });
    std::jthread second([&] {
      start.arrive_and_wait();
      for (std::size_t index = 32; index < 64; ++index) cells[index].reset();
    });
    start.arrive_and_wait();
    registry.close();
    first.join();
    second.join();

    // Either destruction or cancellation may win for the first half. Live
    // sessions in the second half must always be cancelled exactly once.
    for (std::size_t index = 0; index < 64; ++index) {
      EXPECT_LE(cancellations[index]->load(), 1U);
    }
    for (std::size_t index = 64; index < 128; ++index) {
      EXPECT_EQ(cancellations[index]->load(), 1U);
    }
    cells.clear();
    EXPECT_EQ(outstanding->load(), 0U);
  }
}

}  // namespace
