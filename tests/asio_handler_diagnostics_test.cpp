#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>

#include <cassert>
#include <chrono>

#include <servicelib/runtime/detail/asio_handler_diagnostics.hpp>

namespace diagnostics =
    servicelib::async::runtime_detail::asio_handler_diagnostics;

int main() {
  boost::asio::io_context context;

  bool postRan = false;
  boost::asio::post(context, [&] {
    postRan = true;
    assert(diagnostics::Read().running >= 1);
  });
  assert(diagnostics::Read().queued >= 1);
  assert(diagnostics::Read().running == 0);

  context.run();
  assert(postRan);
  auto snapshot = diagnostics::Read();
  assert(snapshot.queued == 0);
  assert(snapshot.running == 0);
  assert(snapshot.suspended == 0);

  context.restart();
  boost::asio::steady_timer timer(context, std::chrono::hours(1));
  bool timerRan = false;
  timer.async_wait([&](const boost::system::error_code&) {
    timerRan = true;
    assert(diagnostics::Read().running >= 1);
  });
  assert(diagnostics::Read().suspended >= 1);

  timer.cancel();
  context.run();
  assert(timerRan);
  snapshot = diagnostics::Read();
  assert(snapshot.queued == 0);
  assert(snapshot.running == 0);
  assert(snapshot.suspended == 0);
}
