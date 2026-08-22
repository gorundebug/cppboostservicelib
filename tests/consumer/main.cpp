#include <servicelib/runtime/config/config.hpp>
#include <servicelib/runtime/context.hpp>
#include <servicelib/runtime/environment/environment.hpp>
#include <servicelib/transformation/streams.hpp>

#include <concepts>

static_assert(std::derived_from<servicelib::IRuntimeEnvironment,
                                servicelib::IServiceEnvironment>);

int main() {
  servicelib::config::ServiceConfig service;
  service.id = 1;
  service.name = "installed-consumer";
  const auto context = servicelib::MessageContext{}.withPriority(7);
  return service.id == 1 && context.hasPriority() && context.priority() == 7
             ? 0
             : 1;
}
