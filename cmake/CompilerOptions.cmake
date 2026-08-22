option(CPPBOOSTSERVICELIB_ASAN "Enable AddressSanitizer" OFF)
option(CPPBOOSTSERVICELIB_UBSAN "Enable UndefinedBehaviorSanitizer" OFF)
option(CPPBOOSTSERVICELIB_TSAN "Enable ThreadSanitizer" OFF)
option(CPPBOOSTSERVICELIB_COVERAGE "Enable source coverage" OFF)
option(CPPBOOSTSERVICELIB_PROFILING "Enable profiler-friendly code generation" OFF)
option(CPPBOOSTSERVICELIB_COROUTINE_DIAGNOSTICS
       "Enable profiling-only Asio handler state diagnostics" OFF)
set(CPPBOOSTSERVICELIB_ASIO_RECYCLING_CACHE_SIZE "2" CACHE STRING
    "Per-thread Boost.Asio recycled operation blocks retained per allocator tag")
if(NOT CPPBOOSTSERVICELIB_ASIO_RECYCLING_CACHE_SIZE MATCHES "^[1-9][0-9]*$")
  message(FATAL_ERROR
      "CPPBOOSTSERVICELIB_ASIO_RECYCLING_CACHE_SIZE must be a positive integer")
endif()

if(CPPBOOSTSERVICELIB_COROUTINE_DIAGNOSTICS AND
   NOT CPPBOOSTSERVICELIB_PROFILING)
  message(FATAL_ERROR
      "CPPBOOSTSERVICELIB_COROUTINE_DIAGNOSTICS requires CPPBOOSTSERVICELIB_PROFILING")
endif()

set(_servicelib_sanitizers 0)
foreach(_option CPPBOOSTSERVICELIB_ASAN CPPBOOSTSERVICELIB_TSAN)
  if(${_option})
    math(EXPR _servicelib_sanitizers "${_servicelib_sanitizers} + 1")
  endif()
endforeach()
if(_servicelib_sanitizers GREATER 1)
  message(FATAL_ERROR "ASan and TSan cannot be enabled together")
endif()

add_library(cppboostservicelib_build_options INTERFACE)
target_compile_definitions(cppboostservicelib_build_options INTERFACE
    BOOST_ASIO_RECYCLING_ALLOCATOR_CACHE_SIZE=${CPPBOOSTSERVICELIB_ASIO_RECYCLING_CACHE_SIZE})
if(CPPBOOSTSERVICELIB_COROUTINE_DIAGNOSTICS)
  target_compile_definitions(cppboostservicelib_build_options INTERFACE
      CPPBOOSTSERVICELIB_COROUTINE_DIAGNOSTICS=1
      BOOST_ASIO_CUSTOM_HANDLER_TRACKING=<servicelib/runtime/detail/asio_handler_diagnostics.hpp>)
endif()
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
  target_compile_options(cppboostservicelib_build_options INTERFACE
      -Wall -Wextra -Wpedantic -Wconversion -Wsign-conversion)
  if(CPPBOOSTSERVICELIB_ASAN)
    # Instrument FetchContent transport dependencies as well as ServiceLib.
    # Coroutine frames cross the gRPC/asio-grpc boundary; partial
    # instrumentation leaves poisoned frame memory invisible to gRPC and can
    # report use-after-poison inside ClientContext::TryCancel().
    add_compile_options(
        $<$<COMPILE_LANGUAGE:C,CXX>:-fsanitize=address>
        $<$<COMPILE_LANGUAGE:C,CXX>:-fno-omit-frame-pointer>)
    add_link_options(-fsanitize=address)
    # add_*_options above are directory-scoped and therefore instrument the
    # framework's FetchContent dependencies. Propagate the same flags through
    # the public build-options target so sibling consumer targets are both
    # instrumented and linked with the ASan runtime as well.
    target_compile_options(cppboostservicelib_build_options INTERFACE
        $<$<COMPILE_LANGUAGE:C,CXX>:-fsanitize=address>
        $<$<COMPILE_LANGUAGE:C,CXX>:-fno-omit-frame-pointer>)
    target_link_options(cppboostservicelib_build_options INTERFACE
        -fsanitize=address)
  endif()
  if(CPPBOOSTSERVICELIB_UBSAN)
    # gRPC's vendored Abseil does not compile under whole-tree UBSan with the
    # supported GCC toolchain. Keep undefined-behavior instrumentation on the
    # ServiceLib consumers while ASan covers the cross-library coroutine frame.
    target_compile_options(cppboostservicelib_build_options INTERFACE
        -fsanitize=undefined -fno-omit-frame-pointer)
    target_link_options(cppboostservicelib_build_options INTERFACE
        -fsanitize=undefined)
  endif()
  if(CPPBOOSTSERVICELIB_TSAN)
    # Instrument FetchContent transport dependencies as well as ServiceLib.
    # Static, non-instrumented gRPC/Abseil code hides its synchronization from
    # TSan and produces false publication races in EventEngine internals.
    add_compile_options(
        $<$<COMPILE_LANGUAGE:C,CXX>:-fsanitize=thread>
        $<$<COMPILE_LANGUAGE:C,CXX>:-fno-omit-frame-pointer>)
    add_link_options(-fsanitize=thread)
    target_compile_options(cppboostservicelib_build_options INTERFACE
        $<$<COMPILE_LANGUAGE:C,CXX>:-fsanitize=thread>
        $<$<COMPILE_LANGUAGE:C,CXX>:-fno-omit-frame-pointer>)
    target_link_options(cppboostservicelib_build_options INTERFACE
        -fsanitize=thread)
  endif()
  if(CPPBOOSTSERVICELIB_COVERAGE)
    target_compile_options(cppboostservicelib_build_options INTERFACE
        --coverage -O0 -g)
    target_link_options(cppboostservicelib_build_options INTERFACE --coverage)
  endif()
  if(CPPBOOSTSERVICELIB_PROFILING)
    target_compile_options(cppboostservicelib_build_options INTERFACE
        -g -fno-omit-frame-pointer)
  endif()
endif()
