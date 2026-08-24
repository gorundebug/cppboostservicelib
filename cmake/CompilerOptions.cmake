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
    # Conan profiles instrument transport dependencies with the same sanitizer
    # and include compiler.sanitizer in their package IDs. These directory and
    # interface flags instrument ServiceLib and its generated consumers.
    add_compile_options(
        $<$<COMPILE_LANGUAGE:C,CXX>:-fsanitize=address>
        $<$<COMPILE_LANGUAGE:C,CXX>:-fno-omit-frame-pointer>)
    add_link_options(-fsanitize=address)
    # Propagate the flags through the public build-options target so sibling
    # consumer targets are instrumented and linked with the ASan runtime too.
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
    # Conan profiles apply the same flags to transport dependencies. Static,
    # non-instrumented gRPC/Abseil code would hide synchronization from TSan.
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
