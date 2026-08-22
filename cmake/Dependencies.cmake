include(FetchContent)
include(${CMAKE_CURRENT_LIST_DIR}/DependencyVersions.cmake)

set(CPPBOOSTSERVICELIB_DEPENDENCY_MODE "SYSTEM" CACHE STRING
    "Dependency provider: SYSTEM, LOCAL or FETCH")
set_property(CACHE CPPBOOSTSERVICELIB_DEPENDENCY_MODE PROPERTY STRINGS
    SYSTEM LOCAL FETCH)

option(CPPBOOSTSERVICELIB_ENABLE_GRPC "Build asio-grpc transport" OFF)
option(CPPBOOSTSERVICELIB_ENABLE_KAFKA "Build librdkafka transport" OFF)
option(CPPBOOSTSERVICELIB_ENABLE_OTEL "Build OpenTelemetry exporters" OFF)
option(CPPBOOSTSERVICELIB_FETCH_PROGRESS
       "Show FetchContent download and Git clone progress" ON)
if(CPPBOOSTSERVICELIB_FETCH_PROGRESS)
  set(FETCHCONTENT_QUIET OFF)
endif()

set(CPPBOOSTSERVICELIB_BOOST_SOURCE_DIR "" CACHE PATH "Local Boost source")
set(CPPBOOSTSERVICELIB_YAML_CPP_SOURCE_DIR "" CACHE PATH "Local yaml-cpp source")
set(CPPBOOSTSERVICELIB_PROTOBUF_SOURCE_DIR "" CACHE PATH "Local protobuf source")
set(CPPBOOSTSERVICELIB_GRPC_SOURCE_DIR "" CACHE PATH "Local gRPC source")
set(CPPBOOSTSERVICELIB_ASIO_GRPC_SOURCE_DIR "" CACHE PATH "Local asio-grpc source")
set(CPPBOOSTSERVICELIB_RDKAFKA_SOURCE_DIR "" CACHE PATH "Local librdkafka source")
set(CPPBOOSTSERVICELIB_OPENTELEMETRY_SOURCE_DIR "" CACHE PATH "Local OpenTelemetry source")
set(CPPBOOSTSERVICELIB_HOST_PROTOC_EXECUTABLE "" CACHE FILEPATH
    "Pinned uninstrumented protoc used only for sanitizer build-time code generation")
set(CPPBOOSTSERVICELIB_HOST_GRPC_CPP_PLUGIN_EXECUTABLE "" CACHE FILEPATH
    "Pinned uninstrumented grpc_cpp_plugin used only for sanitizer build-time code generation")

function(_servicelib_require_local name directory)
  if(NOT directory OR NOT EXISTS "${directory}/CMakeLists.txt")
    message(FATAL_ERROR
        "${name} local source is required in LOCAL dependency mode: ${directory}")
  endif()
endfunction()

# OpenTelemetry's source build unconditionally calls find_package for gRPC and
# Protobuf even when both dependency source trees already created their CMake
# targets.  A gRPC build-tree config is not an installed package config (its
# exported target files do not exist yet), so pointing find_package at it is
# invalid.  Provide the minimal package-result bridge and keep the actual
# target usage requirements on the already-created pinned source targets.
function(_servicelib_prepare_otel_package_bridge)
  set(_bridge "${CMAKE_BINARY_DIR}/_deps/otel-package-bridge")
  file(MAKE_DIRECTORY "${_bridge}")
  file(WRITE "${_bridge}/gRPCConfig.cmake"
       "set(gRPC_FOUND TRUE)\n")
  if(CPPBOOSTSERVICELIB_HOST_PROTOC_EXECUTABLE)
    set(_servicelib_otel_protoc
        "${CPPBOOSTSERVICELIB_HOST_PROTOC_EXECUTABLE}")
  else()
    set(_servicelib_otel_protoc "protobuf::protoc")
  endif()
  file(WRITE "${_bridge}/ProtobufConfig.cmake"
       "set(Protobuf_FOUND TRUE)\nset(PROTOBUF_FOUND TRUE)\nset(Protobuf_PROTOC_EXECUTABLE \"${_servicelib_otel_protoc}\")\nset(PROTOBUF_PROTOC_EXECUTABLE \"${_servicelib_otel_protoc}\")\n")
  set(gRPC_DIR "${_bridge}" PARENT_SCOPE)
  set(Protobuf_DIR "${_bridge}" PARENT_SCOPE)
  set(Protobuf_FOUND TRUE PARENT_SCOPE)
  set(PROTOBUF_FOUND TRUE PARENT_SCOPE)
  set(Protobuf_PROTOC_EXECUTABLE
      "${_servicelib_otel_protoc}" PARENT_SCOPE)
  set(PROTOBUF_PROTOC_EXECUTABLE
      "${_servicelib_otel_protoc}" PARENT_SCOPE)
  unset(_servicelib_otel_protoc)
endfunction()

function(_servicelib_normalize_librdkafka_cmake_config config_path)
  if(NOT EXISTS "${config_path}")
    message(FATAL_ERROR "librdkafka generated config not found: ${config_path}")
  endif()
  file(READ "${config_path}" config_contents)
  set(original_config_contents "${config_contents}")

  # librdkafka 2.8.0's CMake config selects the __atomic implementation for
  # add/get but omits the matching HAVE_ATOMICS_*_ATOMIC defines consumed by
  # rd_atomic*_set. Without them set falls back to a plain write while other
  # threads use atomic accesses. The autotools build already emits these
  # defines from the same successful compiler probe.
  if(NOT config_contents MATCHES "#define HAVE_ATOMICS_32_ATOMIC 1")
    string(REPLACE "#define HAVE_ATOMICS_32_SYNC 0"
                   "#define HAVE_ATOMICS_32_SYNC 0\n#define HAVE_ATOMICS_32_ATOMIC 1"
                   config_contents "${config_contents}")
  endif()
  if(NOT config_contents MATCHES "#define HAVE_ATOMICS_64_ATOMIC 1")
    string(REPLACE "#define HAVE_ATOMICS_64_SYNC 0"
                   "#define HAVE_ATOMICS_64_SYNC 0\n#define HAVE_ATOMICS_64_ATOMIC 1"
                   config_contents "${config_contents}")
  endif()

  # librdkafka's own `dev-conf.sh tsan` disables glibc C11 threads because
  # they crash under ThreadSanitizer. Its CMake build probes and enables them
  # unconditionally, so mirror the upstream-supported TSan setup. The
  # tinycthread source is already part of the rdkafka target and uses pthreads
  # when WITH_C11THREADS is zero.
  if(CPPBOOSTSERVICELIB_TSAN)
    string(REPLACE "#define WITH_C11THREADS 1"
                   "#define WITH_C11THREADS 0"
                   config_contents "${config_contents}")
    string(REPLACE " C11THREADS" "" config_contents "${config_contents}")
  endif()

  if(NOT config_contents STREQUAL original_config_contents)
    file(WRITE "${config_path}" "${config_contents}")
  endif()
endfunction()

if(CPPBOOSTSERVICELIB_DEPENDENCY_MODE STREQUAL "SYSTEM")
  find_package(Boost 1.75 REQUIRED COMPONENTS json)
  find_package(yaml-cpp 0.7 REQUIRED)
elseif(CPPBOOSTSERVICELIB_DEPENDENCY_MODE STREQUAL "LOCAL")
  _servicelib_require_local(Boost "${CPPBOOSTSERVICELIB_BOOST_SOURCE_DIR}")
  _servicelib_require_local(yaml-cpp "${CPPBOOSTSERVICELIB_YAML_CPP_SOURCE_DIR}")
  add_subdirectory("${CPPBOOSTSERVICELIB_BOOST_SOURCE_DIR}"
                   "${CMAKE_BINARY_DIR}/_deps/boost" EXCLUDE_FROM_ALL)
  add_subdirectory("${CPPBOOSTSERVICELIB_YAML_CPP_SOURCE_DIR}"
                   "${CMAKE_BINARY_DIR}/_deps/yaml-cpp" EXCLUDE_FROM_ALL)
  set(CPPBOOSTSERVICELIB_BOOST_BUILD_INCLUDE_DIR
      "${CPPBOOSTSERVICELIB_BOOST_SOURCE_DIR}")
  set(CPPBOOSTSERVICELIB_YAML_BUILD_INCLUDE_DIR
      "${CPPBOOSTSERVICELIB_YAML_CPP_SOURCE_DIR}/include")
  set(CPPBOOSTSERVICELIB_YAML_BUILD_LIBRARY
      "${CMAKE_BINARY_DIR}/_deps/yaml-cpp/${CMAKE_STATIC_LIBRARY_PREFIX}yaml-cpp${CMAKE_STATIC_LIBRARY_SUFFIX}")
elseif(CPPBOOSTSERVICELIB_DEPENDENCY_MODE STREQUAL "FETCH")
  set(BOOST_INCLUDE_LIBRARIES asio beast json CACHE STRING "" FORCE)
  FetchContent_Declare(boost
      URL "${CPPBOOSTSERVICELIB_BOOST_REPOSITORY}/releases/download/boost-${CPPBOOSTSERVICELIB_BOOST_VERSION}/boost-${CPPBOOSTSERVICELIB_BOOST_VERSION}-cmake.tar.xz")
  set(YAML_CPP_BUILD_TESTS OFF CACHE BOOL "" FORCE)
  set(YAML_CPP_BUILD_TOOLS OFF CACHE BOOL "" FORCE)
  FetchContent_Declare(yaml-cpp
      URL "${CPPBOOSTSERVICELIB_YAML_CPP_REPOSITORY}/archive/refs/tags/${CPPBOOSTSERVICELIB_YAML_CPP_VERSION}.tar.gz"
      DOWNLOAD_EXTRACT_TIMESTAMP FALSE)
  FetchContent_MakeAvailable(boost yaml-cpp)
  set(CPPBOOSTSERVICELIB_BOOST_BUILD_INCLUDE_DIR "${boost_SOURCE_DIR}")
  set(CPPBOOSTSERVICELIB_YAML_BUILD_INCLUDE_DIR "${yaml-cpp_SOURCE_DIR}/include")
  set(CPPBOOSTSERVICELIB_YAML_BUILD_LIBRARY
      "${yaml-cpp_BINARY_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}yaml-cpp${CMAKE_STATIC_LIBRARY_SUFFIX}")
else()
  message(FATAL_ERROR "Unknown CPPBOOSTSERVICELIB_DEPENDENCY_MODE: ${CPPBOOSTSERVICELIB_DEPENDENCY_MODE}")
endif()

if(CPPBOOSTSERVICELIB_BUILD_TESTS)
  find_package(GTest QUIET)
  if(NOT TARGET GTest::gtest_main)
    set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
    FetchContent_Declare(googletest
        URL "${CPPBOOSTSERVICELIB_GOOGLETEST_REPOSITORY}/archive/refs/tags/${CPPBOOSTSERVICELIB_GOOGLETEST_VERSION}.tar.gz"
        DOWNLOAD_EXTRACT_TIMESTAMP FALSE)
    FetchContent_MakeAvailable(googletest)
  endif()
endif()

if(CPPBOOSTSERVICELIB_ENABLE_GRPC)
  if(CPPBOOSTSERVICELIB_DEPENDENCY_MODE STREQUAL "SYSTEM")
    find_package(Protobuf CONFIG REQUIRED)
    find_package(gRPC CONFIG REQUIRED)
    find_package(asio-grpc CONFIG REQUIRED)
  elseif(CPPBOOSTSERVICELIB_DEPENDENCY_MODE STREQUAL "LOCAL")
    _servicelib_require_local(protobuf "${CPPBOOSTSERVICELIB_PROTOBUF_SOURCE_DIR}")
    _servicelib_require_local(gRPC "${CPPBOOSTSERVICELIB_GRPC_SOURCE_DIR}")
    _servicelib_require_local(asio-grpc "${CPPBOOSTSERVICELIB_ASIO_GRPC_SOURCE_DIR}")
    add_subdirectory("${CPPBOOSTSERVICELIB_PROTOBUF_SOURCE_DIR}"
                     "${CMAKE_BINARY_DIR}/_deps/protobuf" EXCLUDE_FROM_ALL)
    add_subdirectory("${CPPBOOSTSERVICELIB_GRPC_SOURCE_DIR}"
                     "${CMAKE_BINARY_DIR}/_deps/grpc" EXCLUDE_FROM_ALL)
    add_subdirectory("${CPPBOOSTSERVICELIB_ASIO_GRPC_SOURCE_DIR}"
                     "${CMAKE_BINARY_DIR}/_deps/asio-grpc" EXCLUDE_FROM_ALL)
  else()
    set(gRPC_BUILD_TESTS OFF CACHE BOOL "" FORCE)
    set(gRPC_BUILD_CODEGEN ON CACHE BOOL "" FORCE)
    set(gRPC_INSTALL ON CACHE BOOL "" FORCE)
    set(protobuf_BUILD_TESTS OFF CACHE BOOL "" FORCE)
    set(protobuf_INSTALL ON CACHE BOOL "" FORCE)
    set(ABSL_ENABLE_INSTALL ON CACHE BOOL "" FORCE)
    set(ASIO_GRPC_INSTALL ON CACHE BOOL "" FORCE)
    set(utf8_range_ENABLE_INSTALL ON CACHE BOOL "" FORCE)
    set(utf8_range_ENABLE_TESTS OFF CACHE BOOL "" FORCE)
    set(CARES_BUILD_TOOLS OFF CACHE BOOL "" FORCE)
    set(CARES_INSTALL OFF CACHE BOOL "" FORCE)
    set(RE2_BUILD_TESTING OFF CACHE BOOL "" FORCE)
    set(gRPC_BUILD_GRPC_CSHARP_PLUGIN OFF CACHE BOOL "" FORCE)
    set(gRPC_BUILD_GRPC_NODE_PLUGIN OFF CACHE BOOL "" FORCE)
    set(gRPC_BUILD_GRPC_OBJECTIVE_C_PLUGIN OFF CACHE BOOL "" FORCE)
    set(gRPC_BUILD_GRPC_PHP_PLUGIN OFF CACHE BOOL "" FORCE)
    set(gRPC_BUILD_GRPC_PYTHON_PLUGIN OFF CACHE BOOL "" FORCE)
    set(gRPC_BUILD_GRPC_RUBY_PLUGIN OFF CACHE BOOL "" FORCE)
    # Use the revisions recorded by the pinned gRPC release for its tightly
    # coupled C/C++ dependencies. Fetching every gRPC submodule also downloads
    # large, unused repositories such as googleapis and makes a clean-machine
    # quickstart needlessly slow.
    set(gRPC_ABSL_PROVIDER module CACHE STRING "" FORCE)
    set(gRPC_CARES_PROVIDER module CACHE STRING "" FORCE)
    set(gRPC_PROTOBUF_PROVIDER module CACHE STRING "" FORCE)
    set(gRPC_RE2_PROVIDER module CACHE STRING "" FORCE)
    # OpenSSL and zlib are small, ubiquitous build prerequisites. Using their
    # system packages avoids cloning and compiling BoringSSL on every clean
    # consumer build while retaining gRPC's pinned revisions for ABI-coupled
    # dependencies.
    find_package(OpenSSL REQUIRED)
    find_package(ZLIB REQUIRED)
    set(gRPC_SSL_PROVIDER package CACHE STRING "" FORCE)
    set(gRPC_ZLIB_PROVIDER package CACHE STRING "" FORCE)
    FetchContent_Declare(grpc
        GIT_REPOSITORY "${CPPBOOSTSERVICELIB_GRPC_REPOSITORY}"
        GIT_TAG "${CPPBOOSTSERVICELIB_GRPC_VERSION}"
        GIT_SHALLOW TRUE
        GIT_PROGRESS ${CPPBOOSTSERVICELIB_FETCH_PROGRESS}
        GIT_SUBMODULES
          third_party/abseil-cpp
          third_party/cares/cares
          third_party/protobuf
          third_party/re2
        GIT_SUBMODULES_RECURSE FALSE)
    FetchContent_Declare(asio-grpc
        GIT_REPOSITORY "${CPPBOOSTSERVICELIB_ASIO_GRPC_REPOSITORY}"
        GIT_TAG "${CPPBOOSTSERVICELIB_ASIO_GRPC_VERSION}"
        GIT_SHALLOW TRUE
        GIT_PROGRESS ${CPPBOOSTSERVICELIB_FETCH_PROGRESS})
    FetchContent_MakeAvailable(grpc asio-grpc)
  endif()

  # Installed gRPC exports namespaced targets; its source-tree build exposes
  # the same libraries without a namespace. Normalize that provider detail for
  # the framework component and tests.
  if(TARGET gRPC::grpc++)
    set(CPPBOOSTSERVICELIB_GRPCPP_TARGET gRPC::grpc++)
  elseif(TARGET grpc++)
    set(CPPBOOSTSERVICELIB_GRPCPP_TARGET grpc++)
    add_library(gRPC::grpc++ ALIAS grpc++)
  else()
    message(FATAL_ERROR "gRPC C++ target was not created")
  endif()
  if(TARGET grpc_cpp_plugin AND NOT TARGET gRPC::grpc_cpp_plugin)
    # OTel v1.20 only accepts an imported namespaced plugin target and reads
    # IMPORTED_LOCATION from it. gRPC's source build exposes a regular
    # unnamespaced executable, so bridge that build-tree detail explicitly.
    get_target_property(_servicelib_grpc_plugin_binary_dir grpc_cpp_plugin
                        BINARY_DIR)
    add_executable(gRPC::grpc_cpp_plugin IMPORTED GLOBAL)
    set_target_properties(gRPC::grpc_cpp_plugin PROPERTIES
        IMPORTED_LOCATION
        "${_servicelib_grpc_plugin_binary_dir}/grpc_cpp_plugin")
    add_dependencies(gRPC::grpc_cpp_plugin grpc_cpp_plugin)
    unset(_servicelib_grpc_plugin_binary_dir)
  endif()
  if(TARGET protoc AND NOT TARGET protobuf::protoc)
    add_executable(protobuf::protoc ALIAS protoc)
  endif()
endif()

if(CPPBOOSTSERVICELIB_ENABLE_KAFKA)
  if(CPPBOOSTSERVICELIB_DEPENDENCY_MODE STREQUAL "SYSTEM")
    find_package(RdKafka CONFIG QUIET)
    if(NOT TARGET RdKafka::rdkafka)
      find_path(CPPBOOSTSERVICELIB_RDKAFKA_INCLUDE_DIR
          NAMES librdkafka/rdkafka.h rdkafka.h REQUIRED)
      find_library(CPPBOOSTSERVICELIB_RDKAFKA_LIBRARY
          NAMES rdkafka REQUIRED)
      add_library(RdKafka::rdkafka UNKNOWN IMPORTED)
      set_target_properties(RdKafka::rdkafka PROPERTIES
          IMPORTED_LOCATION "${CPPBOOSTSERVICELIB_RDKAFKA_LIBRARY}"
          INTERFACE_INCLUDE_DIRECTORIES
              "${CPPBOOSTSERVICELIB_RDKAFKA_INCLUDE_DIR}")
    endif()
  elseif(CPPBOOSTSERVICELIB_DEPENDENCY_MODE STREQUAL "LOCAL")
    _servicelib_require_local(librdkafka "${CPPBOOSTSERVICELIB_RDKAFKA_SOURCE_DIR}")
    add_subdirectory("${CPPBOOSTSERVICELIB_RDKAFKA_SOURCE_DIR}"
                     "${CMAKE_BINARY_DIR}/_deps/librdkafka" EXCLUDE_FROM_ALL)
    _servicelib_normalize_librdkafka_cmake_config(
        "${CMAKE_BINARY_DIR}/_deps/librdkafka/generated/config.h")
  else()
    set(RDKAFKA_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
    set(RDKAFKA_BUILD_TESTS OFF CACHE BOOL "" FORCE)
    FetchContent_Declare(librdkafka
        GIT_REPOSITORY "${CPPBOOSTSERVICELIB_RDKAFKA_REPOSITORY}"
        GIT_TAG "${CPPBOOSTSERVICELIB_RDKAFKA_VERSION}"
        GIT_SHALLOW TRUE
        GIT_PROGRESS ${CPPBOOSTSERVICELIB_FETCH_PROGRESS})
    FetchContent_MakeAvailable(librdkafka)
    _servicelib_normalize_librdkafka_cmake_config(
        "${librdkafka_BINARY_DIR}/generated/config.h")
  endif()

  if(TARGET RdKafka::rdkafka)
    set(CPPBOOSTSERVICELIB_RDKAFKA_TARGET RdKafka::rdkafka)
  elseif(TARGET rdkafka)
    set(CPPBOOSTSERVICELIB_RDKAFKA_TARGET rdkafka)
  else()
    message(FATAL_ERROR "librdkafka C target was not created")
  endif()
endif()

if(CPPBOOSTSERVICELIB_ENABLE_OTEL)
  if(NOT CPPBOOSTSERVICELIB_ENABLE_GRPC)
    message(FATAL_ERROR
        "CPPBOOSTSERVICELIB_ENABLE_OTEL requires CPPBOOSTSERVICELIB_ENABLE_GRPC for the OTLP gRPC exporter")
  endif()
  if(CPPBOOSTSERVICELIB_DEPENDENCY_MODE STREQUAL "SYSTEM")
    find_package(opentelemetry-cpp CONFIG REQUIRED)
  elseif(CPPBOOSTSERVICELIB_DEPENDENCY_MODE STREQUAL "LOCAL")
    set(_servicelib_saved_find_package_prefer_config
        "${CMAKE_FIND_PACKAGE_PREFER_CONFIG}")
    set(_servicelib_saved_build_testing "${BUILD_TESTING}")
    set(CMAKE_FIND_PACKAGE_PREFER_CONFIG TRUE)
    set(BUILD_TESTING OFF)
    _servicelib_prepare_otel_package_bridge()
    set(OPENTELEMETRY_INSTALL OFF CACHE BOOL "" FORCE)
    _servicelib_require_local(opentelemetry-cpp "${CPPBOOSTSERVICELIB_OPENTELEMETRY_SOURCE_DIR}")
    add_subdirectory("${CPPBOOSTSERVICELIB_OPENTELEMETRY_SOURCE_DIR}"
                     "${CMAKE_BINARY_DIR}/_deps/opentelemetry" EXCLUDE_FROM_ALL)
    set(CMAKE_FIND_PACKAGE_PREFER_CONFIG
        "${_servicelib_saved_find_package_prefer_config}")
    set(BUILD_TESTING "${_servicelib_saved_build_testing}")
    unset(_servicelib_saved_find_package_prefer_config)
    unset(_servicelib_saved_build_testing)
  else()
    set(WITH_EXAMPLES OFF CACHE BOOL "" FORCE)
    set(WITH_FUNC_TESTS OFF CACHE BOOL "" FORCE)
    set(WITH_BENCHMARK OFF CACHE BOOL "" FORCE)
    set(WITH_OTLP_GRPC ON CACHE BOOL "" FORCE)
    set(WITH_OTLP_HTTP OFF CACHE BOOL "" FORCE)
    set(WITH_OTLP_FILE OFF CACHE BOOL "" FORCE)
    # Embedded dependency targets are consumed only in the build tree. Their
    # source dependencies belong to different export sets (gRPC/protobuf/
    # Abseil); exporting them from the framework build is invalid. Installed
    # framework consumers resolve an independently installed OTel package via
    # cppboostservicelibConfig.cmake.
    set(OPENTELEMETRY_INSTALL OFF CACHE BOOL "" FORCE)
    # The pinned gRPC/protobuf source targets were created by the framework's
    # transport dependency block above. OTel's nested CMake checks package
    # result variables in addition to the targets.
    set(gRPC_FOUND TRUE)
    set(Protobuf_FOUND TRUE)
    set(PROTOBUF_FOUND TRUE)
    set(_servicelib_saved_find_package_prefer_config
        "${CMAKE_FIND_PACKAGE_PREFER_CONFIG}")
    set(_servicelib_saved_build_testing "${BUILD_TESTING}")
    set(CMAKE_FIND_PACKAGE_PREFER_CONFIG TRUE)
    set(BUILD_TESTING OFF)
    _servicelib_prepare_otel_package_bridge()
    # Use the immutable release archive instead of a recursive Git checkout.
    # The upstream repository declares development-only submodules (including
    # vcpkg) that are not needed by the CMake OTLP build but make a clean
    # consumer configure clone them all before compilation can begin.
    set(_servicelib_otel_patch_command "")
    if(CPPBOOSTSERVICELIB_HOST_PROTOC_EXECUTABLE OR
       CPPBOOSTSERVICELIB_HOST_GRPC_CPP_PLUGIN_EXECUTABLE)
      if(NOT CPPBOOSTSERVICELIB_HOST_PROTOC_EXECUTABLE OR
         NOT CPPBOOSTSERVICELIB_HOST_GRPC_CPP_PLUGIN_EXECUTABLE)
        message(FATAL_ERROR
            "Both pinned OpenTelemetry host code generators must be provided")
      endif()
      set(gRPC_CPP_PLUGIN_EXECUTABLE
          "${CPPBOOSTSERVICELIB_HOST_GRPC_CPP_PLUGIN_EXECUTABLE}")
      set(_servicelib_otel_patch_command
          PATCH_COMMAND
          ${CMAKE_COMMAND}
          -DOPENTELEMETRY_SOURCE_DIR=<SOURCE_DIR>
          -P ${CMAKE_CURRENT_LIST_DIR}/PatchOpenTelemetryHostTools.cmake)
    endif()
    FetchContent_Declare(opentelemetry-cpp
        URL "${CPPBOOSTSERVICELIB_OPENTELEMETRY_REPOSITORY}/archive/refs/tags/${CPPBOOSTSERVICELIB_OPENTELEMETRY_VERSION}.tar.gz"
        DOWNLOAD_EXTRACT_TIMESTAMP FALSE
        ${_servicelib_otel_patch_command})
    FetchContent_MakeAvailable(opentelemetry-cpp)

    # OpenTelemetry v1.20 resolves gRPC::grpc_cpp_plugin to the source-tree
    # executable path, but its proto custom command does not retain the build
    # dependency carried by the imported bridge target.  With an unrestricted
    # parallel build protoc can therefore run before grpc_cpp_plugin exists.
    # Express the host-tool edge on the actual OTel generation targets.
    foreach(_servicelib_otel_proto_target IN ITEMS
            opentelemetry_proto opentelemetry_proto_grpc)
      if(TARGET ${_servicelib_otel_proto_target} AND TARGET grpc_cpp_plugin)
        add_dependencies(${_servicelib_otel_proto_target} grpc_cpp_plugin)
      endif()
    endforeach()
    unset(_servicelib_otel_proto_target)
    unset(_servicelib_otel_patch_command)
    set(CMAKE_FIND_PACKAGE_PREFER_CONFIG
        "${_servicelib_saved_find_package_prefer_config}")
    set(BUILD_TESTING "${_servicelib_saved_build_testing}")
    unset(_servicelib_saved_find_package_prefer_config)
    unset(_servicelib_saved_build_testing)
  endif()

  foreach(_servicelib_otel_component IN ITEMS
          TRACE LOGS GRPC GRPC_LOG OSTREAM_TRACE)
    if(_servicelib_otel_component STREQUAL "TRACE")
      set(_servicelib_otel_build_target opentelemetry_trace)
      set(_servicelib_otel_installed_target opentelemetry-cpp::trace)
    elseif(_servicelib_otel_component STREQUAL "LOGS")
      set(_servicelib_otel_build_target opentelemetry_logs)
      set(_servicelib_otel_installed_target opentelemetry-cpp::logs)
    elseif(_servicelib_otel_component STREQUAL "GRPC")
      set(_servicelib_otel_build_target opentelemetry_exporter_otlp_grpc)
      set(_servicelib_otel_installed_target
          opentelemetry-cpp::otlp_grpc_exporter)
    elseif(_servicelib_otel_component STREQUAL "GRPC_LOG")
      set(_servicelib_otel_build_target
          opentelemetry_exporter_otlp_grpc_log)
      set(_servicelib_otel_installed_target
          opentelemetry-cpp::otlp_grpc_log_record_exporter)
    else()
      set(_servicelib_otel_build_target
          opentelemetry_exporter_ostream_span)
      set(_servicelib_otel_installed_target
          opentelemetry-cpp::ostream_span_exporter)
    endif()
    if(TARGET ${_servicelib_otel_build_target})
      set(CPPBOOSTSERVICELIB_OTEL_${_servicelib_otel_component}_TARGET
          ${_servicelib_otel_build_target})
    elseif(TARGET ${_servicelib_otel_installed_target})
      set(CPPBOOSTSERVICELIB_OTEL_${_servicelib_otel_component}_TARGET
          ${_servicelib_otel_installed_target})
    else()
      message(FATAL_ERROR
          "OpenTelemetry target was not created: ${_servicelib_otel_build_target} or ${_servicelib_otel_installed_target}")
    endif()
  endforeach()
endif()
