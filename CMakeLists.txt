cmake_minimum_required(VERSION 3.24)

project(cppboostservicelib VERSION 0.2.61 LANGUAGES CXX)

option(USE_CCACHE "Use ccache when it is available" ON)
if(USE_CCACHE AND NOT CMAKE_CXX_COMPILER_LAUNCHER)
  find_program(SERVICEGEN_CCACHE_PROGRAM ccache)
  if(SERVICEGEN_CCACHE_PROGRAM)
    set(CMAKE_CXX_COMPILER_LAUNCHER "${SERVICEGEN_CCACHE_PROGRAM}")
    message(STATUS "C++ compiler cache: ${SERVICEGEN_CCACHE_PROGRAM}")
  endif()
endif()

include(CTest)
include(GNUInstallDirs)
include(CMakePackageConfigHelpers)
include(cmake/CompilerOptions.cmake)

option(CPPBOOSTSERVICELIB_BUILD_TESTS "Build framework tests" ON)
include(cmake/Dependencies.cmake)

find_package(Threads REQUIRED GLOBAL)
find_package(OpenSSL REQUIRED GLOBAL)

# FetchContent dependency targets expose their headers as ordinary include
# directories. Keep warnings strict for ServiceLib and its consumers, but do
# not turn diagnostics in third-party headers into ServiceLib build failures.
function(_servicelib_mark_dependency_includes_system target_name)
  if(NOT TARGET "${target_name}")
    return()
  endif()
  get_target_property(real_target "${target_name}" ALIASED_TARGET)
  if(NOT real_target)
    set(real_target "${target_name}")
  endif()
  get_target_property(dependency_includes "${real_target}"
      INTERFACE_INCLUDE_DIRECTORIES)
  if(dependency_includes)
    set_property(TARGET "${real_target}" APPEND PROPERTY
        INTERFACE_SYSTEM_INCLUDE_DIRECTORIES "${dependency_includes}")
  endif()
endfunction()

foreach(dependency_target
    Boost::headers Boost::asio Boost::beast Boost::json)
  _servicelib_mark_dependency_includes_system("${dependency_target}")
endforeach()

if(TARGET yaml-cpp::yaml-cpp)
  set(CPPBOOSTSERVICELIB_YAML_TARGET yaml-cpp::yaml-cpp)
else()
  set(CPPBOOSTSERVICELIB_YAML_TARGET yaml-cpp)
endif()

add_library(cppboostservicelib INTERFACE)
add_library(servicelib::servicelib ALIAS cppboostservicelib)
set_target_properties(cppboostservicelib PROPERTIES EXPORT_NAME servicelib)

target_compile_features(cppboostservicelib INTERFACE cxx_std_20)
target_include_directories(cppboostservicelib INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
if(CPPBOOSTSERVICELIB_DEPENDENCY_MODE MATCHES "^(CONAN|SYSTEM)$")
  target_link_libraries(cppboostservicelib INTERFACE
      Boost::headers Boost::json Threads::Threads
      ${CPPBOOSTSERVICELIB_YAML_TARGET})
else()
  # FETCH/LOCAL targets belong to their own projects and cannot be members of
  # our install export. Consume their build-tree usage requirements without
  # exporting those targets; the installed package uses find_dependency.
  target_include_directories(cppboostservicelib SYSTEM INTERFACE
      $<BUILD_INTERFACE:${CPPBOOSTSERVICELIB_BOOST_BUILD_INCLUDE_DIR}>
      $<BUILD_INTERFACE:${CPPBOOSTSERVICELIB_YAML_BUILD_INCLUDE_DIR}>)
  target_link_libraries(cppboostservicelib INTERFACE
      Threads::Threads
      $<BUILD_INTERFACE:Boost::headers>
      $<BUILD_INTERFACE:Boost::asio>
      $<BUILD_INTERFACE:Boost::beast>
      $<BUILD_INTERFACE:Boost::json>
      $<BUILD_INTERFACE:${CPPBOOSTSERVICELIB_YAML_TARGET}>)
endif()
target_link_libraries(cppboostservicelib INTERFACE
    $<BUILD_INTERFACE:cppboostservicelib_build_options>
    OpenSSL::Crypto)

if(CPPBOOSTSERVICELIB_ENABLE_CRON)
  set(CPPBOOSTSERVICELIB_CRON_REQUIRES_LIBCRON OFF)
  if(CPPBOOSTSERVICELIB_DEPENDENCY_MODE STREQUAL "CONAN")
    set(CPPBOOSTSERVICELIB_CRON_REQUIRES_LIBCRON ON)
    add_library(cppboostservicelib_cron STATIC
        src/datasource/cron/libcron.cpp)
  else()
    # libcron v1.3.3 predates the C++20 calendar names and intentionally
    # builds its own sources as C++17 in the legacy provider.
    add_library(cppboostservicelib_libcron_objects OBJECT
        ${libcron_SOURCE_DIR}/libcron/src/CronClock.cpp
        ${libcron_SOURCE_DIR}/libcron/src/CronData.cpp
        ${libcron_SOURCE_DIR}/libcron/src/CronRandomization.cpp
        ${libcron_SOURCE_DIR}/libcron/src/CronSchedule.cpp
        ${libcron_SOURCE_DIR}/libcron/src/Task.cpp)
    set_target_properties(cppboostservicelib_libcron_objects PROPERTIES
        CXX_STANDARD 17 CXX_STANDARD_REQUIRED YES CXX_EXTENSIONS NO)
    target_compile_definitions(cppboostservicelib_libcron_objects PRIVATE
        HAS_UNCAUGHT_EXCEPTIONS)
    target_include_directories(cppboostservicelib_libcron_objects SYSTEM PRIVATE
        ${libcron_SOURCE_DIR}/libcron/include
        ${libcron_date_SOURCE_DIR}/include)
    add_library(cppboostservicelib_cron STATIC
        src/datasource/cron/libcron.cpp
        $<TARGET_OBJECTS:cppboostservicelib_libcron_objects>)
  endif()
  add_library(servicelib::cron ALIAS cppboostservicelib_cron)
  set_target_properties(cppboostservicelib_cron PROPERTIES EXPORT_NAME cron)
  target_compile_features(cppboostservicelib_cron PUBLIC cxx_std_20)
  target_link_libraries(cppboostservicelib_cron PUBLIC servicelib::servicelib)
  if(CPPBOOSTSERVICELIB_DEPENDENCY_MODE STREQUAL "CONAN")
    target_link_libraries(cppboostservicelib_cron PRIVATE libcron::libcron)
  else()
    target_include_directories(cppboostservicelib_cron SYSTEM PRIVATE
        ${libcron_SOURCE_DIR}/libcron/include
        ${libcron_date_SOURCE_DIR}/include)
  endif()
endif()

if(CPPBOOSTSERVICELIB_ENABLE_GRPC)
  # Transport dependencies stay in a separate component: HTTP-only consumers
  # do not inherit protobuf/gRPC, while gRPC services get the exact headers,
  # compile definitions and libraries required by GrpcRuntime and adapters.
  add_library(cppboostservicelib_grpc INTERFACE)
  add_library(servicelib::grpc ALIAS cppboostservicelib_grpc)
  set_target_properties(cppboostservicelib_grpc PROPERTIES EXPORT_NAME grpc)
  if(NOT CPPBOOSTSERVICELIB_DEPENDENCY_MODE MATCHES "^(CONAN|SYSTEM)$")
    target_include_directories(cppboostservicelib_grpc SYSTEM INTERFACE
        $<BUILD_INTERFACE:${asio-grpc_SOURCE_DIR}/src>
        $<BUILD_INTERFACE:${grpc_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${grpc_SOURCE_DIR}/third_party/abseil-cpp>
        $<BUILD_INTERFACE:${grpc_SOURCE_DIR}/third_party/re2>
        $<BUILD_INTERFACE:${grpc_BINARY_DIR}/third_party/cares/cares>
        $<BUILD_INTERFACE:${grpc_SOURCE_DIR}/third_party/cares/cares>
        $<BUILD_INTERFACE:${grpc_SOURCE_DIR}/third_party/cares/cares/include>
        $<BUILD_INTERFACE:${protobuf_SOURCE_DIR}/src>)
  endif()
  target_link_libraries(cppboostservicelib_grpc INTERFACE
      $<BUILD_INTERFACE:servicelib::servicelib>
      $<BUILD_INTERFACE:asio-grpc::asio-grpc>
      $<BUILD_INTERFACE:${CPPBOOSTSERVICELIB_GRPCPP_TARGET}>
      $<INSTALL_INTERFACE:servicelib::servicelib>
      $<INSTALL_INTERFACE:asio-grpc::asio-grpc>
      $<INSTALL_INTERFACE:gRPC::grpc++>)
endif()

if(CPPBOOSTSERVICELIB_ENABLE_KAFKA)
  _servicelib_mark_dependency_includes_system(
      "${CPPBOOSTSERVICELIB_RDKAFKA_TARGET}")
  add_library(cppboostservicelib_kafka INTERFACE)
  add_library(servicelib::kafka ALIAS cppboostservicelib_kafka)
  set_target_properties(cppboostservicelib_kafka PROPERTIES EXPORT_NAME kafka)
  if(NOT CPPBOOSTSERVICELIB_DEPENDENCY_MODE MATCHES "^(CONAN|SYSTEM)$")
    target_include_directories(cppboostservicelib_kafka SYSTEM INTERFACE
        $<BUILD_INTERFACE:${RdKafka_SOURCE_DIR}/src>)
  endif()
  target_link_libraries(cppboostservicelib_kafka INTERFACE
      $<BUILD_INTERFACE:servicelib::servicelib>
      $<BUILD_INTERFACE:${CPPBOOSTSERVICELIB_RDKAFKA_TARGET}>
      $<INSTALL_INTERFACE:servicelib::servicelib>
      $<INSTALL_INTERFACE:RdKafka::rdkafka>)
endif()

if(CPPBOOSTSERVICELIB_ENABLE_OTEL)
  add_library(cppboostservicelib_otel INTERFACE)
  add_library(servicelib::otel ALIAS cppboostservicelib_otel)
  set_target_properties(cppboostservicelib_otel PROPERTIES EXPORT_NAME otel)
  target_link_libraries(cppboostservicelib_otel INTERFACE
      $<BUILD_INTERFACE:servicelib::servicelib>
      $<BUILD_INTERFACE:${CPPBOOSTSERVICELIB_OTEL_TRACE_TARGET}>
      $<BUILD_INTERFACE:${CPPBOOSTSERVICELIB_OTEL_LOGS_TARGET}>
      $<BUILD_INTERFACE:${CPPBOOSTSERVICELIB_OTEL_GRPC_TARGET}>
      $<BUILD_INTERFACE:${CPPBOOSTSERVICELIB_OTEL_GRPC_LOG_TARGET}>
      $<BUILD_INTERFACE:${CPPBOOSTSERVICELIB_OTEL_OSTREAM_TRACE_TARGET}>
      $<INSTALL_INTERFACE:servicelib::servicelib>
      $<INSTALL_INTERFACE:opentelemetry-cpp::trace>
      $<INSTALL_INTERFACE:opentelemetry-cpp::logs>
      $<INSTALL_INTERFACE:opentelemetry-cpp::otlp_grpc_exporter>
      $<INSTALL_INTERFACE:opentelemetry-cpp::otlp_grpc_log_record_exporter>)
  target_link_libraries(cppboostservicelib_otel INTERFACE
      $<INSTALL_INTERFACE:opentelemetry-cpp::ostream_span_exporter>)
endif()

if(CPPBOOSTSERVICELIB_BUILD_TESTS)
  if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
    # Test fixtures intentionally use partial designated initialization for
    # config aggregates whose omitted fields have framework defaults.
    # Release normally defines NDEBUG, but several standalone tests deliberately
    # use the standard assert contract.  Keep assertions enabled for test
    # targets only so Release verification executes the same checks as Debug.
    add_compile_options(-UNDEBUG -Werror -Wno-missing-field-initializers)
  endif()

  add_test(NAME cppboostservicelib_structure_test
      COMMAND ${CMAKE_COMMAND}
          -DCPPBOOSTSERVICELIB_SOURCE_DIR=${CMAKE_CURRENT_SOURCE_DIR}
          -P ${CMAKE_CURRENT_SOURCE_DIR}/cmake/VerifyStructure.cmake)

  add_executable(cppboostservicelib_config_loader_test
      tests/config_loader_test.cpp)
  target_link_libraries(cppboostservicelib_config_loader_test PRIVATE
      servicelib::servicelib)
  add_test(NAME cppboostservicelib_config_loader_test
      COMMAND cppboostservicelib_config_loader_test)

  add_executable(cppboostservicelib_schedule_test
      tests/schedule_test.cpp)
  target_link_libraries(cppboostservicelib_schedule_test PRIVATE
      servicelib::servicelib)
  add_test(NAME cppboostservicelib_schedule_test
      COMMAND cppboostservicelib_schedule_test)
  if(CPPBOOSTSERVICELIB_ENABLE_CRON)
    add_executable(cppboostservicelib_cron_test tests/cron_test.cpp)
    target_link_libraries(cppboostservicelib_cron_test PRIVATE
        servicelib::cron GTest::gtest_main)
    add_test(NAME cppboostservicelib_cron_test
        COMMAND cppboostservicelib_cron_test)
  endif()

  add_executable(cppboostservicelib_asio_runtime_test
      tests/asio_runtime_test.cpp)
  target_link_libraries(cppboostservicelib_asio_runtime_test PRIVATE
      servicelib::servicelib)
  add_test(NAME cppboostservicelib_asio_runtime_test
      COMMAND cppboostservicelib_asio_runtime_test)

  if(CPPBOOSTSERVICELIB_COROUTINE_DIAGNOSTICS)
    add_executable(cppboostservicelib_asio_handler_diagnostics_test
        tests/asio_handler_diagnostics_test.cpp)
    target_link_libraries(cppboostservicelib_asio_handler_diagnostics_test
        PRIVATE servicelib::servicelib)
    add_test(NAME cppboostservicelib_asio_handler_diagnostics_test
        COMMAND cppboostservicelib_asio_handler_diagnostics_test)
  endif()

  add_executable(cppboostservicelib_taskpool_test tests/taskpool_test.cpp)
  target_link_libraries(cppboostservicelib_taskpool_test PRIVATE
      servicelib::servicelib)
  add_test(NAME cppboostservicelib_taskpool_test
      COMMAND cppboostservicelib_taskpool_test)

  add_executable(cppboostservicelib_other_pools_test
      tests/other_pools_test.cpp)
  target_link_libraries(cppboostservicelib_other_pools_test PRIVATE
      servicelib::servicelib GTest::gtest_main)
  add_test(NAME cppboostservicelib_other_pools_test
      COMMAND cppboostservicelib_other_pools_test)

  add_executable(cppboostservicelib_operators_test
      tests/operators_compile_test.cpp)
  target_link_libraries(cppboostservicelib_operators_test PRIVATE
      servicelib::servicelib GTest::gtest_main)
  add_test(NAME cppboostservicelib_operators_test
      COMMAND cppboostservicelib_operators_test)

  add_executable(cppboostservicelib_operators_topology_test
      tests/operators_topology_test.cpp)
  target_link_libraries(cppboostservicelib_operators_topology_test PRIVATE
      servicelib::servicelib GTest::gtest_main)
  add_test(NAME cppboostservicelib_operators_topology_test
      COMMAND cppboostservicelib_operators_topology_test)

  add_executable(cppboostservicelib_join_topology_test
      tests/join_topology_test.cpp)
  target_link_libraries(cppboostservicelib_join_topology_test PRIVATE
      servicelib::servicelib GTest::gtest_main)
  add_test(NAME cppboostservicelib_join_topology_test
      COMMAND cppboostservicelib_join_topology_test)

  add_executable(cppboostservicelib_store_test tests/store_test.cpp)
  target_link_libraries(cppboostservicelib_store_test PRIVATE
      servicelib::servicelib GTest::gtest_main)
  add_test(NAME cppboostservicelib_store_test
      COMMAND cppboostservicelib_store_test)

  add_executable(cppboostservicelib_serde_test tests/serde_test.cpp)
  target_link_libraries(cppboostservicelib_serde_test PRIVATE
      servicelib::servicelib)
  add_test(NAME cppboostservicelib_serde_test
      COMMAND cppboostservicelib_serde_test)

  add_executable(cppboostservicelib_serviceapp_test tests/serviceapp_test.cpp)
  target_link_libraries(cppboostservicelib_serviceapp_test PRIVATE
      servicelib::servicelib)
  add_test(NAME cppboostservicelib_serviceapp_test
      COMMAND cppboostservicelib_serviceapp_test)

  add_executable(cppboostservicelib_telemetry_test tests/telemetry_test.cpp)
  target_link_libraries(cppboostservicelib_telemetry_test PRIVATE
      servicelib::servicelib GTest::gtest_main)
  add_test(NAME cppboostservicelib_telemetry_test
      COMMAND cppboostservicelib_telemetry_test)

  add_executable(cppboostservicelib_tracing_test tests/tracing_test.cpp)
  target_link_libraries(cppboostservicelib_tracing_test PRIVATE
      servicelib::servicelib GTest::gtest_main)
  add_test(NAME cppboostservicelib_tracing_test
      COMMAND cppboostservicelib_tracing_test)

  add_executable(cppboostservicelib_prometheus_test tests/prometheus_test.cpp)
  target_link_libraries(cppboostservicelib_prometheus_test PRIVATE
      servicelib::servicelib)
  add_test(NAME cppboostservicelib_prometheus_test
      COMMAND cppboostservicelib_prometheus_test)

  if(CPPBOOSTSERVICELIB_ENABLE_OTEL)
    if(TARGET opentelemetry_exporter_in_memory)
      set(_servicelib_otel_memory_test_target
          opentelemetry_exporter_in_memory)
    else()
      set(_servicelib_otel_memory_test_target
          opentelemetry-cpp::in_memory_span_exporter)
    endif()
    if(TARGET opentelemetry_exporter_ostream_logs)
      set(_servicelib_otel_ostream_logs_test_target
          opentelemetry_exporter_ostream_logs)
    else()
      set(_servicelib_otel_ostream_logs_test_target
          opentelemetry-cpp::ostream_log_record_exporter)
    endif()
    add_executable(cppboostservicelib_opentelemetry_test
        tests/opentelemetry_test.cpp)
    target_link_libraries(cppboostservicelib_opentelemetry_test PRIVATE
        servicelib::otel GTest::gtest_main
        ${_servicelib_otel_memory_test_target}
        ${_servicelib_otel_ostream_logs_test_target})
    add_test(NAME cppboostservicelib_opentelemetry_test
        COMMAND cppboostservicelib_opentelemetry_test)
    unset(_servicelib_otel_memory_test_target)
    unset(_servicelib_otel_ostream_logs_test_target)
  endif()

  add_executable(cppboostservicelib_status_test tests/status_test.cpp)
  target_link_libraries(cppboostservicelib_status_test PRIVATE
      servicelib::servicelib GTest::gtest_main)
  add_test(NAME cppboostservicelib_status_test
      COMMAND cppboostservicelib_status_test)

  add_executable(cppboostservicelib_http_endpoints_test
      tests/http_endpoints_test.cpp)
  target_link_libraries(cppboostservicelib_http_endpoints_test PRIVATE
      servicelib::servicelib GTest::gtest_main)
  add_test(NAME cppboostservicelib_http_endpoints_test
      COMMAND cppboostservicelib_http_endpoints_test)

  add_executable(cppboostservicelib_custom_endpoints_test
      tests/custom_endpoints_test.cpp)
  target_link_libraries(cppboostservicelib_custom_endpoints_test PRIVATE
      servicelib::servicelib GTest::gtest_main)
  add_test(NAME cppboostservicelib_custom_endpoints_test
      COMMAND cppboostservicelib_custom_endpoints_test)

  if(CPPBOOSTSERVICELIB_ENABLE_KAFKA)
    add_executable(cppboostservicelib_kafka_endpoints_test
        tests/kafka_endpoints_test.cpp)
    target_link_libraries(cppboostservicelib_kafka_endpoints_test PRIVATE
        servicelib::kafka GTest::gtest_main)
    add_test(NAME cppboostservicelib_kafka_endpoints_test
        COMMAND cppboostservicelib_kafka_endpoints_test)
  endif()

  if(CPPBOOSTSERVICELIB_ENABLE_GRPC)
    asio_grpc_protobuf_generate(
        GENERATE_GRPC
        OUT_VAR CPPBOOSTSERVICELIB_TEST_PROTO_SOURCES
        OUT_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated"
        IMPORT_DIRS "${CMAKE_CURRENT_SOURCE_DIR}/tests/proto"
        PROTOS "${CMAKE_CURRENT_SOURCE_DIR}/tests/proto/connector_test.proto")
    add_library(cppboostservicelib_test_proto OBJECT
        ${CPPBOOSTSERVICELIB_TEST_PROTO_SOURCES})
    target_link_libraries(cppboostservicelib_test_proto PUBLIC
        protobuf::libprotobuf ${CPPBOOSTSERVICELIB_GRPCPP_TARGET})
    target_include_directories(cppboostservicelib_test_proto SYSTEM PUBLIC
        "${CMAKE_CURRENT_BINARY_DIR}/generated")

    add_executable(cppboostservicelib_grpc_runtime_test
        tests/grpc_runtime_test.cpp)
    target_link_libraries(cppboostservicelib_grpc_runtime_test PRIVATE
        servicelib::grpc)
    add_test(NAME cppboostservicelib_grpc_runtime_test
        COMMAND cppboostservicelib_grpc_runtime_test)

    add_executable(cppboostservicelib_grpc_endpoints_test
        tests/grpc_endpoints_test.cpp)
    target_link_libraries(cppboostservicelib_grpc_endpoints_test PRIVATE
        servicelib::grpc GTest::gtest_main)
    add_test(NAME cppboostservicelib_grpc_endpoints_test
        COMMAND cppboostservicelib_grpc_endpoints_test)

    add_executable(cppboostservicelib_grpc_unary_test
        tests/grpc_unary_test.cpp)
    target_link_libraries(cppboostservicelib_grpc_unary_test PRIVATE
        servicelib::grpc cppboostservicelib_test_proto)
    add_test(NAME cppboostservicelib_grpc_unary_test
        COMMAND cppboostservicelib_grpc_unary_test)

    add_executable(cppboostservicelib_grpc_streaming_test
        tests/grpc_streaming_test.cpp)
    target_link_libraries(cppboostservicelib_grpc_streaming_test PRIVATE
        servicelib::grpc cppboostservicelib_test_proto)
    add_test(NAME cppboostservicelib_grpc_streaming_test
        COMMAND cppboostservicelib_grpc_streaming_test)
  endif()
endif()

install(TARGETS cppboostservicelib EXPORT cppboostservicelibTargets)
if(CPPBOOSTSERVICELIB_ENABLE_CRON)
  install(TARGETS cppboostservicelib_cron
      EXPORT cppboostservicelibTargets
      ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR})
endif()
install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(EXPORT cppboostservicelibTargets
    FILE cppboostservicelibTargets.cmake
    NAMESPACE servicelib::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/cppboostservicelib)

configure_package_config_file(
    cmake/cppboostservicelibConfig.cmake.in
    ${CMAKE_CURRENT_BINARY_DIR}/cppboostservicelibConfig.cmake
    INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/cppboostservicelib)
write_basic_package_version_file(
    ${CMAKE_CURRENT_BINARY_DIR}/cppboostservicelibConfigVersion.cmake
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMajorVersion)
install(FILES
    ${CMAKE_CURRENT_BINARY_DIR}/cppboostservicelibConfig.cmake
    ${CMAKE_CURRENT_BINARY_DIR}/cppboostservicelibConfigVersion.cmake
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/cppboostservicelib)
