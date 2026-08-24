# Revisions are deliberately centralized. Updating one requires running the
# clean-machine build, connector integration suite and comparative benchmark.
set(CPPBOOSTSERVICELIB_GITHUB_ARCHIVE_BASE "https://github.com" CACHE STRING
    "Base URL for immutable GitHub release and tag archives")
if(DEFINED ENV{SERVICEGEN_GITHUB_RAW_URL} AND
   NOT "$ENV{SERVICEGEN_GITHUB_RAW_URL}" STREQUAL "" AND
   CPPBOOSTSERVICELIB_GITHUB_ARCHIVE_BASE STREQUAL "https://github.com")
  set(CPPBOOSTSERVICELIB_GITHUB_ARCHIVE_BASE
      "$ENV{SERVICEGEN_GITHUB_RAW_URL}" CACHE STRING
      "Base URL for immutable GitHub release and tag archives" FORCE)
endif()
string(REGEX REPLACE "/+$" "" CPPBOOSTSERVICELIB_GITHUB_ARCHIVE_BASE
       "${CPPBOOSTSERVICELIB_GITHUB_ARCHIVE_BASE}")
set(CPPBOOSTSERVICELIB_BOOST_REPOSITORY "https://github.com/boostorg/boost" CACHE STRING "Pinned Boost repository")
set(CPPBOOSTSERVICELIB_BOOST_VERSION "1.89.0" CACHE STRING "Pinned Boost")
set(CPPBOOSTSERVICELIB_GRPC_REPOSITORY "https://github.com/grpc/grpc" CACHE STRING "Pinned gRPC repository")
set(CPPBOOSTSERVICELIB_GRPC_VERSION "v1.71.0" CACHE STRING "Pinned gRPC")
set(CPPBOOSTSERVICELIB_GRPC_ABSEIL_REVISION
    "4447c7562e3bc702ade25105912dce503f0c4010" CACHE STRING
    "Pinned gRPC Abseil revision")
set(CPPBOOSTSERVICELIB_GRPC_CARES_REVISION
    "6360e96b5cf8e5980c887ce58ef727e53d77243a" CACHE STRING
    "Pinned gRPC c-ares revision")
set(CPPBOOSTSERVICELIB_GRPC_PROTOBUF_REVISION
    "2d4414f384dc499af113b5991ce3eaa9df6dd931" CACHE STRING
    "Pinned gRPC protobuf revision")
set(CPPBOOSTSERVICELIB_GRPC_RE2_REVISION
    "0c5616df9c0aaa44c9440d87422012423d91c7d1" CACHE STRING
    "Pinned gRPC re2 revision")
set(CPPBOOSTSERVICELIB_GRPC_OPENCENSUS_PROTO_VERSION "v0.3.0" CACHE STRING
    "Pinned gRPC OpenCensus proto revision")
set(CPPBOOSTSERVICELIB_PROTOBUF_REPOSITORY "https://github.com/protocolbuffers/protobuf" CACHE STRING "Pinned protobuf repository")
set(CPPBOOSTSERVICELIB_PROTOBUF_VERSION "v29.3" CACHE STRING "Pinned protobuf")
set(CPPBOOSTSERVICELIB_ASIO_GRPC_REPOSITORY "https://github.com/Tradias/asio-grpc" CACHE STRING "Pinned asio-grpc repository")
set(CPPBOOSTSERVICELIB_ASIO_GRPC_VERSION "v3.5.0" CACHE STRING "Pinned asio-grpc")
set(CPPBOOSTSERVICELIB_YAML_CPP_REPOSITORY "https://github.com/jbeder/yaml-cpp" CACHE STRING "Pinned yaml-cpp repository")
set(CPPBOOSTSERVICELIB_YAML_CPP_VERSION "0.8.0" CACHE STRING "Pinned yaml-cpp")
set(CPPBOOSTSERVICELIB_RDKAFKA_REPOSITORY "https://github.com/confluentinc/librdkafka" CACHE STRING "Pinned librdkafka repository")
set(CPPBOOSTSERVICELIB_RDKAFKA_VERSION "v2.8.0" CACHE STRING "Pinned librdkafka")
set(CPPBOOSTSERVICELIB_OPENTELEMETRY_REPOSITORY "https://github.com/open-telemetry/opentelemetry-cpp" CACHE STRING "Pinned OpenTelemetry C++ repository")
set(CPPBOOSTSERVICELIB_OPENTELEMETRY_VERSION "v1.20.0" CACHE STRING "Pinned OpenTelemetry C++")
set(CPPBOOSTSERVICELIB_OPENTELEMETRY_PROTO_VERSION "v1.5.0" CACHE STRING
    "Pinned OpenTelemetry proto revision")
set(CPPBOOSTSERVICELIB_GOOGLETEST_REPOSITORY "https://github.com/google/googletest" CACHE STRING "Pinned GoogleTest repository")
set(CPPBOOSTSERVICELIB_GOOGLETEST_VERSION "v1.15.2" CACHE STRING "Pinned GoogleTest")
set(CPPBOOSTSERVICELIB_LIBCRON_REPOSITORY "https://github.com/PerMalmberg/libcron" CACHE STRING "Pinned libcron repository")
set(CPPBOOSTSERVICELIB_LIBCRON_VERSION "v1.3.3" CACHE STRING "Pinned libcron")
set(CPPBOOSTSERVICELIB_LIBCRON_DATE_REPOSITORY "https://github.com/HowardHinnant/date" CACHE STRING "Pinned libcron date repository")
set(CPPBOOSTSERVICELIB_LIBCRON_DATE_REVISION "f94b8f36c6180be0021876c4a397a054fe50c6f2" CACHE STRING "Pinned libcron date revision")
