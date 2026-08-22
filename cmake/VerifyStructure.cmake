cmake_policy(SET CMP0057 NEW)

set(_root "${CPPBOOSTSERVICELIB_SOURCE_DIR}/include/servicelib")

# cppservicelib is the public-path authority.  Keep the full snapshot in the
# repository so standalone/installed builds enforce the same contract without
# requiring the canonical repository to be present.  A path may differ only
# when it is listed below as an explicit userver boundary or its replacement.
file(STRINGS
    "${CPPBOOSTSERVICELIB_SOURCE_DIR}/cmake/CanonicalPublicPaths.txt"
    _canonical_public_paths)

set(_omitted_userver_boundaries
    datasink/grpc/userver.hpp
    datasink/http/userver.hpp
    datasink/kafka/userver.hpp
    datasource/grpc/userver.hpp
    datasource/http/userver.hpp
    datasource/kafka/userver.hpp
    datasource/localsource/custom.hpp
    runtime/config/component.hpp
    runtime/pool/userver_aliases.hpp
    runtime/telemetry/userver/log.hpp
    runtime/telemetry/userver/metrics.hpp
    runtime/telemetry/userver/sampling.hpp
    runtime/telemetry/userver/status.hpp
    runtime/telemetry/userver/tracing.hpp)
list(APPEND _omitted_userver_boundaries
    runtime/telemetry/userver/userver.hpp)

set(_boost_boundary_replacements
    datasink/grpc/asio.hpp
    datasink/http/beast.hpp
    datasink/http/client.hpp
    datasink/kafka/librdkafka.hpp
    datasource/grpc/asio.hpp
    datasource/http/beast.hpp
    datasource/http/router.hpp
    datasource/http/source.hpp
    datasource/kafka/librdkafka.hpp
    runtime/config/command_line.hpp
    runtime/config/substitution.hpp
    runtime/config/yaml.hpp
    runtime/config/yaml_value.hpp
    runtime/detail/asio_runtime.hpp
    runtime/detail/asio_dispatch.hpp
    runtime/detail/asio_handler_diagnostics.hpp
    runtime/detail/async_operations.hpp
    runtime/detail/grpc_runtime.hpp
    runtime/detail/grpc_context.hpp
    runtime/detail/grpc_client.hpp
    runtime/detail/grpc_streaming.hpp
    runtime/detail/grpc_transport.hpp
    runtime/detail/http_types.hpp
    runtime/detail/sync.hpp
    runtime/endpoint.hpp
    runtime/environment/log/json_logger.hpp
    runtime/environment/metrics/prometheus.hpp
    runtime/telemetry/opentelemetry/log.hpp
    runtime/telemetry/opentelemetry/opentelemetry.hpp
    runtime/telemetry/opentelemetry/tracing.hpp
    runtime/telemetry/librdkafka_statistics.hpp
    runtime/status/http.hpp)

foreach(_path IN LISTS _canonical_public_paths)
  if(NOT EXISTS "${_root}/${_path}")
    if(NOT _path IN_LIST _omitted_userver_boundaries)
      message(FATAL_ERROR "missing canonical public path: ${_path}")
    endif()
  endif()
endforeach()

file(GLOB_RECURSE _actual_public_paths
    LIST_DIRECTORIES FALSE RELATIVE "${_root}" "${_root}/*")
foreach(_path IN LISTS _actual_public_paths)
  if(NOT _path IN_LIST _canonical_public_paths AND
     NOT _path IN_LIST _boost_boundary_replacements)
    message(FATAL_ERROR
        "non-canonical public path has no recorded userver boundary: ${_path}")
  endif()
endforeach()

foreach(_directory IN ITEMS
    api datasink datasource operators runtime transformation
    datasink/grpc datasink/http datasink/kafka datasink/localsink
    datasource/grpc datasource/http datasource/kafka datasource/localsource)
  if(NOT IS_DIRECTORY "${_root}/${_directory}")
    message(FATAL_ERROR "missing cppservicelib-compatible directory: ${_directory}")
  endif()
endforeach()

if(IS_DIRECTORY "${_root}/connector")
  message(FATAL_ERROR
      "non-compatible top-level directory include/servicelib/connector exists")
endif()
if(IS_DIRECTORY "${_root}/runtime/async")
  message(FATAL_ERROR
      "runtime/async diverges from cppservicelib; Asio runtime belongs to runtime/app.hpp")
endif()

foreach(_noncanonical_file IN ITEMS
    runtime/graph_runtime.hpp
    runtime/lifecycle.hpp
    runtime/pool/priority_taskpool.hpp
    runtime/store/rotating_map.hpp
    runtime/store/join_storage.hpp)
  if(EXISTS "${_root}/${_noncanonical_file}")
    message(FATAL_ERROR
        "non-canonical public header exists: ${_noncanonical_file}")
  endif()
endforeach()

file(GLOB _top_level LIST_DIRECTORIES TRUE RELATIVE "${_root}" "${_root}/*")
foreach(_entry IN LISTS _top_level)
  if(IS_DIRECTORY "${_root}/${_entry}" AND
     NOT _entry MATCHES "^(api|datasink|datasource|operators|runtime|transformation)$")
    message(FATAL_ERROR "unexpected public servicelib directory: ${_entry}")
  endif()
endforeach()

foreach(_file IN ITEMS
    datasource/grpc/common.hpp
    datasource/grpc/nostreaming.hpp
    datasource/grpc/clientstreaming.hpp
    datasource/grpc/serverstreaming.hpp
    datasource/grpc/bidistreaming.hpp
    datasink/grpc/common.hpp
    datasink/grpc/nostreaming.hpp
    datasink/grpc/clientstreaming.hpp
    datasink/grpc/serverstreaming.hpp
    datasink/grpc/bidistreaming.hpp
    datasource/http/beast.hpp
    datasink/http/beast.hpp)
  if(NOT EXISTS "${_root}/${_file}")
    message(FATAL_ERROR "missing transport compatibility header: ${_file}")
  endif()
endforeach()
