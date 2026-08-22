if(NOT OPENTELEMETRY_SOURCE_DIR)
  message(FATAL_ERROR "OPENTELEMETRY_SOURCE_DIR is required")
endif()

set(_otel_cmake "${OPENTELEMETRY_SOURCE_DIR}/CMakeLists.txt")
set(_otel_proto_cmake
    "${OPENTELEMETRY_SOURCE_DIR}/cmake/opentelemetry-proto.cmake")
foreach(_required IN ITEMS "${_otel_cmake}" "${_otel_proto_cmake}")
  if(NOT EXISTS "${_required}")
    message(FATAL_ERROR "OpenTelemetry patch input not found: ${_required}")
  endif()
endforeach()

file(READ "${_otel_cmake}" _otel_contents)
set(_protoc_original [=[  if(TARGET protobuf::protoc)
    if(CMAKE_CROSSCOMPILING AND Protobuf_PROTOC_EXECUTABLE)
      set(PROTOBUF_PROTOC_EXECUTABLE ${Protobuf_PROTOC_EXECUTABLE})
    else()
      project_build_tools_get_imported_location(PROTOBUF_PROTOC_EXECUTABLE
                                                protobuf::protoc)
      # If protobuf::protoc is not a imported target, then we use the target
      # directly for fallback
      if(NOT PROTOBUF_PROTOC_EXECUTABLE)
        set(PROTOBUF_PROTOC_EXECUTABLE protobuf::protoc)
      endif()
    endif()
  elseif(Protobuf_PROTOC_EXECUTABLE)
    # Some versions of FindProtobuf.cmake uses mixed case instead of uppercase
    set(PROTOBUF_PROTOC_EXECUTABLE ${Protobuf_PROTOC_EXECUTABLE})
  endif()]=])
set(_protoc_patched [=[  if(Protobuf_PROTOC_EXECUTABLE)
    # A superproject may provide the exact pinned host tool while keeping the
    # protobuf runtime target instrumented for ASan/TSan.
    set(PROTOBUF_PROTOC_EXECUTABLE ${Protobuf_PROTOC_EXECUTABLE})
  elseif(TARGET protobuf::protoc)
    project_build_tools_get_imported_location(PROTOBUF_PROTOC_EXECUTABLE
                                              protobuf::protoc)
    if(NOT PROTOBUF_PROTOC_EXECUTABLE)
      set(PROTOBUF_PROTOC_EXECUTABLE protobuf::protoc)
    endif()
  endif()]=])
if(_otel_contents MATCHES
   "A superproject may provide the exact pinned host tool")
  # FetchContent can re-run PATCH_COMMAND for an existing population.
elseif(_otel_contents MATCHES "if\\(TARGET protobuf::protoc\\)")
  string(REPLACE "${_protoc_original}" "${_protoc_patched}"
                 _otel_contents "${_otel_contents}")
  if(NOT _otel_contents MATCHES
     "A superproject may provide the exact pinned host tool")
    message(FATAL_ERROR "Failed to patch OpenTelemetry protoc selection")
  endif()
  file(WRITE "${_otel_cmake}" "${_otel_contents}")
else()
  message(FATAL_ERROR "Unsupported OpenTelemetry protoc selection layout")
endif()

file(READ "${_otel_proto_cmake}" _otel_proto_contents)
set(_plugin_original [=[if(WITH_OTLP_GRPC)
  if(CMAKE_CROSSCOMPILING)
    find_program(gRPC_CPP_PLUGIN_EXECUTABLE grpc_cpp_plugin)
  else()
    if(TARGET gRPC::grpc_cpp_plugin)
      project_build_tools_get_imported_location(gRPC_CPP_PLUGIN_EXECUTABLE
                                                gRPC::grpc_cpp_plugin)
    else()
      find_program(gRPC_CPP_PLUGIN_EXECUTABLE grpc_cpp_plugin)
    endif()
  endif()
  message(STATUS "gRPC_CPP_PLUGIN_EXECUTABLE=${gRPC_CPP_PLUGIN_EXECUTABLE}")
endif()]=])
set(_plugin_patched [=[if(WITH_OTLP_GRPC)
  if(gRPC_CPP_PLUGIN_EXECUTABLE)
    # Preserve an exact pinned host plugin supplied by the superproject.
  elseif(CMAKE_CROSSCOMPILING)
    find_program(gRPC_CPP_PLUGIN_EXECUTABLE grpc_cpp_plugin)
  else()
    if(TARGET gRPC::grpc_cpp_plugin)
      project_build_tools_get_imported_location(gRPC_CPP_PLUGIN_EXECUTABLE
                                                gRPC::grpc_cpp_plugin)
    else()
      find_program(gRPC_CPP_PLUGIN_EXECUTABLE grpc_cpp_plugin)
    endif()
  endif()
  message(STATUS "gRPC_CPP_PLUGIN_EXECUTABLE=${gRPC_CPP_PLUGIN_EXECUTABLE}")
endif()]=])
if(_otel_proto_contents MATCHES
   "Preserve an exact pinned host plugin supplied by the superproject")
  # Already patched.
elseif(_otel_proto_contents MATCHES "if\\(CMAKE_CROSSCOMPILING\\)")
  string(REPLACE "${_plugin_original}" "${_plugin_patched}"
                 _otel_proto_contents "${_otel_proto_contents}")
  if(NOT _otel_proto_contents MATCHES
     "Preserve an exact pinned host plugin supplied by the superproject")
    message(FATAL_ERROR "Failed to patch OpenTelemetry gRPC plugin selection")
  endif()
  file(WRITE "${_otel_proto_cmake}" "${_otel_proto_contents}")
else()
  message(FATAL_ERROR "Unsupported OpenTelemetry gRPC plugin layout")
endif()
