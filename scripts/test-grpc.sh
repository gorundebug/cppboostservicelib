#!/usr/bin/env bash
set -euo pipefail

ROOT="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"

docker build -f "$ROOT/Dockerfile.cmake" -t cppboostservicelib-build "$ROOT"
docker run --rm \
  -e CCACHE_DIR=/ccache \
  -e CCACHE_BASEDIR=/workspace \
  -e CCACHE_COMPILERCHECK=content \
  -e CCACHE_MAXSIZE="${CCACHE_MAXSIZE:-20G}" \
  -v cppboostservicelib-ccache:/ccache \
  -v "$ROOT:/workspace" -w /workspace \
  cppboostservicelib-build \
  bash -lc "cmake --fresh -S . -B build/grpc-docker -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DCPPBOOSTSERVICELIB_DEPENDENCY_MODE=FETCH \
    -DCPPBOOSTSERVICELIB_ENABLE_GRPC=ON \
    -DCPPBOOSTSERVICELIB_BUILD_TESTS=ON \
    && cmake --build build/grpc-docker --parallel \
      --target cppboostservicelib_grpc_runtime_test \
               cppboostservicelib_grpc_unary_test \
               cppboostservicelib_grpc_streaming_test \
    && ctest --test-dir build/grpc-docker --output-on-failure \
      -R 'cppboostservicelib_grpc_(runtime|unary|streaming)_test'"
