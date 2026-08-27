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
    && cmake --install build/grpc-docker \
      --prefix /workspace/build/grpc-install \
    && cmake --fresh -S tests/grpc_consumer -B build/grpc-consumer -G Ninja \
      -DCMAKE_PREFIX_PATH=/workspace/build/grpc-install \
    && cmake --build build/grpc-consumer --parallel \
    && ./build/grpc-consumer/cppboostservicelib_grpc_consumer"
