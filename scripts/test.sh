#!/usr/bin/env bash
set -euo pipefail

ROOT="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"

if [[ -n "${CPPBOOSTSERVICELIB_TEST_SOURCE_CACHE_DIR:-}" ]]; then
  if [[ ! -d "${CPPBOOSTSERVICELIB_TEST_SOURCE_CACHE_DIR}" ]]; then
    echo "C++ source cache does not exist: ${CPPBOOSTSERVICELIB_TEST_SOURCE_CACHE_DIR}" >&2
    exit 1
  fi
  set -- -v "${CPPBOOSTSERVICELIB_TEST_SOURCE_CACHE_DIR}:/servicegen-cpp-source-cache:ro"
else
  set --
fi

if [[ -n "${CPPBOOSTSERVICELIB_TEST_BUILD_VOLUME:-}" ]]; then
  set -- "$@" -v "${CPPBOOSTSERVICELIB_TEST_BUILD_VOLUME}:/workspace/build"
fi

docker build -f "$ROOT/Dockerfile.cmake" -t cppboostservicelib-build "$ROOT"
docker run --rm \
  -e CCACHE_DIR=/ccache \
  -e CCACHE_BASEDIR=/workspace \
  -e CCACHE_COMPILERCHECK=content \
  -e CCACHE_MAXSIZE="${CCACHE_MAXSIZE:-20G}" \
  -v cppboostservicelib-ccache:/ccache \
  "$@" \
  -v "$ROOT:/workspace" -w /workspace \
  cppboostservicelib-build \
  bash -lc '
    cache_reset=()
    source_cache_args=()
    if [[ -f /servicegen-cpp-source-cache/conformance-cache.cmake ]]; then
      source_cache_args=(-C /servicegen-cpp-source-cache/conformance-cache.cmake)
    fi
    if [[ ! -d /servicegen-cpp-source-cache ]]; then
      cache_reset=(-U "FETCHCONTENT_SOURCE_DIR_*" -U OTELCPP_PROTO_PATH)
    fi
    cmake -S . -B build/docker -G Ninja "${cache_reset[@]}" \
      "${source_cache_args[@]}" \
      -DCMAKE_BUILD_TYPE=Debug \
      -DCMAKE_INSTALL_PREFIX=/workspace/build/docker-install \
      -DCPPBOOSTSERVICELIB_BUILD_TESTS=ON \
      -DCPPBOOSTSERVICELIB_ENABLE_KAFKA=ON \
      -DCPPBOOSTSERVICELIB_ENABLE_CRON=ON \
    && cmake --build build/docker --parallel \
    && ctest --test-dir build/docker --output-on-failure \
    && cmake --install build/docker \
    && cmake -S tests/consumer -B build/consumer -G Ninja \
      -DCMAKE_PREFIX_PATH=/workspace/build/docker-install \
    && cmake --build build/consumer --parallel \
    && ./build/consumer/cppboostservicelib_consumer
  '
