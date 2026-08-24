#!/usr/bin/env bash
set -euo pipefail

root="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
image=${CPPBOOSTSERVICELIB_CONAN_IMAGE:-cppboostservicelib-conan-build}
build_type=${1:-Debug}

docker build \
  --build-arg "PIP_INDEX_URL=${PIP_INDEX_URL:-https://pypi.org/simple}" \
  -f "$root/Dockerfile.cmake" \
  -t "$image" \
  "$root"

docker run --rm \
  -e CONAN_HOME=/conan \
  -e CPPBOOSTSERVICELIB_BUILD_TESTS=True \
  -e CPPBOOSTSERVICELIB_ENABLE_CRON=True \
  -e CPPBOOSTSERVICELIB_ENABLE_GRPC=True \
  -e CPPBOOSTSERVICELIB_ENABLE_KAFKA=True \
  -e CPPBOOSTSERVICELIB_ENABLE_OTEL=False \
  -e SERVICEGEN_GITHUB_RAW_URL="${SERVICEGEN_GITHUB_RAW_URL:-}" \
  -v cppboostservicelib-conan2:/conan \
  -v cppboostservicelib-conan-ccache:/ccache \
  -v "$root:/workspace" \
  -w /workspace \
  "$image" \
  bash -euo pipefail -c '
    export CCACHE_DIR=/ccache
    export CCACHE_BASEDIR=/workspace
    export CCACHE_COMPILERCHECK=content
    ./scripts/conan-install.sh '"$build_type"'
    preset=conan-'"$(printf '%s' "$build_type" | tr '[:upper:]' '[:lower:]')"'
    cmake --preset "$preset"
    cmake --build --preset "$preset" --parallel
    ctest --preset "$preset" --output-on-failure
  '
