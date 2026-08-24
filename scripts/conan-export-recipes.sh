#!/usr/bin/env bash
set -euo pipefail

root="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
versions="$root/conan/dependencies_generated.py"

version() {
  python3 "$versions" "$1"
}

conan export "$root/conan/recipes/libcron" --version "$(version libcron)"
conan export "$root/conan/recipes/gtest" --version "$(version googletest)"
conan export "$root/conan/recipes/grpc" --version "$(version grpc)"
conan export "$root/conan/recipes/opentelemetry-proto" --version "$(version opentelemetry-proto)"
conan export "$root/conan/recipes/opentelemetry-cpp" --version "$(version opentelemetry-cpp)"
