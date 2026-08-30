#!/usr/bin/env bash
set -euo pipefail

root="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
versions="$root/conan/dependencies_generated.py"

version() {
  python3 "$versions" "$1"
}

conan export "$root/conan/recipes/libcron" --version "$(version libcron)" --user gorundebug --channel boost
conan export "$root/conan/recipes/librdkafka" --version "$(version librdkafka)" --user gorundebug --channel boost
conan export "$root/conan/recipes/gtest" --version "$(version googletest)" --user gorundebug --channel boost
conan export "$root/conan/recipes/grpc" --version "$(version grpc)" --user gorundebug --channel boost
conan export "$root/conan/recipes/opentelemetry-proto" --version "$(version opentelemetry-proto)" --user gorundebug --channel boost
conan export "$root/conan/recipes/opentelemetry-cpp" --version "$(version opentelemetry-cpp)" --user gorundebug --channel boost
