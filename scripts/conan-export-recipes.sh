#!/usr/bin/env bash
set -euo pipefail

root="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
conan export "$root/conan/recipes/libcron" --version 1.3.3
conan export "$root/conan/recipes/gtest" --version 1.15.2
conan export "$root/conan/recipes/grpc" --version 1.71.0
conan export "$root/conan/recipes/opentelemetry-proto" --version 1.5.0
conan export "$root/conan/recipes/opentelemetry-cpp" --version 1.20.0
