#!/usr/bin/env bash
set -euo pipefail

root="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
conan export "$root/conan/recipes/libcron" --version 1.3.3
