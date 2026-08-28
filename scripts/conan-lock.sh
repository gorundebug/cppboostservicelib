#!/usr/bin/env bash
set -euo pipefail

root="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
source "$root/scripts/conan-cache-guard.sh"
dependency_conan_cache_guard "$0" "$@"
lock_dir="$root/conan/locks"
mkdir -p "$lock_dir"

"$root/scripts/conan-configure-remotes.sh"
"$root/scripts/conan-export-recipes.sh"

options=(
  -o "&:with_grpc=True"
  -o "&:with_kafka=True"
  -o "&:with_otel=True"
  -o "&:with_cron=True"
  -o "&:with_tests=True"
)

for profile in "$root"/conan/profiles/*; do
  [[ -f "$profile" ]] || continue
  output="$lock_dir/$(basename "$profile").lock"
  conan lock create "$root" \
    --profile:host "$profile" \
    --profile:build "$profile" \
    -s:h build_type=Release \
    -s:b build_type=Release \
    "${options[@]}" \
    --lockfile-out "$output"
done
