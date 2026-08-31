#!/usr/bin/env bash
set -euo pipefail

root="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
source "$root/scripts/conan-cache-guard.sh"
dependency_conan_cache_guard "$0" "$@"
conan_home=${CONAN_HOME:-${HOME:?HOME is required}/.conan2}
mkdir -p "$conan_home/extensions/hooks"
install -m 0644 "$root/conan/hooks/hook_source_proxy.py" \
  "$conan_home/extensions/hooks/hook_servicegen_source_proxy.py"
install -m 0644 "$root/conan/hooks/source-proxies.generated.json" \
  "$conan_home/extensions/hooks/source-proxies.generated.json"
lock_dir="$root/conan/locks"
mkdir -p "$lock_dir"

"$root/scripts/conan-configure-remotes.sh"
"$root/scripts/conan-export-recipes.sh"

options=(
  -o "&:with_grpc=True"
  -o "&:with_kafka=True"
  -o "&:with_otel=True"
  -o "&:with_tests=True"
  -o:h "openssl/*:no_engine=False"
  -o:b "openssl/*:no_engine=False"
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
