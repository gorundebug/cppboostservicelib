#!/usr/bin/env bash
set -euo pipefail

root="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
build_type=${1:-Release}
profile=${CPPBOOSTSERVICELIB_CONAN_PROFILE:-}

if [[ -z "$profile" ]]; then
  case "$(uname -s):$(uname -m)" in
    Linux:aarch64|Linux:arm64)
      profile="$root/conan/profiles/linux-gcc-armv8"
      ;;
    Linux:x86_64)
      profile="$root/conan/profiles/linux-gcc-x86_64"
      ;;
    Darwin:arm64)
      profile="$root/conan/profiles/macos-apple-clang-armv8"
      ;;
    *)
      echo "unsupported Conan host: $(uname -s) $(uname -m); set CPPBOOSTSERVICELIB_CONAN_PROFILE" >&2
      exit 1
      ;;
  esac
fi

options=(
  -o "&:with_grpc=${CPPBOOSTSERVICELIB_ENABLE_GRPC:-True}"
  -o "&:with_kafka=${CPPBOOSTSERVICELIB_ENABLE_KAFKA:-True}"
  -o "&:with_otel=${CPPBOOSTSERVICELIB_ENABLE_OTEL:-False}"
  -o "&:with_cron=${CPPBOOSTSERVICELIB_ENABLE_CRON:-True}"
  -o "&:with_tests=${CPPBOOSTSERVICELIB_BUILD_TESTS:-True}"
)

# Sanitizers are compiler sub-settings, rather than root-only CMake flags, so
# every affected dependency gets a distinct package ID. This prevents mixing
# instrumented consumers with ordinary inline implementations from packages
# such as Abseil. Keep downloaded source archives outside individual package
# build folders so Debug, Release and sanitizer variants reuse one source
# download cache.
conan_home=$(conan config home)
install -m 0644 "$root/conan/settings_user.yml" \
  "$conan_home/settings_user.yml"
source_download_cache=${CPPBOOSTSERVICELIB_CONAN_SOURCE_CACHE:-$conan_home/source-download-cache}
mkdir -p "$source_download_cache"
source_cache_args=(
  -cc "core.sources:download_cache=$source_download_cache"
)

# Conan keeps remote recipe archives in per-reference download directories.
# An interrupted download may leave conan_export.tgz behind without a usable
# recipe and the next install then fails with "file to download already
# exists". These folders are explicitly non-critical cache state; clean them
# before resolving while retaining recipes, packages, sources and build data.
conan cache clean "*" --download --temp >/dev/null

lockfile=${CPPBOOSTSERVICELIB_CONAN_LOCKFILE:-}
if [[ -z "$lockfile" ]]; then
  lockfile="$root/conan/locks/$(basename "$profile").lock"
fi
lock_args=()
if [[ "$lockfile" != "none" ]]; then
  if [[ ! -f "$lockfile" ]]; then
    echo "Conan lockfile is missing: $lockfile; run scripts/conan-lock.sh" >&2
    exit 1
  fi
  lock_args=(--lockfile "$lockfile")
fi

"$root/scripts/conan-configure-remotes.sh"
"$root/scripts/conan-export-recipes.sh"

exec conan install "$root" \
  --profile:host "$profile" \
  --profile:build "$profile" \
  -s:h "build_type=$build_type" \
  -s:b "build_type=$build_type" \
  --build=missing \
  "${source_cache_args[@]}" \
  "${lock_args[@]}" \
  "${options[@]}" \
  "${@:2}"
