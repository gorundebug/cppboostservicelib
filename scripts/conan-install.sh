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
  "${lock_args[@]}" \
  "${options[@]}" \
  "${@:2}"
