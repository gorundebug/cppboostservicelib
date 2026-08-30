#!/usr/bin/env bash
set -euo pipefail

root="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
dependency_retry="$root/scripts/retry-dependency-command.sh"
source "$root/scripts/conan-cache-guard.sh"
dependency_conan_cache_guard "$0" "$@"
conan_home=${CONAN_HOME:-${HOME:?HOME is required}/.conan2}
mkdir -p "$conan_home/extensions/hooks"
install -m 0644 "$root/conan/hooks/hook_source_proxy.py" \
  "$conan_home/extensions/hooks/hook_servicegen_source_proxy.py"
install -m 0644 "$root/conan/hooks/source-proxies.generated.json" \
  "$conan_home/extensions/hooks/source-proxies.generated.json"
build_type=${1:-Release}
profile=${CPPBOOSTSERVICELIB_CONAN_PROFILE:-}
network_retry_args=(
  -cc "core.download:retry=${DEPENDENCY_COMMAND_RETRY_ATTEMPTS:-8}"
  -cc "core.download:retry_wait=${DEPENDENCY_COMMAND_RETRY_DELAY_SECONDS:-5}"
  -cc "core.net.http:max_retries=${DEPENDENCY_COMMAND_RETRY_ATTEMPTS:-8}"
  -c:h "tools.files.download:retry=${DEPENDENCY_COMMAND_RETRY_ATTEMPTS:-8}"
  -c:h "tools.files.download:retry_wait=${DEPENDENCY_COMMAND_RETRY_DELAY_SECONDS:-5}"
  -c:b "tools.files.download:retry=${DEPENDENCY_COMMAND_RETRY_ATTEMPTS:-8}"
  -c:b "tools.files.download:retry_wait=${DEPENDENCY_COMMAND_RETRY_DELAY_SECONDS:-5}"
)

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
  -o:h "openssl/*:no_engine=False"
  -o:b "openssl/*:no_engine=False"
)

# Conan's compiler.sanitizer setting is part of the standard package identity.
# Generated sanitizer builds set it for the complete host graph, so ordinary,
# ASan/UBSan and TSan binaries never alias one another. Keep downloaded source
# archives outside individual package build folders so those binary variants
# still reuse one source download cache.
install -m 0644 "$root/conan/settings_user.yml" \
  "$conan_home/settings_user.yml"
source_download_cache=${CPPBOOSTSERVICELIB_CONAN_SOURCE_CACHE:-$conan_home/source-download-cache}
mkdir -p "$source_download_cache"
source_cache_args=(
  -cc "core.sources:download_cache=$source_download_cache"
)

publish_built_graph() {
  local graph_file=$1 built_list
  local -a compression_args
  [[ "${DEPENDENCY_CONAN_PUBLISH:-0}" == "1" ]] || return 0
  built_list="${graph_file%.json}.built.json"
  conan list --graph="$graph_file" --graph-binaries=build \
    --format=json --out-file="$built_list"
  compression_args=(
    -cc "core.gzip:compresslevel=${DEPENDENCY_CONAN_UPLOAD_COMPRESSION_LEVEL:-1}"
  )
  if [[ -n "${DEPENDENCY_CONAN_UPLOAD_COMPRESSION_FORMAT:-}" ]]; then
    compression_args+=(
      -cc "core.upload:compression_format=$DEPENDENCY_CONAN_UPLOAD_COMPRESSION_FORMAT"
    )
  fi
  "$dependency_retry" conan upload --list="$built_list" \
    "${compression_args[@]}" \
    --remote=dependency-cache-write --confirm --check
}

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

output_folder=
args=("${@:2}")
for ((index = 0; index < ${#args[@]}; index++)); do
  case "${args[$index]}" in
    --output-folder=*) output_folder=${args[$index]#*=} ;;
    --output-folder|-of)
      if (( index + 1 < ${#args[@]} )); then
        output_folder=${args[$((index + 1))]}
      fi
      ;;
  esac
done
output_folder=${output_folder:-$root/build/conan-${build_type,,}}
mkdir -p "$output_folder"
graph_file="$output_folder/conan-install.graph.json"

conan install "$root" \
  "${network_retry_args[@]}" \
  --profile:host "$profile" \
  --profile:build "$profile" \
  -s:h "build_type=$build_type" \
  -s:b "build_type=$build_type" \
  --build=missing \
  "${source_cache_args[@]}" \
  "${lock_args[@]}" \
  "${options[@]}" \
  "${@:2}" \
  --format=json \
  --out-file="$graph_file"

publish_built_graph "$graph_file"
