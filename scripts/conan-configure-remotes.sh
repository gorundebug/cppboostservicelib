#!/usr/bin/env bash
set -euo pipefail
root="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
dependency_retry="$root/scripts/retry-dependency-command.sh"

if [[ -n "${DEPENDENCY_CONAN_REMOTE_URL:-}" ]]; then
  conan remote remove conancenter >/dev/null 2>&1 || true
  conan remote remove servicegen-nexus >/dev/null 2>&1 || true
  if conan remote list | grep -q '^dependency-proxy:'; then
    conan remote update dependency-proxy --url "$DEPENDENCY_CONAN_REMOTE_URL" --insecure
  else
    conan remote add dependency-proxy "$DEPENDENCY_CONAN_REMOTE_URL" --insecure
  fi
  conan remote remove dependency-cache-write >/dev/null 2>&1 || true
  if [[ "${DEPENDENCY_CONAN_PUBLISH:-0}" == "1" ]]; then
    upload_url=${DEPENDENCY_CONAN_UPLOAD_URL:?DEPENDENCY_CONAN_UPLOAD_URL is required when DEPENDENCY_CONAN_PUBLISH=1}
    credential_file=${DEPENDENCY_CONAN_CREDENTIAL_FILE:-/run/secrets/dependency_conan_credential}
    if [[ ! -s "$credential_file" ]]; then
      echo "Conan publisher credential is missing: $credential_file" >&2
      exit 2
    fi
    username=$(sed -n '1p' "$credential_file")
    password=$(sed -n '2p' "$credential_file")
    if [[ -z "$username" || -z "$password" ]]; then
      echo "Conan publisher credential must contain username and password" >&2
      exit 2
    fi
    conan remote add dependency-cache-write "$upload_url" --insecure
    "$dependency_retry" conan remote login dependency-cache-write \
      "$username" -p "$password"
  fi
  exit 0
fi

conan remote remove servicegen-nexus >/dev/null 2>&1 || true
conan remote remove dependency-proxy >/dev/null 2>&1 || true
conan remote remove dependency-cache-write >/dev/null 2>&1 || true
if conan remote list | grep -q '^conancenter:'; then
  conan remote update conancenter --url https://center2.conan.io
else
  conan remote add conancenter https://center2.conan.io
fi
