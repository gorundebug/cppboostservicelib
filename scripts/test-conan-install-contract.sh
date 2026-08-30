#!/usr/bin/env bash
set -euo pipefail

root="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
script="$root/scripts/conan-install.sh"
source_proxy_catalog="$root/conan/hooks/source-proxies.generated.json"

install_count=$(grep -Ec '^[[:space:]]*conan install ' "$script")
if [[ "$install_count" != "1" ]]; then
  echo "conan-install.sh must resolve the complete framework graph with one conan install; found $install_count" >&2
  exit 1
fi
if grep -Fq '"$dependency_retry" conan install' "$script"; then
  echo "build failures must not retry the complete Conan install" >&2
  exit 1
fi
grep -Fq -- 'core.download:retry=' "$script" || {
  echo "Conan package downloads must use native network retries" >&2
  exit 1
}
grep -Fq -- 'tools.files.download:retry=' "$script" || {
  echo "Conan recipe source downloads must use native network retries" >&2
  exit 1
}
grep -Fq -- '-o:h "openssl/*:no_engine=False"' "$script" || {
  echo "all C++ variants must share the OpenSSL ENGINE compatibility setting in the host graph" >&2
  exit 1
}
grep -Fq -- '-o:b "openssl/*:no_engine=False"' "$script" || {
  echo "all C++ variants must share the OpenSSL ENGINE compatibility setting in the build graph" >&2
  exit 1
}
if grep -Fq -- 'core.package_id:default_' "$script"; then
  echo "the framework must use Conan's standard package ID model" >&2
  exit 1
fi
[[ -s "$source_proxy_catalog" ]] || {
  echo "generated Conan source proxy catalog is missing" >&2
  exit 1
}
grep -Fq 'source-proxies.generated.json' "$script" || {
  echo "Conan install must copy the generated source proxy catalog beside its hook" >&2
  exit 1
}
