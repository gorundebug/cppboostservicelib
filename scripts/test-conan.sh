#!/usr/bin/env bash
set -euo pipefail

root="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
source "$root/scripts/dependency-proxy-env.sh"
image=${CPPBOOSTSERVICELIB_CONAN_IMAGE:-cppboostservicelib-conan-build}
build_type=${1:-Debug}

docker_build_args=()
docker_run_args=()
conan_home_mount=cppboostservicelib-conan2:/conan
docker_build_args+=(
  --build-arg "DEPENDENCY_DOCKER_REGISTRY=${DEPENDENCY_DOCKER_REGISTRY:-docker.io}"
)
if [[ -n "${DEPENDENCY_PROXY_DIR:-}" ]]; then
  proxy_host=${DEPENDENCY_PROXY_DOCKER_HOST:-host.docker.internal}
  proxy_port=${DEPENDENCY_PROXY_PORT:-18081}
  proxy_base="http://${proxy_host}:${proxy_port}/repository"
  conan_home="$DEPENDENCY_PROXY_DIR/conan2"
  mkdir -p "$conan_home"
  conan_home_mount="$conan_home:/conan"
  docker_build_args+=(
    --add-host host.docker.internal:host-gateway
    --build-arg "PIP_INDEX_URL=$proxy_base/pypi-proxy/simple"
    --build-arg "PIP_TRUSTED_HOST=$proxy_host"
    --build-arg "DEPENDENCY_APT_UBUNTU_ARCHIVE_URL=$proxy_base/apt-ubuntu-archive"
    --build-arg "DEPENDENCY_APT_UBUNTU_SECURITY_URL=$proxy_base/apt-ubuntu-security"
    --build-arg "DEPENDENCY_APT_UBUNTU_PORTS_URL=$proxy_base/apt-ubuntu-ports"
  )
  docker_run_args+=(
    --add-host host.docker.internal:host-gateway
    -e "DEPENDENCY_CONAN_REMOTE_URL=$proxy_base/conan-group"
    -e "DEPENDENCY_CONAN_UPLOAD_URL=$proxy_base/conan-hosted"
    -e "DEPENDENCY_CONAN_PUBLISH=1"
    -e "DEPENDENCY_CONAN_CREDENTIAL_FILE=/run/secrets/dependency_conan_credential"
    -e "DEPENDENCY_GITHUB_RAW_URL=$proxy_base/github-raw"
    -v "${DEPENDENCY_CONAN_CREDENTIAL_FILE}:/run/secrets/dependency_conan_credential:ro"
  )
else
  docker_build_args+=(
    --build-arg "PIP_INDEX_URL=${PIP_INDEX_URL:-https://pypi.org/simple}"
  )
  docker_run_args+=(
    -e "DEPENDENCY_GITHUB_RAW_URL=${DEPENDENCY_GITHUB_RAW_URL:-}"
  )
fi

docker build \
  "${docker_build_args[@]}" \
  -f "$root/Dockerfile.cmake" \
  -t "$image" \
  "$root"

docker run --rm \
  "${docker_run_args[@]}" \
  -e CONAN_HOME=/conan \
  -e CPPBOOSTSERVICELIB_BUILD_TESTS=True \
  -e CPPBOOSTSERVICELIB_ENABLE_GRPC=True \
  -e CPPBOOSTSERVICELIB_ENABLE_KAFKA=True \
  -e "CPPBOOSTSERVICELIB_ENABLE_OTEL=${CPPBOOSTSERVICELIB_ENABLE_OTEL:-False}" \
  -v "$conan_home_mount" \
  -v cppboostservicelib-conan-ccache:/ccache \
  -v "$root:/workspace" \
  -w /workspace \
  "$image" \
  bash -euo pipefail -c '
    export CCACHE_DIR=/ccache
    export CCACHE_BASEDIR=/workspace
    export CCACHE_COMPILERCHECK=content
    ./scripts/conan-install.sh '"$build_type"'
    preset=conan-'"$(printf '%s' "$build_type" | tr '[:upper:]' '[:lower:]')"'
    cmake --fresh --preset "$preset"
    cmake --build --preset "$preset" --parallel
    ctest --preset "$preset" --output-on-failure
  '
