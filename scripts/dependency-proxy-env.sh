#!/usr/bin/env bash

if [[ -n "${DEPENDENCY_PROXY_DIR:-}" ]]; then
  servicegen_proxy_docker_host="${DEPENDENCY_PROXY_DOCKER_HOST:-host.docker.internal}"
  servicegen_proxy_port="${DEPENDENCY_PROXY_PORT:-18081}"
  servicegen_proxy_base="http://${servicegen_proxy_docker_host}:${servicegen_proxy_port}/repository"
  dependency_registry_host="${DEPENDENCY_PROXY_HOST:-localhost}"
  dependency_registry_port="${DEPENDENCY_PROXY_DOCKER_PORT:-18083}"

  export DEPENDENCY_CONAN_HOME="${DEPENDENCY_PROXY_DIR}/conan2"
  export DEPENDENCY_DOCKER_REGISTRY="${dependency_registry_host}:${dependency_registry_port}"
  export DEPENDENCY_GITHUB_RAW_URL="${servicegen_proxy_base}/github-raw"
  export DEPENDENCY_CONAN_REMOTE_URL="${servicegen_proxy_base}/conan-group"
  export DEPENDENCY_CONAN_UPLOAD_URL="${servicegen_proxy_base}/conan-hosted"
  export DEPENDENCY_CONAN_PUBLISH=1
  export DEPENDENCY_CONAN_CREDENTIAL_FILE="${DEPENDENCY_PROXY_DIR%/}/conan.publisher.credential"
  export PIP_INDEX_URL="${servicegen_proxy_base}/pypi-proxy/simple"
  export PIP_TRUSTED_HOST="${servicegen_proxy_docker_host}"
  export DEPENDENCY_APT_UBUNTU_ARCHIVE_URL="${servicegen_proxy_base}/apt-ubuntu-archive"
  export DEPENDENCY_APT_UBUNTU_SECURITY_URL="${servicegen_proxy_base}/apt-ubuntu-security"
  export DEPENDENCY_APT_UBUNTU_PORTS_URL="${servicegen_proxy_base}/apt-ubuntu-ports"
fi
