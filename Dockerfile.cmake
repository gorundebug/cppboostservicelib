ARG DEPENDENCY_DOCKER_REGISTRY=docker.io
FROM ${DEPENDENCY_DOCKER_REGISTRY}/library/ubuntu:24.04

ARG TARGETARCH
ARG PIP_INDEX_URL=https://pypi.org/simple
ARG PIP_TRUSTED_HOST=
ARG DEPENDENCY_APT_UBUNTU_ARCHIVE_URL=
ARG DEPENDENCY_APT_UBUNTU_SECURITY_URL=
ARG DEPENDENCY_APT_UBUNTU_PORTS_URL=
RUN if [ -n "$DEPENDENCY_APT_UBUNTU_ARCHIVE_URL$DEPENDENCY_APT_UBUNTU_SECURITY_URL$DEPENDENCY_APT_UBUNTU_PORTS_URL" ]; then \
      find /etc/apt -type f \( -name '*.list' -o -name '*.sources' \) -exec sed -i \
        -e "s|http://archive.ubuntu.com/ubuntu|$DEPENDENCY_APT_UBUNTU_ARCHIVE_URL|g" \
        -e "s|http://security.ubuntu.com/ubuntu|$DEPENDENCY_APT_UBUNTU_SECURITY_URL|g" \
        -e "s|http://ports.ubuntu.com/ubuntu-ports|$DEPENDENCY_APT_UBUNTU_PORTS_URL|g" {} +; \
    fi
RUN rm -f /etc/apt/apt.conf.d/docker-clean
RUN --mount=type=cache,id=servicegen-apt-lists-${TARGETARCH},target=/var/lib/apt/lists,sharing=locked \
    --mount=type=cache,id=servicegen-apt-cache-${TARGETARCH},target=/var/cache/apt,sharing=locked \
    apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
       build-essential ca-certificates ccache cmake git ninja-build pkg-config \
       python3-venv \
       libboost-dev libboost-json1.83-dev libssl-dev libyaml-cpp-dev \
       libjemalloc-dev librdkafka-dev zlib1g-dev

COPY conan/dependencies_generated.py /tmp/dependencies_generated.py
RUN CONAN_VERSION="$(python3 /tmp/dependencies_generated.py conan)" \
    && python3 -m venv /opt/conan \
    && PIP_TRUSTED_HOST="$PIP_TRUSTED_HOST" \
       /opt/conan/bin/pip install --no-cache-dir --index-url "$PIP_INDEX_URL" \
       "conan==$CONAN_VERSION" \
    && rm -f /tmp/dependencies_generated.py

ENV PATH=/opt/conan/bin:$PATH

WORKDIR /workspace
