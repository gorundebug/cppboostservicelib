FROM ubuntu:24.04

ARG TARGETARCH
ARG SERVICEGEN_APT_UBUNTU_ARCHIVE_URL=
ARG SERVICEGEN_APT_UBUNTU_SECURITY_URL=
ARG SERVICEGEN_APT_UBUNTU_PORTS_URL=
RUN if [ -n "$SERVICEGEN_APT_UBUNTU_ARCHIVE_URL$SERVICEGEN_APT_UBUNTU_SECURITY_URL$SERVICEGEN_APT_UBUNTU_PORTS_URL" ]; then \
      find /etc/apt -type f \( -name '*.list' -o -name '*.sources' \) -exec sed -i \
        -e "s|http://archive.ubuntu.com/ubuntu|$SERVICEGEN_APT_UBUNTU_ARCHIVE_URL|g" \
        -e "s|http://security.ubuntu.com/ubuntu|$SERVICEGEN_APT_UBUNTU_SECURITY_URL|g" \
        -e "s|http://ports.ubuntu.com/ubuntu-ports|$SERVICEGEN_APT_UBUNTU_PORTS_URL|g" {} +; \
    fi
RUN rm -f /etc/apt/apt.conf.d/docker-clean
RUN --mount=type=cache,id=servicegen-apt-lists-${TARGETARCH},target=/var/lib/apt/lists,sharing=locked \
    --mount=type=cache,id=servicegen-apt-cache-${TARGETARCH},target=/var/cache/apt,sharing=locked \
    apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
       build-essential ca-certificates ccache cmake git ninja-build pkg-config \
       libboost-dev libboost-json1.83-dev libssl-dev libyaml-cpp-dev \
       libjemalloc-dev librdkafka-dev zlib1g-dev

WORKDIR /workspace
