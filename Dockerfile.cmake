FROM ubuntu:24.04

ARG TARGETARCH
RUN rm -f /etc/apt/apt.conf.d/docker-clean
RUN --mount=type=cache,id=servicegen-apt-lists-${TARGETARCH},target=/var/lib/apt/lists,sharing=locked \
    --mount=type=cache,id=servicegen-apt-cache-${TARGETARCH},target=/var/cache/apt,sharing=locked \
    apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
       build-essential ca-certificates ccache cmake git ninja-build pkg-config \
       libboost-dev libboost-json1.83-dev libssl-dev libyaml-cpp-dev \
       libjemalloc-dev librdkafka-dev zlib1g-dev

WORKDIR /workspace
