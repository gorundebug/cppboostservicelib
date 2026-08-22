# Build, run and diagnose

This is the command-oriented entry point for `cppboostservicelib`, its
generated example and the repository-level verification tools. All CMake
builds below use unrestricted `--parallel`.

## Requirements

The reproducible path needs Git, Docker with the Compose plugin, Python 3 and
GNU Make. Host builds additionally need CMake 3.24+, Ninja and a C++20 compiler.
Pinned Docker/FETCH builds compile Boost, protobuf, gRPC, asio-grpc, yaml-cpp,
librdkafka and OpenTelemetry from the revisions selected by the framework;
they do not silently use a different system gRPC.

## Framework

From `cppboostservicelib`:

```bash
# Fast framework/unit matrix in its canonical Docker environment.
./scripts/test.sh

# Pinned protobuf/gRPC generation, build and loopback tests.
./scripts/test-grpc.sh

# Install the package and compile an external find_package/FETCH consumer.
./scripts/test-grpc-package.sh
```

Optional host builds:

```bash
cmake --preset debug
cmake --build --preset debug --parallel
ctest --preset debug

cmake --preset release
cmake --build --preset release --parallel
ctest --preset release
```

Replace `debug` with `asan`, `ubsan`, `asan-ubsan`, `tsan`, `coverage`,
`profiling` or `coroutine-diagnostics` when that instrument is required.

For a host without installed dependencies:

```bash
cmake -S . -B build/fetch -G Ninja \
  -DCMAKE_BUILD_TYPE=Release \
  -DCPPBOOSTSERVICELIB_DEPENDENCY_MODE=FETCH
cmake --build build/fetch --parallel
ctest --test-dir build/fetch --output-on-failure
```

## Local C++ build cache

Docker builds use two persistent local caches automatically:

- BuildKit cache mounts retain downloaded `apt` packages and the native
  example's pinned CMake `FetchContent` sources;
- `ccache` retains C++ compilation results in a Docker volume and is selected
  automatically by CMake whenever the executable is available.

The first build on a clean machine is still a cold build. Later builds reuse
the caches when application sources change. No proxy or host package-manager
configuration is required. The caches are local to the active Docker builder
and are never copied into runtime images or Git repositories.

Set the maximum compiler-cache size when invoking a generated example:

```bash
CCACHE_MAXSIZE=40G make cpp-build
```

Inspect its statistics from `cppboostexample`:

```bash
docker compose -f docker-compose.cmake.generated.yml run --build --rm \
  cpp-build ccache --show-stats
```

For a host CMake build, install `ccache`; CMake detects it without additional
flags. Disable it for a diagnostic build with
`-DSERVICEGEN_USE_CCACHE=OFF`. `make cpp-clean` removes the generated
example's build and compiler-cache volumes; Docker's BuildKit download cache
is managed separately by the selected Docker builder.

## Generated example

Use an absolute framework checkout as the Docker build context. From
`cppboostexample`:

```bash
export SERVICELIB_SOURCE_CONTEXT=/absolute/path/to/cppboostservicelib

# Repository-only Release build, tests and live two-service scenario.
./scripts/quickstart.generated.sh

# Configure, compile and run all generated unit/config tests.
./scripts/test.generated.sh docker-release

# Build, start both generated services and run CTest integration cases.
./scripts/integration-test.generated.sh

# Full runtime sanitizer gates.
./scripts/sanitizer-test.generated.sh asan
./scripts/sanitizer-test.generated.sh tsan
```

Start the example for manual requests:

```bash
SERVICELIB_SOURCE_CONTEXT=/absolute/path/to/cppboostservicelib \
  docker compose up --build
```

Order Service listens on `http://127.0.0.1:9091`; Inventory Service listens on
`http://127.0.0.1:9092`. Stop and remove the containers with:

```bash
docker compose down --timeout 30
```

## Native comparison example

The native baseline uses Boost.Beast and asio-grpc directly and intentionally
does not link the framework. From `cppboostnativeexample`:

```bash
# Build the Release service images from the pinned dependency sources.
docker compose build inventoryservice orderservice

# Run unit plus live HTTP-to-gRPC integration tests.
./scripts/test.sh

# Build and execute the sanitizer integration targets.
docker build --target asan-test \
  -t cppboostnativeexample-asan-test:latest .
docker build --target tsan-test \
  -t cppboostnativeexample-tsan-test:latest .

# Start both services for manual requests.
docker compose up --build
```

Clean Docker configuration and build stages print their underlying CMake/Ninja
output plus a heartbeat every 15 seconds while a pinned dependency download or
configuration step is otherwise silent. Builds always use unrestricted
`--parallel`.

A generated binary accepts the same two-file configuration interface as Go
and Rust:

```bash
./example_order_service \
  --config orderservice/config/config.yaml \
  --values orderservice/config/overrides.yaml \
  --workers 2
```

## Generate and merge without replacing business logic

The release gate generates the complete canonical Boost archive, merges it
into a disposable copy of `cppboostexample`, proves every user-owned file and
mode byte-identical, and then performs clean Docker unit and integration
builds:

```bash
make -C /absolute/path/to/conformance generation
```

The manifests, SHA-256 hashes, merge diff and test logs are retained under
`conformance/.artifacts/generation/`. For generation/merge diagnosis without
the mandatory build:

```bash
python3 /absolute/path/to/conformance/generation/run.py --skip-docker
```

To intentionally refresh all checked-out canonical language examples, run
`servicegen/scripts/refresh_examples.sh`. Existing files whose basename does
not contain `generated` are preserved by the merge contract.

## Conformance

From the workspace root:

```bash
make -C conformance all

# Useful focused gates:
make -C conformance structure signatures config config-runtime
make -C conformance pools operators serde transports
make -C conformance tracing metrics scenarios generation profiling
```

Every runner writes a machine-readable summary below
`conformance/.artifacts/<gate>/summary.json`.

## Comparative benchmark

Run Make in conformance's embedded `benchmarks/examples` directory, and point
`BENCHMARK_DEPENDENCIES_DIR` at the directory that contains the local example
projects. For this workspace layout:

```bash
make -C /absolute/path/to/conformance/benchmarks/examples run \
  BENCHMARK_DEPENDENCIES_DIR=/absolute/path/to/stream_app_go \
  CORES=2 \
  LOADGEN_CORES=6 \
  VUS=256 \
  DURATION=60s \
  WARMUP=5s \
  RUNS=3
```

Find the saturation point by increasing closed virtual users:

```bash
make -C /absolute/path/to/conformance/benchmarks/examples capacity \
  BENCHMARK_DEPENDENCIES_DIR=/absolute/path/to/stream_app_go \
  CAPACITY_LANGUAGES="cpp-boost cpp-boost-native" \
  CORES=2 LOADGEN_CORES=6 \
  START_VUS=32 VUS_STEP=32 MAX_VUS=1024 \
  CAPACITY_DURATION=20s CAPACITY_ATTEMPTS=3 \
  MAX_P95_MS=100 MAX_P99_MS=200 MIN_RPS_GAIN_PERCENT=2
```

Normal benchmark commands do not change host-wide sysctls. A dedicated
userver comparison host that exhausts its coroutine mapping limit may opt in
with `MAX_MAP_COUNT=1048576`; the default is `0`.

## Profiling

From the `profiling` repository:

```bash
python3 examples/run.py \
  --language cppboost --language cppboost-native \
  --cores 2 --loadgen-cores 6 --vus 256 \
  --warmup 5s --duration 20s \
  --profile-kind cpu --profile-kind allocation \
  --profile-kind scheduler --profile-kind offcpu
```

The normal profiler does not mutate host settings. If `perf` attachment is
denied, a dedicated profiling host can explicitly use
`--prepare-host-profiling`. A dedicated high-concurrency userver run can
explicitly use `--max-map-count 1048576`.

Symbolized CPU and off-CPU flamegraphs/folded stacks, allocation counters,
sampled allocation call-site/byte flamegraphs and folded stacks, scheduler
data, load results and runtime metrics are written under
`profiling/examples/.artifacts/`.
Allocation sampling defaults to one stack per 4096 allocation calls; use
`--allocation-stack-sample-every N` to change only the profiling interceptor's
sampling interval. It is profiling-only and is not linked into normal builds
or enabled by benchmark commands.
See [PROFILING.md](./PROFILING.md) for GDB, LLDB, core-dump and detailed perf
commands.
