# ServiceLib for C++/Boost

C++20 ServiceLib runtime based on Boost.Asio, Boost.Beast and asio-grpc.

Core tests run with `./scripts/test.sh`. The optional gRPC clean-machine
build, standard protobuf generation and loopback integration tests run with
`./scripts/test-grpc.sh`; its `build/grpc-docker` directory is intentionally
reused as the dependency/compiler cache. The heavier
`./scripts/test-grpc-package.sh` additionally builds every installable FETCH
target, installs the complete package chain and compiles an external
`find_package` consumer.

The implementation preserves the public graph and configuration semantics of
the existing Go and C++ ServiceLib runtimes while removing the userver runtime
and component model.

The practical build, generation, test, benchmark and profiling command index is
[`docs/GETTING_STARTED.md`](docs/GETTING_STARTED.md).

## Initial build

The host must provide CMake, Ninja, a C++20 compiler and Boost headers.

```bash
cmake --preset debug
cmake --build --preset debug --parallel
ctest --preset debug
```

The same workflow is available as `release`, `asan`, `ubsan`,
`asan-ubsan`, `tsan`, `coverage` and `profiling` presets. Profiling builds
retain debug symbols and frame pointers. GDB, LLDB, core-dump and profiler
commands are documented in [`docs/PROFILING.md`](docs/PROFILING.md).

The canonical reproducible build is Docker-based:

```bash
./scripts/test.sh
```

For a host without preinstalled Boost or yaml-cpp, use pinned source archives:

```bash
cmake -S . -B build/fetch -G Ninja \
  -DCPPBOOSTSERVICELIB_DEPENDENCY_MODE=FETCH
cmake --build build/fetch --parallel
```

Generated services use the same two-file configuration entry point as the Go
runtime. The defaults can be overridden explicitly:

```bash
./service \
  --config ./config/config.yaml \
  --values ./config/overrides.yaml \
  --workers 2
```
