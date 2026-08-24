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

The canonical dependency-complete build runs Conan 2, CMake and the compiler
inside Docker; the host needs only Docker:

```bash
./scripts/test-conan.sh Debug
./scripts/test-conan.sh Release
```

Dependency versions originate in ServiceGen's dependency catalog. Generated
Conan manifests, recipes and the committed platform lockfiles make every
resolved recipe revision explicit. After an intentional dependency or recipe
change, refresh all lockfiles in the same Docker toolchain:

```bash
docker run --rm -v "$PWD:/workspace" -w /workspace \
  cppboostservicelib-conan-build ./scripts/conan-lock.sh
```

Host CMake builds remain available for framework development when their
dependencies are already installed. The same presets are available as
`release`, `asan`, `ubsan`, `asan-ubsan`, `tsan`, `coverage` and `profiling`.
Profiling builds retain debug symbols and frame pointers. GDB, LLDB, core-dump
and profiler commands are documented in [`docs/PROFILING.md`](docs/PROFILING.md).

```bash
cmake --preset debug
cmake --build --preset debug --parallel
ctest --preset debug
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
