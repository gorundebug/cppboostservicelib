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
## Service-local SubStream

`servicelib::ISubStream<T, R>` makes an existing service-local graph callable
from business code. Generated typed accessors such as `getLookupSubStream()`
can be injected through custom makers. Generated weak handles avoid ownership
cycles; keep the service alive and do not invoke a handle before graph binding.

```cpp
auto collector = std::make_shared<servicelib::SubStreamCollectorFunc<std::string>>(
    [](servicelib::MessageContext caller, const std::string& result) {
      // Store or process result for this invocation.
      return true;  // false continues collecting
    });
lookup->consume(context, payload, collector);
```

`lookup` is an injected `ISubStream<T, std::string>` handle and `payload` is
`servicelib::Payload<T>`. Interfaces and the function adapter are declared in
`servicelib/runtime/common.hpp`. Use the runtime's context and cancellation
conventions; never block all execution capacity needed by the substream.

The graph is constructed once. Per-call context state isolates concurrent and
nested invocations, and the collector receives the original caller context.
Callbacks within one call are serialized. Preserve context through business
emissions; no extra message ID parameter is necessary.

The entry `valueType` is its argument type. The existing `source` names its
reachable result producer and supplies R. There must be one body consumer;
ordinary Split can branch inside it. No separate ResultStream, error port or
transport endpoint is introduced.

True completes collection; late results are ignored, not all graph work stopped.
Use a deadline/cancellation when no completing result is guaranteed. An active
collector is drained on cancellation and must cooperate. Business failures remain
result values or graph error paths; runtime failures use exception conventions.
Shared Join state, keys and pools keep their existing behavior. Temporal is not
supported by C++/Boost.
