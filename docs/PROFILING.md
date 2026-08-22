# Debugging and profiling

Use the checked-in `profiling` preset for optimized binaries that retain debug
symbols and frame pointers:

```bash
cmake --preset profiling
cmake --build --preset profiling --parallel
ctest --test-dir build/profiling --output-on-failure
```

Do not add a numeric job limit. The build must use every processor made
available to the host or container.

## Interactive debugging

Run a service under GDB on Linux:

```bash
gdb --args ./build/profiling/service \
  --config ./config/config.yaml \
  --values ./config/overrides.yaml \
  --workers 2
```

Run the same binary under LLDB on macOS:

```bash
lldb -- ./build/profiling/service \
  --config ./config/config.yaml \
  --values ./config/overrides.yaml \
  --workers 2
```

Inside either debugger, use `run`, then `thread apply all bt full` in GDB or
`thread backtrace all` in LLDB after a stop.

## Core dumps

Enable core dumps in the shell that starts the service:

```bash
ulimit -c unlimited
```

On Linux, inspect the current destination with `cat /proc/sys/kernel/core_pattern`.
If systemd captures the crash, use `coredumpctl list` followed by
`coredumpctl debug <PID-or-executable>`. For a plain core file use:

```bash
gdb ./build/profiling/service /path/to/core
```

For a Docker run that must capture or debug a crash, add the narrow runtime
permissions explicitly:

```bash
docker run --ulimit core=-1 --cap-add=SYS_PTRACE \
  --security-opt seccomp=unconfined IMAGE COMMAND
```

On macOS, crash reports are written under `~/Library/Logs/DiagnosticReports`.
Attach LLDB to a live process with `lldb -p PID` when macOS permissions allow
it.

## Sampling profiles

Linux `perf` can record both user and kernel stacks because the profiling
preset preserves frame pointers:

```bash
perf record -F 99 -g -- ./build/profiling/service \
  --config ./config/config.yaml \
  --values ./config/overrides.yaml \
  --workers 2
perf report
```

For comparative benchmark profiles, use the repository-level profiling runner.
It applies the same CPU quotas and workload to framework and native services
and produces the existing folded-stack/flamegraph artifacts; do not profile an
unconstrained ad-hoc run when comparing implementations.

The runner also captures timestamped Boost runtime metrics while the workload
is active. Correlate `runtime_active_work`, `runtime_worker_utilization` and
`runtime_event_loop_lag_seconds` in
`cppboost.<service>.runtime-metrics.json` with the load-generator result and
folded stacks. Profiling must fail if this artifact cannot be collected; a
post-run `/metrics` scrape is not equivalent because active work has already
drained by then.

Active work and utilization are sampled from per-worker thread CPU clocks; no
executor wrapper is installed. A metrics scrape is itself Asio work and can
therefore contribute to the current CPU sample. Event-loop lag is sampled
independently by the Asio timer.

## Coroutine/executor diagnostics

Queued/running/suspended handler diagnostics are an explicit intrusive mode,
not a comparative benchmark setting. Build and test the framework directly
with:

```bash
cmake --preset coroutine-diagnostics
cmake --build --preset coroutine-diagnostics --parallel
ctest --preset coroutine-diagnostics
```

For both generated services under the repository profiling workload, use:

```bash
python3 profiling/examples/run.py \
  --language cppboost \
  --profile-kind cpu \
  --coroutine-diagnostics
```

The runner rebuilds the Release example, rejects a stale non-diagnostic build,
and requires `runtime_handler_queued`, `runtime_handler_running` and
`runtime_handler_suspended` in every timestamped metrics sample. The values
mean:

- `handler_running`: completion handlers currently being invoked;
- `handler_suspended`: operations created and still awaiting completion;
- `handler_queued`: immediate executor work (`execute`, `post`, `dispatch`,
  `defer`) or reactor work whose readiness hook has fired, but whose completion
  handler has not started.

Boost.Asio exposes no public exact size for every internal scheduler queue, so
`handler_queued` intentionally does not claim to include a timer between its
internal expiry and dispatch. The hook inherits the Asio operation object and
observes its existing lifecycle. It never wraps/moves a handler, calls an
executor, changes affinity or changes dispatch order. It does add shared
diagnostic state and atomic updates, so throughput and latency from this mode
must not be compared with normal benchmark results.
