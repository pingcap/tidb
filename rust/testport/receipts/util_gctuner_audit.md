# `pkg/util/gctuner` — complete package audit

Status: complete atomic inventory; package not transcreated.

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

| Artifact | Bytes | Blob |
| --- | ---: | --- |
| `pkg/util/gctuner/BUILD.bazel` | 971 | `16de85d2c84f0c5d3a2c6ab91b7a4929221d63f6` |
| `pkg/util/gctuner/finalizer.go` | 1,470 | `3a08a5d3a9057b59a38ce8f3f415fd2fa8267293` |
| `pkg/util/gctuner/finalizer_test.go` | 1,568 | `8c641c06c7882b5157597da4d6d905e5096d11c4` |
| `pkg/util/gctuner/mem.go` | 764 | `e1a4fff9607995ad30d05e251658ca4b436f2a79` |
| `pkg/util/gctuner/mem_test.go` | 888 | `4cf0b7504ae0803c7741b89d9c47b48ee6f8201c` |
| `pkg/util/gctuner/memory_limit_tuner.go` | 7,718 | `ee3760c44290e3dab5ed56cae38f43010d7b6060` |
| `pkg/util/gctuner/memory_limit_tuner_test.go` | 9,420 | `33f6b1526a6a11ba81dec45194481d2ed11bb068` |
| `pkg/util/gctuner/tuner.go` | 4,846 | `9324cda85f703d9f89b770a1c556f25fd7fc43cb` |
| `pkg/util/gctuner/tuner_test.go` | 3,906 | `ebd1cf9ca2bdd3d7e2a3e7445616a13f825b6e95` |

There is no `doc.go`, fixture, benchmark, generated source, or platform
variant. The four test files contain seven tests: `TestFinalizer`, `TestMem`,
`TestGlobalMemoryTuner`, `TestIssue48741`, `TestSetMemoryLimit`, `TestTuner`,
and `TestCalcGCPercent` (seven top-level tests total).

## Whole-package behavior

This package is a Go-runtime controller, not a portable percentage formula:

- a self-rearming `runtime.SetFinalizer` callback runs after every tracing-GC
  cycle and can be stopped atomically;
- `runtime.MemStats.HeapInuse`, the process `GOGC` value, and an adjustable
  heap threshold drive `debug.SetGCPercent` between process-global minimum and
  maximum values;
- `debug.SetMemoryLimit` is dynamically moved between the configured trigger,
  a one-minute 110% fallback, the initial Go runtime limit, and the global
  arbitrator limit;
- consecutive GC classification, adjustment serialization, concurrent sysvar
  updates, BR disable nesting, failpoint timing, metrics, and test-only worker
  drainage are part of the package contract;
- both tuners start from Go package initializers and are controlled by the
  pinned sysvar hooks and BR/domain/testkit consumers.

The tests allocate real Go heap, force `runtime.GC`, observe finalizer ordering,
read `NumGC`/`NextGC`, and query the live `debug.SetMemoryLimit` authority.

## Rust comparison and decision

Rust has no tracing garbage collector, Go finalizer cycle, `GOGC`,
`runtime.MemStats.NextGC`, `debug.SetGCPercent`, or `debug.SetMemoryLimit`.
Its existing process memory controller tracks explicit query allocations and
kills a top consumer; that is the native analogue of Go
`pkg/util/servermemorylimit`, not this package. Reusing it as a fake GC tuner
would change both the controlled resource and the observable state machine.

The Rust sysvar registry retains the Go variable names/defaults and ordinary
read/write behavior, but has no fabricated runtime-tuner side effects. There
is no existing `gctuner` module, detached formula helper, finalizer simulator,
or translated test carrier to remove. Adding only `calcGCPercent`, atomic
getters, or a timer callback would be a partial private-function port and is
rejected by the package-atomic rule. The package remains an explicit runtime
non-equivalence, not a parity claim.

## Read-only evidence

- `git ls-tree -r --long e2788410d8d696605e8cb002585877a063ccc909 pkg/util/gctuner`
- complete `git show` reads of all eight Go source/test files and
  `BUILD.bazel`
- `git grep -n 'gctuner\.' e2788410d8d696605e8cb002585877a063ccc909 -- '*.go'`
- `rg -n 'gctuner|GOGC|SetGCPercent|SetMemoryLimit' rust/crates`

No production source changed for this audit, and no validation gate is
claimed. No Go or Bazel source changed, so `make bazel_prepare` is not
required.
