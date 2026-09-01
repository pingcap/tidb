# `pkg/expression/generator` — complete Go-master parity boundary receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The directory contains eight tracked artifacts and 3,162 Go/Bazel lines. Every
generator input, template, helper, build target, generated-output command, and
build-ignore variant was read before this receipt was written. The five vector
generator programs are explicitly `//go:build ignore` inputs; they are not
compiled package production code. There are no package tests, fixtures,
platform-specific variants, or additional generator inputs under this
directory.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 14 | `23041ca21bbf36b2aad8a269cd40beca564dcc2b` | `d4ac9b1dd48dc2c625db6b7b1a8ec93f0f9e55cbb7ea557e36836f9cb58666d0` | compiled `builtin_threadsafe.go` generator binary |
| `builtin_threadsafe.go` | 235 | `d36eb952873f693914a22a5291a9f81d2b44fb64` | `059f20a49ba151f2634cf09963e84d8f1dda009ffaec4fa40b6209bad4b6463f` | AST classifier and safe/unsafe sharing source generator |
| `compare_vec.go` | 489 | `37496dd2e7b60cd240144db345f605dca9330606` | `69f7753bcffa7aba62a708501f944b884031e84cf8554e2442250a7bfdefb217` | build-ignore comparison, NULL-equality, COALESCE, and generated-test templates |
| `control_vec.go` | 675 | `6916044a0cc3ceb0507839aa6506111e95549c67` | `d6fc0ef6614445c6aff26dcfbaaec4b63c8216881bc23bec6a152d065d3e9dc0` | build-ignore CASE/IFNULL/IF vector and test generator |
| `helper/BUILD.bazel` | 8 | `f8c81a9c0ca0d39dae52ba634d04ca24e789820c` | `1532fec1f57a3d3f342c45299e54e23d8b2b2a0219403e6fa736c86a2135956d` | public helper library target |
| `helper/helper.go` | 49 | `d56b4c118edb42eee41cfb00865f4e1a9fa47aa8` | `086696c249752e8aedbdcd5647d698a4b7fd1a6d47737997ab7fcad6f21a64b8` | seven scalar/vector type contexts used by templates |
| `other_vec.go` | 502 | `19f0338fa534a31a5e496c8a9158e075a0a479c8` | `15992d021f6350297195a2a4a9f9ce731547eba7b3b231e11ad0f922b1020023` | build-ignore `IN` vector and generated-test template |
| `string_vec.go` | 211 | `f3fafcf8a3ea2a7f1ad7fb9d4de67d9092f63564` | `e54f761f267cfe798d609045a00570cbe520d6b2aa3cfd4421ce12a8ce1eda99` | build-ignore `FIELD` vector and generated-test template |
| `time_vec.go` | 979 | `49c5e7aefa9f1d3a59105e83f7456dcb1998c11b` | `5771b87a2b6d251a55211b5c777bc04ffcbee89ada22e0990913ab66f597bdd8` | build-ignore date/time arithmetic, TIMEDIFF, and generated-test templates |

The production generator entrypoint declares `collectThreadSafeBuiltinFuncs`,
`genBuiltinThreadSafeCode`, `generateCode`, and `main`; its templates emit the
safe/unsafe `SafeToShareAcrossSession` methods and the atomic recursive
argument check. `helper.TypeContext` plus the seven exported type values cover
all generated eval/column spelling combinations. The build-ignore programs
declare their template functions, type/signature tables, interval-unit lists,
deterministic/random test generators, `generateDotGo`, `generateTestDotGo`,
`generateOneFile`, and `main` functions. Their templates preserve warning
fallbacks, NULL propagation, collation-aware comparison, unsigned integer
membership, temporal parsing, and generated vector test/benchmark matrices.

The parent `pkg/expression` package owns the generated outputs. The five
`go:generate` directives in `builtin.go` produce these tracked artifacts:
`builtin_{compare,control,other,string,time}_vec_generated.go` and matching
`*_test.go`, plus `builtin_threadsafe_generated.go` and
`builtin_threadunsafe_generated.go`. Those outputs are outside this package's
directory and are covered by the parent expression test-port receipts
(`b066`/`b074`); this receipt does not silently reclassify them as generator
sources.

The current Go master delta from the earlier pinned source
`e2788410d8d696605e8cb002585877a063ccc909` is empty for all eight artifacts.
No package file, generator template, generated output, BUILD target, or module
input changed between the two snapshots.

## Rust ownership and parity status

Rust has no runtime equivalent of this Go-only code-generation package. The
`tidb-expr` crate contains native vector/scalar implementations and source
parity tests; it does not consume Go templates or emit Go files. Porting these
templates would create an unused generator and would not implement a missing
Rust behavior. No Rust-only behavior was found in this package to remove, and
no dependency-closed production Rust fix is justified by the empty master
delta.

The generated Go behavior remains represented by the Rust expression carriers
and their focused source tables. Any future change to a template must be
treated as a parent `pkg/expression` generated-artifact change: regenerate from
the `go:generate` inputs, inventory the resulting outputs, and update the
parent package receipt in the same atomic batch.

## Validation and risk

Profile: **WIP** for this documentation-only boundary audit; no production or
generated file was changed, so a package-complete Ready claim and regression
test are not applicable. The no-delta result was verified with:

```text
git diff e2788410d8d696605e8cb002585877a063ccc909 origin/master -- pkg/expression/generator
git diff --check
```

No Go/Bazel/module source changed, so `make bazel_prepare` was not required.
No Rust source changed, so no cargo gate was required. Unverified here are
Bazel's generator binary action, regeneration on every host filesystem, and
the parent package's full generated vector suites; those belong to the parent
receipt and are unchanged by this audit.

Risks are limited to future generator drift: stale generated outputs, template
formatting failures, and platform-specific filesystem behavior. Runtime
correctness and performance are unchanged by this receipt.

This receipt certifies the bounded `pkg/expression/generator` inventory and
its explicit Go-only boundary; it is not a repository-wide parity claim.
