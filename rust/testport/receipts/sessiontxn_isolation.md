# `pkg/sessiontxn/isolation` — complete Go-master parity boundary receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The package contains 13 tracked artifacts and 3,992 lines, including the
nested `isolation/metrics` support package. Every production source, test,
metrics helper, and Bazel target was read in full before comparing the Rust
workspace. There is no `doc.go`, fixture directory, generated output,
benchmark, fuzz target, or platform/build-tag variant.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 89 | `eae35af4a613125f6b2bf4aacdedc15128b243eb` | `9739b14383f067aa1dd503c88212e20db5a8249cf3c9fe07e6b3eefaa9fa3b86` | isolation library and 28-shard test target |
| `base.go` | 779 | `3251b42b0848b88f7b633ea6d8ab3240c0ccc12c` | `a2ce01dd7970b176bf8e00cfd85a9a2c8fb645b7264314a3f96e26c3c33fb523` | shared transaction provider lifecycle and snapshots |
| `main_test.go` | 180 | `a42338f04d737424baeb6e715392774b8d941e5d` | `add44177334eccde8480ad1e6139904f7efdcc139052f670586e2473d16272c9` | failpoint/goleak lifecycle and assertions |
| `metrics/BUILD.bazel` | 12 | `779c9efc6bcd9edfa61cf13a3596b0ca2ffb6e4a` | `d471bd0eb676459b4c135824ced607956c2b6c33680ce0259259f70f575f67c2` | isolation metrics target |
| `metrics/metrics.go` | 36 | `1b1c2904ba2a35bd5b0e4f747fee671f697aa500` | `c85fdb42405038af1b4a800932c18cd186b74f5d3d4f6246f05ffdfcbaa7deb8` | RC conflict counters |
| `optimistic.go` | 172 | `a0493b08ddc04d6a2401c609c64e2991e6d27253` | `726c4bb8d9158bc29031409ec7def38edaefbe05c552c66268d306bd21d32fc1` | optimistic transaction provider and retry policy |
| `optimistic_test.go` | 336 | `1f8f4294d3336a6703fab4e85c53d30526b6372e` | `b6eeefd8831c8e7bd7b74bfebfd8e18e844930799d095a033784065bb2064c3d` | optimistic timestamps, snapshots, and error tests |
| `readcommitted.go` | 367 | `a5522f12a3b5a17880a54435d65369a4dff90f8d` | `60c0e0b30f205cdb34c1d6456138eeb55ab078580b5a9e3b37fa29f439bda8de` | pessimistic RC timestamp and conflict handling |
| `readcommitted_test.go` | 635 | `2e6eb8ace2380fc3babcde78309dcb0d52f2d40b` | `85db0e89ea068011acd182cda1d154bcf8f0e8d907ac5b23aa12820be48949dd` | RC timestamp, retry, snapshot, and consistency tests |
| `repeatable_read.go` | 315 | `2a0dff114af5c6755605763c374adba2454f5be2` | `e322d6d947f0f809d718b109293821fbf5cf2d0c24b7b90299bd0c3290c5021a` | pessimistic RR timestamp optimization and retries |
| `repeatable_read_test.go` | 712 | `7b41bb645b2c563d7ead42c495fe9540d5d612fe` | `0ecd43eccad31f56b3c5b8fc7ebe54060ba236b7fd967cc7b70a73749a545308` | RR provider, conflict, snapshot, and slow-log tests |
| `serializable.go` | 63 | `a3f27464e525c22f09ee5a31fbdfb88d5dc06726` | `a353fc39192376dedf68ff6385a2269f9e877e571d3cdfadf54b2c0a6d7b8b91` | pessimistic serializable provider |
| `serializable_test.go` | 296 | `d4972e4492038f3b381c240381f9c5949c6ae04f` | `5ec1c5c4cb67d2d6497aa24b141f7be9b1840efa30dd64b4ac3dcf965c573939` | serializable timestamp, locking, and snapshot tests |

The production surface defines 74 declarations (including the nested metrics
initializers); the test surface defines 47 helpers/tests, with 29 top-level
tests across `main_test.go` and the four provider suites. The tests cover
optimistic and pessimistic transaction setup, read-committed per-statement
timestamps and RC conflict checks, repeatable-read `for_update_ts` reuse and
retry, serializable no-retry behavior, `tidb_snapshot` information-schema and
timestamp restoration, local/closest replica scopes, point-get timestamp
optimizations, temporary-table interception, assertion levels, failpoint
error injection, and failed-DML consistency. All 121 function/method
declarations and all 29 top-level tests were checked individually.

## Rust ownership and explicit boundary

Rust has partial owners, but no dependency-closed equivalent of this Go
package. `tidb-exec::isolation_state` owns the pure isolation enum and
one-shot state machine; `tidb-session` owns system-variable validation and
the in-memory transaction lifecycle; `tidb-server`/`tidb-exec` own optimistic
and pessimistic cluster transaction seams. Those owners do not yet provide
the Go provider interface's complete lifecycle, per-isolation timestamp
selection, RC conflict retry metrics, `tidb_snapshot` information-schema
overlay, temporary-table interceptor, or all pessimistic lock/error paths as
one integrated session owner. The Rust source tests therefore cover only the
metadata boundary and selected transaction seams, not the 121-declaration Go
package contract.

No Rust-only behavior was found to remove. The existing Rust isolation
metadata deliberately keeps unsupported enum values separate from storage
capability, and the cluster transaction seam documents its remaining
locking/read differences rather than pretending parity. Implementing only
one provider or timestamp optimization would violate the package-atomic
transcreation rule and could change conflict/linearizability semantics, so no
partial production fix was dispatched. The complete Go package is recorded
as an explicit SEED/boundary; future work must join session, storage,
snapshot, metrics, and retry owners before claiming parity.

## Validation and risk

Profile: **WIP** for this documentation-only boundary record. No Go source,
imports, test declarations, Bazel metadata, or module files changed, so
`make bazel_prepare`, Rust compilation gates, and the Ready lint gate were not
required for this batch.

```text
(cd <detached-origin/master-worktree> && \
 PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
 GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
 ./tools/check/failpoint-go-test.sh ./pkg/sessiontxn/isolation -count=1)
# passed: pkg/sessiontxn/isolation (20.290s); failpoints enabled and disabled
```

The package was tested from an exact detached Go-master worktree. No Rust
code changed, so no Rust owner test was applicable. Not verified here: Bazel
execution, real multi-store lock timing, full Go repository tests, or a
future dependency-closed Rust isolation provider.

This receipt certifies the bounded `pkg/sessiontxn/isolation` inventory and
ownership decision; it is not a repository-wide transcreation claim.
