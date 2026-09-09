# `pkg/lightning/config` — complete package parity receipt

Pinned Go source: `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`
(`origin/master`).

## Complete inventory

The package has exactly ten tracked artifacts and 4,005 lines. Every BUILD,
ownership, production, test, and support line was read in full from the pinned
Go source. The hparser branch had no production-source delta; its only
pre-existing package delta was the `OWNERS` filter change.

| Go artifact | Lines | Blob | Rust disposition |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 53 | `68202741235de72f602cc438a5f1ca70daf5a41d` | public library plus 50-shard flaky test target; no Rust build input |
| `OWNERS` | 10 | `74771439f2d4ce4611ed1b9daad96f5fc61c60d7` | repository ownership metadata; no Rust equivalent |
| `bytesize.go` | 45 | `bc159d08254963c382bc920f42db4991f4f1484d` | `ByteSize` TOML/JSON parsing using Docker unit semantics; no Lightning config owner |
| `bytesize_test.go` | 128 | `4ed7b6e0b7f824f06eeb771a09e3c66ace8b7101` | two byte-size serialization/parsing tests |
| `config.go` | 1,670 | `784053c1a405960f0d93489c896594d850e646ae` | complete Lightning schema/default/validation/marshal behavior; no dependency-closed Rust owner |
| `config_test.go` | 1,478 | `504acc522f9a59b3e642eda9bf95afcef5bcbbc2` | 46 tests, two helpers, and the CPU-count failpoint branch |
| `configlist.go` | 154 | `6a3753881419ea3c02a00768c8215dbb0d9b48a1` | context-aware FIFO task list; no Rust owner |
| `configlist_test.go` | 130 | `c14215132d7199ee986cf9173628dd90f5128dc7` | four FIFO, cancellation, lookup/removal, and movement tests |
| `const.go` | 51 | `a706334933658593c8116fb989d56867073b4f8f` | Lightning defaults and gRPC keepalive constants; no Rust owner |
| `global.go` | 286 | `dcf047f7c2d96d573991fa114c97e88abacf45b5` | global config construction, flag loading, validation, and logging path; no Rust owner |

The five production files contain 66 function/method declarations: 2 in
`bytesize.go`, 52 in `config.go`, 8 in `configlist.go`, none in `const.go`, and
4 in `global.go`. The three test files contain 52 `TestXxx` functions (46 in
`config_test.go`, 2 in `bytesize_test.go`, and 4 in `configlist_test.go`) plus
the two `config_test.go` helpers. There are no benchmarks, fuzz corpora,
fixtures, testdata directories, generated/platform variants, package docs, or
additional build artifacts. `TestRegionConcurrencyUsesUsableCPUCount` is the
one failpoint-enabled test and restores the CPU failpoint during cleanup.

`config.go` covers the full Lightning configuration contract: backend modes,
database and PD endpoint adjustment, route and file-path validation, TLS and
security construction, concurrency and disk defaults, checkpoint and
post-restore policy, duplicate-resolution and compression enum codecs, CSV and
charset handling, global/TOML loading, redaction, and final validation.
`configlist.go` provides the synchronized task queue; `global.go` wires command
flags and process-wide configuration; `bytesize.go` provides Docker-style
human-readable sizes.

## Focused regression

`TestRemoveAllowAllFiles` previously compared the complete formatted DSN as a
literal string. The pinned hparser branch's older Go/dependency toolchain
emits the same query values in a different order, causing a false failure
before any behavior was changed. The test now parses the query with the
already-imported `net/url` and asserts the stable contract: the DSN path is
preserved, `tls=false` and `charset=utf8mb4` survive, and `allowAllFiles` is
absent. The pre-fix branch run failed only on that ordering assertion; the
post-fix failpoint-aware suite passes.

## Rust ownership and parity result

No Rust crate owns the dependency-closed `pkg/lightning/config` package.
Searches found adjacent Lightning utility crates and `rust/crates/tidb-config`,
but that crate's `ByteSize` maps to the separate Go
`pkg/config/configtypes` package and does not implement Lightning's schema,
flag loading, endpoint/TLS adjustment, checkpoint policy, task queue, or
backend validation. No Rust call site consumes a replacement Lightning
configuration object.

No Rust-only behavior was found to remove, and no speculative Rust facade or
ignored parity carrier was added. Moving this package requires its concrete
Lightning command/server consumers, storage backends, common connection/TLS
types, table filters/routes, checkpoint drivers, and task-list lifecycle as one
dependency-closed unit.

## Validation

Profile: Ready for this package batch. The final code delta is limited to an
existing Go test body; no production Go, new test function, import section,
Bazel, module, generated, or Rust source changed, so `make bazel_prepare` is
not required for the final diff.

Passed on the current branch with the repository failpoint wrapper:

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 ./tools/check/failpoint-go-test.sh ./pkg/lightning/config -count=1
PASS
ok   github.com/pingcap/tidb/pkg/lightning/config 1.597s
```

The same exact Go-master failpoint-enabled suite passed in a detached worktree
(`1.688s`, selecting Go 1.25.12 from its `go.mod`). Both wrapper runs emitted
the expected invalid `-tidb-port` diagnostic from the flag parser and returned
failpoint refcount 0 after cleanup. Rust formatting, repository lint, and
`git diff --check` passed for the receipt batch. No Rust regression test is
applicable while the dependency-closed Lightning config owner is absent.

## Risk and next boundary

- Correctness: all ten artifacts, 66 production declarations, 52 tests, the
  CPU failpoint, and the 50-shard test target are mapped; the DSN regression
  now checks behavior rather than serialization order.
- Compatibility: endpoint discovery, TLS fallback, TOML/flag precedence,
  checkpoint persistence, duplicate policy, and queue cancellation remain an
  explicit Rust integration boundary.
- Performance: only test parsing changed; no runtime configuration path or
  allocation behavior changed.

The next executable port must start with the Lightning command/server consumer
and its concrete backend/checkpoint dependencies, not an isolated config
struct or queue wrapper.
