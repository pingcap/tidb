# `pkg/telemetry` — Go-master parity boundary receipt

Comparison source: Go `origin/master` at commit
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01).

## Complete inventory

The package has exactly nine tracked artifacts and 2,190 lines. Every
production source, test, goleak harness, and Bazel target was read in full
before comparing Rust ownership. There is no `doc.go`, fixture, generated
source, benchmark, or platform-specific Go variant.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 66 | `c923ec04c80d567cf2f18a22107428f60fe2c053` | `e2939d6780f448aeccc9946c4ae822366d3a2474c6b7a08e5dca22ee65c8f464` | library/test targets, failpoint and mock-store dependencies |
| `data.go` | 63 | `9b8253a9ecd1c4c836d5dc5f2998e6a3a5e3c6ab` | `c15b2f9d729cad68fb2d01c0636b66b0a774e18b4917443f516ad76624e8c354` | report payload assembly and counter reset orchestration |
| `data_feature_usage.go` | 466 | `9ee945799a299d1c899dabcda7c90b06ef1c16d9` | `b79d3022725d6de634195718c199a044e5dcc7928dbb2d37277a47e85d82c07f` | transaction, DDL, partition, resource, TTL, and feature counters |
| `data_feature_usage_test.go` | 928 | `b59c8d9f7af6e3cd94e085f1ffb8a5c2fe863cbe` | `b7bc6cf7cf207ea53691b135b1577128928e87c43b143d41b81fe7a12ff4241b` | 24 SQL/counter behavior tests and failpoint paths |
| `data_window.go` | 215 | `434cb25b0e9a02f56271d5d9f831916c6956c0ad` | `71aa8aad3e97c6a163baf6f3cc4986aca73a35c367683baf33af5c805cdf7da5` | minute sub-window rotation and hourly aggregation |
| `data_window_test.go` | 106 | `efdfc4b15b533be14535b7e6f191c1c7ac778b08` | `ad66c3d922efb77b4a207ae2e66e6d9a57dfdee8446ae22321b6c4a4ff0cbafc` | builtin-function and TiFlash usage tests |
| `main_test.go` | 50 | `77dbed2989109f787f5890edd66e6c301b29e0c7` | `60fbb40992770283d056abb2778c3348875a846fb689f35cc0044749eeb450bc` | test-only exports, common setup, and goleak harness |
| `telemetry.go` | 85 | `7c5f8cb3e46707ea7be0f7e739916d5f45e3b70d` | `1b3fa74cd7ad82a354c6124fffa2f24e27c547b88a604f226e2316210908f425` | enablement, report, initial run, and logger API |
| `ttl.go` | 211 | `721ba0329a6caba8316ab4aa968b03cc0028eb34` | `e6ca58a7cbd2a6c3b90a3cbd4984c2c7adaee0d927411ce8b20b816325fd798e` | TTL table/history discovery and histogram bucketing |

The inventory covers all production helpers (feature snapshots,
counter-difference/reset logic, window aggregation, TTL SQL queries, and
telemetry report gating) and all 26 Go tests, including the TiFlash mock
cluster, resource-group, failpoint-backed store-batch, and fair-locking
interleavings. The test package uses `@com_github_pingcap_failpoint` and must
run through the repository failpoint wrapper when executed.

## Rust ownership and decision

Rust has no dependency-closed owner for this package. Existing pieces are
deliberately narrower: `tidb-planner::telemetry` classifies plan shapes,
`tidb-config`/`tidb-vardef` expose the telemetry configuration variable, and
`tidb-server` records only bootstrap admission. None provides the global
feature counters, SQL-backed infoschema/TTL collection, six-hour report
window, or telemetry logger/report lifecycle.

Implementing any one of those fragments in a new crate would create a
cache-only or fabricated reporting path without the Go session/domain,
metrics, and restricted-SQL dependencies. The complete package is therefore
recorded as an explicit boundary with no speculative Rust behavior and no
source edit. Because no Rust behavior changed, no new focused regression is
applicable to this receipt.

## Validation and risk

Profile: **WIP** for this boundary audit; the repository-wide loop remains in
progress. `git diff --check` passed for the receipt/ExecPlan batch. No Go or
Bazel source changed, so `make bazel_prepare` is not required.

- Correctness: telemetry collection/reporting remains a known unported
  integration; no parity claim is made from the existing config or planner
  fragments.
- Compatibility: a future implementation must move session/domain access,
  metrics counters, TTL SQL, windows, and report gating as one package unit.
- Performance: unchanged.
- Not verified locally: the failpoint-enabled Go telemetry suite, Bazel
  analysis, and end-to-end telemetry report output.
