# `pkg/sessiontxn/staleread` — complete Go-master parity boundary receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The package contains 10 tracked artifacts and 1,746 lines. Every production
source, test, failpoint hook, and Bazel target was read in full before
comparing the Rust workspace. There is no `doc.go`, fixture directory,
generated output, benchmark, fuzz target, or platform/build-tag variant.
`main_test.go` is included because it owns the package-wide failpoint and
goleak lifecycle.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 77 | `2c241c6594294a471df074264cf045cf624f2da6` | `1a5add82d38ce2620bc66de8b930aba0c10e3ccbe83572dd96fcc0f992e39182` | stale-read library and test target |
| `errors.go` | 15 | `e14884edbf8395ea049e0c5d80011034302b02bf` | `3409530bc0c1b35f18e3163716bab7b3d2224e0c63fd371ed7f7b24072f4b26a` | package error surface (currently empty) |
| `externalts_test.go` | 52 | `8b12f3cf101ec087cd5e4580a4e3a5928ce5fb49` | `5d61b8acabee7515bac14157abc43e2bdc77ddb875911e351deec13f3f856a31` | external timestamp read-only regression |
| `failpoint.go` | 37 | `e7740d746378d57663d1a124a127a25bdfb351a3` | `aa2b1dd76f64ddf7f40d6a656cfa1594b0155ac91d32333095644abce12634fb` | statement-staleness assertion hook |
| `main_test.go` | 35 | `bcb42bb80fb487f38691a10b67ab954e55eac1bd` | `55bfbcd38e8fac0a6f94234cdb5fde8bf6c3e68e3cd2ee9639ddfa5001f7cb93` | package test lifecycle and leak checks |
| `processor.go` | 326 | `27c10f3d18c504562995c6565cd5beae72c2e58a` | `b67bb4276418bc1a62839ef9bebc443e98b7ad95c6eab122d68a9cfa28c5e8ec` | AST stale-read selection and timestamp resolution |
| `processor_test.go` | 547 | `eb13a942097f3af7dd5939a9bfd0803436bd5453` | `f72852b3c96c4d8bd8a3739f36e581e302149068c0da941635b38441b37677b1` | processor, AS OF, TSO, prepared, and validation tests |
| `provider.go` | 288 | `319700220d8c258aa0b0d5b69683e395459ba962` | `432b10ff69841162b7f6cbb370f33a2efc29a7b1bad532d6d942172813406421` | stale transaction context provider lifecycle |
| `provider_test.go` | 205 | `58bc43b6d213f24a8896763fdfa1a24e4e9f76bb` | `11a73e4ba21f8697f9934f46d5273447fa13ead662cb4876b03e74c9dbf42126` | provider activation, scope, snapshot, and failpoint tests |
| `util.go` | 164 | `84f25c2f1d2930e9f8a2adcc2f7860c77035e4a7` | `8a13cb4309a61108632799fc66ab573e6d8ec5e7bcc889bc7f5002088f307d29` | AS OF, staleness, external timestamp, and TSO helpers |

The production surface defines 51 declarations across the processor,
provider, utility, and failpoint files; the test surface defines 20 helper or
test declarations, including 13 top-level tests (`TestMain` plus 12 package
tests). The tests cover table `AS OF TIMESTAMP`, transaction read timestamps,
`tidb_read_staleness`, external timestamps, prepared statements, compact
datetime/TSO parsing, invalid expressions, transaction lifecycle, replica
scope, and read-only enforcement. All 71 function/method declarations and
all 13 top-level tests were checked individually.

## Rust ownership and explicit boundary

Rust has no dependency-closed equivalent of `pkg/sessiontxn/staleread`.
`tidb-session` owns a bounded in-memory `AS OF TIMESTAMP` path and stale
transaction timestamp parsing, while `tidb-vardef` catalogs the related
system variables. The session currently refuses `tidb_snapshot` and
`tidb_read_staleness` reads because the cluster execution tier has no
dependency-closed MVCC snapshot/oracle and cannot safely answer historical
queries. It also has no owner for the Go provider lifecycle, external
timestamp cache, follower-read snapshot options, prepared-statement
staleness evaluator, or session temporary-table snapshot integration.

The refusal is deliberate safety behavior, not a Go-compatible implementation:
returning current rows under a historical timestamp would be an undetectable
wrong answer. No Rust-only behavior was removed, and no partial provider,
variable, or parser fix was dispatched. Implementing one branch (for example,
only `tidb_read_staleness`) would violate the package-atomic transcreation
rule and invent a snapshot/oracle boundary, so the complete Go package is
recorded as an explicit SEED/boundary. Future work must establish the session,
storage, planner, prepared-statement, and external-timestamp owners together
before enabling the corresponding Go behavior.

## Validation and risk

Profile: **WIP** for this documentation-only boundary record. No Go source,
imports, test declarations, Bazel metadata, or module files changed, so
`make bazel_prepare`, Rust compilation gates, and the Ready lint gate were not
required for this batch.

```text
(cd <detached-origin/master-worktree> && \
 PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
 GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
 ./tools/check/failpoint-go-test.sh ./pkg/sessiontxn/staleread -count=1)
# passed: pkg/sessiontxn/staleread (9.742s); failpoints enabled and disabled
```

The package was tested from an exact detached Go-master worktree because the
integration checkout does not carry this Go package. No Rust code changed, so
no Rust owner test was applicable. Not verified here: Bazel execution, real
PD/TiKV external timestamp reads, full Go repository tests, or a future
dependency-closed Rust stale-read owner.

This receipt certifies the bounded `pkg/sessiontxn/staleread` inventory and
ownership decision; it is not a repository-wide transcreation claim.
