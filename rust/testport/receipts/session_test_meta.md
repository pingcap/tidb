# `pkg/session/test/meta` — complete Go-master parity boundary receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The package contains three tracked artifacts and 376 lines. Every bootstrap
DDL/meta-table test, region/key assertion, TTL metric test, timezone assertion,
next-generation reserved-ID check, TestMain/goleak harness, and six-shard
flaky Bazel target was read before this receipt was written. There is no
`doc.go`, fixture or `testdata` directory, generated output,
platform-specific variant, benchmark, fuzz target, or generator input.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 34 | `15d59839ea19ba457dfb0444e89e0e0de3cc8afd` | `5ad2b896af257da2b4d802f964ff2d2427a84e10c38cc959030eaa361a860a26` | six-shard flaky metadata/bootstrap test target and dependency closure |
| `main_test.go` | 62 | `ea7f53f7452f1818458553e955d961142ae2f11d` | `4bad92318902d5601e9bb96d12321eb13e76321be57c1e3ad21edfcbea544e65` | common setup, TiKV failpoints, async-commit settings, and goleak harness |
| `session_test.go` | 280 | `92fe94d147017bb37968056c7f3927f9caa03fd5` | `8d8f2687b4dd0b38323fff94772939ab6c8de2139b332f33d9ff2f076f2eaf38` | DDL/bootstrap metadata, region, TTL, information-schema, and reserved-ID assertions |

`session_test.go` declares `TestInitDDLTables`, `TestInitMetaTable`,
`TestMetaTableRegion`, the `MustReadCounter` helper, `TestRecordTTLRows`,
`TestInformationSchemaCreateTime`, and `TestNextgenBootstrap`. The tests
cover DDL table-version transitions and table ordering, bootstrap metadata
identity, TiKV region start keys and distinct region IDs, TTL insert accounting
across commit/rollback/savepoint, information-schema create time across
timezones and DDL, and reserved schema/table IDs for the next-generation
kernel. `main_test.go` configures common test state, TiKV failpoints, and
goleak exclusions.

The only Go master delta from the earlier pinned source
`e2788410d8d696605e8cb002585877a063ccc909` is the expected next-generation
bootstrap catalog growth assertion in `session_test.go`: reserved base-table
count changes from 60 to 65. This delta is recorded as source evidence; no
Rust behavior was changed in this documentation batch.

## Rust ownership and explicit boundary

Rust has source-backed ignored carriers for all six behavior tests and the
meta-suite harness in `tidb-session::tests_session_part4_source`. Rust's
bootstrap table definitions and metadata readers cover lower-level constants,
but the Go tests require a dependency-closed Domain + mock TiKV + DDL owner,
tablecodec region inspection, TTL transaction metrics, timezone-aware SQL
execution, and next-generation catalog publication. Those seams are not
available as a package-local Rust test surface. No Rust-only behavior was
found to remove, and no safe implementation can be added without duplicating
bootstrap/session ownership.

## Validation and risk

Profile: **WIP** for this documentation-only boundary audit; no production,
test, or Bazel file changed, so no new regression test or package-complete
Ready claim is made.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test -tags=intest,deadlock ./pkg/session/test/meta \
  -run '^TestInitDDLTables$' -count=1                                # passed
```

The exact detached Go-master worktree was used. The package source/build
metadata has no direct `failpoint.` calls (the harness only enables TiKV
client failpoints), so no failpoint wrapper was required for this targeted
run. Rust source, Bazel, and module files were unchanged;
`make bazel_prepare` and Ready lint were not required. Not verified: the
remaining metadata tests, six Bazel shards, full TTL/timezone coverage, or
next-generation live bootstrap. Correctness risk is concentrated in the
master-only reserved-table-count delta; runtime behavior is unchanged because
this batch modifies documentation only.

This receipt certifies the bounded meta test-package inventory and explicit
ownership boundary; it is not a repository-wide parity claim.
