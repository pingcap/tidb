# `pkg/owner` parity receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The package contains exactly eight tracked artifacts and 1,883 Go-master
lines: one BUILD target, one OWNERS policy, four production/test-support Go
files, and two test files. Every artifact was read in full before editing.
There is no `doc.go`, fixture directory, `testdata`, generated source/input,
platform variant, benchmark, or fuzz target.

| artifact | lines | Go-master blob | SHA-256 | role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 58 | `ae2313f441ead009c6050c7a583c9e1bd2168832` | `c2ac1e205241484bce7542bb5a4264248280efab87a5f8c55c41d204e9622fa1` | owner library and 11-shard test target |
| `OWNERS` | 10 | `aa34e19ad1196867a5604cf1e2d76aa5b40035af` | `d98112c6ecfa7a637ef2ce63dec60e190b5967ebd944c1ebd343fb082edee3b9` | BUILD-specific and default approver routing |
| `fail_test.go` | 112 | `b78b393db22800b7cf626de6afcdeb05bce417b0` | `81a115d88dcd85a7565fac6a0066945743d4686245af4537ed4e39b509184a10` | etcd session failure/failpoint test |
| `main_test.go` | 34 | `a2d3f5e6677d056b8c1c1d49fef5af3a8e16e85e` | `974ec1c131c7dc93233a377e51fdff8a937aa244b020720ff249923f811c435a` | common test setup and goleak harness |
| `manager.go` | 723 | `8e5b36ccdb93a3a3010c3f5197922cc19ec3cdd3` | `3e1a2ed79a3843ac44e8419e3cfda1acf766b1fb8bc39311521690e61ef80683` | etcd owner election, watch, retry, and distributed lock |
| `manager_test.go` | 626 | `ed939c3d7e1aa3f02f3f28cb2ece3eae60ff4df4` | `4e36b5c4276e280e5636fc3b41205f31912e8b8c01fb20f4cc585b2d72ba7fc2` | election, owner-op, watch, lock, and listener tests |
| `mock.go` | 235 | `989d8f9cf994e37a39fa50c9dfa68788ac89c655` | `c7cbc19cad8355121f6c07fc7c04be26f1b75a765d70b66e6184e87b6f91ad2c` | single-store owner manager and failpoint hooks |
| `mock_owner_state.go` | 85 | `a40f88308cf7879df5a3547a7d602d40d4c83d21` | `a1a22771aac0defccddfa454b523185176071d8f9403ab9027965cdb38eb7dd2` | synchronized mock owner state |

The production inventory has 66 top-level declarations. The test inventory has
12 top-level test functions including `TestMain`; all Go source, tests, and
BUILD files are byte-identical to Go master. The only pre-change mismatch was
the five-line OWNERS policy.

## Implementation

The OWNERS file now matches Go master: `BUILD.bazel` is routed to
`sig-community-approvers`, while all other paths use `sig-approvers-owner`.
This is repository metadata only; owner-election runtime and tests are
unchanged.

## Native integration decision

`pkg/owner` is Go-native distributed coordination built on etcd elections,
leases, failpoints, metrics, and retry helpers. Rust has no
dependency-closed owner manager or SQL/DDL listener integration. No Rust-only
behavior was found to remove and no speculative Rust implementation was added.

## Validation and risk

Profile: **Ready**. The canonical failpoint-aware suite passes:

    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
    GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
    ./tools/check/failpoint-go-test.sh ./pkg/owner -count=1
    # PASS; ok github.com/pingcap/tidb/pkg/owner 12.566s

`make lint`, Rust formatting, and `git diff --check` pass. No Go or Bazel
source changed, so `make bazel_prepare` is not required. The metadata change
has no runtime compatibility or performance risk.

## Outcome

The complete owner inventory and Go-master ownership-policy boundary are
recorded. The rolling audit continues.
