# `pkg/expression/test/multivaluedindex` — complete Go-master parity boundary receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The package contains three tracked artifacts and 405 lines. Every index-write
test, key-decoding helper, harness, and four-shard/flaky Bazel target was read
before this receipt was written. There is no `doc.go`, fixture/testdata tree,
generated output, platform-specific variant, or generator input.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 30 | `beb311cf6e7e01c2f4cd0d3ddc33cc51e62ff5b2` | `aa688715da27b452d8b385d7258b3ce41c25e4625ff1e1a395e6f0ea3550f69d` | four-shard flaky test target and storage/index dependencies |
| `main_test.go` | 57 | `dde2ffb4d188eb1da51eafecafa5f41268b9b524` | `a6b92e8d3e7f47281a3b957194c151ffc37fd87f41461cdd7ec234d74cc6d397` | common setup, expression-index enablement, failpoints, timezone, and goleak |
| `multi_valued_index_test.go` | 318 | `a49d85b146270ee5c55017f79a1f51873029f9ed` | `ce32a05e72d051509fa43a935fc89aa02be985d6d53cdc7f6f039d2adc3f15f5` | four tests for signed/unsigned/char ARRAY keys, partitioning, uniqueness, composites, updates, deletes, and key decoding |

The test source declares `TestWriteMultiValuedIndex`,
`TestWriteMultiValuedIndexPartitionTable`, `TestWriteMultiValuedIndexUnique`,
`TestWriteMultiValuedIndexComposite`, and the `checkCount`, `checkKey`,
`checkIndex`, and `decodeIndexKey` helpers. It asserts exact encoded KV keys,
duplicate rejection, partition-local prefixes, update/delete cleanup, and
composite-key ordering. The Go master delta from the earlier pinned source
`e2788410d8d696605e8cb002585877a063ccc909` is empty for all three artifacts.

## Rust ownership and parity status

Rust carries multi-valued-index metadata through `tidb-model`, parser ARRAY
modifiers, planner path flags, and KV-table source-column bookkeeping. The
write/duplicate-detection path exercised here is not implemented: the Rust
expression-index checker deliberately returns
`a multi-valued index (CAST(... AS ... ARRAY)) is not supported yet`, and the
session regression records that refusal. Go accepts the ARRAY index and writes
one key per JSON element, including NULL/empty arrays, partitioned tables,
unique indexes, and composites.

This is a genuine cross-package gap spanning DDL index admission, generated
column evaluation, table/index encoding, executor DML, partition routing, and
duplicate checks. Removing the Rust refusal or treating an ARRAY as a scalar
index would be Rust-only behavior and could silently corrupt index contents;
no partial implementation is safe in this package-only audit. The required
implementation unit is that dependency-closed write pipeline, with these four
Go tests as focused regressions.

## Validation and risk

Profile: **WIP** for this documentation-only boundary audit; no production or
test file changed, so no new regression test or Ready claim is made. Exact
Go-master test (detached worktree, required `intest,deadlock` tags) passed:

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test -tags=intest,deadlock ./pkg/expression/test/multivaluedindex \
  -run '^TestWriteMultiValuedIndex' -count=1                    # passed
```

The package has no failpoint references in its source/build metadata, so no
failpoint wrapper was required. Rust source, Bazel, and module files were
unchanged; `make bazel_prepare` was not required. Not verified: Rust DDL/DML
execution, real TiKV index storage, four Bazel shards, and full workspace
suites. Correctness risk is high if the refusal is removed without implementing
element expansion and duplicate semantics; runtime behavior is unchanged by
this receipt.

This receipt certifies the bounded package inventory and explicit multi-valued
index boundary; it is not a repository-wide parity claim.
