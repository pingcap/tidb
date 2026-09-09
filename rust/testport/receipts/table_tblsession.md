# `pkg/table/tblsession` parity receipt

Status: Audited; no dependency-closed Rust implementation was added. This
receipt covers the complete Go package and its current boundary; it is not a
repository-wide parity claim.

Comparison source: Go `origin/master` at
`c6054025ed4c32ab3672a2a24ea46892714d21ec`.
Rust comparison branch: `origin/hparser-integration` at the current audit
tip. No Rust production owner exists for this package.

## Complete Go inventory

All three tracked artifacts in `pkg/table/tblsession` were read in full before
editing: 386 lines total. There is no package `doc.go`, fixture or `testdata`
directory, generated source or input, platform/build-tag variant, benchmark,
fuzz target, README, or ownership artifact.

| artifact | lines | role |
| --- | ---: | --- |
| `BUILD.bazel` | 36 | Go library/test target and dependency graph |
| `table.go` | 191 | session-backed mutation, allocator, statistics, cache, temporary-table, and exchange contexts |
| `table_test.go` | 159 | complete context-field and support-interface regression |

The Go source, test, and BUILD metadata are byte-identical to current Go
master. `TestSessionMutateContextFields` exercises every exported context
adapter, including nil transaction support, reserved row IDs, row encoding,
cached-table handles, global temporary-table deltas, and exchange metadata.

## Rust ownership and boundary decision

The former Rust `tblctx`/`tblsession` seed cluster was intentionally deleted
as an unwired partial carrier. The executor source inventory in
`tests_table_part2_source.rs` records the single Go `tblsession` test as a
`go-parity-carrier`; no Rust production module implements `MutateContext`.

Faithful ownership requires the live Go session context, session variables,
statement/transaction state, auto-ID allocators, temporary-table overlay,
infoschema exchange checks, row-encoding buffers, and DML/table storage
consumers. Adding a standalone Rust context would duplicate an uncalled
interface and create Rust-only behavior. The Go package remains the
authoritative implementation until those dependency owners are integrated.

## Validation

Profile: **Ready** for this no-code boundary audit.

- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/table/tblsession -count=1` — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint` — repository Ready gate (run after this docs batch is staged).
- `git diff --check` — passed after this batch is staged.

No Go/Bazel artifact changed, so `make bazel_prepare` and Rust cargo checks
were not applicable. Existing executor batch `b151` already validates its
source-backed inventory; no Rust source changed here.

## Risks and unverified scope

- Correctness risk is unchanged: Go session/table mutation context behavior
  remains authoritative and its complete source test passes.
- Compatibility risk is limited to the explicit boundary; no API or storage
  semantics changed.
- Performance is unchanged.
- Not verified locally: Rust DML/session integration, live temporary-table and
  exchange-partition flows, non-host platforms, and repository-wide integration
  suites.

The rolling repository audit continues with the next unclaimed package.
