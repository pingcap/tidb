# `pkg/dumpformat/testutils` parity audit ExecPlan

This living plan follows `PLANS.md` at the repository root.

## Objective

Inventory every Go-master artifact in `pkg/dumpformat/testutils`, compare its
test-only Parquet writer contract with Rust owners, and keep the object-store
and Arrow dependency boundary explicit.

## Progress

- [x] (2026-09-01) Fetch and pin Go `origin/master` at
  `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`.
- [x] (2026-09-01) Read both package artifacts (296 lines): BUILD metadata,
  all helper functions, typed dispatch cases, object-store wrapper, and the
  complete row-group writer. Confirm no package-local tests, fixtures,
  generated/platform variants, fuzz/benchmark inputs, or generator sources.
- [x] (2026-09-01) Trace all in-repository Go callers and compare the helper
  with Rust's available Parquet/object-store owners; no dependency-closed Rust
  test writer exists.
- [x] (2026-09-01) Run the pinned Go compile/test command and diff hygiene
  check.
- [ ] Fetch immediately before staging, create one meaningful receipt batch,
  push it to `origin/hparser-integration`, and verify remote SHAs.
- [ ] Continue the rolling audit with the next unrecorded dump-format package.

## Scope and decision

This package is test support, not production format behavior. Its API couples
Arrow Parquet writer internals to TiDB object-store backends and is consumed by
Go Parquet/importer tests. Rust has neither a corresponding fixture owner nor
the dependency closure to reproduce those tests. Keep the boundary explicit;
do not add a detached fixture generator or claim Parquet parity from this
package alone.

## Validation gate

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/dumpformat/testutils -count=1
git diff --check
```

The Go command passes with no package-local tests. No Rust or Bazel source was
changed, and the broader Ready workspace gates are not applicable to this
documentation-only boundary.

## Outcome

The complete inventory and explicit owner decision are recorded in
`rust/testport/receipts/dumpformat_testutils.md`; the rolling audit continues
with the format writer/parser packages and their fixtures.
