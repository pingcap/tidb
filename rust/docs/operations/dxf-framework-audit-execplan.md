# `pkg/dxf/framework` parity audit ExecPlan

This living plan follows `PLANS.md` at the repository root.

## Objective

Inventory every artifact in the top-level Go DXF framework package and record
whether Rust has a dependency-closed owner, without inventing a parallel
framework guide or runtime facade.

## Progress

- [x] (2026-09-02) Pinned Go master at
  `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`; read both direct artifacts in
  full, including package documentation before the Bazel target. Confirmed
  190 total lines and no tests, fixtures, generated/platform variants,
  benchmarks, fuzz targets, or OWNERS files.
- [x] (2026-09-02) Verified `BUILD.bazel` and `doc.go` are byte-identical to
  the pinned Go-master source and recorded the Go-only ownership boundary in
  `rust/testport/receipts/dxf_framework.md`.
- [ ] Continue the rolling package audit after publishing this receipt.

## Scope and decision

`pkg/dxf/framework` contains only public package documentation and its Bazel
library declaration. Rust's `tidb-dxf` crate owns selected generic values, not
the complete Go framework runtime or its SQL/session integrations. This batch
therefore makes no production-code or test changes.

## Validation gate

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/dxf/framework -count=1 -run '^$'
make lint
cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
git diff --check
```

`make bazel_prepare` is not required by this docs-only batch because no Go or
Bazel source changed; if later edits add or move Go sources, apply the normal
Bazel preparation gate.

## Outcome

The complete package inventory and explicit Rust boundary are recorded in
`rust/testport/receipts/dxf_framework.md`; the repository-wide audit remains
in progress.
