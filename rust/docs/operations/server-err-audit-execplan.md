# `pkg/server/err` audit ExecPlan

This living plan records the complete Go-package audit and the Rust error
catalog boundary for `pkg/server/err`.

## Progress

- [x] (2026-09-02) Pinned Go master at
      `c6054025ed4c32ab3672a2a24ea46892714d21ec` and read both package
      artifacts (65 lines, 15 declarations) before editing.
- [x] (2026-09-02) Compared every Go prototype with Rust's
      `tidb-error::server_errors` owner and its shared errno/message catalogs;
      no source delta or Rust-only behavior was found.
- [x] (2026-09-02) Ran the Go compile check, the complete Rust 15-row source
      matrix, and diff hygiene.
- [ ] Continue the rolling package inventory with the next unclaimed package.

## Scope and decision

The atomic unit is the complete Go package, including `BUILD.bazel` and its
ownership metadata. This package has no tests or fixtures. The Rust owner is a
shared, dependency-closed error catalog; no duplicate server adapter is
justified.

## Validation gate

    PATH=<pinned Go>/bin:$PATH GOPATH=<pinned GOPATH> \
      go test ./pkg/server/err -count=1
    cargo +nightly-2026-08-22 test --offline --locked -p tidb-error \
      --test all server_error -- --test-threads=1
    git diff --check

## Outcome

The package inventory and exact prototype mapping are recorded in
`rust/testport/receipts/server_err.md`; no production source was changed.
