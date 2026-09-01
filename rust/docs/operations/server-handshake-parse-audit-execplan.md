# `pkg/server/internal/handshake` and `pkg/server/internal/parse` audit ExecPlan

This living plan records the complete Go-package inventory and the Rust parity
fix for the handshake response contract and parser consumer.

## Progress

- [x] (2026-09-02) Pinned Go master at
      `c6054025ed4c32ab3672a2a24ea46892714d21ec` and read every production,
      test, fixture, and build artifact in both packages (six artifacts, 553
      lines combined).
- [x] (2026-09-02) Traced all Rust constructors, parser consumers, and source
      tests; identified `raw_attrs`, `attr_warnings`, and lossy UTF-8 attrs as
      Rust-only behavior.
- [x] (2026-09-02) Removed the extra response fields, made attrs byte-exact,
      and restored Go-equivalent warning logging through `tidb-log`.
- [x] (2026-09-02) Added focused exact-field and byte-attribute regressions;
      Go tests, both focused Rust filters, formatting, and lint pass.
- [ ] Continue the rolling package inventory with the next unclaimed package.

## Scope and decision

The atomic units are the complete Go packages, including BUILD metadata and
all tests and fixtures. `handshake` owns the response value; `parse` owns its
wire parser and attribute policy. They share one Rust parser owner because the
response shape and parser are inseparable at the protocol boundary. No
speculative compatibility facade was added.

## Validation gate

    PATH=<pinned Go>/bin:$PATH GOPATH=<pinned GOPATH> \
      go test ./pkg/server/internal/parse ./pkg/server/internal/handshake -count=1
    cargo +nightly-2026-08-22 fmt --all -- --check
    OPENSSL_DIR=<bundled OpenSSL> DYLD_LIBRARY_PATH=<bundled OpenSSL>/lib \
      cargo +nightly-2026-08-22 test --offline --locked -p tidb-server \
      --test all response41 -- --test-threads=1
    OPENSSL_DIR=<bundled OpenSSL> DYLD_LIBRARY_PATH=<bundled OpenSSL>/lib \
      cargo +nightly-2026-08-22 test --offline --locked -p tidb-server \
      --test all parse_go_source -- --test-threads=1
    make lint
    git diff --check

The Rust commands use the bundled OpenSSL headers and libraries available in
this workspace; both focused filters pass.

## Outcome

Package inventories and the parity boundary are recorded in
`rust/testport/receipts/server_internal_handshake.md` and
`rust/testport/receipts/server_internal_parse.md`. Runtime consumers now see
the exact eight Go response fields and byte-preserving connection attributes.
