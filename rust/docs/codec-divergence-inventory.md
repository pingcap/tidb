# Encoding divergence inventory: Go `pkg/tablecodec` + `pkg/util/codec` vs Rust `tidb-tablecodec` + `tidb-codec`

Status: IN PROGRESS (skeleton committed early; sections filled as the audit proceeds).

Scope:

| Go package | Rust crate |
| --- | --- |
| `pkg/util/codec` | `rust/crates/tidb-codec` |
| `pkg/tablecodec` | `rust/crates/tidb-tablecodec` |

Method: read both sides function by function, compare semantics (not names), and for
every claimed difference record Go file+line, Rust file+line, and a concrete input that
distinguishes the two.

Ranking:

1. **Corruption** — silent wrong bytes on encode, or the same bytes decoding to a
   different value. A Go node and a Rust node sharing one TiKV disagree about data.
2. **Accept/refuse asymmetry** — one side errors where the other succeeds.
3. **Diagnostic** — message/error-kind differences only, no observable byte or value
   difference.

## Findings

(filled in below as the audit lands)

## Verified equal

(filled in below as the audit lands)

## Not verifiable in this environment

This machine cannot execute freshly built binaries (`syspolicyd` wedged; every new
executable hangs in `_dyld_start`). `cargo check` and `cargo clippy` are the only gates
that ran. Nothing in this document was confirmed by running a test, a fixture, or a Go
program; every claim is derived from reading both sources.
