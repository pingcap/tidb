# datatype clippy `as_chunks` gate cleanup (batch #36)

## Trigger

A toolchain lint refresh introduced `clippy::chunks_exact_to_as_chunks` in our
pinned nightly, surfacing six warnings across `tidb-datatype` (plus one
`wrong_self_convention` on `MyDecimal::to_decimal_parts`). No errors; the Ready
lint gate still exits 0, but the gate-hygiene precedent (the earlier five-batch
clippy sweep) is to keep our owned crates warning-free.

## Sites

| file | line | fix |
| --- | --- | --- |
| `datum/json_envelope.rs` | 250 | `text.as_bytes().chunks_exact(4)` → `as_chunks::<4>().0` |
| `mydecimal.rs` | 1554 | `to_decimal_parts(&self)` → `(self)` (`MyDecimal` is `Copy`; both callers unchanged) |
| `mydecimal.rs` | 1655 | `chunks_exact(4)` → `as_chunks::<4>().0`, `from_ne_bytes(*chunk)` |
| `mydecimal.rs` | 1685 | `chunks_exact_mut(4)` → `as_chunks_mut::<4>().0.iter_mut()`, `*chunk = w.to_ne_bytes()` |
| `mydecimal.rs` | 1718 | same as 1655 |
| `vector.rs` | 468 | `chunks_exact(4)` → `as_chunks::<4>().0` |

All six are behavior-neutral: `as_chunks` returns the same four-byte groups as
`chunks_exact(4)` + `try_into`, minus the runtime `try_into` that the compiler
now proves total. The `to_decimal_parts` signature change relies on
`#[derive(Clone, Copy)]` on `MyDecimal` (mydecimal.rs:67); method-call sites
resolve identically.

## Verification

- `cargo fmt --check -p tidb-datatype` clean
- `cargo clippy -p tidb-datatype`: 0 warnings
- `nextest -p tidb-datatype`: 476/476
- `nextest -p tidb-expr -p tidb-chunk` (datatype consumers): 1545/1545

No behavioral change, so no fail-before regression applies (per the established
clippy-batch convention). The Go-source fidelity of the touched serialization
paths is unaffected — word-buffer layout code was only restructured, not
reinterpreted.
