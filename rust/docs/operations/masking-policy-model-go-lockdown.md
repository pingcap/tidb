# Lock down `pkg/meta/model/masking_policy.go`

This lockdown owns one complete Go source file and one Rust module:

- Go authority: `pkg/meta/model/masking_policy.go`
- Rust implementation: `rust/crates/tidb-model/src/masking_policy.rs`
- Checked-in gate: `rust/crates/tidb-model/src/masking_policy_go_inventory.rs`

The pinned Go source is 3,311 bytes and 93 lines with SHA-256
`6680572e9eefa1aff3c71c2bedf5fb4ef6741ff2993135f77c03850d4b99cae4`.
The inventory classifies all 32 named declarations, both functions, and all
five function branch outcomes. Every row is PORTED; there are no DECLINED or
UNREACHABLE rows and no adjacent `masking_policy_test.go` to leave unowned.

This lockdown deliberately moves no differential oracle or ratchet. That is a
success: its deliverable is complete, drift-gated ownership of the source, not
ratchet movement.

## Measured Go contract

A direct program using Go's `encoding/json` established the boundaries used by
the Rust tests:

- zero `time.Time` values are present despite `omitempty`, as
  `"0001-01-01T00:00:00Z"`;
- `null` leaves every non-pointer field at its current zero value;
- JSON tag matching is ASCII case-insensitive, later duplicate fields win, and
  a later `null` does not erase an earlier concrete value;
- the status admits the complete byte domain and unknown masking-type strings
  round-trip;
- `restrict_ops` preserves all 64 Go bits, including `math.MaxUint64`;
- non-UTC offsets and nanoseconds survive JSON exactly;
- a nil `*MaskingPolicyInfo` clone returns nil, while a non-nil clone is a
  distinct value copy.

The zero and fully populated JSON documents are checked byte-for-byte through
the repository's Go-compatible JSON formatter, including HTML and U+2028
escaping.

## Gates

The inventory test fails if the Go hash, byte count, line count, function list,
inventory cardinality, serde implementation, or any PORTED Rust symbol drifts.
The behavior tests pin status switch branches, typed constant aliases, open
string values, every struct field and `omitempty` boundary, ordered JSON
decoding, time offsets, nil cloning, and value-copy independence.

Scoped validation uses a worktree-exclusive target directory:

    CARGO_BUILD_JOBS=12 CARGO_TARGET_DIR=<exclusive-target> \
      cargo test --offline --locked -j12 -p tidb-model --all-targets

    CARGO_BUILD_JOBS=12 CARGO_TARGET_DIR=<exclusive-target> \
      cargo clippy --offline --locked -j12 -p tidb-model --all-targets -- -D warnings

## Mutation proof

Mutation probes ran only in a disposable detached worktree made from
provisional commit `3061590a07`; the authoritative worktree was never mutated.
All 19 independent mutants were killed:

1. owner source byte/line drift;
2. disappearance of the PORTED clone symbol;
3. `DISABLED` status branch;
4. `ENABLED` status branch;
5. unknown-status default branch;
6. nil clone branch;
7. non-nil clone branch and copied value;
8. one-byte status representation widened to two bytes;
9. known typed masking constant spelling;
10. unknown masking-type string preservation;
11. empty masking-type omission;
12. zero restrict-operation omission;
13. full-width restrict-operation decode truncated to eight bits;
14. Go year-1 zero time;
15. non-UTC offset retention;
16. case-insensitive JSON tag matching;
17. last duplicate value wins;
18. later JSON `null` is a no-op;
19. exact JSON field tag/name.

Each mutant failed the boundary assertion for that rule. None merely failed a
recorded aggregate answer, and no mutant survived.
