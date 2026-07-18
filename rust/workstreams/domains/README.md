# Source-domain records

Each checked `*.toml` file is one bounded, cross-layer Go-to-Rust ownership
contract. A domain owns exact non-test Go source declarations through checked
`file:`, `func:`, or `method:` selectors, the Rust paths that implement its
current boundary, and selector-evidence paths under
`difftests/corpus/coverage/evidence/domain/` that name every selector. These
are distinct from parser-manifest fragments, which remain one `go_source`
row per upstream source file.
It also records the commands an agent must run before asking a steward to
integrate the domain.

`difftest domain_queue -- --check` validates the records. It rejects duplicate
or overlapping Go ownership, missing paths or evidence, an evidence file that
does not name every owned Go selector, unknown statuses, empty command lists,
and malformed records. A symbol-split Go file must account for every one of
its top-level functions and methods. The queue deliberately does not infer
ownership from a Rust filename: Go source is the primary key.

The record format is intentionally a narrow, dependency-free TOML subset:

```toml
schema = "2"
domain = "lowercase_domain_name"
owner = "stable-workstream-owner"
status = "partial" # partial, blocked, or ported
go_owners = [
  "file:pkg/example/source.go",
]
rust_paths = [
  "rust/crates/tidb-parser/src/example.rs",
]
evidence_paths = [
  "rust/difftests/corpus/coverage/evidence/domain/example.tsv",
]
required_commands = [
  "CARGO_BUILD_JOBS=12 cargo test --locked -j12 -p tidb-parser example",
]
```

Use only quoted strings and arrays of quoted strings. This keeps the records
easy to review and lets the verifier provide domain-specific diagnostics
without adding a TOML parser dependency to the differential harness.

## Local claims are leases, not ownership

Before editing a domain, an agent may create an untracked local lease at
`workstreams/claims/<domain>.claim.toml`. That filename is ignored by Git.
It may contain the agent name, start time, intended leaf, and handoff note;
it is advisory only and is never read by `domain_queue`. Remove the lease when
the work is handed off. The checked domain record, source/test ledgers, and
actual files are the durable source of truth, so a crashed agent cannot leave
the queue falsely claimed.
