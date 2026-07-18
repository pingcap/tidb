# Upstream test obligation ledger

`corpus/coverage/go_test_inventory.tsv` is the mechanical source inventory
for the rewrite. It lists every discovered Go test source, `Test...`,
`Benchmark...`, `Fuzz...`, and `Example...` entry point, gocheck lifecycle
hook, every concrete Bazel test target (`go_test`, `sh_test`, `py_test`,
`cc_test`, `java_test`, `rust_test`, or `test_suite`), every unique non-path
Make target whose name identifies test execution or test lifecycle, every
exact Go `//go:embed` or `os.ReadFile`/`os.Open` fixture reference that resolves
to a checked-in file outside the conventional test artifact directories,
every shell test program under a `test`/`tests` suite (plus explicitly
test-named shell runners), every executable/config/data support artifact below
a repository `test`/`tests` suite (including extensionless runners), every SQL
input fixture under `tests/**/t/*.test`, and every checked-in
expected result under `tests/**/r/*.result`, including the regular integration
and cluster-integration suites, with its source path and line. The verifier
also requires every SQL input to have an expected result and every result to
belong to an input. One input may have a normal result plus underscore-suffixed
environment variants such as `_enabled` and `_disabled`; this is modeled as a
result family, not an exception list. Exact names take precedence over a
suffix-family match, and an output that still resolves to multiple inputs is
rejected instead of being silently assigned.
It is intentionally not a parity claim: an entry starts as `UNTRIAGED` until a
porting wave links it to a Rust differential corpus and closes the relevant
parser, plan, result, or transaction ring. Sparse source-owned fragments under
`corpus/coverage/evidence/tests/*.tsv` are the only way to attach that claim:
each fragment names the exact upstream source anchor, status, owning wave,
existing Rust evidence artifact, and a reason. A feature wave owns one stable
owner-named fragment, while the generated inventory remains stewarded. The
generator reads regular `.tsv` fragments in sorted path order and rejects
unknown entries or duplicate anchors across fragments. When several bounded
slices prove one fixture's `PARTIAL` status, the artifact is a checked-in
per-fixture evidence manifest linking every corpus pair rather than silently
dropping all but one. The ledger verifier also rejects stale source anchors,
nonexistent artifacts, and any status other than `PARTIAL`, `COVERED`, or
`BLOCKED`.

The pinned client-go and pd-client universes follow the same conservative rule
in one module-qualified set of checked files.
`external_go_test_declaration_inventory.tsv` records all AST function
declarations, including helpers and invalid runner signatures;
`external_go_test_inventory.tsv` contains only structurally valid
Test/Benchmark/Fuzz/Example/TestMain obligations with file SHA-256 and an
initial `UNTRIAGED` state. `external_go_ledger --check` resolves only the
offline module cache and rejects direct-pin, replacement, exact Go-sum, file,
declaration, runner, and duplicate qualified-key drift.

The invariant is simple: no upstream test may be invisible. Run this from the
Rust workspace after Go tests change:

```sh
cargo run --locked -j 12 -p difftest --bin go_test_ledger -- --check
```

The command fails when any source test is added, removed, renamed, or moved.
Regenerate only as part of an explicit porting/triage change:

```sh
cargo run --locked -j 12 -p difftest --bin go_test_ledger -- --write
git diff -- rust/difftests/corpus/coverage/go_test_inventory.tsv
```

Do not convert generated `UNTRIAGED` entries to a covered status by hand. Add
a row to that wave's `evidence/tests/<owner>.tsv` fragment in the same change
as the owning corpus/test, then run the ledger check. `PARTIAL` means only a
bounded source slice is covered;
`COVERED` means the entire anchored test/fixture is covered; `BLOCKED` requires
an explicit nonempty reason. The status is deliberately conservative until
that mapping exists.

The executable-definition rules are intentionally structural. Bazel macros
whose names merely contain `test`, Make path targets such as
`tools/bin/gotestsum`, computed or wildcard Go fixture paths, and dynamic file
construction are audit exclusions rather than guessed obligations.
Extensionless and Python runners below `test`/`tests` were audited and are
already visible through the suite-artifact rule.

For parallel work, first split entries by `differential_ring`, then dispatch
the generated source units. A source unit is normally one exact Go test file
together with all of its discovered tests, lifecycle hooks, static subtests,
dynamic-subtest generators, and exact external fixture references; one exact
Bazel or Make target (`<definition file>#<target>`); one shell test program,
one SQL input plus all of its expected-result variants, or one exact testdata
or test-suite support artifact. This is intentionally narrower than a Go package: `pkg/expression`
and `pkg/util` contain hundreds of unrelated obligations and cannot be one
agent-owned queue. Each agent owns its source unit, target corpus pair, and
Rust implementation leaf; the orchestrator owns the ledger and shared
dispatch seams. The dispatch queue is generated, not hand-maintained:

```sh
cargo run --locked -j 12 -p difftest --bin go_test_ledger -- --queue result
```

The queue prints `ring`, source unit, evidence status, and count. Use the
optional `package` view only to summarize team-level backlog:

```sh
cargo run --locked -j 12 -p difftest --bin go_test_ledger -- --queue result package
```

Assign `UNTRIAGED` source units first; a `PARTIAL` row is an intentional
incomplete source slice, not permission to claim the fixture or Go test is
complete.

## Shared Go test files

`corpus/coverage/go_test_domain_manifest.tsv` is the only opt-in for splitting
a shared Go test file. Each row is an exact
`source_path:source_line:test_name` anchor for a discovered top-level
`go_test`, paired with its stable test domain. A file absent from this manifest
remains atomic. For a file present in it, the generator dispatches claimed
top-level tests (and their static `t.Run` children or dynamic `t.Run`
generators) to `domain:<test_domain>`, renders every unclaimed top-level test
as its own explicit `UNTRIAGED` inventory-anchor unit, and emits a separate
`#shared-support` unit for the file-level, lifecycle, and fixture contract.

The manifest is checked against source on every ledger run. Duplicate or stale
anchors fail. In a split file, every top-level test that has `PARTIAL`,
`COVERED`, or `BLOCKED` evidence must have exactly one domain row; every
unclaimed top-level test must therefore remain visibly `UNTRIAGED` in the
generated inventory. That generated row is the remainder declaration; do not
duplicate it in the manifest or replace it with a wildcard. Do not add a
function range or a package-level ownership record. A
table-driven `t.Run(tc.name, ...)` is emitted as a `go_test_generator` at its
callsite, so runtime-generated cases cannot disappear from the ledger; a
literal `t.Run("case", ...)` is emitted as an individually anchored
`go_test_subtest`.

`deferred-external` is an explicit queue for BR, Lightning, and Dumpling and
their owned support packages. The rewrite design keeps these independent Go
binaries out of the SQL-node migration initially. This is neither an omission
nor a covered status: their tests remain generated, visible, and `UNTRIAGED`.
It prevents their separate migration schedule from obscuring core SQL-node
work in `unassigned`; it must not be used for server, protocol, or other
unclassified SQL-node paths.
