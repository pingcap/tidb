# Whole-package manifests and frozen legacy evidence

The minimum Go-to-Rust transcreation unit is one complete upstream Go package.
A Go package may map to several Rust crates for native boundaries and faster
parallel builds, but it retains one umbrella claim, staging integration,
completion decision, and receipt.

Read [`../../PARALLEL.md`](../../PARALLEL.md) for the full execution protocol.

## Schema 2: package manifests

A schema-2 manifest owns complete Go packages atomically. At load and claim
time, `scripts/work-unit-queue.py` expands each package to every checked
production source, original test obligation, and content-addressed support
artifact. Test and support paths belong to the nearest ancestor Go package;
recursive `testdata/**` stays with the package above `testdata`, while a nested
directory with its own non-testdata Go files is a separate package. Test-only
packages are valid. External module packages fail closed until an equivalent
support inventory exists.

```toml
schema = "2"
slice = "planner-example-consumer"
status = "ready" # inventory | ready | active | blocked | covered
targets = ["tidb-planner", "tidb-exec"]
rings = ["plan"] # exact obligation rings, or ["unassigned"] when testless
consumer = "planner output consumed by executor result path"
test_target = "planner_example_source"
go_packages = ["pkg/planner/example"]
module_packages = []
depends_on = ["shared-datum-authority"]
rust_paths = [
  "rust/crates/tidb-planner/src/example.rs",
  "rust/crates/tidb-exec/src/example.rs",
]
integration_paths = [
  "rust/crates/tidb-planner/src/lib.rs",
  "rust/crates/tidb-exec/src/lib.rs",
]
```

`targets` is the complete Rust crate set, not a one-to-one translation of Go
files. Write-disjoint subteams may work inside the umbrella package claim, but
their branches merge only into the package staging branch and are never
independently promoted or receipted.

`rust_paths` is the stable, package-agent-owned write set. Those paths are
exclusive across schema-2 manifests and their bytes are content-addressed in
the durable package receipt. Optional `integration_paths` lists every existing
shared crate seam that the package requires the integration steward to edit
once, such as crate-root exports or shared dispatch. Each path must be
canonical, repository-relative, and inside one of the declared `targets`; it
must not overlap any schema-2 `rust_paths` by ancestry. Different packages may
name the same integration path because the steward serializes that seam.

Claims freeze both path lists exactly. Campaign close verifies both lists and
their existence. The package receipt records `integration_paths` and the gate
attestation, but intentionally does not hash or later revalidate seam bytes;
future steward edits to a shared seam therefore do not stale completed leaf
receipts. Package agents edit only `rust_paths`. The integration steward alone
edits `integration_paths`.

Schema-2 claim owners must equal their checked manifest names. Expanded source,
test, content-addressed support, and module sets are immutable claim snapshots;
inventory drift makes the claim stale, and `amend` is rejected. Each claim also
freezes `base_commit`. All active package claims entering one gate share that
base, and every committed change under `rust/crates/**` or a Rust Cargo
manifest since that base must fall under their combined `rust_paths` or
`integration_paths`. Gate begin additionally requires those Rust code and
manifest paths to have no staged, unstaged, or untracked changes. Dependencies
may reference only covered schema-2 package manifests. Schema-1 records and
leaf evidence rows are not package-completion signals.

After the shared gate, `release --owner <slice> --integrated` requires schema-2
status `covered`, an immutable integration receipt, and unchanged implementation
and test inputs. `release --owner <slice> --abandon` is the explicit recovery
path and asserts no integration.

## Schema 1: frozen legacy evidence

All schema-1 records in this directory predate package-complete transcreation.
They remain useful only to locate:

- exact Go source and test anchors previously inspected;
- Rust paths and focused targets containing partial implementation evidence;
- dependencies, claims, integration receipts, and transfer history; and
- bounded live or differential proofs already performed.

Their state describes only the recorded feature slice. It does not prove that
the owning Go package, module, Rust crate, or TiDB behavior is transcreated.
The exact accepted set is frozen in `legacy-schema1-slices.tsv`; a new
schema-1 name fails closed.

Historical non-integrated schema-1 campaigns use status `frozen`. Frozen
campaigns are archives: they cannot be claimed, activated, or gated. Planned
and active campaigns use campaign schema 2 and may contain only schema-2
package manifests. Campaign schema must match every member.

Do not create, copy, extend, reclassify, claim, or dispatch a schema-1 record
for new implementation. Do not infer package coverage from a legacy slice
marked `covered`. Migration preserves its exact evidence and associates that
evidence with the complete owning-package inventory without promoting status.
Raw schema-1 claims remain non-integratable evidence-repair leases only: they
have no Rust write set, cannot enter a shared gate, and can only be abandoned.

## Dispatch preflight

Before a package frontier is planned, the package owner and root steward:

- freeze the complete Go source/test/support inventory and dependency edges;
- declare every Rust target, stable package path, and steward integration seam;
- freeze interfaces and write-disjoint Rust subteam ownership;
- inspect each target crate's test registration and shared manifest/lockfile
  seams;
- trace validation to the last observable consumer; and
- run `python3 scripts/work-unit-queue.py check` before claiming and creating
  worktrees.

Package subteams run focused tests. The integration steward batches cross-crate
compilation, differential/live proofs, and the one expensive 12-job integration
gate only after the complete package frontier freezes.
