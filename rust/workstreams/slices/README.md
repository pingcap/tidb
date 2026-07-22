# Frozen legacy feature-slice evidence

This directory contains schema-1 records from the earlier feature-slice
workflow. They are frozen legacy evidence, not the ownership, dispatch, or
completion unit for new Go-to-Rust work.

The minimum transcreation unit is now one complete upstream Go package or
module together with its complete original package test/support inventory.
Read [`../../PARALLEL.md`](../../PARALLEL.md) for the package-based protocol.

## What these records still mean

Existing schema-1 records may be used to locate:

- exact Go source and test anchors previously inspected;
- Rust paths and focused targets that contain partial implementation evidence;
- dependencies, claims, integration receipts, and transfer history; and
- bounded live or differential proofs already performed.

Their state describes only the recorded feature slice. It does not prove that
the owning Go package, module, or TiDB behavior is transcreated.

## Freeze rules

- Do not create new schema-1 feature slices.
- Do not copy an existing record as a template.
- Do not add functions, files, branches, SQL shapes, or new package work to an
  existing schema-1 record.
- Do not infer package coverage from a slice marked `covered`.
- Do not rewrite slice state, generated ledgers, or status while associating
  legacy evidence with a package manifest.

Package migration must preserve the record and its exact evidence, then map
that evidence into the complete owning-package inventory. Any unrepresented
source, test, fixture, support program, generated artifact, or build metadata
keeps the package incomplete.

## Transitional tooling

`scripts/work-unit-queue.py` and `scripts/slice-worktree.py` may continue to
read these records for archaeology, validation, or closure of an already
active legacy claim. They must not dispatch new feature-slice implementation.

New dispatch waits for the checked package-manifest and package-DAG format
specified by [`../../PARALLEL.md`](../../PARALLEL.md). Until that tooling lands,
audit and group obligations by whole package; do not work around the gate by
opening another schema-1 slice.
