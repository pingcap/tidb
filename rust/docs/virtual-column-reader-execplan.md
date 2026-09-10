# Restore Virtual Dependencies And Remote Record Identity

This living ExecPlan follows the repository root PLANS.md.

## Purpose

Match Go master fdfadb96b2c for virtual-column reader dependencies and
UPDATE/DELETE reads that retain the hidden record handle. Preserve original
SQL output width, row identity, staged replacements, and tombstones.

## Progress

- [x] Reproduce missing UniqueID in the forced-index SELECT preceding UPDATE.
- [x] Trace failure to IndexLookUpReader virtual-expression binding.
- [x] Expand dependent columns at cop-to-root conversion and restore output.
- [x] Remove the obsolete local-only restriction for extra record handles.
- [x] Original two write_range_reader tests pass.
- [ ] Finish independent red/green coverage, broad verification, and commits.

## Discoveries And Decisions

Go physicalop/task_base.go expands virtual columns before constructing readers;
physical_utils.go places dependencies before trailing synthetic handles. Go adds
a projection to hide those columns. Rust readers had only b,c while b depended
on a, causing ResolveIndices to fail. Expansion belongs at task conversion,
not as a bounds guard in chunk access or an early logical-pruning exemption.

The existing RemoteRowCursor already retains record keys while merging staged
rows. TableScanExec's claim that remote rows cannot supply _tidb_rowid was
obsolete. Use that keyed row stream, project virtual values, then insert the
handle; disable direct chunk transfer for that extra-output case.

Pre-resolved protobuf scan metadata must contain any dependency metadata needed
by expansion. Missing origin defaults must not be guessed. Production dispatch
builds its scan metadata later; pre-resolved consumers fail explicitly if their
dependency metadata is incomplete.

## Validation

Run from /tmp/tidb-hparser-current with RUSTUP_TOOLCHAIN=1.97 and
RUST_MIN_STACK=33554432. The exact original failing commands use cargo test
--manifest-path rust/Cargo.toml -p tidb-executor --lib followed by
write_range_reader_reconstructs_virtual_columns or write_range_reader.

Original failure log: /tmp/virtual-write-red.log. Stack evidence:
/tmp/virtual-schema-probe.log. After dependency expansion only, the test advances
to the UPDATE request-count assertion (/tmp/virtual-write-green.log). Both
original cases pass after remote handle materialization (/tmp/write-range-green.log).
Additional SQL coverage: virtual_dependency_expansion_preserves_reader_output.
Run planner task tests, executor tests, and make lint before readiness claims.

## Outcome

Work is in progress. Keep the two root causes independently reviewable and
do not treat the remaining TPCC failures or other full gates as passing.
