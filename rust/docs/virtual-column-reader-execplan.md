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
- [x] Finish independent red/green coverage and reader/planner verification.
- [x] Commit virtual dependency expansion separately as 9f18bd4f96.
- [x] Prepare the independent remote record-handle commit.

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
The disabled-expansion red regression is /tmp/virtual-reader-disabled-red.log.
The restored old local-only handle gate fails the new handle regression with
cop_scans 0 instead of 1 (/tmp/remote-handle-red.log). After restoring the fix,
the complete remote_scan::tests group passes 28 tests on the synchronized
293c474e25 base (/tmp/reader-final-green.log). Planner task tests pass 59 tests.
make lint passes (/tmp/reader-final-lint.log); git diff --check passes.

## Outcome

The two reader root causes have independent red/green evidence. Serial executor
validation before the remote catalog synchronization passed 1291 tests with
two TPCC failures (/tmp/executor-virtual-handle-serial.log). Parallel execution
also exposed a shared statistics-queue test race. Those and other full gates
remain open; these reader fixes do not complete the overall goal.
