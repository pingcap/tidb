# Ranger parity audit ExecPlan

## Objective

Keep the Go `pkg/util/ranger` package and Rust `tidb-planner::ranger` aligned as one complete package claim. Every Go production file, test, fixture/generated/platform/build artifact is inventoried before edits; source behavior and intentional boundaries are recorded in the parity receipt.

## Completed this batch

1. Read and inventoried all 13 Go artifacts under `pkg/util/ranger` and `pkg/util/ranger/context` (8,324 lines), including both Bazel files and all test/benchmark/setup files. No additional fixture or generated/platform variant exists under the package directory.
2. Read the 7 Rust owner files and the transcreated Go-case suite (9,781 lines).
3. Implemented the previously omitted `RangesToString`/`RangeSingleColToString` behavior as typed Rust helpers, including composite validation, exclusion semantics, special-bound simplification, SQL literal restoration, and error boundaries.
4. Aligned the `TestIssue40997` fixture pipeline with Go's pre-ranger comparison refinement so quoted integral BIGINT predicates retain their integer ranges; the complete ranger suite now exercises this regression.
5. Added focused regression tests and ran the complete ranger unit subset successfully.

## Validation gate

- [x] Rust ranger regression subset passes.
- [x] Go ranger tagged tests (`-tags=intest`) pass.
- [x] Ready profile `make lint` passes.
- [ ] Review the staged diff and create one meaningful ranger batch commit.

## Next loop

After the Ready gate and commit, continue with the next uncovered Go package. Do not mark the overall repository audit complete until every package has an inventory, a Rust owner/boundary decision, focused regressions for each source fix, and a receipt.
