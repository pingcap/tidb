# Split sample add-index TiKV prediction into all-encoding and new-encoding modes

Historical note (2026-05-25): the follow-up change requested by the user removed the separate
`sample_new_encoding_tikv_index_bytes` predictor from the current implementation. The live code now
keeps only `sample_predicted_tikv_index_bytes`, and the add-index TiKV capacity
precheck plus compatibility field `predicted_tikv_index_bytes` both use that single predictor.
The remaining content in this document records the superseded dual-predictor design that existed on
2026-05-21.

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root; this plan must be maintained according to it.

## Purpose / Big Picture

After this change, DXF add-index will expose two sample-based TiKV-capacity predictors instead of one:

- `sample_all_encoding_predicted_tikv_index_bytes`: compress sampled index KVs for both old encoding and new encoding through an in-memory SST estimate.
- `sample_new_encoding_tikv_index_bytes`: compress only sampled KVs that use the current code's "new encoding" definition, while keeping old-encoding sampled KVs on logical `len(key)+len(value)` bytes.

The actual TiKV space precheck will use `sample_new_encoding_tikv_index_bytes`.

The user-facing goal is to compare two physically calibrated sample models in logs while keeping the operational capacity gate on the narrower "compress new encoding only" model.

## Progress

- [x] (2026-05-21 17:10 +08:00) Re-read repository policy, `PLANS.md`, DDL orientation docs, and current add-index precheck / observed-capacity plans.
- [x] (2026-05-21 17:18 +08:00) Confirmed the current sample path extrapolates logical `len(key)+len(value)` bytes from sampled rows and does not model SST compression.
- [x] (2026-05-21 17:42 +08:00) Built sampled-KV collection and an in-memory Pebble SST estimator for compressed physical bytes.
- [x] (2026-05-21 18:05 +08:00) Validated the first single-predictor version locally, then replaced that direction with the dual-predictor requirement from the user.
- [x] (2026-05-21 18:46 +08:00) Reworked the sample predictor into dual modes with per-physical-table fallback semantics and switched the TiKV space precheck to `sample_new_encoding_tikv_index_bytes`.
- [x] (2026-05-21 19:08 +08:00) Updated regression coverage to validate old-only, new-only, and mixed sampled-KV behavior for the two predictor modes.
- [x] (2026-05-21 19:14 +08:00) Ran repository-required `Ready` validation (`make lint`) for the final dual-predictor implementation.

## Surprises & Discoveries

- Observation: `logical_index_kv_bytes` already measures exact logical KV bytes produced by the backfill write path, because `writeOneKV` sums `len(key) + len(value)` for each generated index KV.
  Evidence: `pkg/ddl/index.go` `writeOneKV(...)`.

- Observation: The sample predictor already captures real row shape well because it reads real rows from snapshot regions and runs the real index-KV encoding path.
  Evidence: `pkg/ddl/index.go` `sampleIndexKVsFromRegion(...)` and `collectIndexKVsForSampledRow(...)`.

- Observation: `GenIndexValuePortal` already gives a stable code-level boundary for "old encoding" versus "new encoding": old encoding means "without restore data, integer handle, local", while restored-data, common-handle, and global-index layouts use new encoding.
  Evidence: `pkg/tablecodec/tablecodec.go` `GenIndexValuePortal(...)` value-layout comments.

- Observation: TiDB's local-sort SST writer already exists in-repo and uses Pebble SST writing with a 16 KiB default block size when no custom block size is configured, making it a grounded approximation target for the physical-byte calibration layer.
  Evidence: `pkg/lightning/backend/local/engine.go` `newSSTWriter(...)` sets `config.DefaultBlockSize` and uses `sstable.NewWriter(...)`.

- Observation: Compressing restored-data string-prefix samples through SST can reduce the estimate much more aggressively than cluster observation would suggest, but the user explicitly wants that "all encoding" view exposed for comparison and also wants the "new encoding only" path to use SST compression.
  Evidence: local helper tests show restored-data samples shrink materially when routed through `DataSize + IndexSize + FilterSize`.

## Decision Log

- Decision: Replace the single sample predictor with two explicit outputs: `sample_all_encoding_predicted_tikv_index_bytes` and `sample_new_encoding_tikv_index_bytes`.
  Rationale: The user wants both an aggressive "compress everything" observational value and a narrower operational gate that only compresses new-encoding KVs.
  Date/Author: 2026-05-21 / Codex

- Decision: Use `sample_new_encoding_tikv_index_bytes` for the actual TiKV capacity gate.
  Rationale: The user explicitly chose this field as the precheck input, even though `sample_all_encoding...` is also logged.
  Date/Author: 2026-05-21 / Codex

- Decision: Define new encoding exactly by the existing code-path boundary: common handle, global index, or restored-data requirement.
  Rationale: This avoids inventing a separate heuristic and keeps the prediction mode aligned with the actual index-value encoding implementation.
  Date/Author: 2026-05-21 / Codex

- Decision: Apply fallback only at the failing physical-table or partition scope, never to the whole add-index task.
  Rationale: The user wants successful sampled partitions to keep their physical estimate even if another partition's SST calibration fails.
  Date/Author: 2026-05-21 / Codex

- Decision: When `sample_all_encoding...` SST estimation fails for one physical table, fall back that physical table's contribution to full logical sampled bytes; when `sample_new_encoding...` SST estimation fails, fall back only the new-encoding portion of that physical table to logical sampled bytes.
  Rationale: This matches the user-requested local fallback semantics and preserves as much calibrated signal as possible.
  Date/Author: 2026-05-21 / Codex

## Outcomes & Retrospective

Implementation is complete and the dual-mode helper behavior is validated locally:

- old-encoding numeric sampled KVs are compressed only by `sample_all_encoding...`;
- new-encoding restored-data sampled KVs are compressed by both predictors; and
- mixed sampled-KV inputs produce a `sample_new_encoding...` value equal to `old logical bytes + new encoded physical bytes`, while `sample_all_encoding...` stays smaller because it compresses both subsets.

The remaining follow-up is only broader empirical tuning if we later decide to recalibrate either predictor against more dedicated-cluster cases.

## Context and Orientation

This work stays inside the existing owner-side add-index precheck path in `pkg/ddl/index.go`.

`DXF add-index` means distributed add-index backfill driven by the DDL owner and the dist-task framework. The owner performs the TiKV capacity precheck only on the first task submission path inside `(*worker).executeDistTask(...)`.

The sample predictor still works from real table rows:

1. list a few table regions,
2. sample a small number of real rows from those regions,
3. generate real index KVs for those rows,
4. aggregate per-row bytes,
5. multiply the sampled average by estimated table row count.

The change is only in step 4:

- one path now compresses all sampled KVs through an in-memory SST estimate; and
- the other path compresses only sampled KVs that use new encoding, while leaving old-encoding sampled KVs on logical bytes.

## Plan of Work

First, extend the sample-prediction helper layer in `pkg/ddl/index.go` so it can retain sampled index KVs together with an old/new encoding classification derived from the existing index-value encoding rules.

Next, reuse the in-memory Pebble SST helper to estimate compressed physical bytes for arbitrary sampled-KV subsets. Keep this helper focused on the SST payload estimate itself so the caller can decide which sampled subsets should be compressed for each predictor mode.

Then, update `predictTiKVIndexBytesSample(...)` and `estimatePhysicalTableSampleBytesPerRow(...)` to produce both predictor values for every physical table and aggregate them independently across the task.

After that, wire the new predictor fields through task metadata, precheck logging, and post-task observation logging, and switch the TiKV capacity gate to `sample_new_encoding_tikv_index_bytes`.

Finally, update regression tests and this ExecPlan to validate the split behavior and the local fallback semantics.

## Concrete Steps

Work from repository root:

1. Edit `pkg/ddl/index.go` to compute dual sample predictions and switch the TiKV capacity gate to the new-encoding predictor.
2. Edit `pkg/ddl/backfilling_dist_executor.go` to store the two sample values in task metadata.
3. Edit `pkg/ddl/reorg_util_test.go` to validate old-only, new-only, and mixed sampled-KV inputs.
4. Run targeted DDL tests with failpoints enabled because `pkg/ddl` contains failpoint usage.
5. Run repository-required completion checks before claiming the task is done.

Expected validation commands:

    cd /Users/xiaoli/coding/tidb
    GOSUMDB=sum.golang.org GOPATH=/Users/xiaoli/go GOCACHE=/private/tmp/tidb-gocache ./tools/check/failpoint-go-test.sh pkg/ddl -run TestCollectTiKVStoreUsage -count=1
    make lint

## Validation and Acceptance

The change is accepted when:

1. Task metadata and logs expose `sample_all_encoding_predicted_tikv_index_bytes` and `sample_new_encoding_tikv_index_bytes`.
2. The TiKV capacity gate uses `sample_new_encoding_tikv_index_bytes`.
3. Focused tests demonstrate that old-encoding sampled KVs are compressed only by the all-encoding predictor.
4. Focused tests demonstrate that new-encoding sampled KVs are compressed by both predictors.
5. Focused tests demonstrate that mixed sampled-KV input produces distinct all-encoding and new-encoding estimates.
6. The targeted DDL test command passes, and `make lint` passes or any unrelated baseline failure is explicitly documented with evidence.

## Idempotence and Recovery

The helper changes are safe to rerun because they only affect owner-side prediction code and unit-test-only scaffolding.

If a future change makes SST calibration unstable for one physical table:

- keep logical sampled-byte accounting intact,
- keep the per-physical-table fallback local instead of aborting the whole predictor,
- rerun the same targeted test command,
- record the instability and the fix in `Surprises & Discoveries`.

## Artifacts and Notes

Targeted validation result:

    cd /Users/xiaoli/coding/tidb
    GOSUMDB=sum.golang.org GOPATH=/Users/xiaoli/go GOCACHE=/private/tmp/tidb-gocache ./tools/check/failpoint-go-test.sh pkg/ddl -run TestCollectTiKVStoreUsage -count=1

Observed result:

    PASS
    ok  	github.com/pingcap/tidb/pkg/ddl	1.103s

Ready validation result:

    cd /Users/xiaoli/coding/tidb
    GOSUMDB=sum.golang.org GOPATH=/Users/xiaoli/go GOCACHE=/private/tmp/tidb-gocache make lint

Observed result:

    linting
    ...
    [exit 0]

## Interfaces and Dependencies

Expected touched areas:

- `pkg/ddl/index.go`
  - update `(*worker).predictTiKVIndexBytesSample(...)`
  - update `(*worker).estimatePhysicalTableSampleBytesPerRow(...)`
  - add helper(s) for splitting sampled KVs into old/new encoding subsets and estimating their SST physical size

- `pkg/ddl/backfilling_dist_executor.go`
  - store `sample_all_encoding_predicted_tikv_index_bytes` and `sample_new_encoding_tikv_index_bytes`

- `pkg/ddl/reorg_util_test.go`
  - extend the existing `TestCollectTiKVStoreUsage` top-level test with new subtests covering dual sample-prediction behavior

- `pkg/lightning/config`
  - reuse `DefaultBlockSize` as the local-backend-compatible block-size default for the in-memory SST estimate

- Pebble SST writer dependency
  - use `github.com/cockroachdb/pebble/sstable`, `github.com/cockroachdb/pebble/vfs`, and `github.com/cockroachdb/pebble/objstorage/objstorageprovider` only as implementation details of the calibration helper; no external behavior should depend on these package names directly.
