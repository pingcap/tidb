# Close the codec contract and its DistSQL consumer

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` current while work proceeds.

Reference: `PLANS.md` at repository root.

## Purpose / Big Picture

Close accepted `pkg/util/chunk/codec.go` together with `codec_test.go` and the production DistSQL `TypeChunk` consumer. After this work, the chunk wire image is decoded by its owning `tidb-chunk` crate, exact field-type widths remain owned by that crate, and DistSQL retains only one decoded source chunk rather than duplicating every column into a second matrix of owned rows. SQL readers continue to receive rows in the same order and exact requested count. Legal unconsumed suffix bytes follow accepted `DecodeToChunk` behavior instead of becoming a Rust-only response error.

This is one semantic cluster inside the still-incomplete `pkg/util/chunk` whole-package claim.

## Progress

- [x] (2026-08-10) Read accepted `codec.go`, `codec_test.go`, `select_result.go` consumer flow, Rust codec ownership, planner width duplication, and the complete DistSQL response iterator.
- [x] (2026-08-10) Proved `NewDate` width failed before the ownership correction: Rust returned 8 while accepted `EstimateTypeWidth` returns 32.
- [x] (2026-08-10) Moved exact field-type width estimation to `tidb-chunk` and removed the planner duplicate; the focused regression and planner/executor compile gate pass.
- [x] (2026-08-10) Replaced eager TypeChunk transpose with one live chunk-codec state and proved suffix, JSON/vector/NULL, channel-order, truncation, offset, and null-count boundaries.
- [x] (2026-08-10) Closed typed-payload validation at the row boundary so malformed JSON, enum/set, vector, and decimal cells return query errors instead of unwinding DistSQL.
- [ ] Classify all 138 codec production/direct-test obligations and generate proof/mutation evidence from one declarative manifest.
- [ ] Include the cluster in the next batched Ready validation and dual-remote `hparser-integration` push.

## Surprises & Discoveries

- Observation: `SelectResponseChannel` already decodes only one protobuf chunk at a time, and its caller stops after the exact `required_rows` count. A second streaming framework is unnecessary.
  Evidence: `SelectResponseChannel::next_row` installs one `chunks[next_chunk_index]`; `TableIndexReader::next` stops when its requested vector is full.

- Observation: the current TypeChunk path decodes a complete column matrix, materializes a second `Vec<Vec<Datum>>`, then transposes it. This duplicates the source chunk's live value memory and excludes types based on the secondary codec rather than the owning chunk representation.
  Evidence: `response_channel.rs::decode_channel` calls `decode_columnar`, `decode_datums`, and `transpose_columns` before constructing `ChannelIter`.

- Observation: accepted `Decoder.Reset` calls `DecodeToChunk` and ignores its returned suffix. The current response channel instead rejects every nonempty suffix.
  Evidence: accepted `codec.go::DecodeToChunk` returns the remainder; accepted `select_result.go::readFromChunk` does not inspect it; Rust emits `TypeChunk channel has ... trailing bytes`.

- Observation: adding the owning crate edge changed only `tidb-distsql`'s path dependency list in `Cargo.lock`.
  Evidence: the offline lock delta is one `"tidb-chunk"` entry under the existing `tidb-distsql` package; no registry package or version changed.

- Observation: checked column framing alone did not make typed payload materialization safe. Empty JSON, short enum/set, malformed vector, and invalid decimal cells passed structural decoding and then reached panicking trusted getters.
  Evidence: the exact DistSQL regression failed before at `row.rs` with `a JSON cell always carries its type code`; `Row::try_get_datum_row` now validates each declared type and returns `RowDecode` for the complete malformed table.

## Decision Log

- Decision: keep one decoded `tidb_chunk::Chunk` plus a row index in the DistSQL channel state.
  Rationale: this is the native Rust equivalent of retaining the decoder's intermediate chunk. It removes the duplicate transpose, supports the owning chunk's complete Datum domain, and keeps memory bounded to one source chunk without recreating Go slice headers or its eight-row copy optimization.
  Date/Author: 2026-08-10 / Codex.

- Decision: add a fallible chunk-codec boundary while retaining the source-shaped panicking methods.
  Rationale: accepted trusted-package calls panic on malformed byte slices, but the Rust network response boundary must convert malformed remote bytes into `ResponseChannelError`. One checked parser lets both contracts share the same wire ownership without duplicate decoders.
  Date/Author: 2026-08-10 / Codex.

- Decision: put fallible typed-cell materialization on `tidb_chunk::Row`, while trusted source-shaped getters remain panicking wrappers.
  Rationale: framing and typed validity are separate boundaries. A single row authority avoids duplicating Datum conversion in DistSQL and makes malformed network/storage input an ordinary error without weakening trusted package contracts.
  Date/Author: 2026-08-10 / Codex.

- Decision: keep exact `EstimateTypeWidth` in `tidb-chunk`, and let planner cost structures receive a precomputed width.
  Rationale: the accepted owner distinguishes `Date` from `NewDate`; the displaced planner enum collapsed them and produced a real cost error.
  Date/Author: 2026-08-10 / Codex.

## Outcomes & Retrospective

The width ownership correction and DistSQL state migration are green. `NewDate` now estimates 32 instead of the old collapsed fixed width 8. The suffix fail-before regression returned `RowDecode("TypeChunk channel has 4 trailing bytes")`; it now returns the row and cleanly exhausts. The typed-payload fail-before regression panicked on empty JSON; malformed JSON, enum/set, vector, and decimal cells now return `RowDecode`. The direct consumer retains one decoded chunk and materializes only the requested row, including valid JSON, vector, and NULL values. Receipt classification, mutations, and batch Ready gates remain.

## Context and Orientation

`rust/crates/tidb-chunk/src/codec.rs` owns the columnar wire image. `rust/crates/tidb-distsql/src/response_channel.rs` owns protobuf response lifetime and yields owned Datum rows to `tidb-exec`. `rust/crates/tidb-executor/src/access_cost.rs` consumes the codec's static width estimate; `rust/crates/tidb-planner/src/cardinality/row_size.rs` owns statistics formulas but must not duplicate codec type knowledge.

A `TypeChunk` protobuf `Chunk.rows_data` contains all encoded columns. Accepted `DecodeToChunk` decodes exactly the caller-provided field count and returns any suffix. The response iterator owns one protobuf chunk at a time, so storing one decoded chunk is sufficient for bounded row-by-row delivery.

## Plan of Work

Add `CodecDecodeError` and checked decode methods in `tidb-chunk`. Keep `Codec::decode` and `decode_to_chunk` as source-shaped panic wrappers over those checked methods. Add exact width boundary tests next to the owner.

Add `tidb-chunk` to `tidb-distsql`. Replace `SelectResponseChannel.decoded` with an enum: default encoding retains the existing owned-row iterator; TypeChunk encoding retains one decoded chunk and its next row index. Convert only the row being returned into Datum values. Keep protobuf row-metadata validation and existing error ordering. Ignore the checked decoder's returned suffix, as accepted `DecodeToChunk` does.

Add regressions for legal suffix bytes, malformed input returning a channel error, complete non-scalar TypeChunk values, final/intermediate channel order, and exact caller row budgets. Use one fail-before suffix regression and retain the existing `NewDate` fail-before transcript.

After behavior is green, use one declarative cluster manifest to classify the 85 production and 53 direct-test obligations, register semantic symbols/rules, generate proofs, and execute one mutation per invariant. Benchmark runner mechanics are eligible for evidence-backed DECLINED classification only after deterministic encode/decode workloads prove the production operations.

## Concrete Steps

From `rust/`, use the shared target cache and 12 jobs:

    cargo fmt --all -- --check
    cargo test --offline --locked -j12 -p tidb-chunk codec::tests --lib
    cargo test --offline --locked -j12 -p tidb-distsql --test all select_result_source -- --nocapture
    cargo test --offline --locked -j12 -p tidb-executor access_cost::tests::new_date_uses_chunk_estimate_type_width --lib -- --exact
    cargo clippy --offline --locked -j12 -p tidb-chunk -p tidb-distsql -p tidb-planner -p tidb-executor --all-targets -- -D warnings

At the batch boundary, run the repository Ready profile once, including `make -j12 lint`, the workspace tests required by the changed Rust crates, the package receipt checker, and exact dual-remote SHA verification.

## Validation and Acceptance

The `NewDate` regression must fail before with 8 versus 32 and pass after. A TypeChunk with a legal suffix must return its decoded rows and ignore the suffix. Malformed TypeChunk bytes must return `RowDecode`, not unwind the server. Temporal, decimal, JSON, enum/set, vector, NULL, signed, unsigned, float, and byte values must be materialized by `tidb-chunk::Row`. Final channels still precede intermediate channels; earlier chunks still precede later decode errors; caller-required row counts remain exact.

The direct consumer must retain no matrix of all decoded Datum columns or rows. Every accepted codec production/direct-test obligation must have one final evidence-backed verdict, while Go `testing.B`, unsafe slice reinterpretation, exact allocation growth, and incidental panic order are never claimed unless a TiDB boundary observes them.

Focused production evidence at the semantic checkpoint:

    cargo test --offline --locked -j12 -p tidb-chunk codec::tests --lib
    # 13 passed

    cargo test --offline --locked -j12 -p tidb-distsql --test all select_result_source -- --nocapture
    # 11 passed

    cargo test --offline --locked -j12 -p tidb-executor access_cost::tests::new_date_uses_chunk_estimate_type_width --lib -- --exact --nocapture
    # 1 passed

    cargo clippy --offline --locked -j12 -p tidb-chunk -p tidb-distsql -p tidb-planner -p tidb-executor --all-targets -- -D warnings
    # exit 0

## Idempotence and Recovery

All focused tests and manifest generation are rerunnable. Ledger generation must preserve the first seven obligation identity columns byte-for-byte. If a checked parser edit fails, restore only the codec and response-channel paths from the current semantic checkpoint; do not discard the already-proven width ownership patch.

## Interfaces and Dependencies

`tidb-chunk` exposes `estimate_type_width(&FieldType) -> i64` and a checked decode result used by `tidb-distsql`. `tidb-distsql` gains a one-way dependency on `tidb-chunk`; no dependency cycle is introduced because `tidb-chunk` depends only on datatype/util layers. Public `SelectResponseIter` continues yielding `Vec<Datum>` rows, so `tidb-exec` and server callers require no API migration.
