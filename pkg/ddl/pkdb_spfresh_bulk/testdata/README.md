# SPFBLKOB cross-language golden data

Current binary fixtures are shared with `tikv/components/cloud_worker/src/spfresh/bulk/testdata`.
The single envelope has an 8-byte magic, kind (`u16_le`), header length
(`u32_le`), payload length (`u64_le`), JSON header and payload.
An explicit ManifestRef authenticates every complete object with SHA-256 before
structural decoding. There is no embedded digest or second checksum pass.
It has no separate format-version field. Every BaseFragment/RaBitQFragment row
ends with `stored_payload_len: u32_le` and that many bytes, including zero for
rows without a sidecar. Build consumers validate payloads against the frozen
stored schema; projected samples may omit them while retaining full-row digests.

The source digest is still the canonical full-row digest, not a hash of this
new framing. Old SPFBLK01 artifacts are rejected; cancel obsolete development
jobs and drop/rebuild indexes and artifacts. Compatibility exception: none.

Go produces `go_base_fragment.spfmvp`, `go_sorted_run.spfmvp`,
`go_projected_sample.spfmvp`, and `stored_base_fragment.spfmvp`:

```sh
SPFRESH_UPDATE_GOLDEN=1 go test ./pkg/ddl/pkdb_spfresh_bulk -run '^(TestCrossLanguageGoldens|TestProjectedSampleRetainsSourceDigestGolden|TestStoredBaseFragmentGolden)$'
```

The stored base fixture contains column-7 integer 42 and NULL sidecars. The
projected sample includes `08 0e 08 54` in the source digest and writes an empty
payload. Copy all four Go outputs to the Rust testdata directory first.

Rust produces `rust_base_fragment.spfmvp`, `rust_sorted_run.spfmvp`, and
`rust_stored_rabitq_fragment.spfmvp` (variable-size sidecars from the worker codec):

```sh
SPFRESH_UPDATE_GOLDEN=1 cargo test --package cloud_worker --lib spfresh::bulk::artifact
```

Copy all three Rust outputs to the Go testdata directory, then run both codec
suites without the update flag. Producers compare exact bytes; peers validate
headers, records, and source digests. These fixtures cover the current format;
there are no legacy-version fixtures or readers.

The `go_*.json` and `rust_*.json` files freeze the current distributed BuildPlan,
ShuffleResult, ShardInput, ShardResult and FinalManifest contract. Those document
bytes are unchanged by the artifact-envelope change. Their producer tests use
`SPFRESH_UPDATE_GOLDEN=1` in Go's manifest suite and Rust's
`spfresh::bulk::manifest::tests::test_manifest_canonical_round_trips_and_goldens`.
`stored_build_plan.json` records the ordered BIGINT schema and its explicit null
origin default. Copy matching JSON fixtures only after an intentional document
contract change.
