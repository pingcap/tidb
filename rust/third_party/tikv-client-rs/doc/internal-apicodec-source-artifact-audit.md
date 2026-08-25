# `internal/apicodec` source-artifact audit

This is the atomic completion receipt for client-go package `internal/apicodec`, pinned at commit `52c1e76cec993571493c81de442bcbef90cdc106`. The Rust implementation is integrated into the `tikv-client` crate and is validated with `nightly-2026-08-22`.

## Complete source inventory

`git ls-tree -r --name-only 52c1e76cec993571493c81de442bcbef90cdc106 internal/apicodec` contains exactly seven files:

| Source artifact | SHA-256 | Rust owner |
| --- | --- | --- |
| `internal/apicodec/codec.go` (152 lines) | `c66cfcc34441a3a1f67a1be4c216630e61a98a4df43f4450630dbcea41037be9` | Public constants/utilities and V1/V2 context integration in `src/request/{keyspace,mod,plan_builder}.rs` and `src/store/request.rs` |
| `internal/apicodec/codec_v1.go` (240 lines) | `eae454a467c77d877bcd6fa1727015a35ad64effaeb1681931561e8bc259a18e` | `ApiV1Codec`, V1 response implementations in `src/{raw,transaction}/requests.rs`, and request context construction |
| `internal/apicodec/codec_v2.go` (1,201 lines) | `d8606a62a448d0c1eeab9b7abd8de9519ac984cb3bdd3c5ac873fe55901102b9` | `ApiV2Codec`, `EncodeKeyspace`/`TruncateKeyspace`, typed request/response transforms, raw/transaction/TiFlash/store wrappers |
| `internal/apicodec/mem_codec.go` (57 lines) | `fd01c513ddfb80db1cbbdb018747013274bced0bb205735c67ebf771e2c7da29` | Complete `util/codec` byte codec, V1/V2 region transforms, typed `ApiCodecDecode`, and public `is_decode_error` classifier |
| `internal/apicodec/codec_test.go` (68 lines) | `5324f9902e7bf14d5d821fd3dca73053971cb6cf21ccaa3f8f051c2715b28dc2` | Source-derived keyspace utility, version, typed route, and context tests |
| `internal/apicodec/codec_v1_test.go` (8 lines) | `5af869aa3857601e0bbe8f15a2a90c964a7b2266fb13756dd0e8d102bdc310cd` | V1 bucket/region decoding tests; the pinned Go test body itself is empty |
| `internal/apicodec/codec_v2_test.go` (977 lines) | `b33045bc2e32f22664feee7073d2beb32d6208909012bb8e6caaf55b7321781d` | Source-derived V2 constructor, request/response matrix, boundary, error, MVCC, MPP, and bucket tests |

There is no `doc.go`, build-tag/platform variant, benchmark, fixture, example, package build file, or metadata artifact. The package imports generated protocol types but does not generate them. Their directly required pinned inputs and regenerated Rust outputs are nevertheless part of this receipt because an older schema cannot represent the source behavior.

## Required generated inputs

Client-go pins `github.com/pingcap/kvproto` at commit `059694ae4472276644613acccefa24cbc89d959f`. The eight protocol inputs directly carrying API-codec keys, errors, contexts, metadata, or request wrappers match that revision:

| Input | SHA-256 | Package-owned reason |
| --- | --- | --- |
| `proto/apipb.proto` | `ec7e7672893c2c44544b10b8d591a2113f5ac851ce25d78422699df46b115a91` | API V3 namespace/keyspace identities |
| `proto/coprocessor.proto` | `047bf9a5593908327fb0e9f87a9def843ddcc03c75236ab0a4f5b50fda5aa158` | Cop, BatchCop, region/range/task transforms |
| `proto/errorpb.proto` | `9a217e2ab8a8a77ab407a508ee3224a26c5f99a9911192b1acc52d3d9c93e1ea` | KeyNotInRegion and EpochNotMatch transforms |
| `proto/keyspacepb.proto` | `33dacfe45a870857eb401a5e6a5c525120e7ade07663c889f33b75d5208c1f15` | Numeric/V3 keyspace metadata, namespace messages, and lookup/load services |
| `proto/kvrpcpb.proto` | `d107a80efae8c17afd39f9274c0688b1bdfdd9c6819481aec73a3fbe8e963a2c` | V3 API/context/Compact identity oneofs and all transactional/raw key fields |
| `proto/metapb.proto` | `e1f5ea1f9f7701d087847a6a18385f3fa25f3355996fedc8435c55b8bca3a045` | Region bounds |
| `proto/mpp.proto` | `479798510cbd229b718bf5699e58bc6eea45d9d917d53b9313f6b4af8d81a166` | MPP TaskMeta numeric/V3 keyspace identity oneof |
| `proto/tikvpb.proto` | `4549bc2657d6ecb67407f4aa6f18a6cce41e9485cca975a6ee6dbbbd7efe2615` | TiKV RPC service request/response bindings |

`keyspacepb`, `kvrpcpb`, and `mpp` were stale and were copied from the pinned kvproto checkout. `cargo +nightly-2026-08-22 run -p tikv-client-proto-build` regenerated `kvproto/src/generated/{keyspacepb,kvrpcpb,mpp}.rs`; generated bindings now expose Namespace, LookupKeyspace, V3 metadata/context/Compact/MPP identities, and the pinned execution-detail additions. Other kvproto inputs not used to represent this package remain a repository-level generated-artifact gate and are not silently claimed here.

## Production mapping

| client-go production surface | Rust mapping and integration decision |
| --- | --- |
| `Mode`, `KeyspaceID`, defaults, prefixes, uint24 maximum, null ID | `KeyMode`, `u32`, `DEFAULT_KEYSPACE_ID`, `DEFAULT_KEYSPACE_NAME`, raw/txn prefix constants, `MAX_KEYSPACE_ID`, and pinned PD `NULL_KEYSPACE_ID` preserve all values. Enums make Go's invalid-mode panic/error branches unconstructible. |
| `CodecV2Prefixes`, `CodecV1ExcludePrefixes`, `ParseKeyspaceID`, `DecodeKey`, `BuildKeyspaceName` | Separate public Rust functions preserve sorted prefix semantics, validation, V1 identity, V2 prefix splitting, unsupported-version rejection including V1TTL/V3, and empty-name canonicalization. Rust `Result` does not return Go's unusable null-ID value alongside an error. |
| `Codec` interface, request pool, cached oneof wrapper | `ApiV1Codec`/`ApiV2Codec` expose the complete key/region/range/bucket contract; typed request traits own command transforms. Rust requests are owned values cloned at plan/shard/retry boundaries, so a mutable `sync.Pool` and shared pointer oneof would add aliasing without observable protocol behavior. |
| `setAPICtx` | Plan construction writes API version, numeric keyspace oneof, and canonical name before retry cloning. V1/V1TTL write `0xFFFFFFFF`; numeric V2 writes its ID; the no-prefix V2 embedding mode writes zero. Compact and MPP TaskMeta carry their dedicated API/keyspace fields. V3 identities are never reinterpreted as numeric zero. |
| `NewCodecV1` and `mem_codec.go` | Raw V1 region keys are identity; transactional V1 region keys are memcomparable; logical request keys/ranges remain identity. Empty boundaries remain empty. Malformed memcomparable metadata is wrapped in `ApiCodecDecode`, and public `is_decode_error` walks native Rust error chains like source `IsDecodeError`. |
| `codecV1.DecodeResponse` | The exact 27-command V1 matrix transforms only region errors for Get, Scan, Prewrite, Commit, Cleanup, BatchGet, BatchRollback, ScanLock, ResolveLock, GC, DeleteRange, PessimisticLock/Rollback, TxnHeartBeat, CheckTxnStatus, CheckSecondaryLocks, RawGet/BatchGet/Put/BatchPut/Delete/BatchDelete/DeleteRange/Scan/GetKeyTTL/CAS/Checksum. Commands outside that source switch remain untouched. |
| `NewCodecV2`, keyspace metadata | `keyspace_from_pd_meta` rejects the V3 identity arm, accepts absent legacy IDs as zero, and enforces the uint24 maximum. `keyspace_id_from_pd_meta` additionally requires enabled metadata. `ApiV2Codec` retains numeric ID, mode prefix, and incremented end prefix; canonical name remains on client/plan ownership rather than duplicating PD metadata in a value codec. |
| V2 key/range/region transforms | Point keys prepend/remove the four-byte mode/ID prefix. Empty ends map to the next complete prefix, including max-ID carry into the mode byte; reverse scans preserve the source endpoint rule. Region keys are memcomparable. Cross-keyspace region ranges clip to empty logical edges or error exactly where the source does. |
| V2 request encode matrix | Transactional commands cover Get, Scan, Prewrite, Commit, Cleanup, BatchGet, BatchRollback, ScanLock, ResolveLock, GC, DeleteRange, PessimisticLock/Rollback, TxnHeartBeat, CheckTxnStatus, CheckSecondaryLocks, Flush, BufferBatchGet, Flashback, and PrepareFlashback. Raw commands cover Get, BatchGet, Put, BatchPut, Delete, BatchDelete, DeleteRange, Scan, GetKeyTTL, CAS, and Checksum. Other transforms cover BatchCop, MPPTask, UnsafeDestroyRange, PhysicalScanLock, StoreSafeTS, Cop, CopStream, MvccGetByKey, and SplitRegion. Owned protobuf values provide source clone-before-mutation behavior. |
| V2 response decode matrix | Typed response implementations cover the same transactional and raw families, including all region, pair, lock, and per-key errors. Other branches cover UnsafeDestroyRange, PhysicalScanLock, CheckLockObserver, Cop, MvccGetByKey/StartTs, LockWaitInfo, and SplitRegion. BatchCop/MPP are deliberate no-ops; StoreSafeTS has no source response branch; CopStream returns the exact `streaming coprocessor is not supported yet` error after physical receipt. |
| Region/key/MVCC helper transforms | KeyNotInRegion key/range, EpochNotMatch sibling filtering, all KeyError key-bearing variants, shared-lock holders, nested MVCC info, locks, pairs, mutations, Cop ranges, region infos, table regions, TiFlash store tasks, wait entries, and split regions are represented. Empty optional lock keys remain empty instead of becoming a prefix. |
| Bucket transforms | V1 decodes every region key. V2 suppresses only first/last out-of-keyspace edges, removes duplicate empty starts, and preserves source interior filtering. BucketVersionNotMatch keys remain physical in region errors because neither source response switch decodes them; direct bucket responses use `decode_bucket_keys`. |

The Rust command boundary is intentionally compile-time typed. Client-go's `TestEncodeUnknownRequest` proves an unhandled dynamic command is context-only and otherwise unchanged; Rust has no user-constructible unknown command wrapper. The source-enumerated 53-route compile test plus StoreSafeTS/context and stream tests prove the equivalent boundary without inventing a dynamic escape hatch.

## Test/support mapping

Every original test is represented:

- `TestParseKeyspaceID`, `TestDecodeKey`, and `TestCodecListUtilityFunctions` map to `api_key_utilities_match_v1_and_v2_source_contracts`, version tests, the exact constant tests, and malformed/unsupported input cases.
- `TestEncodeUnknownRequest` maps to the complete typed route matrix and `store_safe_ts_keeps_its_contextless_api_v2_key_range_shape`; unknown dynamic commands are unconstructible in the Rust API.
- Empty `TestV1DecodeBucketKey` is superseded by `bucket_key_decoders_preserve_v1_and_apply_v2_edge_suppression` and V1 raw/txn region tests.
- `TestCodecV2/TestEncodeRequest` maps to request lowering/transform tests in `src/request/keyspace.rs`, `src/raw/requests.rs`, `src/transaction/requests.rs`, and `src/store/request.rs`, including the complete source command list.
- `TestEncodeV2KeyRanges` maps to point/bounded/unbounded/reverse range tables and Cop/TiFlash task tests.
- `TestNewCodecV2`, `TestNewCodecV2RejectsKeyspaceIdentity`, and `TestNewCodecV2RejectsNilMeta` map to uint24/mode/end-prefix tests and PD metadata identity/absence tests. Rust's numeric constructor makes a nil metadata pointer impossible; the PD adapter owns optional metadata validation.
- `TestDecodeEpochNotMatch` and `TestDecodeKeyError` map to sibling clipping/filtering and the complete nested key-error/lock/MVCC field tests.
- `TestDecodeResponseHotPathCommands` and `TestDecodeResponseSecondWaveCommands` map to the raw and transaction decoder suites, Cop/TiFlash/stream tests, plus the V1 exact-matrix regression.
- `TestDecodeMvccInfoPreservesEmptyLockKeys`, `TestGetKeyspaceID`, `TestEncodeMPPRequest`, and `TestDecodeBucketKeys` map directly to empty-field, metadata-state/identity, MPP TaskMeta, and bucket-edge regressions.

Additional production-derived tests cover exact V1 null-oneof stamping, canonical keyspace-name retention across clones, API V3 context isolation and generated schema availability, malformed-region-key classification through wrapped errors, maximum-ID end-prefix carry, shared lock wrappers, empty logical lock keys, malformed optional secondaries, transactional/raw response precedence, deprecated Cleanup/legacy GC, pipelined/flashback commands, physical/MVCC/observer/wait/split commands, and ordinary/TiFlash coprocessor transforms.

## Consumer inventory and ownership

Every pinned source importer was inspected and assigned without promoting its package:

- `internal/client/client.go` owns transport-time `EncodeRequest`/`DecodeResponse`; the complete Rust transport package consumes typed context and stream transforms.
- `internal/locate/pd_codec.go`, `region_cache.go`, and locate tests own PD boundary coding and special non-retry treatment of `IsDecodeError`. Rust's `src/pd/codec.rs` uses the complete codecs and typed decode errors; retry policy is now covered by the complete `internal/locate` receipt.
- `tikv/{client,compatible_txn_safe_point_loader,region,test_util}.go` own public aliases, construction, safe-point prefixing, and test factories. Their broader high-level behavior remains on the root `tikv` row.
- `txnkv/transaction/txn_file.go` uses only the numeric ID for chunk-writer metadata; the separate completed transaction receipt retains that integration claim.
- RawKV, transaction, snapshot, and lock request implementations consume the codec through the typed Rust request boundary even though client-go reaches them indirectly through `tikv.Client`. Their complete algorithms remain on their own ledger rows.

## Completion gates

The package is complete when focused keyspace/request tests pass in default and all-feature modes; complete default and all-feature library suites pass; all targets compile with all features; rustdoc, rustfmt, generated-input identity, and diff checks pass; and the ledger records exact results. No live TiKV/PD cluster is required to prove deterministic byte/protobuf transforms. Final API-v1/API-v2 differential validation remains mandatory for the owning high-level packages.
