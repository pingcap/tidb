# `pkg/objstore` — complete Go-master parity boundary receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The root `pkg/objstore` Go package contains 27 root-level tracked artifacts and
8,388 lines. Every root production source, test source, platform variant,
support file, and BUILD target was read in full before this receipt was
written. There are no fixture directories, generated source files, fuzz
corpora, or package-local build artifacts beyond `BUILD.bazel`. The
`compressedio`, `objectio`, `ossstore`, `recording`, `s3like`, `s3store`, and
`storeapi` directories are separate Go packages and are not silently folded
into this root claim; each requires its own complete audit.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 111 | `61f0bc63abbc700ccd8e950653a95f7539806ecb` | `d96ff886547df0e34c6a39bd81be077adbc4ae2e51ec21a1b36d2286e6322e3c` | public library, 50-shard flaky test target, and dependency map |
| `OWNERS` | 11 | `c5c804403c2fc4c8fc00194055ea33add1a7d30c` | `0bb8fe75f6854955c1d1af57035676de2c4b0ba73082bf04580461ba7607b860` | package ownership and BUILD approval filters |
| `azblob.go` | 975 | `9bfc3fe9e05b9e5f8b5d976d1959e5cc13907645` | `95ebfa74770b1031df369bb0007217aa3b57d9160f68f3b1b1f4085a3326e807` | Azure credentials, client builders, copy, range reader, and block uploader |
| `azblob_test.go` | 545 | `d7000eee3e963212bdb9bf5d4d760077812cbe29` | `4320783d5e5982cd25c04e47671c6aac7ce79af36655da64df843c7dc1c53da7` | Azure builder, retry, seek, copy, and concurrent-upload tests |
| `batch.go` | 192 | `6984a5b1d30d1054f6d92c94ae2dd8c0443bc5e8` | `715df6a2342c2efafe9f3d314afae017e28bb9097f958ec864ecc94c2ca4fc67` | dry-run storage effect capture, JSON encoding, and commit |
| `batch_test.go` | 122 | `87178e264e26bbbe8b21cdd8c1eceb667ce9cfad` | `204c1d1afc14be5b733106bc52c4bcd9a110d5a18e64fc9fb198101c437523ad` | batched effects and JSON serialization tests |
| `compress.go` | 202 | `3a023510785ff26262194fe6b141de716102a560` | `5c866e19668803cf238c017de82afe09e404a46ef5d73b4beb1686958f89244b` | compression storage wrapper, bounded decompression, and flush writer |
| `compress_test.go` | 56 | `77293effb6283f086ef1657e2c4a5ffe5e83da40` | `97da06c9b47e7e5bbac783675671b729c16b1a9b382b7d5e4c4a04dea12c2fad` | local gzip read/write round-trip test |
| `flags.go` | 48 | `21d4e0961b1c047ed4f643cfe40b514d9e3df9f2` | `d259ef4ca067f5c98a09ee84fd75af71671e1aea4d923565d5a60a8c990b9aca` | backend flag definition, hiding, and option parsing |
| `gcs.go` | 760 | `483e67a0d39fed0c15ae200186300cee74ad0188` | `273a5daeb6de7b69f6463439ce46ae8f2b4ad8227542c8096cdd0201a155de00` | GCS clients, credentials, retries, object operations, and ranged reader |
| `gcs_extra.go` | 442 | `0ba53045651493d05d2cf4b3f3b8652a85091afe` | `f971dd639747659fbea0edf1813d513f0ffbec4e6b903ce99454f30c6ed9093a` | signed-URL XML multipart writer, workers, finalize, and abort |
| `gcs_test.go` | 800 | `811fc63f4468f1f8f6bf9cc538dde49ed1302283` | `7e8fb53f725dae11cf3278856c74e6a8b64cd916d8a55d6fb830a0fd8f68461b` | fake-GCS CRUD, ranges, retries, recording, and multipart-abort tests |
| `hdfs.go` | 161 | `c6493af0291350d6c0b1b53631a45a90e98edbf8` | `8385706d507923bb0bf1866fb8abd855ace007f541231e335792ad0412fca3a3` | HDFS CLI storage adapter and intentionally unsupported operations |
| `helper.go` | 127 | `0b3b5bc047737de8003ded761e98b36dc595ea9c` | `5cfd63a7c91bd3f2cab28508b7bd515c16c10df2347371100e42934ce853d1a2` | cloud URI validation, upload-worker gauge, and parallel unmarshal iterator |
| `local.go` | 383 | `4b898396bf9d406defba9a423c78c3b9562e0dfb` | `343d0b4e47906a6c865e54206eeae5542ffb18812492e7ff10cbb025a1906cde` | local filesystem storage, walk/range semantics, atomic writes, and copy |
| `local_test.go` | 296 | `254b75bb95d4a65184b6038c22d2a689592193c8` | `3f33230c805b78ee6e2cda462e5620e4b4f2a1b8be697205ed9b72397c146c21` | local deletion, symlink, walk, range, URI, and tombstone tests |
| `local_unix.go` | 31 | `32659eed2b4979bdd92fbc6e32a8d732c919c6c8` | `76fc47f0ade72af45610ba0c53bb7ceef1f528e952da7a4c039f2361e16388fc` | non-Windows umask-preserving directory creation |
| `local_windows.go` | 25 | `a9f14f69742f0aaa850c5559c3e56566cff569e9` | `a6238ce115a47f7cb4e9263576a31bf3983309d3ece5889806286c6a8a0ddd7c` | Windows directory creation variant |
| `locking.go` | 716 | `427a60d76cdb109dc48922bb9ce893d5fa735edb` | `f9811c11e083e59e5b670e037c0ad0b8647d774f423c4b66babb72bb9c028b9c` | conditional remote locks, read/write conflicts, retries, and diagnostics |
| `locking_test.go` | 552 | `452374d563e2eef4b71a0aba3de2025f23a9ca94` | `d8dc98876aeb458d9fe996f34109a49065695b82719b21188070b7e4f9aa3e9e` | lock races, metadata compatibility, conflict enrichment, and logging tests |
| `memstore.go` | 366 | `ebccd4fe340f74beda471b12824ca11759fbac03` | `e7940b3163c421b1a30b217ff8c3c24bf368d4d044e11821563bcd97017a763b` | concurrent in-memory storage, snapshots, range reader, and writer |
| `memstore_test.go` | 314 | `25c675eecb67db7bcd4c06a714a8602daddaae30` | `84e0c1478b9dcc12e967fda367bb3980f87ea22349d86447e67fc4a09f5d743c` | memory CRUD, range/seek, walk filtering, cloning, and concurrent mutation tests |
| `noop.go` | 117 | `1548d868eea6f77d68154d74c45519a5a04ae628` | `8384285991a819ae54571133b702aeaecc1d486a7a1504ec6a2e7f7cf674981a` | no-op storage, reader, and writer implementations |
| `parse.go` | 256 | `cd2797a2e545b2ed4f1ee74f35550fb3c2ad67ff` | `2b3906089443dfa62a1345efab7d3af77c901859fb5cecf57e1ef382c4753475` | URL parsing, backend construction, query normalization, and redacted formatting |
| `parse_test.go` | 571 | `cd9b8b76e4ca879351b9fa1fbc446c7c3a261fa2` | `940b2ec72ae99ccd09009731778f369448abf8952f6ec71cac4c4c42d693a131` | backend URL, credentials/profile, force-style, local, and redaction tests |
| `storage.go` | 163 | `7c709c1d1f64e419b643c6fadde997d196b67e22` | `8d18cf4a5c005931460f49d059d345eecfc901caac52c5283dad590acb1c2447` | backend dispatch, HTTP transport, and bounded range reads |
| `storage_test.go` | 46 | `4bdd7b8c9766b9e4335b79c92a693518d170753a` | `9fa4967ca5b44e44bed28cc500cfa6d86f37f829502f7625e0ebf4fa41bd925a` | default transport/client and memory-storage constructor tests |

The 17 production/support files contain 254 functions and methods. The nine
root test files contain 59 top-level tests and no benchmarks. Function-level
coverage was read across every backend and helper: Azure credential selection,
copy polling, range seek/reopen, block staging and commit; GCS client reset,
credential propagation, retry classification, prefetch/range reads, XML
multipart workers/finalization/abort, and access recording; HDFS command
construction and documented unsupported methods; atomic local writes,
symlink/tombstone traversal, bounded readers, and platform `mkdirAll`; memory
snapshot/cloning and concurrent walk/write behavior; no-op methods; URL/query
normalization and backend dispatch; batching/compression wrappers; and the
conditional lock transaction, conflict sampling, metadata compatibility,
retry, cleanup, and diagnostics paths.

Test support and fixtures were also read: fake-GCS server setup, in-process
Azure HTTP/Azurite builders, TLS multipart server, temporary credentials,
symlink and directory trees, failpoint lock-race coordination, and all
temporary local/memory storage fixtures. The package's only platform split is
`local_unix.go` (`!windows`) versus `local_windows.go` (`windows`); no generated
Go source or checked-in fixture/build output exists.

The current Go-master delta from the earlier pinned source
`e2788410d8d696605e8cb002585877a063ccc909` was read before this receipt:
root BUILD adds the parser AST dependency; Azure adds configurable part size,
errgroup-backed concurrent block staging and error-aware commit; GCS multipart
upload reuses an injectable Resty client, uses the shared max-part constant,
aborts staged uploads on worker/finalize failures, and wraps cancellation
errors; local creation honors `WriterOption.PartSize`; URL errors use
`ast.RedactURL`; and the corresponding Azure/GCS tests cover these regressions.
OWNERS was converted to filtered approval rules. Nested package deltas are
tracked in their own package inventories.

## Rust ownership and explicit boundary

Rust has no dependency-closed owner for Go `pkg/objstore`'s storage interface
and backend implementations. The plan-replayer crate's `DumpFileStorage` is a
small caller-owned trait boundary, not an S3/GCS/Azure/HDFS/local object-store
implementation. Generated TiKV protobuf types, transaction GC state, and local
`tidb-util::extsort` likewise provide protocol vocabulary or unrelated local
sorting, not this package's credential handling, locking, multipart uploads,
range readers, or storage dispatch.

No Rust-only object-storage behavior was found to remove. Inventing a partial
Rust storage facade, cloud SDK adapter, or cache-only memory backend would not
complete this package and would change ownership of the BR/Lightning storage
composition roots. The root package therefore remains an explicit parity
boundary; nested packages are intentionally open audits rather than implied
Rust implementations.

## Validation and risk

Profile: **WIP** for this documentation-only boundary record. No Go, Bazel,
module, or Rust source changed in this batch, so `make bazel_prepare`, Ready
lint, and Rust cargo gates are not required. The package uses failpoints; the
canonical wrapper enabled and disabled them around the exact Go-master suite.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
./tools/check/failpoint-go-test.sh pkg/objstore -count=1
# exact Go origin/master source: passed in 31.069s; failpoints cleaned to refcount 0
# external-storage-only speed/multipart tests skipped because no testing URI was set
```

Not verified here: Bazel's 50-shard/race target, live Azure/Azurite, GCS,
HDFS, S3-compatible or object-store services, Windows execution, and full
workspace tests. No Rust validation was applicable because no Rust source
changed.
