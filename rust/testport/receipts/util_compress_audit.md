# Audit of `pkg/util/compress`

Status: complete atomic inventory; package not claimed implemented.

## Pinned inventory

Behavioral source: Go commit
`e2788410d8d696605e8cb002585877a063ccc909`.

| Artifact | Bytes | Blob |
| --- | ---: | --- |
| `pkg/util/compress/gzip.go` | 947 | `4e4b71cf13536d9754e5eeef0ce1df0b3ddd693b` |
| `pkg/util/compress/BUILD.bazel` | 272 | `f00125ae87b3747c023dff7bab41e7e30f953048` |

There is no `doc.go`, test, support file, fixture, benchmark, generated source,
or platform variant.

## Whole-package behavior

The package exports two process-global `sync.Pool` values. Their constructors
return a klauspost gzip writer initially bound to `io.Discard` and an empty
gzip reader. Callers obtain a stream, reset it to any `io.Writer` or
`io.Reader`, use/close it, and return the same reusable codec state.

Pinned consumers are:

* statistics storage `JSONTableToBlocks` / `BlocksToJSONTable`;
* ingest-control gRPC compressor / decompressor.

Both consumers return a reader to the pool on reset failure and otherwise
return streams after their close/read lifecycle.

## Rust comparison and decision

Rust statistics dump/load uses fresh `flate2` encoders and decoders over fixed
byte buffers for every call. It matches the compressed-data contract but does
not implement this package's reusable, generic stream pools. The pinned
ingest-control package has no Rust owner.

A fixed `Vec<u8>` pool or a new high-level `compress(bytes)` helper would be
narrower than Go and would add an API absent from the source. No such wrapper
is added. The package remains unclaimed until a reusable codec state can serve
generic reader/writer consumers and both production integration decisions are
made atomically. There is no existing false package carrier to delete.

## Validation

Read-only inventory/search commands:

    git ls-tree -r --long e2788410d8d696605e8cb002585877a063ccc909 pkg/util/compress
    git grep -n 'GzipWriterPool\|GzipReaderPool' e2788410d8d696605e8cb002585877a063ccc909 -- '*.go'
    rg -n 'GzEncoder|GzDecoder|gzip.*pool' rust/crates

No package test exists upstream and no Rust code changed for this package. The
Bazel preparation gate is not required.
