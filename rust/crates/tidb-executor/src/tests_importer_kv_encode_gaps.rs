// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache 2.0 license (see the License file at the crate root).

//! Gap tests for Go `pkg/executor/importer/kv_encode_test.go`: the IMPORT
//! table-KV encoder used for duplicate resolution. Go source:
//! `pkg/executor/importer/kv_encode.go` (`NewTableKVEncoderForDupResolve`,
//! `kv_encode.go:62`, built on the Lightning encode session).

/// Go `pkg/executor/importer/kv_encode_test.go:40::TestKVEncoderForDupResolve`:
/// for a `SHARD_ROW_ID_BITS=6` nonclustered table, every `Encode` of one row
/// produces a record key plus an index key; with `UseIdentityAutoRowID` the
/// row handle is the sequential input (1), and without it handles are
/// sharded so that of 10 encodes more than one exceeds 1.
#[test]
#[ignore = "go-parity-gap: NewTableKVEncoderForDupResolve (kv_encode.go:62) and the lightning KV-pair pipeline are unported"]
fn import_kv_encoder_dup_resolve_shards_or_sequences_row_ids() {}

/// Go `pkg/executor/importer/kv_encode_test.go:105::TestKVEncoderCastErrorMessage`:
/// encoding `10000000` into a `tinyint` under STRICT_ALL_TABLES fails with
/// "[Import:ErrCastValue]Value conversion failed for column 'c1'. Expected
/// type: tinyint(4), received value: 10000000. Reason: [types:1690]constant
/// 10000000 overflows tinyint".
#[test]
#[ignore = "go-parity-gap: the importer encoder's ErrCastValue wrapping is unported"]
fn import_kv_encoder_cast_error_names_column_type_value_and_reason() {}

/// Go `pkg/executor/importer/kv_encode_test.go:138::TestKVEncoderCastEnumErrorMessage`:
/// encoding `"c"` into `enum('a','b')` fails with
/// "[Import:ErrCastValue]Value conversion failed for column 'c1'. Expected
/// type: enum('a','b'), received value: \"c\". Reason: ... Data truncated".
#[test]
#[ignore = "go-parity-gap: the importer encoder's ErrCastValue wrapping is unported"]
fn import_kv_encoder_enum_cast_error_reports_truncation() {}
