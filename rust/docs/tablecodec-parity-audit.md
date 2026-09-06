# pkg/tablecodec parity audit (baseline a85e0fd5df)

Full-file audit of Go `pkg/tablecodec` (tablecodec.go, rowindexcodec/
rowindexcodec.go) against `rust/crates/tidb-tablecodec` (lib.rs,
table_row.rs, table_index.rs; key framing hoisted into tidb-codec's
table_key.rs / row_index.rs).

## Fixed this batch (behavior-breaking)

`decode_index_kv`'s clustered-index V1 branch: Go decodes BOTH the unique
common-handle segment and the non-unique key suffix via
`kv.NewCommonHandle` (tablecodec.go:1968-1975); the Rust port routed the
non-unique suffix through `decode_handle_in_index_key`, which collapses a
9-byte single-int-column common handle to an IntHandle and then panics on
the `IntHandle.NumCols` mirror assert. The V1 branch now selects between
the common-handle segment and the raw suffix with `common_handle(...)`
before the general (non-V1) branches. Regression:
`test_v1_non_unique_single_int_column_common_handle` — pre-fix it panics
with "IntHandle.NumCols is unsupported" (confirmed), post-fix it decodes
the padded index column and the 42 handle.

## Verified matching (highlights)

- Prefix bytes/consts: `t`/`_r`/`_i`/`m`, idLen 8, prefixLen 11,
  RecordRowKeyLen 19, MaxOldEncodeValueLen 9, flags 127/126/125/CodecVer,
  TempIndexPrefix 0x7fff000000000000, IndexIDMask — byte-identical.
- Errnos 8221/8045/8222; Encode/DecodeRowKey (incl. PartitionHandle),
  EncodeRecordKey, table/index/meta prefixes and bounds, DecodeKeyHead/
  IndexID/TableID, API-V2 head strip, Cut* family, key ranges,
  EncodeIndexSeekKey.
- GenIndexKey distinct/null rule, TruncateIndexValues ordering,
  non-distinct suffix with GlobalIndexVersion/PartitionIDFlag/handle
  encoding; EncodeRow/EncodeOldRow error text, flatten arms, decode
  dispatch, Unflatten incl. bin-collation enum-default; index value
  layouts v0/v1 incl. padding, tailLen, untouched flags, `'0'` fallback,
  clustered PK skip; SplitIndexValue both layouts; DecodeIndexKV(Ex)
  old-collation/general/V1 paths incl. reEncodeHandle(-ConsiderNewCollation)
  and restored-data V5 re-encode; DecodeIndexHandle recursion on
  PartitionIDFlag; temp index key/value element layouts, FilterOverwritten,
  IsUntouchedIndexKValue; rowindexcodec.GetKeyKind; range-verify error
  strings (incl. the preserved "constrcuted" typo).

## Accepted narrowings / cosmetics (documented)

- Go's `DecodeValuesBytesToStrings` swallows a decode error and returns
  an empty result; Rust returns InvalidIndexKey (Go-bug substitution).
- Go error texts embed the offending key bytes/meta flag; Rust displays
  bare texts with matching errnos. "no handle in index key" text omits
  the key/value detail. DecodeMetaKey uses one 1105 text.
- API-V2 prefix stripping is keyed on the `x` byte rather than next-gen
  kernel mode (unobservable for classic keys).
- Cut*/Truncate clamp instead of panicking on short keys; temp-index
  element decode propagates corrupt-data errors Go ignores; Current()
  returns Option instead of panicking; UTC timezone conversion is not
  skipped like Go's loc==UTC shortcut; CutRowNew counts duplicate column
  IDs once.
- Rust-only additions: decode/generate_index_values_from_index (ports of
  pkg/table/tables helpers) and non-unique key/value convenience builders.

## Validation

- `cargo test -p tidb-tablecodec` (56 + 5 tests incl. the new regression),
  `cargo fmt`, `git diff --check`, `make lint`.
