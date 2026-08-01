# Catalog model JSON parity: `pkg/meta/model` vs `tidb-model` / `tidb-metadef` / `tidb-meta::value`

A field-by-field comparison of the structs TiDB persists into TiKV as shared
cluster state, against their Rust ports. This is the surface where a wrong
`json` tag is written once and misread forever, by every node.

**Nothing in this audit was executed.** The machine cannot run a freshly built
binary (`syspolicyd` is wedged; every new executable hangs at `_dyld_start`).
`cargo check` and `cargo clippy` were run and are clean. Every claim below is
read off the Go source and the Rust source side by side, with file:line for
both. The "unverified" section at the end lists exactly what an execution
would have to settle.

Go tree read: `pkg/meta/model/{table,column,index,db,job,reorg,table_mode,
resource_group,masking_policy,engine_attribute}.go`, `pkg/parser/ast/model.go`,
`pkg/parser/types/field_type.go`, `pkg/meta/meta.go`, `pkg/meta/metadef/*`.

---

## 0. The round-trip answer, first

**We do not drop any field that a same-version Go node knows about.**

Every `json` tag on `TableInfo`, `ColumnInfo`, `IndexInfo`, `IndexColumn`,
`DBInfo`, `PartitionInfo`, `PartitionDefinition`, `SequenceInfo`, `ViewInfo`,
`TTLInfo`, `TiFlashReplicaInfo`, `ConstraintInfo`, `FKInfo`, `TableLockInfo`
and `SchemaDiff` exists on the Rust side under the identical name, with the
identical `omitempty` behaviour. There is no field Go writes that we silently
discard, and no field we invent. The tag inventory is in section 2.

**Unknown keys written by a NEWER Go are dropped on a round trip — and that is
exactly what Go itself does.** `encoding/json` silently ignores keys with no
matching struct field on `Unmarshal`, and `Marshal` writes only the struct's
own fields; an older Go node doing `ADD INDEX` on a table written by a newer
Go node destroys the new fields in precisely the same way. So this is not a
Rust-specific hazard and not damage we introduce — it is TiDB's existing
upgrade-ordering contract. There is no `#[serde(deny_unknown_fields)]` anywhere
in `tidb-model` or `tidb-meta`, so an unknown key is not a hard failure either.

The read-modify-write paths are live: `rust/crates/tidb-exec/src/cluster_ddl.rs`
lines 632, 743 and 777 decode a stored `TableInfo`, mutate it and write it back
through `value::serialize_table_info`. Round-trip fidelity matters in
production here, not only in tests.

**Where we DO lose information a same-version Go would have kept, it is a value
collapse inside a known field, never a dropped field.** Three such collapses
exist: findings F1, F2 and F3.

Byte-identity is already asserted against Go-produced JSON in
`rust/crates/tidb-meta/tests/go_vectors.rs:326-375` — `GO_DBINFO`,
`GO_TABLEINFO`, `GO_TABLEINFO_FULL` plus the two zero-value forms all parse and
re-serialise to the same bytes. `GO_TABLEINFO_FULL` populates every nested
struct: partition, view, sequence, lock, TiFlash replica, constraints, foreign
keys, TTL, placement, exchange-partition, softdelete, affinity, split policy,
storage class and table mode. Those tests are unrun here, but they are the
right shape.

---

## 1. Ranked divergences (8 findings)

### F1 — RANK 1. `CiString.L` is computed with a different Unicode case mapping

- Go: `pkg/parser/ast/model.go:269-273` (`CIStr{O,L}`), `:300-304`
  (`NewCIStr` = `strings.ToLower`).
- Rust: `rust/crates/tidb-ast/src/model.rs:34` (`original.to_lowercase()`).

`strings.ToLower` applies Unicode **simple** case mapping (`unicode.ToLower`
per rune, from `UnicodeData.txt`). Rust's `str::to_lowercase` applies Unicode
**full** case mapping, which includes the `SpecialCasing.txt` multi-character
expansions and the context-sensitive final-sigma rule. They disagree on real
identifiers.

Distinguishing case A — final sigma:

```sql
CREATE TABLE ΟΔΟΣ (a INT);
```

| writer | stored `name` in `TableInfo` |
| --- | --- |
| Go | `{"O":"ΟΔΟΣ","L":"οδοσ"}`  (σ, U+03C3) |
| Rust | `{"O":"ΟΔΟΣ","L":"οδος"}`  (ς, U+03C2) |

Distinguishing case B — `İ` U+0130:

| writer | stored `name` |
| --- | --- |
| Go | `{"O":"İ","L":"i"}` |
| Rust | `{"O":"İ","L":"i̇"}` (`i` + U+0307 COMBINING DOT ABOVE) |

Consequence: catalog lookup is by `Name.L` on both sides. A table created by
one node is **invisible** to the other — `SELECT * FROM ΟΔΟΣ` on a Go node
returns `ERROR 1146 Table doesn't exist` for a table the Rust node created and
can see, and `CREATE TABLE ΟΔΟΣ` succeeds on both nodes, producing two table
objects with the same `O`. The same applies to `DBInfo.Name`,
`ColumnInfo.Name`, `IndexInfo.Name`/`Table` and `PartitionDefinition.Name`.

Not fixed here: `CiString` lives in `tidb-ast`, and the fix — a
simple-case-mapping lowering that mirrors `unicode.ToLower` — is a new code
path with its own table, not a one-line correction. It also affects every other
`to_lowercase()` identifier comparison in the tree, so it wants a single owner.

### F2 — RANK 2. Unknown `index_type` / partition `type` ordinals collapse to 0 on a round trip

- Rust: `rust/crates/tidb-model/src/index.rs:123-135`
  (`index_type_from_i64`, `_ => IndexType::Invalid`);
  `rust/crates/tidb-model/src/partition.rs:53-62`
  (`from_i64`, `_ => PartitionType::None`).
- Go: `pkg/parser/ast/model.go:195-236` — `IndexType` is a plain `int`; any
  value survives `Unmarshal` and `Marshal` untouched. The declaration carries
  an explicit warning at `:221-222`:
  *"May come from a previous version persisted in TableInfo. So you must keep
  it compatible when modifying it."*

Distinguishing document — a stored `TableInfo` containing:

```json
"index_info":[{"id":1,"idx_name":{"O":"i","L":"i"},"index_type":9, ...}]
```

Go re-marshals `"index_type":9`. Rust decodes it to `Invalid` and re-marshals
`"index_type":0`. After one Rust `ADD INDEX` on that table, the stored index
type is permanently 0 for every node in the cluster: `SHOW CREATE TABLE` on a
Go node then omits the `USING <TYPE>` clause, and the planner's index-type
dispatch takes the b-tree path for what was a columnar index.

The partition variant is worse in kind: `PartitionType` 0 is
`PartitionTypeNone`, "not partitioned". A future partition type would be
rewritten to "no partitioning" while `definitions` and `num` stay populated.

Not fixed here: the correct shape is the one this crate already uses everywhere
else — a numeric newtype (`SchemaState(u8)`, `ActionType`, `BackfillState(u8)`,
`ColumnarIndexType(u8)` all preserve unknown values). Converting `IndexType`
and `PartitionType` to that shape means changing types owned by `tidb-ast` and
touching every match site, which is well beyond "small and certain".

### F3 — RANK 2. Five AST enums hard-fail on an unknown ordinal, aborting the whole `TableInfo` decode

- Rust: `rust/crates/tidb-model/src/table.rs:38-64` (`int_enum_serde!`,
  `other => Err(serde::de::Error::custom(...))`), instantiated at `:66`
  (`ViewAlgorithm`), `:71` (`ViewSecurity`), `:75` (`ViewCheckOption`),
  `:79` (`ColumnChoice`), `:85` (`TableLockType`).
- Go: all five are plain `int`/`int8` (`pkg/parser/ast/model.go:28-45, 70-75,
  95-99, 117-121, 400-406`) and accept any value.

Distinguishing document — a `TableInfo` whose `stats_options` carries a
`column_choice` a future TiDB adds:

```json
"stats_options":{"auto_recalc":true,"column_choice":4,"column_list":null,
                 "sample_num":0,"sample_rate":0,"buckets":0,"topn":0,"concurrency":0}
```

Go decodes it fine. `value::parse_table_info` returns
`MetaError::InvalidJson("invalid ColumnChoice value: 4")` and the Rust node
cannot load that table **at all** — not just the stats options, the table. The
same holds for `"view":{"view_algorithm":3,...}` and `"Lock":{"Tp":6,...}`.

Loud rather than silent, which makes it strictly better than F2, but the blast
radius is larger: one unknown ordinal in one sub-struct takes out the whole
table. Ranked below F1 because it needs a future TiDB version to be reachable.

### F4 — RANK 3. `model.Job` is not ported; a Rust DDL never enters the job queue

- Go: `pkg/meta/model/job.go:353` (`type Job struct`), persisted to
  `mDDLJobList` / `mDDLJobHistory`.
- Rust: `rust/crates/tidb-model/src/job.rs` contains only the satellite types
  (`AdminCommandOperator:29`, `InvolvingSchemaInfo:59`, `JobPauseReason:74`,
  `JobResumeReason:83`, `HistoryInfo:90`) and **none of them derive serde**.
  There is no `Job` type and no writer for the job-queue keys.
  `rust/crates/tidb-exec/src/cluster_ddl.rs:632/743/777` writes the mutated
  `TableInfo` and a `SchemaDiff` straight into meta.

Consequence on a mixed cluster: a Rust node's `CREATE TABLE` / `ADD INDEX`
bumps the schema version and updates the catalog, so Go nodes do reload and see
the table — but `ADMIN SHOW DDL JOBS` on any Go node never lists it, and
everything that reconstructs DDL history from the job queue (BR incremental
restore, TiCDC DDL replication) sees a schema version advance with no
corresponding DDL. Downstream diverges silently.

### F5 — RANK 3. Resource groups and masking policies have no serde at all, and the obvious fix is booby-trapped

- Rust: `rust/crates/tidb-model/src/resource_group.rs:33, 52, 61, 211` and
  `rust/crates/tidb-model/src/masking_policy.rs:25, 68` derive only
  `Clone, Debug, Default, PartialEq, Eq`.
- Go persists both behind the magic byte: `pkg/meta/meta.go:660, 678, 725, 742`.

So a Rust node can neither read nor write `mResourceGroups` /
`mMaskingPolicies`. That is an absent capability, not corruption.

The trap: three Go tags do **not** match the Rust field names.

| Go field | Go tag (`pkg/meta/model/resource_group.go`) | Rust field |
| --- | --- | --- |
| `RURate` | `ru_per_sec` (`:47`) | `ru_rate` (`:64`) |
| `CPULimiter` | `cpu_limit` (`:49`) | `cpu_limiter` (`:68`) |
| `ResourceUtilLimit` | `utilization_limit` (`:42`) | `resource_util_limit` (`:57`) |

Adding a bare `#[derive(Serialize, Deserialize)]` — the natural next step —
would emit `{"ru_rate":2000,...}`, which Go reads as `RURate == 0`: a resource
group with a zero RU quota. Explicit `#[serde(rename)]` is mandatory on all
three. Flagging this before anyone writes that derive is the point of the
finding.

### F6 — RANK 3. `ColumnDefaultValue` cannot represent a JSON array or object default

- Go: `pkg/meta/model/column.go:89, 91` — `OriginDefaultValue any`,
  `DefaultValue any`.
- Rust: `rust/crates/tidb-model/src/column.rs:216-228` — a five-variant enum
  (`Int/Uint/Float/Bool/Str`); the visitor at `:255-285` implements
  `visit_bool/i64/u64/f64/str` only. serde's default `visit_map`/`visit_seq`
  return an invalid-type error, so `"default": {"a":1}` or `"default": [1,2]`
  fails the entire `TableInfo` decode.

I could not find a TiDB writer that stores a non-scalar there — DDL routes
defaults through `Datum.ToString()`, and the BIT case goes to the separate
`*_bit` byte fields — so this is ranked low and the failure would be loud.
Listed because `any` is unbounded and a future writer would not announce
itself.

### F7 — RANK 4. Numeric defaults re-serialise *more* precisely in Rust than in Go

Go unmarshals a JSON number into `interface{}` as `float64`. A `TableInfo`
containing `"default": 9223372036854775807` re-marshals from a **Go** node as
`9223372036854776000`; Rust keeps `Int(i64)` and re-marshals the exact value.

Rust is the more correct side, but the two produce different bytes for the same
input, so a byte-for-byte cross-check of this field between a Go node and a
Rust node is not a valid equality test. Worth knowing before someone writes
that test and "fixes" the Rust side to match.

### F8 — RANK 4. `build_storage_class_string` uses the plain serde_json encoder

`rust/crates/tidb-model/src/engine_attribute.rs:127` calls
`serde_json::to_string`, not `serde_helpers::to_go_json`. Go's counterpart
(`pkg/meta/model/engine_attribute.go:78-93`) uses `json.Marshal`, which
HTML-escapes `<`, `>`, `&`. The output is a tier name plus integers, so no
escapable byte can appear today and the strings agree. Latent only: a tier name
containing `&` would produce a persisted string that differs from Go's.

The sibling `EngineAttribute.storage_class` `skip_serializing_if`
(`engine_attribute.rs:28`, versus Go's un-`omitempty` `json:"storage_class"` at
`engine_attribute.go:23`) is **not** a divergence: Go only ever `Unmarshal`s
`EngineAttribute` — the single `json.Marshal` in that file is of a different,
local struct — so the encoded form of `EngineAttribute` is never observed.

---

## 2. Verified equal — where not to look

This list is the other half of the deliverable. Each entry was compared tag by
tag or ordinal by ordinal.

### Struct tag inventories (name, order and `omitempty` all match)

| Struct | Go | Rust | Serialised fields |
| --- | --- | --- | --- |
| `TableInfo` | `table.go:113` | `table_info.rs:69` | 47 |
| `ColumnInfo` | `column.go:86` | `column.rs:314` | 18 |
| `IndexInfo` | `index.go:260` | `index.rs:281` | 20 |
| `IndexColumn` | `index.go` | `index.rs` | 4 |
| `DBInfo` | `db.go:24` | `db.rs:34` | 7 |
| `PartitionInfo` | `table.go:840` | `partition.rs:188` | 20 |
| `PartitionDefinition` | `table.go` | `partition.rs` | 8 |
| `SequenceInfo` | `table.go:812` | `table.rs:696` | 8 |
| `ViewInfo` / `TTLInfo` / `TiFlashReplicaInfo` / `ConstraintInfo` / `FKInfo` / `TableLockInfo` | `table.go` | `table.rs` | 6 / 5 / 4 / 8 / 10 / 4 |
| `SchemaDiff` / `AffectedOption` | `job.go` | `schema_diff.rs:51 / :30` | 9 / 4 |

Specifically confirmed on `TableInfo`, because these are the ones asked about:
`pk_is_handle`, `is_common_handle`, `common_handle_version`, `auto_inc_id`,
`auto_id_cache`, `auto_rand_id`, `auto_random_bits`, `auto_random_range_bits`,
`max_shard_row_id_bits`, `tiflash_replica`, `partition`, `sequence`, `view`,
`temp_table_type`, `cache_table_status`, `ttl_info`, `revision`,
`update_timestamp`, `version` — all present, all identically tagged, none
carrying `omitempty` on either side.

Five Go quirks are reproduced correctly and are worth naming, because getting
any of them wrong would have been invisible:

- `ShardRowIDBits` has **no** `json` tag in Go (`table.go:162`), so
  `encoding/json` writes the Go field name. Rust uses
  `#[serde(rename = "ShardRowIDBits")]` (`table_info.rs:173`). Correct.
- `Lock *TableLockInfo` is tagged `json:"Lock"` with a capital L
  (`table.go:186`). Rust: `#[serde(rename = "Lock")]` (`table_info.rs:211`).
- `DBInfo.Deprecated` is an anonymous struct whose only field is `json:"-"`
  (`db.go:28-30`), so every stored `DBInfo` carries a constant
  `"Deprecated":{}`. Rust emits it from a hand-written `Serialize`
  (`db.rs:71-91`) — including that empty object, which a derive would not
  produce.
- `TempTableType` and `TableCacheStatusType` are *tagged embedded* fields
  (`table.go:196-197`), which makes them plain named fields rather than
  inlined; likewise `ColumnInfo`'s `*ChangeStateInfo` (`column.go:105`) →
  `"change_state_info": {...}` or `null`, not inlined. Rust models all three as
  named `Option`/newtype fields (`table_info.rs:222-227`, `column.rs:411`).
- `TableInfo.DBID` is `json:"-"` (`table.go:225`) and Go re-populates it after
  decoding. Rust: `#[serde(skip)]` (`table_info.rs:267`) plus the same fixup in
  `value::parse_table_info` (`tidb-meta/src/value.rs:91-96`).

`SchemaDiff.IsRefreshMeta` is `json:"-"` in Go; Rust `#[serde(skip)]`
(`schema_diff.rs:92`). Its two `omitempty` fields are handled by a hand-written
`Serialize` (`schema_diff.rs:96-127`) that skips them exactly when Go does, and
still writes `"affected_options":null` for the nil slice.

### `FieldType` (the `ColumnInfo."type"` sub-document)

Go marshals through a private `jsonFieldType`
(`pkg/parser/types/field_type.go:729-772`) with capitalised, untagged field
names. Rust reproduces the same nine keys in the same order
(`tidb-datatype/src/field_type/mod.rs:1360-1380`): `Tp`, `Flag`, `Flen`,
`Decimal`, `Charset`, `Collate`, `Elems`, `ElemsIsBinaryLit`, `Array`. `Elems`
and `ElemsIsBinaryLit` are `Option<Vec<..>>` with explicit presence tracking, so
Go's nil-slice-`null` versus empty-slice-`[]` distinction survives a round trip.

### Enum ordinals — all verified identical

| Enum | Values | Go | Rust |
| --- | --- | --- | --- |
| `SchemaState` | None 0, DeleteOnly 1, WriteOnly 2, WriteReorg 3, DeleteReorg 4, Public 5, ReplicaOnly 6, GlobalTxnOnly 7 | `job.go:269-292` | `schema_state.rs:26-47` |
| `TempTableType` | None 0, Global 1, Local 2 | `table.go:700-707` | `table.rs:810-820` |
| `TableCacheStatusType` | Disable 0, Enable 1, Switching 2 | `table.go:676-683` | `table.rs:784-794` |
| `TableMode` | Normal 0, Import 1, Restore 2 | `table_mode.go:26-35` | `table_mode.rs:28-36` |
| `BackfillState` | Inapplicable 0, Running 1, ReadyToMerge 2, Merging 3 | `reorg.go:28-45` | `reorg.rs:27-39` |
| `ReorgStage` / `ReorgType` | 0-3 / 0-2 | `reorg.go` | `reorg.rs:61-81` |
| `ColumnarIndexType` | NA 0, Inverted 1, Vector 2, Fulltext 3 | `index.go` | `index.rs:168-178` |
| `IndexType` | Invalid 0, Btree 1, Hash 2, Rtree 3, Hypo 4, Vector 5, Inverted 6, HNSW 7, Fulltext 8 | `ast/model.go:223-236` | `index.rs:107-119` |
| `PartitionType` | None 0, Range 1, Hash 2, List 3, Key 4, SystemTime 5 | `ast/model.go:136-149` | `partition.rs:40-49` |
| `ViewAlgorithm` / `ViewSecurity` / `ViewCheckOption` | 0-2 / 0-1 / 0-1 | `ast/model.go:70-121` | `table.rs:66-78` |
| `ColumnChoice` | Default 0, All 1, Predicate 2, List 3 | `ast/model.go:400-406` | `table.rs:79-84` |
| `TableLockType` | None 0, Read 1, ReadLocal 2, ReadOnly 3, Write 4, WriteLocal 5 | `ast/model.go:28-45` | `table.rs:85-92` |
| `ActionType` | all 82 live values within 0..=84 | `job.go:39-124` | `action_type.rs` |

`ActionType` deserves its own line, since it is the largest and the easiest to
get wrong: both sides declare exactly the same 82 live constants, both carry
the two deprecated values 46 (`_DEPRECATEDActionAlterTableAlterPartition`,
`job.go:88`) and 48 (`_DEPRECATEDActionDropIndexes`, `job.go:91`), and both
leave 66 unassigned. No off-by-one anywhere. This matters beyond the DDL job:
`PartitionInfo.DDLAction` (`ddl_action,omitempty`) persists an `ActionType`
inside `TableInfo`.

`ViewCheckOption` is a near-miss worth recording: Go's zero value is
`CheckOptionLocal`, but the Rust `tidb-ast` enum's `Default` is `Cascaded`. The
port supplies `view_check_option_zero()` in `table.rs` as the serde default so
a missing key still decodes to Go's zero. Correct — but anyone removing that
function reintroduces a rank-1 bug.

### Version constants

| Constant | Go | Rust |
| --- | --- | --- |
| `TableInfoVersion0..5`, `CurrLatest = 5` | `table.go:53-86` | `table_info.rs:51-61` |
| `ColumnInfoVersion0..2`, `CurrLatest = 2` | `column.go:28-41` | `column.rs:42-49` |
| `GlobalIndexVersion` legacy 0 / v1 1 / v2 2 | `index.go:280-286` | `index.rs:48-52` |
| `FKVersion0/1` | `table.go` | `table.rs` |

`CommonHandleVersion` has **no** named constants on either side — it is a plain
`uint16`/`u16` documented in place (`table.go:129-131`, `table_info.rs:130-132`:
0 for a clustered index created at 5.0.0-RC, 1 after). Nothing can diverge.

On a version it does not expect, Go does nothing structural: `TableInfoVersion`
is consulted only for charset normalisation on load, and an unknown high
version is treated as "at least version 3", so a forward-written table degrades
gracefully rather than failing. Rust stores the raw `u16` and re-emits it
unchanged, so it participates in that contract correctly rather than clamping.

### Encoder fidelity (`serde_helpers::to_go_json`)

`rust/crates/tidb-model/src/serde_helpers.rs` reproduces four `encoding/json`
behaviours that `serde_json` gets differently, all load-bearing for
byte-identical catalog writes:

- HTML escaping of `<`, `>`, `&` and U+2028/U+2029 (`:73-99`). Not academic: a
  CHECK constraint's `expr_string`, a generated column's expression and a
  partition expression routinely contain `>`.
- Go's float format (`:30-58`): an integral float prints as `0`/`1`, and
  exponent form is used only outside `[1e-6, 1e21)`, with Go's unpadded signed
  exponent (`1e-7`, not `1e-07`).
- Nil slice → `null`, not `[]` (`:111-124`), applied to `cols`, `index_info`,
  `constraint_info`, `fk_info`, `idx_cols`, `affected_options` and friends.
- Go's *string*-sorted integer map keys (`:155-181`), so `{2, 10}` emits `10`
  before `2`.

`ColumnInfo`'s two byte fields use padded standard-alphabet base64 with
nil → `null` (`column.rs:59-121`), matching `encoding/json`'s `[]byte` rule, and
`Dependences` (`map[string]struct{}`) becomes an object of empty objects with
nil → `null` (`column.rs:130-155`). Both are hand-written because serde's
defaults (`[1,2,3]` and `["a","b"]`) would have been wrong and silent.

All production writes go through `to_go_json`: `value.rs:71-76` is the only
encoder used by `serialize_table_info` / `serialize_db_info` /
`serialize_schema_diff` / `serialize_policy_info`. Every direct
`serde_json::to_string` in `tidb-model` is inside `#[cfg(test)]`, except the
`build_storage_class_string` case noted as F8.

### `tidb-meta::value` and the magic byte

`value.rs:57-65` reproduces `meta.detachMagicByte` including `whichMagicType`'s
`<= 0x3F` JSON band (`pkg/meta/meta.go:1713-1728`), and
`CurrentMagicByteVer = 0x00`. Correctly applied to policies (magic byte) and
correctly **not** applied to tables, databases and schema diffs, matching
`pkg/meta/meta.go:815, 1129` where `json.Marshal(tableInfo)` is stored raw.

### `tidb-metadef`

Not a JSON surface — it is identifiers and system-table DDL text. Spot-checked
equal: 53 `CREATE TABLE IF NOT EXISTS` statements on each side
(`pkg/meta/metadef/system_tables_def.go` vs
`rust/crates/tidb-metadef/src/system_tables_def.rs`), and
`ReservedGlobalIDUpperBound = 0x0000FFFFFFFFFFFF` (`system.go:21` /
`system.rs:21`). The `INFORMATION_SCHEMA` / `PERFORMANCE_SCHEMA` /
`METRICS_SCHEMA` name-and-lowercase pairs match. A statement-by-statement diff
of the DDL text was out of scope here and belongs with whoever owns bootstrap.

---

## 3. What is unverified because nothing can execute here

- All Rust tests, including the byte-identity fixtures in
  `crates/tidb-meta/tests/go_vectors.rs`. They were read and look correct; they
  are unrun.
- The F1 case mappings are asserted from the Unicode data files' semantics
  (`UnicodeData.txt` simple mapping versus `SpecialCasing.txt` full mapping and
  the `Final_Sigma` context), not from running `strings.ToLower` and
  `str::to_lowercase` on `ΟΔΟΣ` and `İ`. That comparison is the cheapest thing
  to run once execution works, and it should be run before F1 is acted on.
- Whether any real TiDB writer ever puts a non-scalar into
  `ColumnInfo.OriginDefaultValue` (F6). The DDL paths were searched and none
  was found; a sweep over a real cluster's stored table infos would settle it.
- Nothing was checked against a live mixed cluster. F1 and F4 in particular
  predict observable cross-node behaviour (`ERROR 1146` for a Greek table name;
  a missing row in `ADMIN SHOW DDL JOBS`) and those predictions are unconfirmed.

## 4. Changes made

None to code. Every divergence found is either larger than a serde attribute
(F1, F2, F3, F4, F5) or unobservable today (F6, F7, F8), so nothing met the
"small and certain" bar. `cargo check` and `cargo clippy` were run on the tip
and are clean.
