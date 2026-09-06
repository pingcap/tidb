# checkColumn display-width/field-size batch (batch #39)

## Divergence

Go's planner preprocess validates every `CREATE TABLE` column definition via
`checkColumn` (`pkg/planner/core/preprocess.go:1578-1680`) and rejects bad
type parameters at DDL time with coded errors. The Rust port had only the
`pkg/ddl` `checkColumnAttributes` half (batch-history: 1427/1426-fsp/1291/3505/
vector-dim); the planner-half arms were absent, so the port ACCEPTED tables Go
rejects. Fail-before probe (kept as the regression file):

```
create table t (c bit(65))          => ACCEPTED   (Go: 1439 Display width out of range for column 'c' (max = 64))
create table t (c bit(0))           => ACCEPTED   (Go: 3013 Invalid size for column 'c'.)
create table t (c char(300))        => ACCEPTED   (Go: 1074 Column length too big for column 'c' (max = 255); use BLOB or TEXT instead)
create table t (c varchar(40000))   => ACCEPTED   (Go: 1074 ... (max = 16383) for utf8mb4)
create table t (c decimal(70,5))    => ACCEPTED   (Go: 1426 Too-big precision 70 specified for 'c'. Maximum is 65.)
create table t (c set('a,b'))       => ACCEPTED   (Go: 1367 Illegal SET 'a,b' value found during parsing)
create table t (c float(0))         => ACCEPTED   (correct: Go's Float unspecified-decimal arm passes it)
create table t (c vector(0))        => ACCEPTED   (correct: CheckVectorDimValid only rejects < 0 and > 16383)
```

## Fix

`tidb-executor/src/ddl/column_field_type.rs`: new arms in
`check_column_attributes` (which runs on the BUILT FieldType — the resolved
charset the Go DDL stage uses for its own varchar re-check, per the
"return nil, to make the check in the ddl.CreateTable" comment):

- `Bit`: flen <= 0 → 3013; flen > 64 → 1439 (MaxBitDisplayWidth)
- `String` (CHAR): declared flen > 255 → 1074 (MaxFieldCharLength)
- `Varchar`: declared flen > 65535/charset-maxlen → 1074 (utf8mb4 4,
  utf8/gbk 3, else 1 — matching `IsVarcharTooBigFieldLength`)
- `NewDecimal`: flen > 65 → 1426 (MaxDecimalWidth); scale > 30 → 1425
- `Float`/`Double`: decimal unspecified + Float flen > 24 → 1063
  (ErrWrongFieldSpec; Double's check moved to the parser in Go); decimal
  specified and (flen > 255 or flen == 0) → 1439 (MaxFloatingTypeWidth);
  scale > 30 → 1425 (MaxFloatingTypeScale). Ordering follows Go exactly:
  scale, then width, then M >= D.

`errors/driver_error.rs` + `errors/mod.rs`: seven new coded variants with the
verbatim Go templates (1439/1074/3013/1425/1097/1367/1063);
`column_types.rs`: mapping with `def.name`.

## Fail-before

`tests/check_column_display_width_source.rs` — 9 tests. The probe run before
the fix printed ACCEPTED for every now-rejected shape (recorded in this
receipt); after the fix the tests pin the exact Go error texts, plus
`valid_shapes_still_create` exercising the in-range boundary shapes
(bit(64)/bit(1)/char(255)/varchar(16383)/decimal(65,30)/float(255,30)/
set/vector(16383)/float/float(0)).

## Verification

- The 9 regressions pass; full executor suite compared against clean-HEAD
  baseline: failure-set delta is the documented sibling in-flight breakage +
  spill-dir environmental flake (individually re-verified: year_and_bit,
  multi_schema_change, range_column_partition all fail identically stashed)
- ddl/create/alter/partition filter: 362/372, the 10 failures all pre-existing
  (partition routing/access-path family, sibling reorg work)
- fmt clean; no new executor own-code clippy warnings
