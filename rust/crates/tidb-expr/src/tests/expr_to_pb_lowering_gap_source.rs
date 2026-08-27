// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

//! GO PORT record for the `pkg/expression/expr_to_pb_test.go` slice owned by
//! this batch (`TestConstant2Pb` :44 ... `TestMetadata` :2046). Every one of
//! these tests drives Go's full `PushDownExprs` + `ExpressionsToPBList`
//! pipeline and asserts EXACT TiPB JSON serializations (negative collation ids
//! for new collations, per-signature ScalarFuncSig codes, InUnion proto bytes).
//! That lowering (`pkg/expression/expr_to_pb.go`) is deliberately unported in
//! this workspace; the slices that DO exist here are policy-level admission
//! tables (`pushdown_catalog.rs`, `infer_pushdown.rs`) and the
//! integer/string-predicate TiPB shapes in `pb_predicate.rs`. Each test is
//! therefore recorded as an explicit ignored stub naming what WOULD be
//! exercised. The sibling module [`super::expr_to_pb_switcher_source`] carries
//! part9's three tail tests of the same file with the same rationale.

/// `expr_to_pb_test.go:44 TestConstant2Pb`: upstream SKIPS itself
/// (`t.Skip("constant pb has changed")`), so there is no live behavior to pin.
#[test]
#[ignore = "skipped-reason: upstream test self-skips via t.Skip(\"constant pb has changed\"); no behavior assertion survives on master"]
fn test_constant_2_pb() {}

/// `expr_to_pb_test.go:129 TestColumn2Pb`: TypeSet/TypeGeometry/
/// TypeUnspecified columns must be REFUSED from pushdown while 24 other MySQL
/// types lower to `ExprType_ColumnRef` nodes whose JSON carries each column's
/// FieldType (flen/decimal/collate id), and ID/Index zeroing keeps them pushed.
#[test]
#[ignore = "go-parity-gap: PushDownExprs + ExpressionsToPBList column-ref lowering with exact per-type FieldType serialization is unported"]
fn test_column_2_pb() {}

/// `expr_to_pb_test.go:222 TestCompareFunc2Pb`: lt/le/gt/ge/eq/ne/nulleq over
/// two BIGINT columns all push and lower to Sig codes 100-160 (nulleq setting
/// the maybe-null flag bit).
#[test]
#[ignore = "go-parity-gap: comparison signature assignment inside ExpressionsToPBList is unported; pb_predicate.rs covers only the separate int predicate builder"]
fn test_compare_func_2_pb() {}

/// `expr_to_pb_test.go:259 TestLikeFunc2Pb`: LIKE lowers to Sig 4310 with its
/// escape argument wrapped into a Sig-30 BitNeg-style escape subexpression.
#[test]
#[ignore = "go-parity-gap: LIKE's escape-subexpression PB wrapping is unported"]
fn test_like_func_2_pb() {}

/// `expr_to_pb_test.go:296 TestArithmeticalFunc2Pb`: plus/minus/mul/div/intdiv
/// mod rows over mixed signedness columns pick their typed signatures
/// (PlusInt/PlusUInt/PlusDecimal/PlusReal families) and serialize them.
#[test]
#[ignore = "go-parity-gap: arithmetic signature selection during PB lowering is unported"]
fn test_arithmetical_func_2_pb() {}

/// `expr_to_pb_test.go:345 TestDateFunc2Pb`: DATE_FORMAT(datetime_col,
/// string_col) pushes to Sig 6001.
#[test]
#[ignore = "go-parity-gap: date-function PB signature assignment is unported"]
fn test_date_func_2_pb() {}

/// `expr_to_pb_test.go:365 TestLogicalFunc2Pb`: LogicAnd/LogicOr/LogicXor/
/// UnaryNot lower through Sig 3xxx logical codes with int result types.
#[test]
#[ignore = "go-parity-gap: logical-function PB lowering is unported"]
fn test_logical_func_2_pb() {}

/// `expr_to_pb_test.go:401 TestBitwiseFunc2Pb`: AND/OR/XOR/LSH/RSH/BITNEG
/// select *UIntPermissive signatures whose codes ride the wire.
#[test]
#[ignore = "go-parity-gap: bitwise-function PB lowering is unported"]
fn test_bitwise_func_2_pb() {}

/// `expr_to_pb_test.go:439 TestControlFunc2Pb`: if/ifnull/casewhen control
/// rows push with their control-flow signature codes.
#[test]
#[ignore = "go-parity-gap: control-function PB lowering is unported"]
fn test_control_func_2_pb() {}

/// `expr_to_pb_test.go:480 TestOtherFunc2Pb`: COALESCE -> Sig 4201 and
/// IS NULL -> Sig 3116 (result flag carrying MaybeNull).
#[test]
#[ignore = "go-parity-gap: other-function PB lowering (Coalesce/IsNull codes) is unported"]
fn test_other_func_2_pb() {}

/// `expr_to_pb_test.go:509 TestJsonPushDownToFlash`: TiFlash-admitted JSON
/// functions (json_length, json_extract...) push DOWN ONLY TO FLASH -- they
/// stay unpushed toward TiKV while their Flash JSON signature list matches.
#[test]
#[ignore = "go-parity-gap: ExpressionsToPBList plus the per-store Flash-only JSON admission walk are unported; infer_pushdown.rs models only scalar-name/sign policy"]
fn test_json_push_down_to_flash() {}

/// `expr_to_pb_test.go:668 TestExprPushDownToFlash`: a wide function list
/// (LPAD, TRUNCATE, datediff arithmetics, enum/duration comparisons...)
/// lowers against the Flash client including float32/int32 specializations
/// and uint64 column flags.
#[test]
#[ignore = "go-parity-gap: Flash-targeted ExpressionsToPBList lowering (client capability checks) is unported"]
fn test_expr_push_down_to_flash() {}

/// `expr_to_pb_test.go:1488 TestExprOnlyPushDownToFlash`: functions admitted
/// for Flash but NOT TiKV still convert when the caller passes
/// kv.TiFlash while plain ExpressionsToPBList refuses them.
#[test]
#[ignore = "go-parity-gap: store-scoped conversion entry points (ExpressionsToPBList vs only-flash) are unported"]
fn test_expr_only_push_down_to_flash() {}

/// `expr_to_pb_test.go:1547 TestExprPushDownToTiKV`: the canonical TiKV list
/// -- comparisons across int/real/decimal/string/time families, like/ilike,
/// coalesce, cast trees, date arithmetic -- pushes and serializes through the
/// TiKV client with exact signature codes.
#[test]
#[ignore = "go-parity-gap: full TiKV ExpressionsToPBList lowering with signature-code serialization is unported"]
fn test_expr_push_down_to_tikv() {}

/// `expr_to_pb_test.go:1914 TestExprOnlyPushDownToTiKV`: TiKV-only admission
/// (functions absent from the Flash table) converts under kv.TiKV but is
/// refused elsewhere.
#[test]
#[ignore = "go-parity-gap: store-scoped refusal for TiKV-only functions needs the unported dual-client converter"]
fn test_expr_only_push_down_to_tikv() {}

/// `expr_to_pb_test.go:1941 TestGroupByItem2Pb`: GroupByItemToPB wraps a
/// double column reference into `{"expr": ..., "desc": false}`.
#[test]
#[ignore = "go-parity-gap: GroupByItemToPB wrapper is unported"]
fn test_group_by_item_2_pb() {}

/// `expr_to_pb_test.go:1958 TestSortByItem2Pb`: SortByItemToPB adds `"desc"`
/// true/false exactly as passed.
#[test]
#[ignore = "go-parity-gap: SortByItemToPB wrapper is unported"]
fn test_sort_by_item_2_pb() {}

/// `expr_to_pb_test.go:1981 TestPushCollationDown`: an EQ over two VARCHAR
/// columns round-trips THROUGH ExpressionsToPBList+PBToExpr keeping whatever
/// collation was set on the node (binary/latin1/utf8/utf8mb4) -- the wire-side
/// proof that collations travel in both directions.
#[test]
#[ignore = "go-parity-gap: needs BOTH directions (ExpressionsToPBList + PBToExpr), neither ported"]
fn test_push_collation_down() {}

/// `expr_to_pb_test.go:2005 TestNewCollationsEnabled`: new-collation columns
/// (including invalid charset names carried verbatim and zh_pinyin_tidb_as_cs)
/// keep pushing and serialize NEGATED collation ids on the wire.
#[test]
#[ignore = "go-parity-gap: new-collation negated-id wire encoding lives in the unported field-type-to-PB path"]
fn test_new_collations_enabled() {}

/// `expr_to_pb_test.go:2046 TestMetadata`: BuildCastFunction vs
/// BuildCastFunction4Union attach nil/false/true `InUnionMetadata` to their
/// CastStringAsInt/CastIntAsString signatures, and ExprToPB serializes that
/// metadata into Val as protobuf bytes.
#[test]
#[ignore = "go-parity-gap: BuildCastFunction(In|4Union) metadata plumbing and PbConverter.ExprToPB byte serialization are unported"]
fn test_metadata() {}
