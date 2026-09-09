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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! GO PORTS of `pkg/expression/expr_to_pb_test.go` items 481-483 of the
//! part9 slice: `TestPushDownSwitcher` (:2087),
//! `TestPanicIfPbCodeUnspecified` (:2183) and `TestProjectionColumn2Pb`
//! (:2204). All three drive Go's full `ExpressionsToPBList` /
//! `PbConverter.ExprToPB` pipeline, which is deliberately unported in this
//! workspace; each is recorded here as an `#[ignore]` stub with its
//! go-parity-gap reason and an anchor naming what WOULD be exercised.

/// go-parity-gap: TestPushDownSwitcher (`expr_to_pb_test.go:2087`) needs
/// `ExpressionsToPBList`/`PbConverter.ExprToPB` lowering (TiPB `Expr`
/// serialization of sin/cos/tan scalar functions plus float constants), and it
/// gates pushability through the `PushDownTestSwitcher` failpoint with values
/// "all"/""/"sin,tan". This crate lowers only the integer/string predicate
/// subset (`pb_predicate.rs`) and expresses Flash/TiKV admission as name/sign
/// policy functions over [`crate::infer_pushdown::PushDownPolicy`] without a
/// failpoint channel; neither halves can reproduce the switcher contract yet.
///
/// Fragments that DO exist as separate carriers:
/// - TiFlash rejection of non-default FTS modifiers and other signature
///   boundaries are pinned by infer_pushdown's
///   `infer_pushdown_flash_cast_and_fts_boundaries_match_go`, and
/// - the -0.0 ATAN2 guard lives inside Go's float-constant PB lowering
///   (expr_to_pb.go ~line 142), which stays unported together with this stub.
#[test]
#[ignore = "go-parity-gap: ExpressionsToPBList/PbConverter lowering and the PushDownTestSwitcher failpoint are unported; only policy-level admission (infer_pushdown.rs) exists"]
fn test_push_down_switcher() {}

/// go-parity-gap: TestPanicIfPbCodeUnspecified (`expr_to_pb_test.go:2183`)
/// drives `setPbCode(Unspecified)` + `PanicIfPbCodeUnspecified` failpoint +
/// the panic raised while converting a bitwise `and` node whose signature has
/// no PB code. The crate carries no per-signature pbCode field and no failpoint
/// channel; tidb-proto's ScalarFuncSig enum values exist but nothing reads a
/// code off a built signature.
#[test]
#[ignore = "go-parity-gap: signatures carry no pbCode state in tidb-expr and the PanicIfPbCodeUnspecified failpoint is not modeled"]
fn test_panic_if_pb_code_unspecified() {}

/// go-parity-gap: TestProjectionColumn2Pb (`expr_to_pb_test.go:2204`) requires
/// `ExpressionsToPBList` to reject a TypeSet column while
/// `ProjectionExpressionsToPBList` tolerates every column type for projection
/// pushdown. Neither entry point is transcreated; the crate's TiPB layer has no
/// column-type admission table (`pb_predicate.rs` owns only predicate shapes).
#[test]
#[ignore = "go-parity-gap: ExpressionsToPBList and ProjectionExpressionsToPBList are unported, so the TypeSet-rejected/Projection-accepted split cannot be pinned"]
fn test_projection_column_2_pb() {}
