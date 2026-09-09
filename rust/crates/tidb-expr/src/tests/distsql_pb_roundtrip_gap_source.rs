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

//! `pkg/expression/distsql_builtin_test.go` on `origin/master` ports. The
//! whole `PBToExpr` direction (`pkg/expression/distsql_builtin.go`) -- TiPB
//! wire shapes decoded back into expression trees, including collation ID
//! normalization and per-signature scalar reconstruction -- is unported here;
//! this crate only BUILDS TiPB predicates (`pb_predicate.rs`).

/// `pkg/expression/distsql_builtin_test.go:32 TestPBToExpr`: truncated codec
/// payloads (Int64/Uint64/Float64/Decimal/Duration values cut in half) must
/// error, a bare ScalarFunc wrapper with an empty ValueList child decodes, and
/// a ValueList child with a half-encoded int errors while a well-formed one
/// with an AbsInt signature but missing FieldType also errors.
#[test]
#[ignore = "go-parity-gap: PBToExpr decode-and-rebuild (distsql_builtin.go) is unported; tidb-expr has no tipb::Expr -> Expression decoder to feed truncated payloads"]
fn test_pb_to_expr() {}

/// `pkg/expression/distsql_builtin_test.go:86 TestEval`: datum-shaped tipb
/// expressions (Float32/Float64/Int64/Uint64/Bytes/String/Decimal/Duration/
/// JSON/Enum/Set/Time/VectorFloat32...) evaluate back to their datums, and
/// signature-bearing nodes (CaseWhen* returning NULL, RealIsFalse/
/// DecimalIsFalse/RealIsTrue/DecimalIsTrue over literal 1) answer 0 or 1.
#[test]
#[ignore = "go-parity-gap: tipb.Expr.Eval (distsql_builtin.go exprEval) is unported; neither the datum-decoding nor the boolean-signature reconstruction exists in this crate"]
fn test_eval() {}

/// `pkg/expression/distsql_builtin_test.go:794 TestPBToExprWithNewCollation`:
/// nine collation names round-trip through the PB FieldType Collate id with
/// new-collations OFF (positive ids; unknown names and '' normalize to
/// utf8mb4_bin) and ON (NEGATED ids on the wire), each reconstructed constant
/// carrying the expected collation name.
#[test]
#[ignore = "go-parity-gap: PBToExpr's collation-id <-> name table with negated new-collation encoding is unported"]
fn test_pb_to_expr_with_new_collation() {}

/// `pkg/expression/distsql_builtin_test.go:850 TestPBToScalarFuncExpr`: Sig
/// RegexpSig / RegexpUTF8Tipb nodes reconstruct without error.
#[test]
#[ignore = "go-parity-gap: PBToExpr's ScalarFuncSig registry lookup (including the RegexpSig family) is unported"]
fn test_pb_to_scalar_func_expr() {}
