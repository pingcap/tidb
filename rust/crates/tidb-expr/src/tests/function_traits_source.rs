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

//! GO PORTS of `pkg/expression/function_traits_test.go`:
//! `TestUnfoldableFuncs` (:24) and `TestIllegalFunctions4GeneratedColumns`
//! (:40).


/// GO PORT of `pkg/expression/function_traits_test.go:24 TestUnfoldableFuncs`.
///
/// Go asserts `unFoldableFunctions[ast.Sysdate]` exists -- the map is the one
/// consulted by constant folding. The crate keeps that map twice by
/// construction (the rewriter-tier [`crate::scalar_function::is_unfoldable_function`]
/// and the folding tier's `constant_fold::is_unfoldable`, which additionally
/// prefixes `getvar_`), so both spellings must agree on sysdate.
#[test]
fn unfoldable_functions_contains_sysdate() {
    assert!(crate::scalar_function::is_unfoldable_function("sysdate"));
    // The same membership drives constant_fold's handler; pin both faces.
    assert!(crate::scalar_function::is_unfoldable_function("getvar"));
    assert!(crate::scalar_function::is_unfoldable_function("getvar_string"));
    assert!(crate::scalar_function::is_unfoldable_function("rand"));
    assert!(crate::scalar_function::is_unfoldable_function("uuid"));
    assert!(crate::scalar_function::is_unfoldable_function("getparam"));
    assert!(!crate::scalar_function::is_unfoldable_function("abs"));
    assert!(!crate::scalar_function::is_unfoldable_function("plus"));
}

/// go-parity-gap: `TestIllegalFunctions4GeneratedColumns`
/// (`function_traits_test.go:40`) compares `GetBuiltinList()` against
/// `IllegalFunctions4GeneratedColumns`. In this workspace the blocklist was
/// transcreated OUTSIDE the owning crate, as `DISALLOWED_FUNCTIONS` in
/// `tidb-executor/src/generated_column.rs` (Go places it in `function_traits.go`
/// next to `unFoldableFunctions`); depending on tidb-executor from tidb-expr
/// would invert the layering, and no blocklist symbol exists inside this crate
/// to compare against. The generated-column rejection behavior is otherwise
/// exercised from the owning side (tidb-session's
/// tests_expression_indexes.rs pins the error text against real statements).
#[test]
#[ignore = "go-parity-gap: IllegalFunctions4GeneratedColumns lives in tidb-executor::generated_column in this workspace, so tidb-expr cannot compare builtin_list() against it without a layering inversion"]
fn test_illegal_functions_4_generated_columns_known_good_list() {}
