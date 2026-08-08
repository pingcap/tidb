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

//! Compile anchors for the checked-in `infer_pushdown.go` lockdown ledger.

use crate::infer_pushdown::{
    can_enum_pushdown_preliminarily, can_function_be_pushed, is_push_down_enabled,
    scalar_expr_supported_by_flash, scalar_expr_supported_by_tidb, scalar_expr_supported_by_tikv,
    store_type_mask, PushDownPolicy, PushDownStore,
};
use std::collections::HashMap;
use tidb_datatype::{FieldType, FieldTypeCode};
use tidb_proto::tipb::ScalarFuncSig;

pub(crate) const STORE_MASK: &str = "infer_pushdown_lockdown::STORE_MASK";
pub(crate) const BLACKLIST_POLICY: &str = "infer_pushdown_lockdown::BLACKLIST_POLICY";
pub(crate) const POLICY_TIKV: &str = "infer_pushdown_lockdown::POLICY_TIKV";
pub(crate) const POLICY_FLASH: &str = "infer_pushdown_lockdown::POLICY_FLASH";
pub(crate) const POLICY_TIDB: &str = "infer_pushdown_lockdown::POLICY_TIDB";
pub(crate) const ENUM_POLICY: &str = "infer_pushdown_lockdown::ENUM_POLICY";
pub(crate) const PORTED_SYMBOLS: &[&str] = &[
    STORE_MASK,
    BLACKLIST_POLICY,
    POLICY_TIKV,
    POLICY_FLASH,
    POLICY_TIDB,
    ENUM_POLICY,
];

#[allow(dead_code)]
fn compile_anchors() {
    let policy = PushDownPolicy::new("plus", ScalarFuncSig::Unspecified);
    let blacklist = HashMap::new();
    let field_type = FieldType::new(FieldTypeCode::LongLong);
    let _ = store_type_mask(PushDownStore::TiKv);
    let _ = is_push_down_enabled(&blacklist, "plus", PushDownStore::TiKv);
    let _ = can_function_be_pushed(&policy, PushDownStore::TiKv, &blacklist);
    let _ = scalar_expr_supported_by_tikv(&policy);
    let _ = scalar_expr_supported_by_flash(&policy);
    let _ = scalar_expr_supported_by_tidb(&policy);
    let _ = can_enum_pushdown_preliminarily("cast", &field_type);
}

#[test]
fn every_ported_symbol_has_a_compile_anchor() {
    compile_anchors();
    assert_eq!(PORTED_SYMBOLS.len(), 6);
}
