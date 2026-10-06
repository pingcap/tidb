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

//! Source-shaped evidence for `pkg/kv/txn_scope_var.go`.

use tidb_txnkv::{TxnScopeVar, GLOBAL_TXN_SCOPE, LOCAL_TXN_SCOPE};

#[test]
fn configured_scope_selects_global_or_preserves_local_value() {
    let global = TxnScopeVar::from_configured_scope(GLOBAL_TXN_SCOPE);
    assert_eq!(global.var_value(), GLOBAL_TXN_SCOPE);
    assert_eq!(global.txn_scope(), GLOBAL_TXN_SCOPE);

    for configured in ["zone-a", LOCAL_TXN_SCOPE, "", "unknown-zone"] {
        let local = TxnScopeVar::from_configured_scope(configured);
        assert_eq!(local.var_value(), LOCAL_TXN_SCOPE);
        assert_eq!(local.txn_scope(), configured);
    }
}

#[test]
fn direct_constructors_keep_sql_and_oracle_scopes_distinct() {
    let global = TxnScopeVar::new_global();
    assert_eq!(global.var_value(), GLOBAL_TXN_SCOPE);
    assert_eq!(global.txn_scope(), GLOBAL_TXN_SCOPE);

    let local = TxnScopeVar::new_local("zone-a");
    assert_eq!(local.var_value(), LOCAL_TXN_SCOPE);
    assert_eq!(local.txn_scope(), "zone-a");
}
