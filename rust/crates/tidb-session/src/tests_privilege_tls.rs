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

//! The TLS admission policy as `CREATE USER ... REQUIRE` stores it, through a
//! session (Go `mysql.global_priv.Priv`).

#[test]
fn admission_policy_batch_tls_json_matches_go_order_omission_and_escaping() {
    use crate::{privilege::PrivilegeRegistry, Session};
    let cases: serde_json::Value =
        serde_json::from_str(include_str!("../../tidb-privilege/src/testdata/tls-policy-json-oracle.json")).unwrap();
    for (index, case) in cases.as_array().unwrap().iter().enumerate() {
        let registry = PrivilegeRegistry::default();
        let mut session = Session::new();
        session.attach_privileges(registry.clone());
        session.set_user("root@%".into(), "root@127.0.0.1".into());
        let user = format!("encoding{index}");
        session
            .run(&format!(
                "CREATE USER '{user}'@'%' REQUIRE {}",
                case["clause"].as_str().unwrap()
            ))
            .unwrap();
        // Go keeps the policy in `mysql.global_priv.Priv`, which a store
        // session reads back; the in-process tier's registry holds it.
        let stored = crate::tests_support::row_text(session.run(&format!(
            "SELECT priv FROM mysql.global_priv WHERE user = '{user}' AND host = '%'"
        )))[0][0]
            .clone();
        assert_eq!(stored, case["expected"].as_str().unwrap());
    }
}
