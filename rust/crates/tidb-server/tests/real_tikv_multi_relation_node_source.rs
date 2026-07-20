// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Source-derived guard for the two-relation server authority boundary.

use tidb_server::{NodeConfig, RealTiKvMultiSessionFactory};

#[test]
fn multi_relation_factory_rejects_non_pair_before_pd_side_effects() {
    let config = NodeConfig::parse([
        "tidb-server",
        "--path",
        "127.0.0.1:2379",
        "--read-table",
        "campaign25",
        "orders",
        "42",
        "2",
        "id:1:clustered-pk",
        "account_id:2:stored-not-null",
        "--auth-file",
        "/tmp/campaign25-users.tsv",
    ])
    .expect("single-table configuration itself is valid");

    let error = RealTiKvMultiSessionFactory::connect(&config)
        .err()
        .expect("the multi-relation authority must reject before PD/TiKV startup");
    assert_eq!(
        error.message,
        "multi-relation dispatcher requires exactly two configured tables"
    );
}
