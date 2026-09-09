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

//! Return-contract parity for direct `pkg/meta` source APIs.

use tidb_meta::transaction::{
    default_resource_group_for_test, split_range_int64_max, table_info_must_load, unescape_name,
};
use tidb_meta::{key, value};

#[test]
#[deny(unused_must_use)]
fn remaining_meta_source_returns_may_be_ignored_like_go() {
    let element = tidb_meta::element::ElementKind::Column.element(1);
    element.encode();
    element.string_bytes();

    key::db_key(1);
    key::is_db_key(b"DB:1");
    key::table_key(1);
    key::is_table_key(b"Table:1");
    key::auto_table_id_key(1);
    key::is_auto_table_id_key(b"TID:1");
    key::auto_increment_id_key(1);
    key::is_auto_increment_id_key(b"IID:1");
    key::auto_random_table_id_key(1);
    key::is_auto_random_table_id_key(b"TARID:1");
    key::sequence_key(1);
    key::is_sequence_key(b"SID:1");
    key::sequence_cycle_key(1);
    key::schema_diff_key(1);
    key::policy_key(1);
    key::masking_policy_key(1);
    key::resource_group_key(1);
    key::ddl_job_id_key(1);
    key::ddl_job_history_kv_key(1);

    split_range_int64_max(1);
    table_info_must_load(b"{}");
    unescape_name(r#"a\"b"#);
    default_resource_group_for_test();

    value::which_magic_type(0);
    value::attach_magic_byte(b"{}");
}
