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

//! Source-backed tests for the bootstrap system-database filter.

use tidb_exec::system_db_filter::{is_system_db, skip_load_diff, skip_load_schema};

#[test]
fn system_db_filter_preserves_mysql_only_schema_selection() {
    // Source: pkg/session/global_init.go:29-40 and
    // pkg/meta/metadef/db_test.go:37-40 (TestIsSystemDB).
    assert!(is_system_db("mysql"));
    assert!(!is_system_db("sys"));
    assert!(!is_system_db("MYSQL"));

    assert!(!skip_load_schema("mysql"));
    assert!(skip_load_schema("sys"));
    assert!(skip_load_schema("information_schema"));
    assert!(skip_load_schema("MYSQL"));
    assert!(!skip_load_diff());
}
