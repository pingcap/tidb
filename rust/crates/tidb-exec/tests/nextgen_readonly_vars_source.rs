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

use tidb_exec::nextgen_readonly_vars::is_read_only_var_in_nextgen;

#[test]
fn nextgen_readonly_variable_predicate_matches_source() {
    for name in [
        "tidb_enable_metadata_lock",
        "TIDB_ENABLE_METADATA_LOCK",
        "TiDb_DdL_DiSk_QuOtA",
        "tidb_enable_metadata_loc\u{212a}",
        "tidb_max_dist_task_nodes",
        "tidb_ddl_reorg_max_write_speed",
        "tidb_ddl_disk_quota",
        "tidb_ddl_enable_fast_reorg",
        "tidb_enable_dist_task",
    ] {
        assert!(is_read_only_var_in_nextgen(name), "{name}");
    }
    for name in [
        "",
        "abc",
        "tidb_enable_metadata_lock_suffix",
        " tidb_enable_metadata_lock",
        "TIDB_ENABLE_METADATA_LOC\u{130}",
    ] {
        assert!(!is_read_only_var_in_nextgen(name), "{name}");
    }
}

#[test]
fn unicode_lowering_happens_before_exact_membership() {
    assert!(is_read_only_var_in_nextgen(
        "tidb_enable_metadata_loc\u{212a}"
    ));
    assert!(!is_read_only_var_in_nextgen(
        "tidb_enable_metadata_loc\u{130}"
    ));
    assert!(!is_read_only_var_in_nextgen(
        "tidb_enable_metadata_lock_suffix"
    ));
}
