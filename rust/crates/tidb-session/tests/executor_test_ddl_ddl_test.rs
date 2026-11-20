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

//! Go `pkg/executor/test/ddl/ddl_test.go`.

use std::sync::atomic::Ordering;

use crate::support::testkit::*;

/// Go `units.PiB`.
const PIB: i64 = 1 << 50;

/// Go `TestSetDDLReorgMaxWriteSpeed`.
#[test]
fn test_set_ddl_reorg_max_write_speed() {
    if tidb_config_kerneltype::is_next_gen() {
        // Setting tidb_ddl_reorg_max_write_speed is not supported in the next generation of TiDB
        return;
    }
    let mut tk = TestKit::new();
    assert_eq!(
        tidb_vardef::defaults::DEF_TIDB_DDL_REORG_MAX_WRITE_SPEED,
        tidb_vardef::DDL_REORG_MAX_WRITE_SPEED.load(Ordering::SeqCst)
    );

    // valid values
    for val in [1_i64, 0, 100, 1024 * 1024, 2_147_483_647, PIB] {
        tk.must_exec(&format!("set @@global.tidb_ddl_reorg_max_write_speed = {val}"));
        assert_eq!(val, tidb_vardef::DDL_REORG_MAX_WRITE_SPEED.load(Ordering::SeqCst));
        tk.must_query("select @@global.tidb_ddl_reorg_max_write_speed").check([val.to_string()]);
    }
    // Go computes each expectation with `units.RAMInBytes`.
    for (val, expected) in [
        ("1", 1_i64),
        ("0", 0),
        ("100", 100),
        ("2KB", 2 * 1024),
        ("3MiB", 3 * 1024 * 1024),
        ("4 gb", 4 * 1024 * 1024 * 1024),
        ("2147483647", 2_147_483_647),
        ("1125899906842624" /* 1PiB */, PIB),
    ] {
        tk.must_exec(&format!("set @@global.tidb_ddl_reorg_max_write_speed = '{val}'"));
        assert_eq!(expected, tidb_vardef::DDL_REORG_MAX_WRITE_SPEED.load(Ordering::SeqCst));
        tk.must_query("select @@global.tidb_ddl_reorg_max_write_speed").check([expected.to_string()]);
    }

    // invalid values
    tk.must_exec_to_err("set @@global.tidb_ddl_reorg_max_write_speed = -1");
    tk.must_exec_to_err("set @@global.tidb_ddl_reorg_max_write_speed = invalid_val");
    tk.must_exec_to_err(&format!("set @@global.tidb_ddl_reorg_max_write_speed = {}", PIB + 1));
}
