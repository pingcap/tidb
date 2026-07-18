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

//! Direct source rows for Go's ALTER TABLE ORDER BY and qualified MODIFY leaves.

use super::*;

/// Exact integration parser rows at `tests/integrationtest/t/ddl/db.test:110`
/// and `:114`, plus `db_integration.test:256`.
#[test]
fn alter_order_by_restores_source_rows() {
    assert_eq!(
        r("alter table ob order by c"),
        "ALTER TABLE `ob` ORDER BY `c`"
    );
    assert_eq!(
        r("alter table ob order by c desc, d asc"),
        "ALTER TABLE `ob` ORDER BY `c` DESC,`d`"
    );
}

#[test]
fn alter_qualified_modify_restores_source_row() {
    assert_eq!(
        r("alter table test_error_code_succ modify testx.test_error_code_succ.c1 bigint"),
        "ALTER TABLE `test_error_code_succ` MODIFY COLUMN `testx`.`test_error_code_succ`.`c1` BIGINT"
    );
}
