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

//! Go `br/pkg/utils/filter.go`: the table filter as restore applies it. The
//! PiTR ID tracker belongs to log restore and is not ported.

use tidb_table_filter::Filter;

use super::misc::{is_sys_db, strip_temp_db_prefix_if_needed};

/// Go `MatchSchema`: a temporary system database counts as its system
/// database; system databases match only with `with_sys`.
#[must_use]
pub fn match_schema(filter: &dyn Filter, schema: &str, with_sys: bool) -> bool {
    let schema = strip_temp_db_prefix_if_needed(schema);
    if is_sys_db(schema) && !with_sys {
        return false;
    }
    filter.match_schema(schema)
}

/// Go `MatchTable`.
#[must_use]
pub fn match_table(filter: &dyn Filter, schema: &str, table: &str, with_sys: bool) -> bool {
    let schema = strip_temp_db_prefix_if_needed(schema);
    if is_sys_db(schema) && !with_sys {
        return false;
    }
    filter.match_table(schema, table)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Go `TestMatchSchema` / `TestMatchTable`-like.
    #[test]
    fn system_databases_need_with_sys() {
        let all = tidb_table_filter::all();
        assert!(match_schema(
            all.as_ref(),
            "__TiDB_BR_Temporary_mysql",
            true
        ));
        assert!(!match_schema(
            all.as_ref(),
            "__TiDB_BR_Temporary_mysql",
            false
        ));
        assert!(!match_table(all.as_ref(), "mysql", "user", false));
        assert!(match_table(all.as_ref(), "test", "t", false));
    }
}
