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

use super::*;

/// Source rows from `tests/integrationtest/t/executor/insert.test:907,913`
/// and `tests/integrationtest/t/executor/prepared.test:240`.
#[test]
fn set_restore_mismatch_rows_match_go() {
    assert_eq!(
        r("set @@SQL_MODE='STRICT_TRANS_TABLES'"),
        "SET @@SESSION.`sql_mode`=_UTF8MB4'STRICT_TRANS_TABLES'"
    );
    assert_eq!(
        r("set @@SQL_MODE=''"),
        "SET @@SESSION.`sql_mode`=_UTF8MB4''"
    );
    assert_eq!(r("set @a = 1."), "SET @`a`=1");
}
