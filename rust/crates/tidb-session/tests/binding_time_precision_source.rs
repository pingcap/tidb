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

//! The precision `SHOW BINDINGS` prints a binding's times with.

use crate::support::testkit::TestKit;

/// Go `CreateSessionBinding` stamps `types.NewTime(now, TypeTimestamp, 3)`,
/// so a session binding shows `2026-11-18 13:43:06.564`; a global binding's
/// times come back from `mysql.bind_info`'s `TIMESTAMP(6)` with six digits.
#[test]
fn a_session_bindings_times_have_millisecond_precision() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t (a int, key ia (a))");
    tk.must_exec("create session binding for select * from t where a = 1 using select /*+ use_index(t, ia) */ * from t where a = 1");
    tk.must_exec("create global binding for select * from t where a > 1 using select /*+ use_index(t, ia) */ * from t where a > 1");
    let fraction_digits = |text: &str| text.rsplit_once('.').map_or(0, |(_, fraction)| fraction.len());
    for (sql, digits) in [("show session bindings", 3), ("show global bindings", 6)] {
        let result = tk.must_query(sql);
        let rows = result.rows();
        assert_eq!(rows.len(), 1, "{sql}: {rows:?}");
        // create_time and update_time.
        for column in [4, 5] {
            assert_eq!(fraction_digits(&rows[0][column]), digits, "{sql}: {}", rows[0][column]);
        }
    }
}
