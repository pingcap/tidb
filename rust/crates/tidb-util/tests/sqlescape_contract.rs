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

//! Public byte-preservation contract for `pkg/util/sqlescape`.

use tidb_util::sqlescape::{escape_sql, escape_string, SqlArg};

#[test]
fn sqlescape_go_strings_preserve_arbitrary_bytes() {
    assert_eq!(escape_string(b"value\xff'"), b"value\xff\\'");
    assert_eq!(escape_sql(b"select \xff", &[]).unwrap(), b"select \xff");

    assert_eq!(
        escape_sql(b"select %?", &[SqlArg::String(b"value\xff'")]).unwrap(),
        b"select 'value\xff\\''"
    );
    assert_eq!(
        escape_sql(b"use %n", &[SqlArg::String(b"table\xfe`")]).unwrap(),
        b"use `table\xfe```"
    );

    let values: [&[u8]; 2] = [b"a\xfd", b"b'"];
    assert_eq!(
        escape_sql(b"select %?", &[SqlArg::Strings(&values)]).unwrap(),
        b"select 'a\xfd','b\\''"
    );
}
