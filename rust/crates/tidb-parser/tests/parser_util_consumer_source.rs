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

//! Consumer proof that parser string decoding uses `pkg/parser/util` semantics.

use tidb_parser::parse;

#[test]
fn string_literals_share_the_complete_mysql_escape_contract() {
    let statement =
        parse("select '\\n\\0\\b\\Z\\r\\t'").expect("Go accepts every control-byte escape form");
    assert_eq!(
        statement.restore().as_bytes(),
        b"SELECT _UTF8MB4'\n\0\x08\x1a\r\t'"
    );

    let pattern = parse("select '\\%\\_'").expect("Go retains LIKE pattern escapes");
    assert_eq!(pattern.restore(), "SELECT _UTF8MB4'\\\\%\\\\_'");

    let backslash = parse("select '\\\\'").expect("Go accepts an escaped backslash");
    assert_eq!(backslash.restore(), "SELECT _UTF8MB4'\\\\'");

    let unknown = parse("select '\\a'").expect("Go drops unknown escape backslashes");
    assert_eq!(unknown.restore(), "SELECT _UTF8MB4'a'");
}
