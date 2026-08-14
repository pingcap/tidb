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

use super::Session;
use crate::tests_support::row_text;

#[test]
fn introduced_string_keeps_its_charset_and_collation() {
    let mut session = Session::new();

    assert_eq!(
        row_text(
            session.run("SELECT CHARSET(_latin1'a'), COLLATION(_latin1'a'), _latin1'A'=_latin1'a'")
        ),
        [["latin1", "latin1_bin", "0"]]
    );
    assert_eq!(
        row_text(session.run(
            "SELECT CHARSET(N'a'), COLLATION(N'a'), _utf8'A' COLLATE utf8_general_ci=_utf8'a'"
        )),
        [["utf8", "utf8_bin", "1"]]
    );
    assert_eq!(
        row_text(session.run("SELECT HEX(_binary'ä'), CHARSET(_binary'ä')")),
        [["C3A4", "binary"]]
    );
}

#[test]
fn char_using_decodes_and_tags_the_requested_charset() {
    let mut session = Session::new();

    assert_eq!(
        row_text(session.run(
            "SELECT CHAR(65, 16740, 67.5 USING utf8), \
             CHARSET(CHAR(65 USING latin1)), \
             COLLATION(CHAR(65 USING latin1)), \
             HEX(CHAR(1234567 USING gbk)), \
             CHARSET(CHAR(255 USING binary)), \
             COLLATION(CHAR(255 USING binary)), \
             HEX(CHAR(255 USING binary))"
        )),
        [[
            "AAdD",
            "latin1",
            "latin1_bin",
            "12D687",
            "binary",
            "binary",
            "FF"
        ]]
    );
    assert_eq!(
        row_text(session.run("SELECT CHAR(65, -1, 67.5 USING utf8) IS NULL")),
        [["1"]]
    );
    assert_eq!(session.wire_warning_count(), 1);
    assert_eq!(session.warnings()[0].code, 1300);

    session.run("SET sql_mode=''").unwrap();
    assert_eq!(
        row_text(session.run("SELECT HEX(CHAR(123456 USING utf8))")),
        [["01"]]
    );
    assert_eq!(session.wire_warning_count(), 1);
    assert_eq!(session.warnings()[0].code, 1300);
}
