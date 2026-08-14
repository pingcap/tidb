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
