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

//! Source-owned binary escape coverage for
//! `tests/integrationtest/t/table/partition.test:449`.
//!
//! Go's scanner decodes `\\b` and `\\Z` to bytes 0x08 and 0x1A before the
//! `ValueExpr.Restore` path runs. The complete INSERT row keeps the plain,
//! `_binary`, and `UNHEX` expression forms together so the test proves the
//! shared string decoder and each AST restore boundary at once.

use super::*;

#[test]
fn partition_issue_57675_insert_binary_escapes_restore_like_go() {
    let sql = "insert into tb values ('\\0\\b\\n\\r\\t\\Z', _binary '\\0\\b\\n\\r\\t\\Z', unhex('00080A0D091A'));";
    let expected = "INSERT INTO `tb` VALUES (_UTF8MB4'\0\u{0008}\n\r\t\u{001A}',_BINARY'\0\u{0008}\n\r\t\u{001A}',UNHEX(_UTF8MB4'00080A0D091A'))";
    let statement = parse(sql).expect("Go accepts the Issue57675 INSERT row");
    assert_eq!(statement.restore().as_bytes(), expected.as_bytes());
}
