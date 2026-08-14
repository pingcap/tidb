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

use tidb_datatype::{Datum, FieldType, FieldTypeCode};
use tidb_executor::TableCharset;

use crate::StmtOutput;

pub(crate) fn output(database: String, charset: TableCharset, if_not_exists: bool) -> StmtOutput {
    let mut text = String::from("CREATE DATABASE ");
    if if_not_exists {
        text.push_str("IF NOT EXISTS ");
    }
    text.push('`');
    text.push_str(&database.replace('`', "``"));
    text.push_str("` /*!40100 DEFAULT CHARACTER SET ");
    text.push_str(charset.charset.name());
    if charset.collation != charset.charset.default_collation() {
        text.push_str(" COLLATE ");
        text.push_str(charset.collation.name());
    }
    text.push_str(" */");
    let field_type = FieldType::new(FieldTypeCode::VarString);
    StmtOutput::Rows {
        columns: vec![
            ("Database".to_owned(), field_type.clone()),
            ("Create Database".to_owned(), field_type),
        ],
        rows: vec![vec![
            Datum::Bytes(database.into_bytes()),
            Datum::Bytes(text.into_bytes()),
        ]],
    }
}
