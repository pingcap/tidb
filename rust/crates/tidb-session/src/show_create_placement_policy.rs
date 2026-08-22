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

//! `SHOW CREATE PLACEMENT POLICY`.

use tidb_datatype::{Datum, FieldType, FieldTypeCode};

use crate::StmtOutput;

/// Go `ShowExec.fetchShowCreatePlacementPolicy` (`executor/show.go:1774`):
/// one row of the policy's ORIGINAL-cased name and its `CREATE` text.
///
/// The text is Go's `ConstructResultOfShowCreatePlacementPolicy` (`:1742`):
/// ``CREATE PLACEMENT POLICY `name` <settings>``, where the settings clause
/// is `PlacementSettings.String()`. Go interpolates the name with `%s`
/// between literal backticks rather than escaping it.
#[must_use]
pub(crate) fn output(name: &str, settings_clause: &str) -> StmtOutput {
    let create = if settings_clause.is_empty() {
        format!("CREATE PLACEMENT POLICY `{name}` ")
    } else {
        format!("CREATE PLACEMENT POLICY `{name}` {settings_clause}")
    };
    let field_type = FieldType::new(FieldTypeCode::VarString);
    StmtOutput::Rows {
        columns: vec![
            ("Policy".to_owned(), field_type.clone()),
            ("Create Policy".to_owned(), field_type),
        ],
        rows: vec![vec![
            Datum::Bytes(name.as_bytes().to_vec()),
            Datum::Bytes(create.into_bytes()),
        ]],
    }
}
