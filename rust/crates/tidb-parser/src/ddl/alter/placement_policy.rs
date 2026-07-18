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

//! Go `parseTableOption`'s table-level placement-policy leaf.

use tidb_ast::{AlterTableAction, TableOption};

use crate::{PResult, Parser};

/// Parses `PLACEMENT [POLICY] (SET DEFAULT | [=] (DEFAULT | StringName))`.
///
/// `HandParser.parseAlterTableOptions` delegates this generic option to
/// `parseTableOption`, which records it as one `TableOptionPlacementPolicy`
/// inside `AlterTableOption.Options`. The existing `SetTableOptions` envelope
/// preserves that shape; partition placement and CREATE TABLE options route
/// through separate grammar owners.
pub(crate) fn parse(parser: &mut Parser) -> PResult<Option<AlterTableAction>> {
    if !parser.is_kw("PLACEMENT") {
        return Ok(None);
    }
    parser.bump();
    if parser.is_kw("POLICY") {
        parser.bump();
    }
    let policy = if parser.is_kw("SET") && parser.is_kw_at(1, "DEFAULT") {
        parser.bump();
        parser.bump();
        "DEFAULT".to_owned()
    } else {
        parser.accept_optional_equals();
        if parser.is_kw("DEFAULT") {
            parser.bump();
            "DEFAULT".to_owned()
        } else {
            parser.parse_placement_policy_name()?
        }
    };
    Ok(Some(AlterTableAction::SetTableOptions {
        options: vec![TableOption::PlacementPolicy(policy)],
    }))
}
