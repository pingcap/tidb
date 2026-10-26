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

//! Go `pkg/executor/show.go`: the `ShowExec` fetchers that moved here from
//! the session.

use tidb_datatype::Datum;
use tidb_variable::sysvar;

use crate::sessionctx::{sys_var_hidden_for_sem, SessionContext};

/// Go `ShowExec.fetchShowVariables`: one `Variable_name | Value` row per
/// variable whose name matches `pattern` (already lowercased, Go
/// `ShowBaseExtractor`). GLOBAL lists no SESSION-only variable; neither form
/// lists a noop variable while `tidb_enable_noop_variables` is OFF, a
/// variable SEM hides, or an internal session variable. GLOBAL reads the
/// GLOBAL tier (a variable with no GLOBAL copy reports its default);
/// SESSION reads the session's own values, the `tidb_last_*_info` getters
/// computed from live session state.
pub fn fetch_show_variables(
    ctx: &dyn SessionContext,
    global: bool,
    pattern: Option<&str>,
) -> Vec<Vec<Datum>> {
    let vars = ctx.session_vars();
    let noop_enabled = vars.noop_variables_enabled();
    let mut rows = Vec::new();
    for definition in sysvar::SYS_VARS {
        if (global && definition.scope == sysvar::SCOPE_SESSION)
            || (definition.is_noop() && !noop_enabled)
        {
            continue;
        }
        if sys_var_hidden_for_sem(ctx, definition.name)
            || definition.is_internal_session_variable()
        {
            continue;
        }
        let matches = pattern.is_none_or(|pattern| {
            crate::like_match_with_collation(
                definition.name,
                pattern,
                None,
                tidb_datatype::Collation::Utf8Mb4Bin,
            )
        });
        if !matches {
            continue;
        }
        let live = if global {
            None
        } else {
            match definition.name {
                "tidb_last_ddl_info" => Some(vars.last_ddl_info_value()),
                "tidb_last_query_info" => Some(vars.last_query_info_value()),
                "tidb_last_txn_info" => Some(vars.last_txn_info_value()),
                _ => None,
            }
        };
        let value = match live {
            Some(live) => live,
            None if global => vars
                .get_global(definition.name)
                .unwrap_or_else(|_| sysvar::effective_default(definition)),
            None => vars
                .get_system(definition.name)
                .unwrap_or_else(|_| sysvar::effective_default(definition)),
        };
        rows.push(vec![
            Datum::Bytes(definition.name.as_bytes().to_vec()),
            Datum::Bytes(value.into_bytes()),
        ]);
    }
    rows
}
