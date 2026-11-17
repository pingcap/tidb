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

//! Go `br/pkg/restore/misc.go`: the helpers snapshot restore uses. The log
//! restore blocklist files and the region scanner belong to PiTR and are
//! not ported.

use std::sync::Arc;

use tidb_gocontext::Context;
use tidb_log::{Field, Value};
use tidb_model::TableInfo;

use crate::errors::{self, Result};
use crate::glue::Domain;
use crate::pdclient::{compose_ts, PdClient};
use crate::utils::backoff::new_aggressive_pd_backoff_strategy;
use crate::utils::retry::with_retry;

/// Go `UniqueTableName`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct UniqueTableName {
    /// Go `DB`.
    pub db: String,
    /// Go `Table`.
    pub table: String,
}

/// Go `TransferBoolToValue`.
#[must_use]
pub fn transfer_bool_to_value(enable: bool) -> &'static str {
    if enable {
        "ON"
    } else {
        "OFF"
    }
}

/// Go `GetTableSchema`: the table's schema from the latest info schema.
///
/// # Errors
/// The schema cannot be loaded, or Go's `ErrTableNotExists`.
pub fn get_table_schema(dom: &dyn Domain, db_name: &str, table_name: &str) -> Result<TableInfo> {
    dom.info_schema()?
        .table_by_name(db_name, table_name)
        .ok_or_else(|| {
            errors::errorf(format!(
                "[schema:1146]Table '{db_name}.{table_name}' doesn't exist"
            ))
        })
}

/// Go `maxUserTablesNum`.
const MAX_USER_TABLES_NUM: usize = 10;

/// Go `AssertUserDBsEmpty`: no user database or table exists, apart from
/// an empty `test` database. Go lists the table names from the meta
/// snapshot; they are read from the latest info schema here.
///
/// # Errors
/// [`errors::ERR_RESTORE_NOT_FRESH_CLUSTER`] naming up to ten of them.
pub fn assert_user_dbs_empty(dom: &dyn Domain) -> Result<()> {
    let is = dom.info_schema()?;
    let databases = is.all_schemas();
    let mut user_tables: Vec<String> = Vec::with_capacity(MAX_USER_TABLES_NUM + 1);
    let mut append = |db: &str, table: &str| {
        if user_tables.len() >= MAX_USER_TABLES_NUM {
            user_tables.push("...".to_owned());
            return true;
        }
        user_tables.push(format!("{db}.{table}"));
        false
    };
    'dbs: for db in &databases {
        if tidb_metadef::is_mem_or_sys_db(db.name.lowercase()) {
            continue;
        }
        let tables = is.schema_table_infos(db.name.original());
        if tables.is_empty() {
            // A fresh cluster has an empty `test` database.
            if db.name.lowercase() != "test" && append(db.name.original(), "") {
                break 'dbs;
            }
            continue;
        }
        for t in &tables {
            if append(db.name.original(), t.name.original()) {
                break 'dbs;
            }
        }
    }
    if !user_tables.is_empty() {
        return Err(errors::annotate(
            errors::new(&errors::ERR_RESTORE_NOT_FRESH_CLUSTER),
            format!("user db/tables: {}", user_tables.join(", ")),
        ));
    }
    Ok(())
}

/// Go `GetTS`.
///
/// # Errors
/// PD's error.
pub fn get_ts(ctx: &Context, pd_client: &dyn PdClient) -> Result<u64> {
    let (p, l) = pd_client.get_ts(ctx)?;
    Ok(compose_ts(p, l))
}

/// Go `GetTSWithRetry`.
///
/// # Errors
/// PD's error after the aggressive PD backoff.
pub fn get_ts_with_retry(ctx: &Context, pd_client: &dyn PdClient) -> Result<u64> {
    let mut start_ts = 0;
    let mut retry = 0_u32;
    let res = with_retry(
        ctx,
        || {
            let mut res = get_ts(ctx, pd_client);
            if tidb_failpoint::eval("github.com/pingcap/tidb/br/pkg/restore/get-ts-error")
                .is_some_and(|v| v.bool())
                && retry < 3
            {
                res = Err(errors::errorf("rpc error: code = Unknown desc = [PD:tso:ErrGenerateTimestamp]generate timestamp failed, requested pd is not leader of cluster"));
            }
            retry += 1;
            match res {
                Ok(ts) => {
                    start_ts = ts;
                    Ok(())
                }
                Err(err) => {
                    tidb_log::warn(
                        "failed to get TS, retry it",
                        &[
                            Field::new("retry time", Value::U64(u64::from(retry))),
                            Field::new("error", Value::Str(err.to_string())),
                        ],
                    );
                    Err(err)
                }
            }
        },
        &mut new_aggressive_pd_backoff_strategy(),
    );
    if let Err(err) = &res {
        tidb_log::error(
            "failed to get TS",
            &[Field::new("error", Value::Str(err.to_string()))],
        );
    }
    res.map(|()| start_ts)
}

/// Go `HasRestoreIDColumn`: `mysql.tidb_pitr_id_map` has `restore_id`.
#[must_use]
pub fn has_restore_id_column(dom: &Arc<dyn Domain>) -> bool {
    let Ok(table) = get_table_schema(dom.as_ref(), "mysql", "tidb_pitr_id_map") else {
        return false;
    };
    table
        .columns
        .iter_deref()
        .any(|c| c.read().name.lowercase() == "restore_id")
}
