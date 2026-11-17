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

//! Go `br/pkg/restore/snap_client/placement_rule_manager.go`: an online
//! restore keeps the restored tables on the stores labelled
//! `exclusive=restore`.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use tidb_gocontext::Context;
use tidb_log::{Field, Value};
use tidb_proto::metapb;
use tikv_client::pd_http;

use crate::conn::{get_all_tikv_stores_with_retry, StoreBehavior};
use crate::errors::{self, Result};
use crate::metautil;
use crate::pdclient::PdClient;
use crate::restore::split::{new_client, SplitClient};
use crate::restore_utils::CreatedTable;

/// Go `PlacementRuleManager`.
pub trait PlacementRuleManager: Send + Sync {
    /// Go `SetPlacementRule`.
    ///
    /// # Errors
    /// PD's error, or `ctx` done while waiting for the schedule.
    fn set_placement_rule(
        &mut self,
        ctx: &Context,
        tables: &[CreatedTable<metautil::Table>],
    ) -> Result<()>;
    /// Go `ResetPlacementRules`.
    ///
    /// # Errors
    /// The tables whose rule PD failed to delete.
    fn reset_placement_rules(&self, ctx: &Context) -> Result<()>;
}

const RESTORE_LABEL_KEY: &str = "exclusive";
const RESTORE_LABEL_VALUE: &str = "restore";

/// Go `loadRestoreStores`: the up stores labelled `exclusive=restore`.
fn load_restore_stores(ctx: &Context, pd_client: &dyn PdClient) -> Result<Vec<u64>> {
    let stores = get_all_tikv_stores_with_retry(ctx, pd_client, StoreBehavior::SkipTiFlash)?;
    let restore_stores: Vec<u64> = stores
        .iter()
        .filter(|s| s.state == metapb::StoreState::Up as i32)
        .filter(|s| {
            s.labels
                .iter()
                .any(|l| l.key == RESTORE_LABEL_KEY && l.value == RESTORE_LABEL_VALUE)
        })
        .map(|s| s.id)
        .collect();
    tidb_log::info(
        "load restore stores",
        &[Field::new(
            "store-ids",
            Value::Str(format!("{restore_stores:?}")),
        )],
    );
    Ok(restore_stores)
}

/// Go `NewPlacementRuleManager`: a no-op unless the restore is online and
/// some store carries the label.
///
/// # Errors
/// The stores could not be listed.
pub fn new_placement_rule_manager(
    ctx: &Context,
    pd_client: Arc<dyn PdClient>,
    pd_http_cli: Option<pd_http::Client>,
    security: tidb_pd_client::ClusterSecurity,
    runtime: Arc<tokio::runtime::Runtime>,
    is_online: bool,
) -> Result<Box<dyn PlacementRuleManager>> {
    if !is_online {
        return Ok(Box::new(OfflinePlacementRuleManager));
    }
    let restore_stores = load_restore_stores(ctx, pd_client.as_ref())?;
    if restore_stores.is_empty() {
        tidb_log::warn(
            "The cluster has not any TiKV node with the specify label, so skip setting placement rules",
            &[
                Field::new("label-key", Value::Str(RESTORE_LABEL_KEY.into())),
                Field::new("label-value", Value::Str(RESTORE_LABEL_VALUE.into())),
            ],
        );
        return Ok(Box::new(OfflinePlacementRuleManager));
    }
    Ok(Box::new(OnlinePlacementRuleManager {
        tool_client: Box::new(new_client(
            pd_client,
            pd_http_cli,
            security,
            runtime,
            super::client::MAX_SPLIT_KEYS_ONCE,
            3,
            None,
        )),
        restore_stores,
        restore_tables: HashSet::new(),
    }))
}

/// Go `offlinePlacementRuleManager`.
struct OfflinePlacementRuleManager;

impl PlacementRuleManager for OfflinePlacementRuleManager {
    fn set_placement_rule(
        &mut self,
        _: &Context,
        _: &[CreatedTable<metautil::Table>],
    ) -> Result<()> {
        Ok(())
    }

    fn reset_placement_rules(&self, _: &Context) -> Result<()> {
        Ok(())
    }
}

/// Go `onlinePlacementRuleManager`.
struct OnlinePlacementRuleManager {
    tool_client: Box<dyn SplitClient>,
    restore_stores: Vec<u64>,
    restore_tables: HashSet<i64>,
}

/// The memcomparable-encoded table prefix.
fn encoded_table_prefix(table_id: i64) -> Vec<u8> {
    let mut b = Vec::new();
    tidb_codec::encode_bytes(
        &mut b,
        &tidb_tablecodec::table_key::encode_table_prefix(table_id),
    );
    b
}

/// Go `getRuleID`.
fn get_rule_id(table_id: i64) -> String {
    format!("restore-t{table_id}")
}

impl OnlinePlacementRuleManager {
    /// Go `setupPlacementRules`: PD's default rule, copied per table with
    /// the restore label.
    fn setup_placement_rules(&self, ctx: &Context) -> Result<()> {
        tidb_log::info("start setting placement rules", &[]);
        let mut rule = self.tool_client.get_placement_rule(ctx, "pd", "default")?;
        rule.index = 100;
        rule.r#override = true;
        rule.label_constraints.push(pd_http::LabelConstraint {
            key: RESTORE_LABEL_KEY.to_owned(),
            op: pd_http::IN,
            values: vec![RESTORE_LABEL_VALUE.to_owned()],
        });
        for &table_id in &self.restore_tables {
            rule.id = get_rule_id(table_id);
            rule.start_key_hex = hex::encode(encoded_table_prefix(table_id));
            rule.end_key_hex = hex::encode(encoded_table_prefix(table_id + 1));
            self.tool_client.set_placement_rule(ctx, &rule)?;
        }
        tidb_log::info("finish setting placement rules", &[]);
        Ok(())
    }

    /// Go `checkRegions`.
    fn check_regions(&self, ctx: &Context) -> Result<(bool, String)> {
        for (progress, &table_id) in self.restore_tables.iter().enumerate() {
            let (ok, region_progress) = self.check_range(
                ctx,
                &encoded_table_prefix(table_id),
                &encoded_table_prefix(table_id + 1),
            )?;
            if !ok {
                return Ok((
                    false,
                    format!(
                        "table {progress}/{}, {region_progress}",
                        self.restore_tables.len()
                    ),
                ));
            }
        }
        Ok((true, String::new()))
    }

    /// Go `checkRange`: every peer of the range's regions is on a restore
    /// store.
    fn check_range(&self, ctx: &Context, start: &[u8], end: &[u8]) -> Result<(bool, String)> {
        // Go's limit -1 is PD's unlimited scan.
        let regions = self.tool_client.scan_regions(ctx, start, end, 0)?;
        for (i, r) in regions.iter().enumerate() {
            if r.region
                .peers
                .iter()
                .any(|p| !self.restore_stores.contains(&p.store_id))
            {
                return Ok((false, format!("region {i}/{}", regions.len())));
            }
        }
        Ok((true, String::new()))
    }

    /// Go `waitPlacementSchedule`.
    fn wait_placement_schedule(&self, ctx: &Context) -> Result<()> {
        tidb_log::info("start waiting placement schedule", &[]);
        let interval = if tidb_failpoint::eval("github.com/pingcap/tidb/br/pkg/restore/snap_client/wait-placement-schedule-quicker-ticker").is_some() {
            Duration::from_millis(500)
        } else {
            Duration::from_secs(10)
        };
        let ticker = crossbeam_channel::tick(interval);
        loop {
            crossbeam_channel::select! {
                recv(ticker) -> _ => {
                    let (ok, progress) = self.check_regions(ctx)?;
                    if ok {
                        tidb_log::info("finish waiting placement schedule", &[]);
                        return Ok(());
                    }
                    tidb_log::info(&format!("placement schedule progress: {progress}"), &[]);
                }
                recv(ctx.done()) -> _ => return Err(Arc::new(ctx.err().unwrap_or(tidb_gocontext::CtxError::Canceled))),
            }
        }
    }
}

impl PlacementRuleManager for OnlinePlacementRuleManager {
    fn set_placement_rule(
        &mut self,
        ctx: &Context,
        tables: &[CreatedTable<metautil::Table>],
    ) -> Result<()> {
        for tbl in tables {
            let Some(info) = &tbl.table else { continue };
            self.restore_tables.insert(info.id);
            if let Some(part) = &info.partition {
                part.read().definitions.with_visible(|defs| {
                    for def in defs {
                        self.restore_tables.insert(def.id);
                    }
                });
            }
        }
        self.setup_placement_rules(ctx).inspect_err(|err| {
            tidb_log::error(
                "setup placement rules failed",
                &[Field::new("error", Value::Str(err.to_string()))],
            );
        })?;
        self.wait_placement_schedule(ctx).inspect_err(|err| {
            tidb_log::error(
                "wait placement schedule failed",
                &[Field::new("error", Value::Str(err.to_string()))],
            );
        })
    }

    fn reset_placement_rules(&self, ctx: &Context) -> Result<()> {
        tidb_log::info("start resetting placement rules", &[]);
        let mut failed_tables = Vec::new();
        for &table_id in &self.restore_tables {
            if self
                .tool_client
                .delete_placement_rule(ctx, "pd", &get_rule_id(table_id))
                .is_err()
            {
                tidb_log::info(
                    "failed to delete placement rule for table",
                    &[Field::new("table-id", Value::I64(table_id))],
                );
                failed_tables.push(table_id);
            }
        }
        if !failed_tables.is_empty() {
            return Err(errors::annotatef(
                &errors::ERR_PD_INVALID_RESPONSE,
                format!("failed to delete placement rules for tables {failed_tables:?}"),
            ));
        }
        Ok(())
    }
}
