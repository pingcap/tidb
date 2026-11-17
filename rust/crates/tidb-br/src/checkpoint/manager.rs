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

//! Go `br/pkg/checkpoint/manager.go`: where a snapshot restore keeps its
//! checkpoint. BRIE restore keeps it in TiDB tables (`TableMetaManager`);
//! the external-storage manager serves the `br` CLI's `--checkpoint-storage`
//! and log restore's managers serve PiTR, neither ported.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tidb_gocontext::Context;

use super::restore::{
    parse_restore_group, value_marshaler_for_restore, CheckpointMetadataForSnapshotRestore,
    RestoreRunner, RestoreValueType,
};
use super::storage::{
    drop_checkpoint_tables, init_checkpoint_table, insert_checkpoint_meta,
    select_checkpoint_checksum, select_checkpoint_data, select_checkpoint_meta,
    TableCheckpointStorage, CHECKPOINT_CHECKSUM_TABLE_NAME, CHECKPOINT_DATA_TABLE_NAME,
    CHECKPOINT_INGEST_TABLE_NAME, CHECKPOINT_META_TABLE_NAME, CHECKPOINT_PROGRESS_TABLE_NAME,
};
use super::{
    CheckpointRunner, ChecksumItem, DEFAULT_RETRY_DURATION, DEFAULT_TICK_DURATION_FOR_CHECKSUM,
    DEFAULT_TICK_DURATION_FOR_FLUSH,
};
use crate::errors::{self, Result};
use crate::glue::{Domain, Glue, Session};

/// Go `tickDurationConfig`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TickDurationConfig {
    /// Go `tickDurationForFlush`.
    pub tick_duration_for_flush: Duration,
    /// Go `tickDurationForChecksum`.
    pub tick_duration_for_checksum: Duration,
    /// Go `retryDuration`.
    pub retry_duration: Duration,
}

impl Default for TickDurationConfig {
    /// Go `DefaultTickDurationConfig`.
    fn default() -> Self {
        Self {
            tick_duration_for_flush: DEFAULT_TICK_DURATION_FOR_FLUSH,
            tick_duration_for_checksum: DEFAULT_TICK_DURATION_FOR_CHECKSUM,
            retry_duration: DEFAULT_RETRY_DURATION,
        }
    }
}

/// Go `SnapshotMetaManagerT`: the `MetaManager` of a snapshot restore.
pub trait SnapshotMetaManager: Send + Sync + std::fmt::Display {
    /// Go `LoadCheckpointData`: each restored range through `f`; the
    /// longest recorded run time.
    ///
    /// # Errors
    /// The read error.
    fn load_checkpoint_data(
        &self,
        ctx: &Context,
        f: &mut dyn FnMut(i64, RestoreValueType) -> Result<()>,
    ) -> Result<Duration>;
    /// Go `LoadCheckpointChecksum`.
    ///
    /// # Errors
    /// The read error.
    fn load_checkpoint_checksum(
        &self,
        ctx: &Context,
    ) -> Result<(HashMap<i64, ChecksumItem>, Duration)>;
    /// Go `LoadCheckpointMetadata`.
    ///
    /// # Errors
    /// The read error, or incomplete metadata.
    fn load_checkpoint_metadata(
        &self,
        ctx: &Context,
    ) -> Result<CheckpointMetadataForSnapshotRestore>;
    /// Go `SaveCheckpointMetadata`.
    ///
    /// # Errors
    /// The write error.
    fn save_checkpoint_metadata(
        &self,
        ctx: &Context,
        meta: Option<&CheckpointMetadataForSnapshotRestore>,
    ) -> Result<()>;
    /// Go `ExistsCheckpointMetadata`.
    ///
    /// # Errors
    /// The read error.
    fn exists_checkpoint_metadata(&self, ctx: &Context) -> Result<bool>;
    /// Go `RemoveCheckpointData`.
    ///
    /// # Errors
    /// The write error.
    fn remove_checkpoint_data(&self, ctx: &Context) -> Result<()>;
    /// Go `StartCheckpointRunner` with `valueMarshalerForRestore`.
    ///
    /// # Errors
    /// A second start.
    fn start_checkpoint_runner(
        &self,
        ctx: &Context,
        cfg: TickDurationConfig,
    ) -> Result<Arc<RestoreRunner>>;
    /// Go `Close`.
    fn close(&self);
}

/// Go `TableMetaManager` for a snapshot restore: the checkpoint in the
/// tables of database `<dbName>_<restoreID>`.
pub struct TableMetaManager {
    se: Mutex<Option<Box<dyn Session>>>,
    runner_se: Mutex<Option<Box<dyn Session>>>,
    dom: Arc<dyn Domain>,
    db_name: String,
}

/// Go `NewSnapshotTableMetaManager`.
///
/// # Errors
/// A session cannot be created.
pub fn new_snapshot_table_meta_manager(
    g: &dyn Glue,
    dom: Arc<dyn Domain>,
    db_name: &str,
    restore_id: u64,
) -> Result<TableMetaManager> {
    let se = g.create_session()?;
    let runner_se = g.create_session()?;
    Ok(TableMetaManager {
        se: Mutex::new(Some(se)),
        runner_se: Mutex::new(Some(runner_se)),
        dom,
        db_name: format!("{db_name}_{restore_id}"),
    })
}

impl std::fmt::Display for TableMetaManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "databases[such as {}]", self.db_name)
    }
}

impl TableMetaManager {
    /// The database the checkpoint is kept in.
    #[must_use]
    pub fn db_name(&self) -> &str {
        &self.db_name
    }

    fn with_se<T>(&self, f: impl FnOnce(&dyn Session) -> Result<T>) -> Result<T> {
        let se = self.se.lock().unwrap();
        let se = se
            .as_deref()
            .ok_or_else(|| errors::errorf("the checkpoint meta manager is closed"))?;
        f(se)
    }

    fn table_exists(&self, table: &str) -> Result<bool> {
        Ok(self.dom.info_schema()?.table_exists(&self.db_name, table))
    }
}

impl SnapshotMetaManager for TableMetaManager {
    fn load_checkpoint_data(
        &self,
        ctx: &Context,
        f: &mut dyn FnMut(i64, RestoreValueType) -> Result<()>,
    ) -> Result<Duration> {
        self.with_se(|se| {
            select_checkpoint_data(
                ctx,
                se,
                &self.db_name,
                &parse_restore_group,
                &mut |k: &i64, v| f(*k, v),
            )
        })
    }

    fn load_checkpoint_checksum(
        &self,
        ctx: &Context,
    ) -> Result<(HashMap<i64, ChecksumItem>, Duration)> {
        self.with_se(|se| select_checkpoint_checksum(ctx, se, &self.db_name))
    }

    fn load_checkpoint_metadata(
        &self,
        ctx: &Context,
    ) -> Result<CheckpointMetadataForSnapshotRestore> {
        let data = self.with_se(|se| {
            select_checkpoint_meta(ctx, se, &self.db_name, CHECKPOINT_META_TABLE_NAME)
        })?;
        CheckpointMetadataForSnapshotRestore::from_go_json(&data)
    }

    fn save_checkpoint_metadata(
        &self,
        ctx: &Context,
        meta: Option<&CheckpointMetadataForSnapshotRestore>,
    ) -> Result<()> {
        self.with_se(|se| {
            init_checkpoint_table(
                ctx,
                se,
                &self.db_name,
                &[CHECKPOINT_DATA_TABLE_NAME, CHECKPOINT_CHECKSUM_TABLE_NAME],
            )?;
            match meta {
                Some(meta) => insert_checkpoint_meta(
                    ctx,
                    se,
                    &self.db_name,
                    CHECKPOINT_META_TABLE_NAME,
                    &meta.to_go_json(),
                ),
                None => Ok(()),
            }
        })
    }

    fn exists_checkpoint_metadata(&self, _: &Context) -> Result<bool> {
        self.table_exists(CHECKPOINT_META_TABLE_NAME)
    }

    fn remove_checkpoint_data(&self, ctx: &Context) -> Result<()> {
        self.with_se(|se| {
            drop_checkpoint_tables(
                ctx,
                self.dom.as_ref(),
                se,
                &self.db_name,
                &[
                    CHECKPOINT_DATA_TABLE_NAME,
                    CHECKPOINT_CHECKSUM_TABLE_NAME,
                    CHECKPOINT_META_TABLE_NAME,
                    CHECKPOINT_PROGRESS_TABLE_NAME,
                    CHECKPOINT_INGEST_TABLE_NAME,
                ],
            )
        })
    }

    /// Go `StartCheckpointRunner`: the runner takes the runner session and
    /// never takes the lock (its lock tick is 0).
    fn start_checkpoint_runner(
        &self,
        ctx: &Context,
        cfg: TickDurationConfig,
    ) -> Result<Arc<RestoreRunner>> {
        let se = self
            .runner_se
            .lock()
            .unwrap()
            .take()
            .ok_or_else(|| errors::errorf("the checkpoint runner session is taken"))?;
        let storage = Arc::new(TableCheckpointStorage::new(se, &self.db_name));
        Ok(Arc::new(CheckpointRunner::start(
            ctx,
            storage,
            None,
            Arc::new(value_marshaler_for_restore),
            [
                cfg.tick_duration_for_flush,
                cfg.tick_duration_for_checksum,
                Duration::ZERO,
                cfg.retry_duration,
            ],
        )))
    }

    fn close(&self) {
        self.se.lock().unwrap().take();
        self.runner_se.lock().unwrap().take();
    }
}
