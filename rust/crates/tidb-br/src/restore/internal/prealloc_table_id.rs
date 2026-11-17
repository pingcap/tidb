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

//! Go `br/pkg/restore/internal/prealloc_table_id`: the table IDs restore
//! preallocates, so a restored table keeps its backed-up ID when that ID is
//! free and TiKV need not rewrite its keys.

use std::collections::HashMap;

use sha2::{Digest, Sha256};
use tidb_log::{Field, Value};
use tidb_model::TableInfo;

use crate::checkpoint::restore::PreallocIDs as CheckpointPreallocIDs;
use crate::errors::{self, Result};
use crate::glue::GlobalIdAllocator;
use crate::metautil;

/// Go `InsaneTableIDThreshold`: an ID this large is always rewritten.
pub const INSANE_TABLE_ID_THRESHOLD: i64 = u32::MAX as i64;

/// Go `PreallocIDs`.
#[derive(Clone, Debug)]
pub struct PreallocIDs {
    start: i64,
    reusable_border: i64,
    end: i64,
    hash: [u8; 32],
    unalloced_ids: Option<Vec<i64>>,
    alloc_rule: HashMap<i64, i64>,
}

impl Default for PreallocIDs {
    /// Go's `&PreallocIDs{start: math.MaxInt64}` for no tables.
    fn default() -> Self {
        Self {
            start: i64::MAX,
            reusable_border: 0,
            end: 0,
            hash: [0; 32],
            unalloced_ids: None,
            alloc_rule: HashMap::new(),
        }
    }
}

/// Go `NewAndPrealloc`.
///
/// # Errors
/// An ID too large, or the allocator's error.
pub fn new_and_prealloc(
    tables: &[metautil::Table],
    m: &dyn GlobalIdAllocator,
) -> Result<PreallocIDs> {
    if tables.is_empty() {
        return Ok(PreallocIDs::default());
    }
    let mut ids = new(tables).map_err(|e| errors::annotate(e, "failed to create preallocIDs"))?;
    ids.prealloc_ids(m)
        .map_err(|e| errors::annotate(e, "failed to allocate prealloc IDs"))?;
    Ok(ids)
}

/// The IDs of a table and its partitions.
fn physical_ids(info: &TableInfo) -> Vec<i64> {
    let mut ids = vec![info.id];
    if let Some(part) = &info.partition {
        part.read()
            .definitions
            .with_visible(|defs| ids.extend(defs.iter().map(|d| d.id)));
    }
    ids
}

/// Go `collectTableIDs`: the largest sane ID and every ID, sorted.
fn collect_table_ids(tables: &[metautil::Table]) -> Result<(i64, Vec<i64>)> {
    let mut max_id = 0;
    let mut ids = Vec::with_capacity(tables.len());
    for t in tables {
        for id in physical_ids(t.info()) {
            if id > max_id && id < INSANE_TABLE_ID_THRESHOLD {
                max_id = id;
            }
            ids.push(id);
        }
    }
    if max_id + ids.len() as i64 + 1 > INSANE_TABLE_ID_THRESHOLD {
        return Err(errors::errorf(format!("table ID {max_id} is too large")));
    }
    ids.sort_unstable();
    Ok((max_id, ids))
}

/// Go `New`: the IDs to allocate, not yet allocated.
///
/// # Errors
/// An ID too large.
pub fn new(tables: &[metautil::Table]) -> Result<PreallocIDs> {
    if tables.is_empty() {
        return Ok(PreallocIDs::default());
    }
    let (max_id, unalloced) = collect_table_ids(tables)?;
    Ok(PreallocIDs {
        start: i64::MAX,
        reusable_border: max_id + 1,
        end: 0,
        hash: compute_sorted_ids_hash(&unalloced),
        alloc_rule: HashMap::with_capacity(unalloced.len()),
        unalloced_ids: Some(unalloced),
    })
}

/// Go `ReuseCheckpoint`: the allocation a checkpoint recorded, checked
/// against the tables.
///
/// # Errors
/// [`errors::ERR_INVALID_RANGE`] when the tables do not match the
/// checkpoint.
pub fn reuse_checkpoint(
    legacy: Option<&CheckpointPreallocIDs>,
    tables: &[metautil::Table],
) -> Result<PreallocIDs> {
    let Some(legacy) = legacy else {
        return Err(errors::errorf("no prealloc IDs to be reused"));
    };
    let (max_id, ids) = collect_table_ids(tables)?;
    if legacy.reusable_border < max_id + 1 {
        return Err(errors::annotatef(
            &errors::ERR_INVALID_RANGE,
            format!(
                "prealloc IDs reusable border {} does not match with the tables max ID {}",
                legacy.reusable_border,
                max_id + 1
            ),
        ));
    }
    if legacy.hash != compute_sorted_ids_hash(&ids) {
        return Err(errors::annotatef(
            &errors::ERR_INVALID_RANGE,
            "prealloc IDs hash mismatch",
        ));
    }
    let mut alloc_rule = HashMap::with_capacity(ids.len());
    let mut rewrite_cnt = 0;
    for id in ids {
        if id < legacy.start || id > INSANE_TABLE_ID_THRESHOLD {
            alloc_rule.insert(id, legacy.reusable_border + rewrite_cnt);
            rewrite_cnt += 1;
        } else if id < legacy.reusable_border {
            alloc_rule.insert(id, id);
        } else {
            return Err(errors::annotatef(
                &errors::ERR_INVALID_RANGE,
                format!(
                    "table ID {id} is out of range [{}, {})",
                    legacy.start, legacy.reusable_border
                ),
            ));
        }
    }
    Ok(PreallocIDs {
        start: legacy.start,
        reusable_border: legacy.reusable_border,
        end: legacy.end,
        hash: legacy.hash,
        unalloced_ids: None,
        alloc_rule,
    })
}

impl std::fmt::Display for PreallocIDs {
    /// Go `String`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.start >= self.end {
            return write!(f, "ID:empty(end={})", self.end);
        }
        write!(f, "ID:[{},{})", self.start, self.end)
    }
}

impl PreallocIDs {
    /// Go `GetIDRange`.
    #[must_use]
    pub fn get_id_range(&self) -> (i64, i64) {
        (self.start, self.end)
    }

    /// Go `PreallocIDs`: the free backed-up IDs kept, the others rewritten
    /// past the reusable border, and the range advanced in one call.
    ///
    /// # Errors
    /// A second allocation, or the allocator's error.
    pub fn prealloc_ids(&mut self, m: &dyn GlobalIdAllocator) -> Result<()> {
        let Some(unalloced) = self.unalloced_ids.as_ref().filter(|ids| !ids.is_empty()) else {
            return Ok(());
        };
        if self.start < self.end {
            return Err(errors::errorf("table ID should only be allocated once"));
        }
        let current = m.get_global_id().inspect_err(|err| {
            tidb_log::error(
                "failed to get global ID",
                &[Field::new("error", Value::Str(err.to_string()))],
            );
        })?;
        self.start = current + 1;
        if self.reusable_border <= self.start {
            self.reusable_border = self.start;
        }
        let mut rewrite_cnt = 0;
        for &id in unalloced {
            if id >= self.start && id < INSANE_TABLE_ID_THRESHOLD {
                self.alloc_rule.insert(id, id);
                continue;
            }
            self.alloc_rule
                .insert(id, self.reusable_border + rewrite_cnt);
            rewrite_cnt += 1;
        }
        let id_range = self.reusable_border - self.start + rewrite_cnt;
        m.advance_global_ids(id_range as i32)?;
        self.end = self.start + id_range;
        self.unalloced_ids = None;
        Ok(())
    }

    /// Go `AllocID`.
    ///
    /// # Errors
    /// The IDs are not allocated yet, or the ID is out of the range.
    pub fn alloc_id(&self, original_id: i64) -> Result<i64> {
        if self.unalloced_ids.is_some() {
            return Err(errors::errorf(format!(
                "table ID {original_id} is not allocated yet"
            )));
        }
        let rewrite_id = self.alloc_rule.get(&original_id).copied().unwrap_or(0);
        if rewrite_id < self.start || rewrite_id >= self.end {
            return Err(errors::errorf(format!(
                "table ID {rewrite_id} is not in range [{}, {})",
                self.start, self.end
            )));
        }
        Ok(rewrite_id)
    }

    /// Go `RewriteTableInfo`: a copy of the table with its allocated IDs.
    ///
    /// # Errors
    /// An ID without an allocation.
    pub fn rewrite_table_info(&self, info: &TableInfo) -> Result<TableInfo> {
        let mut copy = info.clone_like_go();
        copy.id = self.alloc_id(info.id).map_err(|e| {
            errors::annotate(e, format!("failed to allocate table ID for {}", info.id))
        })?;
        if let Some(part) = &copy.partition {
            let defs = &part.read().definitions;
            for i in 0..defs.with_visible(<[_]>::len) {
                let old = defs.get(i).id;
                let new = self.alloc_id(old).map_err(|e| {
                    errors::annotate(e, format!("failed to allocate partition ID for {old}"))
                })?;
                defs.update(i, |d| d.id = new);
            }
        }
        Ok(copy)
    }

    /// Go `CreateCheckpoint`: `None` for an empty range.
    #[must_use]
    pub fn create_checkpoint(&self) -> Option<CheckpointPreallocIDs> {
        if self.start >= self.end {
            return None;
        }
        Some(CheckpointPreallocIDs {
            start: self.start,
            reusable_border: self.reusable_border,
            end: self.end,
            hash: self.hash,
        })
    }
}

/// Go `computeSortedIDsHash`: SHA-256 over the big-endian IDs.
fn compute_sorted_ids_hash(ids: &[i64]) -> [u8; 32] {
    let mut h = Sha256::new();
    for id in ids {
        h.update(id.to_be_bytes());
    }
    h.finalize().into()
}

#[cfg(test)]
mod tests;
