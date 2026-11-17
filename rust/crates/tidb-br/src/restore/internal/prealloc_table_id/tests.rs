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

//! Go `prealloc_table_id/alloc_test.go`.

use std::cell::Cell;
use std::collections::HashMap;

use tidb_ast::CiString;
use tidb_model::{DBInfo, GoShared, PartitionDefinition, PartitionInfo, TableInfo};

use super::*;

/// Go `testAllocator`.
struct TestAllocator(Cell<i64>);

impl GlobalIdAllocator for TestAllocator {
    fn get_global_id(&self) -> Result<i64> {
        Ok(self.0.get())
    }
    fn advance_global_ids(&self, n: i32) -> Result<i64> {
        let old = self.0.get();
        self.0.set(old + i64::from(n));
        Ok(old)
    }
}

fn table(id: i64, parts: &[i64]) -> metautil::Table {
    let defs: Vec<PartitionDefinition> = parts
        .iter()
        .map(|&id| PartitionDefinition {
            id,
            ..Default::default()
        })
        .collect();
    let info = TableInfo {
        name: CiString::new(format!("t{id}")),
        id,
        partition: Some(GoShared::new(PartitionInfo {
            definitions: defs.into(),
            ..Default::default()
        })),
        ..Default::default()
    };
    metautil::Table::with_schema(
        DBInfo {
            name: CiString::new("test"),
            ..Default::default()
        },
        info,
    )
}

/// Go `checkBatchAlloc`.
fn check_batch_alloc(
    ret: &HashMap<String, TableInfo>,
    tables: &[metautil::Table],
    current: i64,
    reusable: i64,
) {
    assert_eq!(ret.len(), tables.len());
    for t in tables {
        let info = t.info();
        let got = &ret[info.name.lowercase()];
        if info.id > current && info.id < INSANE_TABLE_ID_THRESHOLD {
            assert_eq!(
                got.id,
                info.id,
                "table {} keeps its ID",
                info.name.original()
            );
        }
        if (info.id <= current || info.id >= INSANE_TABLE_ID_THRESHOLD) && got.id < reusable {
            panic!(
                "table {} got {}, below {reusable}",
                info.name.original(),
                got.id
            );
        }
    }
}

/// Go `TestAllocator`.
#[test]
fn allocator() {
    struct Case {
        table_ids: &'static [i64],
        partitions: &'static [(i64, &'static [i64])],
        has_allocated_to: i64,
        reusable_border: i64,
    }
    let msg = |c: &Case| {
        if c.table_ids.is_empty() {
            return "ID:empty(end=0)".to_owned();
        }
        let rewritten = |id: &i64| *id <= c.has_allocated_to || *id >= INSANE_TABLE_ID_THRESHOLD;
        let rewrite_cnt = c.table_ids.iter().filter(|id| rewritten(id)).count()
            + c.partitions
                .iter()
                .flat_map(|(_, p)| p.iter())
                .filter(|id| rewritten(id))
                .count();
        format!(
            "ID:[{},{})",
            c.has_allocated_to + 1,
            c.reusable_border + rewrite_cnt as i64
        )
    };
    let cases = [
        Case {
            table_ids: &[],
            partitions: &[],
            has_allocated_to: 20,
            reusable_border: 0,
        },
        Case {
            table_ids: &[1, 2, 15, 6, 7],
            partitions: &[],
            has_allocated_to: 6,
            reusable_border: 16,
        },
        Case {
            table_ids: &[4, 6, 9, 2],
            partitions: &[],
            has_allocated_to: 1,
            reusable_border: 10,
        },
        Case {
            table_ids: &[1, 2, 3, 4],
            partitions: &[],
            has_allocated_to: 5,
            reusable_border: 6,
        },
        Case {
            table_ids: &[2, 3, 4, 5],
            partitions: &[],
            has_allocated_to: 5,
            reusable_border: 6,
        },
        Case {
            table_ids: &[10, 7, 8, 9],
            partitions: &[(7, &[2, 3, 4, 11, 12])],
            has_allocated_to: 5,
            reusable_border: 13,
        },
        Case {
            table_ids: &[1, 2, 5, 6, 1 << 50, (1 << 50) + 2479],
            partitions: &[],
            has_allocated_to: 3,
            reusable_border: 7,
        },
        Case {
            table_ids: &[11, 22, 5, 6, 7],
            partitions: &[(7, &[8, 9, 10, 11, 12])],
            has_allocated_to: 6,
            reusable_border: 23,
        },
        Case {
            table_ids: &[1, 2, 9_000_005, 7, 17, 130],
            partitions: &[(7, &[8, 9, 10, 11, 12])],
            has_allocated_to: 9,
            reusable_border: 9_000_006,
        },
    ];
    for (i, c) in cases.iter().enumerate() {
        let tables: Vec<metautil::Table> = c
            .table_ids
            .iter()
            .map(|&id| {
                table(
                    id,
                    c.partitions
                        .iter()
                        .find(|(t, _)| *t == id)
                        .map_or(&[][..], |(_, p)| *p),
                )
            })
            .collect();
        let mut ids = new(&tables).unwrap();
        ids.prealloc_ids(&TestAllocator(Cell::new(c.has_allocated_to)))
            .unwrap();
        let alloc: HashMap<String, TableInfo> = tables
            .iter()
            .map(|t| {
                (
                    t.info().name.lowercase().to_owned(),
                    ids.rewrite_table_info(t.info()).unwrap(),
                )
            })
            .collect();
        check_batch_alloc(&alloc, &tables, c.has_allocated_to, c.reusable_border);
        assert_eq!(ids.to_string(), msg(c), "case #{i}");
    }
}

/// `RewriteTableInfo` copies: the source keeps its IDs.
#[test]
fn rewrite_table_info_leaves_the_source_alone() {
    let tables = vec![table(3, &[4, 5])];
    let mut ids = new(&tables).unwrap();
    ids.prealloc_ids(&TestAllocator(Cell::new(10))).unwrap();
    let got = ids.rewrite_table_info(tables[0].info()).unwrap();
    assert_eq!(got.id, 11);
    let parts: Vec<i64> = got
        .partition
        .as_ref()
        .unwrap()
        .read()
        .definitions
        .map_visible(|d| d.id);
    assert_eq!(parts, vec![12, 13]);
    let src: Vec<i64> = tables[0]
        .info()
        .partition
        .as_ref()
        .unwrap()
        .read()
        .definitions
        .map_visible(|d| d.id);
    assert_eq!((tables[0].info().id, src), (3, vec![4, 5]));
}

/// The checkpoint round trip: `CreateCheckpoint` then `ReuseCheckpoint`
/// give the same IDs, and other tables are refused.
#[test]
fn reuse_checkpoint_matches_the_allocation() {
    let tables = vec![table(3, &[]), table(15, &[]), table(1 << 40, &[])];
    let mut ids = new(&tables).unwrap();
    ids.prealloc_ids(&TestAllocator(Cell::new(10))).unwrap();
    let cp = ids.create_checkpoint().unwrap();
    let reused = reuse_checkpoint(Some(&cp), &tables).unwrap();
    for t in &tables {
        assert_eq!(
            reused.alloc_id(t.info().id).unwrap(),
            ids.alloc_id(t.info().id).unwrap()
        );
    }
    assert_eq!(reused.get_id_range(), ids.get_id_range());
    let err = reuse_checkpoint(Some(&cp), &[table(3, &[]), table(15, &[])]).unwrap_err();
    assert!(
        err.to_string().contains("prealloc IDs hash mismatch"),
        "{err}"
    );
    assert!(reuse_checkpoint(None, &tables).is_err());
    assert!(PreallocIDs::default().create_checkpoint().is_none());
}
