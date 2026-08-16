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

//! Go `br/pkg/restore/tiflashrec` lands as a complete package: the
//! [`TiFlashRecorder`] that parks every table's TiFlash replica setting while a
//! restore runs, and the `ALTER TABLE ... SET TIFLASH REPLICA` DDLs that put
//! the settings back afterwards.
//!
//! The package has one production file, `tiflash_recorder.go`, and every one of
//! its symbols is here: `New`, `Load`, `GetItems`, `AddTable`, `DelTable`,
//! `Iterate`, `Rewrite`, `GenerateAlterTableDDLs`,
//! `GenerateResetAlterTableDDLs` and the package-private `alterTableSpecOf`.
//! All three upstream tests are ported in [`mod tests`](self#tests):
//! `TestRecorder`, `TestGenSql` and `TestGenResetSql`.
//!
//! Why a replica setting has to be parked at all: during a PiTR restore the
//! transaction model is broken, which breaks TiFlash with it, so no restored
//! table may be replicated to TiFlash until the restore finishes. The recorder
//! is driven by three hooks — before full restore creates the tables (record
//! and strip), while rewrite rules are generated (rewrite the recorded ids, so
//! a `RENAME` is followed by table ID rather than by name), and after PiTR
//! rewrites the meta keys (replay the settings).
//!
//! # Boundaries
//!
//! - `// boundary:` Go `pkg/infoschema.InfoSchema` — [`TableNameCatalog`]. The
//!   generators use the info schema for exactly one thing, resolving a table id
//!   to the qualified name a DDL statement needs, so the whole interface
//!   narrows to that one lookup; see the trait for why Go's two steps
//!   (`TableByID` then `infoschema.SchemaByTable`) become one.
//! - Go `br/pkg/utils.EncloseDBAndTable` is inlined: this crate restores the
//!   whole `ALTER TABLE` statement through `tidb-ast`, whose `back_quote` is
//!   Go's `utils.EncloseName` (a `` ` ``-wrapped name with inner backticks
//!   doubled), so the helper has nothing left to do. See
//!   [`alter_table_ddl_of`].
//! - Go `br/pkg/logutil.ShortError` is dropped along with the two call sites
//!   that needed it; see [`alter_table_ddl_of`] for why restore here cannot
//!   fail. The remaining `log.Info`/`log.Warn` lines are `tracing` events with
//!   the same fields.

use std::collections::HashMap;

use tidb_ast::{AlterTableAction, AlterTableStmt, DdlStmt, NodeBox, Stmt};
use tidb_model::TiFlashReplicaInfo;

/// boundary: Go `pkg/infoschema.InfoSchema`, narrowed to the one lookup the
/// DDL generators perform.
///
/// Go takes two steps, `info.TableByID(ctx, id)` and then
/// `infoschema.SchemaByTable(info, table.Meta())`, and reads exactly one field
/// out of each result: the original-case schema name and the original-case
/// table name. Both steps skip the same way when they miss, so they collapse
/// into a single fallible lookup here. The observable cost is that the two
/// distinct Go warnings ("Table do not exist" / "Schema do not exist") become
/// one; the generated DDL list is unchanged.
///
/// `infoschema` has no Rust owner reachable from this crate, and the full Go
/// interface is the entire catalog surface, which a replica recorder has no
/// business naming.
pub trait TableNameCatalog {
    /// The `(schema name, table name)` of `table_id`, both in their original
    /// case (Go `.Name.O`), or `None` when either does not exist.
    fn table_name_by_id(&self, table_id: i64) -> Option<(String, String)>;
}

/// Go `TiFlashRecorder`: the TiFlash replica information of the tables being
/// restored, keyed by table ID.
#[derive(Debug, Default, Clone)]
pub struct TiFlashRecorder {
    /// Go `items`: table ID -> TiFlash replica info.
    items: HashMap<i64, TiFlashReplicaInfo>,
}

impl TiFlashRecorder {
    /// Go `New`.
    #[must_use]
    pub fn new() -> Self {
        Self {
            items: HashMap::new(),
        }
    }

    /// Go `TiFlashRecorder.Load`.
    pub fn load(&mut self, items: HashMap<i64, TiFlashReplicaInfo>) {
        self.items = items;
    }

    /// Go `TiFlashRecorder.GetItems`.
    #[must_use]
    pub fn get_items(&self) -> &HashMap<i64, TiFlashReplicaInfo> {
        &self.items
    }

    /// Go `TiFlashRecorder.AddTable`.
    pub fn add_table(&mut self, table_id: i64, replica: TiFlashReplicaInfo) {
        tracing::info!(table = table_id, ?replica, "recording tiflash replica");
        self.items.insert(table_id, replica);
    }

    /// Go `TiFlashRecorder.DelTable`.
    pub fn del_table(&mut self, table_id: i64) {
        self.items.remove(&table_id);
    }

    /// Go `TiFlashRecorder.Iterate`.
    ///
    /// Go ranges over a map, so the visit order is unspecified in both
    /// languages; every caller and every test treats the result as a set.
    pub fn iterate<F: FnMut(i64, &TiFlashReplicaInfo)>(&self, mut f: F) {
        for (id, replica) in &self.items {
            f(*id, replica);
        }
    }

    /// Go `TiFlashRecorder.Rewrite`: re-keys a recorded setting from the
    /// upstream table ID to the one the restore created for it.
    pub fn rewrite(&mut self, old_id: i64, new_id: i64) {
        if new_id == old_id {
            return;
        }
        let old = self.items.remove(&old_id);
        tracing::info!(
            old = old_id,
            new = new_id,
            success = old.is_some(),
            "rewriting tiflash replica"
        );
        if let Some(old) = old {
            self.items.insert(new_id, old);
        }
    }

    /// Go `TiFlashRecorder.GenerateResetAlterTableDDLs`.
    ///
    /// A volume-snapshot backup does not carry the TiFlash cluster volume, yet
    /// the restored table still has replica info, so each table gets a pair:
    /// reset the count to 0, then set it back. Without the reset the `ALTER
    /// TABLE ... SET TIFLASH REPLICA` would fail.
    pub fn generate_reset_alter_table_ddls(&self, info: &dyn TableNameCatalog) -> Vec<String> {
        let mut items = Vec::with_capacity(self.items.len());
        self.iterate(|id, replica| {
            let Some((schema, table)) = info.table_name_by_id(id) else {
                tracing::warn!(id, "Table do not exist, skipping");
                return;
            };
            items.push(alter_table_ddl_of(&schema, &table, replica, true));
            items.push(alter_table_ddl_of(&schema, &table, replica, false));
        });
        items
    }

    /// Go `TiFlashRecorder.GenerateAlterTableDDLs`.
    pub fn generate_alter_table_ddls(&self, info: &dyn TableNameCatalog) -> Vec<String> {
        let mut items = Vec::with_capacity(self.items.len());
        self.iterate(|table_id, replica| {
            let Some((schema, table)) = info.table_name_by_id(table_id) else {
                tracing::warn!(
                    table_id,
                    "Table does not exist, might get filtered out if a custom filter is specified, skipping"
                );
                return;
            };
            items.push(alter_table_ddl_of(&schema, &table, replica, false));
        });
        items
    }
}

/// Go `alterTableSpecOf`, plus the `fmt.Sprintf("ALTER TABLE %s %s", ...)` its
/// two callers wrap it in.
///
/// Go restores the bare `ast.AlterTableSpec` and prefixes it with
/// `utils.EncloseDBAndTable`. This AST keeps spec restoration crate-private and
/// only publishes whole-statement restore, so the statement is what gets built
/// here -- byte-identically, because `AlterTableStmt`'s restore emits
/// `ALTER TABLE `, the back-quoted name path, a space, and then the same spec
/// text, and `back_quote` doubles inner backticks exactly as `EncloseName`
/// does.
///
/// The flag set Go passes (`RestoreKeyWordUppercase | RestoreNameBackQuotes |
/// RestoreStringSingleQuotes | RestoreStringEscapeBackslash`) is what this
/// AST's default restore already does, so it has no Rust counterpart.
///
/// Go returns `(string, error)` because `spec.Restore` writes into an
/// `io.Writer`; restoring into a `String` cannot fail, so the error -- and with
/// it both `log.Warn("Failed to generate the alter table spec", ...)` call
/// sites and their `logutil.ShortError` -- has no Rust form.
fn alter_table_ddl_of(
    schema: &str,
    table: &str,
    replica: &TiFlashReplicaInfo,
    reset: bool,
) -> String {
    let action = if reset {
        AlterTableAction::SetTiFlashReplica {
            hypo: false,
            count: 0,
            labels: Vec::new(),
        }
    } else {
        AlterTableAction::SetTiFlashReplica {
            hypo: false,
            count: replica.count,
            labels: replica.location_labels.iter().cloned().collect(),
        }
    };
    Stmt::Ddl(NodeBox::new(DdlStmt::AlterTable(Box::new(
        AlterTableStmt {
            name: vec![schema.to_owned(), table.to_owned()],
            actions: vec![action],
        },
    ))))
    .restore()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Go's `infoschema.MockInfoSchema`, narrowed to what
    /// [`TableNameCatalog`] asks: the mock puts every table in the `test`
    /// schema, keyed by ID.
    struct MockInfoSchema {
        tables: Vec<(i64, &'static str)>,
    }

    impl MockInfoSchema {
        fn new(tables: Vec<(i64, &'static str)>) -> Self {
            Self { tables }
        }
    }

    impl TableNameCatalog for MockInfoSchema {
        fn table_name_by_id(&self, table_id: i64) -> Option<(String, String)> {
            self.tables
                .iter()
                .find(|(id, _)| *id == table_id)
                .map(|(_, name)| ("test".to_owned(), (*name).to_owned()))
        }
    }

    fn replica(count: u64, labels: &[&str]) -> TiFlashReplicaInfo {
        TiFlashReplicaInfo {
            count,
            location_labels: labels
                .iter()
                .map(|label| (*label).to_owned())
                .collect::<Vec<_>>()
                .into(),
            ..Default::default()
        }
    }

    /// Go's `require.ElementsMatch`: same multiset, any order. The recorder
    /// iterates a hash map, so the DDL order is unspecified.
    fn assert_elements_match(actual: &[String], expected: &[&str]) {
        let mut actual: Vec<&str> = actual.iter().map(String::as_str).collect();
        let mut expected = expected.to_vec();
        actual.sort_unstable();
        expected.sort_unstable();
        assert_eq!(actual, expected);
    }

    /// Go's `op`: one recorded operation, closed over its arguments.
    type Op = Box<dyn Fn(&mut TiFlashRecorder)>;

    /// Go's `Case`: an operation sequence and the `(id, count)` tables that
    /// must survive it.
    type Case = (Vec<Op>, Vec<(i64, u64)>);

    // Go TestRecorder.
    #[test]
    fn recorder() {
        fn add(table_id: i64, count: u64) -> Op {
            Box::new(move |rec| rec.add_table(table_id, replica(count, &[])))
        }
        fn rewrite(table_id: i64, new_table_id: i64) -> Op {
            Box::new(move |rec| rec.rewrite(table_id, new_table_id))
        }
        fn del(table_id: i64) -> Op {
            Box::new(move |rec| rec.del_table(table_id))
        }

        let cases: Vec<Case> = vec![
            (vec![add(42, 1), add(43, 2)], vec![(42, 1), (43, 2)]),
            (vec![add(42, 3), add(43, 1), del(42)], vec![(43, 1)]),
            (
                vec![
                    add(41, 4),
                    add(42, 8),
                    rewrite(42, 1890),
                    rewrite(1890, 43),
                    rewrite(41, 100),
                ],
                vec![(43, 8), (100, 4)],
            ),
        ];

        for (index, (ops, tables)) in cases.into_iter().enumerate() {
            let mut rec = TiFlashRecorder::new();
            for op in &ops {
                op(&mut rec);
            }
            let mut tmap: HashMap<i64, u64> = tables.into_iter().collect();
            rec.iterate(|table_id, replica_real| {
                let count = tmap
                    .remove(&table_id)
                    .unwrap_or_else(|| panic!("case #{index}: the key {table_id} not recorded"));
                assert_eq!(
                    count, replica_real.count,
                    "case #{index}: the replica mismatch"
                );
            });
            assert!(
                tmap.is_empty(),
                "case #{index}: not all required are recorded"
            );
        }
    }

    // Go TestGenSql.
    #[test]
    fn gen_sql() {
        let fake_info = MockInfoSchema::new(vec![
            (1, "fruits"),
            (2, "whisper"),
            (3, "woods"),
            (4, "evils"),
        ]);
        let mut rec = TiFlashRecorder::new();
        rec.add_table(1, replica(1, &[]));
        rec.add_table(2, replica(2, &["climate"]));
        rec.add_table(3, replica(3, &["leaf", "seed"]));
        rec.add_table(
            4,
            replica(
                1,
                &[
                    r"kIll'; OR DROP DATABASE test --",
                    r#"dEaTh with \"quoting\""#,
                ],
            ),
        );

        let sqls = rec.generate_alter_table_ddls(&fake_info);
        assert_elements_match(
            &sqls,
            &[
                "ALTER TABLE `test`.`whisper` SET TIFLASH REPLICA 2 LOCATION LABELS 'climate'",
                "ALTER TABLE `test`.`woods` SET TIFLASH REPLICA 3 LOCATION LABELS 'leaf', 'seed'",
                "ALTER TABLE `test`.`fruits` SET TIFLASH REPLICA 1",
                concat!(
                    "ALTER TABLE `test`.`evils` SET TIFLASH REPLICA 1 LOCATION LABELS ",
                    r"'kIll''; OR DROP DATABASE test --', ",
                    r#"'dEaTh with \\"quoting\\"'"#
                ),
            ],
        );
    }

    // Go TestGenResetSql.
    #[test]
    fn gen_reset_sql() {
        let fake_info = MockInfoSchema::new(vec![(1, "fruits"), (2, "whisper")]);
        let mut rec = TiFlashRecorder::new();
        rec.add_table(1, replica(1, &[]));
        rec.add_table(2, replica(2, &["climate"]));

        let sqls = rec.generate_reset_alter_table_ddls(&fake_info);
        assert_elements_match(
            &sqls,
            &[
                "ALTER TABLE `test`.`whisper` SET TIFLASH REPLICA 0",
                "ALTER TABLE `test`.`whisper` SET TIFLASH REPLICA 2 LOCATION LABELS 'climate'",
                "ALTER TABLE `test`.`fruits` SET TIFLASH REPLICA 0",
                "ALTER TABLE `test`.`fruits` SET TIFLASH REPLICA 1",
            ],
        );
    }
}
