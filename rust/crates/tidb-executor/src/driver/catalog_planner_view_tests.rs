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

    use crate::*;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tidb_infoschema::*;
    use tidb_infoschema::statistics_access::*;
    use tidb_infoschema::planner_view::*;
    use tidb_infoschema::table_source::TableSource as _;

    #[test]
    fn schema_versions_distinguish_rebuilt_and_divergent_catalogs() {
        let mut original = tidb_infoschema_perfschema::mock_catalog();
        crate::run_create_table_on("CREATE TABLE epoch(a INT)", &mut original).unwrap();
        let snapshot = original.clone();
        let mut branch = original.clone();
        assert_eq!(original.metadata_version(), branch.metadata_version());
        assert_eq!(
            original.metadata_version(),
            snapshot.metadata_version()
        );
        let before_write = original.metadata_version();
        crate::run_insert_on(
            "INSERT INTO epoch VALUES (1)",
            &mut original,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        assert_eq!(before_write, original.metadata_version());
        crate::run_create_table_on("CREATE TABLE left_branch(a INT)", &mut original).unwrap();
        crate::run_create_table_on("CREATE TABLE right_branch(a VARCHAR(10))", &mut branch)
            .unwrap();
        assert_ne!(original.metadata_version(), branch.metadata_version());
        let mut rebuilt = tidb_infoschema_perfschema::mock_catalog();
        crate::run_create_table_on("CREATE TABLE epoch(a INT, b INT)", &mut rebuilt).unwrap();
        assert_ne!(
            rebuilt.metadata_version(),
            snapshot.metadata_version()
        );
    }

    /// The planner's schema view is Go's immutable per-version `InfoSchema`:
    /// one value shared by every statement and every clone that plans the
    /// same image, and rebuilt by any mutation -- including a schema change
    /// made through the mutable table handle, which moves the mutation
    /// counter without claiming to be DDL.
    #[test]
    fn the_planner_view_is_shared_until_the_catalog_moves() {
        let mut catalog = tidb_infoschema_perfschema::mock_catalog();
        crate::run_create_table_on("CREATE TABLE pv(a INT PRIMARY KEY, b INT)", &mut catalog)
            .unwrap();
        let TableEntry::Kv(table) = catalog.get_in(DEFAULT_DATABASE, "pv").unwrap() else {
            panic!("pv")
        };
        let table_id = table.table_id();
        let directory = Arc::clone(&catalog.table_id_names);
        let original = catalog.clone();
        let snapshot = catalog.clone();
        assert!(directory.get().is_none());
        assert_eq!(catalog.kv_table_by_id(table_id).unwrap().name(), "pv");
        assert!(directory.get().unwrap().contains_key(&table_id));
        assert!(Arc::ptr_eq(&directory, &catalog.table_id_names));
        assert!(Arc::ptr_eq(&directory, &original.table_id_names));
        assert!(catalog.physical_kv_table_by_id(i64::MAX).is_none());
        let first = catalog.planner_schema_view();
        assert!(Arc::ptr_eq(&first, &catalog.planner_schema_view()));
        assert!(
            Arc::ptr_eq(&first, &catalog.clone().planner_schema_view()),
            "a clone plans against the same image"
        );
        assert!(catalog
            .planner_catalog(DEFAULT_DATABASE, None)
            .find_table(DEFAULT_DATABASE, "pv")
            .is_some());

        // An index added through the mutable handle is a schema change even
        // though the accessor is the DML-narrow one: the next statement must
        // see it.
        let TableEntry::Kv(table) = catalog.get_mut_in(DEFAULT_DATABASE, "pv").unwrap() else {
            panic!("pv is not a KV table");
        };
        Arc::make_mut(table).add_index(
            tidb_table::kv_table::KvIndex {
                id: 7,
                name: "ib".to_owned(),
                comment: String::new(),
                unique: false,
                prefix_lengths: vec![tidb_datatype::UNSPECIFIED_LENGTH],
                column_offsets: vec![1],
                visible: true,
                global: false,
                global_index_version: 0,
                clustered_primary: false,
                primary: false,
            },
            false,
        );
        let after_index = catalog.planner_schema_view();
        // An index does not move a table's id or its (database, name) key,
        // so the id directory survives even though the planner view (which
        // does carry index shape) does not.
        assert!(Arc::ptr_eq(&directory, &catalog.table_id_names));
        assert!(catalog
            .kv_table_by_id(table_id)
            .unwrap()
            .indexes()
            .iter()
            .any(|index| index.id == 7));
        assert!(!original
            .kv_table_by_id(table_id)
            .unwrap()
            .indexes()
            .iter()
            .any(|index| index.id == 7));
        assert!(!Arc::ptr_eq(&first, &after_index), "the view is rebuilt");
        assert!(
            after_index
                .tables
                .get(&CatalogTableKey::new(DEFAULT_DATABASE, "pv"))
                .is_some_and(|table| table.indexes.iter().any(|index| index.read().name.original() == "ib")),
            "the new index reaches the planner"
        );

        crate::run_create_table_on(
            "CREATE TABLE pv2(a INT) PARTITION BY HASH(a) PARTITIONS 2",
            &mut catalog,
        )
        .unwrap();
        let after_ddl = catalog.planner_schema_view();
        assert!(
            !Arc::ptr_eq(&after_index, &after_ddl),
            "DDL rebuilds it too"
        );
        let planner = catalog.planner_catalog(DEFAULT_DATABASE, None);
        assert!(planner.find_table(DEFAULT_DATABASE, "pv").is_some());
        assert!(planner.find_table(DEFAULT_DATABASE, "pv2").is_some());
        assert_eq!(after_index.tables.len() + 1, after_ddl.tables.len());

        let TableEntry::Kv(partitioned) = catalog.get_in(DEFAULT_DATABASE, "pv2").unwrap() else {
            panic!("partitioned table")
        };
        for definition in &partitioned.partition().unwrap().definitions {
            assert!(catalog.kv_table_by_id(definition.id).is_none());
            let physical = catalog.physical_kv_table_by_id(definition.id).unwrap();
            assert_eq!(physical.table_id(), partitioned.table_id());
            assert_eq!(physical.record_physical_ids(), vec![definition.id]);
            assert_eq!(
                catalog.physical_kv_table_database_by_id(definition.id),
                Some(DEFAULT_DATABASE)
            );
        }

        // Reassigning a table's id is a schema change, so it goes through
        // `table_mut_in` (every production caller of it is DDL), not the
        // DML-narrow `get_mut_in` -- ordinary writes never move an id, so
        // they no longer pay to rebuild this directory (see `bump_version`).
        let TableEntry::Kv(table) = catalog.table_mut_in(DEFAULT_DATABASE, "pv").unwrap() else {
            panic!("pv")
        };
        Arc::make_mut(table).set_table_id(9000);
        assert!(catalog.kv_table_by_id(table_id).is_none());
        assert_eq!(catalog.kv_table_by_id(9000).unwrap().name(), "pv");
        let restored = snapshot;
        assert!(Arc::ptr_eq(&directory, &restored.table_id_names));
        assert!(restored.kv_table_by_id(table_id).is_some());
        assert!(restored.kv_table_by_id(9000).is_none());
    }

    /// Every production caller of `get_mut_in` is a row-content write
    /// (`INSERT`/`UPDATE`/`DELETE`, sequence advance, stats-delta flush): it
    /// never moves a table's id or its (database, name) key, so it must not
    /// force the next id lookup to rebuild this directory from every table
    /// in the catalog. A profiled `oltp_update_index` run found this
    /// directory rebuilt and torn down on every single statement -- built
    /// to resolve the statement's own cached-plan target, then thrown away
    /// by that same statement's write before anything else could reuse it.
    #[test]
    fn ordinary_dml_through_get_mut_in_does_not_rebuild_the_id_directory() {
        let mut catalog = tidb_infoschema_perfschema::mock_catalog();
        crate::run_create_table_on("CREATE TABLE od(a INT PRIMARY KEY, b INT)", &mut catalog)
            .unwrap();
        let TableEntry::Kv(table) = catalog.get_in(DEFAULT_DATABASE, "od").unwrap() else {
            panic!("od")
        };
        let table_id = table.table_id();
        assert_eq!(catalog.kv_table_by_id(table_id).unwrap().name(), "od");
        let directory = Arc::clone(&catalog.table_id_names);
        assert!(directory.get().is_some(), "the lookup above built it");

        let TableEntry::Kv(od) = catalog.get_mut_in(DEFAULT_DATABASE, "od").unwrap() else {
            panic!("od is not a KV table")
        };
        Arc::make_mut(od)
            .insert_row(
                &[tidb_datatype::Datum::Int(1), tidb_datatype::Datum::Int(2)],
                &tidb_exprctx::NoColumns,
            )
            .unwrap();
        assert!(
            Arc::ptr_eq(&directory, &catalog.table_id_names),
            "an ordinary write through get_mut_in must not invalidate the id directory"
        );
        assert_eq!(catalog.kv_table_by_id(table_id).unwrap().name(), "od");
        assert!(Arc::ptr_eq(&directory, &catalog.table_id_names));

        // A real schema change (here, a second table joining the catalog)
        // still invalidates it -- the directory is a metadata_version cache,
        // not a permanently frozen one.
        crate::run_create_table_on("CREATE TABLE od2(a INT PRIMARY KEY)", &mut catalog).unwrap();
        assert!(!Arc::ptr_eq(&directory, &catalog.table_id_names));
    }

    /// Two independent catalog images (an open transaction's private COW
    /// copy, and the shared autocommit catalog) writing the SAME table id
    /// must never mix up which write staged which record key. Go answers
    /// this with two entirely separate membuffers; here it is two entirely
    /// separate [`crate::kv_table::StagedWrites`] handles, one per
    /// [`crate::StmtContext`] -- state that was never on the `KvTable` (or
    /// any catalog-owned cache of it) to begin with, so a copy-on-write
    /// detach of the table entry has nothing to invalidate.
    #[test]
    fn independent_statement_contexts_do_not_share_staged_writes() {
        let mut catalog = tidb_infoschema_perfschema::mock_catalog();
        crate::run_create_table_on("CREATE TABLE cd(a INT PRIMARY KEY, b INT)", &mut catalog)
            .unwrap();
        let mut active_transaction = catalog.clone();
        let transaction_ctx = crate::StmtContext::for_query();
        crate::run_insert_on(
            "INSERT INTO cd VALUES (1, 10)",
            &mut active_transaction,
            &transaction_ctx,
        )
        .unwrap();
        let autocommit_ctx = crate::StmtContext::for_query();
        crate::run_insert_on(
            "INSERT INTO cd VALUES (2, 20)",
            &mut catalog,
            &autocommit_ctx,
        )
        .unwrap();

        // `has_dirty_content` is table-scoped WITHIN one tracker -- like Go's
        // membuffer prefix seek, it answers for the logical table id, not for
        // a specific catalog copy or record key -- so it is true under BOTH
        // trackers here (each staged something under this table id). The
        // record-level check is what must tell the two writes apart.
        let staged = |image: &Catalog, ctx: &crate::StmtContext, handle: i64| {
            let TableEntry::Kv(table) = image.get_in(DEFAULT_DATABASE, "cd").unwrap() else {
                panic!("expected KV table")
            };
            table.record_key_is_staged(
                ctx.staged_writes(),
                table.table_id(),
                &tidb_table::kv_table::TableHandle::Int(handle),
            )
        };
        assert!(
            staged(&catalog, &autocommit_ctx, 2),
            "the autocommit write is visible under its own statement's tracker"
        );
        assert!(
            !staged(&catalog, &transaction_ctx, 2),
            "the OTHER transaction's tracker must not see that record key"
        );
        assert!(
            staged(&active_transaction, &transaction_ctx, 1),
            "the transaction's own write is visible under its own tracker"
        );
        assert!(
            !staged(&active_transaction, &autocommit_ctx, 1),
            "the autocommit tracker must not see the transaction's record key either"
        );
    }
