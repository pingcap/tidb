//! The ALTER-family cluster DDL, split out of `cluster_ddl_source` when that
//! file passed the repository's 2200-line ceiling.
//!
//! Each test pins one Go action -- ActionModifyTableComment, onRebaseAutoID,
//! ActionSetDefaultValue, ActionRenameIndex and the rest -- against the
//! stored `TableInfo` the change publishes.

#![allow(missing_docs)]

use tidb_exec::cluster_ddl::{lower_ddl, plan_ddl, DdlPlan, DdlPlanError};
use tidb_meta::key;

// The fixture helpers stay with the file that owns the bootstrap; the
// aggregated harness makes every `tests/*.rs` a sibling module.
use crate::cluster_ddl_source::{
    apply, bootstrapped, plan, statement, stored_table, stored_value, MetaStore,
};

/// Go `onModifyTableComment` (`ddl/table.go`) is a one-line
/// `tblInfo.Comment = args.Comment` under `ActionModifyTableComment`, with
/// `validateCommentLength` applied when the statement is admitted. The
/// stored comment is what `SHOW CREATE TABLE` prints and what
/// `information_schema.tables.table_comment` reports, so a comment that
/// never reaches the stored `TableInfo` is invisible everywhere.
#[test]
fn alter_table_comment_replaces_the_stored_comment() {
    let mut store = bootstrapped();
    let write = plan(
        &mut store,
        "CREATE TABLE u6.t (id BIGINT PRIMARY KEY CLUSTERED) COMMENT='original'",
        100,
    );
    apply(&mut store, &write);
    let table_id = write.created_id.expect("CREATE TABLE allocates an id");
    assert_eq!(
        stored_table(&write, table_id)["comment"],
        serde_json::json!("original"),
        "CREATE TABLE stores its COMMENT option"
    );

    let write = plan(&mut store, "ALTER TABLE u6.t COMMENT='changed'", 200);
    apply(&mut store, &write);
    assert_eq!(
        stored_table(&write, table_id)["comment"],
        serde_json::json!("changed")
    );

    // Go clears the comment rather than refusing the empty form.
    let write = plan(&mut store, "ALTER TABLE u6.t COMMENT=''", 300);
    apply(&mut store, &write);
    assert_eq!(stored_table(&write, table_id)["comment"], serde_json::json!(""));
}

/// Go `onRebaseAutoID` over `autoid.AutoIncrementType`, plus the
/// `adjustNewBaseToNextGlobalID` floor it applies without FORCE.
///
/// Two keys move together: `TableInfo.AutoIncID` and the allocator counter,
/// which holds the id LAST handed out and therefore lands one below the new
/// base. Moving only the first would leave the next INSERT allocating from
/// the old counter.
#[test]
fn rebase_auto_increment_moves_the_counter_and_floors_without_force() {
    let mut store = bootstrapped();
    let write = plan(
        &mut store,
        "CREATE TABLE u6.t (id BIGINT PRIMARY KEY AUTO_INCREMENT)",
        100,
    );
    apply(&mut store, &write);
    let table_id = write.created_id.expect("CREATE TABLE allocates an id");
    let counter_key = key::auto_table_id_kv_key(112, table_id);

    let counter = |store: &MetaStore| {
        store
            .pairs
            .get(&counter_key)
            .map(|value| tidb_meta::value::parse_int_value(value).expect("an int counter"))
    };

    // Raising the base is the ordinary case: the table records 500 and the
    // counter becomes 499, so the next allocation is exactly 500.
    let write = plan(&mut store, "ALTER TABLE u6.t AUTO_INCREMENT = 500", 200);
    assert_eq!(write.warning, None, "a rise needs no adjustment");
    apply(&mut store, &write);
    assert_eq!(
        stored_table(&write, table_id)["auto_inc_id"],
        serde_json::json!(500)
    );
    assert_eq!(counter(&store), Some(499));

    // Lowering it WITHOUT force is floored at the allocator's next id
    // (counter + 1 = 500) and warns that it used that instead. Both the
    // recorded base and the counter therefore stay put.
    let write = plan(&mut store, "ALTER TABLE u6.t AUTO_INCREMENT = 10", 300);
    assert_eq!(
        write.warning.as_deref(),
        Some("Can't reset AUTO_INCREMENT to 10 without FORCE option, using 500 instead")
    );
    apply(&mut store, &write);
    assert_eq!(
        stored_table(&write, table_id)["auto_inc_id"],
        serde_json::json!(500)
    );
    assert_eq!(counter(&store), Some(499));

    // FORCE sets it exactly, backwards, and raises no warning.
    let write = plan(&mut store, "ALTER TABLE u6.t FORCE AUTO_INCREMENT = 10", 400);
    assert_eq!(write.warning, None, "FORCE does what it was told");
    apply(&mut store, &write);
    assert_eq!(
        stored_table(&write, table_id)["auto_inc_id"],
        serde_json::json!(10)
    );
    assert_eq!(counter(&store), Some(9));
}

/// Go's `ALTER TABLE` option switch has EMPTY cases for `ENGINE`,
/// `ENGINE_ATTRIBUTE`, `STORAGE_CLASS` and `ROW_FORMAT`: the statement
/// succeeds and no job is published. Refusing them instead would reject the
/// `ENGINE=InnoDB` every mysqldump emits.
///
/// `OrderByColumns` is the same shape with one addition: Go warns when the
/// table has a user-defined primary key column, because the ordering it asks
/// for cannot survive one.
#[test]
fn accepted_no_op_alters_publish_nothing_and_order_by_warns_under_a_primary_key() {
    let mut store = bootstrapped();
    let write = plan(
        &mut store,
        "CREATE TABLE u6.keyed (id BIGINT PRIMARY KEY CLUSTERED, v BIGINT)",
        100,
    );
    apply(&mut store, &write);
    let write = plan(&mut store, "CREATE TABLE u6.bare (v BIGINT)", 200);
    apply(&mut store, &write);

    let satisfied = |store: &mut MetaStore, sql: &str, ts: u64| match plan_ddl(
        store,
        &statement(sql),
        ts,
    )
    .expect("the statement plans")
    {
        DdlPlan::AlreadySatisfied { detail, warning } => (detail, warning),
        DdlPlan::Write(_) => panic!("`{sql}` must publish nothing"),
    };

    for (offset, sql) in [
        "ALTER TABLE u6.keyed ENGINE = InnoDB",
        "ALTER TABLE u6.keyed ROW_FORMAT = DYNAMIC",
    ]
    .into_iter()
    .enumerate()
    {
        let (_, warning) = satisfied(&mut store, sql, 300 + offset as u64);
        assert_eq!(warning, None, "`{sql}` is accepted silently");
    }

    let (_, warning) = satisfied(&mut store, "ALTER TABLE u6.keyed ORDER BY v", 400);
    assert_eq!(
        warning.as_deref(),
        Some("ORDER BY ignored as there is a user-defined clustered index in the table 'keyed'")
    );

    // No primary key column, so Go raises nothing at all.
    let (_, warning) = satisfied(&mut store, "ALTER TABLE u6.bare ORDER BY v", 500);
    assert_eq!(warning, None);

    // Go resolves the table first, so a missing one still fails.
    plan_ddl(
        &mut store,
        &statement("ALTER TABLE u6.nosuch ENGINE = InnoDB"),
        600,
    )
    .expect_err("a missing table is refused");
}

/// Go `AlterColumn` followed by `updateColumnDefaultValue`
/// (`ActionSetDefaultValue`). `SET DEFAULT` installs the exact stored
/// spelling the column's own definition would have produced; `DROP DEFAULT`
/// stores nothing and sets `NoDefaultValueFlag`.
#[test]
fn set_and_drop_column_default_rewrite_the_stored_column() {
    let mut store = bootstrapped();
    let write = plan(
        &mut store,
        "CREATE TABLE u6.t (id BIGINT PRIMARY KEY CLUSTERED, a INT, b VARCHAR(10))",
        100,
    );
    apply(&mut store, &write);
    let table_id = write.created_id.expect("CREATE TABLE allocates an id");

    let column_of = |write: &tidb_exec::cluster_ddl::DdlWrite, name: &str| {
        stored_table(write, table_id)["cols"]
            .as_array()
            .expect("column array")
            .iter()
            .find(|column| column["name"]["O"] == name)
            .expect("the column is there")
            .clone()
    };

    let write = plan(&mut store, "ALTER TABLE u6.t ALTER COLUMN a SET DEFAULT 7", 200);
    apply(&mut store, &write);
    let column = column_of(&write, "a");
    assert_eq!(column["default"], serde_json::json!("7"));

    // Go stores no value and marks the column, which is what makes a later
    // omitted INSERT report 1364 under strict mode.
    let write = plan(&mut store, "ALTER TABLE u6.t ALTER COLUMN a DROP DEFAULT", 300);
    apply(&mut store, &write);
    let column = column_of(&write, "a");
    assert_eq!(column["default"], serde_json::json!(null));

    // A spelling the column type cannot hold is Go's 1067, not the generic
    // refusal: the code is what a MySQL client switches on.
    let error = plan_ddl(
        &mut store,
        &statement("ALTER TABLE u6.t ALTER COLUMN a SET DEFAULT 'zz'"),
        400,
    )
    .expect_err("a bad default is refused");
    match error {
        DdlPlanError::Admission(ref admission) => {
            assert_eq!(admission.code, 1067, "{admission:?}");
            assert!(
                admission.reason.contains("Invalid default value for 'a'"),
                "{admission:?}"
            );
        }
        other => panic!("expected an admission refusal, got {other:?}"),
    }

    // Go resolves the column before the default, so a missing one is 1054.
    let error = plan_ddl(
        &mut store,
        &statement("ALTER TABLE u6.t ALTER COLUMN nope SET DEFAULT 1"),
        500,
    )
    .expect_err("a missing column is refused");
    assert!(
        matches!(error, DdlPlanError::UnknownColumn { ref column, .. } if column == "nope"),
        "{error:?}"
    );
}

/// Go `ValidateRenameIndex` then `renameIndexes` (`ActionRenameIndex`).
/// The entries already written are untouched: an index's key prefix comes
/// from its ID, not its name.
#[test]
fn rename_index_rewrites_the_name_and_rejects_a_collision() {
    let mut store = bootstrapped();
    let write = plan(
        &mut store,
        "CREATE TABLE u6.t (id BIGINT PRIMARY KEY CLUSTERED, a BIGINT, b BIGINT, \
         KEY ia(a), KEY ib(b))",
        100,
    );
    apply(&mut store, &write);
    let table_id = write.created_id.expect("CREATE TABLE allocates an id");

    let names = |write: &tidb_exec::cluster_ddl::DdlWrite| {
        stored_table(write, table_id)["index_info"]
            .as_array()
            .expect("index array")
            .iter()
            .map(|index| index["idx_name"]["O"].as_str().expect("a name").to_owned())
            .collect::<Vec<_>>()
    };

    let write = plan(&mut store, "ALTER TABLE u6.t RENAME INDEX ia TO renamed", 200);
    apply(&mut store, &write);
    assert_eq!(names(&write), ["renamed", "ib"]);

    // Case-only renames are real: Go's no-op test is case-SENSITIVE, so this
    // publishes a new spelling rather than finishing early.
    let write = plan(
        &mut store,
        "ALTER TABLE u6.t RENAME INDEX renamed TO ReNamed",
        300,
    );
    apply(&mut store, &write);
    assert_eq!(names(&write), ["ReNamed", "ib"]);

    // The same spelling changes nothing and spends no schema version.
    match plan_ddl(
        &mut store,
        &statement("ALTER TABLE u6.t RENAME INDEX ReNamed TO ReNamed"),
        400,
    )
    .expect("an identical rename plans")
    {
        DdlPlan::AlreadySatisfied { detail, .. } => {
            assert!(detail.contains("already has that name"), "{detail}");
        }
        DdlPlan::Write(_) => panic!("an identical rename must publish nothing"),
    }

    // Renaming onto a DIFFERENT existing index is 1061.
    let error = plan_ddl(
        &mut store,
        &statement("ALTER TABLE u6.t RENAME INDEX ReNamed TO ib"),
        500,
    )
    .expect_err("a collision is refused");
    assert!(
        matches!(error, DdlPlanError::DuplicateKeyName(ref name) if name == "ib"),
        "{error:?}"
    );

    // A missing source index is Go's ErrKeyNotExists, as in ALTER INDEX.
    let error = plan_ddl(
        &mut store,
        &statement("ALTER TABLE u6.t RENAME INDEX nosuch TO x"),
        600,
    )
    .expect_err("a missing index is refused");
    assert!(
        matches!(error, DdlPlanError::KeyNotExists { ref index, .. } if index == "nosuch"),
        "{error:?}"
    );
}

/// Go `ModifySchemaCharsetAndCollate` then `onModifySchemaCharsetAndCollate`.
/// A written COLLATE settles the charset and vice versa, and a change that is
/// already true finishes without spending a schema version.
#[test]
fn alter_database_charset_settles_the_pair_and_no_ops_when_already_true() {
    let mut store = bootstrapped();

    let stored_database = |write: &tidb_exec::cluster_ddl::DdlWrite| -> serde_json::Value {
        serde_json::from_slice(stored_value(write, &key::database_kv_key(112)))
            .expect("a stored DBInfo")
    };

    // CHARACTER SET alone settles the collation to that charset's default.
    let write = plan(&mut store, "ALTER DATABASE u6 CHARACTER SET latin1", 200);
    apply(&mut store, &write);
    let database = stored_database(&write);
    assert_eq!(database["charset"], serde_json::json!("latin1"));
    assert_eq!(database["collate"], serde_json::json!("latin1_bin"));

    // COLLATE alone settles the charset it belongs to.
    let write = plan(&mut store, "ALTER DATABASE u6 COLLATE utf8mb4_general_ci", 300);
    apply(&mut store, &write);
    let database = stored_database(&write);
    assert_eq!(database["charset"], serde_json::json!("utf8mb4"));
    assert_eq!(database["collate"], serde_json::json!("utf8mb4_general_ci"));

    match plan_ddl(
        &mut store,
        &statement("ALTER DATABASE u6 COLLATE utf8mb4_general_ci"),
        400,
    )
    .expect("an already-satisfied charset plans")
    {
        DdlPlan::AlreadySatisfied { detail, .. } => {
            assert!(detail.contains("is already utf8mb4/utf8mb4_general_ci"), "{detail}");
        }
        DdlPlan::Write(_) => panic!("a no-op charset must publish nothing"),
    }

    // A collation that does not belong to the written charset is refused at
    // admission, under Go's own 1253 rather than the generic 1105.
    let parsed = tidb_parser::parse("ALTER DATABASE u6 CHARACTER SET latin1 COLLATE utf8mb4_bin")
        .expect("the fixture SQL parses");
    let error = lower_ddl(&parsed, "u6").expect_err("a mismatched pair is refused");
    assert_eq!(error.code, 1253, "{error:?}");
    assert!(
        error
            .reason
            .contains("COLLATION 'utf8mb4_bin' is not valid for CHARACTER SET 'latin1'"),
        "{error:?}"
    );

    plan_ddl(
        &mut store,
        &statement("ALTER DATABASE nosuch CHARACTER SET latin1"),
        600,
    )
    .expect_err("a missing database is refused");
}

/// Go `handleTableOptions`: `CREATE TABLE ... AUTO_ID_CACHE=n` stores the
/// value on the TableInfo, and anything past int64 is refused with its own
/// message. It was accepted by `ALTER TABLE` here but refused at CREATE,
/// which is the same option disagreeing with itself.
#[test]
fn create_table_stores_auto_id_cache() {
    let mut store = bootstrapped();
    let write = plan(
        &mut store,
        "CREATE TABLE u6.t (id BIGINT PRIMARY KEY AUTO_INCREMENT) AUTO_ID_CACHE=100",
        100,
    );
    apply(&mut store, &write);
    let table_id = write.created_id.expect("CREATE TABLE allocates an id");
    assert_eq!(
        stored_table(&write, table_id)["auto_id_cache"],
        serde_json::json!(100)
    );

    // Unset stays Go's zero rather than being materialised as the default.
    let write = plan(
        &mut store,
        "CREATE TABLE u6.plain (id BIGINT PRIMARY KEY AUTO_INCREMENT)",
        200,
    );
    apply(&mut store, &write);
    let plain_id = write.created_id.expect("CREATE TABLE allocates an id");
    let stored = stored_table(&write, plain_id);
    assert!(
        stored["auto_id_cache"].is_null() || stored["auto_id_cache"] == serde_json::json!(0),
        "{}",
        stored["auto_id_cache"]
    );

    let parsed = tidb_parser::parse(
        "CREATE TABLE u6.big (id BIGINT PRIMARY KEY AUTO_INCREMENT) AUTO_ID_CACHE=9223372036854775808",
    )
    .expect("the fixture SQL parses");
    let error = lower_ddl(&parsed, "u6").expect_err("an overflowing cache is refused");
    assert!(
        error.reason.contains("auto_id_cache overflows int64"),
        "{error:?}"
    );
}

/// Go `checkAlterTableCharset` then `onModifyTableCharsetAndCollate`.
///
/// TiDB never rewrites stored bytes for a charset change, so it permits only
/// the conversions whose encoding is a superset of the original. `CONVERT TO`
/// additionally rewrites each column's own charset; the bare option moves the
/// table default alone.
#[test]
fn convert_to_character_set_rewrites_columns_and_refuses_a_narrowing() {
    let mut store = bootstrapped();
    let write = plan(
        &mut store,
        "CREATE TABLE u6.t (id BIGINT PRIMARY KEY CLUSTERED, a VARCHAR(10), b INT) \
         CHARACTER SET latin1",
        100,
    );
    apply(&mut store, &write);
    let table_id = write.created_id.expect("CREATE TABLE allocates an id");

    let column_charsets = |write: &tidb_exec::cluster_ddl::DdlWrite| {
        stored_table(write, table_id)["cols"]
            .as_array()
            .expect("column array")
            .iter()
            .map(|column| {
                (
                    column["name"]["O"].as_str().expect("a name").to_owned(),
                    column["type"]["Charset"]
                        .as_str()
                        .unwrap_or_default()
                        .to_owned(),
                )
            })
            .collect::<Vec<_>>()
    };

    let write = plan(
        &mut store,
        "ALTER TABLE u6.t CONVERT TO CHARACTER SET utf8mb4",
        200,
    );
    apply(&mut store, &write);
    let stored = stored_table(&write, table_id);
    assert_eq!(stored["charset"], serde_json::json!("utf8mb4"));
    assert_eq!(stored["collate"], serde_json::json!("utf8mb4_bin"));
    // A text column takes the new pair; everything else is marked binary,
    // which is Go's `field_types.HasCharset` split.
    let charsets = column_charsets(&write);
    assert_eq!(
        charsets
            .iter()
            .find(|(name, _)| name == "a")
            .expect("column a")
            .1,
        "utf8mb4"
    );
    assert_eq!(
        charsets
            .iter()
            .find(|(name, _)| name == "b")
            .expect("column b")
            .1,
        "binary"
    );

    // Repeating it changes nothing and spends no schema version.
    match plan_ddl(
        &mut store,
        &statement("ALTER TABLE u6.t CONVERT TO CHARACTER SET utf8mb4"),
        300,
    )
    .expect("an already-satisfied conversion plans")
    {
        DdlPlan::AlreadySatisfied { detail, .. } => {
            assert!(detail.contains("is already utf8mb4/utf8mb4_bin"), "{detail}");
        }
        DdlPlan::Write(_) => panic!("a no-op conversion must publish nothing"),
    }

    // Narrowing back is refused: the stored bytes would be reinterpreted.
    let error = plan_ddl(
        &mut store,
        &statement("ALTER TABLE u6.t CONVERT TO CHARACTER SET latin1"),
        400,
    )
    .expect_err("a narrowing conversion is refused");
    match error {
        DdlPlanError::Admission(ref admission) => {
            assert_eq!(admission.code, 8200, "{admission:?}");
            assert!(
                admission
                    .reason
                    .contains("Unsupported modify charset from utf8mb4 to latin1"),
                "{admission:?}"
            );
        }
        other => panic!("expected an admission refusal, got {other:?}"),
    }
}

/// Go `BuildTableInfoWithLike`: the copy inherits the source's definition and
/// nothing that identifies it.
///
/// The reset list is where the bugs live -- an inherited auto-increment
/// counter would make the copy's first row collide with the source's handles,
/// and an inherited foreign key would name a constraint that already exists.
#[test]
fn create_table_like_copies_the_definition_and_resets_the_identity() {
    let mut store = bootstrapped();
    let write = plan(
        &mut store,
        "CREATE TABLE u6.src (id BIGINT PRIMARY KEY AUTO_INCREMENT, a VARCHAR(10), KEY ia(a)) \
         COMMENT='the source' AUTO_INCREMENT=500",
        100,
    );
    apply(&mut store, &write);
    let source_id = write.created_id.expect("CREATE TABLE allocates an id");

    let write = plan(&mut store, "CREATE TABLE u6.cp LIKE u6.src", 200);
    apply(&mut store, &write);
    let copy_id = write.created_id.expect("CREATE TABLE LIKE allocates an id");
    assert_ne!(copy_id, source_id, "the copy is its own table");

    let copy = stored_table(&write, copy_id);
    assert_eq!(copy["name"]["O"], serde_json::json!("cp"));
    // The definition is inherited whole.
    assert_eq!(copy["comment"], serde_json::json!("the source"));
    assert_eq!(
        copy["cols"]
            .as_array()
            .expect("column array")
            .iter()
            .map(|column| column["name"]["O"].as_str().expect("a name"))
            .collect::<Vec<_>>(),
        ["id", "a"]
    );
    assert_eq!(
        copy["index_info"]
            .as_array()
            .expect("index array")
            .iter()
            .map(|index| index["idx_name"]["O"].as_str().expect("a name"))
            .collect::<Vec<_>>(),
        ["ia"]
    );
    // The identity is NOT: Go's `tblInfo.AutoIncID = 0` means the copy's
    // first row is handle 1 however far the source has run.
    assert!(
        copy["auto_inc_id"].is_null() || copy["auto_inc_id"] == serde_json::json!(0),
        "{}",
        copy["auto_inc_id"]
    );

    // Go `ErrWrongObject`: the source has to be a real table.
    // A view's `TableInfo` is finished at the statement ROUTE (the columns
    // come from resolving the body), so it is built here rather than lowered
    // from text like the rest of the fixture.
    let view = tidb_exec::cluster_ddl::DdlStatement::CreateView {
        schema: "u6".to_owned(),
        name: "v".to_owned(),
        or_replace: false,
        info: Box::new(tidb_model::TableInfo {
            name: tidb_ast::CiString::new("v"),
            state: tidb_model::SchemaState::PUBLIC,
            view: Some(tidb_model::GoShared::new(tidb_model::table::ViewInfo::default())),
            ..tidb_model::TableInfo::default()
        }),
    };
    let DdlPlan::Write(write) = plan_ddl(&mut store, &view, 300).expect("the view plans") else {
        panic!("CREATE VIEW publishes a write");
    };
    apply(&mut store, &write);
    let error = plan_ddl(&mut store, &statement("CREATE TABLE u6.c2 LIKE u6.v"), 400)
        .expect_err("a view source is refused");
    assert!(error.to_string().contains("is not BASE TABLE"), "{error}");

    // The new name still collides the ordinary way.
    let error = plan_ddl(&mut store, &statement("CREATE TABLE u6.cp LIKE u6.src"), 500)
        .expect_err("an existing name is refused");
    assert!(
        matches!(error, DdlPlanError::TableExists { ref table, .. } if table == "cp"),
        "{error:?}"
    );
    match plan_ddl(
        &mut store,
        &statement("CREATE TABLE IF NOT EXISTS u6.cp LIKE u6.src"),
        600,
    )
    .expect("IF NOT EXISTS plans")
    {
        DdlPlan::AlreadySatisfied { .. } => {}
        DdlPlan::Write(_) => panic!("IF NOT EXISTS must publish nothing"),
    }
}

/// Go `CheckIsDropPrimaryKey` followed by the ordinary index drop.
///
/// A clustered primary key is what the rows are STORED under -- whether it is
/// the int handle (`PKIsHandle`) or a composite one (`IsCommonHandle`) -- so
/// dropping it would leave every row unaddressable, and Go refuses. Only a
/// NONCLUSTERED primary key is a real index that can go.
#[test]
fn drop_primary_key_refuses_a_clustered_one_and_drops_a_nonclustered_one() {
    let mut store = bootstrapped();
    let write = plan(
        &mut store,
        "CREATE TABLE u6.clustered (id BIGINT PRIMARY KEY CLUSTERED, v BIGINT)",
        100,
    );
    apply(&mut store, &write);
    let error = plan_ddl(
        &mut store,
        &statement("ALTER TABLE u6.clustered DROP PRIMARY KEY"),
        200,
    )
    .expect_err("a clustered primary key cannot be dropped");
    match error {
        DdlPlanError::Admission(ref admission) => {
            assert_eq!(admission.code, 8200, "{admission:?}");
            assert!(
                admission
                    .reason
                    .contains("Unsupported drop primary key when the table is using clustered index"),
                "{admission:?}"
            );
        }
        other => panic!("expected an admission refusal, got {other:?}"),
    }

    // No primary key at all is Go's ErrCantDropFieldOrKey, the same 1091
    // DROP INDEX answers for a name that is not there.
    let write = plan(&mut store, "CREATE TABLE u6.bare (id BIGINT, v BIGINT)", 300);
    apply(&mut store, &write);
    let error = plan_ddl(
        &mut store,
        &statement("ALTER TABLE u6.bare DROP PRIMARY KEY"),
        400,
    )
    .expect_err("a missing primary key is refused");
    assert!(
        matches!(error, DdlPlanError::UnknownIndex(ref name) if name == "PRIMARY"),
        "{error:?}"
    );
    assert!(
        error
            .to_string()
            .contains("Can't DROP 'PRIMARY'; check that column/key exists"),
        "{error}"
    );

    // A NONCLUSTERED primary key is an ordinary index and goes, entries and
    // all -- the same backfill DROP INDEX owes.
    let write = plan(
        &mut store,
        "CREATE TABLE u6.plain (id BIGINT, v BIGINT, PRIMARY KEY (id) NONCLUSTERED)",
        500,
    );
    apply(&mut store, &write);
    let table_id = write.created_id.expect("CREATE TABLE allocates an id");
    let write = plan(&mut store, "ALTER TABLE u6.plain DROP PRIMARY KEY", 600);
    assert!(
        write.backfill.is_some(),
        "the entries go with the definition"
    );
    apply(&mut store, &write);
    assert_eq!(
        stored_table(&write, table_id)["index_info"]
            .as_array()
            .map_or(0, Vec::len),
        0
    );
}

/// Go answers a missing table with TWO different errors, and which one
/// depends on the statement.
///
/// `DROP TABLE` uses `ErrBadTable` (1051, "Unknown table"), which Go's own
/// `TestDropTableWithoutIfExists` pins. Every other statement resolves its
/// table through `getSchemaAndTableByIdent` and answers
/// `infoschema.ErrTableNotExists` (1146, "Table ... doesn't exist"). This
/// port had used the DROP spelling everywhere.
#[test]
fn a_missing_table_is_1146_everywhere_except_drop_table() {
    let mut store = bootstrapped();

    for sql in [
        "ALTER TABLE u6.nosuch COMMENT='x'",
        "ALTER TABLE u6.nosuch AUTO_ID_CACHE = 4",
        "ALTER TABLE u6.nosuch DROP PRIMARY KEY",
        "ALTER TABLE u6.nosuch RENAME INDEX a TO b",
        "ALTER TABLE u6.nosuch ALTER COLUMN a SET DEFAULT 1",
        "CREATE INDEX i ON u6.nosuch (a)",
        "CREATE TABLE u6.copy LIKE u6.nosuch",
        "RENAME TABLE u6.nosuch TO u6.other",
    ] {
        let error = plan_ddl(&mut store, &statement(sql), 100).expect_err("refused");
        assert!(
            matches!(error, DdlPlanError::TableNotExists { ref table, .. } if table == "nosuch"),
            "`{sql}` must answer Go's ErrTableNotExists, got {error:?}"
        );
        assert_eq!(error.to_string(), "Table 'u6.nosuch' doesn't exist");
    }

    // DROP TABLE keeps Go's own, different answer.
    let error = plan_ddl(&mut store, &statement("DROP TABLE u6.nosuch"), 200)
        .expect_err("refused");
    assert!(
        matches!(error, DdlPlanError::UnknownTable { ref table, .. } if table == "nosuch"),
        "{error:?}"
    );
    assert_eq!(error.to_string(), "Unknown table 'u6.nosuch'");
}

/// A `DATETIME(n) DEFAULT CURRENT_TIMESTAMP(n)` column must produce a
/// `TableInfo` this node's own catalog loader can then load.
///
/// Publishing one it cannot load is the worst shape a DDL can take: the
/// CREATE reports success, the table exists as far as a later CREATE is
/// concerned (1050), and every read of it answers 1146.
#[test]
fn a_fractional_current_timestamp_default_is_storable() {
    let mut store = bootstrapped();
    let write = plan(
        &mut store,
        "CREATE TABLE u6.dt (o DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3))",
        100,
    );
    apply(&mut store, &write);
    let table_id = write.created_id.expect("CREATE TABLE allocates an id");
    let stored = stored_table(&write, table_id);
    let column = &stored["cols"].as_array().expect("column array")[0];
    // Go stores the marker WORD; the fsp is the column's own decimal.
    assert_eq!(column["default"], serde_json::json!("CURRENT_TIMESTAMP"));
    println!("origin = {}", column["origin_default"]);
}

/// A refusal reaches the client verbatim, so it must name the user's SQL and
/// carry Go's own error number.
///
/// `ADD COLUMN ... AS (a+1) VIRTUAL` used to be refused as
/// `1105 catalog encode failed: Unsupported ADD COLUMN option Generated {
/// expression: Binary(Plus, Column(["a"]), Int("1")), expression_text:
/// [97, 43, 49], stored: false }` -- this port's AST, the byte spelling of
/// the user's own expression, an internal step the statement never reached,
/// and the generic code in place of `ErrUnsupportedDDLOperation` (8200),
/// which `DdlAdmissionError::unsupported` had set all along.
#[test]
fn an_add_column_refusal_names_the_option_and_keeps_gos_code() {
    let mut store = bootstrapped();
    let write = plan(
        &mut store,
        "CREATE TABLE u6.t (id BIGINT PRIMARY KEY CLUSTERED, a BIGINT, b BIGINT)",
        100,
    );
    apply(&mut store, &write);

    for (sql, expected) in [
        (
            "ALTER TABLE u6.t ADD COLUMN c BIGINT AS (a+b) VIRTUAL",
            "a VIRTUAL generated expression",
        ),
        (
            "ALTER TABLE u6.t ADD COLUMN d BIGINT AS (a*2) STORED",
            "a STORED generated expression",
        ),
        (
            "ALTER TABLE u6.t ADD COLUMN e BIGINT AUTO_INCREMENT",
            "AUTO_INCREMENT",
        ),
        (
            "ALTER TABLE u6.t ADD COLUMN f BIGINT COLLATE utf8mb4_bin",
            "COLLATE",
        ),
    ] {
        let error = plan_ddl(&mut store, &statement(sql), 200).expect_err("refused");
        match error {
            DdlPlanError::Admission(ref admission) => {
                assert_eq!(admission.code, 8200, "`{sql}`: {admission:?}");
                assert_eq!(
                    admission.reason,
                    format!("Unsupported ADD COLUMN {expected} waits on its DDL course"),
                );
            }
            other => panic!("`{sql}` expected an admission refusal, got {other:?}"),
        }
        // Nothing of the port's own vocabulary reaches the client.
        assert!(
            !error.to_string().contains("catalog encode failed")
                && !error.to_string().contains('{'),
            "{error}"
        );
    }
}

/// No DDL refusal may reach a client carrying this port's own vocabulary.
///
/// The failure this guards has recurred three times at different sites: a
/// coded `DdlAdmissionError` wrapped into `DdlPlanError::Encode`, which
/// flattens Go's error number to the generic 1105 AND prefixes the message
/// with "catalog encode failed" -- naming an internal step the statement
/// never reached. A brace in the text means a Rust `Debug` dump escaped, as
/// it did for `ADD COLUMN ... AS (a+1) VIRTUAL`, which echoed the byte
/// spelling of the user's own expression back at them.
///
/// A sweep of the whole file found the two `build_added_column` call sites
/// were the last; this keeps the class from returning.
#[test]
fn no_ddl_refusal_leaks_this_ports_vocabulary() {
    let mut store = bootstrapped();
    let write = plan(
        &mut store,
        "CREATE TABLE u6.t (id BIGINT PRIMARY KEY CLUSTERED, a BIGINT, b VARCHAR(10))",
        100,
    );
    apply(&mut store, &write);

    // Shapes this node refuses, at admission or while planning.
    const REFUSED: &[&str] = &[
        "ALTER TABLE u6.t ADD COLUMN c BIGINT AS (a+1) VIRTUAL",
        "ALTER TABLE u6.t ADD COLUMN c BIGINT AS (a+1) STORED",
        "ALTER TABLE u6.t ADD COLUMN c BIGINT AUTO_INCREMENT",
        "ALTER TABLE u6.t ADD COLUMN c BIGINT COLLATE utf8mb4_bin",
        "ALTER TABLE u6.t ALTER COLUMN a SET DEFAULT 'zz'",
        "ALTER TABLE u6.t DROP PRIMARY KEY",
        "ALTER TABLE u6.t RENAME INDEX nosuch TO other",
        "ALTER TABLE u6.t DROP INDEX nosuch",
        "ALTER TABLE u6.nosuch COMMENT='x'",
        "DROP TABLE u6.nosuch",
        "CREATE TABLE u6.t (id BIGINT PRIMARY KEY)",
        "CREATE TABLE u6.c2 LIKE u6.nosuch",
    ];

    for sql in REFUSED {
        let Err(error) = plan_ddl(&mut store, &statement(sql), 200) else {
            panic!("[{sql}] was expected to be refused");
        };
        let text = error.to_string();
        assert!(
            !text.contains("catalog encode failed"),
            "[{sql}] blames an encode step it never reached: {text}"
        );
        assert!(
            !text.contains('{') && !text.contains("::"),
            "[{sql}] leaks a Rust value into the client's message: {text}"
        );
        assert!(
            text.chars().next().is_some_and(char::is_uppercase),
            "[{sql}] should read as a sentence: {text}"
        );
    }
}
