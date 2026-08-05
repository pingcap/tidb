//! What a primary key does to the row's identity: the integer key that
//! BECOMES the row handle, the clustered common handle, the non-integer key
//! enforced by an index instead, and the handle a table without any primary
//! key gets.
//!
//! Mirrors Go `pkg/table/tables`' `PKIsHandle`/`IsCommonHandle` behaviour and
//! the `_tidb_rowid` allocation that stands in when neither applies.

use super::*;

/// A single-column integer PRIMARY KEY becomes the row handle (Go's
/// TableInfo.PKIsHandle), so the key value addresses the row and a repeat
/// is ErrDupEntry. Transcreated from Go's own duplicate-key behavior in
/// pkg/table/tables `AddRecord`.
#[test]
fn integer_primary_key_is_the_row_handle() {
    for ddl in [
        "CREATE TABLE p (id BIGINT PRIMARY KEY, v BIGINT)",
        "CREATE TABLE p (id BIGINT, v BIGINT, PRIMARY KEY (id))",
    ] {
        let mut catalog = Catalog::default();
        crate::run_create_table_on(ddl, &mut catalog).unwrap();
        run_insert_on(
            "INSERT INTO p VALUES (10, 100), (20, 200)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();

        // The rows come back in handle order, which is the key's order --
        // not insertion order, because the handle IS the primary key.
        run_insert_on(
            "INSERT INTO p VALUES (5, 50)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        assert_eq!(
            run_select_on(
                "SELECT id FROM p",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![
                vec![Datum::Int(5)],
                vec![Datum::Int(10)],
                vec![Datum::Int(20)],
            ],
            "{ddl}"
        );

        // A repeated key is Go's ErrDupEntry.
        assert!(
            matches!(
                run_insert_on(
                    "INSERT INTO p VALUES (10, 999)",
                    &mut catalog,
                    &crate::StmtContext::for_query()
                ),
                Err(DriverError::DuplicateEntry { .. })
            ),
            "{ddl}"
        );
        // The failed insert left the original row untouched.
        assert_eq!(
            run_select_on(
                "SELECT v FROM p WHERE id = 10",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(100)]],
            "{ddl}"
        );
        // A negative key works too: the key codec sign-flips handles.
        run_insert_on(
            "INSERT INTO p VALUES (-1, 1)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        assert_eq!(
            run_select_on(
                "SELECT id FROM p",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap()
            .len(),
            4,
            "{ddl}"
        );
    }
}

/// Without a primary key the handle is the allocated row id, so repeated
/// values are fine -- the table is a heap, as in Go with _tidb_rowid.
#[test]
fn without_a_primary_key_rows_repeat_freely() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on("CREATE TABLE h (a BIGINT)", &mut catalog).unwrap();
    run_insert_on(
        "INSERT INTO h VALUES (1), (1), (1)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    assert_eq!(
        run_select_on(
            "SELECT a FROM h",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap()
        .len(),
        3
    );
}

/// Constraint shapes that need tiers this seed lacks are rejected rather
/// than silently dropped, so a table never claims what it cannot enforce.
#[test]
fn unsupported_constraints_are_rejected() {
    let mut catalog = Catalog::default();
    for ddl in [
        // Two primary keys is not a table.
        "CREATE TABLE c (a BIGINT PRIMARY KEY, b BIGINT PRIMARY KEY)",
        // A prefix-length primary key needs prefix index support.
        "CREATE TABLE c (a VARCHAR(10), PRIMARY KEY (a(3)))",
    ] {
        assert!(
            crate::run_create_table_on(ddl, &mut catalog).is_err(),
            "{ddl} should be rejected"
        );
    }
}

/// A non-integer primary key never sets `PKIsHandle` -- Go reserves that for
/// a single integer column -- so the key value does not address the row
/// directly, and a repeat is caught on the way in either way.
///
/// Under this tier's stock `@@tidb_enable_clustered_index = ON` the key is a
/// clustered COMMON handle rather than a `_tidb_rowid` table with a unique
/// index; `clustered_common_handle` below pins that layout, and
/// `the_clustered_index_mode_decides_the_handle` pins when each applies.
/// What this test asserts holds for both.
#[test]
fn a_non_integer_primary_key_is_enforced_by_its_index() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on("CREATE TABLE s (k VARCHAR(10) PRIMARY KEY)", &mut catalog).unwrap();
    run_insert_on(
        "INSERT INTO s VALUES ('a'), ('b')",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    assert_eq!(
        run_select_on(
            "SELECT k FROM s",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap()
        .len(),
        2
    );
    // The duplicate is now caught by the index, as in real TiDB.
    assert!(matches!(
        run_insert_on(
            "INSERT INTO s VALUES ('a')",
            &mut catalog,
            &crate::StmtContext::for_query()
        ),
        Err(DriverError::DuplicateEntry { .. })
    ));
}

/// A primary key that is not a single integer column becomes a clustered
/// COMMON handle: its encoding IS the row key, so rows scan in key order,
/// the columns live in the key rather than the value, and a repeat is a
/// duplicate (Go's IsCommonHandle path in addRecord).
#[test]
fn clustered_common_handle() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE c (k VARCHAR(8) PRIMARY KEY, v BIGINT)",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO c VALUES ('b', 2), ('a', 1), ('c', 3)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();

    // Key order, not insertion order -- the key IS the primary key.
    assert_eq!(
        run_select_on(
            "SELECT k, v FROM c",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap()
        .into_iter()
        .map(|row| datum_text_for_test(&row[0]))
        .collect::<Vec<_>>(),
        vec!["a".to_owned(), "b".to_owned(), "c".to_owned()]
    );
    // The key column round-trips even though the value omits it.
    assert_eq!(
        run_select_on(
            "SELECT v FROM c WHERE k = 'b'",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(2)]]
    );
    // A repeated key is a duplicate.
    assert!(matches!(
        run_insert_on(
            "INSERT INTO c VALUES ('a', 9)",
            &mut catalog,
            &crate::StmtContext::for_query()
        ),
        Err(DriverError::DuplicateEntry { .. })
    ));

    // Writes address the row through its clustered key.
    run_update_on(
        "UPDATE c SET v = 20 WHERE k = 'b'",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    assert_eq!(
        run_select_on(
            "SELECT v FROM c WHERE k = 'b'",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(20)]]
    );
    run_delete_on(
        "DELETE FROM c WHERE k = 'a'",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    assert_eq!(
        run_select_on(
            "SELECT k FROM c",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap()
        .len(),
        2
    );
    // The freed key can be inserted again.
    run_insert_on(
        "INSERT INTO c VALUES ('a', 1)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();

    // A multi-column primary key is a clustered common handle too.
    crate::run_create_table_on(
        "CREATE TABLE m (a BIGINT, b VARCHAR(4), v BIGINT, PRIMARY KEY (a, b))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO m VALUES (1, 'y', 10), (1, 'x', 20), (2, 'a', 30)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    assert_eq!(
        run_select_on(
            "SELECT a, b FROM m",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap()
        .into_iter()
        .map(|row| format!("{:?}/{}", row[0], datum_text_for_test(&row[1])))
        .collect::<Vec<_>>(),
        vec![
            "Int(1)/x".to_owned(),
            "Int(1)/y".to_owned(),
            "Int(2)/a".to_owned()
        ]
    );
    // Only the whole key must be unique; a repeated leading column is fine.
    assert!(matches!(
        run_insert_on(
            "INSERT INTO m VALUES (1, 'x', 99)",
            &mut catalog,
            &crate::StmtContext::for_query()
        ),
        Err(DriverError::DuplicateEntry { .. })
    ));
    run_insert_on(
        "INSERT INTO m VALUES (1, 'z', 40)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();

    // A secondary index over a clustered table stores the common handle
    // and still resolves to its row.
    crate::run_create_table_on(
        "CREATE TABLE s (k VARCHAR(4) PRIMARY KEY, tag BIGINT, KEY tag_idx (tag))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO s VALUES ('p', 1), ('q', 2)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    assert_eq!(
        run_select_on(
            "SELECT k FROM s WHERE tag >= 2",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap()
        .into_iter()
        .map(|row| datum_text_for_test(&row[0]))
        .collect::<Vec<_>>(),
        vec!["q".to_owned()]
    );
}

/// A handle stores its columns FLAT -- a `bit` as an integer, a `datetime` as
/// its packed `uint64` -- so reading one back without Go's `Unflatten` hands
/// the engine a datum whose kind disagrees with the column's declared type. Go
/// `tablecodec.DecodeHandleToDatumMap` unflattens every handle column, and
/// `decodeHandleToDatum` reads an int handle as UNSIGNED when the column says
/// so; this is the assertion that both happen here.
///
/// Captured from real TiDB with `difftests/gorun`:
///
/// ```text
/// CREATE TABLE `tb` (`a` bit(1) NOT NULL, PRIMARY KEY (`a`));
/// insert into tb value(1), (0);
/// select a+0 from tb;                     -> 0; 1
/// create table tdt (a datetime not null, b int, primary key(a));
/// insert into tdt values ('2020-01-02 03:04:05', 9);
/// select a, b from tdt;                   -> 2020-01-02 03:04:05|9
/// create table tu (a bigint unsigned not null, b int, primary key(a));
/// insert into tu values (18446744073709551615, 5);
/// select a, b from tu;                    -> 18446744073709551615|5
/// ```
#[test]
fn a_handle_column_reads_back_in_its_own_type() {
    let mut catalog = Catalog::default();
    let ctx = crate::StmtContext::for_query();

    // A `bit` primary key: the handle holds the integer, the column is BIT,
    // and the datum must be a bit value -- appending an integer datum to the
    // (variable-length) BIT chunk column used to panic outright.
    crate::run_create_table_on(
        "CREATE TABLE tb (a BIT(1) NOT NULL, PRIMARY KEY (a))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on("INSERT INTO tb VALUES (1), (0)", &mut catalog, &ctx).unwrap();
    let rows = run_select_on("SELECT a FROM tb", &catalog, &ctx).unwrap();
    assert!(
        rows.iter().all(|row| matches!(row[0], Datum::Bit(_))),
        "a BIT primary key reads back as a bit value, got {rows:?}"
    );
    // UNSIGNED, because a `BIT` column carries `UnsignedFlag` (Go
    // `processColumnFlags`) and Go's `arithmeticPlusFunctionClass.getFunction`
    // adds that flag to the RESULT when either argument has it:
    //
    //     if mysql.HasUnsignedFlag(args[0].GetType(...).GetFlag()) ||
    //        mysql.HasUnsignedFlag(args[1].GetType(...).GetFlag()) {
    //         bf.tp.AddFlag(mysql.UnsignedFlag)
    //     }
    //
    // This read `Int` until the column builder ported `processColumnFlags`;
    // the captured `0; 1` above is the same either way, since it is the VALUE.
    assert_eq!(
        run_select_on("SELECT a + 0 FROM tb", &catalog, &ctx).unwrap(),
        vec![vec![Datum::UInt(0)], vec![Datum::UInt(1)]]
    );

    // A `datetime` primary key: the handle holds the PACKED uint64, which is
    // not the value. Without unflattening this answered a bare integer.
    crate::run_create_table_on(
        "CREATE TABLE tdt (a DATETIME NOT NULL, b INT, PRIMARY KEY (a))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO tdt VALUES ('2020-01-02 03:04:05', 9)",
        &mut catalog,
        &ctx,
    )
    .unwrap();
    let rows = run_select_on("SELECT a, b FROM tdt", &catalog, &ctx).unwrap();
    assert_eq!(rows.len(), 1);
    let Datum::Time(time) = &rows[0][0] else {
        panic!("a DATETIME primary key reads back as a time, got {rows:?}");
    };
    assert_eq!(time.to_string(), "2020-01-02 03:04:05");

    // An `unsigned bigint` primary key past i64::MAX: Go's
    // `decodeHandleToDatum` reads an int handle as an UNSIGNED datum when the
    // column says so, which is the difference between the recorded value and
    // -1.
    crate::run_create_table_on(
        "CREATE TABLE tu (a BIGINT UNSIGNED NOT NULL, b INT, PRIMARY KEY (a))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO tu VALUES (18446744073709551615, 5)",
        &mut catalog,
        &ctx,
    )
    .unwrap();
    assert_eq!(
        run_select_on("SELECT a, b FROM tu", &catalog, &ctx).unwrap(),
        vec![vec![Datum::UInt(u64::MAX), Datum::Int(5)]]
    );
}

/// Builds `sql` under one `@@tidb_enable_clustered_index` mode, exactly the
/// way the session's dispatch path does.
fn create_with_mode(
    sql: &str,
    mode: tidb_vardef::modes::ClusteredIndexDefMode,
    catalog: &mut Catalog,
) -> Result<bool, DriverError> {
    crate::ddl::run_create_table_in(
        sql,
        catalog,
        crate::driver::DEFAULT_DATABASE,
        crate::CreateTableSettings {
            clustered_index_mode: mode,
            ..crate::CreateTableSettings::default()
        },
        &crate::StmtContext::default(),
    )
}

/// The layout a built table actually has, read back off the catalog entry:
/// `(pk_is_handle, common handle columns)`.
fn layout_of(catalog: &Catalog, name: &str) -> (bool, Vec<usize>) {
    let Some(crate::driver::TableEntry::Kv(table)) = catalog.get_table_for_test(name) else {
        panic!("{name} was not built as a KV table");
    };
    (
        table.pk_handle_offset().is_some(),
        table.common_handle_offsets().to_vec(),
    )
}

/// Whether the built table carries a `PRIMARY` index -- which is what a
/// NON-clustered primary key becomes, and what a clustered one never is
/// (its encoding IS the row key).
fn has_primary_index(catalog: &Catalog, name: &str) -> bool {
    let Some(crate::driver::TableEntry::Kv(table)) = catalog.get_table_for_test(name) else {
        panic!("{name} was not built as a KV table");
    };
    table
        .indexes()
        .iter()
        .any(|index| index.name.eq_ignore_ascii_case("PRIMARY"))
}

/// Go `ShouldBuildClusteredIndex` (`pkg/ddl/create_table.go:1754`) crossed
/// with the `isSingleIntPK` split that follows it at line 1418, over the
/// three primary-key shapes and the three modes of
/// `@@tidb_enable_clustered_index`.
///
/// The variable is an ENUM, not a boolean: `ClusteredIndexDefModeIntOnly` is
/// Go's ZERO value but NOT the registered default -- `sysvar.go:2782`
/// registers the variable with `Value: vardef.On` and warns that `INT_ONLY`
/// is deprecated, and `vardef.DefTiDBEnableClusteredIndex` is
/// `ClusteredIndexDefModeOn`.
///
/// UNRUN when written: no freshly compiled binary could be executed on the
/// machine this landed from, so every expectation below is read off the Go
/// source cited above rather than off a run. They are ordinary tests and the
/// next runnable gate executes them with everything else.
#[test]
fn the_clustered_index_mode_decides_the_handle() {
    use tidb_vardef::modes::ClusteredIndexDefMode as Mode;
    // (mode, DDL, pk_is_handle, common handle columns)
    let cases: &[(Mode, &str, bool, &[usize])] = &[
        // ON clusters every primary key it can: an integer one onto the int
        // handle, a string or composite one onto a common handle.
        (Mode::ON, "CREATE TABLE m (a BIGINT PRIMARY KEY)", true, &[]),
        (
            Mode::ON,
            "CREATE TABLE m (a VARCHAR(10) PRIMARY KEY)",
            false,
            &[0],
        ),
        (
            Mode::ON,
            "CREATE TABLE m (a BIGINT, b BIGINT, PRIMARY KEY (a, b))",
            false,
            &[0, 1],
        ),
        // INT_ONLY clusters ONLY the single integer key. This is the mode
        // whose name is the whole rule.
        (
            Mode::INT_ONLY,
            "CREATE TABLE m (a BIGINT PRIMARY KEY)",
            true,
            &[],
        ),
        (
            Mode::INT_ONLY,
            "CREATE TABLE m (a VARCHAR(10) PRIMARY KEY)",
            false,
            &[],
        ),
        (
            Mode::INT_ONLY,
            "CREATE TABLE m (a BIGINT, b BIGINT, PRIMARY KEY (a, b))",
            false,
            &[],
        ),
        // OFF clusters nothing: every table gets the implicit `_tidb_rowid`
        // handle and the primary key is an ordinary unique index.
        (
            Mode::OFF,
            "CREATE TABLE m (a BIGINT PRIMARY KEY)",
            false,
            &[],
        ),
        (
            Mode::OFF,
            "CREATE TABLE m (a VARCHAR(10) PRIMARY KEY)",
            false,
            &[],
        ),
        (
            Mode::OFF,
            "CREATE TABLE m (a BIGINT, b BIGINT, PRIMARY KEY (a, b))",
            false,
            &[],
        ),
    ];
    for (mode, ddl, pk_is_handle, common) in cases {
        let mut catalog = Catalog::default();
        create_with_mode(ddl, *mode, &mut catalog)
            .unwrap_or_else(|_| panic!("{ddl} under {mode:?}"));
        assert_eq!(
            layout_of(&catalog, "m"),
            (*pk_is_handle, common.to_vec()),
            "{ddl} under {mode:?}"
        );
        // A clustered key is the row key and has no index; a non-clustered
        // one is enforced by a `PRIMARY` unique index. Exactly one holds.
        let clustered = *pk_is_handle || !common.is_empty();
        assert_eq!(
            has_primary_index(&catalog, "m"),
            !clustered,
            "{ddl} under {mode:?}"
        );
    }
}

/// An explicit `CLUSTERED`/`NONCLUSTERED` clause OVERRIDES the variable in
/// both directions, and Go applies no type-eligibility test to it: it neither
/// refuses nor silently downgrades a `CLUSTERED` string or composite key, and
/// it honours `NONCLUSTERED` on an integer key.
///
/// Go `ShouldBuildClusteredIndex` returns `opt.PrimaryKeyTp ==
/// ast.PrimaryKeyTypeClustered` for ANY explicit clause, without consulting
/// the mode or the column type (`pkg/ddl/create_table.go:1765`).
///
/// UNRUN when written; see `the_clustered_index_mode_decides_the_handle`.
#[test]
fn an_explicit_clause_overrides_the_variable() {
    use tidb_vardef::modes::ClusteredIndexDefMode as Mode;
    let cases: &[(Mode, &str, bool, &[usize])] = &[
        // NONCLUSTERED against a mode that would have clustered.
        (
            Mode::ON,
            "CREATE TABLE m (a BIGINT PRIMARY KEY NONCLUSTERED)",
            false,
            &[],
        ),
        (
            Mode::ON,
            "CREATE TABLE m (a VARCHAR(10), PRIMARY KEY (a) NONCLUSTERED)",
            false,
            &[],
        ),
        (
            Mode::INT_ONLY,
            "CREATE TABLE m (a BIGINT, PRIMARY KEY (a) NONCLUSTERED)",
            false,
            &[],
        ),
        // CLUSTERED against a mode that would not have clustered -- including
        // on a string and on a composite key, which become COMMON handles
        // rather than being refused or downgraded.
        (
            Mode::OFF,
            "CREATE TABLE m (a BIGINT PRIMARY KEY CLUSTERED)",
            true,
            &[],
        ),
        (
            Mode::OFF,
            "CREATE TABLE m (a VARCHAR(10), PRIMARY KEY (a) CLUSTERED)",
            false,
            &[0],
        ),
        (
            Mode::INT_ONLY,
            "CREATE TABLE m (a BIGINT, b VARCHAR(4), PRIMARY KEY (a, b) CLUSTERED)",
            false,
            &[0, 1],
        ),
    ];
    for (mode, ddl, pk_is_handle, common) in cases {
        let mut catalog = Catalog::default();
        create_with_mode(ddl, *mode, &mut catalog)
            .unwrap_or_else(|_| panic!("{ddl} under {mode:?}"));
        assert_eq!(
            layout_of(&catalog, "m"),
            (*pk_is_handle, common.to_vec()),
            "{ddl} under {mode:?}"
        );
    }
}

/// Go `BuildTableInfo` raises `ErrUnsupportedClusteredSecondaryKey` --
/// "CLUSTERED/NONCLUSTERED keyword is only supported for primary key"
/// (`pkg/util/dbterror/ddl_terror.go:383`) -- for the keyword on any
/// constraint that is not the primary key. It is a REFUSAL, not a warning.
///
/// UNRUN when written; see `the_clustered_index_mode_decides_the_handle`.
#[test]
fn the_keyword_is_refused_on_a_secondary_key() {
    use tidb_vardef::modes::ClusteredIndexDefMode as Mode;
    for ddl in [
        "CREATE TABLE m (a BIGINT, UNIQUE KEY u (a) CLUSTERED)",
        "CREATE TABLE m (a BIGINT, UNIQUE KEY u (a) NONCLUSTERED)",
        "CREATE TABLE m (a BIGINT, KEY k (a) CLUSTERED)",
    ] {
        let mut catalog = Catalog::default();
        assert!(
            create_with_mode(ddl, Mode::ON, &mut catalog).is_err(),
            "{ddl} should be refused"
        );
    }
}
