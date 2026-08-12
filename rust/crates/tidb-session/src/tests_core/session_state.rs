//! Session state a statement reads or changes: system variables, `sql_mode`,
//! the noop-function gate, `sql_select_limit`, the `SET_VAR` hint, and
//! prepared-statement parameters -- Go `pkg/sessionctx/variable`.

use crate::tests_support::*;
use crate::*;

/// SET and the variable reads a connecting client performs.
#[test]
fn session_variables() {
    let mut session = Session::new();

    // A stock client's opening statements.
    assert_eq!(session.apply_set("SET NAMES utf8mb4").unwrap(), Some(()));
    assert_eq!(
        session.vars().get_system("character_set_client").unwrap(),
        "utf8mb4"
    );
    assert_eq!(session.apply_set("SET autocommit = 0").unwrap(), Some(()));
    // Go's checkBoolSystemVar canonicalizes 0/1 to OFF/ON.
    assert_eq!(session.vars().get_system("autocommit").unwrap(), "OFF");

    // Reading variables back through a query. The STORED form is `OFF`, but
    // Go's `GetNativeValType` gives a `TypeBool` variable the integer domain,
    // so the query reports `0` (confirmed against Go: `SELECT @@autocommit`
    // is `1` on a fresh session, never `ON`).
    assert_eq!(
        scalar_text(&mut session, "SELECT @@autocommit"),
        Some("0".to_owned())
    );
    let comment = scalar_text(&mut session, "SELECT @@version_comment").unwrap();
    assert!(
        comment.starts_with("TiDB Server (Apache License 2.0)"),
        "{comment}"
    );

    // DEFAULT restores the registry default.
    session.apply_set("SET autocommit = DEFAULT").unwrap();
    assert_eq!(session.vars().get_system("autocommit").unwrap(), "ON");

    // An unknown system variable is Go's 1193, on read and on write.
    assert!(matches!(
        session.apply_set("SET nonexistent_variable = 1"),
        Err(DriverError::Var(
            tidb_executor::VarErrorKind::UnknownSystemVariable(_)
        ))
    ));
    assert!(matches!(
        session.run("SELECT @@nonexistent_variable"),
        Err(DriverError::Var(
            tidb_executor::VarErrorKind::UnknownSystemVariable(_)
        ))
    ));
    // A read-only variable cannot be set.
    assert!(matches!(
        session.apply_set("SET version = '1'"),
        Err(DriverError::Var(
            tidb_executor::VarErrorKind::ReadOnlyVariable(_)
        ))
    ));

    // User variables: unset reads as NULL, never an error.
    assert_eq!(
        session.run("SELECT @nope").unwrap(),
        StmtResult::Rows(vec![vec![Datum::Null]])
    );
    session.apply_set("SET @x = 41 + 1").unwrap();
    assert_eq!(
        scalar_text(&mut session, "SELECT @x"),
        Some("42".to_owned())
    );

    // A non-SET statement is not claimed by the hook.
    assert_eq!(session.apply_set("SELECT 1").unwrap(), None);
}

#[test]
fn version_comment_uses_the_server_identity_snapshot() {
    let mut session = Session::new();
    let info = tidb_util::versioninfo::VersionInfo::build_default()
        .with_configured_edition("Starter")
        .with_configured_versions("v9.0.0", "8.0.11-TiDB-v9.0.0")
        .with_runtime_environment(true, "tikv", "Classic", None);
    let expected_tidb_info = tidb_util::printer::get_tidb_info(&info);
    let expected_server_version = info.server_version.clone();
    session.set_version_info(info);

    assert_eq!(
        scalar_text(&mut session, "SELECT @@version_comment"),
        Some("TiDB Server (Apache License 2.0) Starter Edition, MySQL 8.0 compatible".to_owned())
    );
    assert_eq!(
        scalar_text(&mut session, "SELECT TIDB_VERSION()"),
        Some(expected_tidb_info)
    );
    assert_eq!(
        scalar_text(&mut session, "SELECT VERSION()"),
        Some(expected_server_version)
    );
    assert!(matches!(
        session.apply_set("SET version_comment = 'changed'"),
        Err(DriverError::Var(
            tidb_executor::VarErrorKind::ReadOnlyVariable(_)
        ))
    ));
}

/// Hash-join versions are source string variables with a closed value domain:
/// casing is accepted and retained, while every other spelling is refused by
/// the variable-specific validation closure.
#[test]
fn hash_join_versions_accept_only_legacy_or_optimized() {
    let mut session = Session::new();

    for (name, default) in [
        ("tidb_hash_join_version", "optimized"),
        ("tiflash_hash_join_version", "legacy"),
    ] {
        assert_eq!(
            scalar_text(&mut session, &format!("SELECT @@{name}")),
            Some(default.to_owned())
        );

        for value in ["Legacy", "OptimiZed"] {
            session
                .apply_set(&format!("SET {name} = '{value}'"))
                .unwrap();
            assert_eq!(
                scalar_text(&mut session, &format!("SELECT @@{name}")),
                Some(value.to_owned())
            );
        }

        for value in ["invalid", "v2", "optimized "] {
            let error = session
                .apply_set(&format!("SET {name} = '{value}'"))
                .unwrap_err()
                .to_mysql_error();
            assert_eq!(error.code, 1105);
            assert_eq!(error.state, *b"HY000");
            assert_eq!(
                error.message,
                format!("incorrect value: `{value}`. {name} options: legacy, optimized")
            );
            assert_eq!(
                scalar_text(&mut session, &format!("SELECT @@{name}")),
                Some("OptimiZed".to_owned()),
                "a refused SET must leave the previous value intact"
            );
        }
    }
}

/// `sql_mode` is normalized at SET time, so every reader afterwards sees the
/// expanded, canonical set rather than the shorthand the user typed.
///
/// Captured from TiDB (`SET sql_mode='TRADITIONAL'; SELECT @@sql_mode`): the
/// combination expands to its member modes AND keeps its own name at the end.
/// Without this normalization `SET sql_mode='TRADITIONAL'` left the literal
/// string `TRADITIONAL` stored, and every reader looking for
/// `STRICT_TRANS_TABLES` silently found a NON-strict session -- an over-long
/// INSERT then truncated and stored a wrong value instead of failing.
#[test]
fn sql_mode_is_normalized_when_it_is_set() {
    let mut session = Session::new();
    session.run("CREATE TABLE t (c VARCHAR(3))").unwrap();

    session.apply_set("SET sql_mode = 'TRADITIONAL'").unwrap();
    assert_eq!(
        session.run("SELECT @@sql_mode").unwrap(),
        StmtResult::Rows(vec![vec![Datum::new_string(
            "STRICT_TRANS_TABLES,STRICT_ALL_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,\
             ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION,TRADITIONAL"
        )]])
    );
    // Captured: `[types:1406]Data too long for column 'c' at row 1`, and the
    // row is NOT stored.
    assert!(session.run("INSERT INTO t VALUES ('abcdef')").is_err());
    assert_eq!(
        session.run("SELECT c FROM t").unwrap(),
        StmtResult::Rows(vec![])
    );

    // Captured: ANSI expands to its five member modes plus its own name, which
    // is how ONLY_FULL_GROUP_BY comes along with it.
    session.apply_set("SET sql_mode = 'ansi'").unwrap();
    assert_eq!(
        session.run("SELECT @@sql_mode").unwrap(),
        StmtResult::Rows(vec![vec![Datum::new_string(
            "REAL_AS_FLOAT,PIPES_AS_CONCAT,ANSI_QUOTES,IGNORE_SPACE,ONLY_FULL_GROUP_BY,ANSI"
        )]])
    );

    // A non-combination mode is only uppercased, and duplicates collapse.
    session
        .apply_set("SET sql_mode = 'ansi_quotes,ANSI_QUOTES'")
        .unwrap();
    assert_eq!(
        session.run("SELECT @@sql_mode").unwrap(),
        StmtResult::Rows(vec![vec![Datum::new_string("ANSI_QUOTES")]])
    );

    // Captured: an unknown mode and a numeric bitmask are both 1231, and the
    // message names the offending TOKEN rather than the whole assignment.
    for (value, token) in [
        ("NO_SUCH_MODE", "NO_SUCH_MODE"),
        ("2097152", "2097152"),
        ("ANSI_QUOTES,bogus", "BOGUS"),
        // Captured: only TRAILING spaces are trimmed, so a leading one makes
        // the whole token invalid.
        (" STRICT_TRANS_TABLES", " STRICT_TRANS_TABLES"),
    ] {
        match session.apply_set(&format!("SET sql_mode = '{value}'")) {
            Err(DriverError::Var(tidb_executor::VarErrorKind::WrongValueForVar(
                name,
                reported,
            ))) => {
                assert_eq!(name, "sql_mode");
                assert_eq!(reported, token, "for {value}");
            }
            other => panic!("{value} should be rejected, got {other:?}"),
        }
    }
    // A rejected SET left the previous value in place.
    assert_eq!(
        session.run("SELECT @@sql_mode").unwrap(),
        StmtResult::Rows(vec![vec![Datum::new_string("ANSI_QUOTES")]])
    );
}

/// The clauses TiDB parses but only implements as no-ops, checked
/// against captured TiDB output with `tidb_enable_noop_functions` at its
/// `OFF` default.
///
/// NOT PORTED from Go's own suites: `tidb_enable_shared_lock_promotion`
/// (no locking layer here to promote to) and the `READ ONLY` /
/// `OFFLINE MODE` / `sql_auto_is_null` gates, which belong to variable
/// and transaction surfaces this tier does not have.
#[test]
fn noop_function_gate() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (a BIGINT PRIMARY KEY, b BIGINT)")
        .unwrap();
    session
        .run("INSERT INTO t VALUES (1, 10), (2, 20)")
        .unwrap();

    // Captured: FOR UPDATE runs and returns the rows.
    assert_eq!(
        session
            .run("SELECT b FROM t WHERE a = 1 FOR UPDATE")
            .unwrap(),
        StmtResult::Rows(vec![vec![Datum::Int(10)]])
    );
    // Its waiting options only shape a lock this tier does not take.
    session.run("SELECT b FROM t FOR UPDATE NOWAIT").unwrap();
    session.run("SELECT b FROM t FOR UPDATE OF t").unwrap();

    // Captured: the shared lock and SQL_CALC_FOUND_ROWS are 1235.
    for sql in [
        "SELECT b FROM t FOR SHARE",
        "SELECT b FROM t LOCK IN SHARE MODE",
        "SELECT SQL_CALC_FOUND_ROWS b FROM t LIMIT 1",
        "SELECT b FROM t GROUP BY b DESC",
    ] {
        assert!(
            matches!(session.run(sql), Err(DriverError::FunctionsNoopImpl(_))),
            "expected a noop-function error from {sql}"
        );
    }
    // An explicit ASC is written too, so it is gated the same way.
    assert!(matches!(
        session.run("SELECT b FROM t GROUP BY b ASC"),
        Err(DriverError::FunctionsNoopImpl("GROUP BY expr ASC|DESC"))
    ));
    // A GROUP BY with no direction is not.
    session.run("SELECT b FROM t GROUP BY b").unwrap();

    // The gate reaches a subquery, a derived table and a set operation.
    assert!(matches!(
        session.run("SELECT b FROM t WHERE a IN (SELECT a FROM t LOCK IN SHARE MODE)"),
        Err(DriverError::FunctionsNoopImpl(_))
    ));
    assert!(matches!(
        session.run("SELECT x.b FROM (SELECT b FROM t LOCK IN SHARE MODE) x"),
        Err(DriverError::FunctionsNoopImpl(_))
    ));
    assert!(matches!(
        session.run("SELECT b FROM t UNION SELECT a FROM t LOCK IN SHARE MODE"),
        Err(DriverError::FunctionsNoopImpl(_))
    ));

    // ON: the clause is accepted and does nothing, with no warning.
    session
        .apply_set("SET tidb_enable_noop_functions = 'ON'")
        .unwrap();
    session.run("SELECT b FROM t LOCK IN SHARE MODE").unwrap();
    assert!(session.warnings().is_empty());

    // WARN: accepted, with the same message as a warning.
    session
        .apply_set("SET tidb_enable_noop_functions = 'WARN'")
        .unwrap();
    session.run("SELECT b FROM t LOCK IN SHARE MODE").unwrap();
    assert_eq!(session.warnings().len(), 1);
    assert_eq!(session.warnings()[0].code, 1235);
    assert!(session.warnings()[0].message.contains("LOCK IN SHARE MODE"));
    // The warnings belong to the last statement only.
    session.run("SELECT b FROM t").unwrap();
    assert!(session.warnings().is_empty());

    // INTO OUTFILE writes a server-side file, which this tier cannot do,
    // so it is refused rather than answered with rows.
    session
        .apply_set("SET tidb_enable_noop_functions = 'OFF'")
        .unwrap();
    assert!(matches!(
        session.run("SELECT b FROM t INTO OUTFILE '/tmp/x'"),
        Err(DriverError::Unsupported(_))
    ));
}

/// Go `varsutil.go:checkReadOnly`: turning one of `noop.go`'s read-only
/// variables ON needs `tidb_enable_noop_functions`, because the server does
/// not actually stop writes.
///
/// Captured from TiDB (`testkit`, `pkg/executor`) at the `OFF` default:
///
/// ```text
/// set transaction_read_only = 1  ERR errno=1235 "[variable:1235]function READ ONLY has only
///                                noop implementation in tidb now, use
///                                tidb_enable_noop_functions to enable these functions"
/// set tx_read_only = 1           ERR errno=1235 (same message)
/// set tidb_enable_noop_functions = warn   OK
/// set tx_read_only = 1                    OK
/// select @@tx_read_only, @@transaction_read_only   [[1 1]]
/// ```
#[test]
fn read_only_noop_variables_need_the_noop_gate() {
    let mut session = Session::new();
    /// The one row a `SELECT @@...` returns, as text.
    fn read(session: &mut Session, sql: &str) -> Vec<String> {
        match session.run(sql).unwrap() {
            StmtResult::Rows(rows) => rows[0].iter().map(|d| format!("{d:?}")).collect(),
            other => panic!("expected rows from {sql}, got {other:?}"),
        }
    }

    // The default is OFF for both spellings, and neither can be turned ON.
    assert_eq!(
        read(
            &mut session,
            "SELECT @@tx_read_only, @@transaction_read_only"
        ),
        ["Int(0)", "Int(0)"]
    );
    for sql in ["SET tx_read_only = 1", "SET transaction_read_only = 1"] {
        assert!(
            matches!(
                session.run(sql),
                Err(DriverError::FunctionsNoopImpl("READ ONLY"))
            ),
            "expected 1235 from {sql}"
        );
    }
    // The refusal leaves the value alone -- including the alias's copy.
    assert_eq!(
        read(
            &mut session,
            "SELECT @@tx_read_only, @@transaction_read_only"
        ),
        ["Int(0)", "Int(0)"]
    );

    // Turning one OFF is never gated.
    session.run("SET tx_read_only = 0").unwrap();

    // WARN accepts the value and reports the same message as a warning.
    session
        .apply_set("SET tidb_enable_noop_functions = 'WARN'")
        .unwrap();
    session.run("SET tx_read_only = 1").unwrap();
    assert_eq!(session.warnings().len(), 1);
    assert_eq!(session.warnings()[0].code, 1235);
    assert!(session.warnings()[0].message.contains("READ ONLY"));
    assert_eq!(
        read(
            &mut session,
            "SELECT @@tx_read_only, @@transaction_read_only"
        ),
        ["Int(1)", "Int(1)"]
    );

    // ON accepts it silently, and `SET TRANSACTION READ ONLY` -- the same
    // assignment under another spelling -- goes through the same gate.
    session
        .apply_set("SET tidb_enable_noop_functions = 'ON'")
        .unwrap();
    session.run("SET TRANSACTION READ WRITE").unwrap();
    assert_eq!(read(&mut session, "SELECT @@tx_read_only"), ["Int(0)"]);
    session.run("SET TRANSACTION READ ONLY").unwrap();
    assert!(session.warnings().is_empty());
    assert_eq!(read(&mut session, "SELECT @@tx_read_only"), ["Int(1)"]);

    // Back at OFF, `SET TRANSACTION READ ONLY` is refused too: it is one and
    // the same `tx_read_only = 1`.
    session.run("SET TRANSACTION READ WRITE").unwrap();
    session
        .apply_set("SET tidb_enable_noop_functions = 'OFF'")
        .unwrap();
    assert!(matches!(
        session.run("SET TRANSACTION READ ONLY"),
        Err(DriverError::FunctionsNoopImpl("READ ONLY"))
    ));
}

/// Go `preprocess.go:TryAddExtraLimit`: `sql_select_limit` caps a top-level
/// SELECT or set operation that writes no LIMIT of its own, and nothing else.
///
/// Captured from TiDB (`testkit`, `pkg/executor`) with three rows 1,2,3 in
/// `sslt` and `set @@sql_select_limit = 2`:
///
/// ```text
/// select a from sslt order by a                    [[1] [2]]
/// select a from sslt order by a limit 3            [[1] [2] [3]]
/// select a from sslt order by a limit 1            [[1]]
/// select a from sslt order by a limit 1, 5         [[2] [3]]
/// select 1 union all select 2 union all select 3   two rows
/// select (select count(*) from sslt)               [[3]]
/// select a from (select a from sslt order by a) d order by a   [[1] [2]]
/// select count(*) from sslt                        [[3]]
/// select * from sslt where a in (select a from sslt)           [[1] [2]]
/// set @@sql_select_limit = 0   -> select a from sslt order by a  []
/// set @@sql_select_limit = default -> [[1] [2] [3]]
/// ```
#[test]
fn sql_select_limit_caps_a_statement_that_wrote_no_limit() {
    let mut session = Session::new();
    session.run("CREATE TABLE sslt (a INT)").unwrap();
    session
        .run("INSERT INTO sslt VALUES (1), (2), (3)")
        .unwrap();
    // The default is MaxUint64, which caps nothing.
    assert_eq!(
        row_text(session.run("SELECT a FROM sslt ORDER BY a")),
        [["1"], ["2"], ["3"]]
    );

    session.apply_set("SET @@sql_select_limit = 2").unwrap();
    assert_eq!(
        row_text(session.run("SELECT a FROM sslt ORDER BY a")),
        [["1"], ["2"]]
    );
    // A statement that writes its OWN limit is left alone, even one asking
    // for more rows than the cap.
    assert_eq!(
        row_text(session.run("SELECT a FROM sslt ORDER BY a LIMIT 3")),
        [["1"], ["2"], ["3"]]
    );
    assert_eq!(
        row_text(session.run("SELECT a FROM sslt ORDER BY a LIMIT 1")),
        [["1"]]
    );
    assert_eq!(
        row_text(session.run("SELECT a FROM sslt ORDER BY a LIMIT 1, 5")),
        [["2"], ["3"]]
    );
    // A set operation is capped at the statement level.
    assert_eq!(
        row_text(session.run("SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3")).len(),
        2
    );

    // Only the TOP-LEVEL statement is capped: a scalar subquery, a derived
    // table and an IN subquery all still see every row.
    assert_eq!(
        row_text(session.run("SELECT (SELECT COUNT(*) FROM sslt)")),
        [["3"]]
    );
    assert_eq!(
        row_text(session.run("SELECT a FROM (SELECT a FROM sslt ORDER BY a) d ORDER BY a")),
        [["1"], ["2"]]
    );
    assert_eq!(row_text(session.run("SELECT COUNT(*) FROM sslt")), [["3"]]);
    assert_eq!(
        row_text(session.run("SELECT a FROM sslt WHERE a IN (SELECT a FROM sslt) ORDER BY a")),
        [["1"], ["2"]]
    );

    // Zero is a cap like any other, and DEFAULT lifts it.
    session.apply_set("SET @@sql_select_limit = 0").unwrap();
    assert!(row_text(session.run("SELECT a FROM sslt ORDER BY a")).is_empty());
    session
        .apply_set("SET @@sql_select_limit = DEFAULT")
        .unwrap();
    assert_eq!(
        row_text(session.run("SELECT a FROM sslt ORDER BY a")),
        [["1"], ["2"], ["3"]]
    );
}

/// Go `hint.go`'s `set_var` arm and `optimize.go`'s application of
/// `StmtHints.SetVars`: a `SET_VAR` hint overlays a session variable for ONE
/// statement, first occurrence wins, and the overlay is put back whether the
/// statement succeeded or failed.
///
/// Captured from TiDB (`testkit`, `pkg/executor`):
///
/// ```text
/// select @@sql_safe_updates                                        [[0]]
/// select /*+ SET_VAR(sql_safe_updates=1) */ @@sql_safe_updates      [[1]]
/// select @@sql_safe_updates                                        [[0]]
/// select /*+ SET_VAR(sql_safe_updates=1) SET_VAR(sql_safe_updates=0) */
///        @@sql_safe_updates                                        [[1]]
/// set sql_safe_updates=1
/// select /*+ SET_VAR(sql_safe_updates=0) */ @@sql_safe_updates      [[0]]
/// select @@sql_safe_updates                                        [[1]]
/// select /*+ SET_VAR(sql_safe_updates=0) */ no_such_column
///     ERR errno=1054 "[planner:1054]Unknown column 'no_such_column' in
///                     'field list'"
/// select @@sql_safe_updates                                        [[1]]
/// select /*+ SET_VAR(no_such_variable=1) */ @@sql_safe_updates      [[1]]
/// select /*+ SET_VAR(sql_safe_updates=99) */ @@sql_safe_updates     [[1]]
/// set sql_safe_updates=0
/// select /*+ SET_VAR(max_execution_time=123) */ @@max_execution_time [[123]]
/// select @@max_execution_time                                      [[0]]
/// select /*+ SET_VAR(sql_select_limit=1) */ 1 union all select 2    [[1]]
/// ```
#[test]
fn set_var_hint_overlays_one_statement() {
    let mut session = Session::new();
    assert_eq!(row_text(session.run("SELECT @@sql_safe_updates")), [["0"]]);
    assert_eq!(
        row_text(session.run("SELECT /*+ SET_VAR(sql_safe_updates=1) */ @@sql_safe_updates")),
        [["1"]]
    );
    // The overlay does not outlive the statement.
    assert_eq!(row_text(session.run("SELECT @@sql_safe_updates")), [["0"]]);

    // Two hints for one name: the FIRST wins.
    assert_eq!(
        row_text(session.run(
            "SELECT /*+ SET_VAR(sql_safe_updates=1) SET_VAR(sql_safe_updates=0) */ \
             @@sql_safe_updates"
        )),
        [["1"]]
    );

    // The overlay is a write, not a floor: it can also turn a value OFF.
    session.apply_set("SET sql_safe_updates = 1").unwrap();
    assert_eq!(
        row_text(session.run("SELECT /*+ SET_VAR(sql_safe_updates=0) */ @@sql_safe_updates")),
        [["0"]]
    );
    assert_eq!(row_text(session.run("SELECT @@sql_safe_updates")), [["1"]]);

    // A statement that FAILS restores it too. Go reports 1054 "Unknown column
    // 'no_such_column' in 'field list'" here; this tier still answers a
    // FROM-less unresolved column with 1105, a SEPARATE gap that predates this
    // overlay -- what is asserted is the failure and the restore.
    assert!(session
        .run("SELECT /*+ SET_VAR(sql_safe_updates=0) */ no_such_column")
        .is_err());
    assert_eq!(row_text(session.run("SELECT @@sql_safe_updates")), [["1"]]);

    // An unknown name and a value the registry rejects are both ignored
    // rather than failing the statement.
    assert_eq!(
        row_text(session.run("SELECT /*+ SET_VAR(no_such_variable=1) */ @@sql_safe_updates")),
        [["1"]]
    );
    assert_eq!(
        row_text(session.run("SELECT /*+ SET_VAR(sql_safe_updates=99) */ @@sql_safe_updates")),
        [["1"]]
    );
    assert_eq!(row_text(session.run("SELECT @@sql_safe_updates")), [["1"]]);

    // Any hint-writable variable, not just the boolean: a numeric one
    // restores to its default rather than to the default's text.
    session.apply_set("SET sql_safe_updates = 0").unwrap();
    assert_eq!(
        row_text(session.run("SELECT /*+ SET_VAR(max_execution_time=123) */ @@max_execution_time")),
        [["123"]]
    );
    assert_eq!(
        row_text(session.run("SELECT @@max_execution_time")),
        [["0"]]
    );

    // A set operation's hints are its FIRST term's, and they reach the
    // statement-level `sql_select_limit` cap.
    assert_eq!(
        row_text(session.run("SELECT /*+ SET_VAR(sql_select_limit=1) */ 1 UNION ALL SELECT 2")),
        [["1"]]
    );
}

/// Prepared-statement parameters: the marker count a PREPARE reports and
/// the values an EXECUTE binds.
///
/// This is the session half of the binary protocol -- what a JDBC or Go
/// driver client needs to run anything at all. The wire half wires
/// `COM_STMT_PREPARE`/`EXECUTE` to it.
#[test]
fn prepared_statement_parameters() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (a BIGINT PRIMARY KEY, b VARCHAR(20), c BIGINT)")
        .unwrap();

    // The marker count is what PREPARE reports.
    assert_eq!(
        session
            .parameter_count("SELECT a FROM t WHERE a = ?")
            .unwrap(),
        1
    );
    assert_eq!(
        session
            .parameter_count("INSERT INTO t (a,b,c) VALUES (?,?,?)")
            .unwrap(),
        3
    );
    assert_eq!(session.parameter_count("SELECT 1").unwrap(), 0);
    assert_eq!(
        session
            .parameter_count("SELECT a FROM t WHERE b LIKE ? AND c BETWEEN ? AND ?")
            .unwrap(),
        3
    );

    // An INSERT binds its values positionally.
    session
        .run_with_params(
            "INSERT INTO t (a,b,c) VALUES (?,?,?)",
            &[Datum::Int(1), Datum::Bytes(b"one".to_vec()), Datum::Int(10)],
        )
        .unwrap();
    session
        .run_with_params(
            "INSERT INTO t (a,b,c) VALUES (?,?,?)",
            &[Datum::Int(2), Datum::Bytes(b"two".to_vec()), Datum::Int(20)],
        )
        .unwrap();

    // A SELECT binds in WHERE, and the markers keep their source order.
    let output = session
        .run_with_params("SELECT b FROM t WHERE a = ?", &[Datum::Int(2)])
        .unwrap();
    match output {
        StmtOutput::Rows { rows, .. } => {
            assert_eq!(datum_text(&rows[0][0]).unwrap(), "two");
        }
        other => panic!("expected rows, got {other:?}"),
    }
    let output = session
        .run_with_params(
            "SELECT a FROM t WHERE c BETWEEN ? AND ? ORDER BY a",
            &[Datum::Int(5), Datum::Int(15)],
        )
        .unwrap();
    match output {
        StmtOutput::Rows { rows, .. } => assert_eq!(rows.len(), 1),
        other => panic!("expected rows, got {other:?}"),
    }

    // A value that is not UTF-8 does NOT fit a utf8mb4 column: captured, TiDB
    // answers 1366 "Incorrect string value '\xFF' for column 'b'" rather than
    // storing mangled bytes.
    assert!(matches!(
        session.run_with_params(
            "INSERT INTO t (a,b,c) VALUES (?,?,?)",
            &[
                Datum::Int(3),
                Datum::Bytes(vec![0xff, 0xfe, b'z']),
                Datum::Int(30),
            ],
        ),
        Err(DriverError::IncorrectValue { .. })
    ));

    // The same bytes DO survive the round trip through a binary column, which
    // has no character set to validate them against (captured).
    session
        .run("CREATE TABLE tv (a BIGINT PRIMARY KEY, b VARBINARY(20))")
        .unwrap();
    session
        .run_with_params(
            "INSERT INTO tv (a,b) VALUES (?,?)",
            &[Datum::Int(3), Datum::Bytes(vec![0xff, 0xfe, b'z'])],
        )
        .unwrap();
    match session
        .run_with_params("SELECT b FROM tv WHERE a = ?", &[Datum::Int(3)])
        .unwrap()
    {
        StmtOutput::Rows { rows, .. } => {
            let stored = match &rows[0][0] {
                Datum::Bytes(bytes) => bytes.clone(),
                Datum::String(text) => text.bytes().to_vec(),
                other => panic!("expected a string datum, got {other:?}"),
            };
            assert_eq!(stored, vec![0xff, 0xfe, b'z']);
        }
        other => panic!("expected rows, got {other:?}"),
    }

    // A NULL parameter binds as NULL, not as the text "NULL".
    session
        .run_with_params(
            "INSERT INTO t (a,b,c) VALUES (?,?,?)",
            &[Datum::Int(4), Datum::Null, Datum::Int(40)],
        )
        .unwrap();
    assert_eq!(
        row_text(session.run("SELECT a FROM t WHERE b IS NULL")),
        [["4"]]
    );

    // Too few or too many values is Go's `plannererrors.ErrWrongParamCount`
    // (8112, `[planner:8112]Wrong parameter count`), captured from TiDB in
    // both directions: `EXECUTE p0 USING @a` for a marker-free statement and
    // `EXECUTE stmt` for one that carries a marker report the same code. The
    // check is `planCachePreprocess`'s step 1, which the binary protocol and
    // the SQL-level `EXECUTE` both reach.
    match session.run_with_params("SELECT a FROM t WHERE a = ?", &[]) {
        Ok(_) => panic!("an unbound marker should fail"),
        Err(error) => assert_eq!(error.to_mysql_error().code, 8112),
    }
    match session.run_with_params(
        "SELECT a FROM t WHERE a = ?",
        &[Datum::Int(1), Datum::Int(2)],
    ) {
        Ok(_) => panic!("an extra value should fail"),
        Err(error) => assert_eq!(error.to_mysql_error().code, 8112),
    }
}

/// Go's one warning sink stops appending at `math.MaxUint16`
/// (`StaticWarnHandler.appendWarningWithLevel`), because the count it
/// publishes is a `uint16`. The session buffer is that sink here, so the limit
/// belongs on its one door rather than at each of the seven callers.
#[test]
fn the_session_warning_buffer_stops_at_the_source_retention_limit() {
    let mut session = Session::new();
    for index in 0..tidb_executor::MAX_WARNING_COUNT + 16 {
        session.append_warning(WarningLevel::Warning, 1292, format!("value {index}"));
    }
    assert_eq!(session.warnings().len(), tidb_executor::MAX_WARNING_COUNT);
    // The FIRST entries survive: Go appends until the limit and then drops.
    assert_eq!(session.warnings()[0].message, "value 0");
    assert_eq!(
        session.warnings()[tidb_executor::MAX_WARNING_COUNT - 1].message,
        "value 65534"
    );
}

/// A warning TiKV reported for a coprocessor request reaches BOTH channels a
/// client can learn about it from: the buffer `SHOW WARNINGS` reads and the
/// count the OK/EOF packet carries.
///
/// The two are proven independent here (`wire_warning_count` returns 0 while
/// `InShowWarning` is set, so it is not the buffer's length), and a fix
/// validated through only one of them is exactly how eleven of these stayed
/// invisible: `response_channel` appended them correctly into a collector
/// every production site built FRESH and then dropped.
///
/// DEFERRED LIVE CHECK: the audit's named case is `SELECT ROUND(s) FROM t`
/// with `s = '12abc'`, where TiDB reports the truncated value plus a 1292
/// warning. Producing the warning needs a real region acting on
/// `DAGRequest.flags = 482`; what is pinned here is that a warning which
/// arrives through the statement's coprocessor sink is reported.
#[test]
fn a_coprocessor_warning_reaches_both_the_show_warnings_buffer_and_the_wire_count() {
    let mut session = Session::new();
    let ctx = tidb_executor::StmtContext::for_query();
    // What `tidb_distsql`'s `response_channel` does with
    // `SelectResponse.warnings`; the sink is the statement's own.
    ctx.cop_warning_sink()
        .append_tikv_warning(1292, "Truncated incorrect DOUBLE value: '12abc'");

    session.drain_eval_warnings(&ctx);

    assert_eq!(session.warnings().len(), 1);
    assert_eq!(session.warnings()[0].code, 1292);
    assert_eq!(
        session.warnings()[0].message,
        "Truncated incorrect DOUBLE value: '12abc'"
    );
    assert_eq!(
        session.wire_warning_count(),
        1,
        "the OK packet's count is a separate channel from the buffer"
    );
}

/// Whether a statement inherits the previous statement's warning buffer is a
/// decision Go makes on the PARSED node: `ResetContextOfStmt` switches on
/// `*ast.ShowStmt` and copies the outgoing context's entries forward only for
/// `ShowWarnings`, `ShowErrors`, and `ShowSessionStates`
/// (`pkg/executor/select.go`).
///
/// `WARNINGS` and `ERRORS` are UNRESERVED keywords, so `warnings` is a legal
/// table name. Deciding on the raw SQL text instead of the node therefore let
/// `SHOW CREATE TABLE warnings` inherit the buffer, and the `SHOW WARNINGS`
/// after it reported a statement two back.
#[test]
fn only_the_parsed_reporting_nodes_inherit_the_warning_buffer() {
    let mut session = Session::new();
    session.run("CREATE TABLE warnings (a INT)").unwrap();
    session
        .apply_set("SET tidb_enable_noop_functions = 'WARN'")
        .unwrap();

    let warn_once = |session: &mut Session| {
        session
            .run("SELECT a FROM warnings LOCK IN SHARE MODE")
            .unwrap();
        assert_eq!(session.warnings().len(), 1);
    };
    let reported = |session: &mut Session, sql: &str| match session.run_with_columns(sql).unwrap() {
        StmtOutput::Rows { rows, .. } => rows.len(),
        other => panic!("expected rows, got {other:?}"),
    };

    // The reporting nodes DO inherit it, and keep inheriting it: Go's
    // `SetWarnings` installs the entries into the fresh context, so the next
    // `SHOW WARNINGS` copies them forward again.
    warn_once(&mut session);
    assert_eq!(reported(&mut session, "SHOW WARNINGS"), 1);
    assert_eq!(reported(&mut session, "SHOW WARNINGS"), 1);

    // A SHOW that merely NAMES a table called `warnings` or `errors` does not.
    warn_once(&mut session);
    session.run("SHOW CREATE TABLE warnings").unwrap();
    assert_eq!(reported(&mut session, "SHOW WARNINGS"), 0);

    warn_once(&mut session);
    session.run("SHOW COLUMNS FROM warnings").unwrap();
    assert_eq!(reported(&mut session, "SHOW WARNINGS"), 0);

    session.run("CREATE TABLE errors (a INT)").unwrap();
    warn_once(&mut session);
    session.run("SHOW CREATE TABLE errors").unwrap();
    assert_eq!(reported(&mut session, "SHOW WARNINGS"), 0);

    // A statement that fails to parse clears the buffer, because a failed
    // parse never reaches the copy that would carry it forward. What is left
    // is the failure's OWN entry, which `run_with_columns` files as an
    // `Error` row -- not the preceding statement's warning.
    warn_once(&mut session);
    assert!(session.run("SELCT 1").is_err());
    assert_eq!(session.warnings().len(), 1);
    assert_eq!(session.warnings()[0].level, WarningLevel::Error);
    assert_ne!(session.warnings()[0].code, 1235);
}
