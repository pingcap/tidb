//! The per-statement status values a client reads back: `LAST_INSERT_ID`
//! (function form and wire field, which are one publication) and
//! `ROW_COUNT` -- Go `pkg/sessionctx/stmtctx` and the OK-packet fields.

use crate::*;

/// LAST_INSERT_ID, checked against a sequence captured from real TiDB:
/// 0, 1, 2 (the FIRST id of a multi-row insert), unchanged by an explicit
/// value, then 101 and 102, and unchanged by a non-allocating statement.
#[test]
fn last_insert_id() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE a (id BIGINT AUTO_INCREMENT PRIMARY KEY, v BIGINT)")
        .unwrap();
    let read = |session: &mut Session| match session.run("SELECT LAST_INSERT_ID()").unwrap() {
        StmtResult::Rows(rows) => datum_text(&rows[0][0]).unwrap(),
        other => panic!("expected rows, got {other:?}"),
    };

    assert_eq!(read(&mut session), "0", "captured: start");
    session.run("INSERT INTO a (v) VALUES (10)").unwrap();
    assert_eq!(read(&mut session), "1", "captured: after single auto");
    session
        .run("INSERT INTO a (v) VALUES (20), (30), (40)")
        .unwrap();
    assert_eq!(
        read(&mut session),
        "2",
        "captured: a multi-row insert reports its FIRST id"
    );
    session.run("INSERT INTO a VALUES (100, 50)").unwrap();
    assert_eq!(
        read(&mut session),
        "2",
        "captured: an explicit value leaves it unchanged"
    );
    session.run("INSERT INTO a (v) VALUES (60)").unwrap();
    assert_eq!(read(&mut session), "101", "captured: after auto again");
    session.run("INSERT INTO a VALUES (NULL, 70)").unwrap();
    assert_eq!(read(&mut session), "102", "captured: NULL allocates");

    // A table with no auto column, and an UPDATE, both leave it alone.
    session
        .run("CREATE TABLE b (id BIGINT PRIMARY KEY)")
        .unwrap();
    session.run("INSERT INTO b VALUES (5)").unwrap();
    assert_eq!(read(&mut session), "102", "captured: non-auto insert");
    session.run("UPDATE a SET v = 0 WHERE id = 1").unwrap();
    assert_eq!(read(&mut session), "102", "captured: after update");

    // The OK packet's field is per statement, so it is 0 for a statement
    // that allocated nothing, unlike the sticky function value.
    session.run("INSERT INTO a (v) VALUES (80)").unwrap();
    assert_eq!(session.statement_insert_id(), 103);
    session.run("INSERT INTO b VALUES (6)").unwrap();
    assert_eq!(session.statement_insert_id(), 0);
    assert_eq!(session.last_insert_id(), 103);
}

/// Reads one scalar as text through the ordinary statement path.
fn session_scalar(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        StmtResult::Rows(rows) => datum_text(&rows[0][0]).unwrap(),
        other => panic!("expected rows, got {other:?}"),
    }
}

/// `ROW_COUNT()`'s captured rule table (real TiDB, `corpus/table/row_count`):
/// it reports what the PRECEDING statement did -- `-1` after any SELECT,
/// including the `SELECT ROW_COUNT()` before it; the affected-row count after
/// a DML statement (0 when it matched but changed nothing); and 0 after DDL,
/// SET, or transaction control.
#[test]
fn row_count_reports_the_previous_statements_class() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE rc (id INT PRIMARY KEY, u INT UNIQUE, v INT)")
        .unwrap();
    assert_eq!(
        session_scalar(&mut session, "SELECT ROW_COUNT()"),
        "0",
        "captured: after DDL"
    );
    assert_eq!(
        session_scalar(&mut session, "SELECT ROW_COUNT()"),
        "-1",
        "captured: after the SELECT above"
    );

    session
        .run("INSERT INTO rc VALUES (1, 10, 1), (2, 20, 2)")
        .unwrap();
    assert_eq!(
        session_scalar(&mut session, "SELECT ROW_COUNT()"),
        "2",
        "captured: a two-row insert"
    );

    session.run("UPDATE rc SET v = 1 WHERE id = 1").unwrap();
    assert_eq!(
        session_scalar(&mut session, "SELECT ROW_COUNT()"),
        "0",
        "captured: an update that matched but changed nothing"
    );
    session.run("UPDATE rc SET v = 3 WHERE id = 1").unwrap();
    assert_eq!(session_scalar(&mut session, "SELECT ROW_COUNT()"), "1");

    session.run("DELETE FROM rc WHERE id = 99").unwrap();
    assert_eq!(
        session_scalar(&mut session, "SELECT ROW_COUNT()"),
        "0",
        "captured: a delete that matched nothing"
    );
    session.run("DELETE FROM rc WHERE id = 2").unwrap();
    assert_eq!(session_scalar(&mut session, "SELECT ROW_COUNT()"), "1");

    // A FAILED statement still classifies itself, because Go sets the
    // `In*Stmt` bits before execution: a failed SELECT leaves -1, a failed
    // INSERT leaves 0.
    assert!(session
        .run("SELECT * FROM no_such_row_count_table")
        .is_err());
    assert_eq!(
        session_scalar(&mut session, "SELECT ROW_COUNT()"),
        "-1",
        "captured: after a failed SELECT"
    );
    assert!(session
        .run("INSERT INTO no_such_row_count_table VALUES (1)")
        .is_err());
    assert_eq!(
        session_scalar(&mut session, "SELECT ROW_COUNT()"),
        "0",
        "captured: after a failed INSERT"
    );

    session.run("SET @@sql_safe_updates = 0").unwrap();
    assert_eq!(
        session_scalar(&mut session, "SELECT ROW_COUNT()"),
        "0",
        "captured: after SET"
    );
}

/// `LAST_INSERT_ID(expr)`'s captured rules
/// (`corpus/table/last_insert_id_uint`): the one-argument form returns its
/// coerced UNSIGNED value immediately and publishes it for the NEXT statement
/// only, `NULL` publishes nothing, and a `ROLLBACK` does not undo it.
/// `@@last_insert_id` and `@@identity` report the same 64-bit pattern.
#[test]
fn last_insert_id_argument_form_publishes_for_the_next_statement() {
    let mut session = Session::new();
    assert_eq!(
        session
            .run("SELECT LAST_INSERT_ID(5), LAST_INSERT_ID()")
            .unwrap(),
        StmtResult::Rows(vec![vec![Datum::UInt(5), Datum::UInt(0)]]),
        "captured: the no-argument read is the PREVIOUS statement's value"
    );
    assert_eq!(session_scalar(&mut session, "SELECT LAST_INSERT_ID()"), "5");

    // Negative and out-of-`i64` arguments are the same two's-complement bits.
    assert_eq!(
        session_scalar(&mut session, "SELECT LAST_INSERT_ID(-1)"),
        "18446744073709551615"
    );
    assert_eq!(
        session_scalar(&mut session, "SELECT LAST_INSERT_ID(18446744073709551615)"),
        "18446744073709551615",
        "captured: a literal above the i64 domain is UNSIGNED, not an error"
    );

    // `EvalInt` coercion: rounding, and a string's leading integer run.
    assert_eq!(
        session_scalar(&mut session, "SELECT LAST_INSERT_ID(1.5)"),
        "2"
    );
    assert_eq!(
        session_scalar(&mut session, "SELECT LAST_INSERT_ID('1e2tail')"),
        "1"
    );
    // NULL returns NULL and publishes nothing, so the previous value stands.
    assert_eq!(
        session.run("SELECT LAST_INSERT_ID(NULL)").unwrap(),
        StmtResult::Rows(vec![vec![Datum::Null]])
    );
    assert_eq!(session_scalar(&mut session, "SELECT LAST_INSERT_ID()"), "1");

    // A ROLLBACK does not undo the publication.
    session.run("BEGIN").unwrap();
    assert_eq!(
        session_scalar(&mut session, "SELECT LAST_INSERT_ID(9)"),
        "9"
    );
    session.run("ROLLBACK").unwrap();
    assert_eq!(session_scalar(&mut session, "SELECT LAST_INSERT_ID()"), "9");
    assert_eq!(session_scalar(&mut session, "SELECT @@last_insert_id"), "9");
    assert_eq!(session_scalar(&mut session, "SELECT @@identity"), "9");
}

/// `LAST_INSERT_ID(const)` is folded while the select list is being REWRITTEN,
/// so its session side effect outlives a later expression that fails to
/// resolve at all.
///
/// Captured from real TiDB via `rust/difftests/gorun`, each `ERR` statement
/// followed by `SELECT LAST_INSERT_ID(), @@last_insert_id`:
///
/// | statement | outcome | published |
/// | --- | --- | --- |
/// | `SELECT LAST_INSERT_ID(17), no_such_fn()` | `ERR` 1305 | `17|17` |
/// | `SELECT no_such_fn(), LAST_INSERT_ID(18)` | `ERR` 1305 | still `17|17` |
/// | `SELECT LAST_INSERT_ID(19+1), no_such_fn()` | `ERR` 1305 | `20|20` |
/// | `SELECT LAST_INSERT_ID(21) FROM no_such_table` | `ERR` 1146 | still `20|20` |
/// | `SELECT LAST_INSERT_ID(22), 1/0 AS x, no_such_fn()` | `ERR` 1305 | `22|22` |
///
/// Three rules fall out of that table, and each one is load-bearing:
///
///  * the fold happens DURING rewriting, not during execution -- the
///    statement never evaluates a row and the id still moves;
///  * rewriting is LEFT TO RIGHT and stops at the first failure, which is why
///    putting the unknown function first publishes nothing;
///  * table resolution runs BEFORE expression rewriting, which is why an
///    unknown table publishes nothing either.
///
/// REFUSED, not deferred quietly: reproducing this needs TWO mechanisms this
/// engine does not have, and neither is a last-insert-id detail.
///
///  1. Go's rewriter-wide CONSTANT FOLDING (`expression.foldConstant`, called
///     from `newFunction`), which evaluates any all-constant call once during
///     rewriting. It is what runs `LAST_INSERT_ID(17)`'s side effect without
///     the statement ever reaching a row, and it also folds the `19+1`
///     argument. This tier has only a shallow local helper
///     (`driver::agg_build::fold_constant`, which documents itself as "not a
///     rewriter-wide folding pass"); a folder that only ever runs for
///     `LAST_INSERT_ID` would be a workaround, and a real one changes results
///     across the whole engine.
///  2. Builtin NAME RESOLUTION raising `ErrSpDoesNotExist` (1305) during
///     rewriting. Today an unknown function survives planning and dies in the
///     chunk tier as 1105 "this builtin is not yet built for chunk
///     evaluation" -- the same error a builtin that MySQL does know but this
///     engine has not built yet produces. Separating the two needs the
///     complete builtin-name registry, and getting it wrong turns
///     nearly-working paths into hard 1305s.
///
/// A future unit needs (1) first: with a real folder, ordering and the 1305
/// boundary follow from where the folder sits relative to name resolution.
/// The guard below pins what this engine does today so the change is visible.
#[test]
fn a_folded_last_insert_id_publication_is_not_modelled_today() {
    let mut session = Session::new();
    let mysql = session
        .run("SELECT LAST_INSERT_ID(17), no_such_fn()")
        .expect_err("the statement fails either way")
        .to_mysql_error();
    assert_eq!(
        mysql.code, 1105,
        "guard: an unknown function is still a generic chunk-tier 1105, not Go's 1305 -- {}",
        mysql.message
    );
    assert_eq!(
        session_scalar(&mut session, "SELECT LAST_INSERT_ID()"),
        "0",
        "guard: nothing is folded, so nothing is published (Go publishes 17)"
    );
}

/// Go's answer for the same five statements, asserted so the refusal above is
/// a tracked work item rather than a wish. Ignored until a rewriter-wide
/// constant folder exists.
#[test]
#[ignore = "needs Go's rewriter-wide constant folding plus 1305 builtin name resolution"]
fn a_folded_last_insert_id_publishes_even_when_a_later_expression_fails_to_resolve() {
    let mut session = Session::new();

    let wire_error = |session: &mut Session, sql: &str| {
        let mysql = session.run(sql).expect_err(sql).to_mysql_error();
        (mysql.code, mysql.message)
    };

    let (code, message) = wire_error(&mut session, "SELECT LAST_INSERT_ID(17), no_such_fn()");
    assert_eq!(code, 1305, "got {message}");
    assert!(
        message.contains("no_such_fn"),
        "the unresolved name must be named: {message}"
    );
    assert_eq!(
        session_scalar(&mut session, "SELECT LAST_INSERT_ID()"),
        "17",
        "the fold's side effect outlives the failed statement"
    );

    // Rewriting stops at the FIRST failure, so an unknown function ahead of
    // the fold publishes nothing.
    session
        .run("SELECT no_such_fn(), LAST_INSERT_ID(18)")
        .expect_err("an unknown function is 1305");
    assert_eq!(
        session_scalar(&mut session, "SELECT LAST_INSERT_ID()"),
        "17"
    );

    // A folded ARGUMENT is folded too -- the published value is 20, not 19.
    session
        .run("SELECT LAST_INSERT_ID(19+1), no_such_fn()")
        .expect_err("an unknown function is 1305");
    assert_eq!(
        session_scalar(&mut session, "SELECT LAST_INSERT_ID()"),
        "20"
    );

    // Table resolution precedes expression rewriting, so nothing is folded.
    session
        .run("SELECT LAST_INSERT_ID(21) FROM no_such_table")
        .expect_err("an unknown table is 1146");
    assert_eq!(
        session_scalar(&mut session, "SELECT LAST_INSERT_ID()"),
        "20"
    );

    // An expression that merely WARNS between the two does not stop rewriting.
    session
        .run("SELECT LAST_INSERT_ID(22), 1/0 AS x, no_such_fn()")
        .expect_err("an unknown function is 1305");
    assert_eq!(
        session_scalar(&mut session, "SELECT LAST_INSERT_ID()"),
        "22"
    );
}

/// The FUNCTION and the WIRE value read ONE publication, so they can differ
/// only where Go itself makes them differ.
///
/// Go keeps two readers of `StmtCtx`: the OK packet answers
/// `LastInsertID`-or-`InsertID` (`session.LastInsertID()`), while
/// `LAST_INSERT_ID()` answers `PrevLastInsertID`. The shapes below are
/// exactly where that shows -- an allocating insert agrees on both, an
/// EXPLICIT id moves the wire only, and a non-allocating statement resets the
/// wire to 0 while the function stays put -- and the last block proves the
/// function form writes the insert path's channel rather than a second copy.
#[test]
fn last_insert_id_function_and_wire_value_are_one_publication() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE w (id BIGINT AUTO_INCREMENT PRIMARY KEY, v BIGINT)")
        .unwrap();

    session.run("INSERT INTO w (v) VALUES (1)").unwrap();
    assert_eq!(session.statement_insert_id(), 1, "wire: allocated");
    assert_eq!(session_scalar(&mut session, "SELECT LAST_INSERT_ID()"), "1");

    session.run("INSERT INTO w VALUES (50, 2)").unwrap();
    assert_eq!(
        session.statement_insert_id(),
        50,
        "captured: the wire reports an EXPLICIT id"
    );
    assert_eq!(
        session_scalar(&mut session, "SELECT LAST_INSERT_ID()"),
        "1",
        "captured: the function never follows an explicit value"
    );

    session.run("SELECT 1").unwrap();
    assert_eq!(
        session.statement_insert_id(),
        0,
        "the wire field is per statement"
    );
    assert_eq!(
        session_scalar(&mut session, "SELECT LAST_INSERT_ID()"),
        "1",
        "the function is sticky"
    );

    session.run("SELECT LAST_INSERT_ID(77)").unwrap();
    assert_eq!(session.last_insert_id(), 77);
    assert_eq!(
        session_scalar(&mut session, "SELECT LAST_INSERT_ID()"),
        "77"
    );
}
