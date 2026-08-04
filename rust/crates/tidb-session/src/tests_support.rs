//! Shared test fixtures and helpers used across the tidb-session test modules.
#![cfg(test)]

use crate::*;

/// The single value a one-column, one-row query returns, as text.
pub(crate) fn scalar_text(session: &mut Session, sql: &str) -> Option<String> {
    match session.run(sql).unwrap() {
        StmtResult::Rows(rows) => datum_text(&rows[0][0]),
        other => panic!("expected rows, got {other:?}"),
    }
}

/// One result cell as the text a client reads off the wire.
///
/// This is deliberately NOT [`crate::variables::datum_text`], which is the
/// *system variable* text function: its `_ => None` arm answers `None` for
/// every temporal datum, and a `None` here is indistinguishable from SQL
/// NULL. A `DATE`/`DATETIME`/`TIMESTAMP`/`TIME` column read through that
/// helper therefore printed `"NULL"` for a value that is stored, non-NULL,
/// and matched by `WHERE i_date = '2020-01-01'` -- so an assertion written
/// against it would have pinned the wrong answer. The wire path
/// (`tidb_server::resultset_writer::format_datum`) renders a row cell with
/// `Datum::to_bytes`, and that is what this uses, so NULL is the only value
/// that can produce `"NULL"`.
pub(crate) fn cell_text(value: &Datum) -> String {
    if value.is_null() {
        return "NULL".to_owned();
    }
    match value.to_bytes() {
        Ok(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
        // A datum the wire cannot render either (MinNotNull / MaxValue) is a
        // bug at the producing site, not something to paper over as NULL.
        Err(error) => panic!("cannot render {value:?} as a result cell: {error}"),
    }
}

/// RENAME TABLE, checked against captured TiDB behavior.
/// A result's rows as text, so an assertion does not depend on which
/// datum kind the codec hands back for a given column type.
pub(crate) fn row_text(result: Result<StmtResult, DriverError>) -> Vec<Vec<String>> {
    match result.unwrap() {
        StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| row.iter().map(cell_text).collect())
            .collect(),
        other => panic!("expected rows, got {other:?}"),
    }
}

/// The warnings the last statement left, as `(code, message)`.
pub(crate) fn warnings_of(session: &Session) -> Vec<(u16, String)> {
    session
        .warnings()
        .iter()
        .map(|w| (w.code, w.message.clone()))
        .collect()
}

/// The `Create Table` text of one table.
pub(crate) fn show_create(session: &mut Session, table: &str) -> String {
    match session
        .run_with_columns(&format!("SHOW CREATE TABLE {table}"))
        .unwrap()
    {
        StmtOutput::Rows { rows, .. } => datum_text(&rows[0][1]).unwrap(),
        other => panic!("expected rows, got {other:?}"),
    }
}

/// The fixture the captured `LATERAL` cases run against: `s` has a
/// different number of rows per key (2, 1, 3), so a per-outer-row
/// re-evaluation is visibly different from any single inner run.
pub(crate) fn lateral_session() -> Session {
    let mut session = Session::new();
    session.run("CREATE TABLE t (a BIGINT, b BIGINT)").unwrap();
    session
        .run("INSERT INTO t VALUES (1,10),(2,20),(3,30)")
        .unwrap();
    session.run("CREATE TABLE s (k BIGINT, v BIGINT)").unwrap();
    session
        .run("INSERT INTO s VALUES (1,100),(1,101),(2,200),(3,300),(3,301),(3,302)")
        .unwrap();
    session.run("CREATE TABLE u (a BIGINT, z BIGINT)").unwrap();
    session.run("INSERT INTO u VALUES (1,7),(2,8)").unwrap();
    session
}

/// The two tables the captured semi-join cases run against.
pub(crate) fn semi_join_session() -> Session {
    let mut session = Session::new();
    session.run("CREATE TABLE t1 (id INT, v INT)").unwrap();
    session.run("CREATE TABLE t2 (t1_id INT, w INT)").unwrap();
    session
        .run("INSERT INTO t1 VALUES (1,10),(2,20),(3,30),(4,NULL)")
        .unwrap();
    session
        .run("INSERT INTO t2 VALUES (1,10),(1,5),(2,25),(2,NULL),(3,30)")
        .unwrap();
    session
}

/// A session seeded with the ranking-window fixture: duplicate `v` values
/// inside each `g` group, so ties are exercised in every direction.
pub(crate) fn window_session() -> Session {
    let mut session = Session::new();
    session.run("CREATE TABLE t (g BIGINT, v BIGINT)").unwrap();
    session
        .run("INSERT INTO t VALUES (1,10),(1,20),(1,20),(1,30),(1,40),(2,5),(2,5),(2,7)")
        .unwrap();
    session
}

/// A result's column names and rows as text, matching how the captured
/// Go output above prints them.
pub(crate) fn query_text(session: &mut Session, sql: &str) -> (Vec<String>, Vec<Vec<String>>) {
    match session.run_with_columns(sql).unwrap() {
        StmtOutput::Rows { columns, rows } => (
            columns
                .into_iter()
                .map(|(name, _)| name)
                .collect::<Vec<_>>(),
            rows.into_iter()
                .map(|row| {
                    row.iter()
                        .map(|value| match value {
                            Datum::Null => "<nil>".to_owned(),
                            Datum::Int(v) => v.to_string(),
                            other => datum_text(other).unwrap_or_default(),
                        })
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>(),
        ),
        other => panic!("expected rows, got {other:?}"),
    }
}

/// A session with `t`, `s` and the views the captures were taken over.
pub(crate) fn view_session() -> Session {
    let mut session = Session::new();
    session.run("CREATE TABLE t (a BIGINT, b BIGINT)").unwrap();
    session
        .run("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();
    session
        .run("CREATE TABLE s (a BIGINT, c VARCHAR(20))")
        .unwrap();
    session
        .run("INSERT INTO s VALUES (1, 'x'), (2, 'y')")
        .unwrap();
    session.run("CREATE VIEW v AS SELECT a, b FROM t").unwrap();
    session
        .run("CREATE VIEW v2(a2) AS SELECT a FROM t")
        .unwrap();
    session
        .run("CREATE VIEW v3 AS SELECT * FROM v WHERE b > 10")
        .unwrap();
    session
}

/// The fixture the value-measured `RANGE` frame captures ran over: keys
/// with a TIE (`3,3`) and a GAP (`3 -> 7`), which is what separates a
/// value frame from a positional one.
pub(crate) fn range_session() -> Session {
    let mut session = Session::new();
    session
        .run("CREATE TABLE ri (g BIGINT, k BIGINT, v BIGINT)")
        .unwrap();
    session
        .run("INSERT INTO ri VALUES (1,1,10),(1,3,20),(1,3,30),(1,7,40),(1,8,50)")
        .unwrap();
    session
}

/// The fixture the `RANGE ... INTERVAL` captures ran over: a DATETIME
/// key with a sub-day step, a TIE, a multi-day GAP, and a second
/// partition.
pub(crate) fn interval_session() -> Session {
    let mut session = Session::new();
    session
        .run("CREATE TABLE td (g BIGINT, k DATETIME, v BIGINT)")
        .unwrap();
    session
        .run(
            "INSERT INTO td VALUES \
                 (1,'2020-01-01 00:00:00',10),(1,'2020-01-01 12:00:00',20), \
                 (1,'2020-01-02 00:00:00',30),(1,'2020-01-02 00:00:00',40), \
                 (1,'2020-01-05 00:00:00',50),(2,'2020-01-01 00:00:00',60)",
        )
        .unwrap();
    session
}

/// A session with a GLOBAL-scope privilege registry attached, over a
/// fresh catalog. Root is bootstrapped with every privilege, matching
/// what `PipelineSessionFactory` gives every connection.
pub(crate) fn session_with_privileges() -> Session {
    let mut session = Session::new();
    session.attach_privileges(privilege::PrivilegeRegistry::default());
    session
}

/// A session on `registry` with NO authenticated identity, and therefore
/// unrestricted -- the in-process shape the server's own bootstrap uses.
///
/// The privilege gates on `CREATE USER`/`GRANT` exist to stop an
/// AUTHENTICATED session escalating itself, so a test that merely needs the
/// rows to exist provisions them from here rather than from the session
/// under test, exactly as a real deployment provisions them before anybody
/// logs in.
pub(crate) fn bootstrap_session(registry: &privilege::PrivilegeRegistry) -> Session {
    let mut session = Session::new();
    session.attach_privileges(registry.clone());
    session
}

/// A connection to `registry` authenticated as `user`@`host`, over a fresh
/// catalog. [`session_as`] is the same thing on a catalog the caller owns.
pub(crate) fn authenticated_session(
    registry: &privilege::PrivilegeRegistry,
    user: &str,
    host: &str,
) -> Session {
    let mut session = Session::new();
    session.set_user(format!("{user}@{host}"), format!("{user}@{host}"));
    session.attach_privileges(registry.clone());
    session
}

/// A second connection to the same server, authenticated as `user`@`host`.
///
/// The identity is installed BEFORE the registry, which is the order
/// `PipelineSessionFactory` uses and the order that makes the account's
/// DEFAULT roles activate -- exactly as Go's `Auth` does at login.
pub(crate) fn session_as(
    registry: &privilege::PrivilegeRegistry,
    catalog: SharedCatalog,
    user: &str,
    host: &str,
) -> Session {
    let mut session = Session::with_catalog(catalog);
    session.set_user(format!("{user}@{host}"), format!("{user}@{host}"));
    session.attach_privileges(registry.clone());
    session
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A stored temporal value must never read back as the text `NULL`.
    ///
    /// This guards the helper itself: [`cell_text`]'s predecessor delegated to
    /// `variables::datum_text`, whose `_ => None` arm covers `Datum::Time` and
    /// `Datum::Duration`, so every `DATE`, `DATETIME`, `TIMESTAMP` and `TIME`
    /// column in every session test rendered as `"NULL"`. `COUNT(d) = 1` and
    /// the matching `WHERE` below are the proof the value was there all
    /// along -- the reader was lying, not the storage.
    #[test]
    fn a_temporal_column_renders_its_value_not_null() {
        let mut session = Session::new();
        session
            .run("CREATE TABLE t (d DATE, dt DATETIME, ts TIMESTAMP, tm TIME)")
            .unwrap();
        session
            .run(
                "INSERT INTO t VALUES \
                 ('2020-01-01','2020-01-01 10:20:30','2020-01-01 10:20:30','10:20:30')",
            )
            .unwrap();
        assert_eq!(
            row_text(session.run("SELECT * FROM t")),
            vec![vec![
                "2020-01-01".to_owned(),
                "2020-01-01 10:20:30".to_owned(),
                "2020-01-01 10:20:30".to_owned(),
                "10:20:30".to_owned(),
            ]]
        );
        assert_eq!(
            row_text(session.run("SELECT COUNT(d) FROM t")),
            vec![vec!["1"]]
        );
        assert_eq!(
            row_text(session.run("SELECT d FROM t WHERE d = '2020-01-01'")),
            vec![vec!["2020-01-01"]]
        );
        // A real NULL still reads as NULL, which is the only value that may.
        session
            .run("INSERT INTO t VALUES (NULL,NULL,NULL,NULL)")
            .unwrap();
        assert_eq!(
            row_text(session.run("SELECT d FROM t WHERE d IS NULL")),
            vec![vec!["NULL"]]
        );
    }
}
