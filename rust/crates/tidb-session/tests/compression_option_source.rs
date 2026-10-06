//! The `COMPRESSION = 'zstd'` table option: stored verbatim, printed by
//! SHOW CREATE when non-empty, ALTER can re-set it, and the last option
//! wins when CREATE carries several. Go `create_table.go:964-965` +
//! `executor/show.go:1373-1375`.

use tidb_session::Session;

use crate::support::byte_lines as strings;

#[test]
fn compression_option_round_trips() {
    let mut session = Session::new();
    session
        .run("create table t (id int primary key) compression = 'zstd'")
        .unwrap();

    let shown = strings(&mut session, "show create table t");
    assert!(shown.contains("COMPRESSION='zstd'"), "{shown}");
}

#[test]
fn alter_replaces_compression_and_last_wins() {
    let mut session = Session::new();
    session.run("create table t (id int primary key)").unwrap();

    // No option at CREATE: nothing prints.
    assert!(
        !strings(&mut session, "show create table t").contains("COMPRESSION="),
        "fresh table must not print COMPRESSION"
    );

    session.run("alter table t compression = 'lz4'").unwrap();
    assert!(
        strings(&mut session, "show create table t").contains("COMPRESSION='lz4'"),
        "{:?}",
        strings(&mut session, "show create table t")
    );

    // Several options at CREATE: the last one wins (Go's loop overwrites).
    session
        .run("create table t2 (id int primary key) compression = 'zstd' compression = 'lz4'")
        .unwrap();
    let shown = strings(&mut session, "show create table t2");
    assert!(shown.contains("COMPRESSION='lz4'"), "{shown}");
    assert!(!shown.contains("COMPRESSION='zstd'"), "{shown}");
}
