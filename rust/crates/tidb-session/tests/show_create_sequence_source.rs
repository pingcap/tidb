//! SHOW CREATE SEQUENCE prints Go's `ConstructResultOfShowCreateSequence`
//! shape (show.go:1575-1599): lowercase keywords, explicit start/min/max/
//! increment/cache/cycle, `ENGINE=InnoDB`, with ascending-sequence defaults
//! filled in (minvalue 1, maxvalue MaxInt64-1).

use tidb_session::Session;

use crate::support::byte_lines as strings;

#[test]
fn sequence_ddl_round_trips() {
    let mut session = Session::new();
    session
        .run("create sequence s start with 100 increment 5 cache 50 cycle")
        .unwrap();

    let shown = strings(&mut session, "show create sequence s");
    assert_eq!(
        shown.lines().nth(1).unwrap(),
        "CREATE SEQUENCE `s` start with 100 minvalue 1 \
         maxvalue 9223372036854775806 increment by 5 cache 50 cycle ENGINE=InnoDB"
    );
}
