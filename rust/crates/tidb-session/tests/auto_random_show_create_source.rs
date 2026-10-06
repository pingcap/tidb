//! AUTO_RANDOM shard width round-trips in SHOW CREATE as the version-gated
//! column comment `/*T![auto_rand] AUTO_RANDOM(4) */` on the id column.

use tidb_session::Session;

use crate::support::byte_lines as strings;

#[test]
fn auto_random_shard_width_round_trips() {
    let mut session = Session::new();
    session
        .run("create table t (id bigint primary key auto_random(4), v int)")
        .unwrap();

    let shown = strings(&mut session, "show create table t");
    assert!(
        shown.contains("/*T![auto_rand] AUTO_RANDOM(4) */"),
        "{shown}"
    );
}
