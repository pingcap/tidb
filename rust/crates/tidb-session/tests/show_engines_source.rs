//! `SHOW ENGINES`: a single InnoDB row with the DEFAULT support flag and
//! TiDB's transaction/row-lock/FK description.

use tidb_session::Session;

use crate::support::text_rows as strings;

#[test]
fn show_engines_lists_innodb_as_default() {
    let mut session = Session::new();
    let engines = strings(&mut session, "show engines");
    assert_eq!(engines.len(), 1, "TiDB lists exactly one engine");

    let innodb = &engines[0];
    assert!(innodb.starts_with("InnoDB|DEFAULT"), "{innodb}");
    assert!(
        innodb.contains("Supports transactions, row-level locking, and foreign keys"),
        "{innodb}"
    );
}
