//! SHOW DATABASES with a LIKE filter: the user database answers by its
//! stored name and the system schema answers by its UPPERCASE display
//! name — both filtered from the full listing.

use tidb_session::Session;

use crate::support::byte_rows_joined as rows;

#[test]
fn like_filter_and_system_display_name() {
    let mut session = Session::new();
    session.run("create database app_db").unwrap();

    // The user database answers under its created name.
    assert_eq!(rows(&mut session, "show databases like 'app%'"), "app_db");

    // The system schema displays as INFORMATION_SCHEMA.
    assert_eq!(
        rows(&mut session, "show databases like 'information_schema'"),
        "INFORMATION_SCHEMA"
    );
}
