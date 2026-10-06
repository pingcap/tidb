//! SHOW CREATE for generated columns: STORED prints
//! `GENERATED ALWAYS AS (...) STORED` and VIRTUAL prints
//! `GENERATED ALWAYS AS (...) VIRTUAL`.

use tidb_session::Session;

use crate::support::byte_lines as strings;

#[test]
fn stored_and_virtual_generated_show_create() {
    let mut session = Session::new();
    session
        .run("create table st (a int primary key, b int as (a * 3) stored)")
        .unwrap();
    let stored = strings(&mut session, "show create table st");
    assert!(stored.contains("GENERATED ALWAYS AS (`a` * 3) STORED"), "{stored}");

    session
        .run("create table vt (a int primary key, b int as (a * 3) virtual)")
        .unwrap();
    let virt = strings(&mut session, "show create table vt");
    assert!(virt.contains("GENERATED ALWAYS AS (`a` * 3) VIRTUAL"), "{virt}");
}
