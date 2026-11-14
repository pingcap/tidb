//! Go `pkg/keyspace/keyspace_test.go`. Separate executable to reset Go's once state.

/// Go `TestNoKeyspaceNameSet`.
#[test]
fn test_no_keyspace_name_set() {
    tidb_config::config_tree::config::update_global(|config| config.keyspace_name.clear());
    let name = tidb_keyspace::get_keyspace_name_by_settings();
    assert_eq!(name, "");
    assert!(tidb_keyspace::is_keyspace_name_empty(&name));
    assert_eq!(
        tidb_keyspace::get_keyspace_name_bytes_by_settings(),
        tidb_config_kerneltype::is_next_gen().then_some(&b""[..]),
    );
}
