//! Go `pkg/keyspace/keyspace_test.go`. Separate executable to reset Go's once state.

/// Go `TestSetKeyspaceNameInConf`.
#[test]
fn test_set_keyspace_name_in_conf() {
    tidb_config::config_tree::config::update_global(|config| {
        config.keyspace_name = "test_keyspace_cfg".to_owned();
    });
    let name = tidb_keyspace::get_keyspace_name_by_settings();
    assert_eq!(name, "test_keyspace_cfg");
    assert!(!tidb_keyspace::is_keyspace_name_empty(&name));
    let expected = tidb_config_kerneltype::is_next_gen().then_some(name.as_bytes());
    assert_eq!(
        tidb_keyspace::get_keyspace_name_bytes_by_settings(),
        expected
    );
    // Go sync.Once retains the first result after configuration changes.
    tidb_config::config_tree::config::update_global(|config| config.keyspace_name.clear());
    assert_eq!(
        tidb_keyspace::get_keyspace_name_bytes_by_settings(),
        expected
    );
}
