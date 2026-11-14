//! Go `pkg/keyspace/keyspace_test.go`.

/// Go `TestUsernamePolicy`.
#[test]
fn test_username_policy() {
    tidb_config::config_tree::config::update_global(|config| config.keyspace_name = "ks".into());
    let policy = tidb_keyspace::get_username_policy();
    assert!(policy.validate_username("user").is_ok());
    assert!(policy.get_username_variants("user").is_empty());
    assert!(policy.get_original_username("ks.user").is_empty());
    if !tidb_config_kerneltype::is_next_gen() {
        return;
    }
    tidb_config_deploymode::set(tidb_config_deploymode::Mode::Starter).unwrap();
    let policy = tidb_keyspace::get_username_policy();
    assert!(policy.validate_username("ks.user").is_ok());
    assert!(policy.validate_username_format("other.user"));
    assert!(!policy.validate_username_format("other.user.extra"));
    assert_eq!(
        policy.validate_username("user").unwrap_err().to_string(),
        "[ddl:1468]User name must start with `ks.` (use `ks.user` instead)"
    );
    assert_eq!(policy.get_username_variants("user"), ["ks.user"]);
    assert!(policy.get_username_variants("ks.user").is_empty());
    assert_eq!(
        policy.get_username_variants("other.user"),
        ["ks.other.user"]
    );
    assert_eq!(
        policy.get_username_variants("other.user.extra"),
        ["ks.other.user.extra"]
    );
    assert_eq!(policy.get_original_username("ks.user"), "user");
}
