//! Stable Cargo shard for independently owned privilege and identity selectors.

#[path = "selectors/security/alter_user_full_selector.rs"]
mod alter_user_full;
#[path = "selectors/security/grant_identified_by_selector.rs"]
mod grant_identified_by;
#[path = "selectors/security/grant_privilege_selector.rs"]
mod grant_privilege;
#[path = "selectors/security/grant_role_selector.rs"]
mod grant_role;
#[path = "selectors/security/grant_tls_selector.rs"]
mod grant_tls;
#[path = "selectors/security/rename_user_selector.rs"]
mod rename_user;
#[path = "selectors/security/revoke_all_grant_option_selector.rs"]
mod revoke_all_grant_option;
#[path = "selectors/security/revoke_dynamic_privilege_selector.rs"]
mod revoke_dynamic_privilege;
#[path = "selectors/security/revoke_privilege_selector.rs"]
mod revoke_privilege;
#[path = "selectors/security/revoke_role_selector.rs"]
mod revoke_role;
