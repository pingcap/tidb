//! Stable Cargo shard for independently owned SET parser selectors.

#[path = "selectors/set/set_charset_selector.rs"]
mod set_charset;
#[path = "selectors/set/set_password_selector.rs"]
mod set_password;
#[path = "selectors/set/set_resource_session_states_selector.rs"]
mod set_resource_session_states;
#[path = "selectors/set/set_restore_mismatch_selector.rs"]
mod set_restore_mismatch;
#[path = "selectors/set/set_role_selector.rs"]
mod set_role;
#[path = "selectors/set/set_transaction_snapshot_selector.rs"]
mod set_transaction_snapshot;
