#![cfg(test)]

//! Account, privilege and role behavior, split by the `mysql` system table
//! each group exercises.

mod accounts;
mod column_grants;
mod dynamic_grants;
mod enforcement;
mod password_policy;
mod processlist;
mod roles;
mod static_grants;
mod table_scope;
mod visibility;
