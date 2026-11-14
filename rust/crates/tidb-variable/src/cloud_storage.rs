// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `variable.ValidateCloudStorageURI` and the cloud-storage sysvar hooks.
use std::sync::OnceLock;
use tidb_gocontext::Context;

/// Go `variable.ValidateCloudStorageURI`, installed by `objstore.init`.
pub type Validator = fn(&Context, &str) -> Result<(), String>;
static VALIDATE_CLOUD_STORAGE_URI: OnceLock<Validator> = OnceLock::new();

/// Go `objstore.init`: installs its validation function in the variable package.
pub fn install_validator(validator: Validator) {
    let _ = VALIDATE_CLOUD_STORAGE_URI.set(validator);
}

/// Go `TiDBCloudStorageURI.SetGlobal`: validate only a changed nonempty URI.
pub fn validate_cloud_storage_uri(ctx: &Context, value: &str) -> Result<(), String> {
    let current = tidb_vardef::CLOUD_STORAGE_URI
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if value.is_empty() || value == *current {
        return Ok(());
    }
    drop(current);
    VALIDATE_CLOUD_STORAGE_URI
        .get()
        .expect("objstore.init installs ValidateCloudStorageURI")(ctx, value)
}

/// Go `TiDBCloudStorageURI.SetGlobal`: failures leave the prior URI intact.
pub fn set_cloud_storage_uri(ctx: &Context, value: &str) -> Result<(), String> {
    validate_cloud_storage_uri(ctx, value)?;
    *tidb_vardef::CLOUD_STORAGE_URI
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = value.to_owned();
    Ok(())
}

/// Go `TiDBCloudStorageURI.GetGlobal`: credentials stay in the runtime value.
pub fn get_cloud_storage_uri() -> String {
    let value = tidb_vardef::CLOUD_STORAGE_URI
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if value.is_empty() {
        String::new()
    } else {
        tidb_ast::redact_url(&value)
    }
}
