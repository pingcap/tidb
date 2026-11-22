// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Go `variable`'s function slots that the domain fills in
//! `initDomainSysVars`: variables whose `SetGlobal` needs the domain call
//! through them, and a slot nobody filled is Go's noop.

use std::sync::{Arc, RwLock};
use std::time::Duration;

use crate::error::VarError;

/// Go `variable.SetPDClientDynamicOption`'s function type: the variable's
/// name and its validated value.
pub type PdClientDynamicOption = Arc<dyn Fn(&str, &str) -> Result<(), VarError> + Send + Sync>;

/// Go `variable.SetLowResolutionTSOUpdateInterval`'s function type.
pub type LowResolutionTsoUpdateInterval = Arc<dyn Fn(Duration) -> Result<(), VarError> + Send + Sync>;

static SET_PD_CLIENT_DYNAMIC_OPTION: RwLock<Option<PdClientDynamicOption>> = RwLock::new(None);
static SET_LOW_RESOLUTION_TSO_UPDATE_INTERVAL: RwLock<Option<LowResolutionTsoUpdateInterval>> =
    RwLock::new(None);

/// Go `variable.SetPDClientDynamicOption.Store(&do.setPDClientDynamicOption)`.
pub fn set_pd_client_dynamic_option(option: PdClientDynamicOption) {
    *SET_PD_CLIENT_DYNAMIC_OPTION
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(option);
}

/// Go `(*SetPDClientDynamicOption.Load())(name, val)`.
///
/// # Errors
///
/// The domain's refusal.
pub fn pd_client_dynamic_option(name: &str, value: &str) -> Result<(), VarError> {
    let option = SET_PD_CLIENT_DYNAMIC_OPTION
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    option.map_or(Ok(()), |option| option(name, value))
}

/// Go `variable.SetLowResolutionTSOUpdateInterval = do.setLowResolutionTSOUpdateInterval`.
pub fn set_low_resolution_tso_update_interval_func(set: LowResolutionTsoUpdateInterval) {
    *SET_LOW_RESOLUTION_TSO_UPDATE_INTERVAL
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(set);
}

/// Go `if SetLowResolutionTSOUpdateInterval != nil { return SetLowResolutionTSOUpdateInterval(interval) }`.
///
/// # Errors
///
/// The domain's refusal.
pub fn low_resolution_tso_update_interval(interval: Duration) -> Result<(), VarError> {
    let set = SET_LOW_RESOLUTION_TSO_UPDATE_INTERVAL
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    set.map_or(Ok(()), |set| set(interval))
}

static EXTERNAL_TIMESTAMP: RwLock<Option<Arc<dyn crate::vars::ExternalTimestampProvider>>> = RwLock::new(None);

/// Go `variable.SetExternalTimestamp = do.setExternalTimestamp` and
/// `variable.GetExternalTimestamp = do.getExternalTimestamp`
/// (`domain_sysvars.go`).
pub fn set_external_timestamp_funcs(provider: Arc<dyn crate::vars::ExternalTimestampProvider>) {
    *EXTERNAL_TIMESTAMP
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(provider);
}

fn external_timestamp() -> Option<Arc<dyn crate::vars::ExternalTimestampProvider>> {
    EXTERNAL_TIMESTAMP
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone()
}

/// Go `variable.SetExternalTimestamp(ctx, ts)`; no domain filled it: noop.
///
/// # Errors
///
/// The storage refused the timestamp.
pub fn set_external_timestamp(ts: u64) -> Result<(), VarError> {
    external_timestamp().map_or(Ok(()), |provider| provider.set(ts).map_err(VarError::unknown))
}

/// Go `variable.GetExternalTimestamp(ctx)`; no domain filled it: Go's
/// `DefTiDBExternalTS`.
///
/// # Errors
///
/// The storage could not answer.
pub fn get_external_timestamp() -> Result<u64, VarError> {
    external_timestamp().map_or(Ok(0), |provider| provider.get().map_err(VarError::unknown))
}
