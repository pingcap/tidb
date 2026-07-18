// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use tonic::metadata::MetadataValue;

use super::DirectUnaryClientError;

/// Exact pinned client-go forwarding metadata key.
pub(super) const FORWARD_METADATA_KEY: &str = "tikv-forwarded-host";

/// Attaches the logical target to one fresh physical-proxy request.
///
/// Empty forwarding is omission, not an empty metadata entry. `insert`
/// preserves the stronger invariant that a request can contain at most one
/// value even if construction changes later.
pub(super) fn attach_forwarded_host<T>(
    request: &mut tonic::Request<T>,
    forwarded_host: Option<&str>,
) -> Result<(), DirectUnaryClientError> {
    let Some(forwarded_host) = forwarded_host else {
        return Ok(());
    };
    if forwarded_host.is_empty() {
        return Err(DirectUnaryClientError::InvalidRequest(
            "forwarded TiKV host must not be empty".to_owned(),
        ));
    }
    let value = MetadataValue::try_from(forwarded_host).map_err(|error| {
        DirectUnaryClientError::InvalidRequest(format!(
            "invalid forwarded TiKV host metadata: {error}"
        ))
    })?;
    request.metadata_mut().insert(FORWARD_METADATA_KEY, value);
    Ok(())
}
