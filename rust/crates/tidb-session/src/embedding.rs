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

//! Process-wide embedding configuration from Go's `pkg/sessionctx/variable`.
//!
//! The embedding providers read credentials outside a session, so the values
//! are process settings rather than session overrides.  The SQL-facing global
//! variable layer calls [`publish_global`] after validation; reads expose the
//! endpoint's default and redact credentials exactly as Go does.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{OnceLock, RwLock};

use tidb_vardef::{defaults, tidb_vars};

/// Go's endpoint-whitelist diagnostic.
pub const OPENAI_ENDPOINT_WHITELIST_ERR_MSG: &str = "For security reasons currently only OpenAI, Azure OpenAI, or Alibaba Cloud DashScope Endpoint is allowed";

static EMBEDDING_CONFIG_VERSION: AtomicU64 = AtomicU64::new(0);

fn openai_base() -> &'static RwLock<String> {
    static VALUE: OnceLock<RwLock<String>> = OnceLock::new();
    VALUE.get_or_init(|| RwLock::new(String::new()))
}

fn api_key_slot(name: &str) -> Option<&'static RwLock<String>> {
    macro_rules! slot {
        ($name:expr, $id:ident) => {
            if name.eq_ignore_ascii_case($name) {
                static $id: OnceLock<RwLock<String>> = OnceLock::new();
                return Some($id.get_or_init(|| RwLock::new(String::new())));
            }
        };
    }
    slot!(tidb_vars::TIDB_EXP_EMBED_JINA_AI_API_KEY, JINA);
    slot!(tidb_vars::TIDB_EXP_EMBED_OPENAI_API_KEY, OPENAI);
    slot!(tidb_vars::TIDB_EXP_EMBED_COHERE_API_KEY, COHERE);
    slot!(tidb_vars::TIDB_EXP_EMBED_HUGGINGFACE_API_KEY, HUGGINGFACE);
    slot!(tidb_vars::TIDB_EXP_EMBED_NVIDIA_NIM_API_KEY, NVIDIA_NIM);
    slot!(tidb_vars::TIDB_EXP_EMBED_GEMINI_API_KEY, GEMINI);
    None
}

/// Returns true for one of the seven embedding system variables.
#[must_use]
pub fn is_embedding_variable(name: &str) -> bool {
    name.eq_ignore_ascii_case(tidb_vars::TIDB_EXP_EMBED_OPENAI_API_BASE)
        || api_key_slot(name).is_some()
}

/// Validates and canonicalizes Go's OpenAI-compatible endpoint input.
///
/// This deliberately keeps the authority's spelling (including an Azure
/// resource's case and port) while comparing a lowercased hostname against
/// the allowlist.  The SQL layer appends `/embeddings`, so an endpoint-form
/// input is reduced to its base path.
pub fn normalize_openai_embedding_api_base(base: &str) -> Result<String, String> {
    let trimmed = base.trim();
    if trimmed.is_empty() {
        return Ok(String::new());
    }

    let Some(scheme_end) = trimmed.find("://") else {
        return Err(format!(
            "invalid value for {}: absolute https URL is required",
            tidb_vars::TIDB_EXP_EMBED_OPENAI_API_BASE
        ));
    };
    if !trimmed[..scheme_end].eq_ignore_ascii_case("https") {
        return Err(format!(
            "invalid value for {}: only https scheme is supported",
            tidb_vars::TIDB_EXP_EMBED_OPENAI_API_BASE
        ));
    }

    let rest = &trimmed[scheme_end + 3..];
    let authority_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let authority = &rest[..authority_end];
    if authority.is_empty() || authority.chars().any(char::is_whitespace) {
        return Err(format!(
            "invalid value for {}: absolute https URL is required",
            tidb_vars::TIDB_EXP_EMBED_OPENAI_API_BASE
        ));
    }
    if rest[authority_end..].contains('?') || rest[authority_end..].contains('#') {
        return Err(format!(
            "invalid value for {}: query parameters and fragments are not allowed",
            tidb_vars::TIDB_EXP_EMBED_OPENAI_API_BASE
        ));
    }

    // URL.Hostname() discards user-info and a port before the allowlist
    // comparison. Go's `u.Host` also excludes user-info, so strip it before
    // rebuilding the normalized URL while preserving the original host/port
    // spelling (including case and an explicit port).
    let host_with_port = authority.rsplit('@').next().unwrap_or(authority);
    let host = if let Some(stripped) = host_with_port.strip_prefix('[') {
        stripped.split(']').next().unwrap_or(stripped)
    } else {
        host_with_port.split(':').next().unwrap_or(host_with_port)
    }
    .to_ascii_lowercase();
    let allowed = host == "api.openai.com"
        || host == "dashscope.aliyuncs.com"
        || host == "dashscope-intl.aliyuncs.com"
        || host == "dashscope-us.aliyuncs.com"
        || host.ends_with(".openai.azure.com");
    if !allowed {
        return Err(OPENAI_ENDPOINT_WHITELIST_ERR_MSG.to_owned());
    }

    let path = &rest[authority_end..];
    let mut normalized = format!("https://{host_with_port}");
    if !path.is_empty() {
        let mut path = path.to_owned();
        // Go trims one trailing slash from u.Path.
        if path.ends_with('/') {
            path.pop();
        }
        normalized.push_str(&path);
    }
    if normalized.ends_with("/embeddings") {
        normalized.truncate(normalized.len() - "/embeddings".len());
    }
    Ok(normalized)
}

/// Returns the effective OpenAI endpoint, falling back to Go's default.
#[must_use]
pub fn openai_embedding_base_url() -> String {
    let value = openai_base()
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if value.is_empty() {
        defaults::DEF_TIDB_EMBED_OPENAI_API_BASE.to_owned()
    } else {
        value.clone()
    }
}

/// Returns the raw provider key, for provider code that needs credentials.
#[must_use]
pub fn embedding_api_key(name: &str) -> Option<String> {
    api_key_slot(name).map(|slot| {
        slot.read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    })
}

/// Redacts a provider key as Go's `maskEmbeddingAPIKey` does.
#[must_use]
pub fn mask_embedding_api_key(key: &str) -> String {
    if key.is_empty() {
        return String::new();
    }
    if key.len() <= 6 {
        return "******".to_owned();
    }
    // Go slices strings by bytes.  Keep the same byte-oriented suffix while
    // avoiding a UTF-8 boundary panic for a non-ASCII credential.
    let suffix = String::from_utf8_lossy(&key.as_bytes()[key.len() - 4..]);
    format!("******{suffix}")
}

/// Returns the SQL-facing redacted value for an embedding variable.
#[must_use]
pub fn masked_global_value(name: &str) -> Option<String> {
    if name.eq_ignore_ascii_case(tidb_vars::TIDB_EXP_EMBED_OPENAI_API_BASE) {
        return Some(openai_embedding_base_url());
    }
    embedding_api_key(name).map(|value| mask_embedding_api_key(&value))
}

/// Publishes a validated GLOBAL value into the process-wide embedding state.
///
/// The returned boolean indicates whether the effective setting changed, and
/// mirrors Go's `EmbeddingConfigVersion.Inc()` rule.
pub(crate) fn publish_global(name: &str, value: Option<&str>) -> bool {
    let value = value.unwrap_or_default();
    if let Some(slot) = api_key_slot(name) {
        let mut current = slot
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if *current == value {
            return false;
        }
        *current = value.to_owned();
        EMBEDDING_CONFIG_VERSION.fetch_add(1, Ordering::SeqCst);
        return true;
    }
    if name.eq_ignore_ascii_case(tidb_vars::TIDB_EXP_EMBED_OPENAI_API_BASE) {
        let mut current = openai_base()
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let old_effective = if current.is_empty() {
            defaults::DEF_TIDB_EMBED_OPENAI_API_BASE
        } else {
            current.as_str()
        };
        let new_effective = if value.is_empty() {
            defaults::DEF_TIDB_EMBED_OPENAI_API_BASE
        } else {
            value
        };
        let changed = old_effective != new_effective;
        *current = value.to_owned();
        if changed {
            EMBEDDING_CONFIG_VERSION.fetch_add(1, Ordering::SeqCst);
        }
        return changed;
    }
    false
}

/// Current process-wide embedding configuration generation.
#[must_use]
pub fn config_version() -> u64 {
    EMBEDDING_CONFIG_VERSION.load(Ordering::SeqCst)
}
