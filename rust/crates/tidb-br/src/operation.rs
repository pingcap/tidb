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


//! Go `br/pkg/operation`: the identity of one BR operation, which tags the
//! lock files it writes into an external storage.

use std::time::SystemTime;

use tidb_log::{Field, Value};
use tidb_objstore::locking::LockMetaInput;

use crate::errors::{self, Result};

/// Go `LockResourceType`.
pub type LockResourceType = &'static str;
/// Go `LockResourceLogTruncateExclusive`.
pub const LOCK_RESOURCE_LOG_TRUNCATE_EXCLUSIVE: LockResourceType = "log-truncate-exclusive";
/// Go `LockResourceMigrationRead`.
pub const LOCK_RESOURCE_MIGRATION_READ: LockResourceType = "migration-read";
/// Go `LockResourceMigrationWrite`.
pub const LOCK_RESOURCE_MIGRATION_WRITE: LockResourceType = "migration-write";
/// Go `LockResourceMigrationAppend`.
pub const LOCK_RESOURCE_MIGRATION_APPEND: LockResourceType = "migration-append";

/// Go `HintField`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HintField {
    /// Go `Key`.
    pub key: String,
    /// Go `Value`.
    pub value: String,
}

/// Go `Context`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Context {
    /// Go `OperationID`.
    pub operation_id: String,
    /// Go `StartedAt`; `None` is Go's zero time.
    pub started_at: Option<SystemTime>,
    hint_fields: Vec<HintField>,
}

fn rfc3339(t: SystemTime) -> String {
    chrono::DateTime::<chrono::Local>::from(t).to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

/// Go `strconv.Quote` for the hint detail.
fn go_quote(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 || c as u32 == 0x7f => out.push_str(&format!("\\x{:02x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

impl Context {
    /// Go `NewContext`: a fresh id, logged with the command.
    #[must_use]
    pub fn new(command: &str) -> Self {
        let ctx = Self {
            operation_id: uuid::Uuid::new_v4().to_string(),
            started_at: Some(SystemTime::now()),
            hint_fields: Vec::new(),
        };
        let host = tidb_sem::operating_system_hostname().unwrap_or_else(|| "unknown".to_owned());
        tidb_log::info(
            "BR operation started",
            &[
                Field::new("operation_id", Value::Str(ctx.operation_id.clone())),
                Field::new("operation_started_at", Value::Str(ctx.started_at.map(rfc3339).unwrap_or_default())),
                Field::new("host", Value::Str(host)),
                Field::new("pid", Value::U64(u64::from(std::process::id()))),
                Field::new("command", Value::Str(command.to_owned())),
            ],
        );
        ctx
    }

    /// Go `HintFields`.
    #[must_use]
    pub fn hint_fields(&self) -> Vec<HintField> {
        self.hint_fields.clone()
    }

    /// Go `SetHintField`: an empty value removes the field.
    pub fn set_hint_field(&mut self, key: &str, value: &str) {
        if key.is_empty() || (!value.is_empty() && !self.is_initialized()) {
            return;
        }
        let idx = self.hint_fields.iter().position(|f| f.key == key);
        if value.is_empty() {
            if let Some(i) = idx {
                self.hint_fields.remove(i);
            }
            return;
        }
        match idx {
            Some(i) => value.clone_into(&mut self.hint_fields[i].value),
            None => self.hint_fields.push(HintField { key: key.to_owned(), value: value.to_owned() }),
        }
    }

    fn is_initialized(&self) -> bool {
        !self.operation_id.is_empty() && self.started_at.is_some()
    }

    /// Go `LockMeta`: the lock metadata of this operation.
    ///
    /// # Errors
    /// The context is not initialized, or `resource` is empty.
    pub fn lock_meta(&self, resource: LockResourceType, hint: &str) -> Result<LockMetaInput> {
        if self.operation_id.is_empty() {
            return Err(errors::errorf("operation ID is required"));
        }
        let Some(started_at) = self.started_at else {
            return Err(errors::errorf("operation started time is required"));
        };
        if resource.is_empty() {
            return Err(errors::errorf("lock resource type is required"));
        }
        let mut fields = vec![format!("operation_started_at={}", rfc3339(started_at))];
        for field in &self.hint_fields {
            if !field.key.is_empty() && !field.value.is_empty() {
                fields.push(format!("{}={}", field.key, field.value));
            }
        }
        if !hint.is_empty() {
            fields.push(format!("detail={}", go_quote(hint)));
        }
        Ok(LockMetaInput { owner_id: self.operation_id.clone(), lock_type: resource.to_owned(), hint: fields.join(" ") })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Go `TestLockMeta`. The start time renders in the local zone, where
    /// Go renders it in the time's own location.
    #[test]
    fn test_lock_meta() {
        let mut ctx = Context {
            operation_id: "operation-id".into(),
            started_at: Some(SystemTime::now()),
            hint_fields: Vec::new(),
        };
        ctx.set_hint_field("lineage_id", "123");
        let meta = ctx.lock_meta(LOCK_RESOURCE_MIGRATION_READ, "test hint").unwrap();
        assert_eq!(meta.owner_id, "operation-id");
        assert_eq!(meta.lock_type, LOCK_RESOURCE_MIGRATION_READ);
        assert!(meta.hint.starts_with("operation_started_at="), "{}", meta.hint);
        assert!(meta.hint.contains("lineage_id=123"), "{}", meta.hint);
        assert!(meta.hint.contains(r#"detail="test hint""#), "{}", meta.hint);
    }

    /// Go `TestLockMetaValidation`.
    #[test]
    fn test_lock_meta_validation() {
        let cases = [
            (Context::default(), LOCK_RESOURCE_MIGRATION_READ, "operation ID"),
            (Context { operation_id: "operation-id".into(), ..Context::default() }, LOCK_RESOURCE_MIGRATION_READ, "operation started time"),
            (
                Context { operation_id: "operation-id".into(), started_at: Some(SystemTime::now()), hint_fields: Vec::new() },
                "",
                "resource type",
            ),
        ];
        for (ctx, resource, expected) in cases {
            let err = ctx.lock_meta(resource, "hint").unwrap_err();
            assert!(err.to_string().contains(expected), "{err}");
        }
    }
}
