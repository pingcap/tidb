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

//! `pkg/meta/model/masking_policy.go`: the masking-policy metadata.

use chrono::{DateTime, Utc};
use tidb_ast::{CiString, MaskingPolicyRestrictOps};

use crate::schema_state::SchemaState;

/// Go `MaskingPolicyStatus` (a `byte`): whether a masking policy is active.
/// A newtype over `u8` so unknown stored values round-trip and `Display`
/// yields `""` for them, matching Go's `switch` default.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MaskingPolicyStatus(pub u8);

impl MaskingPolicyStatus {
    /// The policy is disabled (Go `MaskingPolicyStatusDisable`, zero value).
    pub const DISABLE: MaskingPolicyStatus = MaskingPolicyStatus(0);
    /// The policy is enabled (Go `MaskingPolicyStatusEnable`).
    pub const ENABLE: MaskingPolicyStatus = MaskingPolicyStatus(1);
    /// Compatibility alias for [`DISABLE`](Self::DISABLE)
    /// (Go `MaskingPolicyStatusDisabled`).
    pub const DISABLED: MaskingPolicyStatus = Self::DISABLE;
    /// Compatibility alias for [`ENABLE`](Self::ENABLE)
    /// (Go `MaskingPolicyStatusEnabled`).
    pub const ENABLED: MaskingPolicyStatus = Self::ENABLE;
}

impl std::fmt::Display for MaskingPolicyStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match *self {
            MaskingPolicyStatus::DISABLE => "DISABLED",
            MaskingPolicyStatus::ENABLE => "ENABLED",
            _ => "",
        })
    }
}

/// The masking-policy type value (Go `MaskingPolicyType`, a named `string`).
/// The known values are exposed as constants; the field type stays `String`
/// so any stored value round-trips, matching Go's open string type.
pub mod masking_policy_type {
    /// Go `MaskingPolicyTypeFull` / `MaskingPolicyTypeMaskFull`.
    pub const FULL: &str = "MASK_FULL";
    /// Go `MaskingPolicyTypePartial` / `MaskingPolicyTypeMaskPartial`.
    pub const PARTIAL: &str = "MASK_PARTIAL";
    /// Go `MaskingPolicyTypeNull` / `MaskingPolicyTypeMaskNull`.
    pub const NULL: &str = "MASK_NULL";
    /// Go `MaskingPolicyTypeDate` / `MaskingPolicyTypeMaskDate`.
    pub const DATE: &str = "MASK_DATE";
    /// Go `MaskingPolicyTypeCustom`.
    pub const CUSTOM: &str = "CUSTOM";
}

/// Go `MaskingPolicyInfo`: the stored definition of a column masking policy.
#[derive(Clone, Debug)]
pub struct MaskingPolicyInfo {
    /// The policy ID.
    pub id: i64,
    /// The policy name.
    pub name: CiString,
    /// The database the target column lives in.
    pub db_name: CiString,
    /// The target table name.
    pub table_name: CiString,
    /// The target table ID.
    pub table_id: i64,
    /// The target column name.
    pub column_name: CiString,
    /// The target column ID.
    pub column_id: i64,
    /// The masking expression.
    pub expression: String,
    /// Whether the policy is enabled.
    pub status: MaskingPolicyStatus,
    /// The masking type (see [`masking_policy_type`] for known values).
    pub masking_type: String,
    /// The operations the policy restricts.
    pub restrict_ops: MaskingPolicyRestrictOps,
    /// When the policy was created.
    pub created_at: DateTime<Utc>,
    /// When the policy was last updated.
    pub updated_at: DateTime<Utc>,
    /// Who created the policy.
    pub created_by: String,
    /// Who last updated the policy.
    pub updated_by: String,
    /// The online-DDL state of the policy object.
    pub state: SchemaState,
}

#[cfg(test)]
mod tests {
    use super::*;

    // Go's MaskingPolicyStatus.String() switch, plus the "" default and the
    // compatibility aliases.
    #[test]
    fn status_strings() {
        assert_eq!(MaskingPolicyStatus::DISABLE.to_string(), "DISABLED");
        assert_eq!(MaskingPolicyStatus::ENABLE.to_string(), "ENABLED");
        assert_eq!(MaskingPolicyStatus(9).to_string(), "");
        assert_eq!(MaskingPolicyStatus::default(), MaskingPolicyStatus::DISABLE);
        assert_eq!(MaskingPolicyStatus::DISABLED, MaskingPolicyStatus::DISABLE);
        assert_eq!(MaskingPolicyStatus::ENABLED, MaskingPolicyStatus::ENABLE);
    }

    #[test]
    fn type_values() {
        assert_eq!(masking_policy_type::FULL, "MASK_FULL");
        assert_eq!(masking_policy_type::PARTIAL, "MASK_PARTIAL");
        assert_eq!(masking_policy_type::NULL, "MASK_NULL");
        assert_eq!(masking_policy_type::DATE, "MASK_DATE");
        assert_eq!(masking_policy_type::CUSTOM, "CUSTOM");
    }
}
