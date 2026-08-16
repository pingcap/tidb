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

//! Complete transcreation of Go `pkg/ddl/bdr` (`bdr.go`): the policy deciding
//! whether a DDL is denied under a BDR cluster role.
//!
//! The classification tables themselves already live in [`crate::bdr`]
//! (Go `pkg/meta/model`'s `ActionBDRMap`); this module is the policy that
//! reads them, kept next to those tables because its only other inputs are
//! AST column options and a field type.
//!
//! Go takes the role as `ast.BDRRole`, a string whose zero value is
//! `BDRRoleNone`. `tidb_ast::BdrRole` has no such variant, so the role
//! arrives as an `Option`, where `None` is Go's unset role — the case Go's
//! `default` arm answers "deny nothing".

use tidb_ast::{BdrRole, ColumnOption};
use tidb_datatype::FieldType;

use crate::bdr::{DDLBDRType, ACTION_BDR_MAP};
use crate::ActionType;

/// What Go `IsDenied` reads out of `model.JobArgs`.
///
/// Go receives the open `JobArgs` interface and type-asserts it to
/// `*model.ModifyIndexArgs` to reach `IndexArgs[0].Unique`. That args type is
/// not modeled in this crate yet, so the one fact the policy consumes is
/// passed directly: `None` is Go's nil args, which skips the check entirely.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BdrJobArgs {
    /// Whether the first index in `ModifyIndexArgs.IndexArgs` is `UNIQUE`.
    pub first_index_unique: bool,
}

/// Go `IsAddColumnDenied`.
///
/// Only the primary role restricts anything. A column may be added when it
/// carries no options at all, is explicitly nullable, is nullable-by-omission
/// with a default, or is `NOT NULL` with a default. `COMMENT` and the
/// generated-column option are discounted from the option count before the
/// shape is judged, so either may accompany any allowed form.
#[must_use]
pub fn is_add_column_denied(role: Option<BdrRole>, options: &[ColumnOption]) -> bool {
    if role != Some(BdrRole::Primary) {
        return false;
    }

    let mut nullable = false;
    let mut not_null = false;
    let mut default_value = false;
    let mut comment = 0_usize;
    let mut generated = 0_usize;
    for option in options {
        match option {
            ColumnOption::Default(_) => default_value = true,
            // Go counts these two as at most one each, not once per
            // occurrence: a second COMMENT re-assigns the same flag.
            ColumnOption::Comment(_) => comment = 1,
            ColumnOption::Generated { .. } => generated = 1,
            ColumnOption::NotNull => not_null = true,
            ColumnOption::Null => nullable = true,
            _ => {}
        }
    }
    let type_len = options.len() - comment - generated;

    !(type_len == 0
        || (type_len == 1 && nullable)
        || (type_len == 1 && !not_null && default_value)
        || (type_len == 2 && not_null && default_value))
}

/// Go `IsModifyColumnDenied`.
///
/// Only the primary role restricts anything, and any change to the field type
/// is denied outright. With the type unchanged, the column may change its
/// default value, optionally alongside its comment, and nothing else.
#[must_use]
pub fn is_modify_column_denied(
    role: Option<BdrRole>,
    new_field_type: &FieldType,
    old_field_type: &FieldType,
    options: &[ColumnOption],
) -> bool {
    if role != Some(BdrRole::Primary) {
        return false;
    }

    if !new_field_type.equal(old_field_type) {
        return true;
    }

    let mut default_value = false;
    let mut comment = false;
    for option in options {
        if matches!(option, ColumnOption::Default(_)) {
            default_value = true;
        }
        if matches!(option, ColumnOption::Comment(_)) {
            comment = true;
        }
    }

    if options.len() == 1 && default_value {
        return false;
    }
    if options.len() == 2 && default_value && comment {
        return false;
    }
    true
}

/// Go `IsDenied`: whether a DDL action is denied for this role.
///
/// An unset role denies nothing. Both managed roles deny any action missing
/// from the classification map. The primary role additionally allows only
/// safe and unmanaged DDL, and refuses to add a unique index; the secondary
/// role allows only unmanaged DDL.
#[must_use]
pub fn is_denied(role: Option<BdrRole>, action: ActionType, args: Option<BdrJobArgs>) -> bool {
    let ddl_type = ACTION_BDR_MAP.read().get(&action).cloned();

    match role {
        Some(BdrRole::Primary) => {
            let Some(ddl_type) = ddl_type else {
                return true;
            };

            // A unique index cannot be added on the primary role.
            if let Some(args) = args {
                if (action == ActionType::ACTION_ADD_INDEX
                    || action == ActionType::ACTION_ADD_PRIMARY_KEY)
                    && args.first_index_unique
                {
                    return true;
                }
            }

            !(ddl_type == DDLBDRType::SAFE_DDL || ddl_type == DDLBDRType::UNMANAGEMENT_DDL)
        }
        Some(BdrRole::Secondary) => {
            let Some(ddl_type) = ddl_type else {
                return true;
            };
            ddl_type != DDLBDRType::UNMANAGEMENT_DDL
        }
        // Go: "If user do not set bdr role, we will not deny any ddl as `none`."
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_ast::Expr;
    use tidb_datatype::FieldTypeCode;

    fn default_option() -> ColumnOption {
        ColumnOption::Default(Expr::Null)
    }

    fn comment_option() -> ColumnOption {
        ColumnOption::Comment("c".to_owned())
    }

    fn generated_option() -> ColumnOption {
        ColumnOption::Generated {
            expression: Expr::Null,
            expression_text: b"NULL".to_vec(),
            stored: false,
        }
    }

    // Go `TestIsAddColumnDenied`: a non-primary role never denies, whatever
    // the options say.
    #[test]
    fn add_column_is_only_restricted_on_the_primary_role() {
        let denied_shape = [ColumnOption::NotNull];
        for role in [None, Some(BdrRole::Secondary)] {
            assert!(!is_add_column_denied(role, &denied_shape));
        }
        assert!(is_add_column_denied(Some(BdrRole::Primary), &denied_shape));
    }

    // Go `TestIsAddColumnDenied`'s allowed shapes.
    #[test]
    fn add_column_allows_nullable_and_defaulted_forms() {
        let primary = Some(BdrRole::Primary);
        // No options at all.
        assert!(!is_add_column_denied(primary, &[]));
        // Explicitly nullable.
        assert!(!is_add_column_denied(primary, &[ColumnOption::Null]));
        // Nullable by omission, with a default.
        assert!(!is_add_column_denied(primary, &[default_option()]));
        // NOT NULL with a default.
        assert!(!is_add_column_denied(
            primary,
            &[ColumnOption::NotNull, default_option()]
        ));
    }

    // Go `TestIsAddColumnDenied`'s denied shapes.
    #[test]
    fn add_column_denies_a_bare_not_null_or_extra_options() {
        let primary = Some(BdrRole::Primary);
        // NOT NULL with no default.
        assert!(is_add_column_denied(primary, &[ColumnOption::NotNull]));
        // An unrelated option counts toward the shape.
        assert!(is_add_column_denied(
            primary,
            &[ColumnOption::AutoIncrement]
        ));
        assert!(is_add_column_denied(
            primary,
            &[default_option(), ColumnOption::AutoIncrement]
        ));
    }

    // COMMENT and GENERATED are discounted before the shape is judged, and
    // each counts at most once however often it appears.
    #[test]
    fn add_column_discounts_comment_and_generated_options() {
        let primary = Some(BdrRole::Primary);
        assert!(!is_add_column_denied(primary, &[comment_option()]));
        assert!(!is_add_column_denied(
            primary,
            &[ColumnOption::Null, comment_option()]
        ));
        assert!(!is_add_column_denied(
            primary,
            &[ColumnOption::NotNull, default_option(), comment_option()]
        ));
        assert!(!is_add_column_denied(
            primary,
            &[generated_option(), comment_option()]
        ));
        // Two comments still discount only one, so this stays a bare NOT NULL
        // plus one surplus option and is denied.
        assert!(is_add_column_denied(
            primary,
            &[ColumnOption::NotNull, comment_option(), comment_option()]
        ));
    }

    // Go `TestIsModifyColumnDenied`.
    #[test]
    fn modify_column_is_only_restricted_on_the_primary_role() {
        let long = FieldType::parser(FieldTypeCode::Long);
        let blob = FieldType::parser(FieldTypeCode::Blob);
        for role in [None, Some(BdrRole::Secondary)] {
            assert!(!is_modify_column_denied(role, &long, &blob, &[]));
        }
        assert!(is_modify_column_denied(
            Some(BdrRole::Primary),
            &long,
            &blob,
            &[]
        ));
    }

    // With the type unchanged, only a default change (optionally with a
    // comment) is allowed.
    #[test]
    fn modify_column_allows_only_default_and_comment_changes() {
        let primary = Some(BdrRole::Primary);
        let long = FieldType::parser(FieldTypeCode::Long);
        let same = FieldType::parser(FieldTypeCode::Long);

        assert!(!is_modify_column_denied(
            primary,
            &long,
            &same,
            &[default_option()]
        ));
        assert!(!is_modify_column_denied(
            primary,
            &long,
            &same,
            &[default_option(), comment_option()]
        ));

        // A comment alone is not enough.
        assert!(is_modify_column_denied(
            primary,
            &long,
            &same,
            &[comment_option()]
        ));
        // No options at all is denied too.
        assert!(is_modify_column_denied(primary, &long, &same, &[]));
        // A third option breaks the allowed pair.
        assert!(is_modify_column_denied(
            primary,
            &long,
            &same,
            &[default_option(), comment_option(), ColumnOption::NotNull]
        ));
    }

    // Go `TestIsDenied`: an unset role denies nothing, including actions the
    // classification map has never heard of.
    #[test]
    fn an_unset_role_denies_nothing() {
        assert!(!is_denied(None, ActionType::ACTION_ADD_COLUMN, None));
        assert!(!is_denied(None, ActionType::ACTION_DROP_TABLE, None));
        assert!(!is_denied(None, ActionType::ACTION_NONE, None));
    }

    // The primary role allows safe and unmanaged DDL and denies the rest.
    #[test]
    fn the_primary_role_allows_safe_and_unmanaged_ddl() {
        let primary = Some(BdrRole::Primary);
        // Unclassified actions are denied outright.
        assert!(is_denied(primary, ActionType::ACTION_NONE, None));

        for (action, class) in classified_samples() {
            let denied = is_denied(primary, action, None);
            let expected =
                !(class == DDLBDRType::SAFE_DDL || class == DDLBDRType::UNMANAGEMENT_DDL);
            assert_eq!(denied, expected, "{action:?}");
        }
    }

    // The secondary role allows only unmanaged DDL.
    #[test]
    fn the_secondary_role_allows_only_unmanaged_ddl() {
        let secondary = Some(BdrRole::Secondary);
        assert!(is_denied(secondary, ActionType::ACTION_NONE, None));

        for (action, class) in classified_samples() {
            let denied = is_denied(secondary, action, None);
            assert_eq!(denied, class != DDLBDRType::UNMANAGEMENT_DDL, "{action:?}");
        }
    }

    // Adding a unique index is denied on the primary role even though the
    // action itself is otherwise permitted; nil args skip the check.
    #[test]
    fn the_primary_role_refuses_a_unique_index() {
        let primary = Some(BdrRole::Primary);
        let unique = Some(BdrJobArgs {
            first_index_unique: true,
        });
        let non_unique = Some(BdrJobArgs {
            first_index_unique: false,
        });

        for action in [
            ActionType::ACTION_ADD_INDEX,
            ActionType::ACTION_ADD_PRIMARY_KEY,
        ] {
            assert!(is_denied(primary, action, unique), "{action:?}");
            // Without the unique flag the action falls through to its class.
            let by_class = is_denied(primary, action, None);
            assert_eq!(is_denied(primary, action, non_unique), by_class);
        }

        // The unique check is scoped to those two actions.
        assert_eq!(
            is_denied(primary, ActionType::ACTION_ADD_COLUMN, unique),
            is_denied(primary, ActionType::ACTION_ADD_COLUMN, None)
        );
    }

    /// A sample of actions paired with their class, taken from the shared map
    /// so the expectations follow the source table rather than restating it.
    fn classified_samples() -> Vec<(ActionType, DDLBDRType)> {
        let map = ACTION_BDR_MAP.read();
        let mut samples: Vec<(ActionType, DDLBDRType)> = map
            .iter()
            .map(|(action, class)| (*action, class.clone()))
            .collect();
        samples.sort_by_key(|(action, _)| format!("{action:?}"));
        assert!(!samples.is_empty());
        samples
    }
}
