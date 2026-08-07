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

//! LOCKDOWN INVENTORY: `pkg/meta/model/masking_policy.go` -> `masking_policy.rs`.
//!
//! Go is authoritative. Every named production declaration, every function,
//! and every branch outcome in the 93-line owner has exactly one verdict. The
//! source has no adjacent `masking_policy_test.go`; byte-exact direct-Go JSON
//! probes and boundary tests are checked in with the Rust implementation.
//!
//! The hash, size, function-list, inventory-cardinality, serde, and compile
//! gates make any owner drift or disappeared PORTED symbol fail this crate.

use sha2::{Digest, Sha256};

use crate::{
    clone_masking_policy_info, MaskingPolicyInfo, MaskingPolicyStatus, MaskingPolicyType,
    SchemaState,
};
use tidb_ast::{CiString, MaskingPolicyRestrictOps};

#[allow(dead_code)] // This owner currently has no declined or unreachable rows.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Verdict {
    Ported,
    Declined,
    Unreachable,
}

type Row = (&'static str, Verdict, &'static str);

const GO_SOURCE: &str = include_str!("../../../../pkg/meta/model/masking_policy.go");
const GO_SOURCE_SHA256: &str = "6680572e9eefa1aff3c71c2bedf5fb4ef6741ff2993135f77c03850d4b99cae4";
const GO_SOURCE_BYTES: usize = 3_311;
const GO_SOURCE_LINES: usize = 93;

const DECLARATIONS: &[Row] = &[
    (
        "MaskingPolicyStatus",
        Verdict::Ported,
        "MaskingPolicyStatus",
    ),
    (
        "MaskingPolicyStatusDisable",
        Verdict::Ported,
        "MaskingPolicyStatus::DISABLE",
    ),
    (
        "MaskingPolicyStatusEnable",
        Verdict::Ported,
        "MaskingPolicyStatus::ENABLE",
    ),
    (
        "MaskingPolicyStatusDisabled",
        Verdict::Ported,
        "MaskingPolicyStatus::DISABLED",
    ),
    (
        "MaskingPolicyStatusEnabled",
        Verdict::Ported,
        "MaskingPolicyStatus::ENABLED",
    ),
    ("MaskingPolicyType", Verdict::Ported, "MaskingPolicyType"),
    (
        "MaskingPolicyTypeFull",
        Verdict::Ported,
        "MaskingPolicyType::FULL",
    ),
    (
        "MaskingPolicyTypePartial",
        Verdict::Ported,
        "MaskingPolicyType::PARTIAL",
    ),
    (
        "MaskingPolicyTypeNull",
        Verdict::Ported,
        "MaskingPolicyType::NULL",
    ),
    (
        "MaskingPolicyTypeDate",
        Verdict::Ported,
        "MaskingPolicyType::DATE",
    ),
    (
        "MaskingPolicyTypeCustom",
        Verdict::Ported,
        "MaskingPolicyType::CUSTOM",
    ),
    (
        "MaskingPolicyTypeMaskFull",
        Verdict::Ported,
        "MaskingPolicyType::MASK_FULL",
    ),
    (
        "MaskingPolicyTypeMaskPartial",
        Verdict::Ported,
        "MaskingPolicyType::MASK_PARTIAL",
    ),
    (
        "MaskingPolicyTypeMaskNull",
        Verdict::Ported,
        "MaskingPolicyType::MASK_NULL",
    ),
    (
        "MaskingPolicyTypeMaskDate",
        Verdict::Ported,
        "MaskingPolicyType::MASK_DATE",
    ),
    ("MaskingPolicyInfo", Verdict::Ported, "MaskingPolicyInfo"),
    (
        "MaskingPolicyInfo.ID",
        Verdict::Ported,
        "MaskingPolicyInfo::id",
    ),
    (
        "MaskingPolicyInfo.Name",
        Verdict::Ported,
        "MaskingPolicyInfo::name",
    ),
    (
        "MaskingPolicyInfo.DBName",
        Verdict::Ported,
        "MaskingPolicyInfo::db_name",
    ),
    (
        "MaskingPolicyInfo.TableName",
        Verdict::Ported,
        "MaskingPolicyInfo::table_name",
    ),
    (
        "MaskingPolicyInfo.TableID",
        Verdict::Ported,
        "MaskingPolicyInfo::table_id",
    ),
    (
        "MaskingPolicyInfo.ColumnName",
        Verdict::Ported,
        "MaskingPolicyInfo::column_name",
    ),
    (
        "MaskingPolicyInfo.ColumnID",
        Verdict::Ported,
        "MaskingPolicyInfo::column_id",
    ),
    (
        "MaskingPolicyInfo.Expression",
        Verdict::Ported,
        "MaskingPolicyInfo::expression",
    ),
    (
        "MaskingPolicyInfo.Status",
        Verdict::Ported,
        "MaskingPolicyInfo::status",
    ),
    (
        "MaskingPolicyInfo.MaskingType",
        Verdict::Ported,
        "MaskingPolicyInfo::masking_type",
    ),
    (
        "MaskingPolicyInfo.RestrictOps",
        Verdict::Ported,
        "MaskingPolicyInfo::restrict_ops",
    ),
    (
        "MaskingPolicyInfo.CreatedAt",
        Verdict::Ported,
        "MaskingPolicyInfo::created_at",
    ),
    (
        "MaskingPolicyInfo.UpdatedAt",
        Verdict::Ported,
        "MaskingPolicyInfo::updated_at",
    ),
    (
        "MaskingPolicyInfo.CreatedBy",
        Verdict::Ported,
        "MaskingPolicyInfo::created_by",
    ),
    (
        "MaskingPolicyInfo.UpdatedBy",
        Verdict::Ported,
        "MaskingPolicyInfo::updated_by",
    ),
    (
        "MaskingPolicyInfo.State",
        Verdict::Ported,
        "MaskingPolicyInfo::state",
    ),
];

const FUNCTIONS: &[Row] = &[
    (
        "(s MaskingPolicyStatus) String",
        Verdict::Ported,
        "Display for MaskingPolicyStatus",
    ),
    (
        "(p *MaskingPolicyInfo) Clone",
        Verdict::Ported,
        "clone_masking_policy_info",
    ),
];

const BRANCHES: &[Row] = &[
    (
        "MaskingPolicyStatus.String:L39:case:disable",
        Verdict::Ported,
        "masking_policy::tests::status_strings",
    ),
    (
        "MaskingPolicyStatus.String:L41:case:enable",
        Verdict::Ported,
        "masking_policy::tests::status_strings",
    ),
    (
        "MaskingPolicyStatus.String:L43:default",
        Verdict::Ported,
        "masking_policy::tests::status_strings",
    ),
    (
        "MaskingPolicyInfo.Clone:L88:if:nil",
        Verdict::Ported,
        "masking_policy::tests::clone_matches_value_copy_and_nil_receiver",
    ),
    (
        "MaskingPolicyInfo.Clone:L88:if:non-nil",
        Verdict::Ported,
        "masking_policy::tests::clone_matches_value_copy_and_nil_receiver",
    ),
];

const SOURCE_TEST_RECEIPTS: &[Row] = &[
    (
        "direct-go:zero-and-missing-json",
        Verdict::Ported,
        "masking_policy::tests::zero_json_matches_go_and_null_time_decodes_to_zero",
    ),
    (
        "direct-go:full-field-json-boundaries",
        Verdict::Ported,
        "masking_policy::tests::full_json_matches_go_at_every_field_boundary",
    ),
    (
        "direct-go:nil-and-non-nil-clone",
        Verdict::Ported,
        "masking_policy::tests::clone_matches_value_copy_and_nil_receiver",
    ),
];

fn go_functions(source: &str) -> Vec<String> {
    source
        .lines()
        .filter_map(|line| line.trim_start().strip_prefix("func "))
        .map(|declaration| {
            if let Some(after_receiver) = declaration.strip_prefix('(') {
                let receiver_end = after_receiver.find(") ").expect("Go receiver terminates");
                let receiver = &declaration[..receiver_end + 2];
                let method = &after_receiver[receiver_end + 2..];
                format!(
                    "{receiver} {}",
                    method.split_once('(').expect("Go method has arguments").0
                )
            } else {
                declaration
                    .split_once('(')
                    .expect("Go function has arguments")
                    .0
                    .to_owned()
            }
        })
        .collect()
}

#[test]
fn masking_policy_go_source_identity_is_current() {
    assert_eq!(GO_SOURCE.len(), GO_SOURCE_BYTES);
    assert_eq!(GO_SOURCE.lines().count(), GO_SOURCE_LINES);
    assert_eq!(
        format!("{:x}", Sha256::digest(GO_SOURCE.as_bytes())),
        GO_SOURCE_SHA256
    );
}

#[test]
fn every_go_function_and_branch_has_exactly_one_verdict() {
    let inventory_functions = FUNCTIONS
        .iter()
        .map(|(name, _, _)| (*name).to_owned())
        .collect::<Vec<_>>();
    assert_eq!(go_functions(GO_SOURCE), inventory_functions);
    assert_eq!(BRANCHES.len(), 5);

    let mut names = std::collections::BTreeSet::new();
    for (name, verdict, receipt) in DECLARATIONS
        .iter()
        .chain(FUNCTIONS)
        .chain(BRANCHES)
        .chain(SOURCE_TEST_RECEIPTS)
    {
        assert!(matches!(
            verdict,
            Verdict::Ported | Verdict::Declined | Verdict::Unreachable
        ));
        assert!(!receipt.is_empty());
        assert!(names.insert(*name), "duplicate inventory row: {name}");
    }
    assert_eq!(DECLARATIONS.len(), 32);
}

#[test]
fn every_ported_masking_policy_symbol_still_compiles() {
    fn assert_serde<T: serde::Serialize + for<'de> serde::Deserialize<'de>>() {}
    assert_serde::<MaskingPolicyStatus>();
    assert_serde::<MaskingPolicyType>();
    assert_serde::<MaskingPolicyInfo>();

    let _: fn(Option<&MaskingPolicyInfo>) -> Option<MaskingPolicyInfo> = clone_masking_policy_info;
    assert_eq!(std::mem::size_of::<MaskingPolicyStatus>(), 1);
    assert_eq!(MaskingPolicyStatus::DISABLE.0, 0);
    assert_eq!(MaskingPolicyStatus::ENABLE.0, 1);
    assert_eq!(MaskingPolicyStatus::DISABLED, MaskingPolicyStatus::DISABLE);
    assert_eq!(MaskingPolicyStatus::ENABLED, MaskingPolicyStatus::ENABLE);
    assert_eq!(MaskingPolicyType::MASK_FULL, MaskingPolicyType::FULL);
    assert_eq!(MaskingPolicyType::MASK_PARTIAL, MaskingPolicyType::PARTIAL);
    assert_eq!(MaskingPolicyType::MASK_NULL, MaskingPolicyType::NULL);
    assert_eq!(MaskingPolicyType::MASK_DATE, MaskingPolicyType::DATE);

    let policy = MaskingPolicyInfo::default();
    let _: &i64 = &policy.id;
    let _: &CiString = &policy.name;
    let _: &CiString = &policy.db_name;
    let _: &CiString = &policy.table_name;
    let _: &i64 = &policy.table_id;
    let _: &CiString = &policy.column_name;
    let _: &i64 = &policy.column_id;
    let _: &String = &policy.expression;
    let _: &MaskingPolicyStatus = &policy.status;
    let _: &MaskingPolicyType = &policy.masking_type;
    let _: &MaskingPolicyRestrictOps = &policy.restrict_ops;
    let _ = &policy.created_at;
    let _ = &policy.updated_at;
    let _: &String = &policy.created_by;
    let _: &String = &policy.updated_by;
    let _: &SchemaState = &policy.state;
}
