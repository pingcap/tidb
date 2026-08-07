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

//! LOCKDOWN INVENTORY: `pkg/meta/model/resource_group.go` ->
//! `resource_group.rs`.
//!
//! Go is authoritative. The source has no adjacent
//! `pkg/meta/model/resource_group_test.go`; direct-Go probes establish the
//! JSON, string, default, integer-width, and shallow-clone boundaries.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    ResourceGroupBackgroundSettings, ResourceGroupInfo, ResourceGroupRunawayAction,
    ResourceGroupRunawaySettings, ResourceGroupRunawayWatch, ResourceGroupSettings,
    ResourceGroupShared, SchemaState,
};
use tidb_ast::CiString;

#[allow(dead_code)] // This lockdown has no declined or unreachable rows.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Verdict {
    Ported,
    Declined,
    Unreachable,
}

type Row = (&'static str, Verdict, &'static str);

const GO_SOURCE: &str = include_str!("../../../../pkg/meta/model/resource_group.go");
const GO_SOURCE_SHA256: &str = "f1275f07a14c09e4b5c608f896c6620e5c8525b4bbd2b4f51e94e076208bae47";
const GO_SOURCE_BYTES: usize = 6_612;
const GO_SOURCE_LINES: usize = 191;

// Ordered named declarations. Functions are inventoried separately because
// their ordered list is also derived mechanically from the owner below.
const DECLARATIONS: &[Row] = &[
    ("unlimitedRURate", Verdict::Ported, "UNLIMITED_RU_RATE"),
    (
        "ResourceGroupRunawaySettings",
        Verdict::Ported,
        "ResourceGroupRunawaySettings",
    ),
    (
        "ResourceGroupRunawaySettings.ExecElapsedTimeMs",
        Verdict::Ported,
        "ResourceGroupRunawaySettings::exec_elapsed_time_ms",
    ),
    (
        "ResourceGroupRunawaySettings.ProcessedKeys",
        Verdict::Ported,
        "ResourceGroupRunawaySettings::processed_keys",
    ),
    (
        "ResourceGroupRunawaySettings.RequestUnit",
        Verdict::Ported,
        "ResourceGroupRunawaySettings::request_unit",
    ),
    (
        "ResourceGroupRunawaySettings.Action",
        Verdict::Ported,
        "ResourceGroupRunawaySettings::action",
    ),
    (
        "ResourceGroupRunawaySettings.SwitchGroupName",
        Verdict::Ported,
        "ResourceGroupRunawaySettings::switch_group_name",
    ),
    (
        "ResourceGroupRunawaySettings.WatchType",
        Verdict::Ported,
        "ResourceGroupRunawaySettings::watch_type",
    ),
    (
        "ResourceGroupRunawaySettings.WatchDurationMs",
        Verdict::Ported,
        "ResourceGroupRunawaySettings::watch_duration_ms",
    ),
    (
        "ResourceGroupBackgroundSettings",
        Verdict::Ported,
        "ResourceGroupBackgroundSettings",
    ),
    (
        "ResourceGroupBackgroundSettings.JobTypes",
        Verdict::Ported,
        "ResourceGroupBackgroundSettings::job_types",
    ),
    (
        "ResourceGroupBackgroundSettings.ResourceUtilLimit",
        Verdict::Ported,
        "ResourceGroupBackgroundSettings::resource_util_limit",
    ),
    (
        "ResourceGroupSettings",
        Verdict::Ported,
        "ResourceGroupSettings",
    ),
    (
        "ResourceGroupSettings.RURate",
        Verdict::Ported,
        "ResourceGroupSettings::ru_rate",
    ),
    (
        "ResourceGroupSettings.Priority",
        Verdict::Ported,
        "ResourceGroupSettings::priority",
    ),
    (
        "ResourceGroupSettings.CPULimiter",
        Verdict::Ported,
        "ResourceGroupSettings::cpu_limiter",
    ),
    (
        "ResourceGroupSettings.IOReadBandwidth",
        Verdict::Ported,
        "ResourceGroupSettings::io_read_bandwidth",
    ),
    (
        "ResourceGroupSettings.IOWriteBandwidth",
        Verdict::Ported,
        "ResourceGroupSettings::io_write_bandwidth",
    ),
    (
        "ResourceGroupSettings.BurstLimit",
        Verdict::Ported,
        "ResourceGroupSettings::burst_limit",
    ),
    (
        "ResourceGroupSettings.Runaway",
        Verdict::Ported,
        "ResourceGroupSettings::runaway",
    ),
    (
        "ResourceGroupSettings.Background",
        Verdict::Ported,
        "ResourceGroupSettings::background",
    ),
    ("ResourceGroupInfo", Verdict::Ported, "ResourceGroupInfo"),
    (
        "ResourceGroupInfo.ResourceGroupSettings",
        Verdict::Ported,
        "ResourceGroupInfo::settings",
    ),
    (
        "ResourceGroupInfo.ID",
        Verdict::Ported,
        "ResourceGroupInfo::id",
    ),
    (
        "ResourceGroupInfo.Name",
        Verdict::Ported,
        "ResourceGroupInfo::name",
    ),
    (
        "ResourceGroupInfo.State",
        Verdict::Ported,
        "ResourceGroupInfo::state",
    ),
];

const FUNCTIONS: &[Row] = &[
    (
        "(p *ResourceGroupSettings) GetBurstLimitAdjusted",
        Verdict::Ported,
        "ResourceGroupSettings::get_burst_limit_adjusted",
    ),
    (
        "NewResourceGroupSettings",
        Verdict::Ported,
        "ResourceGroupSettings::new",
    ),
    (
        "(p *ResourceGroupSettings) String",
        Verdict::Ported,
        "Display for ResourceGroupSettings",
    ),
    (
        "(p *ResourceGroupSettings) Adjust",
        Verdict::Ported,
        "ResourceGroupSettings::adjust",
    ),
    (
        "(p *ResourceGroupSettings) Clone",
        Verdict::Ported,
        "Clone for ResourceGroupSettings",
    ),
    (
        "(p *ResourceGroupInfo) Clone",
        Verdict::Ported,
        "Clone for ResourceGroupInfo",
    ),
];

// One row per syntactic branch outcome: nineteen `if` statements contribute
// 38 outcomes and the one switch contributes three cases.
const BRANCHES: &[Row] = &[
    (
        "GetBurstLimitAdjusted:L59:if:unlimited",
        Verdict::Ported,
        "resource_group::tests::constructors_and_adjustment_boundaries",
    ),
    (
        "GetBurstLimitAdjusted:L59:if:limited",
        Verdict::Ported,
        "resource_group::tests::constructors_and_adjustment_boundaries",
    ),
    (
        "String:L83:if:ru-present",
        Verdict::Ported,
        "resource_group_lockdown::settings_string_pins_every_source_branch_boundary",
    ),
    (
        "String:L83:if:ru-zero",
        Verdict::Ported,
        "resource_group_lockdown::settings_string_pins_every_source_branch_boundary",
    ),
    (
        "String:L87:if:cpu-present",
        Verdict::Ported,
        "resource_group_lockdown::settings_string_pins_every_source_branch_boundary",
    ),
    (
        "String:L87:if:cpu-empty",
        Verdict::Ported,
        "resource_group_lockdown::settings_string_pins_every_source_branch_boundary",
    ),
    (
        "String:L90:if:read-present",
        Verdict::Ported,
        "resource_group_lockdown::settings_string_pins_every_source_branch_boundary",
    ),
    (
        "String:L90:if:read-empty",
        Verdict::Ported,
        "resource_group_lockdown::settings_string_pins_every_source_branch_boundary",
    ),
    (
        "String:L93:if:write-present",
        Verdict::Ported,
        "resource_group_lockdown::settings_string_pins_every_source_branch_boundary",
    ),
    (
        "String:L93:if:write-empty",
        Verdict::Ported,
        "resource_group_lockdown::settings_string_pins_every_source_branch_boundary",
    ),
    (
        "String:L98:switch:moderated",
        Verdict::Ported,
        "resource_group_lockdown::settings_string_pins_every_source_branch_boundary",
    ),
    (
        "String:L98:switch:unlimited",
        Verdict::Ported,
        "resource_group_lockdown::settings_string_pins_every_source_branch_boundary",
    ),
    (
        "String:L98:switch:default",
        Verdict::Ported,
        "resource_group_lockdown::settings_string_pins_every_source_branch_boundary",
    ),
    (
        "String:L105:if:runaway-present",
        Verdict::Ported,
        "resource_group_lockdown::settings_string_pins_every_source_branch_boundary",
    ),
    (
        "String:L105:if:runaway-nil",
        Verdict::Ported,
        "resource_group_lockdown::settings_string_pins_every_source_branch_boundary",
    ),
    (
        "String:L109:if:elapsed-positive",
        Verdict::Ported,
        "resource_group_lockdown::settings_string_pins_every_source_branch_boundary",
    ),
    (
        "String:L109:if:elapsed-nonpositive",
        Verdict::Ported,
        "resource_group_lockdown::settings_string_pins_every_source_branch_boundary",
    ),
    (
        "String:L113:if:processed-positive",
        Verdict::Ported,
        "resource_group_lockdown::settings_string_pins_every_source_branch_boundary",
    ),
    (
        "String:L113:if:processed-nonpositive",
        Verdict::Ported,
        "resource_group_lockdown::settings_string_pins_every_source_branch_boundary",
    ),
    (
        "String:L114:if:separator-needed",
        Verdict::Ported,
        "resource_group_lockdown::settings_string_pins_every_source_branch_boundary",
    ),
    (
        "String:L114:if:first-parameter",
        Verdict::Ported,
        "resource_group_lockdown::settings_string_pins_every_source_branch_boundary",
    ),
    (
        "String:L120:if:request-unit-positive",
        Verdict::Ported,
        "resource_group_lockdown::settings_string_pins_every_source_branch_boundary",
    ),
    (
        "String:L120:if:request-unit-nonpositive",
        Verdict::Ported,
        "resource_group_lockdown::settings_string_pins_every_source_branch_boundary",
    ),
    (
        "String:L121:if:separator-needed",
        Verdict::Ported,
        "resource_group_lockdown::settings_string_pins_every_source_branch_boundary",
    ),
    (
        "String:L121:if:first-parameter",
        Verdict::Ported,
        "resource_group_lockdown::settings_string_pins_every_source_branch_boundary",
    ),
    (
        "String:L127:if:switch-group",
        Verdict::Ported,
        "resource_group_lockdown::settings_string_pins_every_source_branch_boundary",
    ),
    (
        "String:L127:if:other-action",
        Verdict::Ported,
        "resource_group_lockdown::settings_string_pins_every_source_branch_boundary",
    ),
    (
        "String:L132:if:watch-present",
        Verdict::Ported,
        "resource_group_lockdown::settings_string_pins_every_source_branch_boundary",
    ),
    (
        "String:L132:if:watch-none",
        Verdict::Ported,
        "resource_group_lockdown::settings_string_pins_every_source_branch_boundary",
    ),
    (
        "String:L134:if:duration-positive",
        Verdict::Ported,
        "resource_group_lockdown::settings_string_pins_every_source_branch_boundary",
    ),
    (
        "String:L134:if:duration-unlimited",
        Verdict::Ported,
        "resource_group_lockdown::settings_string_pins_every_source_branch_boundary",
    ),
    (
        "String:L142:if:background-present",
        Verdict::Ported,
        "resource_group_lockdown::settings_string_pins_every_source_branch_boundary",
    ),
    (
        "String:L142:if:background-nil",
        Verdict::Ported,
        "resource_group_lockdown::settings_string_pins_every_source_branch_boundary",
    ),
    (
        "String:L145:if:jobs-present",
        Verdict::Ported,
        "resource_group_lockdown::settings_string_pins_every_source_branch_boundary",
    ),
    (
        "String:L145:if:jobs-empty",
        Verdict::Ported,
        "resource_group_lockdown::settings_string_pins_every_source_branch_boundary",
    ),
    (
        "String:L149:if:util-positive",
        Verdict::Ported,
        "resource_group_lockdown::settings_string_pins_every_source_branch_boundary",
    ),
    (
        "String:L149:if:util-nonpositive",
        Verdict::Ported,
        "resource_group_lockdown::settings_string_pins_every_source_branch_boundary",
    ),
    (
        "String:L150:if:separator-needed",
        Verdict::Ported,
        "resource_group_lockdown::settings_string_pins_every_source_branch_boundary",
    ),
    (
        "String:L150:if:first-parameter",
        Verdict::Ported,
        "resource_group_lockdown::settings_string_pins_every_source_branch_boundary",
    ),
    (
        "Adjust:L167:if:adjust",
        Verdict::Ported,
        "resource_group::tests::constructors_and_adjustment_boundaries",
    ),
    (
        "Adjust:L167:if:preserve",
        Verdict::Ported,
        "resource_group::tests::constructors_and_adjustment_boundaries",
    ),
];

// There is no adjacent source-owned `_test.go` file for this owner.
const SOURCE_TEST_SUPPORT: &[Row] = &[];

const DIRECT_GO_RECEIPTS: &[Row] = &[
    ("direct-go:zero-new-and-embedded-json", Verdict::Ported, "resource_group_lockdown::zero_settings_json_matches_go"),
    ("direct-go:full-width-json", Verdict::Ported, "resource_group_lockdown::info_nil_embedding_and_full_json_match_go"),
    ("direct-go:null-case-duplicate-cistr-json", Verdict::Ported, "resource_group_lockdown::decode_matches_go_embedding_null_case_duplicate_and_ci_string_rules"),
    ("direct-go:nil-versus-empty-slice-json", Verdict::Ported, "resource_group_lockdown::nil_and_allocated_empty_job_slices_remain_distinct"),
    ("direct-go:string-branch-matrix", Verdict::Ported, "resource_group_lockdown::settings_string_pins_every_source_branch_boundary"),
    ("direct-go:shallow-pointer-clone", Verdict::Ported, "resource_group::tests::clone_is_top_level_copy_with_shared_nested_pointers"),
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
fn resource_group_go_source_identity_is_current() {
    assert_eq!(GO_SOURCE.len(), GO_SOURCE_BYTES);
    assert_eq!(GO_SOURCE.lines().count(), GO_SOURCE_LINES);
    assert_eq!(
        format!("{:x}", Sha256::digest(GO_SOURCE.as_bytes())),
        GO_SOURCE_SHA256
    );
}

#[test]
fn every_go_declaration_function_and_branch_has_exactly_one_verdict() {
    let inventory_functions = FUNCTIONS
        .iter()
        .map(|(name, _, _)| (*name).to_owned())
        .collect::<Vec<_>>();
    assert_eq!(go_functions(GO_SOURCE), inventory_functions);
    assert_eq!(DECLARATIONS.len(), 26);
    assert_eq!(FUNCTIONS.len(), 6);
    assert_eq!(BRANCHES.len(), 41);
    assert!(SOURCE_TEST_SUPPORT.is_empty());
    assert_eq!(DIRECT_GO_RECEIPTS.len(), 6);

    let mut names = std::collections::BTreeSet::new();
    for (name, verdict, receipt) in DECLARATIONS
        .iter()
        .chain(FUNCTIONS)
        .chain(BRANCHES)
        .chain(SOURCE_TEST_SUPPORT)
        .chain(DIRECT_GO_RECEIPTS)
    {
        assert!(matches!(
            verdict,
            Verdict::Ported | Verdict::Declined | Verdict::Unreachable
        ));
        assert!(!receipt.is_empty());
        assert!(names.insert(*name), "duplicate inventory row: {name}");
    }
}

#[test]
fn every_ported_resource_group_symbol_still_compiles() {
    fn assert_serde<T: Serialize + for<'de> Deserialize<'de>>() {}
    fn assert_display<T: std::fmt::Display>() {}
    fn assert_clone<T: Clone>() {}

    assert_serde::<ResourceGroupRunawayAction>();
    assert_serde::<ResourceGroupRunawayWatch>();
    assert_serde::<ResourceGroupRunawaySettings>();
    assert_serde::<ResourceGroupBackgroundSettings>();
    assert_serde::<ResourceGroupSettings>();
    assert_serde::<ResourceGroupInfo>();
    assert_display::<ResourceGroupSettings>();
    assert_clone::<ResourceGroupSettings>();
    assert_clone::<ResourceGroupInfo>();

    let _: fn() -> ResourceGroupSettings = ResourceGroupSettings::new;
    let _: fn(&ResourceGroupSettings) -> i64 = ResourceGroupSettings::get_burst_limit_adjusted;
    let _: fn(&mut ResourceGroupSettings) = ResourceGroupSettings::adjust;
    let _: fn(&ResourceGroupSettings) -> ResourceGroupSettings = ResourceGroupSettings::clone;
    let _: fn(&ResourceGroupInfo) -> ResourceGroupInfo = ResourceGroupInfo::clone;

    assert_eq!(crate::resource_group::UNLIMITED_RU_RATE, i32::MAX as u64);
    let runaway = ResourceGroupRunawaySettings::default();
    let _: &u64 = &runaway.exec_elapsed_time_ms;
    let _: &i64 = &runaway.processed_keys;
    let _: &i64 = &runaway.request_unit;
    let _: &ResourceGroupRunawayAction = &runaway.action;
    let _: &String = &runaway.switch_group_name;
    let _: &ResourceGroupRunawayWatch = &runaway.watch_type;
    let _: &i64 = &runaway.watch_duration_ms;

    let background = ResourceGroupBackgroundSettings::default();
    let _: &Option<Vec<String>> = &background.job_types;
    let _: &u64 = &background.resource_util_limit;

    let settings = ResourceGroupSettings::default();
    let _: &u64 = &settings.ru_rate;
    let _: &u64 = &settings.priority;
    let _: &String = &settings.cpu_limiter;
    let _: &String = &settings.io_read_bandwidth;
    let _: &String = &settings.io_write_bandwidth;
    let _: &i64 = &settings.burst_limit;
    let _: &Option<ResourceGroupShared<ResourceGroupRunawaySettings>> = &settings.runaway;
    let _: &Option<ResourceGroupShared<ResourceGroupBackgroundSettings>> = &settings.background;

    let info = ResourceGroupInfo::default();
    let _: &Option<Box<ResourceGroupSettings>> = &info.settings;
    let _: &i64 = &info.id;
    let _: &CiString = &info.name;
    let _: &SchemaState = &info.state;
}
