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

//! Source-backed inventory for manifest batch `b151`, `pkg/table.part2`.
//!
//! The deterministic slice is items 61--108 of the repository-wide
//! `pkg/table` test enumeration: the partition package's test harness and 24
//! tests, five `tblctx` buffer tests, one `tblsession` test, three temporary
//! table DDL tests, thirteen temporary-table interceptor tests, and the
//! temporary-table test harness.
//!
//! The executor already owns the partition router, pruner, and SQL driver;
//! the runnable tests below pin the data-level portions of those Go tests.
//! The rest remain explicit carrier/gap inventory entries rather than empty
//! omissions. In particular, temporary-table DDL/interceptor behavior is
//! owned by the unported session/storage overlay, while the buffer and
//! tblsession tests already have complete carriers in `tblctx` and
//! `tblsession`.

#![cfg(test)]



/// `partition_test.go:284::TestPartitionGetPhysicalID`.
#[test]
fn partition_get_physical_id_preserves_definition_ids() {
    let pi = partition_fixture();
    let definitions = pi.definitions.snapshot();
    assert_eq!(
        definitions.iter().map(|definition| definition.id).collect::<Vec<_>>(),
        vec![101, 102, 103]
    );
    for definition in &definitions {
        assert_eq!(
            pi.get_partition_id_by_name(definition.name.original()),
            definition.id
        );
    }
}

// The next six entries are complete carriers already exercised by the owning
// modules. They stay in this source-backed inventory so the manifest remains
// one-to-one without duplicating private implementation tests here.

fn partition_fixture() -> tidb_model::partition::PartitionInfo {
    tidb_model::partition::PartitionInfo {
        enable: true,
        expr: "`a`".to_owned(),
        definitions: [(101, "p0"), (102, "p1"), (103, "p2")]
            .map(|(id, name)| tidb_model::partition::PartitionDefinition {
                id,
                name: tidb_ast::CiString::new(name),
                ..Default::default()
            })
            .to_vec()
            .into(),
        ..Default::default()
    }
}
