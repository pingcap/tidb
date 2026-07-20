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

//! Source guard for the configured ordered real-TiKV server route.

#[test]
fn ordered_route_prepares_before_opening_the_join_and_keeps_limit_zero_local() {
    let source = include_str!("../src/real_tikv_multi_node.rs");
    let prepare = source
        .find("let route = prepare_configured_query")
        .expect("one typed ordered route preparation");
    let execute = source
        .find("execute_configured_inner_join_with_cancellation")
        .expect("one physical join open");
    assert!(
        prepare < execute,
        "TopN admission precedes reader execution"
    );
    assert!(source.contains("ConfiguredQueryRoute::LocalEmpty"));
    assert!(source.contains("ConfiguredOrderedQueryRecordSet::local_empty"));
}
