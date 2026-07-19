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

#![allow(missing_docs)]

#[test]
fn store_not_match_uses_response_channel_evidence_only_for_direct_routes() {
    let source = include_str!("../src/cop_paging/direct_unary_query_transport.rs");
    let recovery = source
        .split_once("fn recover_region_error(")
        .expect("region-error recovery must remain explicit")
        .1;

    assert!(recovery.contains("region_error.store_not_match.is_some()"));
    assert!(recovery.contains("selected.forwarded_host().is_none()"));
    assert!(recovery.contains("physical_response.address"));
    assert!(recovery.contains("physical_response.channel_version"));
    assert!(recovery.contains(".close_address_version("));
    assert!(!recovery.contains(".close_address(physical_response.address"));
}
