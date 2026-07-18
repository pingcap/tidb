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

#![allow(missing_docs)]

const REGION: &str = include_str!("../src/region/mod.rs");
const RPC: &str = include_str!("../src/rpc/async_completion.rs");

// client-go's RegionRequest owns one selector/cache/retry loop. The async RPC
// leaf contributes only one pending physical attempt to that owner.
#[test]
fn region_layer_has_no_second_async_policy_or_completion_owner() {
    assert!(!REGION.contains("mod async_request"));
    assert!(!REGION.contains("AsyncRegionAttemptPolicy"));
    assert!(!REGION.contains("AsyncRegionRequestAttempt"));
    assert!(!REGION.contains("CompletionRequest"));
}

#[test]
fn async_dispatcher_is_one_address_directed_attempt() {
    let dispatcher = RPC
        .split("pub trait AsyncRequestDispatcher")
        .nth(1)
        .expect("async dispatcher trait");
    let dispatcher = dispatcher
        .split('}')
        .next()
        .expect("async dispatcher trait body");
    assert_eq!(dispatcher.matches("fn begin(").count(), 1);
    assert!(!dispatcher.contains("retry"));
    assert!(!dispatcher.contains("selector"));
    assert!(!dispatcher.contains("RegionCache"));
}
