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

//! Direct source vector for DistSQL paging configuration defaults.

use tidb_distsql::{PagingConfig, MIN_ALLOWED_MAX_PAGING_SIZE, MIN_PAGING_SIZE};

#[test]
fn paging_config_defaults_consume_the_policy_authority() {
    let defaults = PagingConfig::source_defaults();
    assert_eq!(PagingConfig::default(), defaults);
    assert!(defaults.enabled);
    assert_eq!(defaults.min_size, MIN_PAGING_SIZE);
    assert_eq!(defaults.max_size, MIN_ALLOWED_MAX_PAGING_SIZE);
    assert_eq!(defaults.size_bytes, 0);
}
