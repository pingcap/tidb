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

//! Source tests for Go `pkg/config/store_test.go`.

use tidb_config::store::store_type_list;

#[test]
fn store_type_matches_source() {
    assert_eq!(store_type_list().len(), 3);
    for tp in store_type_list() {
        assert!(tp.valid(), "{tp}");
    }
}
