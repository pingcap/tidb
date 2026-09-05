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

//! Source-derived return-value tolerance for `pkg/distsql/context`.

use tidb_distsql::DistSqlContext;

#[deny(unused_must_use)]
#[test]
fn detach_result_may_be_ignored_like_go() {
    let context = DistSqlContext::default();
    context.detach();
}
