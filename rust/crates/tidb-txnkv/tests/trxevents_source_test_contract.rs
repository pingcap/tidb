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

//! Source-derived return-value tolerance for `pkg/util/trxevents`.

use std::sync::Arc;

use tidb_txnkv::{wrap_cop_meet_lock, CopMeetLock};

#[deny(unused_must_use)]
#[test]
fn source_return_values_may_be_ignored_like_go() {
    let event = wrap_cop_meet_lock(None);
    event.get_cop_meet_lock();
    wrap_cop_meet_lock(Some(Arc::new(CopMeetLock::default())));

    assert!(event.get_cop_meet_lock().is_none());
}
