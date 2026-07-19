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

#[test]
fn production_constructor_opens_a_session_from_the_process_authority() {
    let source = include_str!("../src/cop_paging/direct_unary_query_transport.rs");
    assert!(source.contains("pub fn from_read_authority<S>"));
    assert!(source.contains("authority.open_session()"));
    assert!(!source.contains("Arc<Mutex<DirectUnaryQueryTransport"));
    assert!(!source.contains("Arc<Mutex<SharedReadRuntime"));
}
