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

//! Behavioral session tests retained from the original Go test inventory.
//! Empty harness entries moved to session-cleanup-obligations.json in
//! rust/docs/parity/current-audit; their Go obligations remain unverified.

#![cfg(test)]

/// Go `pkg/session/bootstrap_test.go:428::TestOldPasswordUpgrade`.
/// The Go helper decodes the old stage-one SHA-1 hex and hashes those bytes
/// once more. This pure cryptographic contract is already exposed by the
/// parser auth module, so it can be checked without pretending to port the
/// surrounding bootstrap machinery.
#[test]
fn test_old_password_upgrade() {
    let stage_one = tidb_parser::auth::sha1_hash(b"abc");
    let stage_two = tidb_parser::auth::sha1_hash(&stage_one);
    let mut upgraded = String::from("*");
    for byte in stage_two {
        use std::fmt::Write as _;
        write!(&mut upgraded, "{byte:02X}").unwrap();
    }
    assert_eq!(upgraded, "*0D3CED9BEC10A777AEC23CCC353A8C08A633045E");
}
