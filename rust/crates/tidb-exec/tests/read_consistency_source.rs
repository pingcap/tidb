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

//! Source-backed tests for read-consistency level metadata.

use tidb_exec::read_consistency::{
    ReadConsistencyLevel, READ_CONSISTENCY_STRICT, READ_CONSISTENCY_WEAK,
};

#[test]
fn read_consistency_accepts_source_labels_and_marks_weak_reads() {
    // Source: pkg/sessionctx/variable/session.go:687-705 and
    // pkg/sessionctx/stmtctx/stmtctx_test.go:136-180 (TestWeakConsistencyRead).
    assert_eq!(
        ReadConsistencyLevel::default().as_str(),
        READ_CONSISTENCY_STRICT
    );
    assert!(!ReadConsistencyLevel::strict().is_weak());
    assert!(ReadConsistencyLevel::weak().is_weak());

    for value in ["strict", "STRICT", "Strict"] {
        let parsed = ReadConsistencyLevel::parse(value).expect("strict is valid");
        assert_eq!(parsed.as_str(), READ_CONSISTENCY_STRICT);
        assert!(!parsed.is_weak());
    }
    for value in ["weak", "WEAK", "Weak"] {
        let parsed = ReadConsistencyLevel::parse(value).expect("weak is valid");
        assert_eq!(parsed.as_str(), READ_CONSISTENCY_WEAK);
        assert!(parsed.is_weak());
    }
}

#[test]
fn read_consistency_rejects_unknown_values_without_changing_raw_semantics() {
    // Source: pkg/sessionctx/variable/session.go:698-705. The validator
    // rejects unknown labels, while IsWeak itself compares the raw string.
    assert!(ReadConsistencyLevel::parse("eventual").is_none());
    assert!(!ReadConsistencyLevel::from("WEAK").is_weak());
    assert_eq!(ReadConsistencyLevel::from("WEAK").as_str(), "WEAK");
}
