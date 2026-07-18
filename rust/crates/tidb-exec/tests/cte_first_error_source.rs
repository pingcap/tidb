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

//! Source-backed tests for CTE first-error precedence.

use tidb_exec::cte_first_error::retain_first_error;

#[test]
fn cte_first_error_preserves_existing_error_and_adopts_only_when_empty() {
    // Source: pkg/executor/cte.go:127-136.
    // Direct Go coverage: pkg/executor/test/cte/cte_test.go:113-140
    // (TestCTEExecError), which verifies an execution error survives repeated
    // CTE worker/close activity.
    assert_eq!(
        retain_first_error::<&str>(None, Some("first close error")),
        Some("first close error")
    );
    assert_eq!(
        retain_first_error(Some("first close error"), Some("later close error")),
        Some("first close error")
    );
    assert_eq!(
        retain_first_error(Some("first close error"), None),
        Some("first close error")
    );
    assert_eq!(retain_first_error::<&str>(None, None), None);
}

#[test]
fn cte_first_error_preserves_concrete_value_without_reformatting() {
    // Source: pkg/executor/cte.go:129-134. The helper returns the original
    // error object rather than wrapping or replacing it when one is present.
    let first = String::from("source error with context");
    let later = String::from("later error");
    let preserved = retain_first_error(Some(first), Some(later));
    assert_eq!(preserved.as_deref(), Some("source error with context"));
}
