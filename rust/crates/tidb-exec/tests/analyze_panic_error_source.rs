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

//! Source-backed tests for analyze-worker panic classification.

use tidb_exec::analyze_panic_error::{
    get_analyze_panic_error, is_analyze_worker_panic, AnalyzePanicError, AnalyzePanicValue,
    GLOBAL_PANIC_ANALYZE_MEMORY_EXCEED,
};

#[test]
fn source_memory_panic_maps_to_the_oom_sentinel() {
    // Source: pkg/executor/analyze_utils.go:94-103.
    // Direct Go coverage: pkg/executor/analyze_utils_test.go:41-44
    // (TestGetAnalyzePanicErr), which passes an error carrying the memory
    // sentinel and checks that formatting does not append `%!(EXTRA`.
    let text = get_analyze_panic_error(AnalyzePanicValue::Text(
        GLOBAL_PANIC_ANALYZE_MEMORY_EXCEED.to_owned(),
    ));
    let error = get_analyze_panic_error(AnalyzePanicValue::Error(
        GLOBAL_PANIC_ANALYZE_MEMORY_EXCEED.to_owned(),
    ));

    assert_eq!(text, AnalyzePanicError::OutOfMemory);
    assert_eq!(error, AnalyzePanicError::OutOfMemory);
    assert!(is_analyze_worker_panic(&text));
    assert!(!text.rendered_message().contains("%!(EXTRA"));
    assert!(text
        .rendered_message()
        .contains("smaller samplerate(refer to 110000/count)"));
}

#[test]
fn source_non_memory_errors_propagate_and_other_values_use_worker_sentinel() {
    // Source: pkg/executor/analyze_utils.go:96-107.
    let propagated = get_analyze_panic_error(AnalyzePanicValue::Error(
        "wrapped analyze failure".to_owned(),
    ));
    assert_eq!(
        propagated,
        AnalyzePanicError::Propagated("wrapped analyze failure".to_owned())
    );
    assert_eq!(propagated.rendered_message(), "wrapped analyze failure");
    assert!(!is_analyze_worker_panic(&propagated));

    let text = get_analyze_panic_error(AnalyzePanicValue::Text("ordinary panic".to_owned()));
    let other = get_analyze_panic_error(AnalyzePanicValue::Other);
    assert_eq!(text, AnalyzePanicError::WorkerPanic);
    assert_eq!(other, AnalyzePanicError::WorkerPanic);
    assert!(is_analyze_worker_panic(&text));
    assert!(is_analyze_worker_panic(&other));
    assert_eq!(text.rendered_message(), "analyze worker panic");
}
