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

//! Direct parity assertions for `Go code: pkg/util/sqlescape`.

use std::any::Any;
use std::panic::{catch_unwind, AssertUnwindSafe};

use crate::sqlescape::{must_escape_sql, must_format_sql};

fn panic_text(payload: &(dyn Any + Send)) -> Option<&str> {
    payload
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| payload.downcast_ref::<&str>().copied())
}

// Go pkg/util/sqlescape/utils_test.go: TestMustUtils.
#[test]
fn must_utils_preserve_exact_panic_messages_and_success_paths() {
    const MISSING_ARGUMENT: &str = "missing arguments, need 1-th arg, but only got 0 args";

    let panic = catch_unwind(|| must_escape_sql("%?", &[]))
        .expect_err("MustEscapeSQL must panic when its argument is missing");
    assert_eq!(panic_text(panic.as_ref()), Some(MISSING_ARGUMENT));

    let mut output = Vec::new();
    let panic = catch_unwind(AssertUnwindSafe(|| {
        must_format_sql(&mut output, "%?", &[]);
    }))
    .expect_err("MustFormatSQL must panic when its argument is missing");
    assert_eq!(panic_text(panic.as_ref()), Some(MISSING_ARGUMENT));

    must_format_sql(&mut output, "t", &[]);
    assert_eq!(output, b"t");
    assert_eq!(must_escape_sql("tt", &[]), b"tt");
}
