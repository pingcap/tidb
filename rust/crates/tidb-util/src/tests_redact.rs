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

//! Direct ports of `Go code: pkg/util/redact` unit tests (redact_test.go).

use crate::redact::{de_redact, init_redact, key, string, stringer, value};

// Go pkg/util/redact/redact_test.go: TestRedact.
#[test]
fn redact_string() {
    for (mode, input, output) in [
        ("OFF", "fxcv", "fxcv"),
        ("OFF", "f‹xcv", "f‹xcv"),
        ("ON", "f‹xcv", ""),
        ("MARKER", "f‹xcv", "‹f‹‹xcv›"),
        ("MARKER", "f›xcv", "‹f››xcv›"),
    ] {
        assert_eq!(string(mode, input), output, "{mode} {input}");
        assert_eq!(stringer(mode, &input).to_string(), output, "{mode} {input}");
    }
}

// Go pkg/util/redact/redact_test.go: TestDeRedact.
#[test]
fn de_redact_cases() {
    for (remove, input, output) in [
        (true, "‹fxcv›ggg", "?ggg"),
        (false, "‹fxcv›ggg", "fxcvggg"),
        (true, "fxcv", "fxcv"),
        (false, "fxcv", "fxcv"),
        (true, "‹fxcv›ggg‹fxcv›eee", "?ggg?eee"),
        (false, "‹fxcv›ggg‹fxcv›eee", "fxcvgggfxcveee"),
        (true, "‹›", "?"),
        (false, "‹›", ""),
        (true, "gg‹ee", "gg‹ee"),
        (false, "gg‹ee", "gg‹ee"),
        (true, "gg›ee", "gg›ee"),
        (false, "gg›ee", "gg›ee"),
        (true, "gg‹ee‹ee", "gg‹ee‹ee"),
        (false, "gg‹ee‹gg", "gg‹ee‹gg"),
        (true, "gg›ee›gg", "gg›ee›gg"),
        (false, "gg›ee›ee", "gg›ee›ee"),
    ] {
        assert_eq!(de_redact(remove, input, "").unwrap(), output, "{input}");
    }
}

// Go pkg/util/redact/redact_test.go: TestRedactInitAndValueAndKey. This is the
// only test touching the process-wide redact flag; keep it in its own #[test]
// so it does not race with other redact tests (none of which read the flag).
#[test]
fn redact_init_value_key() {
    let secret = "secret";

    init_redact(false);
    assert_eq!(value(secret), secret);
    // Go compares Key against hex.EncodeToString (lower case); "secret" has no
    // a-f nibbles so lower == upper here. Also pin the upper-casing branch.
    assert_eq!(key(secret.as_bytes()), "736563726574");
    assert_eq!(key(&[0xab, 0xcd, 0xef]), "ABCDEF");

    init_redact(true);
    assert_eq!(value(secret), "?");
    assert_eq!(key(secret.as_bytes()), "?");

    // Reset so the shared flag does not leak into other tests.
    init_redact(false);
}
