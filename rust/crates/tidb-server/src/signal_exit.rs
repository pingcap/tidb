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

//! Go `cmd/tidb-server/main.go`'s shutdown exit-code contract.
//!
//! Found by pivoting to the capstone: the Go binary maps the signal that
//! ended it to a process exit code (`main.go:499-529`), and this node had no
//! such surface at all. The contract is deployment-visible — scripts branch
//! on it — so it is observable behavior, not plumbing.

/// Go `exitCodeOK` (`main.go:164`).
pub const EXIT_CODE_OK: i32 = 0;

/// Go `exitCodeInt` (`main.go:166`): `128 + SIGINT`, the Unix convention for
/// death-by-signal, pinned to SIGINT's number.
pub const EXIT_CODE_INT: i32 = 128 + SIGINT;

/// `syscall.SIGINT`'s number, the one signal the contract distinguishes.
pub const SIGINT: i32 = 2;

/// Go `exitCodeForSignal(sig)` (`main.go:522`): SIGINT alone answers
/// `128+SIGINT` — "standby force shutdown uses SIGINT. Return 128+SIGINT so
/// deployment scripts can identify this force-shutdown path" — and every
/// other signal, SIGTERM and SIGHUP and SIGQUIT included, answers OK, as
/// does Go's nil signal (`None` here).
#[must_use]
pub const fn exit_code_for_signal(signal: Option<i32>) -> i32 {
    match signal {
        Some(SIGINT) => EXIT_CODE_INT,
        Some(_) | None => EXIT_CODE_OK,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// TRANSCREATED from Go `TestExitCodeForSignal`
    /// (`cmd/tidb-server/main_test.go:56`), all five cases.
    #[test]
    fn test_exit_code_for_signal() {
        const SIGTERM: i32 = 15;
        const SIGHUP: i32 = 1;
        const SIGQUIT: i32 = 3;
        let cases: [(&str, Option<i32>, i32); 5] = [
            ("SIGINT", Some(SIGINT), EXIT_CODE_INT),
            ("SIGTERM", Some(SIGTERM), EXIT_CODE_OK),
            ("SIGHUP", Some(SIGHUP), EXIT_CODE_OK),
            ("SIGQUIT", Some(SIGQUIT), EXIT_CODE_OK),
            ("nil", None, EXIT_CODE_OK),
        ];
        for (name, signal, want) in cases {
            assert_eq!(exit_code_for_signal(signal), want, "{name}");
        }
        assert_eq!(EXIT_CODE_INT, 130, "128 + SIGINT");
    }
}
