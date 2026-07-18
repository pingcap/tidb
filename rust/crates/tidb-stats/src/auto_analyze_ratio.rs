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

//! Auto-analyze ratio parsing from `pkg/statistics/handle/autoanalyze/exec/exec.go`.
//!
//! The parser is intentionally independent of SQL/session state. Time-window
//! parsing, global-variable access, and execution scheduling remain separate
//! owners.

/// Source default used when the configured ratio cannot be parsed.
pub const DEFAULT_AUTO_ANALYZE_RATIO: f64 = 0.5;

/// Parses a configured auto-analyze ratio.
///
/// Invalid input falls back to the source default, while valid negative values
/// clamp to zero. Go's `math.Max` preserves a parsed NaN, so that case is kept
/// explicit instead of using Rust's NaN-selecting `f64::max` behavior.
#[must_use]
pub fn parse_auto_analyze_ratio(ratio: &str) -> f64 {
    let Ok(parsed) = ratio.parse::<f64>() else {
        return DEFAULT_AUTO_ANALYZE_RATIO;
    };
    if parsed.is_nan() {
        parsed
    } else {
        parsed.max(0.0)
    }
}
