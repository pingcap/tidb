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

//! Build-time race-instrumentation status.

/// Whether this build uses the source-equivalent race configuration.
pub const RACE_ENABLED: bool = cfg!(feature = "race");

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(not(feature = "race"))]
    #[test]
    fn default_build_reports_race_disabled() {
        const { assert!(!RACE_ENABLED) };
    }

    #[cfg(feature = "race")]
    #[test]
    fn race_build_reports_race_enabled() {
        const { assert!(RACE_ENABLED) };
    }
}
