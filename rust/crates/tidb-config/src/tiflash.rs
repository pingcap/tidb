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

//! Transcreation of Go `pkg/config/tiflash.go`: TiFlash-compute
//! autoscaler type names.

/// Mock AutoScaler (Go `MockASStr`).
pub const MOCK_AS_STR: &str = "mock";
/// AWS AutoScaler (Go `AWSASStr`).
pub const AWS_AS_STR: &str = "aws";
/// GCP AutoScaler (Go `GCPASStr`).
pub const GCP_AS_STR: &str = "gcp";
/// Test AutoScaler (Go `TestASStr`).
pub const TEST_AS_STR: &str = "test";
/// Invalid AutoScaler (Go `InvalidASStr`).
pub const INVALID_AS_STR: &str = "invalid";

/// Default AWS AutoScaler address (Go `DefAWSAutoScalerAddr`).
pub const DEF_AWS_AUTO_SCALER_ADDR: &str =
    "tiflash-autoscale-lb.tiflash-autoscale.svc.cluster.local:8081";
/// Default AutoScaler (Go `DefASStr`).
pub const DEF_AS_STR: &str = AWS_AS_STR;

/// AutoScaler type (Go's iota int constants).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum AutoScalerType {
    /// Mock.
    Mock = 0,
    /// AWS.
    Aws = 1,
    /// GCP.
    Gcp = 2,
    /// Local test.
    Test = 3,
    /// Invalid.
    Invalid = 4,
}

/// Go `GetAutoScalerType`.
pub fn get_auto_scaler_type(typ: &str) -> AutoScalerType {
    match typ {
        MOCK_AS_STR => AutoScalerType::Mock,
        AWS_AS_STR => AutoScalerType::Aws,
        GCP_AS_STR => AutoScalerType::Gcp,
        TEST_AS_STR => AutoScalerType::Test,
        _ => AutoScalerType::Invalid,
    }
}

/// Go `IsValidAutoScalerConfig`.
pub fn is_valid_auto_scaler_config(typ: &str) -> bool {
    matches!(
        get_auto_scaler_type(typ),
        AutoScalerType::Mock | AutoScalerType::Aws | AutoScalerType::Gcp
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auto_scaler() {
        assert!(is_valid_auto_scaler_config("mock"));
        assert!(is_valid_auto_scaler_config("aws"));
        assert!(is_valid_auto_scaler_config("gcp"));
        assert!(!is_valid_auto_scaler_config("test"));
        assert!(!is_valid_auto_scaler_config("bogus"));
        assert_eq!(get_auto_scaler_type("test"), AutoScalerType::Test);
        assert_eq!(get_auto_scaler_type(""), AutoScalerType::Invalid);
    }
}
