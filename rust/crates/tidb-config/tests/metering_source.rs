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

//! Source test for Go `pkg/config/config_test.go::TestMetering`.

use tidb_config::config_tree::config::MeteringConfig;
use tidb_config::config_tree::new_config;

#[test]
fn metering_matches_source() {
    if !tidb_config::kerneltype::is_next_gen() {
        return;
    }

    let mut config = new_config();
    config.metering_storage_uri = "s3://test-bucket/test-prefix?region-id=test-region".to_owned();
    config.valid().unwrap();
    let metering = MeteringConfig::from_uri(&config.metering_storage_uri).unwrap();
    assert_eq!(metering.storage_type, "s3");
    assert_eq!(metering.bucket, "test-bucket");
    assert_eq!(metering.prefix, "test-prefix");
    assert_eq!(metering.region, "test-region");

    let mut config = new_config();
    config.metering_storage_uri =
        "azure://metering-data/test-prefix?account-name=test-account&account-key=test-key"
            .to_owned();
    config.valid().unwrap();
    let metering = MeteringConfig::from_uri(&config.metering_storage_uri).unwrap();
    assert_eq!(metering.storage_type, "azure");
    assert_eq!(metering.bucket, "metering-data");
    assert_eq!(metering.prefix, "test-prefix");
    let azure = metering.azure.unwrap();
    assert_eq!(azure.account_name, "test-account");
    assert_eq!(azure.account_key, "test-key");
}
