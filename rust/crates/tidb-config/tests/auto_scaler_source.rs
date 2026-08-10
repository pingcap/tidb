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

//! Source test for Go `pkg/config/config_test.go::TestAutoScalerConfig`.

use std::sync::Mutex;

use tidb_config::config_tree::config::{get_global_config, update_global, Config};
use tidb_config::config_tree::new_config;

static TEST_LOCK: Mutex<()> = Mutex::new(());

struct RestoreGlobal(Config);

impl Drop for RestoreGlobal {
    fn drop(&mut self) {
        update_global(|config| *config = self.0.clone());
    }
}

#[test]
fn auto_scaler_config_matches_source() {
    let _guard = TEST_LOCK.lock().unwrap();
    let original = get_global_config();
    let _restore = RestoreGlobal(original);

    let config = new_config();
    assert!(!config.use_auto_scaler);
    assert!(!get_global_config().use_auto_scaler);

    update_global(|config| config.use_auto_scaler = true);
    assert!(get_global_config().use_auto_scaler);

    update_global(|config| config.use_auto_scaler = false);
}
