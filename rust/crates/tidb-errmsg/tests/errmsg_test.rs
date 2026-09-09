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

//! Tests transcreated from Go `pkg/util/errmsg/errmsg_test.go`.

use std::sync::Mutex;

use tidb_config::config_tree::config::{get_global_config, store_global_config};
use tidb_config::config_tree::ErrorMessageExtension;
use tidb_error::mysql::SqlError;

static CONFIG_LOCK: Mutex<()> = Mutex::new(());

fn error(message: &str) -> SqlError {
    SqlError {
        code: 1105,
        message: message.into(),
        state: "HY000",
    }
}

fn extension(pattern: &str, suffix: &str) -> ErrorMessageExtension {
    ErrorMessageExtension {
        pattern: pattern.into(),
        suffix: suffix.into(),
        regexp: None,
    }
}

fn with_extensions(extensions: Vec<ErrorMessageExtension>, test: impl FnOnce()) {
    let _guard = CONFIG_LOCK.lock().unwrap();
    let original = get_global_config();
    let mut config = original.clone();
    config.error_message_extensions = extensions;
    store_global_config(config);
    test();
    store_global_config(original);
}

#[test]
fn TestExtendByRegex() {
    with_extensions(
        vec![
            extension(
                r"^Access denied for user '.+'@'.+' \(using password: (YES|NO)\)$",
                "see https://docs.pingcap.com/tidbcloud/select-cluster-tier#user-name-prefix for more details",
            ),
            extension(
                r"^require_secure_transport can not be set to ON with SEM\(security enhanced mode\) enabled$",
                "see https://docs.pingcap.com/tidbcloud/secure-connections-to-serverless-tier-clusters for more details",
            ),
            extension(
                r"^sleep\(\) argument is greater than [0-9]+$",
                "see https://docs.pingcap.com/tidbcloud/serverless-tier-limitations#sql for more details",
            ),
            extension(
                r"^[A-Z ]+ command denied to user '[^']+'@'[^']+' for table '[^']+'$",
                "see https://docs.pingcap.com/tidbcloud/limited-sql-features#system-tables for more details",
            ),
            extension(
                r"^Access denied; you need \(at least one of\) the RESTRICTED_VARIABLES_ADMIN privilege\(s\) for this operation$",
                "see https://docs.pingcap.com/tidbcloud/limited-sql-features#system-variables for more details",
            ),
            extension(
                r"^Feature '.+' is not supported when security enhanced mode is enabled$",
                "see https://docs.pingcap.com/tidbcloud/limited-sql-features#statements for more details",
            ),
            extension(r"^Error message\.$", "suffix."),
            extension(r"^Error message without period$", "suffix"),
            extension(r"^Error message with multiple periods\.\.\.$", "suffix..."),
            extension(r"^Error message with empty suffix$", ""),
        ],
        || {
            for (message, expected) in [
                (
                    "Access denied for user 'root.foo'@'127.0.0.1' (using password: YES)",
                    "Access denied for user 'root.foo'@'127.0.0.1' (using password: YES), see https://docs.pingcap.com/tidbcloud/select-cluster-tier#user-name-prefix for more details.",
                ),
                (
                    "sleep() argument is greater than 31536000",
                    "sleep() argument is greater than 31536000, see https://docs.pingcap.com/tidbcloud/serverless-tier-limitations#sql for more details.",
                ),
                (
                    "require_secure_transport can not be set to ON with SEM(security enhanced mode) enabled",
                    "require_secure_transport can not be set to ON with SEM(security enhanced mode) enabled, see https://docs.pingcap.com/tidbcloud/secure-connections-to-serverless-tier-clusters for more details.",
                ),
                (
                    "Exceeded resource group quota limitation",
                    "Exceeded resource group quota limitation",
                ),
                (
                    "Feature 'SELECT INTO' is not supported when security enhanced mode is enabled",
                    "Feature 'SELECT INTO' is not supported when security enhanced mode is enabled, see https://docs.pingcap.com/tidbcloud/limited-sql-features#statements for more details.",
                ),
                (
                    "SELECT command denied to user 'u'@'%' for table 'tidb'",
                    "SELECT command denied to user 'u'@'%' for table 'tidb', see https://docs.pingcap.com/tidbcloud/limited-sql-features#system-tables for more details.",
                ),
                (
                    "Access denied; you need (at least one of) the RESTRICTED_VARIABLES_ADMIN privilege(s) for this operation",
                    "Access denied; you need (at least one of) the RESTRICTED_VARIABLES_ADMIN privilege(s) for this operation, see https://docs.pingcap.com/tidbcloud/limited-sql-features#system-variables for more details.",
                ),
                (
                    "Table 'test.t' doesn't exist",
                    "Table 'test.t' doesn't exist",
                ),
                ("Error message.", "Error message, suffix."),
                (
                    "Error message without period",
                    "Error message without period, suffix.",
                ),
                (
                    "Error message with multiple periods...",
                    "Error message with multiple periods, suffix.",
                ),
                (
                    "Error message with empty suffix",
                    "Error message with empty suffix",
                ),
            ] {
                let mut error = error(message);
                tidb_errmsg::extend(Some(&mut error));
                assert_eq!(error.message, expected);
            }
        },
    );
}

#[test]
fn TestExtendWithoutConfig() {
    with_extensions(Vec::new(), || {
        let mut error = error("Exceeded resource group quota limitation");
        tidb_errmsg::extend(Some(&mut error));
        assert_eq!(error.message, "Exceeded resource group quota limitation");
    });
}

#[test]
fn TestExtendSkipsInvalidRegex() {
    with_extensions(
        vec![
            extension("[", "invalid regex"),
            extension(
                r"^sleep\(\) argument is greater than [0-9]+$",
                "see https://docs.pingcap.com/tidbcloud/serverless-tier-limitations#sql for more details",
            ),
        ],
        || {
            let mut error = error("sleep() argument is greater than 31536000");
            tidb_errmsg::extend(Some(&mut error));
            assert_eq!(
                error.message,
                "sleep() argument is greater than 31536000, see https://docs.pingcap.com/tidbcloud/serverless-tier-limitations#sql for more details."
            );
        },
    );
}

#[test]
fn TestExtendPrefersLongestPattern() {
    with_extensions(
        vec![
            extension("^Access denied", "generic access denied message"),
            extension(
                r"^Access denied for user '.+'@'.+' \(using password: (YES|NO)\)$",
                "specific user prefix message",
            ),
        ],
        || {
            let mut error =
                error("Access denied for user 'root.foo'@'127.0.0.1' (using password: YES)");
            tidb_errmsg::extend(Some(&mut error));
            assert_eq!(
                error.message,
                "Access denied for user 'root.foo'@'127.0.0.1' (using password: YES), specific user prefix message."
            );
        },
    );
}

#[test]
fn TestExtendConcurrentWithStoreGlobalConfig() {
    with_extensions(
        vec![
            extension("^Access denied", "generic access denied message"),
            extension(
                r"^Access denied for user '.+'@'.+' \(using password: (YES|NO)\)$",
                "specific user prefix message",
            ),
        ],
        || {
            std::thread::scope(|scope| {
                let published = get_global_config();
                scope.spawn(move || {
                    for _ in 0..1_000 {
                        store_global_config(published.clone());
                    }
                });
                scope.spawn(|| {
                    for _ in 0..1_000 {
                        let mut error = error(
                            "Access denied for user 'root.foo'@'127.0.0.1' (using password: YES)",
                        );
                        tidb_errmsg::extend(Some(&mut error));
                    }
                });
                scope.spawn(|| {
                    for i in 0..1_000 {
                        tidb_config::config_tree::config::update_global(|config| {
                            config.instance.enable_slow_log =
                                tidb_config::config_tree::AtomicBool::new(i % 2 == 0);
                        });
                    }
                });
            });
        },
    );
}
