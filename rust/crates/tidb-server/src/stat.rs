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

//! Go `pkg/server/stat.go`: the server's `SHOW STATUS` provider, and
//! `conn.go`'s `compressionStats`.

use std::collections::BTreeMap;
use std::sync::Arc;

use tidb_session::statusvar::{Statistics, DEFAULT_STATUS_VAR_SCOPE_FLAG};
use tidb_session::SessionVars;

use crate::mysql_tls::MysqlTlsManager;

const SERVER_NOT_AFTER: &str = "Ssl_server_not_after";
const SERVER_NOT_BEFORE: &str = "Ssl_server_not_before";
const UP_TIME: &str = "Uptime";

/// Go `Server`'s statuses: its certificate's validity and its uptime.
pub(crate) struct ServerStatistics {
    pub(crate) tls: Arc<MysqlTlsManager>,
}

impl Statistics for ServerStatistics {
    fn get_scope(&self, _status: &str) -> u8 {
        DEFAULT_STATUS_VAR_SCOPE_FLAG
    }

    fn stats(&self, _vars: Option<&SessionVars>) -> Result<BTreeMap<String, String>, String> {
        let mut m = BTreeMap::from([
            (SERVER_NOT_AFTER.to_owned(), String::new()),
            (SERVER_NOT_BEFORE.to_owned(), String::new()),
        ]);
        if let Some(tls) = self.tls.current() {
            match tls.certificate_validity() {
                Ok((not_before, not_after)) => {
                    m.insert(SERVER_NOT_AFTER.to_owned(), certificate_time(not_after));
                    m.insert(SERVER_NOT_BEFORE.to_owned(), certificate_time(not_before));
                }
                Err(error) => tidb_util::logutil::bg_logger().warn(
                    "Failed to parse TLS certificates to get server status",
                    &[tidb_log::Field::new("error", tidb_log::Value::Str(error))],
                ),
            }
        }
        let start = tidb_session::domain::real_tikv_node::server_start_unix_timestamp();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |since| since.as_secs() as i64);
        m.insert(UP_TIME.to_owned(), (now - start).to_string());
        Ok(m)
    }
}

/// Go `time.Format("Jan _2 15:04:05 2006 MST")` of a certificate's UTC time.
fn certificate_time(unix_seconds: i64) -> String {
    chrono::DateTime::from_timestamp(unix_seconds, 0)
        .map(|time| time.format("%b %e %H:%M:%S %Y UTC").to_string())
        .unwrap_or_default()
}

/// Go `compressionStats`: the connection's protocol compression.
pub(crate) struct CompressionStatistics;

impl Statistics for CompressionStatistics {
    fn get_scope(&self, _status: &str) -> u8 {
        tidb_session::sysvar::SCOPE_SESSION
    }

    fn stats(&self, vars: Option<&SessionVars>) -> Result<BTreeMap<String, String>, String> {
        use tidb_mysql::consts::{CompressionZlib, CompressionZstd, ZlibCompressDefaultLevel};
        let (algorithm, level) = vars.map_or((0, 0), |vars| {
            (vars.compression_algorithm, vars.compression_level)
        });
        let (compression, name, level) = match algorithm {
            CompressionZlib => ("ON", "zlib", ZlibCompressDefaultLevel.to_string()),
            CompressionZstd => ("ON", "zstd", level.to_string()),
            _ => ("OFF", "", "0".to_owned()),
        };
        Ok(BTreeMap::from([
            ("Compression".to_owned(), compression.to_owned()),
            ("Compression_algorithm".to_owned(), name.to_owned()),
            ("Compression_level".to_owned(), level),
        ]))
    }
}

/// A provider registered for as long as its server lives.
pub(crate) struct RegisteredStatistics(Arc<dyn Statistics>);

impl RegisteredStatistics {
    /// Go `variable.RegisterStatistics`.
    pub(crate) fn register(statistics: Arc<dyn Statistics>) -> Self {
        tidb_session::statusvar::register_statistics(Arc::clone(&statistics));
        Self(statistics)
    }
}

impl Drop for RegisteredStatistics {
    fn drop(&mut self) {
        tidb_session::statusvar::unregister_statistics(&self.0);
    }
}

/// Go `init`'s `variable.RegisterStatistics(&compressionStats{})`: once per
/// process.
pub(crate) fn register_compression_statistics() {
    static REGISTERED: std::sync::Once = std::sync::Once::new();
    REGISTERED.call_once(|| {
        tidb_session::statusvar::register_statistics(Arc::new(CompressionStatistics));
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Go `TestCompressionStats` (`server/conn_test.go`).
    #[test]
    fn compression_statuses_follow_the_session() {
        let mut vars = SessionVars::new();
        let stats = |vars: &SessionVars| CompressionStatistics.stats(Some(vars)).unwrap();
        assert_eq!(stats(&vars)["Compression"], "OFF");
        assert_eq!(stats(&vars)["Compression_algorithm"], "");
        assert_eq!(stats(&vars)["Compression_level"], "0");
        vars.compression_algorithm = tidb_mysql::consts::CompressionZlib;
        assert_eq!(stats(&vars)["Compression"], "ON");
        assert_eq!(stats(&vars)["Compression_algorithm"], "zlib");
        assert_eq!(stats(&vars)["Compression_level"], "6");
        vars.compression_algorithm = tidb_mysql::consts::CompressionZstd;
        vars.compression_level = 1;
        assert_eq!(stats(&vars)["Compression_algorithm"], "zstd");
        assert_eq!(stats(&vars)["Compression_level"], "1");
    }

    #[test]
    fn certificate_times_are_go_s_layout() {
        // 2026-01-02T03:04:05Z and 2026-11-12T13:14:15Z.
        assert_eq!(certificate_time(1_767_323_045), "Jan  2 03:04:05 2026 UTC");
        assert_eq!(certificate_time(1_794_489_255), "Nov 12 13:14:15 2026 UTC");
    }
}
