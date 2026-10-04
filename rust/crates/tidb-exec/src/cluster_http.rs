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

//! Shared internal HTTP TLS construction.

pub(crate) fn cluster_http_client(
    security: &tidb_pd_client::ClusterSecurity,
) -> Result<reqwest::blocking::Client, String> {
    let mut builder = reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(5 * 60));
    if security.is_tls_enabled() {
        let ca = std::fs::read(security.ca_path()).map_err(|error| error.to_string())?;
        let roots =
            reqwest::Certificate::from_pem_bundle(&ca).map_err(|error| error.to_string())?;
        if roots.is_empty() {
            return Err("cluster CA contains no certificates".to_owned());
        }
        builder = builder.tls_certs_only(roots);
        if !security.cert_path().is_empty() && !security.key_path().is_empty() {
            let mut identity =
                std::fs::read(security.cert_path()).map_err(|error| error.to_string())?;
            identity.push(b'\n');
            identity.extend(std::fs::read(security.key_path()).map_err(|error| error.to_string())?);
            builder = builder.identity(
                reqwest::Identity::from_pem(&identity).map_err(|error| error.to_string())?,
            );
        }
    }
    builder.build().map_err(|error| error.to_string())
}
