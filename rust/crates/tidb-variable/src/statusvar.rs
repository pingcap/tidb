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

//! Go `pkg/sessionctx/variable/statusvar.go`: the process-wide
//! status-variable providers `SHOW STATUS` collects from, and the default
//! provider `init` registers.

use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
use std::sync::{Arc, LazyLock, RwLock};

use crate::sysvar::{SCOPE_GLOBAL, SCOPE_SESSION};

use crate::SessionVars;

/// Go `DefaultStatusVarScopeFlag`.
pub const DEFAULT_STATUS_VAR_SCOPE_FLAG: u8 = SCOPE_GLOBAL | SCOPE_SESSION;

/// Go `Statistics`.
pub trait Statistics: Send + Sync {
    /// Go `GetScope`: the scope of one of the provider's statuses.
    fn get_scope(&self, status: &str) -> u8;
    /// Go `Stats`: the provider's statuses by name, rendered as Go's
    /// `types.ToString` renders them. `vars` is `None` where Go's may be
    /// nil.
    fn stats(&self, vars: Option<&SessionVars>) -> Result<BTreeMap<String, String>, String>;
}

/// Go `StatusVal`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StatusVal {
    /// Go `Scope`.
    pub scope: u8,
    /// Go `Value`, rendered.
    pub value: String,
}

/// Go `statisticsList`, holding the default provider Go's `init` registers.
static STATISTICS_LIST: LazyLock<RwLock<Vec<Arc<dyn Statistics>>>> =
    LazyLock::new(|| RwLock::new(vec![Arc::new(DefaultStatusStat) as Arc<dyn Statistics>]));

/// Go `RegisterStatistics`.
pub fn register_statistics(statistics: Arc<dyn Statistics>) {
    STATISTICS_LIST
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .push(statistics);
}

/// Go `UnregisterStatistics`.
pub fn unregister_statistics(statistics: &Arc<dyn Statistics>) {
    let mut list = STATISTICS_LIST
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(index) = list
        .iter()
        .rposition(|registered| Arc::ptr_eq(registered, statistics))
    {
        list.swap_remove(index);
    }
}

/// Go `GetStatusVars`: every registered provider's statuses; a later
/// provider's status replaces an earlier one of the same name.
pub fn get_status_vars(
    vars: Option<&SessionVars>,
) -> Result<BTreeMap<String, StatusVal>, String> {
    let list = STATISTICS_LIST
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let mut status_vars = BTreeMap::new();
    for statistics in list.iter() {
        for (name, value) in statistics.stats(vars)? {
            let scope = statistics.get_scope(&name);
            status_vars.insert(name, StatusVal { scope, value });
        }
    }
    Ok(status_vars)
}

/// Go `tlsCiphers`.
const TLS_CIPHERS: [u16; 25] = [
    0x0005, // TLS_RSA_WITH_RC4_128_SHA
    0x000a, // TLS_RSA_WITH_3DES_EDE_CBC_SHA
    0x002f, // TLS_RSA_WITH_AES_128_CBC_SHA
    0x0035, // TLS_RSA_WITH_AES_256_CBC_SHA
    0x003c, // TLS_RSA_WITH_AES_128_CBC_SHA256
    0x009c, // TLS_RSA_WITH_AES_128_GCM_SHA256
    0x009d, // TLS_RSA_WITH_AES_256_GCM_SHA384
    0xc007, // TLS_ECDHE_ECDSA_WITH_RC4_128_SHA
    0xc009, // TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA
    0xc00a, // TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA
    0xc011, // TLS_ECDHE_RSA_WITH_RC4_128_SHA
    0xc012, // TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA
    0xc013, // TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA
    0xc014, // TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA
    0xc023, // TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256
    0xc027, // TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256
    0xc02f, // TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256
    0xc02b, // TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256
    0xc030, // TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384
    0xc02c, // TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384
    0xcca8, // TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305
    0xcca9, // TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305
    0x1301, // TLS_AES_128_GCM_SHA256
    0x1302, // TLS_AES_256_GCM_SHA384
    0x1303, // TLS_CHACHA20_POLY1305_SHA256
];

/// Go `tlsSupportedCiphers`: each of `tlsCiphers`' names followed by `:`.
static TLS_SUPPORTED_CIPHERS: LazyLock<String> = LazyLock::new(|| {
    TLS_CIPHERS
        .iter()
        .map(|cipher| format!("{}:", tidb_util::tls::cipher_suite_name(*cipher)))
        .collect()
});

/// Go `defaultStatus`: each status's scope and value.
const DEFAULT_STATUS: [(&str, u8, &str); 7] = [
    ("Ssl_cipher", SCOPE_GLOBAL | SCOPE_SESSION, ""),
    ("Ssl_cipher_list", SCOPE_GLOBAL | SCOPE_SESSION, ""),
    ("Ssl_verify_mode", SCOPE_GLOBAL | SCOPE_SESSION, "0"),
    ("Ssl_version", SCOPE_GLOBAL | SCOPE_SESSION, ""),
    (
        "Performance_schema_session_connect_attrs_longest_seen",
        SCOPE_GLOBAL,
        "0",
    ),
    ("Performance_schema_session_connect_attrs_lost", SCOPE_GLOBAL, "0"),
    ("tidb_keys_examined", SCOPE_SESSION, "0"),
];

/// Go `defaultStatusStat`.
struct DefaultStatusStat;

impl Statistics for DefaultStatusStat {
    fn get_scope(&self, status: &str) -> u8 {
        DEFAULT_STATUS
            .iter()
            .find_map(|(name, scope, _)| (*name == status).then_some(*scope))
            .unwrap_or_default()
    }

    fn stats(&self, vars: Option<&SessionVars>) -> Result<BTreeMap<String, String>, String> {
        let mut status_vars = DEFAULT_STATUS
            .iter()
            .map(|(name, _, value)| ((*name).to_owned(), (*value).to_owned()))
            .collect::<BTreeMap<_, _>>();
        // Read live values from atomic counters for connect attrs status
        // variables.
        status_vars.insert(
            "Performance_schema_session_connect_attrs_longest_seen".to_owned(),
            tidb_vardef::CONNECT_ATTRS_LONGEST_SEEN
                .load(Ordering::Relaxed)
                .to_string(),
        );
        status_vars.insert(
            "Performance_schema_session_connect_attrs_lost".to_owned(),
            tidb_vardef::CONNECT_ATTRS_LOST.load(Ordering::Relaxed).to_string(),
        );
        if let Some(vars) = vars {
            status_vars.insert(
                "tidb_keys_examined".to_owned(),
                vars.keys_examined.to_string(),
            );
            if let Some(state) = &vars.tls_connection_state {
                status_vars.insert(
                    "Ssl_cipher".to_owned(),
                    tidb_util::tls::cipher_suite_name(state.cipher_suite).to_owned(),
                );
                status_vars.insert("Ssl_cipher_list".to_owned(), TLS_SUPPORTED_CIPHERS.clone());
                // tls.VerifyClientCertIfGiven == SSL_VERIFY_PEER | SSL_VERIFY_CLIENT_ONCE
                status_vars.insert("Ssl_verify_mode".to_owned(), (0x01 | 0x04).to_string());
                status_vars.insert(
                    "Ssl_version".to_owned(),
                    tidb_util::tls::version_name(state.version),
                );
            }
        }
        Ok(status_vars)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vars::TlsConnectionState;

    struct Fixed(&'static str, &'static str, u8);

    impl Statistics for Fixed {
        fn get_scope(&self, _status: &str) -> u8 {
            self.2
        }

        fn stats(&self, _vars: Option<&SessionVars>) -> Result<BTreeMap<String, String>, String> {
            Ok(BTreeMap::from([(self.0.to_owned(), self.1.to_owned())]))
        }
    }

    #[test]
    fn registered_statistics_are_collected_until_unregistered() {
        let provider: Arc<dyn Statistics> =
            Arc::new(Fixed("statusvar_test_status", "1", SCOPE_SESSION));
        register_statistics(Arc::clone(&provider));
        assert_eq!(
            Some(&StatusVal {
                scope: SCOPE_SESSION,
                value: "1".to_owned()
            }),
            get_status_vars(None).unwrap().get("statusvar_test_status")
        );
        unregister_statistics(&provider);
        assert!(!get_status_vars(None)
            .unwrap()
            .contains_key("statusvar_test_status"));
    }

    /// Go `TestStatusVar`'s default provider: the plaintext values, then a
    /// TLS session's negotiated names, Go's cipher list and verify mode.
    #[test]
    fn the_default_statuses_follow_the_session() {
        let mut vars = SessionVars::new();
        vars.keys_examined = 20;
        let plain = get_status_vars(Some(&vars)).unwrap();
        let value = |statuses: &BTreeMap<String, StatusVal>, name: &str| {
            statuses.get(name).map(|status| (status.value.clone(), status.scope))
        };
        assert_eq!(value(&plain, "Ssl_cipher"), Some((String::new(), 3)));
        assert_eq!(value(&plain, "Ssl_verify_mode"), Some(("0".to_owned(), 3)));
        assert_eq!(value(&plain, "tidb_keys_examined"), Some(("20".to_owned(), 2)));
        assert_eq!(
            value(&plain, "Performance_schema_session_connect_attrs_lost").map(|v| v.1),
            Some(1)
        );
        // `vars` may be nil: the defaults.
        let detached = get_status_vars(None).unwrap();
        assert_eq!(value(&detached, "tidb_keys_examined"), Some(("0".to_owned(), 2)));

        vars.tls_connection_state = Some(TlsConnectionState {
            version: 0x0304,
            cipher_suite: 0x1301,
        });
        let secure = get_status_vars(Some(&vars)).unwrap();
        assert_eq!(
            value(&secure, "Ssl_cipher").unwrap().0,
            "TLS_AES_128_GCM_SHA256"
        );
        assert_eq!(value(&secure, "Ssl_version").unwrap().0, "TLSv1.3");
        assert_eq!(value(&secure, "Ssl_verify_mode").unwrap().0, "5");
        assert_eq!(
            value(&secure, "Ssl_cipher_list").unwrap().0,
            "RC4-SHA:DES-CBC3-SHA:AES128-SHA:AES256-SHA:AES128-SHA256:AES128-GCM-SHA256:\
             AES256-GCM-SHA384:ECDHE-ECDSA-RC4-SHA:ECDHE-ECDSA-AES128-SHA:\
             ECDHE-ECDSA-AES256-SHA:ECDHE-RSA-RC4-SHA:ECDHE-RSA-DES-CBC3-SHA:\
             ECDHE-RSA-AES128-SHA:ECDHE-RSA-AES256-SHA:ECDHE-ECDSA-AES128-SHA256:\
             ECDHE-RSA-AES128-SHA256:ECDHE-RSA-AES128-GCM-SHA256:\
             ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES256-GCM-SHA384:\
             ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-CHACHA20-POLY1305:\
             ECDHE-ECDSA-CHACHA20-POLY1305:TLS_AES_128_GCM_SHA256:\
             TLS_AES_256_GCM_SHA384:TLS_CHACHA20_POLY1305_SHA256:"
        );
    }
}
