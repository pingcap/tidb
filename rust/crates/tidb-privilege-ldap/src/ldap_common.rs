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

//! Go `ldap_common.go`.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, PoisonError, RwLock, RwLockReadGuard};
use std::time::Duration;

use crate::ldap_conn::{self, Conn};

/// Go `ldapTimeout`, in milliseconds: the TCP / TLS dialing timeout and the
/// LDAP request timeout. With TLS, a connection can fail only after twice
/// this, because both `StartTLS` and `TLS` are tried.
pub(crate) static LDAP_TIMEOUT_MS: AtomicU64 = AtomicU64::new(10_000);

/// Go `skipTLSForTest`: only `StartTLS` is tried.
pub(crate) static SKIP_TLS_FOR_TEST: AtomicBool = AtomicBool::new(false);

fn ldap_timeout() -> Duration {
    Duration::from_millis(LDAP_TIMEOUT_MS.load(Ordering::Relaxed))
}

/// Go `getConnectionMaxRetry`.
const GET_CONNECTION_MAX_RETRY: usize = 10;
/// Go `getConnectionRetryInterval`.
const GET_CONNECTION_RETRY_INTERVAL: Duration = Duration::from_millis(500);

/// The configuration a connection is made with, captured when the pool is
/// (re)built: Go's factory closure reads the impl's fields at each call,
/// which the impl's lock keeps stable while the pool is in use.
#[derive(Clone)]
struct ConnectConfig {
    ldap_server_host: String,
    ldap_server_port: i64,
    enable_tls: bool,
    ca_pool: Option<rustls::RootCertStore>,
}

/// Go `ngaut/pools.ResourcePool` with no idle timeout: `capacity` slots,
/// each empty until a `Get` fills it through the factory; `Put(nil)`
/// empties a slot again.
struct ResourcePool {
    config: ConnectConfig,
    slots: Mutex<VecDeque<Option<Conn>>>,
    available: Condvar,
}

impl ResourcePool {
    fn new(config: ConnectConfig, capacity: usize) -> Self {
        Self {
            config,
            slots: Mutex::new((0..capacity).map(|_| None).collect()),
            available: Condvar::new(),
        }
    }

    /// Go `ResourcePool.Get`: waits for a slot, and makes a connection for
    /// an empty one (a failure empties the slot again).
    fn get(&self) -> Result<Conn, ldap_conn::Error> {
        let slot = {
            let mut slots = self.slots.lock().unwrap_or_else(PoisonError::into_inner);
            loop {
                if let Some(slot) = slots.pop_front() {
                    break slot;
                }
                slots = self
                    .available
                    .wait(slots)
                    .unwrap_or_else(PoisonError::into_inner);
            }
        };
        match slot {
            Some(conn) => Ok(conn),
            None => connection_factory(&self.config).inspect_err(|_| self.put(None)),
        }
    }

    /// Go `ResourcePool.Put`.
    fn put(&self, conn: Option<Conn>) {
        self.slots
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push_back(conn);
        self.available.notify_one();
    }
}

/// The fields Go's `ldapAuthImpl` keeps behind its `RWMutex`.
#[derive(Default)]
pub(crate) struct LdapAuthState {
    // the following attributes are used to search the users
    pub(crate) bind_base_dn: String,
    pub(crate) bind_root_dn: String,
    pub(crate) bind_root_pwd: String,
    pub(crate) search_attr: String,

    // the following attributes are used to connect to LDAP server
    ldap_server_host: String,
    ldap_server_port: i64,
    enable_tls: bool,
    ca_path: String,
    init_capacity: i64,
    max_capacity: i64,

    ca_pool: Option<rustls::RootCertStore>,

    ldap_connection_pool: Option<Arc<ResourcePool>>,
}

/// Go `ldapAuthImpl`: the internal utilities of authentication with LDAP.
/// The getters and setters lock inside; the other methods run on a held
/// [`LdapAuthState`].
#[derive(Default)]
pub struct LdapAuthImpl {
    state: RwLock<LdapAuthState>,
}

impl LdapAuthState {
    /// Go `searchUser`: binds the root DN and finds the DN whose
    /// `searchAttr` is `user_name`.
    pub(crate) fn search_user(&self, user_name: &str) -> Result<String, String> {
        let mut l = self.get_connection()?;
        let result = (|| {
            l.bind(&self.bind_root_dn, &self.bind_root_pwd)
                .map_err(|error| format!("bind root dn to search user: {error}"))?;
            let entries = l
                .search(
                    &self.bind_base_dn,
                    ldap_conn::SCOPE_WHOLE_SUBTREE,
                    &format!("({}={})", self.search_attr, user_name),
                )
                .map_err(|error| error.to_string())?;
            entries
                .into_iter()
                .next()
                .ok_or_else(|| "LDAP user not found".to_owned())
        })();
        self.put_connection(l);
        result
    }

    /// Go `canonicalizeDN`: turns the `dn` stored in the database into the
    /// `dn` the LDAP server recognizes. A leading `+` becomes
    /// `${searchAttr}=${userName},...`. Both arguments are non-empty.
    pub(crate) fn canonicalize_dn(&self, user_name: &str, dn: &str) -> String {
        match dn.strip_prefix('+') {
            Some(rest) => format!("{}={},{}", self.search_attr, user_name, rest),
            None => dn.to_owned(),
        }
    }

    /// Go `initializeCAPool`.
    fn initialize_ca_pool(&mut self) -> Result<(), String> {
        if self.ca_path.is_empty() {
            self.ca_pool = None;
            return Ok(());
        }
        let ca_cert = std::fs::read(&self.ca_path)
            .map_err(|error| format!("read ca certificate at {}: {error}", self.ca_path))?;
        self.ca_pool = Some(ca_pool_from_pem(&ca_cert).ok_or("fail to parse ca certificate")?);
        Ok(())
    }

    fn connect_config(&self) -> ConnectConfig {
        ConnectConfig {
            ldap_server_host: self.ldap_server_host.clone(),
            ldap_server_port: self.ldap_server_port,
            enable_tls: self.enable_tls,
            ca_pool: self.ca_pool.clone(),
        }
    }

    /// Go `getConnection`: a pooled connection, bound back to the root DN.
    /// The bind clears the previous user's binding and detects a
    /// connection the server closed; a failed one is dropped and another
    /// taken, ten times at most.
    pub(crate) fn get_connection(&self) -> Result<Conn, String> {
        let pool = self
            .ldap_connection_pool
            .clone()
            .ok_or("the LDAP connection pool is not initialized")?;
        let mut retry_count = 0;
        loop {
            let mut conn = pool.get().map_err(|error| error.to_string())?;
            match conn.simple_bind(&self.bind_root_dn, &self.bind_root_pwd) {
                Ok(()) => return Ok(conn),
                Err(error) => {
                    tidb_logutil::bg_logger().warn(
                        "fail to use LDAP connection bind to anonymous user. Retrying",
                        &[
                            tidb_log::Field::new("error", tidb_log::Value::Str(error.to_string())),
                            tidb_log::Field::new(
                                "backoff",
                                tidb_log::Value::Duration(
                                    GET_CONNECTION_RETRY_INTERVAL.as_nanos() as i64
                                ),
                            ),
                        ],
                    );
                    // fail to bind to anonymous user, just release this
                    // connection and try to get a new one
                    drop(conn);
                    pool.put(None);
                    retry_count += 1;
                    if retry_count >= GET_CONNECTION_MAX_RETRY {
                        return Err(format!("fail to bind to anonymous user: {error}"));
                    }
                    std::thread::sleep(GET_CONNECTION_RETRY_INTERVAL);
                }
            }
        }
    }

    /// Go `putConnection`.
    pub(crate) fn put_connection(&self, conn: Conn) {
        if let Some(pool) = &self.ldap_connection_pool {
            pool.put(Some(conn));
        }
    }

    /// Go `initializePool`: skips re-initialization when the capacities
    /// are not correct.
    fn initialize_pool(&mut self) {
        if self.init_capacity > 0 && self.max_capacity >= self.init_capacity {
            self.ldap_connection_pool = Some(Arc::new(ResourcePool::new(
                self.connect_config(),
                usize::try_from(self.init_capacity).unwrap_or(usize::MAX),
            )));
        }
    }
}

/// Go `x509.NewCertPool()` + `AppendCertsFromPEM`: `None` when no
/// certificate parses.
pub(crate) fn ca_pool_from_pem(pem: &[u8]) -> Option<rustls::RootCertStore> {
    let mut pool = rustls::RootCertStore::empty();
    for cert in rustls_pemfile::certs(&mut &pem[..]).flatten() {
        let _ = pool.add(cert);
    }
    (!pool.is_empty()).then_some(pool)
}

/// Go `tryConnectLDAPThroughStartTLS`.
fn try_connect_ldap_through_start_tls(
    config: &ConnectConfig,
    address: &str,
) -> Result<Conn, ldap_conn::Error> {
    let mut ldap_connection = Conn::dial_plain(address, ldap_timeout())?;
    ldap_connection.set_timeout(ldap_timeout());
    ldap_connection.start_tls(
        ldap_conn::tls_config(config.ca_pool.as_ref())?,
        &config.ldap_server_host,
    )
}

/// Go `tryConnectLDAPThroughTLS`.
fn try_connect_ldap_through_tls(
    config: &ConnectConfig,
    address: &str,
) -> Result<Conn, ldap_conn::Error> {
    let mut ldap_connection = Conn::dial_tls(
        address,
        ldap_conn::tls_config(config.ca_pool.as_ref())?,
        &config.ldap_server_host,
        ldap_timeout(),
    )?;
    ldap_connection.set_timeout(ldap_timeout());
    Ok(ldap_connection)
}

use tidb_go_net::join_host_port_number;

/// Go `connectionFactory`.
fn connection_factory(config: &ConnectConfig) -> Result<Conn, ldap_conn::Error> {
    let address = join_host_port_number(&config.ldap_server_host, config.ldap_server_port);
    if config.enable_tls {
        return match try_connect_ldap_through_start_tls(config, &address) {
            Ok(conn) => Ok(conn),
            Err(error) if tidb_intest::IN_TEST && SKIP_TLS_FOR_TEST.load(Ordering::Relaxed) => {
                Err(error)
            }
            Err(_) => try_connect_ldap_through_tls(config, &address)
                .map_err(|error| wrap(&error, "create ldap connection")),
        };
    }
    let mut ldap_connection = Conn::dial_plain(&address, ldap_timeout())
        .map_err(|error| wrap(&error, "create ldap connection"))?;
    ldap_connection.set_timeout(ldap_timeout());
    Ok(ldap_connection)
}

/// Go `errors.Wrap(err, message)` for an LDAP error.
fn wrap(error: &ldap_conn::Error, message: &str) -> ldap_conn::Error {
    ldap_conn::Error {
        result_code: error.result_code,
        message: format!("{message}: {}", error.message),
    }
}

impl LdapAuthImpl {
    pub(crate) fn read(&self) -> RwLockReadGuard<'_, LdapAuthState> {
        self.state.read().unwrap_or_else(PoisonError::into_inner)
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, LdapAuthState> {
        self.state.write().unwrap_or_else(PoisonError::into_inner)
    }

    /// Go `SetBindBaseDN`: the BaseDN users are searched under.
    pub fn set_bind_base_dn(&self, bind_base_dn: &str) {
        self.write().bind_base_dn = bind_base_dn.to_owned();
    }

    /// Go `SetBindRootDN`: the user a connection binds before searching.
    pub fn set_bind_root_dn(&self, bind_root_dn: &str) {
        self.write().bind_root_dn = bind_root_dn.to_owned();
    }

    /// Go `SetBindRootPW`: the password of the root DN.
    pub fn set_bind_root_pw(&self, bind_root_pw: &str) {
        self.write().bind_root_pwd = bind_root_pw.to_owned();
    }

    /// Go `SetSearchAttr`.
    pub fn set_search_attr(&self, search_attr: &str) {
        self.write().search_attr = search_attr.to_owned();
    }

    /// Go `SetLDAPServerHost`.
    pub fn set_ldap_server_host(&self, ldap_server_host: &str) {
        let mut state = self.write();
        if ldap_server_host != state.ldap_server_host {
            state.ldap_server_host = ldap_server_host.to_owned();
            state.initialize_pool();
        }
    }

    /// Go `SetLDAPServerPort`.
    pub fn set_ldap_server_port(&self, ldap_server_port: i64) {
        let mut state = self.write();
        if ldap_server_port != state.ldap_server_port {
            state.ldap_server_port = ldap_server_port;
            state.initialize_pool();
        }
    }

    /// Go `SetEnableTLS`: whether to connect through StartTLS (or TLS).
    pub fn set_enable_tls(&self, enable_tls: bool) {
        let mut state = self.write();
        if enable_tls != state.enable_tls {
            state.enable_tls = enable_tls;
            state.initialize_pool();
        }
    }

    /// Go `SetCAPath`: the CA certificate that verifies the LDAP server.
    ///
    /// # Errors
    ///
    /// The certificate cannot be read or parsed.
    pub fn set_ca_path(&self, path: &str) -> Result<(), String> {
        let mut state = self.write();
        if path != state.ca_path {
            state.ca_path = path.to_owned();
            state.initialize_ca_pool()?;
        }
        Ok(())
    }

    /// Go `SetInitCapacity`.
    pub fn set_init_capacity(&self, init_capacity: i64) {
        let mut state = self.write();
        if init_capacity != state.init_capacity {
            state.init_capacity = init_capacity;
            state.initialize_pool();
        }
    }

    /// Go `SetMaxCapacity`.
    pub fn set_max_capacity(&self, max_capacity: i64) {
        let mut state = self.write();
        if max_capacity != state.max_capacity {
            state.max_capacity = max_capacity;
            state.initialize_pool();
        }
    }

    /// Go `GetBindBaseDN`.
    pub fn get_bind_base_dn(&self) -> String {
        self.read().bind_base_dn.clone()
    }

    /// Go `GetBindRootDN`.
    pub fn get_bind_root_dn(&self) -> String {
        self.read().bind_root_dn.clone()
    }

    /// Go `GetBindRootPW`.
    pub fn get_bind_root_pw(&self) -> String {
        self.read().bind_root_pwd.clone()
    }

    /// Go `GetSearchAttr`.
    pub fn get_search_attr(&self) -> String {
        self.read().search_attr.clone()
    }

    /// Go `GetLDAPServerHost`.
    pub fn get_ldap_server_host(&self) -> String {
        self.read().ldap_server_host.clone()
    }

    /// Go `GetLDAPServerPort`.
    pub fn get_ldap_server_port(&self) -> i64 {
        self.read().ldap_server_port
    }

    /// Go `GetEnableTLS`.
    pub fn get_enable_tls(&self) -> bool {
        self.read().enable_tls
    }

    /// Go `GetCAPath`.
    pub fn get_ca_path(&self) -> String {
        self.read().ca_path.clone()
    }

    /// Go `GetInitCapacity`.
    pub fn get_init_capacity(&self) -> i64 {
        self.read().init_capacity
    }

    /// Go `GetMaxCapacity`.
    pub fn get_max_capacity(&self) -> i64 {
        self.read().max_capacity
    }
}

/// Go `ldap_common_test.go`.
#[cfg(test)]
mod ldap_common_test {
    use super::*;
    use std::io::Read;
    use std::net::TcpListener;
    use std::time::Instant;

    const TLS_CA_STR: &[u8] = include_bytes!("../testdata/ca.crt");
    const TLS_CRT_STR: &[u8] = include_bytes!("../testdata/ldap.crt");
    const TLS_KEY_STR: &[u8] = include_bytes!("../testdata/ldap.key");

    fn impl_for(port: u16) -> LdapAuthState {
        LdapAuthState {
            enable_tls: true,
            ldap_server_host: "localhost".to_owned(),
            ldap_server_port: i64::from(port),
            ca_pool: ca_pool_from_pem(TLS_CA_STR),
            ..LdapAuthState::default()
        }
    }

    /// Go `tls.Listen` with the test certificate, each connection read to
    /// its end.
    fn tls_server() -> (u16, std::thread::JoinHandle<()>) {
        let certs: Vec<_> = rustls_pemfile::certs(&mut &TLS_CRT_STR[..])
            .flatten()
            .collect();
        let key = rustls_pemfile::private_key(&mut &TLS_KEY_STR[..])
            .unwrap()
            .unwrap();
        let config = Arc::new(
            rustls::ServerConfig::builder_with_provider(Arc::new(
                rustls::crypto::ring::default_provider(),
            ))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .unwrap(),
        );
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let handle = std::thread::spawn(move || {
            // handling one connection at a time is enough for test: the
            // StartTLS attempt, then the TLS one.
            for _ in 0..2 {
                let Ok((tcp, _)) = listener.accept() else {
                    return;
                };
                let Ok(connection) = rustls::ServerConnection::new(Arc::clone(&config)) else {
                    return;
                };
                let mut stream = rustls::StreamOwned::new(connection, tcp);
                let mut buf = [0u8; 256];
                while matches!(stream.read(&mut buf), Ok(n) if n > 0) {}
            }
        });
        (port, handle)
    }

    /// Go `TestCanonicalizeDN`.
    #[test]
    fn test_canonicalize_dn() {
        let state = LdapAuthState {
            search_attr: "cn".to_owned(),
            ..LdapAuthState::default()
        };
        assert_eq!(
            state.canonicalize_dn("yka", "cn=y,dc=ping,dc=cap"),
            "cn=y,dc=ping,dc=cap"
        );
        assert_eq!(
            state.canonicalize_dn("yka", "+dc=ping,dc=cap"),
            "cn=yka,dc=ping,dc=cap"
        );
    }

    /// Go `TestConnectThrough636`: with TLS enabled, a server that speaks
    /// TLS from the first byte fails StartTLS and is reached through TLS.
    #[test]
    fn test_connect_through_636() {
        let (port, server) = tls_server();
        let state = impl_for(port);
        let conn = connection_factory(&state.connect_config()).unwrap();
        drop(conn);
        server.join().unwrap();
    }

    /// Go `TestConnectWithTLS11`: a server that only speaks TLS 1.1 answers
    /// the client's hello with a `protocol_version` alert, on both tries.
    #[test]
    fn test_connect_with_tls11() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = std::thread::spawn(move || {
            for _ in 0..2 {
                let Ok((mut tcp, _)) = listener.accept() else {
                    return;
                };
                let mut buf = [0u8; 512];
                let _ = tcp.read(&mut buf);
                // TLS 1.1 record: alert, fatal, protocol_version.
                let _ = std::io::Write::write_all(
                    &mut tcp,
                    &[0x15, 0x03, 0x02, 0x00, 0x02, 0x02, 0x46],
                );
            }
        });
        let state = impl_for(port);
        let error = connection_factory(&state.connect_config()).err().unwrap();
        assert!(error.to_string().contains("ProtocolVersion"), "{error}");
        server.join().unwrap();
    }

    /// Go `TestLDAPStartTLSTimeout`: a server that never answers StartTLS
    /// fails the connection after the LDAP timeout.
    #[test]
    fn test_ldap_start_tls_timeout() {
        LDAP_TIMEOUT_MS.store(2_000, Ordering::Relaxed);
        SKIP_TLS_FOR_TEST.store(true, Ordering::Relaxed);
        struct Restore;
        impl Drop for Restore {
            fn drop(&mut self) {
                LDAP_TIMEOUT_MS.store(10_000, Ordering::Relaxed);
                SKIP_TLS_FOR_TEST.store(false, Ordering::Relaxed);
            }
        }
        let _restore = Restore;
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let (after_timeout, wait) = std::sync::mpsc::channel::<()>();
        let server = std::thread::spawn(move || {
            let (conn, _) = listener.accept().unwrap();
            let _ = wait.recv();
            drop(conn);
        });
        let mut state = impl_for(port);
        state.init_capacity = 1;
        state.max_capacity = 1;
        let now = Instant::now();
        let error = connection_factory(&state.connect_config()).err().unwrap();
        after_timeout.send(()).unwrap();
        let dur = now.elapsed();
        assert!(dur > Duration::from_secs(2), "{dur:?}");
        assert!(dur < Duration::from_secs(3), "{dur:?}");
        assert!(
            error.to_string().contains("connection timed out"),
            "{error}"
        );
        server.join().unwrap();
    }
}
