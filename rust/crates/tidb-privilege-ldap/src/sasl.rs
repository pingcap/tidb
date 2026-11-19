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

//! Go `sasl.go`.

use std::sync::{LazyLock, Mutex, PoisonError};

use crate::ldap_common::LdapAuthImpl;
use crate::ldap_conn::LDAP_RESULT_SUCCESS;

/// Go `ldapSASLAuthImpl`.
#[derive(Default)]
pub struct LdapSaslAuthImpl {
    inner: LdapAuthImpl,
    sasl_auth_method: Mutex<String>,
}

impl std::ops::Deref for LdapSaslAuthImpl {
    type Target = LdapAuthImpl;

    fn deref(&self) -> &LdapAuthImpl {
        &self.inner
    }
}

impl LdapSaslAuthImpl {
    /// Go `AuthLDAPSASL`: authenticates the user through LDAP SASL, relaying
    /// each server challenge to the client and its answer back.
    ///
    /// # Errors
    ///
    /// The user search, the connection, a bind step or the client
    /// exchange failed.
    pub fn auth_ldap_sasl(
        &self,
        user_name: &str,
        dn: &str,
        client_cred: &[u8],
        auth_conn: &mut dyn tidb_privilege_conn::AuthConn,
    ) -> Result<(), String> {
        // The configuration is read-locked only while the DN and the
        // connection are resolved: the exchange below waits on the client,
        // which may never answer.
        let (dn, mut ldap_conn) = {
            let state = self.inner.read();
            let dn = if dn.is_empty() {
                state.search_user(user_name)?
            } else {
                state.canonicalize_dn(user_name, dn)
            };
            let ldap_conn = state
                .get_connection()
                .map_err(|error| format!("create LDAP connection: {error}"))?;
            (dn, ldap_conn)
        };
        let method = self.get_sasl_auth_method();
        let result = (|| {
            let mut client_cred = client_cred.to_vec();
            loop {
                let (result_code, server_cred) = ldap_conn
                    .server_bind_step(&client_cred, &dn, &method)
                    .map_err(|error| error.to_string())?;
                // The client is still waiting for the last serverCred, so
                // the loop cannot end before it is sent even when the
                // result code is 0.
                auth_conn.write_auth_more_data(&server_cred)?;
                auth_conn.flush()?;
                // `LDAP_SUCCESS`: the authentication succeeded.
                if result_code == LDAP_RESULT_SUCCESS {
                    return Ok(());
                }
                client_cred = auth_conn.read_packet()?;
            }
        })();
        self.inner.read().put_connection(ldap_conn);
        result
    }

    /// Go `SetSASLAuthMethod`.
    pub fn set_sasl_auth_method(&self, sasl_auth_method: &str) {
        *self
            .sasl_auth_method
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = sasl_auth_method.to_owned();
    }

    /// Go `GetSASLAuthMethod`.
    pub fn get_sasl_auth_method(&self) -> String {
        self.sasl_auth_method
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

/// Go `LDAPSASLAuthImpl`: authentication with LDAP SASL.
pub static LDAP_SASL_AUTH_IMPL: LazyLock<LdapSaslAuthImpl> =
    LazyLock::new(LdapSaslAuthImpl::default);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ldap_conn::ber;
    use std::io::{Read, Write};

    struct MockAuthConn {
        written: Vec<Vec<u8>>,
        to_read: Vec<Vec<u8>>,
    }

    impl tidb_privilege_conn::AuthConn for MockAuthConn {
        fn write_auth_more_data(&mut self, data: &[u8]) -> Result<(), String> {
            self.written.push(data.to_vec());
            Ok(())
        }
        fn read_packet(&mut self) -> Result<Vec<u8>, String> {
            Ok(self.to_read.remove(0))
        }
        fn flush(&mut self) -> Result<(), String> {
            Ok(())
        }
    }

    fn reply(id: i64, code: i64, server_cred: Option<&[u8]>) -> Vec<u8> {
        let mut op = vec![
            ber::integer(ber::TAG_ENUMERATED, code),
            ber::string(ber::TAG_OCTET_STRING, b""),
            ber::string(ber::TAG_OCTET_STRING, b""),
        ];
        op.extend(server_cred.map(|cred| ber::string(ber::CLASS_CONTEXT | 7, cred)));
        ber::constructed(
            ber::TAG_SEQUENCE,
            &[
                ber::integer(ber::TAG_INTEGER, id),
                ber::constructed(ber::CLASS_APPLICATION | ber::CONSTRUCTED | 1, &op),
            ],
        )
    }

    /// Go `AuthLDAPSASL`: each server challenge is written to the client
    /// (the last one too, after `LDAP_SUCCESS`), and each client answer is
    /// the next bind step's credentials, against the stored DN with the
    /// configured mechanism.
    #[test]
    fn sasl_relays_the_exchange_until_success() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = std::thread::spawn(move || {
            let (mut tcp, _) = listener.accept().unwrap();
            let mut steps = Vec::new();
            for _ in 0..3 {
                let mut header = [0u8; 2];
                tcp.read_exact(&mut header).unwrap();
                let mut body = vec![0u8; usize::from(header[1])];
                tcp.read_exact(&mut body).unwrap();
                let parts = ber::children(&body).unwrap();
                let id = ber::integer_value(&parts[0]);
                let request = ber::children(&parts[1].content).unwrap();
                let dn = String::from_utf8(request[1].content.clone()).unwrap();
                let response = if request[2].tag == ber::CLASS_CONTEXT {
                    // The root DN's simple bind.
                    steps.push(format!("simple {dn}"));
                    reply(id, 0, None)
                } else {
                    let sasl = ber::children(&request[2].content).unwrap();
                    let mechanism = String::from_utf8(sasl[0].content.clone()).unwrap();
                    let cred = String::from_utf8(sasl[1].content.clone()).unwrap();
                    steps.push(format!("sasl {dn} {mechanism} {cred}"));
                    if cred == "client-first" {
                        reply(id, 14, Some(b"server-first"))
                    } else {
                        reply(id, 0, Some(b"server-final"))
                    }
                };
                tcp.write_all(&response).unwrap();
            }
            steps
        });
        let impl_ = LdapSaslAuthImpl::default();
        impl_.set_ldap_server_host("127.0.0.1");
        impl_.set_ldap_server_port(i64::from(port));
        impl_.set_bind_root_dn("cn=root");
        impl_.set_bind_root_pw("rootpw");
        impl_.set_search_attr("uid");
        impl_.set_sasl_auth_method("SCRAM-SHA-1");
        impl_.set_max_capacity(1);
        impl_.set_init_capacity(1);
        let mut auth_conn = MockAuthConn {
            written: Vec::new(),
            to_read: vec![b"client-final".to_vec()],
        };
        impl_
            .auth_ldap_sasl("yka", "+dc=ping,dc=cap", b"client-first", &mut auth_conn)
            .unwrap();
        assert_eq!(
            auth_conn.written,
            [b"server-first".to_vec(), b"server-final".to_vec()]
        );
        assert_eq!(
            server.join().unwrap(),
            [
                "simple cn=root",
                "sasl uid=yka,dc=ping,dc=cap SCRAM-SHA-1 client-first",
                "sasl uid=yka,dc=ping,dc=cap SCRAM-SHA-1 client-final",
            ]
        );
    }
}
