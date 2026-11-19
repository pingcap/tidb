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

//! The subset of `github.com/go-ldap/ldap/v3` (TiDB pins the
//! `YangKeao/ldap` fork) that Go's `ldap` package calls: `DialURL` over
//! `ldap://` and `ldaps://`, `StartTLS`, `SetTimeout`, `Bind` /
//! `SimpleBind`, `Search` with a one-item filter, and the fork's
//! `ServerBindStep`. Requests run one at a time on the caller's thread,
//! which is how Go's package uses a pooled connection.

use std::io::{self, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::Arc;
use std::time::Duration;

/// Go `ldap.Error`: `LDAP Result Code <code> "<name>": <message>`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Error {
    /// Go `Error.ResultCode`.
    pub result_code: u16,
    /// Go `Error.Err`.
    pub message: String,
}

/// Go `ErrorNetwork`.
pub const ERROR_NETWORK: u16 = 200;
/// Go `ErrorFilterCompile`.
pub const ERROR_FILTER_COMPILE: u16 = 201;
/// Go `ErrorEmptyPassword`.
pub const ERROR_EMPTY_PASSWORD: u16 = 206;
/// Go `ErrorUnexpectedResponse`.
pub const ERROR_UNEXPECTED_RESPONSE: u16 = 205;
/// Go `LDAPResultSuccess`.
pub const LDAP_RESULT_SUCCESS: u16 = 0;
/// Go `LDAPResultSaslBindInProgress`.
pub const LDAP_RESULT_SASL_BIND_IN_PROGRESS: u16 = 14;

impl Error {
    fn new(result_code: u16, message: impl Into<String>) -> Self {
        Self {
            result_code,
            message: message.into(),
        }
    }

    fn network(error: &io::Error) -> Self {
        if matches!(
            error.kind(),
            io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
        ) {
            return Self::new(ERROR_NETWORK, "ldap: connection timed out");
        }
        Self::new(ERROR_NETWORK, error.to_string())
    }
}

/// Go `LDAPResultCodeMap`, for the codes this subset can see.
fn result_code_name(code: u16) -> &'static str {
    match code {
        0 => "Success",
        1 => "Operations Error",
        2 => "Protocol Error",
        3 => "Time Limit Exceeded",
        4 => "Size Limit Exceeded",
        7 => "Auth Method Not Supported",
        8 => "Strong Auth Required",
        14 => "SASL Bind In Progress",
        32 => "No Such Object",
        34 => "Invalid DN Syntax",
        48 => "Inappropriate Authentication",
        49 => "Invalid Credentials",
        50 => "Insufficient Access Rights",
        51 => "Busy",
        52 => "Unavailable",
        53 => "Unwilling To Perform",
        80 => "Other",
        200 => "Network Error",
        201 => "Filter Compile Error",
        202 => "Filter Decompile Error",
        203 => "Debug Error",
        204 => "Not Implemented",
        205 => "Unexpected Response",
        206 => "Empty password not allowed by the client",
        _ => "Unknown",
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "LDAP Result Code {} {:?}: {}",
            self.result_code,
            result_code_name(self.result_code),
            self.message
        )
    }
}

impl std::error::Error for Error {}

/// BER, as `go-asn1-ber` encodes and decodes LDAP messages.
pub(crate) mod ber {
    use super::Error;

    pub(crate) const CLASS_APPLICATION: u8 = 0x40;
    pub(crate) const CLASS_CONTEXT: u8 = 0x80;
    pub(crate) const CONSTRUCTED: u8 = 0x20;
    pub(crate) const TAG_INTEGER: u8 = 0x02;
    pub(crate) const TAG_OCTET_STRING: u8 = 0x04;
    pub(crate) const TAG_BOOLEAN: u8 = 0x01;
    pub(crate) const TAG_ENUMERATED: u8 = 0x0a;
    pub(crate) const TAG_SEQUENCE: u8 = 0x30;

    /// One element.
    #[derive(Debug)]
    pub(crate) struct Packet {
        pub(crate) tag: u8,
        pub(crate) content: Vec<u8>,
    }

    pub(crate) fn encode(tag: u8, content: &[u8]) -> Vec<u8> {
        let mut out = vec![tag];
        let len = content.len();
        if len < 0x80 {
            out.push(len as u8);
        } else {
            let bytes = len.to_be_bytes();
            let skip = bytes.iter().take_while(|byte| **byte == 0).count();
            out.push(0x80 | (bytes.len() - skip) as u8);
            out.extend_from_slice(&bytes[skip..]);
        }
        out.extend_from_slice(content);
        out
    }

    pub(crate) fn integer(tag: u8, value: i64) -> Vec<u8> {
        let bytes = value.to_be_bytes();
        let mut start = 0;
        while start < 7
            && ((bytes[start] == 0 && bytes[start + 1] & 0x80 == 0)
                || (bytes[start] == 0xff && bytes[start + 1] & 0x80 != 0))
        {
            start += 1;
        }
        encode(tag, &bytes[start..])
    }

    pub(crate) fn string(tag: u8, value: &[u8]) -> Vec<u8> {
        encode(tag, value)
    }

    pub(crate) fn constructed(tag: u8, children: &[Vec<u8>]) -> Vec<u8> {
        encode(tag, &children.concat())
    }

    /// Reads one element's tag, length and content from `bytes` at `pos`.
    pub(crate) fn parse_one(bytes: &[u8], pos: &mut usize) -> Result<Packet, Error> {
        let malformed = || {
            Error::new(
                super::ERROR_UNEXPECTED_RESPONSE,
                "ldap: malformed BER packet",
            )
        };
        let tag = *bytes.get(*pos).ok_or_else(malformed)?;
        *pos += 1;
        let first = *bytes.get(*pos).ok_or_else(malformed)?;
        *pos += 1;
        let len = if first & 0x80 == 0 {
            usize::from(first)
        } else {
            let count = usize::from(first & 0x7f);
            if count == 0 || count > 8 {
                return Err(malformed());
            }
            let mut len = 0usize;
            for _ in 0..count {
                len = (len << 8) | usize::from(*bytes.get(*pos).ok_or_else(malformed)?);
                *pos += 1;
            }
            len
        };
        let end = pos
            .checked_add(len)
            .filter(|end| *end <= bytes.len())
            .ok_or_else(malformed)?;
        let content = bytes[*pos..end].to_vec();
        *pos = end;
        Ok(Packet { tag, content })
    }

    pub(crate) fn children(content: &[u8]) -> Result<Vec<Packet>, Error> {
        let mut pos = 0;
        let mut out = Vec::new();
        while pos < content.len() {
            out.push(parse_one(content, &mut pos)?);
        }
        Ok(out)
    }

    pub(crate) fn integer_value(packet: &Packet) -> i64 {
        let mut value: i64 = if packet.content.first().is_some_and(|byte| byte & 0x80 != 0) {
            -1
        } else {
            0
        };
        for byte in &packet.content {
            value = (value << 8) | i64::from(*byte);
        }
        value
    }
}

const APPLICATION_BIND_REQUEST: u8 = 0;
const APPLICATION_BIND_RESPONSE: u8 = 1;
const APPLICATION_SEARCH_REQUEST: u8 = 3;
const APPLICATION_SEARCH_RESULT_ENTRY: u8 = 4;
const APPLICATION_SEARCH_RESULT_DONE: u8 = 5;
const APPLICATION_SEARCH_RESULT_REFERENCE: u8 = 19;
const APPLICATION_EXTENDED_REQUEST: u8 = 23;
const APPLICATION_EXTENDED_RESPONSE: u8 = 24;

/// Go `ScopeWholeSubtree`.
pub const SCOPE_WHOLE_SUBTREE: i64 = 2;

enum Stream {
    Plain(TcpStream),
    Tls(Box<rustls::StreamOwned<rustls::ClientConnection, TcpStream>>),
}

impl Stream {
    fn tcp(&self) -> &TcpStream {
        match self {
            Self::Plain(stream) => stream,
            Self::Tls(stream) => stream.get_ref(),
        }
    }
}

impl Read for Stream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Plain(stream) => stream.read(buf),
            Self::Tls(stream) => stream.read(buf),
        }
    }
}

impl Write for Stream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Self::Plain(stream) => stream.write(buf),
            Self::Tls(stream) => stream.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::Plain(stream) => stream.flush(),
            Self::Tls(stream) => stream.flush(),
        }
    }
}

/// Go `tls.Config` as the `ldap` package builds it: `RootCAs`,
/// `ServerName` and `MinVersion: tls.VersionTLS12` (every version this TLS
/// stack speaks). `None` roots are Go's nil `RootCAs`: the system's.
pub fn tls_config(
    roots: Option<&rustls::RootCertStore>,
) -> Result<Arc<rustls::ClientConfig>, Error> {
    let roots = match roots {
        Some(roots) => roots.clone(),
        None => {
            let mut system = rustls::RootCertStore::empty();
            for cert in rustls_native_certs::load_native_certs().certs {
                let _ = system.add(cert);
            }
            system
        }
    };
    let config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(|error| Error::new(ERROR_NETWORK, error.to_string()))?
    .with_root_certificates(roots)
    .with_no_client_auth();
    Ok(Arc::new(config))
}

/// Go `ldap.Conn`.
pub struct Conn {
    stream: Stream,
    message_id: i64,
}

fn dial(address: &str, timeout: Duration) -> Result<TcpStream, Error> {
    let mut last = None;
    for addr in address
        .to_socket_addrs()
        .map_err(|error| Error::network(&error))?
    {
        match TcpStream::connect_timeout(&addr, timeout) {
            Ok(stream) => return Ok(stream),
            Err(error) => last = Some(error),
        }
    }
    Err(last.map_or_else(
        || Error::new(ERROR_NETWORK, format!("ldap: no address for {address}")),
        |error| Error::network(&error),
    ))
}

fn tls_handshake(
    mut tcp: TcpStream,
    config: Arc<rustls::ClientConfig>,
    server_name: &str,
) -> Result<rustls::StreamOwned<rustls::ClientConnection, TcpStream>, io::Error> {
    let name = rustls::pki_types::ServerName::try_from(server_name.to_owned())
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
    let mut connection = rustls::ClientConnection::new(config, name).map_err(io::Error::other)?;
    while connection.is_handshaking() {
        connection.complete_io(&mut tcp)?;
    }
    Ok(rustls::StreamOwned::new(connection, tcp))
}

impl Conn {
    /// Go `DialURL("ldap://" + address, DialWithDialer(&net.Dialer{Timeout}))`.
    pub fn dial_plain(address: &str, timeout: Duration) -> Result<Self, Error> {
        Ok(Self {
            stream: Stream::Plain(dial(address, timeout)?),
            message_id: 0,
        })
    }

    /// Go `DialURL("ldaps://" + address, DialWithTLSDialer(config, ...))`.
    pub fn dial_tls(
        address: &str,
        config: Arc<rustls::ClientConfig>,
        server_name: &str,
        timeout: Duration,
    ) -> Result<Self, Error> {
        let tcp = dial(address, timeout)?;
        tcp.set_read_timeout(Some(timeout))
            .map_err(|error| Error::network(&error))?;
        tcp.set_write_timeout(Some(timeout))
            .map_err(|error| Error::network(&error))?;
        let stream =
            tls_handshake(tcp, config, server_name).map_err(|error| Error::network(&error))?;
        Ok(Self {
            stream: Stream::Tls(Box::new(stream)),
            message_id: 0,
        })
    }

    /// Go `Conn.SetTimeout`: every request's wait.
    pub fn set_timeout(&mut self, timeout: Duration) {
        let tcp = self.stream.tcp();
        let _ = tcp.set_read_timeout(Some(timeout));
        let _ = tcp.set_write_timeout(Some(timeout));
    }

    fn next_message_id(&mut self) -> i64 {
        self.message_id += 1;
        self.message_id
    }

    fn send(&mut self, id: i64, op: Vec<u8>) -> Result<(), Error> {
        let message =
            ber::constructed(ber::TAG_SEQUENCE, &[ber::integer(ber::TAG_INTEGER, id), op]);
        self.stream
            .write_all(&message)
            .map_err(|error| Error::network(&error))?;
        self.stream.flush().map_err(|error| Error::network(&error))
    }

    /// Reads one `LDAPMessage` of request `id`: its protocol operation.
    fn receive(&mut self, id: i64) -> Result<ber::Packet, Error> {
        loop {
            let mut header = [0u8; 2];
            self.stream
                .read_exact(&mut header)
                .map_err(|error| Error::network(&error))?;
            let mut raw = header.to_vec();
            let len = if header[1] & 0x80 == 0 {
                usize::from(header[1])
            } else {
                let count = usize::from(header[1] & 0x7f);
                if count == 0 || count > 4 {
                    return Err(Error::new(
                        ERROR_UNEXPECTED_RESPONSE,
                        "ldap: malformed BER packet",
                    ));
                }
                let mut bytes = vec![0u8; count];
                self.stream
                    .read_exact(&mut bytes)
                    .map_err(|error| Error::network(&error))?;
                raw.extend_from_slice(&bytes);
                bytes
                    .iter()
                    .fold(0usize, |len, byte| (len << 8) | usize::from(*byte))
            };
            let mut content = vec![0u8; len];
            self.stream
                .read_exact(&mut content)
                .map_err(|error| Error::network(&error))?;
            if header[0] != ber::TAG_SEQUENCE {
                return Err(Error::new(
                    ERROR_UNEXPECTED_RESPONSE,
                    "ldap: unexpected message",
                ));
            }
            let mut children = ber::children(&content)?.into_iter();
            let message_id = children
                .next()
                .map(|packet| ber::integer_value(&packet))
                .ok_or_else(|| {
                    Error::new(ERROR_UNEXPECTED_RESPONSE, "ldap: message without an id")
                })?;
            let op = children.next().ok_or_else(|| {
                Error::new(
                    ERROR_UNEXPECTED_RESPONSE,
                    "ldap: message without an operation",
                )
            })?;
            if message_id == id {
                return Ok(op);
            }
            // An unsolicited notification (message id 0) ends the session.
            if message_id == 0 {
                return Err(Error::new(
                    ERROR_NETWORK,
                    "ldap: connection closed by the server",
                ));
            }
        }
    }

    /// Go `GetLDAPError`: an `LDAPResult`'s code and diagnostic message as
    /// an error, `None` on success; and the rest of the result's elements.
    fn ldap_result(op: &ber::Packet) -> Result<(u16, String, Vec<ber::Packet>), Error> {
        let mut children = ber::children(&op.content)?.into_iter();
        let code = children
            .next()
            .map(|packet| ber::integer_value(&packet) as u16)
            .ok_or_else(|| Error::new(ERROR_UNEXPECTED_RESPONSE, "ldap: result without a code"))?;
        let _matched_dn = children.next();
        let message = children
            .next()
            .map(|packet| String::from_utf8_lossy(&packet.content).into_owned())
            .unwrap_or_default();
        Ok((code, message, children.collect()))
    }

    /// Go `Conn.StartTLS(config)`: the StartTLS extended operation, then the
    /// TLS handshake over the same connection.
    pub fn start_tls(
        self,
        config: Arc<rustls::ClientConfig>,
        server_name: &str,
    ) -> Result<Self, Error> {
        let mut this = self;
        let Stream::Plain(_) = this.stream else {
            return Err(Error::new(ERROR_NETWORK, "ldap: already encrypted"));
        };
        let id = this.next_message_id();
        let request = ber::constructed(
            ber::CLASS_APPLICATION | ber::CONSTRUCTED | APPLICATION_EXTENDED_REQUEST,
            &[ber::string(ber::CLASS_CONTEXT, b"1.3.6.1.4.1.1466.20037")],
        );
        this.send(id, request)?;
        let op = this.receive(id)?;
        if op.tag != ber::CLASS_APPLICATION | ber::CONSTRUCTED | APPLICATION_EXTENDED_RESPONSE {
            return Err(Error::new(
                ERROR_UNEXPECTED_RESPONSE,
                "ldap: unexpected StartTLS response",
            ));
        }
        let (code, message, _) = Self::ldap_result(&op)?;
        if code != LDAP_RESULT_SUCCESS {
            return Err(Error::new(code, message));
        }
        let Stream::Plain(tcp) = this.stream else {
            unreachable!("checked above")
        };
        let stream = tls_handshake(tcp, config, server_name).map_err(|error| {
            Error::new(ERROR_NETWORK, format!("TLS handshake failed ({error})"))
        })?;
        Ok(Self {
            stream: Stream::Tls(Box::new(stream)),
            message_id: this.message_id,
        })
    }

    /// Go `Conn.SimpleBind(&SimpleBindRequest{Username, Password})`, whose
    /// `AllowEmptyPassword` is false: an empty password is refused before
    /// any request.
    pub fn simple_bind(&mut self, username: &str, password: &str) -> Result<(), Error> {
        if password.is_empty() {
            return Err(Error::new(
                ERROR_EMPTY_PASSWORD,
                "ldap: empty password not allowed by the client",
            ));
        }
        let id = self.next_message_id();
        let request = ber::constructed(
            ber::CLASS_APPLICATION | ber::CONSTRUCTED | APPLICATION_BIND_REQUEST,
            &[
                ber::integer(ber::TAG_INTEGER, 3),
                ber::string(ber::TAG_OCTET_STRING, username.as_bytes()),
                ber::string(ber::CLASS_CONTEXT, password.as_bytes()),
            ],
        );
        self.send(id, request)?;
        let op = self.receive(id)?;
        let (code, message, _) = Self::ldap_result(&op)?;
        if code != LDAP_RESULT_SUCCESS {
            return Err(Error::new(code, message));
        }
        Ok(())
    }

    /// Go `Conn.Bind(username, password)`.
    pub fn bind(&mut self, username: &str, password: &str) -> Result<(), Error> {
        self.simple_bind(username, password)
    }

    /// Go `Conn.Search(&SearchRequest{BaseDN, Scope, Filter})`: the DNs of
    /// the entries found (`NeverDerefAliases`, no size or time limit, every
    /// attribute).
    pub fn search(
        &mut self,
        base_dn: &str,
        scope: i64,
        filter: &str,
    ) -> Result<Vec<String>, Error> {
        let filter = compile_filter(filter)?;
        let id = self.next_message_id();
        let request = ber::constructed(
            ber::CLASS_APPLICATION | ber::CONSTRUCTED | APPLICATION_SEARCH_REQUEST,
            &[
                ber::string(ber::TAG_OCTET_STRING, base_dn.as_bytes()),
                ber::integer(ber::TAG_ENUMERATED, scope),
                ber::integer(ber::TAG_ENUMERATED, 0),
                ber::integer(ber::TAG_INTEGER, 0),
                ber::integer(ber::TAG_INTEGER, 0),
                ber::encode(ber::TAG_BOOLEAN, &[0]),
                filter,
                ber::constructed(ber::TAG_SEQUENCE, &[]),
            ],
        );
        self.send(id, request)?;
        let mut entries = Vec::new();
        loop {
            let op = self.receive(id)?;
            match op.tag & 0x1f {
                APPLICATION_SEARCH_RESULT_ENTRY => {
                    let dn = ber::children(&op.content)?
                        .into_iter()
                        .next()
                        .map(|packet| String::from_utf8_lossy(&packet.content).into_owned())
                        .unwrap_or_default();
                    entries.push(dn);
                }
                APPLICATION_SEARCH_RESULT_REFERENCE => {}
                APPLICATION_SEARCH_RESULT_DONE => {
                    let (code, message, _) = Self::ldap_result(&op)?;
                    if code != LDAP_RESULT_SUCCESS {
                        return Err(Error::new(code, message));
                    }
                    return Ok(entries);
                }
                _ => {
                    return Err(Error::new(
                        ERROR_UNEXPECTED_RESPONSE,
                        "ldap: unexpected search response",
                    ));
                }
            }
        }
    }

    /// The `YangKeao/ldap` fork's `Conn.ServerBindStep(clientCred, dn,
    /// methodName, nil)`: one SASL bind round, answering the result code
    /// and the server's credentials. A code other than success or
    /// `SASLBindInProgress` is an error.
    pub fn server_bind_step(
        &mut self,
        client_cred: &[u8],
        dn: &str,
        method_name: &str,
    ) -> Result<(u16, Vec<u8>), Error> {
        let id = self.next_message_id();
        let request = ber::constructed(
            ber::CLASS_APPLICATION | ber::CONSTRUCTED | APPLICATION_BIND_REQUEST,
            &[
                ber::integer(ber::TAG_INTEGER, 3),
                ber::string(ber::TAG_OCTET_STRING, dn.as_bytes()),
                ber::constructed(
                    ber::CLASS_CONTEXT | ber::CONSTRUCTED | 3,
                    &[
                        ber::string(ber::TAG_OCTET_STRING, method_name.as_bytes()),
                        ber::string(ber::TAG_OCTET_STRING, client_cred),
                    ],
                ),
            ],
        );
        self.send(id, request)?;
        let op = self.receive(id)?;
        if op.tag != ber::CLASS_APPLICATION | ber::CONSTRUCTED | APPLICATION_BIND_RESPONSE {
            return Err(Error::new(
                ERROR_UNEXPECTED_RESPONSE,
                "ldap: unexpected bind response",
            ));
        }
        let (code, message, rest) = Self::ldap_result(&op)?;
        if code != LDAP_RESULT_SUCCESS && code != LDAP_RESULT_SASL_BIND_IN_PROGRESS {
            return Err(Error::new(code, message));
        }
        // `serverSaslCreds [7] OCTET STRING OPTIONAL`.
        let server_cred = rest
            .into_iter()
            .find(|packet| packet.tag == ber::CLASS_CONTEXT | 7)
            .map(|packet| packet.content)
            .unwrap_or_default();
        Ok((code, server_cred))
    }
}

/// Go `CompileFilter` for the one-item filters the `ldap` package builds,
/// `(attr=value)`: `*` alone is a presence filter, other `*`s make a
/// substring filter, `\XX` escapes are decoded, and anything after the
/// closing parenthesis is an error.
fn compile_filter(filter: &str) -> Result<Vec<u8>, Error> {
    let error = |message: &str| Error::new(ERROR_FILTER_COMPILE, format!("ldap: {message}"));
    let inner = filter
        .strip_prefix('(')
        .ok_or_else(|| error("filter does not start with an '('"))?;
    let close = inner
        .find(')')
        .ok_or_else(|| error("unexpected end of filter"))?;
    if close + 1 != inner.len() {
        return Err(error("finished compiling filter with extra at end"));
    }
    let item = &inner[..close];
    let (attr, value) = item
        .split_once('=')
        .ok_or_else(|| error("error parsing filter"))?;
    if attr.is_empty()
        || attr.contains(['(', ')', '&', '|', '!', '<', '>', '~', ':', '*'])
        || value.contains('(')
    {
        return Err(error("error parsing filter"));
    }
    let decode = |raw: &str| -> Result<Vec<u8>, Error> {
        let bytes = raw.as_bytes();
        let mut out = Vec::with_capacity(bytes.len());
        let mut i = 0;
        while i < bytes.len() {
            if bytes[i] == b'\\' {
                let hex = raw
                    .get(i + 1..i + 3)
                    .and_then(|hex| u8::from_str_radix(hex, 16).ok())
                    .ok_or_else(|| error("invalid characters for escape in filter"))?;
                out.push(hex);
                i += 3;
            } else {
                out.push(bytes[i]);
                i += 1;
            }
        }
        Ok(out)
    };
    let attr_bytes = ber::string(ber::TAG_OCTET_STRING, attr.as_bytes());
    if value == "*" {
        return Ok(ber::string(ber::CLASS_CONTEXT | 7, attr.as_bytes()));
    }
    if value.contains('*') {
        let parts: Vec<&str> = value.split('*').collect();
        let mut substrings = Vec::new();
        for (index, part) in parts.iter().enumerate() {
            if part.is_empty() {
                continue;
            }
            let tag = if index == 0 {
                0
            } else if index == parts.len() - 1 {
                2
            } else {
                1
            };
            substrings.push(ber::string(ber::CLASS_CONTEXT | tag, &decode(part)?));
        }
        return Ok(ber::constructed(
            ber::CLASS_CONTEXT | ber::CONSTRUCTED | 4,
            &[attr_bytes, ber::constructed(ber::TAG_SEQUENCE, &substrings)],
        ));
    }
    Ok(ber::constructed(
        ber::CLASS_CONTEXT | ber::CONSTRUCTED | 3,
        &[
            attr_bytes,
            ber::string(ber::TAG_OCTET_STRING, &decode(value)?),
        ],
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ber_integers_are_minimal_twos_complement() {
        assert_eq!(ber::integer(ber::TAG_INTEGER, 0), [2, 1, 0]);
        assert_eq!(ber::integer(ber::TAG_INTEGER, 3), [2, 1, 3]);
        assert_eq!(ber::integer(ber::TAG_INTEGER, 128), [2, 2, 0, 128]);
        assert_eq!(ber::integer(ber::TAG_INTEGER, -1), [2, 1, 0xff]);
        assert_eq!(ber::integer(ber::TAG_INTEGER, 256), [2, 2, 1, 0]);
    }

    #[test]
    fn long_lengths_use_the_long_form() {
        let encoded = ber::encode(ber::TAG_OCTET_STRING, &[7; 300]);
        assert_eq!(&encoded[..4], &[4, 0x82, 1, 44]);
        let mut pos = 0;
        let packet = ber::parse_one(&encoded, &mut pos).unwrap();
        assert_eq!(packet.content.len(), 300);
        assert_eq!(pos, encoded.len());
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    fn reply(id: i64, app_tag: u8, code: i64, extra: &[Vec<u8>]) -> Vec<u8> {
        let mut op = vec![
            ber::integer(ber::TAG_ENUMERATED, code),
            ber::string(ber::TAG_OCTET_STRING, b""),
            ber::string(ber::TAG_OCTET_STRING, b""),
        ];
        op.extend_from_slice(extra);
        ber::constructed(
            ber::TAG_SEQUENCE,
            &[
                ber::integer(ber::TAG_INTEGER, id),
                ber::constructed(ber::CLASS_APPLICATION | ber::CONSTRUCTED | app_tag, &op),
            ],
        )
    }

    /// The requests go-ldap (the `YangKeao/ldap` fork) sends for `Bind`,
    /// `Search(ScopeWholeSubtree, "(uid=yka)")` and `ServerBindStep`, byte
    /// for byte (captured with a recording server, `zzprobe/ldapbytes`).
    #[test]
    fn requests_are_go_ldap_bytes() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let server = std::thread::spawn(move || {
            let (mut tcp, _) = listener.accept().unwrap();
            let mut requests = Vec::new();
            for i in 1..=3 {
                let mut header = [0u8; 2];
                tcp.read_exact(&mut header).unwrap();
                let mut body = vec![0u8; usize::from(header[1])];
                tcp.read_exact(&mut body).unwrap();
                requests.push(hex(&[&header[..], &body].concat()));
                let response = match i {
                    1 => reply(1, APPLICATION_BIND_RESPONSE, 0, &[]),
                    2 => {
                        let entry = ber::constructed(
                            ber::TAG_SEQUENCE,
                            &[
                                ber::integer(ber::TAG_INTEGER, 2),
                                ber::constructed(
                                    ber::CLASS_APPLICATION
                                        | ber::CONSTRUCTED
                                        | APPLICATION_SEARCH_RESULT_ENTRY,
                                    &[
                                        ber::string(
                                            ber::TAG_OCTET_STRING,
                                            b"uid=yka,dc=ping,dc=cap",
                                        ),
                                        ber::constructed(ber::TAG_SEQUENCE, &[]),
                                    ],
                                ),
                            ],
                        );
                        [entry, reply(2, APPLICATION_SEARCH_RESULT_DONE, 0, &[])].concat()
                    }
                    _ => reply(
                        3,
                        APPLICATION_BIND_RESPONSE,
                        14,
                        &[ber::string(ber::CLASS_CONTEXT | 7, b"server-first")],
                    ),
                };
                tcp.write_all(&response).unwrap();
            }
            requests
        });
        let mut l = Conn::dial_plain(&address, Duration::from_secs(5)).unwrap();
        l.set_timeout(Duration::from_secs(5));
        l.bind("cn=root,dc=ping,dc=cap", "rootpw").unwrap();
        let entries = l
            .search("dc=ping,dc=cap", SCOPE_WHOLE_SUBTREE, "(uid=yka)")
            .unwrap();
        assert_eq!(entries, ["uid=yka,dc=ping,dc=cap"]);
        let (code, cred) = l
            .server_bind_step(b"client-first", "uid=yka,dc=ping,dc=cap", "SCRAM-SHA-1")
            .unwrap();
        assert_eq!((code, cred.as_slice()), (14, &b"server-first"[..]));
        assert_eq!(
            server.join().unwrap(),
            [
                "302802010160230201030416636e3d726f6f742c64633d70696e672c64633d6361708006726f6f747077",
                "3032020102632d040e64633d70696e672c64633d6361700a01020a0100020100020100010100a30a04037569640403796b613000",
                "303d020103603802010304167569643d796b612c64633d70696e672c64633d636170a31b040b534352414d2d5348412d31040c636c69656e742d6669727374",
            ]
        );
    }

    #[test]
    fn filters_compile_as_go_ldap() {
        assert_eq!(
            compile_filter("(uid=yka)").unwrap(),
            [0xa3, 10, 4, 3, b'u', b'i', b'd', 4, 3, b'y', b'k', b'a']
        );
        assert_eq!(
            compile_filter("(uid=*)").unwrap(),
            [0x87, 3, b'u', b'i', b'd']
        );
        assert_eq!(
            compile_filter("(cn=a\\2ab)").unwrap(),
            [0xa3, 9, 4, 2, b'c', b'n', 4, 3, b'a', b'*', b'b']
        );
        assert_eq!(
            compile_filter("(cn=a*b)").unwrap(),
            [
                0xa4, 12, 4, 2, b'c', b'n', 0x30, 6, 0x80, 1, b'a', 0x82, 1, b'b'
            ]
        );
        // An injected second filter is extra at the end.
        assert_eq!(
            compile_filter("(uid=a)(uid=*)").unwrap_err().result_code,
            ERROR_FILTER_COMPILE
        );
    }
}
