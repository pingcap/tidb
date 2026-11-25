//! Go `net/url` (Go 1.27, the toolchain Go TiDB master's `go.mod` pins): URL parsing,
//! escaping and query values, for code ported from Go that reads storage
//! URLs (`pkg/objstore`, BR destinations, IMPORT INTO sources).
//!
//! Go strings are byte strings, so every component is `Vec<u8>`: a
//! percent-escape can decode to bytes that are not UTF-8.
//! `ResolveReference`, `JoinPath` and the binary marshalers are not ported.

mod escape;
mod netip;
mod parse;
mod values;

pub use escape::{path_escape, path_unescape, query_escape, query_escape_string, query_unescape};
pub use parse::{parse, parse_request_uri};
pub use values::{parse_query, Values};

use std::borrow::Cow;

use escape::{append_escaped, escape_cow, unescape, unescape_matches, Encoding};
use parse::{valid_encoded, valid_optional_port};
use tidb_go_strconv::quote_bytes;

/// The errors Go `net/url` returns.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    /// Go `*url.Error`: the operation, the URL and the cause.
    Url {
        /// Go `Op`, such as `parse`.
        op: &'static str,
        /// Go `URL`, the text that failed.
        url: Vec<u8>,
        /// Go `Err`.
        err: Box<Error>,
    },
    /// Go `url.EscapeError`.
    Escape(Vec<u8>),
    /// Go `url.InvalidHostError`.
    InvalidHost(Vec<u8>),
    /// A Go `errors.New` / `fmt.Errorf` cause.
    Other(String),
}

impl Error {
    fn url(op: &'static str, url: &[u8], err: Error) -> Self {
        Error::Url {
            op,
            url: url.to_vec(),
            err: Box::new(err),
        }
    }

    /// The error with the URL text of a [`Error::Url`] replaced by
    /// `redact(url)`, for callers that must not echo credentials.
    pub fn redact_url(self, redact: impl FnOnce(&str) -> String) -> Self {
        match self {
            Error::Url { op, url, err } => Error::Url {
                op,
                url: redact(&String::from_utf8_lossy(&url)).into_bytes(),
                err,
            },
            other => other,
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Url { op, url, err } => write!(f, "{op} {}: {err}", quote_bytes(url)),
            Error::Escape(s) => write!(f, "invalid URL escape {}", quote_bytes(s)),
            Error::InvalidHost(s) => write!(f, "invalid character {} in host name", quote_bytes(s)),
            Error::Other(s) => f.write_str(s),
        }
    }
}

impl std::error::Error for Error {}

/// Go `url.Userinfo`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Userinfo {
    username: Vec<u8>,
    password: Vec<u8>,
    password_set: bool,
}

impl Userinfo {
    /// Go `url.User`.
    pub fn user(username: impl Into<Vec<u8>>) -> Self {
        Self {
            username: username.into(),
            password: Vec::new(),
            password_set: false,
        }
    }

    /// Go `url.UserPassword`.
    pub fn user_password(username: impl Into<Vec<u8>>, password: impl Into<Vec<u8>>) -> Self {
        Self {
            username: username.into(),
            password: password.into(),
            password_set: true,
        }
    }

    /// Go `Userinfo.Username`.
    pub fn username(&self) -> &[u8] {
        &self.username
    }

    /// Go `Userinfo.Password`: the password and whether it is set.
    pub fn password(&self) -> (&[u8], bool) {
        (&self.password, self.password_set)
    }

    /// Go `Userinfo.String`.
    pub fn string(&self) -> Vec<u8> {
        let mut s = Vec::with_capacity(
            self.username.len() + self.password.len() + usize::from(self.password_set),
        );
        self.append_to(
            &mut s,
            self.password_set.then_some(self.password.as_slice()),
        );
        s
    }

    fn append_to(&self, s: &mut Vec<u8>, password: Option<&[u8]>) {
        append_escaped(s, &self.username, Encoding::UserPassword);
        if let Some(password) = password {
            s.push(b':');
            append_escaped(s, password, Encoding::UserPassword);
        }
    }
}

/// Go `url.URL`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct URL {
    /// Lowercased scheme, without `:`.
    pub scheme: Vec<u8>,
    /// Encoded opaque data.
    pub opaque: Vec<u8>,
    /// Username and password information.
    pub user: Option<Userinfo>,
    /// `host` or `host:port`.
    pub host: Vec<u8>,
    /// Decoded path.
    pub path: Vec<u8>,
    /// Encoded path hint, see [`URL::escaped_path`].
    pub raw_path: Vec<u8>,
    /// Do not emit an empty host (authority).
    pub omit_host: bool,
    /// Append `?` even when `raw_query` is empty.
    pub force_query: bool,
    /// Encoded query values, without `?`.
    pub raw_query: Vec<u8>,
    /// Decoded fragment, without `#`.
    pub fragment: Vec<u8>,
    /// Encoded fragment hint, see [`URL::escaped_fragment`].
    pub raw_fragment: Vec<u8>,
}

impl URL {
    /// Go `URL.setPath`: `path` decoded, `raw_path` kept only when the
    /// default encoding of `path` differs from `p`.
    pub(crate) fn set_path(&mut self, p: &[u8]) -> Result<(), Error> {
        let path = unescape(p, Encoding::Path)?;
        self.raw_path = if escape_cow(&path, Encoding::Path).as_ref() == p {
            Vec::new()
        } else {
            p.to_vec()
        };
        self.path = path;
        Ok(())
    }

    /// Go `URL.setFragment`.
    pub(crate) fn set_fragment(&mut self, f: &[u8]) -> Result<(), Error> {
        let frag = unescape(f, Encoding::Fragment)?;
        self.raw_fragment = if escape_cow(&frag, Encoding::Fragment).as_ref() == f {
            Vec::new()
        } else {
            f.to_vec()
        };
        self.fragment = frag;
        Ok(())
    }

    /// Go `URL.EscapedPath`.
    pub fn escaped_path(&self) -> Vec<u8> {
        self.escaped_path_cow().into_owned()
    }

    /// Go `URL.EscapedPath` with its pass-through paths borrowing the URL.
    pub fn escaped_path_cow(&self) -> Cow<'_, [u8]> {
        if !self.raw_path.is_empty()
            && valid_encoded(&self.raw_path, Encoding::Path)
            && unescape_matches(&self.raw_path, &self.path)
        {
            return Cow::Borrowed(&self.raw_path);
        }
        if self.path == b"*" {
            return Cow::Borrowed(&self.path);
        }
        escape_cow(&self.path, Encoding::Path)
    }

    /// Go `URL.EscapedFragment`.
    pub fn escaped_fragment(&self) -> Vec<u8> {
        self.escaped_fragment_cow().into_owned()
    }

    fn escaped_fragment_cow(&self) -> Cow<'_, [u8]> {
        if !self.raw_fragment.is_empty()
            && valid_encoded(&self.raw_fragment, Encoding::Fragment)
            && unescape_matches(&self.raw_fragment, &self.fragment)
        {
            return Cow::Borrowed(&self.raw_fragment);
        }
        escape_cow(&self.fragment, Encoding::Fragment)
    }

    /// Go `URL.String`, as bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        self.render_bytes::<false>()
    }

    fn render_bytes<const REDACT_PASSWORD: bool>(&self) -> Vec<u8> {
        let escaped_path: Cow<'_, [u8]> = if self.opaque.is_empty() {
            self.escaped_path_cow()
        } else {
            Cow::Borrowed(&[])
        };
        let fragment: Cow<'_, [u8]> = if self.fragment.is_empty() {
            Cow::Borrowed(&[])
        } else {
            self.escaped_fragment_cow()
        };
        let user = self.user.as_ref().map(|user| {
            let password = if REDACT_PASSWORD {
                b"xxxxx".as_slice()
            } else {
                user.password.as_slice()
            };
            (user, user.password_set.then_some(password))
        });
        let user_len = user.map_or(0, |(user, password)| {
            user.username.len() + password.map_or(0, <[u8]>::len)
        });
        let mut buf = Vec::with_capacity(
            self.scheme.len()
                + self.opaque.len()
                + self.host.len()
                + user_len
                + escaped_path.len()
                + self.raw_query.len()
                + fragment.len()
                + 12,
        );
        if !self.scheme.is_empty() {
            buf.extend_from_slice(&self.scheme);
            buf.push(b':');
        }
        if !self.opaque.is_empty() {
            buf.extend_from_slice(&self.opaque);
        } else {
            if (!self.scheme.is_empty() || !self.host.is_empty() || self.user.is_some())
                && !(self.omit_host && self.host.is_empty() && self.user.is_none())
            {
                if !self.host.is_empty() || !self.path.is_empty() || self.user.is_some() {
                    buf.extend_from_slice(b"//");
                }
                if let Some((user, password)) = user {
                    user.append_to(&mut buf, password);
                    buf.push(b'@');
                }
                append_escaped(&mut buf, &self.host, Encoding::Host);
            }
            let mut path = escaped_path.as_ref();
            if self.omit_host
                && self.host.is_empty()
                && self.user.is_none()
                && path.starts_with(b"//")
            {
                // Re-parsing must not turn such a path into an authority.
                buf.extend_from_slice(b"%2F");
                path = &path[1..];
            }
            if path.first().is_some_and(|&c| c != b'/') && !self.host.is_empty() {
                buf.push(b'/');
            }
            if buf.is_empty()
                && path
                    .split(|&c| c == b'/')
                    .next()
                    .is_some_and(|s| s.contains(&b':'))
            {
                buf.extend_from_slice(b"./");
            }
            buf.extend_from_slice(path);
        }
        if self.force_query || !self.raw_query.is_empty() {
            buf.push(b'?');
            buf.extend_from_slice(&self.raw_query);
        }
        if !self.fragment.is_empty() {
            buf.push(b'#');
            buf.extend_from_slice(&fragment);
        }
        buf
    }

    /// Go `URL.String`. Exact for a URL parsed from text: escaped components
    /// are ASCII and the raw ones are slices of the input. Other bytes are
    /// replaced by U+FFFD; [`URL::to_bytes`] keeps them.
    pub fn string(&self) -> String {
        rendered_text(self.to_bytes())
    }

    /// Go `URL.Redacted`: [`URL::string`] with any password as `xxxxx`.
    pub fn redacted(&self) -> String {
        rendered_text(self.render_bytes::<true>())
    }

    /// Go `URL.IsAbs`.
    pub fn is_abs(&self) -> bool {
        !self.scheme.is_empty()
    }

    /// Go `URL.Query`: the parsed `raw_query`, malformed pairs dropped.
    pub fn query(&self) -> Values {
        parse_query(&self.raw_query).0
    }

    /// Go `URL.RequestURI`.
    pub fn request_uri(&self) -> Vec<u8> {
        let mut result = self.opaque.clone();
        if result.is_empty() {
            result = self.escaped_path();
            if result.is_empty() {
                result = b"/".to_vec();
            }
        } else if result.starts_with(b"//") {
            result = [self.scheme.as_slice(), b":", &result].concat();
        }
        if self.force_query || !self.raw_query.is_empty() {
            result.push(b'?');
            result.extend_from_slice(&self.raw_query);
        }
        result
    }

    /// Go `URL.Hostname`.
    pub fn hostname(&self) -> &[u8] {
        split_host_port(&self.host).0
    }

    /// Go `URL.Port`.
    pub fn port(&self) -> &[u8] {
        split_host_port(&self.host).1
    }
}

fn rendered_text(bytes: Vec<u8>) -> String {
    String::from_utf8(bytes)
        .unwrap_or_else(|error| String::from_utf8_lossy(error.as_bytes()).into_owned())
}

/// Go `splitHostPort`: a numeric port is split off; brackets are removed.
fn split_host_port(host_port: &[u8]) -> (&[u8], &[u8]) {
    let mut host = host_port;
    let mut port: &[u8] = b"";
    if let Some(colon) = host
        .iter()
        .rposition(|&c| c == b':')
        .filter(|&colon| valid_optional_port(&host[colon..]))
    {
        (host, port) = (&host[..colon], &host[colon + 1..]);
    }
    if host.starts_with(b"[") && host.ends_with(b"]") && host.len() >= 2 {
        host = &host[1..host.len() - 1];
    }
    (host, port)
}

#[cfg(test)]
mod tests;
