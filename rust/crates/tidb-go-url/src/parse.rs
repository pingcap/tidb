//! Go `net/url`'s parser: `Parse`, `ParseRequestURI`, `parse`,
//! `parseAuthority`, `parseHost` and their helpers.

use crate::escape::{should_escape, unescape, Encoding};
use crate::{Error, Userinfo, URL};

fn msg(text: &str) -> Error {
    Error::Other(text.to_owned())
}

fn contains(haystack: &[u8], needle: u8) -> bool {
    haystack.contains(&needle)
}

/// Go `strings.Cut`.
fn cut(s: &[u8], sep: u8) -> (&[u8], &[u8], bool) {
    match s.iter().position(|&c| c == sep) {
        Some(i) => (&s[..i], &s[i + 1..], true),
        None => (s, b"", false),
    }
}

/// Go `getScheme`.
fn get_scheme(raw: &[u8]) -> Result<(&[u8], &[u8]), Error> {
    for (i, &c) in raw.iter().enumerate() {
        match c {
            b'a'..=b'z' | b'A'..=b'Z' => {}
            b'0'..=b'9' | b'+' | b'-' | b'.' => {
                if i == 0 {
                    return Ok((b"", raw));
                }
            }
            b':' => {
                if i == 0 {
                    return Err(msg("missing protocol scheme"));
                }
                return Ok((&raw[..i], &raw[i + 1..]));
            }
            _ => return Ok((b"", raw)),
        }
    }
    Ok((b"", raw))
}

/// Go `url.Parse`.
pub fn parse(raw_url: impl AsRef<[u8]>) -> Result<URL, Error> {
    let raw_url = raw_url.as_ref();
    let (u, frag, _) = cut(raw_url, b'#');
    let mut url = parse_inner(u, false).map_err(|err| Error::url("parse", u, err))?;
    if frag.is_empty() {
        return Ok(url);
    }
    url.set_fragment(frag)
        .map_err(|err| Error::url("parse", raw_url, err))?;
    Ok(url)
}

/// Go `url.ParseRequestURI`.
pub fn parse_request_uri(raw_url: impl AsRef<[u8]>) -> Result<URL, Error> {
    let raw_url = raw_url.as_ref();
    parse_inner(raw_url, true).map_err(|err| Error::url("parse", raw_url, err))
}

/// Go `parse`.
fn parse_inner(raw_url: &[u8], via_request: bool) -> Result<URL, Error> {
    if raw_url.iter().any(|&b| b < b' ' || b == 0x7f) {
        return Err(msg("net/url: invalid control character in URL"));
    }
    if raw_url.is_empty() && via_request {
        return Err(msg("empty url"));
    }
    let mut url = URL::default();
    if raw_url == b"*" {
        url.path = b"*".to_vec();
        return Ok(url);
    }
    let (scheme, mut rest) = get_scheme(raw_url)?;
    url.scheme = scheme.to_ascii_lowercase();
    if rest.last() == Some(&b'?') && rest.iter().filter(|&&c| c == b'?').count() == 1 {
        url.force_query = true;
        rest = &rest[..rest.len() - 1];
    } else {
        let (path, query, _) = cut(rest, b'?');
        rest = path;
        url.raw_query = query.to_vec();
    }
    if !rest.starts_with(b"/") {
        if !url.scheme.is_empty() {
            url.opaque = rest.to_vec();
            return Ok(url);
        }
        if via_request {
            return Err(msg("invalid URI for request"));
        }
        if contains(cut(rest, b'/').0, b':') {
            return Err(msg("first path segment in URL cannot contain colon"));
        }
    }
    if (!url.scheme.is_empty() || !via_request && !rest.starts_with(b"///"))
        && rest.starts_with(b"//")
    {
        let mut authority = &rest[2..];
        rest = b"";
        if let Some(i) = authority.iter().position(|&c| c == b'/') {
            (authority, rest) = (&authority[..i], &authority[i..]);
        }
        (url.user, url.host) = parse_authority(authority)?;
    } else if !url.scheme.is_empty() && rest.starts_with(b"/") {
        url.omit_host = true;
    }
    url.set_path(rest)?;
    Ok(url)
}

/// Go `parseAuthority`.
fn parse_authority(authority: &[u8]) -> Result<(Option<Userinfo>, Vec<u8>), Error> {
    let at = authority.iter().rposition(|&c| c == b'@');
    let host = parse_host(at.map_or(authority, |i| &authority[i + 1..]))?;
    let Some(i) = at else {
        return Ok((None, host));
    };
    let userinfo = &authority[..i];
    if !valid_userinfo(userinfo) {
        return Err(msg("net/url: invalid userinfo"));
    }
    let user = if !contains(userinfo, b':') {
        Userinfo::user(unescape(userinfo, Encoding::UserPassword)?)
    } else {
        let (username, password, _) = cut(userinfo, b':');
        Userinfo::user_password(
            unescape(username, Encoding::UserPassword)?,
            unescape(password, Encoding::UserPassword)?,
        )
    };
    Ok((Some(user), host))
}

/// Go `parseHost`: `host[:port]` without user information.
fn parse_host(host: &[u8]) -> Result<Vec<u8>, Error> {
    let open = host.iter().rposition(|&c| c == b'[');
    if open.is_some_and(|i| i > 0) {
        return Err(msg("invalid IP-literal"));
    }
    if open == Some(0) {
        let Some(close) = host.iter().rposition(|&c| c == b']') else {
            return Err(msg("missing ']' in host"));
        };
        let colon_port = &host[close + 1..];
        if !valid_optional_port(colon_port) {
            return Err(invalid_port(colon_port));
        }
        let unescaped_colon_port = unescape(colon_port, Encoding::Host)?;
        let hostname = &host[1..close];
        let unescaped_hostname = match find(hostname, b"%25") {
            Some(zone) => {
                let mut part = unescape(&hostname[..zone], Encoding::Host)?;
                part.extend(unescape(&hostname[zone..], Encoding::Zone)?);
                part
            }
            None => unescape(hostname, Encoding::Host)?,
        };
        match crate::netip::parse_addr_is4(&unescaped_hostname) {
            Err(err) => return Err(Error::Other(format!("invalid host: {err}"))),
            Ok(true) => return Err(msg("invalid IP-literal")),
            Ok(false) => {}
        }
        let mut out = Vec::with_capacity(unescaped_hostname.len() + 2 + colon_port.len());
        out.push(b'[');
        out.extend(unescaped_hostname);
        out.push(b']');
        out.extend(unescaped_colon_port);
        return Ok(out);
    }
    if let Some(i) = host.iter().rposition(|&c| c == b':') {
        let colon_port = &host[i..];
        if !valid_optional_port(colon_port) {
            return Err(invalid_port(colon_port));
        }
    }
    unescape(host, Encoding::Host)
}

/// Go `fmt.Errorf("invalid port %q after host", colonPort)`.
fn invalid_port(colon_port: &[u8]) -> Error {
    Error::Other(format!(
        "invalid port {} after host",
        tidb_go_strconv::quote_bytes(colon_port)
    ))
}

/// Go `strings.Index`.
fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

/// Go `validOptionalPort`: empty, or `:` followed by digits.
pub(crate) fn valid_optional_port(port: &[u8]) -> bool {
    match port.split_first() {
        None => true,
        Some((b':', digits)) => digits.iter().all(u8::is_ascii_digit),
        Some(_) => false,
    }
}

/// Go `validEncoded`: `s` holds no byte that `mode` would escape.
pub(crate) fn valid_encoded(s: &[u8], mode: Encoding) -> bool {
    s.iter().all(|&c| {
        matches!(
            c,
            b'!' | b'$'
                | b'&'
                | b'\''
                | b'('
                | b')'
                | b'*'
                | b'+'
                | b','
                | b';'
                | b'='
                | b':'
                | b'@'
                | b'['
                | b']'
                | b'%'
        ) || !should_escape(c, mode)
    })
}

/// Go `validUserinfo` (RFC 3986 section 3.2.1, plus Go's tolerated `@`).
/// Go ranges over runes, so a non-ASCII byte is rejected like any other.
fn valid_userinfo(s: &[u8]) -> bool {
    s.iter().all(|&c| {
        c.is_ascii_alphanumeric()
            || matches!(
                c,
                b'-' | b'.'
                    | b'_'
                    | b':'
                    | b'~'
                    | b'!'
                    | b'$'
                    | b'&'
                    | b'\''
                    | b'('
                    | b')'
                    | b'*'
                    | b'+'
                    | b','
                    | b';'
                    | b'='
                    | b'%'
                    | b'@'
            )
    })
}
