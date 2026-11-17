//! Go `net/url`'s escaping: `shouldEscape`, `unescape`, `escape` and their
//! exported wrappers.

use crate::Error;

/// Go `url.encoding`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Encoding {
    Path,
    PathSegment,
    Host,
    Zone,
    UserPassword,
    QueryComponent,
    Fragment,
}

const UPPERHEX: &[u8; 16] = b"0123456789ABCDEF";

/// Go `ishex`.
pub(crate) fn ishex(c: u8) -> bool {
    c.is_ascii_hexdigit()
}

/// Go `unhex`; the caller has checked [`ishex`].
pub(crate) fn unhex(c: u8) -> u8 {
    match c {
        b'0'..=b'9' => c - b'0',
        b'a'..=b'f' => c - b'a' + 10,
        b'A'..=b'F' => c - b'A' + 10,
        _ => unreachable!("invalid hex character"),
    }
}

/// Go `shouldEscape`: whether `c` is escaped in the URL section `mode`.
pub(crate) fn should_escape(c: u8, mode: Encoding) -> bool {
    use Encoding as E;
    if c.is_ascii_alphanumeric() {
        return false;
    }
    if matches!(mode, E::Host | E::Zone)
        && matches!(
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
                | b'['
                | b']'
                | b'<'
                | b'>'
                | b'"'
        )
    {
        return false;
    }
    match c {
        b'-' | b'_' | b'.' | b'~' => return false,
        b'$' | b'&' | b'+' | b',' | b'/' | b':' | b';' | b'=' | b'?' | b'@' => match mode {
            E::Path => return c == b'?',
            E::PathSegment => return matches!(c, b'/' | b';' | b',' | b'?'),
            E::UserPassword => return matches!(c, b'@' | b'/' | b'?' | b':'),
            E::QueryComponent => return true,
            E::Fragment => return false,
            E::Host | E::Zone => {}
        },
        _ => {}
    }
    if mode == E::Fragment && matches!(c, b'!' | b'(' | b')' | b'*') {
        return false;
    }
    true
}

/// Go `unescape`.
pub(crate) fn unescape(s: &[u8], mode: Encoding) -> Result<Vec<u8>, Error> {
    let mut n = 0;
    let mut has_plus = false;
    let mut i = 0;
    while i < s.len() {
        match s[i] {
            b'%' => {
                n += 1;
                if i + 2 >= s.len() || !ishex(s[i + 1]) || !ishex(s[i + 2]) {
                    let rest = &s[i..];
                    return Err(Error::Escape(rest[..rest.len().min(3)].to_vec()));
                }
                if mode == Encoding::Host && unhex(s[i + 1]) < 8 && &s[i..i + 3] != b"%25" {
                    return Err(Error::Escape(s[i..i + 3].to_vec()));
                }
                if mode == Encoding::Zone {
                    let v = (unhex(s[i + 1]) << 4) | unhex(s[i + 2]);
                    if &s[i..i + 3] != b"%25" && v != b' ' && should_escape(v, Encoding::Host) {
                        return Err(Error::Escape(s[i..i + 3].to_vec()));
                    }
                }
                i += 3;
            }
            b'+' => {
                has_plus = mode == Encoding::QueryComponent;
                i += 1;
            }
            c => {
                if matches!(mode, Encoding::Host | Encoding::Zone)
                    && c < 0x80
                    && should_escape(c, mode)
                {
                    return Err(Error::InvalidHost(vec![c]));
                }
                i += 1;
            }
        }
    }
    if n == 0 && !has_plus {
        return Ok(s.to_vec());
    }
    let mut t = Vec::with_capacity(s.len() - 2 * n);
    let mut i = 0;
    while i < s.len() {
        match s[i] {
            b'%' => {
                t.push((unhex(s[i + 1]) << 4) | unhex(s[i + 2]));
                i += 2;
            }
            b'+' if mode == Encoding::QueryComponent => t.push(b' '),
            c => t.push(c),
        }
        i += 1;
    }
    Ok(t)
}

/// Go `escape`.
pub(crate) fn escape(s: &[u8], mode: Encoding) -> Vec<u8> {
    let mut t = Vec::with_capacity(s.len());
    for &c in s {
        if c == b' ' && mode == Encoding::QueryComponent {
            t.push(b'+');
        } else if should_escape(c, mode) {
            t.extend_from_slice(&[
                b'%',
                UPPERHEX[usize::from(c >> 4)],
                UPPERHEX[usize::from(c & 15)],
            ]);
        } else {
            t.push(c);
        }
    }
    t
}

/// Go `url.QueryUnescape`.
pub fn query_unescape(s: &[u8]) -> Result<Vec<u8>, Error> {
    unescape(s, Encoding::QueryComponent)
}

/// Go `url.PathUnescape`.
pub fn path_unescape(s: &[u8]) -> Result<Vec<u8>, Error> {
    unescape(s, Encoding::PathSegment)
}

/// Go `url.QueryEscape`.
pub fn query_escape(s: &[u8]) -> Vec<u8> {
    escape(s, Encoding::QueryComponent)
}

/// Go `url.PathEscape`.
pub fn path_escape(s: &[u8]) -> Vec<u8> {
    escape(s, Encoding::PathSegment)
}
