//! Go `net/url`'s escaping: `shouldEscape`, `unescape`, `escape` and their
//! exported wrappers.

use std::borrow::Cow;

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
    ESCAPE[mode as usize][usize::from(c)]
}

// Generate each row from the Go rules, so escaping and host validation
// classify bytes without repeating the character tests.
const ESCAPE: [[bool; 256]; 7] = {
    use Encoding::*;
    let modes = [
        Path,
        PathSegment,
        Host,
        Zone,
        UserPassword,
        QueryComponent,
        Fragment,
    ];
    let mut table = [[false; 256]; 7];
    let mut i = 0;
    while i < modes.len() {
        let mut c = 0;
        while c < 256 {
            table[modes[i] as usize][c] = escape_rule(c as u8, modes[i]);
            c += 1;
        }
        i += 1;
    }
    table
};

const fn escape_rule(c: u8, mode: Encoding) -> bool {
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
    if matches!(mode, E::Fragment) && matches!(c, b'!' | b'(' | b')' | b'*') {
        return false;
    }
    true
}

/// Go `unescape`.
pub(crate) fn unescape(s: &[u8], mode: Encoding) -> Result<Vec<u8>, Error> {
    if matches!(
        mode,
        Encoding::Path | Encoding::PathSegment | Encoding::Fragment
    ) && !s.contains(&b'%')
    {
        return Ok(s.to_vec());
    }
    unescape_from(s, mode, 0)
}

fn unescape_from(s: &[u8], mode: Encoding, start: usize) -> Result<Vec<u8>, Error> {
    let mut n = 0;
    let mut has_plus = false;
    let mut i = start;
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
    t.extend_from_slice(&s[..start]);
    let mut i = start;
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
    escape_cow(s, mode).into_owned()
}

pub(crate) fn escape_cow(s: &[u8], mode: Encoding) -> Cow<'_, [u8]> {
    let row = &ESCAPE[mode as usize];
    let Some(first) = s.iter().position(|&c| row[usize::from(c)]) else {
        return Cow::Borrowed(s);
    };
    let mut out = Vec::with_capacity(s.len() + 2);
    append_from_escape(&mut out, s, mode, first);
    Cow::Owned(out)
}

/// Append an escaped component when the caller is already building a URL.
pub(crate) fn append_escaped(t: &mut Vec<u8>, s: &[u8], mode: Encoding) {
    let row = &ESCAPE[mode as usize];
    let Some(first) = s.iter().position(|&c| row[usize::from(c)]) else {
        t.extend_from_slice(s);
        return;
    };
    t.reserve(s.len() + 2);
    append_from_escape(t, s, mode, first);
}

fn append_from_escape(t: &mut Vec<u8>, s: &[u8], mode: Encoding, first: usize) {
    let row = &ESCAPE[mode as usize];
    t.extend_from_slice(&s[..first]);
    for &c in &s[first..] {
        if !row[usize::from(c)] {
            t.push(c);
        } else if c == b' ' && mode == Encoding::QueryComponent {
            t.push(b'+');
        } else {
            t.extend_from_slice(&[
                b'%',
                UPPERHEX[usize::from(c >> 4)],
                UPPERHEX[usize::from(c & 15)],
            ]);
        }
    }
}

/// Checks a path or fragment hint against the decoded bytes without allocating.
pub(crate) fn unescape_matches(mut raw: &[u8], mut decoded: &[u8]) -> bool {
    while let Some((&first, tail)) = raw.split_first() {
        let byte = if first == b'%' {
            let Some(([upper, lower], rest)) = tail.split_first_chunk::<2>() else {
                return false;
            };
            if !ishex(*upper) || !ishex(*lower) {
                return false;
            }
            raw = rest;
            (unhex(*upper) << 4) | unhex(*lower)
        } else {
            raw = tail;
            first
        };
        let Some(rest) = decoded.strip_prefix(&[byte]) else {
            return false;
        };
        decoded = rest;
    }
    decoded.is_empty()
}

/// Go `url.QueryUnescape`.
pub fn query_unescape(s: &[u8]) -> Result<Vec<u8>, Error> {
    query_unescape_cow(s).map(Cow::into_owned)
}

/// Borrows an unchanged component and skips its plain prefix when decoding.
pub(crate) fn query_unescape_cow(s: &[u8]) -> Result<Cow<'_, [u8]>, Error> {
    let Some(first) = memchr::memchr2(b'%', b'+', s) else {
        return Ok(Cow::Borrowed(s));
    };
    unescape_from(s, Encoding::QueryComponent, first).map(Cow::Owned)
}

/// Go `url.PathUnescape`.
pub fn path_unescape(s: &[u8]) -> Result<Vec<u8>, Error> {
    unescape(s, Encoding::PathSegment)
}

/// Go `url.QueryEscape`.
pub fn query_escape(s: &[u8]) -> Vec<u8> {
    escape(s, Encoding::QueryComponent)
}

/// Go `url.QueryEscape` as text: the escaped form is ASCII.
#[must_use]
pub fn query_escape_string(s: impl AsRef<[u8]>) -> String {
    String::from_utf8(query_escape(s.as_ref())).expect("an escaped query component is ASCII")
}

/// Go `url.PathEscape`.
pub fn path_escape(s: &[u8]) -> Vec<u8> {
    escape(s, Encoding::PathSegment)
}
