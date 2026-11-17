//! The part of Go `net/netip.ParseAddr` that `url.parseHost` reads: whether
//! a bracketed host is an IPv4 address, and Go's `parseAddrError` text.

use tidb_go_strconv::quote_bytes;

/// Go `netip.parseAddrError.Error`.
fn parse_addr_error(input: &[u8], msg: &str, at: &[u8]) -> String {
    let mut out = format!("ParseAddr({}): {msg}", quote_bytes(input));
    if !at.is_empty() {
        out.push_str(&format!(" (at {})", quote_bytes(at)));
    }
    out
}

/// Go `netip.ParseAddr`, answering `Addr.Is4` of the result.
pub(crate) fn parse_addr_is4(s: &[u8]) -> Result<bool, String> {
    for &c in s {
        match c {
            b'.' => return parse_ipv4_fields(s, 0, s.len()).map(|()| true),
            b':' => return parse_ipv6(s).map(|()| false),
            b'%' => return Err(parse_addr_error(s, "missing IPv6 address", b"")),
            _ => {}
        }
    }
    Err(parse_addr_error(s, "unable to parse IP", b""))
}

/// Go `netip.parseIPv4Fields`.
fn parse_ipv4_fields(input: &[u8], off: usize, end: usize) -> Result<(), String> {
    let s = &input[off..end];
    let (mut val, mut pos, mut dig_len) = (0u32, 0, 0);
    for (i, &c) in s.iter().enumerate() {
        if c.is_ascii_digit() {
            if dig_len == 1 && val == 0 {
                return Err(parse_addr_error(
                    input,
                    "IPv4 field has octet with leading zero",
                    b"",
                ));
            }
            val = val * 10 + u32::from(c - b'0');
            dig_len += 1;
            if val > 255 {
                return Err(parse_addr_error(input, "IPv4 field has value >255", b""));
            }
        } else if c == b'.' {
            if i == 0 || i == s.len() - 1 || s[i - 1] == b'.' {
                return Err(parse_addr_error(
                    input,
                    "IPv4 field must have at least one digit",
                    &s[i..],
                ));
            }
            if pos == 3 {
                return Err(parse_addr_error(input, "IPv4 address too long", b""));
            }
            pos += 1;
            val = 0;
            dig_len = 0;
        } else {
            return Err(parse_addr_error(input, "unexpected character", &s[i..]));
        }
    }
    if pos < 3 {
        return Err(parse_addr_error(input, "IPv4 address too short", b""));
    }
    Ok(())
}

/// Go `netip.parseIPv6`.
fn parse_ipv6(input: &[u8]) -> Result<(), String> {
    let mut s = input;
    let mut zone: &[u8] = b"";
    if let Some(i) = s.iter().position(|&c| c == b'%') {
        (s, zone) = (&s[..i], &s[i + 1..]);
        if zone.is_empty() {
            return Err(parse_addr_error(
                input,
                "zone must be a non-empty string",
                b"",
            ));
        }
    }
    let mut ellipsis: Option<usize> = None;
    if s.len() >= 2 && s[0] == b':' && s[1] == b':' {
        ellipsis = Some(0);
        s = &s[2..];
        if s.is_empty() {
            return Ok(());
        }
    }
    let mut i = 0;
    while i < 16 {
        let mut off = 0;
        let mut acc = 0u32;
        while off < s.len() {
            let Some(digit) = char::from(s[off]).to_digit(16) else {
                break;
            };
            acc = (acc << 4) + digit;
            if off > 3 {
                return Err(parse_addr_error(
                    input,
                    "each group must have 4 or less digits",
                    s,
                ));
            }
            if acc > u32::from(u16::MAX) {
                return Err(parse_addr_error(input, "IPv6 field has value >=2^16", s));
            }
            off += 1;
        }
        if off == 0 {
            return Err(parse_addr_error(
                input,
                "each colon-separated field must have at least one digit",
                s,
            ));
        }
        if off < s.len() && s[off] == b'.' {
            if ellipsis.is_none() && i != 12 {
                return Err(parse_addr_error(
                    input,
                    "embedded IPv4 address must replace the final 2 fields of the address",
                    s,
                ));
            }
            if i + 4 > 16 {
                return Err(parse_addr_error(
                    input,
                    "too many hex fields to fit an embedded IPv4 at the end of the address",
                    s,
                ));
            }
            let mut end = input.len();
            if !zone.is_empty() {
                end -= zone.len() + 1;
            }
            parse_ipv4_fields(input, end - s.len(), end)?;
            s = b"";
            i += 4;
            break;
        }
        i += 2;
        s = &s[off..];
        if s.is_empty() {
            break;
        }
        if s[0] != b':' {
            return Err(parse_addr_error(
                input,
                "unexpected character, want colon",
                s,
            ));
        } else if s.len() == 1 {
            return Err(parse_addr_error(
                input,
                "colon must be followed by more characters",
                s,
            ));
        }
        s = &s[1..];
        if s[0] == b':' {
            if ellipsis.is_some() {
                return Err(parse_addr_error(input, "multiple :: in address", s));
            }
            ellipsis = Some(i);
            s = &s[1..];
            if s.is_empty() {
                break;
            }
        }
    }
    if !s.is_empty() {
        return Err(parse_addr_error(input, "trailing garbage after address", s));
    }
    if i < 16 {
        if ellipsis.is_none() {
            return Err(parse_addr_error(input, "address string too short", b""));
        }
    } else if ellipsis.is_some() {
        return Err(parse_addr_error(
            input,
            "the :: must expand to at least one field of zeros",
            b"",
        ));
    }
    Ok(())
}
