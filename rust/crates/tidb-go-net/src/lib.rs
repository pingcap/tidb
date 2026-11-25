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

//! Go `net`'s host:port helpers (`net/ipsock.go`).

/// Joins a host and a preformatted port using Go's `net.JoinHostPort` rules.
#[inline]
#[must_use]
pub fn join_host_port_str(host: &str, port: &str) -> String {
    join_text(host, port, port.len())
}

/// Formats a decimal port and joins it with the host.
#[inline]
#[must_use]
pub fn join_host_port_number(host: &str, port: impl itoa::Integer) -> String {
    join_host_port_str(host, itoa::Buffer::new().format(port))
}

/// Go `net.JoinHostPort`: combines host and port into a network address of
/// the form "host:port". If host contains a colon, as found in literal IPv6
/// addresses, it returns "[host]:port".
#[must_use]
pub fn join_host_port(host: &str, port: impl std::fmt::Display) -> String {
    use std::fmt::Write;

    let mut writer = HostPortWriter {
        host,
        address: String::new(),
    };
    write!(writer, "{port}").expect("port formatter failed");
    if writer.address.is_empty() {
        join_text(host, "", 8)
    } else {
        writer.address
    }
}

struct HostPortWriter<'a> {
    host: &'a str,
    address: String,
}

impl std::fmt::Write for HostPortWriter<'_> {
    #[inline]
    fn write_str(&mut self, port: &str) -> std::fmt::Result {
        // A joined address always has at least its colon separator.
        if self.address.is_empty() {
            // Negative integers write their sign separately from the digits.
            let port_capacity = if port == "-" { 20 } else { port.len().max(8) };
            self.address = join_text(self.host, port, port_capacity);
        } else {
            self.address.push_str(port);
        }
        Ok(())
    }
}

#[inline]
fn join_text(host: &str, port: &str, port_capacity: usize) -> String {
    let bracketed = has_colon(host.as_bytes());
    let mut address = String::with_capacity(host.len() + port_capacity + if bracketed { 3 } else { 1 });
    if bracketed {
        address.push('[');
        address.push_str(host);
        address.push(']');
    } else {
        address.push_str(host);
    }
    address.push(':');
    address.push_str(port);
    address
}

#[inline]
fn has_colon(host_bytes: &[u8]) -> bool {
    if let (Some(head), Some(tail)) =
        (host_bytes.first_chunk::<8>(), host_bytes.last_chunk::<8>())
    {
        if host_bytes.len() <= 16 {
            // Two overlapping words cover the short host. XOR turns colons
            // into zero bytes, which the subtraction detects in either word.
            let has_colon = |half: &[u8; 8]| {
                let word = u64::from_ne_bytes(*half) ^ u64::from_ne_bytes([b':'; 8]);
                word.wrapping_sub(0x0101_0101_0101_0101) & !word & 0x8080_8080_8080_8080 != 0
            };
            has_colon(head) || has_colon(tail)
        } else {
            memchr::memchr(b':', host_bytes).is_some()
        }
    } else {
        host_bytes.contains(&b':')
    }
}

/// Go `net.SplitHostPort`: splits a network address of the form "host:port",
/// "host%zone:port", "[host]:port" or "[host%zone]:port" into host or
/// host%zone and port. `None` where Go answers an `AddrError` (a missing
/// port, too many colons, a misplaced bracket).
#[must_use]
pub fn split_host_port(hostport: &str) -> Option<(&str, &str)> {
    let colon = memchr::memrchr(b':', hostport.as_bytes())?;
    let (host, port) = (&hostport[..colon], &hostport[colon + 1..]);
    if let Some(inner) = host.strip_prefix('[') {
        let inner = inner.strip_suffix(']')?;
        if has_bracket(inner) {
            return None;
        }
        return (!has_bracket(port)).then_some((inner, port));
    }
    // An unbracketed host has no colon and neither part can contain a
    // bracket. For short addresses the two parts fit in one bracket scan.
    if hostport.len() <= 32 {
        return (!has_colon(host.as_bytes()) && !has_bracket(hostport)).then_some((host, port));
    }
    if memchr::memchr3(b':', b'[', b']', host.as_bytes()).is_some() || has_bracket(port) {
        return None;
    }
    Some((host, port))
}

fn has_bracket(s: &str) -> bool {
    memchr::memchr2(b'[', b']', s.as_bytes()).is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn join_host_port_brackets_a_host_with_colons() {
        assert_eq!(join_host_port("127.0.0.1", 4000), "127.0.0.1:4000");
        assert_eq!(join_host_port("::1", "4000"), "[::1]:4000");
        assert_eq!(join_host_port("", 0_u16), ":0");
        assert_eq!(join_host_port("node", "named-service"), "node:named-service");
    }

    #[test]
    fn short_host_scan_finds_a_colon_at_every_byte() {
        for length in 1..=20 {
            let host = "a".repeat(length);
            assert_eq!(join_host_port_str(&host, "svc"), format!("{host}:svc"));
            for position in 0..length {
                let mut host = host.clone();
                host.replace_range(position..position + 1, ":");
                assert_eq!(join_host_port_str(&host, "svc"), format!("[{host}]:svc"));
            }
        }
        assert_eq!(
            join_host_port_number("::1", i64::MIN),
            "[::1]:-9223372036854775808"
        );
        assert_eq!(
            join_host_port_number("", u128::MAX),
            format!(":{}", u128::MAX)
        );
    }

    #[test]
    fn join_host_port_accepts_multiple_or_zero_port_writes() {
        struct Port<'a>(&'a [&'a str]);

        impl std::fmt::Display for Port<'_> {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                for part in self.0 {
                    formatter.write_str(part)?;
                }
                Ok(())
            }
        }

        assert_eq!(
            join_host_port("fe80::1%lo0", Port(&["", "named", "-service"])),
            "[fe80::1%lo0]:named-service"
        );
        assert_eq!(join_host_port("::1", Port(&[])), "[::1]:");
        assert_eq!(join_host_port("", Port(&[])), ":");
        assert_eq!(
            join_host_port("::1", i64::MIN),
            "[::1]:-9223372036854775808"
        );
    }

    // Go `TestSplitHostPort` / `TestSplitHostPortError` rows.
    #[test]
    fn split_host_port_is_gos() {
        assert_eq!(split_host_port("127.0.0.1:80"), Some(("127.0.0.1", "80")));
        assert_eq!(split_host_port("[::1]:80"), Some(("::1", "80")));
        assert_eq!(split_host_port("[::1%lo0]:80"), Some(("::1%lo0", "80")));
        assert_eq!(split_host_port(":80"), Some(("", "80")));
        assert_eq!(split_host_port("golang.org:"), Some(("golang.org", "")));
        for bad in ["golang.org", "127.0.0.1", "[::1]", "[fe80::1%lo0]", "::1", "golang.org:80:443", "[::1", "::1]:80", "[go]lang.org:80"] {
            assert_eq!(split_host_port(bad), None, "{bad}");
        }
    }

    #[test]
    fn split_host_port_checks_brackets_on_both_sides_of_short_addresses() {
        for len in 30..=34 {
            let host = "a".repeat(len - 2);
            assert_eq!(
                split_host_port(&format!("{host}:7")),
                Some((host.as_str(), "7"))
            );
            assert_eq!(split_host_port(&format!("{host}:]")), None);
            assert_eq!(split_host_port(&format!("{host}[:7")), None);
            assert_eq!(split_host_port(&format!("{host}::7")), None);
        }
        assert_eq!(split_host_port("数据库:服务"), Some(("数据库", "服务")));
    }
}
