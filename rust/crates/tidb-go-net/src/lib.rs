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

/// Go `net.JoinHostPort`: combines host and port into a network address of
/// the form "host:port". If host contains a colon, as found in literal IPv6
/// addresses, it returns "[host]:port".
#[must_use]
pub fn join_host_port(host: &str, port: impl std::fmt::Display) -> String {
    // We assume that host is a literal IPv6 address if host has colons.
    if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

/// Go `net.SplitHostPort`: splits a network address of the form "host:port",
/// "host%zone:port", "[host]:port" or "[host%zone]:port" into host or
/// host%zone and port. `None` where Go answers an `AddrError` (a missing
/// port, too many colons, a misplaced bracket).
#[must_use]
pub fn split_host_port(hostport: &str) -> Option<(&str, &str)> {
    let colon = hostport.rfind(':')?;
    let (host, port) = (&hostport[..colon], &hostport[colon + 1..]);
    if let Some(inner) = host.strip_prefix('[') {
        let inner = inner.strip_suffix(']')?;
        if inner.contains(['[', ']']) {
            return None;
        }
        return (!port.contains(['[', ']'])).then_some((inner, port));
    }
    // An unbracketed host has no colon and no bracket of its own.
    if host.contains([':', '[', ']']) || port.contains(['[', ']']) {
        return None;
    }
    Some((host, port))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn join_host_port_brackets_a_host_with_colons() {
        assert_eq!(join_host_port("127.0.0.1", 4000), "127.0.0.1:4000");
        assert_eq!(join_host_port("::1", "4000"), "[::1]:4000");
        assert_eq!(join_host_port("", 0_u16), ":0");
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
}
