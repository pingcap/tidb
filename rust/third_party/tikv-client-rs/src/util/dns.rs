// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, UdpSocket};
use tokio::time::timeout;

static NEXT_QUERY_ID: AtomicU16 = AtomicU16::new(1);
const DNS_TIMEOUT: Duration = Duration::from_secs(10);

/// Append the Kubernetes DNS suffix to `host:port`.
pub fn wrap_with_domain(target: &str, domain: &str) -> io::Result<String> {
    if domain.is_empty() {
        return Ok(target.to_owned());
    }
    let fields: Vec<_> = target.split(':').collect();
    if fields.len() != 2 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("target {target} is not valid"),
        ));
    }
    Ok(format!("{}.{}:{}", fields[0], domain, fields[1]))
}

/// TCP dialer whose A/AAAA lookups are sent to one explicit DNS server.
#[derive(Clone, Debug)]
pub struct CustomDnsDialer {
    dns_server: String,
    dns_domain: String,
}

impl CustomDnsDialer {
    pub fn new(dns_server: impl Into<String>, dns_domain: impl Into<String>) -> Self {
        Self {
            dns_server: dns_server.into(),
            dns_domain: dns_domain.into(),
        }
    }

    pub async fn connect(&self, target: &str) -> io::Result<TcpStream> {
        let target = wrap_with_domain(target, &self.dns_domain)?;
        let (host, port) = split_host_port(&target)?;
        let port = port.parse::<u16>().map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid port: {error}"),
            )
        })?;
        let addresses = if let Ok(address) = host.parse::<IpAddr>() {
            vec![address]
        } else {
            self.resolve(host).await?
        };
        let mut last_error = None;
        for address in addresses {
            match TcpStream::connect(SocketAddr::new(address, port)).await {
                Ok(stream) => return Ok(stream),
                Err(error) => last_error = Some(error),
            }
        }
        Err(last_error.unwrap_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("DNS returned no address for {host}"),
            )
        }))
    }

    async fn resolve(&self, host: &str) -> io::Result<Vec<IpAddr>> {
        let dns_server = tokio::net::lookup_host(self.dns_server.as_str())
            .await?
            .next()
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotFound, "DNS server resolved to no address")
            })?;
        let mut addresses = Vec::new();
        let mut first_error = None;
        for query_type in [1_u16, 28_u16] {
            match resolve_type(dns_server, host, query_type).await {
                Ok(mut resolved) => addresses.append(&mut resolved),
                Err(error) if first_error.is_none() => first_error = Some(error),
                Err(_) => {}
            }
        }
        if addresses.is_empty() {
            Err(first_error.unwrap_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    "DNS response contained no addresses",
                )
            }))
        } else {
            Ok(addresses)
        }
    }
}

async fn resolve_type(server: SocketAddr, host: &str, query_type: u16) -> io::Result<Vec<IpAddr>> {
    let mut current = host.to_owned();
    for _ in 0..8 {
        let response = query(server, &current, query_type).await?;
        if !response.addresses.is_empty() {
            return Ok(response.addresses);
        }
        match response.canonical_name {
            Some(canonical_name) => current = canonical_name,
            None => return Ok(Vec::new()),
        }
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        "DNS CNAME chain exceeds eight redirects",
    ))
}

fn split_host_port(target: &str) -> io::Result<(&str, &str)> {
    if let Some(target) = target.strip_prefix('[') {
        if let Some((host, port)) = target.split_once("]:") {
            if !host.is_empty() && !port.is_empty() {
                return Ok((host, port));
            }
        }
    }
    let fields: Vec<_> = target.split(':').collect();
    if fields.len() != 2 || fields[0].is_empty() || fields[1].is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("target {target} is not valid"),
        ));
    }
    Ok((fields[0], fields[1]))
}

async fn query(server: SocketAddr, host: &str, query_type: u16) -> io::Result<DnsResponse> {
    let id = NEXT_QUERY_ID.fetch_add(1, Ordering::Relaxed);
    let request = build_query(id, host, query_type)?;
    let bind_address = if server.is_ipv4() {
        "0.0.0.0:0"
    } else {
        "[::]:0"
    };
    let response = timeout(DNS_TIMEOUT, async {
        let socket = UdpSocket::bind(bind_address).await?;
        socket.connect(server).await?;
        socket.send(&request).await?;
        let mut response = vec![0_u8; 65_535];
        let size = socket.recv(&mut response).await?;
        response.truncate(size);
        io::Result::Ok(response)
    })
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "custom DNS lookup timed out"))??;
    let response = parse_response(&response, id)?;
    if response.truncated {
        query_tcp(server, &request, id).await
    } else {
        Ok(response)
    }
}

async fn query_tcp(server: SocketAddr, request: &[u8], id: u16) -> io::Result<DnsResponse> {
    timeout(DNS_TIMEOUT, async {
        let mut stream = TcpStream::connect(server).await?;
        let length = u16::try_from(request.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "DNS query is too large"))?;
        stream.write_all(&length.to_be_bytes()).await?;
        stream.write_all(request).await?;
        let response_length = stream.read_u16().await? as usize;
        let mut response = vec![0_u8; response_length];
        stream.read_exact(&mut response).await?;
        parse_response(&response, id)
    })
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "custom DNS TCP lookup timed out"))?
}

fn build_query(id: u16, host: &str, query_type: u16) -> io::Result<Vec<u8>> {
    let mut packet = Vec::with_capacity(64);
    packet.extend_from_slice(&id.to_be_bytes());
    packet.extend_from_slice(&0x0100_u16.to_be_bytes());
    packet.extend_from_slice(&1_u16.to_be_bytes());
    packet.extend_from_slice(&0_u16.to_be_bytes());
    packet.extend_from_slice(&0_u16.to_be_bytes());
    packet.extend_from_slice(&0_u16.to_be_bytes());
    let host = host.trim_end_matches('.');
    if host.is_empty() || host.len() > 253 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid DNS name",
        ));
    }
    for label in host.split('.') {
        if label.is_empty() || label.len() > 63 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid DNS label",
            ));
        }
        packet.push(label.len() as u8);
        packet.extend_from_slice(label.as_bytes());
    }
    packet.push(0);
    packet.extend_from_slice(&query_type.to_be_bytes());
    packet.extend_from_slice(&1_u16.to_be_bytes());
    Ok(packet)
}

struct DnsResponse {
    addresses: Vec<IpAddr>,
    canonical_name: Option<String>,
    truncated: bool,
}

fn parse_response(packet: &[u8], id: u16) -> io::Result<DnsResponse> {
    if packet.len() < 12 || u16::from_be_bytes([packet[0], packet[1]]) != id {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid DNS response",
        ));
    }
    let flags = u16::from_be_bytes([packet[2], packet[3]]);
    if flags & 0x8000 == 0 || flags & 0x000f != 0 {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("DNS response error code {}", flags & 0x000f),
        ));
    }
    let questions = u16::from_be_bytes([packet[4], packet[5]]) as usize;
    let answers = u16::from_be_bytes([packet[6], packet[7]]) as usize;
    let authorities = u16::from_be_bytes([packet[8], packet[9]]) as usize;
    let additional = u16::from_be_bytes([packet[10], packet[11]]) as usize;
    let mut offset = 12;
    for _ in 0..questions {
        offset = skip_name(packet, offset)?;
        offset = offset
            .checked_add(4)
            .filter(|end| *end <= packet.len())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "truncated DNS question"))?;
    }
    let mut addresses = Vec::new();
    let mut canonical_name = None;
    for index in 0..answers + authorities + additional {
        offset = skip_name(packet, offset)?;
        if offset + 10 > packet.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "truncated DNS answer",
            ));
        }
        let record_type = u16::from_be_bytes([packet[offset], packet[offset + 1]]);
        let class = u16::from_be_bytes([packet[offset + 2], packet[offset + 3]]);
        let data_len = u16::from_be_bytes([packet[offset + 8], packet[offset + 9]]) as usize;
        offset += 10;
        let end = offset
            .checked_add(data_len)
            .filter(|end| *end <= packet.len())
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "truncated DNS record data")
            })?;
        if class == 1 && record_type == 1 && data_len == 4 {
            addresses.push(IpAddr::V4(Ipv4Addr::new(
                packet[offset],
                packet[offset + 1],
                packet[offset + 2],
                packet[offset + 3],
            )));
        } else if class == 1 && record_type == 28 && data_len == 16 {
            let mut bytes = [0_u8; 16];
            bytes.copy_from_slice(&packet[offset..end]);
            addresses.push(IpAddr::V6(Ipv6Addr::from(bytes)));
        } else if index < answers && class == 1 && record_type == 5 {
            canonical_name = Some(read_name(packet, offset)?.0);
        }
        offset = end;
    }
    Ok(DnsResponse {
        addresses,
        canonical_name,
        truncated: flags & 0x0200 != 0,
    })
}

fn read_name(packet: &[u8], offset: usize) -> io::Result<(String, usize)> {
    let mut labels = Vec::new();
    let mut cursor = offset;
    let mut next = None;
    for _ in 0..128 {
        let length = *packet
            .get(cursor)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "truncated DNS name"))?;
        if length & 0xc0 == 0xc0 {
            let low = *packet.get(cursor + 1).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "truncated DNS compression pointer",
                )
            })?;
            next.get_or_insert(cursor + 2);
            cursor = (((length & 0x3f) as usize) << 8) | low as usize;
            continue;
        }
        cursor += 1;
        if length == 0 {
            return Ok((labels.join("."), next.unwrap_or(cursor)));
        }
        if length & 0xc0 != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid DNS label",
            ));
        }
        let end = cursor
            .checked_add(length as usize)
            .filter(|end| *end <= packet.len())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "truncated DNS label"))?;
        labels.push(String::from_utf8_lossy(&packet[cursor..end]).into_owned());
        cursor = end;
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        "DNS compression pointer loop",
    ))
}

fn skip_name(packet: &[u8], mut offset: usize) -> io::Result<usize> {
    loop {
        let length = *packet
            .get(offset)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "truncated DNS name"))?;
        if length & 0xc0 == 0xc0 {
            return offset
                .checked_add(2)
                .filter(|end| *end <= packet.len())
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "truncated DNS compression pointer",
                    )
                });
        }
        offset += 1;
        if length == 0 {
            return Ok(offset);
        }
        if length & 0xc0 != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid DNS label",
            ));
        }
        offset = offset
            .checked_add(length as usize)
            .filter(|end| *end <= packet.len())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "truncated DNS label"))?;
    }
}

#[cfg(test)]
mod tests {
    use tokio::net::TcpListener;

    use super::*;

    #[test]
    fn source_domain_wrapping_and_rejection() {
        assert_eq!(
            wrap_with_domain("pd0.pd:2379", "cluster.local").unwrap(),
            "pd0.pd.cluster.local:2379"
        );
        assert_eq!(wrap_with_domain("pd0.pd:2379", "").unwrap(), "pd0.pd:2379");
        assert_eq!(split_host_port("[::1]:2379").unwrap(), ("::1", "2379"));
        assert_eq!(
            wrap_with_domain("bad-target", "cluster.local")
                .unwrap_err()
                .to_string(),
            "target bad-target is not valid"
        );
        assert!(wrap_with_domain("[::1]:2379", "cluster.local").is_err());
    }

    #[tokio::test]
    async fn custom_server_resolution_reaches_the_resolved_tcp_endpoint() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let dns = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let dns_address = dns.local_addr().unwrap();
        let dns_task = tokio::spawn(async move {
            for _ in 0..2 {
                let mut request = [0_u8; 512];
                let (size, peer) = dns.recv_from(&mut request).await.unwrap();
                let mut response = request[..size].to_vec();
                response[2] = 0x81;
                response[3] = 0x80;
                let query_type = u16::from_be_bytes([response[size - 4], response[size - 3]]);
                if query_type == 1 {
                    response[6..8].copy_from_slice(&1_u16.to_be_bytes());
                    response.extend_from_slice(&[
                        0xc0, 0x0c, 0, 1, 0, 1, 0, 0, 0, 1, 0, 4, 127, 0, 0, 1,
                    ]);
                }
                dns.send_to(&response, peer).await.unwrap();
            }
        });
        let accept = tokio::spawn(async move { listener.accept().await.unwrap() });
        let dialer = CustomDnsDialer::new(dns_address.to_string(), "cluster.local");
        let stream = dialer.connect(&format!("pd0.pd:{port}")).await.unwrap();
        assert_eq!(
            stream.peer_addr().unwrap().ip(),
            IpAddr::V4(Ipv4Addr::LOCALHOST)
        );
        accept.await.unwrap();
        dns_task.await.unwrap();
    }
}
