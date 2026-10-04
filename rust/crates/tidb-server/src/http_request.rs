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

//! Framing for the status listener's one-request HTTP connections.
use std::io::{BufRead, BufReader, Read};
const MAX_HEADERS: usize = 1 << 20;
// net/http ParseForm's default form-body bound.
const MAX_BODY: usize = 10 << 20;

fn line(reader: &mut impl BufRead, limit: usize) -> Result<Vec<u8>, String> {
    let mut bytes = Vec::new();
    reader
        .take(limit as u64 + 1)
        .read_until(b'\n', &mut bytes)
        .map_err(|e| e.to_string())?;
    if bytes.len() > limit || !bytes.ends_with(b"\r\n") {
        return Err("invalid or oversized HTTP header".into());
    }
    Ok(bytes)
}

pub(crate) fn read_request(input: impl Read) -> Result<String, String> {
    let mut input = BufReader::new(input);
    let mut headers = Vec::new();
    loop {
        let next = line(&mut input, MAX_HEADERS.saturating_sub(headers.len()))?;
        let end = next == b"\r\n";
        headers.extend(next);
        if end {
            break;
        }
    }
    let headers = String::from_utf8(headers).map_err(|_| "invalid HTTP headers")?;
    let mut length = None;
    let mut chunked = false;
    for (key, value) in headers
        .lines()
        .skip(1)
        .filter_map(|line| line.split_once(':'))
    {
        let value = value.trim();
        if key.eq_ignore_ascii_case("Content-Length") {
            if value.is_empty() || !value.bytes().all(|b| b.is_ascii_digit()) {
                return Err("invalid Content-Length".into());
            }
            let parsed: usize = value.parse().map_err(|_| "invalid Content-Length")?;
            if length.is_some_and(|old| old != parsed) {
                return Err("conflicting Content-Length".into());
            }
            length = Some(parsed);
        }
        if key.eq_ignore_ascii_case("Transfer-Encoding") {
            if !value.eq_ignore_ascii_case("chunked") || chunked {
                return Err("unsupported Transfer-Encoding".into());
            }
            chunked = true;
        }
    }
    let mut body = Vec::new();
    if chunked {
        loop {
            let size = line(&mut input, MAX_HEADERS)?;
            let size = std::str::from_utf8(&size).map_err(|_| "invalid chunk size")?;
            let size = size.trim_end().split(';').next().unwrap_or("");
            if size.is_empty() || !size.bytes().all(|b| b.is_ascii_hexdigit()) {
                return Err("invalid chunk size".into());
            }
            let size = usize::from_str_radix(size, 16).map_err(|_| "invalid chunk size")?;
            if size == 0 {
                let mut remaining = MAX_HEADERS;
                loop {
                    let trailer = line(&mut input, remaining)?;
                    remaining = remaining.saturating_sub(trailer.len());
                    if trailer == b"\r\n" {
                        break;
                    }
                }
                break;
            }
            if size > MAX_BODY.saturating_sub(body.len()) {
                return Err("http: POST too large".into());
            }
            let offset = body.len();
            body.resize(offset + size, 0);
            input
                .read_exact(&mut body[offset..])
                .map_err(|e| e.to_string())?;
            let mut end = [0; 2];
            input.read_exact(&mut end).map_err(|e| e.to_string())?;
            if end != *b"\r\n" {
                return Err("malformed chunked encoding".into());
            }
        }
    } else if let Some(length) = length {
        if length > MAX_BODY {
            return Err("http: POST too large".into());
        }
        body.resize(length, 0);
        input.read_exact(&mut body).map_err(|e| e.to_string())?;
    }
    let body = String::from_utf8(body).map_err(|_| "invalid UTF-8 request body")?;
    Ok(headers + &body)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn status_framing_reads_fragmented_and_chunked_forms() {
        struct Fragment<'a>(&'a [u8]);
        impl Read for Fragment<'_> {
            fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
                let n = bytes.len().min(3).min(self.0.len());
                bytes[..n].copy_from_slice(&self.0[..n]);
                self.0 = &self.0[n..];
                Ok(n)
            }
        }
        let raw = "POST /settings HTTP/1.1\r\nContent-Length: 3\r\n\r\na=1";
        assert_eq!(read_request(Fragment(raw.as_bytes())).unwrap(), raw);
        let chunk = "POST /settings HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n2\r\na=\r\n1;x=1\r\n1\r\n0\r\nX-Trailer: yes\r\n\r\n";
        assert!(read_request(Fragment(chunk.as_bytes()))
            .unwrap()
            .ends_with("\r\n\r\na=1"));
        for raw in [
            "POST / HTTP/1.1\r\nContent-Length: 4\r\n\r\na=1",
            "POST / HTTP/1.1\r\nContent-Length: 3\r\nContent-Length: 2\r\n\r\na=1",
            "POST / HTTP/1.1\r\nContent-Length: 10485761\r\n\r\n",
            "POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n3\r\na=1xx",
        ] {
            assert!(read_request(raw.as_bytes()).is_err(), "{raw}");
        }
    }
}
