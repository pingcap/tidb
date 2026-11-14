// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
use std::{
    io::{BufRead, BufReader, Write},
    net::TcpListener,
    time::Duration,
};

/// Go `TestValidateCloudStorageURI`, captured in `zzprobe/o15_extstore`.
#[test]
fn test_validate_cloud_storage_uri() {
    for failure in ["", "NoSuchKey", "NoSuchBucket", "AccessDenied"] {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let succeeds = matches!(failure, "" | "NoSuchKey");
        let server = std::thread::spawn(move || {
            let mut calls = Vec::new();
            for index in 0..if succeeds { 3 } else { 2 } {
                let (mut socket, _) = listener.accept().unwrap();
                socket
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let mut reader = BufReader::new(socket.try_clone().unwrap());
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                let parts: Vec<_> = line.split_whitespace().collect();
                let method = parts[0].to_owned();
                let url = url::Url::parse(&format!("http://localhost{}", parts[1])).unwrap();
                let path = url.path().to_owned();
                calls.push(method.clone());
                let mut signed = false;
                loop {
                    line.clear();
                    reader.read_line(&mut line).unwrap();
                    if line == "\r\n" {
                        break;
                    }
                    if line
                        .to_ascii_lowercase()
                        .starts_with("authorization: aws4-hmac-sha256 ")
                    {
                        signed = true;
                    }
                }
                assert!(signed);
                if index == 0 {
                    assert_eq!(path.trim_end_matches('/'), "/bucket");
                    let params: std::collections::HashMap<_, _> = url.query_pairs().collect();
                    assert_eq!(params["prefix"], "base/");
                    assert_eq!(params["max-keys"], "1");
                } else if index == 1 {
                    assert!(path.starts_with("/bucket/base/perm-check/"));
                } else {
                    assert_eq!(path.trim_end_matches('/'), "/bucket");
                }
                let code = if index == 1 && !failure.is_empty() {
                    404
                } else {
                    200
                };
                let body = if code == 404 {
                    format!("<Error><Code>{failure}</Code></Error>")
                } else {
                    String::new()
                };
                write!(socket,"HTTP/1.1 {code} Result\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).unwrap();
            }
            assert_eq!(
                calls,
                if succeeds {
                    vec!["GET", "GET", "HEAD"]
                } else {
                    vec!["GET", "GET"]
                }
            );
        });
        let uri = format!(
            "s3://bucket/base?provider=minio&region=us-east-1&access-key=fixture&secret-access-key=private-fixture&endpoint=http://{addr}"
        );
        let result =
            crate::validate_cloud_storage_uri(&tidb_gocontext::Context::background(), &uri);
        assert_eq!(result.is_ok(), succeeds, "{failure}: {result:?}");
        if let Err(error) = result {
            assert!(!error.contains("private-fixture"));
            assert!(error.starts_with("permission GetObject:"));
        }
        server.join().unwrap();
    }
}

/// Go `zzprobe/o15_extstore.TestValidateCloudStorageURICancellation`.
#[test]
fn test_validate_cloud_storage_uri_cancellation() {
    use std::io::Read;
    use std::time::Instant;
    for scheme in ["s3", "oss"] {
        for already in [true, false] {
            let (ctx, cancel) =
                tidb_gocontext::Context::with_cancel(&tidb_gocontext::Context::background());
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();
            listener.set_nonblocking(true).unwrap();
            if already {
                cancel.cancel();
            }
            let server = std::thread::spawn(move || {
                let started = Instant::now();
                loop {
                    match listener.accept() {
                        Ok((mut socket, _)) => {
                            socket.set_nonblocking(false).unwrap();
                            socket
                                .set_read_timeout(Some(Duration::from_millis(500)))
                                .unwrap();
                            let mut reader = BufReader::new(socket.try_clone().unwrap());
                            let mut line = String::new();
                            loop {
                                line.clear();
                                reader.read_line(&mut line).unwrap();
                                if line == "\r\n" {
                                    break;
                                }
                            }
                            cancel.cancel();
                            let mut byte = [0];
                            let closed = socket.read(&mut byte).is_ok_and(|n| n == 0);
                            return (1, closed);
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            if started.elapsed() > Duration::from_millis(500) {
                                return (0, true);
                            }
                            std::thread::sleep(Duration::from_millis(2));
                        }
                        Err(error) => panic!("fixture accept: {error}"),
                    }
                }
            });
            let uri=format!("{scheme}://bucket/base?region=cn-hangzhou&access-key=fixture&secret-access-key=fixture&endpoint=http://{addr}");
            let started = Instant::now();
            let error = crate::validate_cloud_storage_uri(&ctx, &uri)
                .err()
                .expect("canceled constructor");
            assert!(
                error.contains("canceled"),
                "{scheme}, already={already}, context={:?}: {error}",
                ctx.err()
            );
            assert!(started.elapsed() < Duration::from_millis(300));
            assert_eq!(server.join().unwrap(), (if already { 0 } else { 1 }, true));
        }
    }
}
