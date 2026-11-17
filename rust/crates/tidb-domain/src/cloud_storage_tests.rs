// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
use crate::global_sysvars::GlobalSysvars;
use tidb_vardef::tidb_vars::TIDB_CLOUD_STORAGE_URI;

/// Go master `TestSetTiDBCloudStorageURI` (`variable/tests/session_test.go`).
#[test]
fn test_set_tidb_cloud_storage_uri() {
    struct Restore(String);
    impl Drop for Restore {
        /// Go `TestSetTiDBCloudStorageURI` deferred global-state restoration.
        fn drop(&mut self) {
            *tidb_vardef::CLOUD_STORAGE_URI.write().unwrap() = self.0.clone();
        }
    }
    let _restore = Restore(tidb_vardef::CLOUD_STORAGE_URI.read().unwrap().clone());
    *tidb_vardef::CLOUD_STORAGE_URI.write().unwrap() = String::new();
    let globals = GlobalSysvars::new();
    assert_eq!(globals.get(TIDB_CLOUD_STORAGE_URI).unwrap(), "");
    let noop = "noop://blackhole?access-key=hello&secret-access-key=world";
    globals.set(TIDB_CLOUD_STORAGE_URI, noop.into()).unwrap();
    // Go master: unknown schemes drop the query on read; the stored value
    // keeps it.
    assert_eq!(globals.get(TIDB_CLOUD_STORAGE_URI).unwrap(), "noop://blackhole");
    assert_eq!(*tidb_vardef::CLOUD_STORAGE_URI.read().unwrap(), noop);
    for (scheme, requests) in [("s3", 4), ("ks3", 3)] {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            use std::io::{BufRead, BufReader, Write};
            for _ in 0..requests {
                let (mut socket, _) = listener.accept().unwrap();
                socket
                    .set_read_timeout(Some(std::time::Duration::from_secs(5)))
                    .unwrap();
                let mut reader = BufReader::new(socket.try_clone().unwrap());
                loop {
                    let mut line = String::new();
                    reader.read_line(&mut line).unwrap();
                    if line == "\r\n" {
                        break;
                    }
                }
                socket
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                    .unwrap();
            }
        });
        let region = if scheme == "ks3" { "region=test&" } else { "" };
        let s3 = format!(
            "{scheme}://tiflow-test/?{region}access-key=testid&secret-access-key=testkey8&session-token=testtoken&endpoint=http://{addr}"
        );
        globals.set(TIDB_CLOUD_STORAGE_URI, s3.clone()).unwrap();
        server.join().unwrap();
        let visible = globals.get(TIDB_CLOUD_STORAGE_URI).unwrap();
        assert!(visible.starts_with(&format!("{scheme}://tiflow-test/")));
        for name in ["access-key", "secret-access-key", "session-token"] {
            assert!(visible.contains(&format!("{name}=xxxxxx")));
        }
        assert_eq!(*tidb_vardef::CLOUD_STORAGE_URI.read().unwrap(), s3);
        // Go skips another storage request when the raw URI is unchanged.
        globals.set(TIDB_CLOUD_STORAGE_URI, s3.clone()).unwrap();
        let invalid = s3.replacen(&format!("{scheme}://"), "invalid://", 1);
        assert!(globals.set(TIDB_CLOUD_STORAGE_URI, invalid).is_err());
        assert_eq!(*tidb_vardef::CLOUD_STORAGE_URI.read().unwrap(), s3);
        assert_eq!(globals.get(TIDB_CLOUD_STORAGE_URI).unwrap(), visible);
    }
    globals.set(TIDB_CLOUD_STORAGE_URI, String::new()).unwrap();
    assert_eq!(globals.get(TIDB_CLOUD_STORAGE_URI).unwrap(), "");
}
