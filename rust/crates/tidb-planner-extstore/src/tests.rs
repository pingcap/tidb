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

//! Go `extstore_test.go`, and the Go captures (`zzprobe/o15_extstore`,
//! `o15_hdfs`) of what `NewExtStorage` builds for each backend. Every
//! storage here is `objstore`'s, reached through [`super::new_ext_storage`].

use std::io;
use std::path::Path;
use std::time::Duration;

use tidb_gocontext::Context;
use tidb_objstore::objectio::Reader;

use super::*;

/// One storage `new_ext_storage` built, with the background context every
/// call takes, so the captured fixtures read as Go's calls do.
pub(crate) struct S(ExtStorage, Context);

impl S {
    pub(crate) fn new(raw_url: &str, namespace: &str) -> Result<Self, String> {
        new_ext_storage(&Context::background(), raw_url, namespace).map(|storage| Self(storage, Context::background()))
    }
    pub(crate) fn uri(&self) -> String {
        self.0.uri()
    }
    pub(crate) fn write_file(&self, name: &str, data: &[u8]) -> tidb_objstore::error::Result<()> {
        self.0.write_file(&self.1, name, data)
    }
    pub(crate) fn read_file(&self, name: &str) -> tidb_objstore::error::Result<Vec<u8>> {
        self.0.read_file(&self.1, name)
    }
    pub(crate) fn file_exists(&self, name: &str) -> tidb_objstore::error::Result<bool> {
        self.0.file_exists(&self.1, name)
    }
    pub(crate) fn delete_file(&self, name: &str) -> Result<(), String> {
        self.0.delete_file(&self.1, name).map_err(|e| e.to_string())
    }
    pub(crate) fn delete_files(&self, names: &[&str]) -> Result<(), String> {
        let names: Vec<String> = names.iter().map(|name| (*name).to_owned()).collect();
        self.0.delete_files(&self.1, &names).map_err(|e| e.to_string())
    }
    pub(crate) fn open(&self, name: &str) -> tidb_objstore::error::Result<Box<dyn Reader>> {
        self.0.open(&self.1, name, None)
    }
    pub(crate) fn create_writer(&self, path: &Path) -> io::Result<tidb_replayer::StorageWriter> {
        tidb_replayer::Storage::create(self.0.as_ref(), path)
    }
    pub(crate) fn create(&self, path: &Path) -> io::Result<tidb_replayer::StorageWriter> {
        self.create_writer(path)
    }
    pub(crate) fn rename(&self, old: &str, new: &str) -> tidb_objstore::error::Result<()> {
        self.0.rename(&self.1, old, new)
    }
    /// Go `WalkDir` with `SubDir`: the names it calls back with.
    pub(crate) fn walk_dir(&self, sub_dir: &str) -> tidb_objstore::error::Result<Vec<String>> {
        let mut names = Vec::new();
        let option = tidb_objstore::storeapi::WalkOption { sub_dir: sub_dir.to_owned(), ..Default::default() };
        self.0.walk_dir(&self.1, Some(&option), &mut |name, _| {
            names.push(name.to_owned());
            Ok(())
        })?;
        Ok(names)
    }
    pub(crate) fn presign_file(&self, path: &str, expire: Duration) -> Result<String, String> {
        self.0.presign_file(&self.1, path, expire).map_err(|e| e.to_string())
    }
    pub(crate) fn close(&self) {
        self.0.close();
    }
}


mod cloud_tests {
    #[allow(unused_imports)]
    use super::*;
    use std::io::{BufRead, BufReader, Read, Write};
    /// Go `zzprobe/o15_extstore.TestExtStorageExplicitProfile`.
    #[test]
    fn test_ext_storage_explicit_profile() {
        profile_fixture(false);
    }
    /// Go `zzprobe/o15_extstore.TestExtStorageDefaultCredentialProcess`.
    #[test]
    fn test_ext_storage_default_credential_process() {
        profile_fixture(true);
    }
    /// Go `s3store.NewS3Storage`: profile and credential-process test fixture.
    fn profile_fixture(process: bool) {
        if std::env::var_os("O15_PROFILE_TEST_CHILD").is_none() {
            let root = std::env::temp_dir().join(format!("o15-profile-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir_all(&root).unwrap();
            std::fs::write(root.join("credentials"),"[wanted]\naws_access_key_id = wanted_fixture\naws_secret_access_key = wanted_fixture\n[ambient]\naws_access_key_id = ambient_fixture\naws_secret_access_key = ambient_fixture\n").unwrap();
            std::fs::write(root.join("process"),r#"printf '%s' '{"Version":1,"AccessKeyId":"wanted_fixture","SecretAccessKey":"wanted_fixture"}'"#).unwrap();
            std::fs::write(
                root.join("config"),
                format!(
                    "[profile process]\ncredential_process = /bin/sh {}\n",
                    root.join("process").display()
                ),
            )
            .unwrap();
            let mut command = std::process::Command::new(std::env::current_exe().unwrap());
            command
                .args([
                    "--exact",
                    if process {
                        "tests::cloud_tests::test_ext_storage_default_credential_process"
                    } else {
                        "tests::cloud_tests::test_ext_storage_explicit_profile"
                    },
                    "--nocapture",
                ])
                .env("O15_PROFILE_TEST_CHILD", "1")
                .env("AWS_SHARED_CREDENTIALS_FILE", root.join("credentials"))
                .env("AWS_CONFIG_FILE", root.join("config"))
                .env("AWS_PROFILE", "ambient")
                .env("AWS_ACCESS_KEY_ID", "environment_fixture")
                .env("AWS_SECRET_ACCESS_KEY", "environment_fixture")
                .env("AWS_EC2_METADATA_DISABLED", "true")
                .env_remove("AWS_SESSION_TOKEN")
                .env_remove("AWS_WEB_IDENTITY_TOKEN_FILE")
                .env_remove("AWS_ROLE_ARN");
            if process {
                command
                    .env("AWS_PROFILE", "process")
                    .env_remove("AWS_ACCESS_KEY_ID")
                    .env_remove("AWS_SECRET_ACCESS_KEY");
            }
            let result = command.status().unwrap();
            std::fs::remove_dir_all(root).unwrap();
            assert!(result.success());
            return;
        }
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let mut matched = true;
            for _ in 0..2 {
                let (mut socket, _) = listener.accept().unwrap();
                socket
                    .set_read_timeout(Some(Duration::from_secs(10)))
                    .unwrap();
                let mut reader = BufReader::new(socket.try_clone().unwrap());
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                let mut length = 0;
                let mut explicit_profile = false;
                loop {
                    line.clear();
                    reader.read_line(&mut line).unwrap();
                    if line == "\r\n" {
                        break;
                    }
                    if let Some((name, value)) = line.split_once(':') {
                        if name.eq_ignore_ascii_case("authorization") {
                            explicit_profile = value.contains("Credential=wanted_fixture/");
                        }
                        if name.eq_ignore_ascii_case("content-length") {
                            length = value.trim().parse().unwrap();
                        }
                    }
                }
                matched &= explicit_profile;
                let mut body = vec![0; length];
                reader.read_exact(&mut body).unwrap();
                write!(socket,"HTTP/1.1 200 OK\r\nx-amz-bucket-region: us-east-1\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").unwrap();
            }
            matched
        });
        let profile = if process { "" } else { "&profile=wanted" };
        let storage = S::new(
            &format!("s3://bucket/base?region=us-east-1{profile}&endpoint=http://{addr}"),
            "tenant",
        )
        .unwrap();
        storage.write_file("file", b"payload").unwrap();
        assert!(
            server.join().unwrap(),
            "explicit profile must override AWS_PROFILE and environment credentials"
        );
    }

    /// Go `zzprobe/o15_extstore.TestExtStorageNamespaceClean`.
    #[test]
    fn test_ext_storage_namespace_clean() {
        let root = std::env::temp_dir().join(format!("o15-path-{}", uuid::Uuid::new_v4()));
        for (path, namespace, expected) in [
            ("/base", "../tenant", "/tenant"),
            ("/base/./old/..", "/tenant//x/../y", "/base/tenant/y"),
            ("/base", "", "/base"),
        ] {
            let storage =
                S::new(&format!("file://{}{path}", root.display()), namespace)
                    .unwrap();
            assert_eq!(
                storage.uri(),
                format!("file://{}{expected}", root.display())
            );
        }
        std::fs::remove_dir_all(root).unwrap();
    }
    /// Go `zzprobe/o15_extstore.TestExtStorageRelativeLocal`.
    #[test]
    fn test_ext_storage_relative_local() {
        let cwd = std::env::current_dir().unwrap();
        let storage = S::new("", "").unwrap();
        assert_eq!(storage.uri(), format!("file://{}", cwd.display()));
        let name = format!("o15-relative-{}", uuid::Uuid::new_v4());
        let storage = S::new(&format!("{name}/base/../dir"), "tenant").unwrap();
        assert_eq!(
            storage.uri(),
            format!("file://{}/{name}/dir/tenant", cwd.display())
        );
        std::fs::remove_dir_all(cwd.join(name)).unwrap();
    }
    /// Go `TestExtStorageCloudNamespace`, captured under `zzprobe/o15_extstore`.
    #[test]
    fn test_ext_storage_cloud_namespace() {
        cloud_namespace(false, false);
    }
    /// Go `zzprobe/o15_extstore.TestExtStorageKS3Namespace`.
    #[test]
    fn test_ext_storage_ks3_namespace() {
        cloud_namespace(true, false);
    }
    /// Go `zzprobe/o15_extstore.TestExtStorageOSSNamespace`.
    #[test]
    fn test_ext_storage_oss_namespace() {
        cloud_namespace(false, true);
    }
    /// Go `TestExtStorage`: shared S3-compatible namespace fixture.
    fn cloud_namespace(ks3: bool, oss: bool) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let mut objects = std::collections::BTreeMap::<String, Vec<u8>>::new();
            for _ in 0..if oss { 9 } else { 8 } {
                let (mut socket, _) = listener.accept().unwrap();
                socket
                    .set_read_timeout(Some(Duration::from_secs(10)))
                    .unwrap();
                let mut reader = BufReader::new(socket.try_clone().unwrap());
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                let parts: Vec<_> = line.split_whitespace().collect();
                let method = parts[0].to_owned();
                let uri = url::Url::parse(&format!("http://localhost{}", parts[1])).unwrap();
                let mut length = 0;
                let mut signed = false;
                let mut acl = false;
                loop {
                    line.clear();
                    reader.read_line(&mut line).unwrap();
                    if line == "\r\n" {
                        break;
                    }
                    if let Some((name, value)) = line.split_once(':') {
                        if name.eq_ignore_ascii_case("content-length") {
                            length = value.trim().parse().unwrap();
                        }
                        if name.eq_ignore_ascii_case("authorization") {
                            signed = value.trim().starts_with(if ks3 {
                                "AWS fixture:"
                            } else if oss {
                                "OSS4-HMAC-SHA256 "
                            } else {
                                "AWS4-HMAC-SHA256 "
                            });
                        }
                        if name.eq_ignore_ascii_case("x-amz-acl") {
                            acl = value.trim() == "private";
                        }
                    }
                }
                assert!(signed);
                if method == "PUT" && !oss {
                    assert!(acl);
                }
                let mut body = vec![0; length];
                reader.read_exact(&mut body).unwrap();
                let path = uri.path();
                if oss && uri.query_pairs().any(|(key, _)| key == "location") {
                    let body = "<LocationConstraint>oss-us-east-1</LocationConstraint>";
                    write!(
                        socket,
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    )
                    .unwrap();
                    continue;
                }
                let list = uri.query_pairs().any(|(key, _)| key == "prefix");
                if list {
                    assert_eq!(uri.query_pairs().any(|(key, _)| key == "list-type"), !ks3);
                }
                if !list {
                    assert!(path.starts_with("/bucket/base/tenant/"));
                }
                let (code, response) = match method.as_str() {
                    "PUT" => {
                        objects.insert(path.into(), body);
                        (200, Vec::new())
                    }
                    "HEAD" => (
                        if objects.contains_key(path) { 200 } else { 404 },
                        Vec::new(),
                    ),
                    "GET" if list => {
                        let mut xml =
                            "<ListBucketResult><Name>bucket</Name><IsTruncated>false</IsTruncated>"
                                .to_owned();
                        for (name, bytes) in &objects {
                            xml.push_str(&format!("<Contents><Key>{}</Key><Size>{}</Size><LastModified>2019-01-01T00:00:00Z</LastModified></Contents>",name.strip_prefix("/bucket/").unwrap(),bytes.len()));
                        }
                        xml.push_str("</ListBucketResult>");
                        (200, xml.into_bytes())
                    }
                    "GET" => (200, objects.get(path).unwrap().clone()),
                    "DELETE" => {
                        objects.remove(path);
                        (204, Vec::new())
                    }
                    _ => panic!("unexpected method"),
                };
                let length = if method == "HEAD" {
                    objects.get(path).map_or(0, Vec::len)
                } else {
                    response.len()
                };
                write!(socket,"HTTP/1.1 {code} Result\r\nContent-Length: {length}\r\nLast-Modified: Tue, 01 Jan 2019 00:00:00 GMT\r\nETag: \"fixture\"\r\nConnection: close\r\n\r\n").unwrap();
                socket.write_all(&response).unwrap();
            }
            assert!(objects.is_empty());
        });
        let scheme = if ks3 {
            "ks3"
        } else if oss {
            "oss"
        } else {
            "s3"
        };
        let uri = format!(
            "{scheme}://bucket/base?acl=private&provider=minio&region=us-east-1&access-key=fixture&secret-access-key=fixture&endpoint=http://{addr}"
        );
        let s = S::new(&uri, "tenant").unwrap();
        assert_eq!(
            s.uri(),
            format!("{}://bucket/base/tenant/", if oss { "s3" } else { scheme })
        );
        s.write_file("file", b"payload").unwrap();
        assert_eq!(s.read_file("file").unwrap(), b"payload");
        assert!(s.file_exists("file").unwrap());
        s.rename("file", "renamed").unwrap();
        assert_eq!(s.walk_dir("").unwrap(), ["renamed"]);
        if ks3 {
            assert_eq!(
                s.presign_file("renamed", Duration::from_secs(60))
                    .unwrap_err(),
                "KS3 backend does not support PresignFile"
            );
        } else {
            let signed = s.presign_file("renamed", Duration::from_secs(60)).unwrap();
            let url = url::Url::parse(&signed).unwrap();
            assert_eq!(url.path(), "/bucket/base/tenant/renamed");
            assert!(url.query_pairs().any(|(key, _)| key
                == if oss {
                    "x-oss-signature"
                } else {
                    "X-Amz-Signature"
                }));
        }
        s.delete_file("renamed").unwrap();
        server.join().unwrap();
    }
}

mod gcs_tests {
    #[allow(unused_imports)]
    use super::*;
    /// Go `zzprobe/o15_extstore.TestExtStorageGCSNamespace`.
    #[test]
    fn test_ext_storage_gcs_namespace() {
        use std::io::{BufRead, BufReader, Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let mut objects = std::collections::BTreeMap::<String, Vec<u8>>::new();
            for _ in 0..9 {
                let (mut socket, _) = listener.accept().unwrap();
                socket
                    .set_read_timeout(Some(std::time::Duration::from_secs(10)))
                    .unwrap();
                let mut reader = BufReader::new(socket.try_clone().unwrap());
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                let parts: Vec<_> = line.split_whitespace().collect();
                let method = parts[0].to_owned();
                let url = url::Url::parse(&format!("http://localhost{}", parts[1])).unwrap();
                let mut headers = std::collections::HashMap::new();
                loop {
                    line.clear();
                    reader.read_line(&mut line).unwrap();
                    if line == "\r\n" {
                        break;
                    }
                    if let Some((name, value)) = line.split_once(':') {
                        headers.insert(name.to_ascii_lowercase(), value.trim().to_owned());
                    }
                }
                let length = headers
                    .get("content-length")
                    .map_or(0, |v| v.parse::<usize>().unwrap());
                let mut data = vec![0; length];
                reader.read_exact(&mut data).unwrap();
                let query: std::collections::HashMap<_, _> = url.query_pairs().collect();
                let (code, body) = if url.path() == "/token" {
                    let form: std::collections::HashMap<_, _> =
                        url::form_urlencoded::parse(&data).collect();
                    assert_eq!(form["grant_type"], "refresh_token");
                    assert_eq!(form["refresh_token"], "refresh_fixture");
                    (200,br#"{"access_token":"token_fixture","token_type":"Bearer","expires_in":3600}"#.to_vec())
                } else {
                    assert_eq!(headers["authorization"], "Bearer token_fixture");
                    if method == "POST" {
                        let name = query["name"].to_string();
                        assert!(name.starts_with("base/tenant/"));
                        assert_eq!(query["predefinedAcl"], "private");
                        let text = String::from_utf8(data).unwrap();
                        assert!(text.contains("NEARLINE"));
                        assert!(text.contains("payload"));
                        objects.insert(name.clone(), b"payload".to_vec());
                        (200,serde_json::to_vec(&serde_json::json!({"name":name,"bucket":"bucket","size":"7","generation":"1","updated":"2019-01-01T00:00:00Z"})).unwrap())
                    } else if url.path().ends_with("/o") {
                        let items:Vec<_>=objects.iter().map(|(name,data)|serde_json::json!({"name":name,"size":data.len().to_string(),"updated":"2019-01-01T00:00:00Z"})).collect();
                        (
                            200,
                            serde_json::to_vec(&serde_json::json!({"items":items})).unwrap(),
                        )
                    } else {
                        let encoded = url.path().split_once("/o/").unwrap().1;
                        let name = percent_encoding::percent_decode_str(encoded)
                            .decode_utf8()
                            .unwrap();
                        assert!(name.starts_with("base/tenant/"));
                        if method == "DELETE" {
                            objects.remove(name.as_ref());
                            (204, Vec::new())
                        } else if query.get("alt").is_some_and(|v| v == "media") {
                            (200, objects[name.as_ref()].clone())
                        } else {
                            (200,serde_json::to_vec(&serde_json::json!({"name":name,"size":objects[name.as_ref()].len().to_string(),"updated":"2019-01-01T00:00:00Z"})).unwrap())
                        }
                    }
                };
                write!(socket,"HTTP/1.1 {code} Result\r\nContent-Type: application/json\r\nContent-Length: {}\r\nLast-Modified: Tue, 01 Jan 2019 00:00:00 GMT\r\nConnection: close\r\n\r\n",body.len()).unwrap();
                socket.write_all(&body).unwrap();
            }
            assert!(objects.is_empty());
        });
        let root = std::env::temp_dir().join(format!("o15-gcs-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let file = root.join("credentials.json");
        std::fs::write(&file,serde_json::to_vec(&serde_json::json!({"type":"authorized_user","client_id":"client_fixture","client_secret":"secret_fixture","refresh_token":"refresh_fixture","token_uri":format!("http://{addr}/token")})).unwrap()).unwrap();
        let uri = format!(
            "gcs://bucket/base?credentials-file={}&storage-class=NEARLINE&predefined-acl=private&endpoint=http://{addr}",
            file.display()
        );
        let storage = S::new(&uri, "tenant").unwrap();
        assert_eq!(storage.uri(), "gcs://bucket/base/tenant");
        storage.write_file("file", b"payload").unwrap();
        assert_eq!(storage.read_file("file").unwrap(), b"payload");
        assert!(storage.file_exists("file").unwrap());
        storage.rename("file", "renamed").unwrap();
        assert_eq!(storage.walk_dir("").unwrap(), ["renamed"]);
        storage.delete_file("renamed").unwrap();
        server.join().unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }
}

mod azblob_tests {
    #[allow(unused_imports)]
    use super::*;
    /// Go `zzprobe/o15_extstore.TestExtStorageAzureSAS`.
    #[test]
    fn test_ext_storage_azure_sas() {
        use std::io::{BufRead, BufReader, Read, Write};
        for endpoint_token in ["", "endpoint-fixture"] {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();
            let server = std::thread::spawn(move || {
                for _ in 0..2 {
                    let (mut socket, _) = listener.accept().unwrap();
                    let mut reader = BufReader::new(socket.try_clone().unwrap());
                    let mut line = String::new();
                    reader.read_line(&mut line).unwrap();
                    let parts: Vec<_> = line.split_whitespace().collect();
                    let put = parts[0] == "PUT";
                    let url = url::Url::parse(&format!("http://localhost{}", parts[1])).unwrap();
                    let signature: Vec<_> = url
                        .query_pairs()
                        .filter(|(key, _)| key == "sig")
                        .map(|(_, value)| value.into_owned())
                        .collect();
                    let expected: Vec<_> = if endpoint_token.is_empty() {
                        vec![]
                    } else {
                        vec![endpoint_token.to_owned()]
                    };
                    let valid = signature == expected;
                    let mut length = 0;
                    loop {
                        line.clear();
                        reader.read_line(&mut line).unwrap();
                        if line == "\r\n" {
                            break;
                        }
                        assert!(!line.to_ascii_lowercase().starts_with("authorization:"));
                        if let Some((key, value)) = line.split_once(':') {
                            if key.eq_ignore_ascii_case("content-length") {
                                length = value.trim().parse::<usize>().unwrap();
                            }
                        }
                    }
                    let mut body = vec![0; length];
                    reader.read_exact(&mut body).unwrap();
                    let code = if !valid {
                        403
                    } else if put {
                        201
                    } else {
                        200
                    };
                    write!(
                        socket,
                        "HTTP/1.1 {code} Result\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    )
                    .unwrap();
                    assert!(valid, "custom endpoint SAS must be preserved exactly once");
                }
            });
            let endpoint = if endpoint_token.is_empty() {
                format!("http://{addr}")
            } else {
                format!("http://{addr}?sig={endpoint_token}")
            };
            let mut uri =
                url::Url::parse("azure://bucket/base?account-name=fixture&sas-token=sig%3Dignored")
                    .unwrap();
            uri.query_pairs_mut().append_pair("endpoint", &endpoint);
            let storage = S::new(uri.as_str(), "tenant").unwrap();
            storage.write_file("file", b"payload").unwrap();
            server.join().unwrap();
        }
    }
    /// Go `zzprobe/o15_extstore.TestExtStorageAzureNamespace`.
    #[test]
    fn test_ext_storage_azure_namespace() {
        use std::io::{BufRead, BufReader, Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let mut objects = std::collections::BTreeMap::<String, Vec<u8>>::new();
            for _ in 0..11 {
                let (mut socket, _) = listener.accept().unwrap();
                socket
                    .set_read_timeout(Some(std::time::Duration::from_secs(10)))
                    .unwrap();
                let mut reader = BufReader::new(socket.try_clone().unwrap());
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                let parts: Vec<_> = line.split_whitespace().collect();
                let method = parts[0].to_owned();
                let url = url::Url::parse(&format!("http://localhost{}", parts[1])).unwrap();
                let mut headers = std::collections::HashMap::new();
                loop {
                    line.clear();
                    reader.read_line(&mut line).unwrap();
                    if line == "\r\n" {
                        break;
                    }
                    if let Some((name, value)) = line.split_once(':') {
                        headers.insert(name.to_ascii_lowercase(), value.trim().to_owned());
                    }
                }
                assert!(headers["authorization"].starts_with("SharedKey fixture:"));
                let length = headers
                    .get("content-length")
                    .map_or(0, |v| v.parse::<usize>().unwrap());
                let mut data = vec![0; length];
                reader.read_exact(&mut data).unwrap();
                let query: std::collections::HashMap<_, _> = url.query_pairs().collect();
                let path = url.path();
                let (code, body) = if method == "GET"
                    && query.get("restype").is_some_and(|v| v == "container")
                    && !query.contains_key("comp")
                {
                    (200, Vec::new())
                } else if query.get("comp").is_some_and(|v| v == "list") {
                    let mut xml = "<EnumerationResults><Blobs>".to_owned();
                    for (name, bytes) in &objects {
                        xml.push_str(&format!("<Blob><Name>{}</Name><Properties><Content-Length>{}</Content-Length><Last-Modified>Tue, 01 Jan 2019 00:00:00 GMT</Last-Modified><BlobType>BlockBlob</BlobType></Properties></Blob>",name.strip_prefix("/bucket/").unwrap(),bytes.len()));
                    }
                    xml.push_str("</Blobs><NextMarker></NextMarker></EnumerationResults>");
                    (200, xml.into_bytes())
                } else {
                    assert!(path.starts_with("/bucket/base/tenant/"));
                    match method.as_str() {
                        "PUT" => {
                            assert_eq!(headers["x-ms-access-tier"], "Cool");
                            objects.insert(path.to_owned(), data);
                            (201, Vec::new())
                        }
                        "GET" => (200, objects.get(path).unwrap().clone()),
                        "HEAD" => (
                            if objects.contains_key(path) { 200 } else { 404 },
                            Vec::new(),
                        ),
                        "DELETE" => {
                            objects.remove(path);
                            (202, Vec::new())
                        }
                        _ => panic!("unexpected request"),
                    }
                };
                let length = if method == "HEAD" {
                    objects.get(path).map_or(0, Vec::len)
                } else {
                    body.len()
                };
                write!(socket,"HTTP/1.1 {code} Result\r\nContent-Length: {length}\r\nLast-Modified: Tue, 01 Jan 2019 00:00:00 GMT\r\nETag: \"fixture\"\r\nConnection: close\r\n\r\n").unwrap();
                socket.write_all(&body).unwrap();
            }
            assert!(objects.is_empty());
        });
        let uri = format!(
            "azure://bucket/base?account-name=fixture&account-key=Zml4dHVyZQ==&access-tier=cool&endpoint=http://{addr}"
        );
        let storage = S::new(&uri, "tenant").unwrap();
        assert_eq!(storage.uri(), "azure://bucket/base/tenant");
        storage.write_file("file", b"payload").unwrap();
        assert_eq!(storage.read_file("file").unwrap(), b"payload");
        assert!(storage.file_exists("file").unwrap());
        storage.rename("file", "renamed").unwrap();
        assert_eq!(storage.walk_dir("").unwrap(), ["renamed"]);
        assert_eq!(
            storage
                .presign_file("renamed", std::time::Duration::from_secs(60))
                .unwrap_err(),
            "AzureBlobStorage backend does not support PresignFile"
        );
        storage.delete_file("renamed").unwrap();
        for extra in ["&encryption-scope=scope", "&encryption-key=fixture"] {
            assert!(S::new(&format!("{uri}{extra}"), "tenant")
                .err()
                .unwrap()
                .contains("Set Blob Tier cannot be used"));
        }
        server.join().unwrap();
    }
}

mod azblob_credential_tests {
    #[allow(unused_imports)]
    use super::*;
    /// Go `zzprobe/o15_extstore.TestExtStorageAzureCredentialFallback`.
    #[test]
    fn test_ext_storage_azure_credential_fallback() {
        if std::env::var_os("O15_AZURE_FALLBACK_CHILD").is_none() {
            for tenant in ["", "invalid tenant"] {
                let status = std::process::Command::new(std::env::current_exe().unwrap())
                    .args([
                        "--exact",
                        "tests::azblob_credential_tests::test_ext_storage_azure_credential_fallback",
                        "--nocapture",
                    ])
                    .env("O15_AZURE_FALLBACK_CHILD", "1")
                    .env("AZURE_TENANT_ID", tenant)
                    .env("AZURE_CLIENT_ID", "fixture")
                    .env("AZURE_CLIENT_SECRET", "fixture")
                    .env("AZURE_STORAGE_KEY", "Zml4dHVyZQ==")
                    .env("AZURE_TOKEN_CREDENTIALS", "invalid")
                    .status()
                    .unwrap();
                assert!(status.success());
            }
            return;
        }
        use std::io::{BufRead, BufReader, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            socket
                .set_read_timeout(Some(std::time::Duration::from_secs(5)))
                .unwrap();
            let mut reader = BufReader::new(socket.try_clone().unwrap());
            let mut signed = false;
            let mut line = String::new();
            loop {
                line.clear();
                reader.read_line(&mut line).unwrap();
                if line == "\r\n" {
                    break;
                }
                if line
                    .to_lowercase()
                    .starts_with("authorization: sharedkey account:")
                {
                    signed = true;
                }
            }
            write!(
                socket,
                "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            )
            .unwrap();
            signed
        });
        let storage = S::new(
            &format!("azure://container/base?account-name=account&endpoint=http://{addr}"),
            "tenant",
        )
        .unwrap();
        storage.close();
        assert!(server.join().unwrap());
    }
}

mod hdfs_tests {
    #[allow(unused_imports)]
    use super::*;
    use std::process::Command;
    const UNSUPPORTED: &str = "currently HDFS backend only support rawkv backup: [BR:Common:ErrUnsupportedOperation]the operation is not supported";
    const MISSING_HOME: &str = "please specify environment variable HADOOP_HOME: [BR:Common:ErrEnvNotSpecified]environment variable not found";
    /// Go `zzprobe/o15_hdfs.TestHDFSStorage`.
    #[test]
    fn test_hdfs_storage() {
        use std::os::unix::fs::PermissionsExt as _;
        let s = S::new("hdfs://host:9000/base", "tenant").unwrap();
        assert_eq!(s.uri(), "hdfs://host:9000/base/tenant");
        assert_eq!(s.read_file("x").unwrap_err().to_string(), UNSUPPORTED);
        assert_eq!(
            s.presign_file("x", Duration::ZERO).unwrap_err(),
            "HDFS backend does not support PresignFile"
        );
        assert_eq!(s.delete_file("x").unwrap_err(), UNSUPPORTED);
        assert_eq!(s.delete_files(&["x"]).unwrap_err(), UNSUPPORTED);
        assert_eq!(s.open("x").err().unwrap().to_string(), UNSUPPORTED);
        assert_eq!(
            s.create_writer(Path::new("x")).err().unwrap().to_string(),
            UNSUPPORTED
        );
        assert_eq!(s.rename("x", "y").unwrap_err().to_string(), UNSUPPORTED);
        assert_eq!(s.walk_dir("").unwrap_err().to_string(), UNSUPPORTED);
        match std::env::var("O15_HDFS_TEST_CHILD").as_deref() {
            Ok("run") => {
                let home = std::path::PathBuf::from(std::env::var_os("HADOOP_HOME").unwrap());
                s.write_file("item", b"payload").unwrap();
                assert_eq!(std::fs::read(home.join("data")).unwrap(), b"payload");
                assert_eq!(
                    std::fs::read_to_string(home.join("args")).unwrap(),
                    "dfs\n-put\n-\nhdfs://host:9000/base/tenant/item\n"
                );
                assert!(s.file_exists("item").unwrap());
                assert!(!s.file_exists("missing").unwrap());
                return;
            }
            Ok("missing") => {
                assert_eq!(s.file_exists("item").unwrap_err().to_string(), MISSING_HOME);
                return;
            }
            _ => {}
        }
        let home = std::env::temp_dir().join(format!("o15-hdfs-{}", uuid::Uuid::new_v4()));
        struct Cleanup(std::path::PathBuf);
        impl Drop for Cleanup {
            /// Go `TestHDFSStorage` temporary directory cleanup.
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        let _cleanup = Cleanup(home.clone());
        std::fs::create_dir_all(home.join("bin")).unwrap();
        let script=b"#!/bin/sh\ncase \"$2\" in\n-put) cat > \"$HADOOP_HOME/data\"; printf '%s\\n' \"$@\" > \"$HADOOP_HOME/args\";;\n-ls) case \"$3\" in */missing) exit 1;; esac;;\nesac\n";
        std::fs::write(home.join("bin/hdfs"), script).unwrap();
        std::fs::set_permissions(
            home.join("bin/hdfs"),
            std::fs::Permissions::from_mode(0o755),
        )
        .unwrap();
        for mode in ["run", "missing"] {
            let mut command = Command::new(std::env::current_exe().unwrap());
            command
                .args(["--exact", "tests::hdfs_tests::test_hdfs_storage", "--nocapture"])
                .env("O15_HDFS_TEST_CHILD", mode)
                .env_remove("HADOOP_LINUX_USER");
            if mode == "run" {
                command.env("HADOOP_HOME", &home);
            } else {
                command.env_remove("HADOOP_HOME");
            }
            let output = command.output().unwrap();
            assert!(
                output.status.success(),
                "child failed: {} {}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        s.close();
    }
}

mod noop_tests {
    #[allow(unused_imports)]
    use super::*;
    use std::io::{Read as _, Seek as _};
    use tidb_replayer::ObjectWriter as _;
    /// Go `TestExtStorageNoop`, captured under `zzprobe/o15_extstore`.
    #[test]
    fn test_ext_storage_noop() {
        let s = S::new("noop://blackhole?access-key=fixture", "tenant").unwrap();
        assert_eq!(s.uri(), "noop:///");
        s.write_file("file", b"payload").unwrap();
        assert!(s.read_file("file").unwrap().is_empty());
        assert!(!s.file_exists("file").unwrap());
        let mut reader = s.open("file").unwrap();
        let mut bytes = *b"unchanged";
        assert_eq!(reader.read(&mut bytes).unwrap(), 9);
        assert_eq!(&bytes, b"unchanged");
        assert_eq!(reader.seek(io::SeekFrom::End(7)).unwrap(), 7);
        assert_eq!(reader.get_file_size().unwrap(), 0);
        let mut writer = s.create_writer(Path::new("file")).unwrap();
        assert_eq!(writer.write(&bytes).unwrap(), 9);
        writer.close().unwrap();
        s.rename("file", "new").unwrap();
        s.delete_file("missing").unwrap();
        s.delete_files(&["missing", "new"]).unwrap();
        assert!(s.walk_dir("").unwrap().is_empty());
        assert_eq!(s.presign_file("file", Duration::from_secs(60)).unwrap(), "");
        s.close();
    }
}

mod permission_tests {
    #[allow(unused_imports)]
    use super::*;
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
                super::validate_cloud_storage_uri(&tidb_gocontext::Context::background(), &uri);
            assert_eq!(result.is_ok(), succeeds, "{failure}: {result:?}");
            if let Err(error) = result {
                assert!(!error.contains("private-fixture"));
                // Go `NewS3Storage`: "check permission failed due to %v" of
                // `s3like.CheckPermissions`' "permission %s" annotation.
                assert!(error.starts_with("check permission failed due to permission GetObject:"), "{error}");
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
                let error = super::validate_cloud_storage_uri(&ctx, &uri)
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

}

mod go_tests {
    use super::*;
    use std::io::Read as _;
    use tidb_replayer::ObjectWriter as _;

    /// Go `zzprobe/o15_extstore.TestExtStorageParse`.
    #[test]
    fn test_ext_storage_parse() {
        let root = std::env::current_dir()
            .unwrap()
            .join(format!("o15-parse-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&root).unwrap();
        for (raw, namespace, expected) in [
            ("relative+file", "", "relative%2Bfile"),
            ("relative+file", "tenant", "relative+file/tenant"),
            ("relative%20file", "tenant", "relative%20file/tenant"),
            ("relative file", "", "relative%20file"),
            ("relative[abc]", "", "relative[abc]"),
            ("relative[abc]", "tenant", "relative%5Babc%5D/tenant"),
            ("relative!file", "", "relative!file"),
            ("relative!file", "tenant", "relative%21file/tenant"),
            ("relative?x=1", "tenant", "relative/tenant?x=1"),
        ] {
            let storage = S::new(&format!("{}/{raw}", root.display()), namespace).unwrap();
            assert_eq!(
                storage.uri(),
                format!("file://{}/{expected}", root.display())
            );
        }
        for raw in ["relative%zz", "relative%"] {
            assert!(S::new(raw, "").is_err());
        }
        assert!(
            S::new("s3:bucket", "tenant")
                .err()
                .unwrap()
                .contains("please specify the bucket")
        );
        assert_eq!(
            S::new("noop:opaque", "tenant").unwrap().uri(),
            "noop:///"
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    /// Go `TestExtStorage` (`pkg/planner/extstore/extstore_test.go`).
    #[test]
    fn test_ext_storage() {
        let base = std::env::temp_dir().join(format!("o15-extstore-{}", uuid::Uuid::new_v4()));
        let storage =
            S::new(&format!("file://{}", base.display()), "test_namespace").unwrap();
        assert!(storage.uri().contains(base.to_str().unwrap()));
        assert!(storage.uri().contains("test_namespace"));
        storage.write_file("test_file.txt", b"hello world").unwrap();
        assert_eq!(storage.read_file("test_file.txt").unwrap(), b"hello world");
        assert!(storage.file_exists("test_file.txt").unwrap());
        assert_eq!(storage.walk_dir("").unwrap(), ["test_file.txt"]);
        assert_eq!(
            storage
                .open("test_file.txt")
                .unwrap()
                .get_file_size()
                .unwrap(),
            11
        );
        storage.delete_file("test_file.txt").unwrap();
        assert!(!storage.file_exists("test_file.txt").unwrap());
        let mut writer = storage.create(Path::new("test_writer.txt")).unwrap();
        writer.write(b"test writer").unwrap();
        writer.close().unwrap();
        assert_eq!(
            storage.read_file("test_writer.txt").unwrap(),
            b"test writer"
        );
        let mut data = [0; 11];
        storage
            .open("test_writer.txt")
            .unwrap()
            .read_exact(&mut data)
            .unwrap();
        assert_eq!(&data, b"test writer");
        storage
            .rename("test_writer.txt", "test_writer_renamed.txt")
            .unwrap();
        assert!(!storage.file_exists("test_writer.txt").unwrap());
        assert!(storage.file_exists("test_writer_renamed.txt").unwrap());
        storage.write_file("file1", b"1").unwrap();
        storage.write_file("file2", b"2").unwrap();
        storage.delete_files(&["file1", "file2"]).unwrap();
        assert!(!storage.file_exists("file1").unwrap());
        assert!(!storage.file_exists("file2").unwrap());
        std::fs::remove_dir_all(base).unwrap();
    }

    struct RestoreConfig(
        std::sync::Arc<tidb_config::config_tree::config::Config>,
        String,
    );
    impl Drop for RestoreConfig {
        fn drop(&mut self) {
            tidb_config::config_tree::config::store_global_config(self.0.clone());
            *tidb_vardef::CLOUD_STORAGE_URI.write().unwrap() = self.1.clone();
            set_global_ext_storage_for_test(None);
        }
    }

    /// Go `TestGetLocalPathDirNameWithWritePerm`.
    #[test]
    fn test_get_local_path_dir_name_with_write_perm() {test_local_path(true);}

    /// Go `TestGetLocalPathDirNameWithoutWritePerm`.
    #[test]
    fn test_get_local_path_dir_name_without_write_perm() {test_local_path(false);}

    /// Go extstore local-directory fixtures, using isolated native directories.
    fn test_local_path(writable:bool) {
        use tidb_config::config_tree::config::{get_global_config,update_global};
        use std::os::unix::fs::PermissionsExt;
        let _restore=RestoreConfig(get_global_config(),tidb_vardef::CLOUD_STORAGE_URI.read().unwrap().clone());
        let base=std::env::temp_dir().join(format!("o15-local-path-{}",uuid::Uuid::new_v4()));
        let log=base.join("log");let temp=base.join("tmp");let replayer=log.join("replayer");
        std::fs::create_dir_all(&replayer).unwrap();
        if !writable {std::fs::set_permissions(&replayer,std::fs::Permissions::from_mode(0o555)).unwrap();}
        update_global(|config|{config.log.file.filename=log.join("tidb.log").to_string_lossy().into_owned();config.temp_dir=temp.to_string_lossy().into_owned();});
        let actual=get_local_path_dir_name();
        std::fs::set_permissions(&replayer,std::fs::Permissions::from_mode(0o755)).unwrap();
        std::fs::remove_dir_all(base).unwrap();
        assert_eq!(actual,if writable{log}else{temp});
    }

    /// Go `TestGetGlobalExtStorageWithWritePerm`.
    #[test]
    fn test_get_global_ext_storage_with_write_perm() {
        test_global_storage(true);
    }

    /// Go `TestGetGlobalExtStorageWithoutWritePerm`.
    #[test]
    fn test_get_global_ext_storage_without_write_perm() {
        test_global_storage(false);
    }

    /// Go `TestGetGlobalExtStorageWithWritePerm` / `WithoutWritePerm` setup.
    fn test_global_storage(writable: bool) {
        use tidb_config::config_tree::config::{get_global_config, update_global};
        let _restore = RestoreConfig(
            get_global_config(),
            tidb_vardef::CLOUD_STORAGE_URI.read().unwrap().clone(),
        );
        let base = std::env::temp_dir().join(format!("o15-extstore-{}", uuid::Uuid::new_v4()));
        let log = base.join("log");
        let temp = base.join("tmp");
        if writable {
            std::fs::create_dir_all(log.join("replayer")).unwrap();
        }
        update_global(|config| {
            config.log.file.filename = log.join("tidb.log").to_string_lossy().into_owned();
            config.temp_dir = temp.to_string_lossy().into_owned();
        });
        *tidb_vardef::CLOUD_STORAGE_URI.write().unwrap() = String::new();
        set_global_ext_storage_for_test(None);
        let storage = get_global_ext_storage().unwrap();
        assert!(storage.uri().starts_with(&format!(
            "file://{}",
            if writable { &log } else { &temp }.display()
        )));
        assert!(Arc::ptr_eq(&storage, &get_global_ext_storage().unwrap()));
        std::fs::remove_dir_all(base).unwrap();
    }
}



mod local_tests {
    use super::*;

    /// Go `canWriteToReplayerDirFile` needs an existing `replayer`
    /// subdirectory; it leaves no probe file behind.
    #[test]
    fn the_replayer_dir_probe_needs_an_existing_subdirectory() {
        let base = std::env::temp_dir().join(format!("extstore-probe-{}", std::process::id()));
        std::fs::create_dir_all(&base).unwrap();
        assert!(!can_write_to_replayer_dir_file(&base));
        std::fs::create_dir_all(base.join("replayer")).unwrap();
        assert!(can_write_to_replayer_dir_file(&base));
        assert!(std::fs::read_dir(base.join("replayer")).unwrap().next().is_none());
        std::fs::remove_dir_all(&base).unwrap();
    }
}
