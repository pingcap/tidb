// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `objstore/gcs.go`: credential files, ADC and namespaced GCS storage.
use super::{cloud::RUNTIME, object_storage::ObjectStorage};
use std::sync::Arc;
const SCOPE: &str = "https://www.googleapis.com/auth/devstorage.read_write";
#[derive(serde::Deserialize)]
struct AuthorizedUser {
    client_id: String,
    client_secret: String,
    refresh_token: String,
    #[serde(default)]
    token_uri: String,
}
impl std::fmt::Debug for AuthorizedUser {
    /// Go `google.credentialsFile`: Rust debug output redacts credential fields.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("AuthorizedUser { credentials: [REDACTED] }")
    }
}
#[async_trait::async_trait]
impl reqsign::GoogleTokenLoad for AuthorizedUser {
    /// Go `google.credentialsFile.tokenSource` authorized_user / OAuth2 refresh.
    async fn load(
        &self,
        client: reqwest12::Client,
    ) -> anyhow::Result<Option<reqsign::GoogleToken>> {
        let endpoint = if self.token_uri.is_empty() {
            "https://oauth2.googleapis.com/token"
        } else {
            &self.token_uri
        };
        let response = client
            .post(endpoint)
            .form(&[
                ("grant_type", "refresh_token"),
                ("client_id", &self.client_id),
                ("client_secret", &self.client_secret),
                ("refresh_token", &self.refresh_token),
            ])
            .send()
            .await
            .map_err(|_| anyhow::anyhow!("GCS credential refresh failed"))?;
        if !response.status().is_success() {
            return Err(anyhow::anyhow!(
                "GCS credential refresh: HTTP {}",
                response.status().as_u16()
            ));
        }
        #[derive(serde::Deserialize)]
        struct Token {
            access_token: String,
            expires_in: usize,
        }
        let bytes = response
            .bytes()
            .await
            .map_err(|_| anyhow::anyhow!("invalid GCS credential response"))?;
        let token: Token = serde_json::from_slice(&bytes)
            .map_err(|_| anyhow::anyhow!("invalid GCS credential response"))?;
        Ok(Some(reqsign::GoogleToken::new(
            &token.access_token,
            token.expires_in,
            SCOPE,
        )))
    }
}
/// Go `NewGCSStorage`: validate credential JSON eagerly, load tokens on demand.
pub(super) fn new(url: &url::Url, prefix: &str) -> Result<Arc<ObjectStorage>, String> {
    let options = super::cloud::query_options(url);
    let get = |key: &str| options.get(key).map_or("", String::as_str);
    let bucket = url.host_str().unwrap_or("");
    if bucket.is_empty() {
        return Err("please specify the bucket for gcs".into());
    }
    let explicit = get("credentials-file");
    let credential_path = if !explicit.is_empty() {
        Some(std::path::PathBuf::from(explicit))
    } else if let Some(path) = std::env::var("GOOGLE_APPLICATION_CREDENTIALS")
        .ok()
        .filter(|s| !s.is_empty())
    {
        Some(path.into())
    } else {
        std::env::var_os("HOME")
            .map(std::path::PathBuf::from)
            .map(|p| p.join(".config/gcloud/application_default_credentials.json"))
            .filter(|p| p.is_file())
    };
    let _guard = RUNTIME.enter();
    let mut builder = opendal::services::Gcs::default()
        .bucket(bucket)
        .scope(SCOPE);
    if !get("endpoint").is_empty() {
        builder = builder.endpoint(get("endpoint"));
    }
    if !get("storage-class").is_empty() {
        builder = builder.default_storage_class(get("storage-class"));
    }
    if !get("predefined-acl").is_empty() {
        builder = builder.predefined_acl(get("predefined-acl"));
    }
    if let Some(path) = credential_path {
        let bytes =
            std::fs::read(path).map_err(|_| "cannot read GCS credentials file".to_owned())?;
        let value: serde_json::Value = serde_json::from_slice(&bytes)
            .map_err(|_| "invalid GCS credentials JSON".to_owned())?;
        if value["type"] == "authorized_user" {
            let user: AuthorizedUser = serde_json::from_value(value)
                .map_err(|_| "invalid GCS authorized_user credentials".to_owned())?;
            builder = builder
                .disable_config_load()
                .customized_token_loader(Box::new(user));
        } else {
            reqsign::GoogleCredential::from_slice(&bytes)
                .map_err(|_| "invalid GCS credentials JSON".to_owned())?;
            use base64::Engine;
            builder = builder.credential(&base64::engine::general_purpose::STANDARD.encode(bytes));
        }
    } else {
        // Go FindDefaultCredentials selects metadata only on Compute Engine.
        // Keep metadata probing bounded; no credential material enters errors.
        let on_gce = RUNTIME.block_on(async {
            let endpoint = std::env::var("GCE_METADATA_HOST")
                .map(|host| format!("http://{host}/"))
                .unwrap_or_else(|_| "http://169.254.169.254/".into());
            reqwest12::Client::new()
                .get(endpoint)
                .header("Metadata-Flavor", "Google")
                .timeout(std::time::Duration::from_millis(500))
                .send()
                .await
                .is_ok_and(|r| {
                    r.headers()
                        .get("Metadata-Flavor")
                        .is_some_and(|v| v == "Google")
                })
        });
        if !on_gce {
            return Err("could not find default credentials; provide credentials-file".into());
        }
    }
    let operator = opendal::Operator::new(builder)
        .map_err(|e| super::cloud::native_error(e).to_string())?
        .finish();
    Ok(Arc::new(ObjectStorage {
        credentials: None,
        operator,
        presigner: None,
        scheme: "gcs",
        bucket: bucket.into(),
        prefix: prefix.trim_matches('/').into(),
    }))
}

#[cfg(test)]
mod tests {
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
        let storage = crate::new_ext_storage(&uri, "tenant").unwrap();
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
