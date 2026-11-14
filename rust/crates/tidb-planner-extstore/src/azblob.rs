// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `objstore/azblob.go`: options, credential selection and container initialization.
use super::{cloud::RUNTIME, object_storage::ObjectStorage};
use opendal::raw::{HttpBody, HttpClient, HttpFetch};
use std::sync::Arc;
struct Transport {
    client: reqwest12::Client,
    credentials: Arc<tidb_azure_sdk::Credentials>,
    tier: String,
    scope: String,
    sas: bool,
}
impl HttpFetch for Transport {
    /// Go `AzureBlobStorage.WriteFile` / `azblobUploader.Close` upload options.
    async fn fetch(
        &self,
        mut req: http::Request<opendal::Buffer>,
    ) -> opendal::Result<http::Response<HttpBody>> {
        let error = || {
            opendal::Error::new(
                opendal::ErrorKind::Unexpected,
                "Azure request signing failed",
            )
        };
        let upload = req.method() == http::Method::PUT
            && !req
                .uri()
                .query()
                .is_some_and(|q| q.split('&').any(|p| p == "comp=block"));
        if upload && !self.tier.is_empty() {
            req.headers_mut()
                .insert("x-ms-access-tier", self.tier.parse().map_err(|_| error())?);
        }
        if upload && !self.scope.is_empty() {
            req.headers_mut().insert(
                "x-ms-encryption-scope",
                self.scope.parse().map_err(|_| error())?,
            );
        }
        // Sign after adding Go's per-upload headers. Bearer/SAS credentials use
        // the same native chain as the container initialization request.
        if !self.sas {
            if self.credentials.is_bearer() && req.uri().scheme_str() != Some("https") {
                return Err(opendal::Error::new(
                    opendal::ErrorKind::ConfigInvalid,
                    "Azure bearer credentials require HTTPS",
                ));
            }
            let cred = self.credentials.load().await.map_err(|_| error())?;
            req.headers_mut().remove(http::header::AUTHORIZATION);
            reqsign::AzureStorageSigner::new()
                .sign(&mut req, &cred)
                .map_err(|_| error())?;
        }
        let response = HttpClient::with(self.client.clone()).fetch(req).await?;
        if response.status() == http::StatusCode::UNAUTHORIZED {
            self.credentials.expire();
        }
        Ok(response)
    }
}
/// Go `newAzureBlobStorage`, including GetContainerProperties before option checks.
pub(super) fn new(url: &url::Url, prefix: &str) -> Result<Arc<ObjectStorage>, String> {
    let options = super::cloud::query_options(url);
    let get = |key: &str| options.get(key).map_or("", String::as_str);
    let bucket = url.host_str().unwrap_or("");
    if bucket.is_empty() {
        return Err("bucket(container) cannot be empty to access azure blob storage".into());
    }
    let account = if get("account-name").is_empty() {
        std::env::var("AZURE_STORAGE_ACCOUNT").unwrap_or_default()
    } else {
        get("account-name").into()
    };
    if account.is_empty() {
        return Err("account name cannot be empty to access azure blob storage".into());
    }
    let endpoint = if get("endpoint").is_empty() {
        format!("https://{account}.blob.core.windows.net")
    } else {
        get("endpoint").into()
    };
    let mut config = reqsign::AzureStorageConfig::default().from_env();
    config.account_name = Some(account.clone());
    // Go explicit SAS > explicit shared key > service principal > environment key > default identity.
    let explicit_sas = !get("account-name").is_empty() && !get("sas-token").is_empty();
    let explicit_key = !get("account-name").is_empty() && !get("account-key").is_empty();
    let complete_principal = [&config.client_id, &config.client_secret, &config.tenant_id]
        .iter()
        .all(|value| value.as_ref().is_some_and(|s| !s.is_empty()));
    // Go tries the service-principal constructor before the environment key,
    // but falls back when that constructor rejects its configuration.
    let direct = if !explicit_sas && !explicit_key && complete_principal {
        let mut principal = config.clone();
        principal.account_key = None;
        principal.sas_token = None;
        tidb_azure_sdk::Credentials::new(principal, true).ok()
    } else {
        None
    };
    let key = if explicit_key {
        get("account-key").to_owned()
    } else if direct.is_some() {
        String::new()
    } else {
        std::env::var("AZURE_STORAGE_KEY").unwrap_or_default()
    };
    config.account_key = None;
    config.sas_token = None;
    if explicit_sas {
        // Go custom SAS endpoint is used as supplied; append the SAS only for the default endpoint.
        let sas = if get("endpoint").is_empty() {
            get("sas-token").trim_start_matches('?').to_owned()
        } else {
            url::Url::parse(&endpoint)
                .ok()
                .and_then(|u| u.query().map(str::to_owned))
                .unwrap_or_default()
        };
        config.sas_token = Some(sas);
    } else if !key.is_empty() {
        config.account_key = Some(key.clone());
    }
    let sas = config.sas_token.clone();
    let credentials = Arc::new(match direct {
        Some(credential) => credential,
        None => tidb_azure_sdk::Credentials::new(config, false)?,
    });
    let client = reqwest12::Client::builder()
        .pool_max_idle_per_host(0)
        .build()
        .map_err(|_| "cannot create Azure HTTP client".to_owned())?;
    RUNTIME.block_on(async {
        if credentials.is_bearer() && !endpoint.starts_with("https://") {
            return Err("Azure bearer credentials require HTTPS".to_owned());
        }
        let cred = credentials
            .load()
            .await
            .map_err(|_| "cannot load Azure credentials".to_owned())?;
        let mut target =
            url::Url::parse(&endpoint).map_err(|_| "invalid Azure endpoint".to_owned())?;
        target.set_path(&format!("{}/{bucket}", target.path().trim_end_matches('/')));
        target.set_query(Some("restype=container"));
        let mut request = client
            .get(target)
            .header("x-ms-version", "2022-11-02")
            .build()
            .map_err(|_| "invalid Azure request".to_owned())?;
        reqsign::AzureStorageSigner::new()
            .sign(&mut request, &cred)
            .map_err(|_| "Azure signing failed".to_owned())?;
        let response = client
            .execute(request)
            .await
            .map_err(|_| "Azure container properties request failed".to_owned())?;
        if !response.status().is_success() {
            return Err(format!(
                "Azure container properties: HTTP {}",
                response.status().as_u16()
            ));
        }
        Ok(())
    })?;
    let encryption_key = if get("encryption-key").is_empty() {
        std::env::var("AZURE_ENCRYPTION_KEY").unwrap_or_default()
    } else {
        get("encryption-key").to_owned()
    };
    let scope = get("encryption-scope");
    if (!encryption_key.is_empty() || !scope.is_empty()) && !get("access-tier").is_empty() {
        return Err("Set Blob Tier cannot be used with customer-provided key/scope. Please don't supply the access-tier when use encryption-key or encryption-scope.".into());
    }
    if !encryption_key.is_empty() && !scope.is_empty() {
        return Err("Undefined input: There are both encryption-scope and customer provided key. Please select only one to encrypt blobs.".into());
    }
    let tier = match get("access-tier") {
        "archive" => "Archive",
        "cool" => "Cool",
        "hot" => "Hot",
        other => other,
    };
    let transport = Transport {
        client,
        credentials,
        tier: tier.into(),
        scope: scope.into(),
        sas: explicit_sas,
    };
    let _guard = RUNTIME.enter();
    // The Go SAS client uses endpoint query parameters as its credential.
    // Strip them from the service URL and let the native signer append them once.
    let mut data_endpoint =
        url::Url::parse(&endpoint).map_err(|_| "invalid Azure endpoint".to_owned())?;
    if explicit_sas {
        data_endpoint.set_query(None);
    }
    use opendal::Configurator;
    let mut builder = opendal::services::AzblobConfig {
        // Credentials are applied by our final transport. An empty SAS keeps
        // the preparatory native signer from consulting a second ambient chain.
        sas_token: Some(sas.unwrap_or_default()),
        ..Default::default()
    }
    .into_builder()
    .container(bucket)
    .endpoint(data_endpoint.as_str())
    .account_name(&account)
    .http_client(HttpClient::with(transport));
    if !explicit_sas && !key.is_empty() {
        builder = builder.account_key(&key);
    }
    if !encryption_key.is_empty() {
        builder = builder.server_side_encryption_with_customer_key(encryption_key.as_bytes());
    }
    let operator = opendal::Operator::new(builder)
        .map_err(|e| super::cloud::native_error(e).to_string())?
        .finish();
    Ok(Arc::new(ObjectStorage {
        credentials: None,
        operator,
        presigner: None,
        scheme: "azure",
        bucket: bucket.into(),
        prefix: prefix.trim_matches('/').into(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Storage;
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
            let storage = crate::new_ext_storage(uri.as_str(), "tenant").unwrap();
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
        let storage = crate::new_ext_storage(&uri, "tenant").unwrap();
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
            assert!(crate::new_ext_storage(&format!("{uri}{extra}"), "tenant")
                .err()
                .unwrap()
                .contains("Set Blob Tier cannot be used"));
        }
        server.join().unwrap();
    }
}

#[cfg(test)]
mod credential_tests {
    /// Go `zzprobe/o15_extstore.TestExtStorageAzureCredentialFallback`.
    #[test]
    fn test_ext_storage_azure_credential_fallback() {
        if std::env::var_os("O15_AZURE_FALLBACK_CHILD").is_none() {
            for tenant in ["", "invalid tenant"] {
                let status = std::process::Command::new(std::env::current_exe().unwrap())
                    .args([
                        "--exact",
                        "azblob::credential_tests::test_ext_storage_azure_credential_fallback",
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
        let storage = crate::new_ext_storage(
            &format!("azure://container/base?account-name=account&endpoint=http://{addr}"),
            "tenant",
        )
        .unwrap();
        storage.close();
        assert!(server.join().unwrap());
    }
}
