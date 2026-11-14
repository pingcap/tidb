// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `metering_sdk/config`.

/// Go `MeteringAWSConfig` and `MeteringOSSConfig` credential fields.
#[derive(Clone, Default)]
pub struct CloudCredentials {
    pub assume_role_arn: String,
    pub s3_force_path_style: bool,
    pub access_key: String,
    pub secret_access_key: String,
    pub session_token: String,
}
/// Go `MeteringAzureConfig`.
#[derive(Clone, Default)]
pub struct MeteringAzureConfig {
    pub account_name: String,
    pub account_key: String,
    pub sas_token: String,
}
/// Go `MeteringLocalFSConfig`.
#[derive(Clone, Default)]
pub struct MeteringLocalFSConfig {
    pub base_path: String,
    pub create_dirs: bool,
    pub permissions: String,
}
/// Go `MeteringConfig`. Credential-bearing configurations deliberately omit Debug.
#[derive(Clone, Default)]
pub struct MeteringConfig {
    pub tp: String,
    pub region: String,
    pub bucket: String,
    pub prefix: String,
    pub endpoint: String,
    pub shared_pool_id: String,
    pub aws: Option<CloudCredentials>,
    pub oss: Option<CloudCredentials>,
    pub azure: Option<MeteringAzureConfig>,
    pub local_fs: Option<MeteringLocalFSConfig>,
}

/// Go `NewFromURI`. Unlike objstore.ParseRawURL, SDK query '+' means space.
pub fn new_from_uri(uri: &str) -> Result<MeteringConfig, String> {
    let parsed = url::Url::parse(uri).map_err(|error| {
        format!(
            "failed to parse URI: {}: {error}",
            tidb_ast::redact_url(uri)
        )
    })?;
    let tp = match parsed.scheme().to_ascii_lowercase().as_str() {
        "s3" => "s3",
        "oss" => "oss",
        "azure" | "azblob" => "azure",
        "localfs" | "file" => "localfs",
        _ => return Err(format!("unsupported URI scheme: {}", parsed.scheme())),
    };
    let path = percent_encoding::percent_decode_str(parsed.path()).decode_utf8_lossy();
    let host = parsed.host_str().unwrap_or("");
    let mut result = MeteringConfig {
        tp: tp.into(),
        ..Default::default()
    };
    if tp == "localfs" {
        let base_path = if host.is_empty() {
            path.into_owned()
        } else if path.is_empty() || path == "/" {
            format!("/{host}")
        } else {
            format!("/{host}/{}", path.strip_prefix('/').unwrap_or(&path))
        };
        result.local_fs = Some(MeteringLocalFSConfig {
            base_path,
            create_dirs: true,
            permissions: String::new(),
        });
    } else {
        result.bucket = host.into();
        result.prefix = path.strip_prefix('/').unwrap_or(&path).into();
    }
    let query: Vec<_> = parsed.query_pairs().collect();
    let get = |name: &str| {
        query
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.to_string())
            .unwrap_or_default()
    };
    let first = |a, b| {
        let a = get(a);
        if a.is_empty() {
            get(b)
        } else {
            a
        }
    };
    result.region = first("region-id", "region");
    if !get("prefix").is_empty() {
        result.prefix = get("prefix");
    }
    result.endpoint = get("endpoint");
    result.shared_pool_id = get("shared-pool-id");
    match tp {
        "s3" | "oss" => {
            let value = CloudCredentials {
                access_key: get("access-key"),
                secret_access_key: get("secret-access-key"),
                session_token: get("session-token"),
                assume_role_arn: first("assume-role-arn", "role-arn"),
                s3_force_path_style: tp == "s3"
                    && first("s3-force-path-style", "force-path-style") == "true",
            };
            if !value.access_key.is_empty()
                || !value.secret_access_key.is_empty()
                || !value.session_token.is_empty()
                || !value.assume_role_arn.is_empty()
                || value.s3_force_path_style
            {
                if tp == "s3" {
                    result.aws = Some(value);
                } else {
                    result.oss = Some(value);
                }
            }
        }
        "azure" => {
            let value = MeteringAzureConfig {
                account_name: get("account-name"),
                account_key: get("account-key"),
                sas_token: get("sas-token"),
            };
            if !value.account_name.is_empty()
                || !value.account_key.is_empty()
                || !value.sas_token.is_empty()
            {
                result.azure = Some(value);
            }
        }
        "localfs" => {
            let value = result.local_fs.as_mut().unwrap();
            value.create_dirs = get("create-dirs") != "false";
            value.permissions = get("permissions");
        }
        _ => unreachable!(),
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    /// Go SDK `TestNewFromURI`.
    #[test]
    fn test_new_from_uri() {
        let c = new_from_uri("s3://test-bucket/data?region-id=us-west-2&access-key=AKSKEXAMPLE&secret-access-key=AK/SK/EXAMPLEKEY").unwrap();
        assert_eq!(
            (&*c.tp, &*c.bucket, &*c.region, &*c.prefix),
            ("s3", "test-bucket", "us-west-2", "data")
        );
        assert_eq!(c.aws.as_ref().unwrap().access_key, "AKSKEXAMPLE");
        assert_eq!(
            c.aws.as_ref().unwrap().secret_access_key,
            "AK/SK/EXAMPLEKEY"
        );
        assert!(c.oss.is_none() && c.azure.is_none() && c.local_fs.is_none());
        let c = new_from_uri("s3://my-bucket/logs?region-id=us-east-1&assume-role-arn=arn:aws:iam::123456789012:role/TestRole&s3-force-path-style=true&session-token=token123&shared-pool-id=pool-001").unwrap();
        assert_eq!(c.shared_pool_id, "pool-001");
        let aws = c.aws.unwrap();
        assert_eq!(
            aws.assume_role_arn,
            "arn:aws:iam::123456789012:role/TestRole"
        );
        assert!(aws.s3_force_path_style);
        assert_eq!(aws.session_token, "token123");
        let c = new_from_uri("oss://my-bucket/data/metering/tmp?region-id=oss-ap-southeast-1&access-key=AKSKEXAMPLE&secret-access-key=AK/SK/EXAMPLEKEY&session-token=STS.token").unwrap();
        assert_eq!(c.tp, "oss");
        assert_eq!(c.prefix, "data/metering/tmp");
        assert_eq!(c.oss.unwrap().session_token, "STS.token");
        for scheme in ["azure", "azblob"] {
            let c = new_from_uri(&format!("{scheme}://my-container/prefix?account-name=testacct&account-key=testkey&endpoint=https://testacct.blob.core.windows.net")).unwrap();
            assert_eq!(c.tp, "azure");
            assert_eq!(c.bucket, "my-container");
            assert_eq!(c.prefix, "prefix");
            assert_eq!(c.endpoint, "https://testacct.blob.core.windows.net");
            let azure = c.azure.unwrap();
            assert_eq!(azure.account_name, "testacct");
            assert_eq!(azure.account_key, "testkey");
        }
        let c = new_from_uri("localfs:///data/storage/logs?create-dirs=false&permissions=0644")
            .unwrap();
        assert!(c.bucket.is_empty());
        let local = c.local_fs.unwrap();
        assert_eq!(local.base_path, "/data/storage/logs");
        assert!(!local.create_dirs);
        assert_eq!(local.permissions, "0644");
        for uri in ["invalid://test/bucket", "://invalid"] {
            assert!(new_from_uri(uri).is_err());
        }
    }
    /// Go SDK `TestNewFromURI_ParameterAliases`.
    #[test]
    fn test_new_from_uri_parameter_aliases() {
        for scheme in ["s3", "oss"] {
            let c = new_from_uri(&format!("{scheme}://bucket/data?region=alias&region-id=primary&role-arn=alias&assume-role-arn=primary&force-path-style=true&prefix=override")).unwrap();
            assert_eq!(c.region, "primary");
            assert_eq!(c.prefix, "override");
            let credentials = c.aws.or(c.oss).unwrap();
            assert_eq!(credentials.assume_role_arn, "primary");
            assert_eq!(credentials.s3_force_path_style, scheme == "s3");
        }
        let c =
            new_from_uri("s3://bucket?region=alias&role-arn=alias&force-path-style=true").unwrap();
        assert_eq!(c.region, "alias");
        let credentials = c.aws.unwrap();
        assert_eq!(credentials.assume_role_arn, "alias");
        assert!(credentials.s3_force_path_style);
    }
    /// Go SDK `TestNewFromURI_PathConstruction`.
    #[test]
    fn test_new_from_uri_path_construction() {
        for (uri, path) in [
            ("localfs://data/logs", "/data/logs"),
            ("localfs://data/storage/logs", "/data/storage/logs"),
            ("localfs://data", "/data"),
            ("localfs://data/", "/data"),
            ("localfs:///data/storage/logs", "/data/storage/logs"),
            ("file:///logs?create-dirs=true", "/logs"),
        ] {
            let c = new_from_uri(uri).unwrap();
            let local = c.local_fs.unwrap();
            assert_eq!(local.base_path, path);
            assert!(local.create_dirs);
        }
    }
}
