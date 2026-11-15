//! Go `pkg/objstore/parse_test.go`.

use super::*;

fn s3_of(backend: StorageBackend) -> S3 {
    match backend.backend {
        Some(Backend::S3(s3)) => s3,
        _ => panic!("not an S3 backend"),
    }
}

fn gcs_of(backend: StorageBackend) -> Gcs {
    match backend.backend {
        Some(Backend::Gcs(gcs)) => gcs,
        _ => panic!("not a GCS backend"),
    }
}

fn azblob_of(backend: StorageBackend) -> AzureBlobStorage {
    match backend.backend {
        Some(Backend::AzureBlobStorage(azblob)) => azblob,
        _ => panic!("not an Azure Blob backend"),
    }
}

fn local_path(backend: &StorageBackend) -> &str {
    match &backend.backend {
        Some(Backend::Local(local)) => &local.path,
        _ => panic!("not a local backend"),
    }
}

/// Go `TestCreateStorage`.
#[test]
fn test_create_storage() {
    let err = parse_backend("1invalid:", None).unwrap_err();
    assert_eq!(
        err,
        r#"parse "1invalid:": first path segment in URL cannot contain colon"#
    );
    let err = parse_backend("net:storage", None).unwrap_err();
    assert!(err.starts_with("storage net not support yet"), "{err}");

    let s = parse_backend("local:///tmp/storage", None).unwrap();
    assert_eq!(local_path(&s), "/tmp/storage");
    let s = parse_backend("file:///tmp/storage", None).unwrap();
    assert_eq!(local_path(&s), "/tmp/storage");
    let s = parse_backend("noop://", None).unwrap();
    assert!(matches!(s.backend, Some(Backend::Noop(_))));
    let s = parse_backend("hdfs://127.0.0.1:1231/backup", None).unwrap();
    assert!(
        matches!(s.backend, Some(Backend::Hdfs(h)) if h.remote == "hdfs://127.0.0.1:1231/backup")
    );

    let err = parse_backend(
        "s3:///bucket/more/prefix/",
        Some(&BackendOptions::default()),
    )
    .unwrap_err();
    assert!(err.starts_with("please specify the bucket for s3 in s3:///bucket/more/prefix/"));

    let s3opt = BackendOptions {
        s3: S3BackendOptions {
            endpoint: "https://s3.example.com/".into(),
            ..Default::default()
        },
        ..Default::default()
    };
    let s3 = s3_of(parse_backend("s3://bucket2/prefix/", Some(&s3opt)).unwrap());
    assert_eq!(
        (s3.bucket.as_str(), s3.prefix.as_str()),
        ("bucket2", "prefix")
    );
    assert_eq!(s3.endpoint, "https://s3.example.com");
    assert!(!s3.force_path_style);

    let s3 = s3_of(parse_backend("ks3://bucket2/prefix/", Some(&s3opt)).unwrap());
    assert_eq!(
        (s3.bucket.as_str(), s3.prefix.as_str()),
        ("bucket2", "prefix")
    );
    assert_eq!(s3.endpoint, "https://s3.example.com");
    assert_eq!(s3.provider, tidb_objstore_s3like::KS3_SDK_PROVIDER);
    assert!(!s3.force_path_style);

    let s3 = s3_of(
        parse_backend(
            "s3://bucket3/prefix/path?endpoint=https://127.0.0.1:9000&force_path_style=0&SSE=aws:kms&sse-kms-key-id=TestKey&xyz=abc",
            None,
        )
        .unwrap(),
    );
    assert_eq!(
        (s3.bucket.as_str(), s3.prefix.as_str()),
        ("bucket3", "prefix/path")
    );
    assert_eq!(s3.endpoint, "https://127.0.0.1:9000");
    assert!(!s3.force_path_style);
    assert_eq!(s3.sse, "aws:kms");
    assert_eq!(s3.sse_kms_key_id, "TestKey");

    // special character in access keys
    let s3 = s3_of(
        parse_backend(
            "s3://bucket4/prefix/path?access-key=******&secret-access-key=******+&session-token=******",
            None,
        )
        .unwrap(),
    );
    assert_eq!(
        (s3.bucket.as_str(), s3.prefix.as_str()),
        ("bucket4", "prefix/path")
    );
    assert_eq!(s3.access_key, "******");
    assert_eq!(s3.secret_access_key, "******+");
    assert_eq!(s3.session_token, "******");
    assert!(s3.force_path_style);

    // parse role ARN and external ID
    let role = "arn:aws:iam::888888888888:role/my-role";
    let raw = format!(
        "s3://bucket5/prefix/path?role-arn={}&external-id={}",
        String::from_utf8(tidb_go_url::query_escape(role.as_bytes())).unwrap(),
        "abcd1234"
    );
    let s3 = s3_of(parse_backend(&raw, None).unwrap());
    assert_eq!(
        (s3.bucket.as_str(), s3.prefix.as_str()),
        ("bucket5", "prefix/path")
    );
    assert_eq!(s3.role_arn, role);
    assert_eq!(s3.external_id, "abcd1234");

    let mut gcs_opt = BackendOptions {
        gcs: GCSBackendOptions {
            endpoint: "https://gcs.example.com/".into(),
            ..Default::default()
        },
        ..Default::default()
    };
    let gcs = gcs_of(parse_backend("gcs://bucket2/prefix/", Some(&gcs_opt)).unwrap());
    assert_eq!(
        (gcs.bucket.as_str(), gcs.prefix.as_str()),
        ("bucket2", "prefix")
    );
    assert_eq!(gcs.endpoint, "https://gcs.example.com/");
    assert_eq!(gcs.credentials_blob, "");
    let gcs = gcs_of(parse_backend("gcs://bucket2", Some(&gcs_opt)).unwrap());
    assert_eq!((gcs.bucket.as_str(), gcs.prefix.as_str()), ("bucket2", ""));
    assert_eq!(gcs.endpoint, "https://gcs.example.com/");

    let dir = tempfile::tempdir().unwrap();
    let cred = dir.path().join("fakeCredentialsFile");
    std::fs::write(&cred, "fakeCredentials").unwrap();
    gcs_opt.gcs.credentials_file = cred.to_str().unwrap().into();
    let gcs = gcs_of(parse_backend("gcs://bucket/more/prefix/", Some(&gcs_opt)).unwrap());
    assert_eq!(
        (gcs.bucket.as_str(), gcs.prefix.as_str()),
        ("bucket", "more/prefix")
    );
    assert_eq!(gcs.endpoint, "https://gcs.example.com/");
    assert_eq!(gcs.credentials_blob, "fakeCredentials");
    let gcs =
        gcs_of(parse_backend("gcs://bucket?endpoint=http://127.0.0.1/", Some(&gcs_opt)).unwrap());
    assert_eq!(gcs.endpoint, "http://127.0.0.1/");

    std::fs::write(&cred, "fakeCreds2").unwrap();
    let raw = format!(
        "gs://bucket4/backup/?credentials-file={}",
        String::from_utf8(tidb_go_url::query_escape(cred.to_str().unwrap().as_bytes())).unwrap()
    );
    let gcs = gcs_of(parse_backend(&raw, None).unwrap());
    assert_eq!(
        (gcs.bucket.as_str(), gcs.prefix.as_str()),
        ("bucket4", "backup")
    );
    assert_eq!(gcs.credentials_blob, "fakeCreds2");

    let azblob = azblob_of(
        parse_backend(
            "azure://bucket1/prefix/path?account-name=user&account-key=cGFzc3dk&endpoint=http://127.0.0.1/user",
            None,
        )
        .unwrap(),
    );
    assert_eq!(
        (azblob.bucket.as_str(), azblob.prefix.as_str()),
        ("bucket1", "prefix/path")
    );
    assert_eq!(azblob.endpoint, "http://127.0.0.1/user");
    assert_eq!(azblob.account_name, "user");
    assert_eq!(azblob.shared_key, "cGFzc3dk");

    let s = parse_backend("/test", None).unwrap();
    assert_eq!(local_path(&s), "/test");
}

/// Go `TestFormatBackendURL`.
#[test]
fn test_format_backend_url() {
    let url = |b| format_backend_url(&backend(b)).string();
    assert_eq!(
        url(Backend::Local(Local {
            path: "/tmp/file".into()
        })),
        "local:///tmp/file"
    );
    assert_eq!(url(Backend::Noop(Noop {})), "noop:///");
    let s3 = S3 {
        bucket: "bucket".into(),
        prefix: "/some prefix/".into(),
        endpoint: "https://s3.example.com/".into(),
        ..S3::default()
    };
    assert_eq!(url(Backend::S3(s3)), "s3://bucket/some%20prefix/");
    let gcs = Gcs {
        bucket: "bucket".into(),
        prefix: "/some prefix/".into(),
        endpoint: "https://gcs.example.com/".into(),
        ..Gcs::default()
    };
    assert_eq!(url(Backend::Gcs(gcs)), "gcs://bucket/some%20prefix/");
    let azblob = AzureBlobStorage {
        bucket: "bucket".into(),
        prefix: "/some prefix/".into(),
        endpoint: "https://azure.example.com/".into(),
        ..AzureBlobStorage::default()
    };
    assert_eq!(
        url(Backend::AzureBlobStorage(azblob)),
        "azure://bucket/some%20prefix/"
    );
}

/// Go `TestParseRawURL`: a `+` in a secret survives (the Go fixture's
/// fake keys are replaced by equally fake ones).
#[test]
fn test_parse_raw_url() {
    for (raw, secret) in [
        (
            "s3://bucket/prefix/path?access-key=FAKEACCESSKEY&secret-access-key=fake/secret=key",
            "fake/secret=key",
        ),
        (
            "s3://bucket/prefix/path?access-key=FAKEACCESSKEY&secret-access-key=fake+secret/key+",
            "fake+secret/key+",
        ),
    ] {
        let u = parse_raw_url(raw).unwrap();
        assert_eq!(u.scheme, b"s3");
        assert_eq!(u.host, b"bucket");
        assert_eq!(u.path, b"/prefix/path");
        let query = u.query();
        assert_eq!(
            query.0[b"access-key".as_slice()],
            [b"FAKEACCESSKEY".to_vec()]
        );
        assert_eq!(
            query.0[b"secret-access-key".as_slice()],
            [secret.as_bytes().to_vec()]
        );
    }
}

/// Go `TestIsLocal`.
#[test]
fn test_is_local() {
    let err = is_local_path(":").unwrap_err().to_string();
    assert!(err.contains("missing protocol scheme"), "{err}");
    for (path, want) in [
        ("~/tmp/file", true),
        (".", true),
        ("..", true),
        ("./tmp/file", true),
        ("/tmp/file", true),
        ("local:///tmp/file", true),
        ("file:///tmp/file", true),
        ("s3://bucket/tmp/file", false),
    ] {
        assert_eq!(is_local_path(path).unwrap(), want, "{path}");
    }
}

/// Go `TestS3ProfileOption`.
#[test]
fn test_s3_profile_option() {
    let s3 = s3_of(parse_backend("s3://bucket/prefix/?profile=my-test-profile", None).unwrap());
    assert_eq!(
        (s3.bucket.as_str(), s3.prefix.as_str()),
        ("bucket", "prefix")
    );
    assert_eq!(s3.profile, "my-test-profile");
    let opt = BackendOptions {
        s3: S3BackendOptions {
            profile: "profile-from-options".into(),
            ..Default::default()
        },
        ..Default::default()
    };
    let s3 = s3_of(parse_backend("s3://bucket2/prefix/", Some(&opt)).unwrap());
    assert_eq!(s3.profile, "profile-from-options");
    let s3 = s3_of(
        parse_backend(
            "s3://bucket3/prefix/?profile=dev-profile&region=us-west-2&endpoint=https://s3.example.com",
            None,
        )
        .unwrap(),
    );
    assert_eq!(s3.profile, "dev-profile");
    assert_eq!(s3.region, "us-west-2");
    assert_eq!(s3.endpoint, "https://s3.example.com");
    let s3 = s3_of(parse_backend("s3://bucket4/prefix/", None).unwrap());
    assert_eq!(s3.profile, "");
}

/// Go `TestS3ProfileCredentialsValidation`.
#[test]
fn test_s3_profile_credentials_validation() {
    let s3 = s3_of(
        parse_backend(
            "s3://bucket/prefix/?profile=production&region=us-west-2",
            None,
        )
        .unwrap(),
    );
    assert_eq!(
        (s3.profile.as_str(), s3.region.as_str()),
        ("production", "us-west-2")
    );
    assert_eq!(
        (s3.access_key.as_str(), s3.secret_access_key.as_str()),
        ("", "")
    );
    let s3 = s3_of(
        parse_backend(
            "s3://bucket/prefix/?profile=dev&access-key=override-key",
            None,
        )
        .unwrap(),
    );
    assert_eq!(
        (s3.profile.as_str(), s3.access_key.as_str()),
        ("dev", "override-key")
    );
    assert_eq!(s3.secret_access_key, "");
    let s3 = s3_of(
        parse_backend(
            "s3://bucket/prefix/?profile=staging&access-key=explicit-access&secret-access-key=explicit-secret",
            None,
        )
        .unwrap(),
    );
    assert_eq!(s3.profile, "staging");
    assert_eq!(
        (s3.access_key.as_str(), s3.secret_access_key.as_str()),
        ("explicit-access", "explicit-secret")
    );
}

/// Go `TestS3NoProfileCredentialsValidation`.
#[test]
fn test_s3_no_profile_credentials_validation() {
    let s3_options = |access_key: &str, secret_access_key: &str, region: &str| BackendOptions {
        s3: S3BackendOptions {
            access_key: access_key.into(),
            secret_access_key: secret_access_key.into(),
            region: region.into(),
            ..Default::default()
        },
        ..Default::default()
    };
    let err = parse_backend(
        "s3://bucket/prefix/",
        Some(&s3_options("only-access-key", "", "")),
    )
    .unwrap_err();
    assert!(err.contains("secret_access_key not found"), "{err}");
    let s3 = s3_of(
        parse_backend(
            "s3://bucket/prefix/",
            Some(&s3_options("test-access", "test-secret", "")),
        )
        .unwrap(),
    );
    assert_eq!(
        (s3.access_key.as_str(), s3.secret_access_key.as_str()),
        ("test-access", "test-secret")
    );
    let s3 = s3_of(
        parse_backend(
            "s3://bucket/prefix/",
            Some(&s3_options("", "", "us-east-1")),
        )
        .unwrap(),
    );
    assert_eq!(
        (s3.access_key.as_str(), s3.secret_access_key.as_str()),
        ("", "")
    );
}

const S3_URL: &str = "s3://bucket3/prefix/path?endpoint=https://127.0.0.1:9000&force_path_style=0&sse-kms-key-id=TestKey&xyz=abc";
const GCS_URL: &str = "gcs://bucket?endpoint=http://127.0.0.1&predefined-acl=1234";
const AZURE_URL: &str = "azure://bucket1/prefix/path?account-name=user&account-key=cGFzc3dk&endpoint=http://127.0.0.1/user&encryption-scope=test";

/// Go `TestParseBackend`: the caller's options are copied, never changed,
/// and the URL's query fills the copy.
#[test]
fn test_parse_backend() {
    let options = BackendOptions::default();
    parse_backend(S3_URL, Some(&options)).unwrap();
    parse_backend(GCS_URL, Some(&options)).unwrap();
    parse_backend(AZURE_URL, Some(&options)).unwrap();
    assert!(options == BackendOptions::default());

    let options = BackendOptions {
        s3: S3BackendOptions {
            storage_class: "test".into(),
            ..Default::default()
        },
        gcs: GCSBackendOptions {
            storage_class: "test".into(),
            ..Default::default()
        },
        azblob: AzblobBackendOptions {
            access_tier: "test".into(),
            ..Default::default()
        },
    };
    let before = options.clone();
    let s3 = s3_of(parse_backend(S3_URL, Some(&options)).unwrap());
    assert_eq!(
        s3,
        S3 {
            endpoint: "https://127.0.0.1:9000".into(),
            bucket: "bucket3".into(),
            prefix: "prefix/path".into(),
            storage_class: "test".into(),
            sse_kms_key_id: "TestKey".into(),
            ..S3::default()
        }
    );
    let gcs = gcs_of(parse_backend(GCS_URL, Some(&options)).unwrap());
    assert_eq!(
        gcs,
        Gcs {
            endpoint: "http://127.0.0.1".into(),
            bucket: "bucket".into(),
            storage_class: "test".into(),
            predefined_acl: "1234".into(),
            ..Gcs::default()
        }
    );
    let azblob = azblob_of(parse_backend(AZURE_URL, Some(&options)).unwrap());
    let want_azblob = |storage_class: &str| AzureBlobStorage {
        endpoint: "http://127.0.0.1/user".into(),
        bucket: "bucket1".into(),
        prefix: "prefix/path".into(),
        storage_class: storage_class.into(),
        account_name: "user".into(),
        shared_key: "cGFzc3dk".into(),
        encryption_scope: "test".into(),
        ..AzureBlobStorage::default()
    };
    assert_eq!(azblob, want_azblob("test"));
    assert!(options == before);

    let s3 = s3_of(parse_backend(S3_URL, None).unwrap());
    assert_eq!(
        s3,
        S3 {
            endpoint: "https://127.0.0.1:9000".into(),
            bucket: "bucket3".into(),
            prefix: "prefix/path".into(),
            sse_kms_key_id: "TestKey".into(),
            ..S3::default()
        }
    );
    let s3 = s3_of(
        parse_backend(
            "s3://bucket3/prefix/path?endpoint=https://127.0.0.1:9000&sse-kms-key-id=TestKey&xyz=abc",
            None,
        )
        .unwrap(),
    );
    assert_eq!(
        s3,
        S3 {
            endpoint: "https://127.0.0.1:9000".into(),
            bucket: "bucket3".into(),
            prefix: "prefix/path".into(),
            force_path_style: true,
            sse_kms_key_id: "TestKey".into(),
            ..S3::default()
        }
    );
    let gcs = gcs_of(parse_backend(GCS_URL, None).unwrap());
    assert_eq!(
        gcs,
        Gcs {
            endpoint: "http://127.0.0.1".into(),
            bucket: "bucket".into(),
            predefined_acl: "1234".into(),
            ..Gcs::default()
        }
    );
    assert_eq!(
        azblob_of(parse_backend(AZURE_URL, None).unwrap()),
        want_azblob("")
    );
}

/// Go `TestS3DefaultForceStylePath`.
#[test]
fn test_s3_default_force_style_path() {
    let force = |raw| s3_of(parse_backend(raw, None).unwrap()).force_path_style;
    assert!(!force(
        "s3://bucket3/prefix/path?endpoint=http://xxx.amazonaws.com"
    ));
    assert!(!force("s3://bucket3/prefix/path?force-path-style=false"));
    assert!(force("s3://bucket3/prefix/path?force-path-style=true"));
}

/// Go `filepath.Abs` for the empty scheme: a relative path joins the
/// working directory and the result is cleaned.
#[test]
fn local_path_without_scheme_is_absolute_and_clean() {
    let wd = std::env::current_dir().unwrap();
    let s = parse_backend("a/./b/../c", None).unwrap();
    assert_eq!(local_path(&s), wd.join("a/c").to_str().unwrap());
    let s = parse_backend("/x//y/../z/", None).unwrap();
    assert_eq!(local_path(&s), "/x/z");
}
