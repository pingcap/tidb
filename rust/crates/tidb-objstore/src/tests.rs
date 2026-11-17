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
    assert!(
        err.contains("parse storage URL failed: invalid format"),
        "{err}"
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
        "s3:///bucket/more/prefix/?access-key=secret-id&secret-access-key=secret-key&session-token=secret-token",
        Some(&BackendOptions::default()),
    )
    .unwrap_err();
    assert!(err.contains("please specify the bucket for s3 in s3:///bucket/more/prefix/?access-key=xxxxxx&secret-access-key=xxxxxx&session-token=xxxxxx"), "{err}");
    for secret in ["secret-id", "secret-key", "secret-token"] {
        assert!(!err.contains(secret), "{err}");
    }

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
    use tidb_objstore_s3like::{KS3_SDK_PROVIDER, OSS_PROVIDER};
    for (provider, scheme, parsed_provider) in [
        ("", "s3", ""),
        ("aws", "s3", ""),
        ("unknown", "s3", ""),
        (OSS_PROVIDER, "oss", OSS_PROVIDER),
        (KS3_SDK_PROVIDER, "ks3", KS3_SDK_PROVIDER),
    ] {
        let s3 = S3 {
            bucket: "bucket".into(),
            prefix: "/some prefix/".into(),
            endpoint: "https://s3.example.com/".into(),
            provider: provider.into(),
            access_key: "secret-id".into(),
            secret_access_key: "secret-key".into(),
            session_token: "secret-token".into(),
            ..S3::default()
        };
        let formatted = url(Backend::S3(s3));
        assert_eq!(formatted, format!("{scheme}://bucket/some%20prefix/"));
        let back = s3_of(parse_backend(&formatted, None).unwrap());
        assert_eq!(
            (back.bucket.as_str(), back.prefix.as_str()),
            ("bucket", "some prefix")
        );
        assert_eq!(back.provider, parsed_provider);
    }
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
    // malformed credentials
    let err = parse_raw_url("s3://FAKE_RAW_USER:FAKE_RAW_PASS@bucket/path%zz?access-key=FAKE_RAW_AK&secret-access-key=FAKE_RAW_SK").unwrap_err();
    for secret in [
        "FAKE_RAW_USER",
        "FAKE_RAW_PASS",
        "FAKE_RAW_AK",
        "FAKE_RAW_SK",
    ] {
        assert!(!err.contains(secret), "{err}");
    }
    let err = is_local_path("s3://FAKE_LOCAL_USER:FAKE_LOCAL_PASS@bucket/path%zz?access-key=FAKE_LOCAL_AK&secret-access-key=FAKE_LOCAL_SK").unwrap_err();
    for secret in [
        "FAKE_LOCAL_USER",
        "FAKE_LOCAL_PASS",
        "FAKE_LOCAL_AK",
        "FAKE_LOCAL_SK",
    ] {
        assert!(!err.contains(secret), "{err}");
    }
}

/// Go `TestIsLocal`.
#[test]
fn test_is_local() {
    let err = is_local_path(":").unwrap_err();
    assert!(err.contains("invalid storage URL"), "{err}");
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
    let endpoint = String::from_utf8(tidb_go_url::query_escape(
        b"https://FAKE_BACKEND_USER:FAKE_BACKEND_PASS@host/path%zz?sig=FAKE_BACKEND_TOKEN",
    ))
    .unwrap();
    for raw in [
        "s3://FAKE_BACKEND_USER:FAKE_BACKEND_PASS@bucket/path%zz?access-key=FAKE_BACKEND_AK&secret-access-key=FAKE_BACKEND_SK".to_owned(),
        "s3:///path?access-key=FAKE_BACKEND_AK&secret-access-key=FAKE_BACKEND_SK&unknown-token=FAKE_BACKEND_TOKEN".to_owned(),
        "gcs:///path?credentials=FAKE_BACKEND_GCS".to_owned(),
        "azure:///path?sig=FAKE_BACKEND_SAS".to_owned(),
        format!("s3://bucket/prefix?endpoint={endpoint}"),
    ] {
        let err = parse_backend(&raw, None).unwrap_err();
        for secret in [
            "FAKE_BACKEND_USER",
            "FAKE_BACKEND_PASS",
            "FAKE_BACKEND_AK",
            "FAKE_BACKEND_SK",
            "FAKE_BACKEND_TOKEN",
            "FAKE_BACKEND_GCS",
            "FAKE_BACKEND_SAS",
        ] {
            assert!(!err.contains(secret), "{raw}: {err}");
        }
    }
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

/// Go `TestRedactURL`.
#[test]
fn test_redact_url() {
    for (raw, expected) in [
        ("/local/path", "/local/path"),
        ("/local/path#name", "/local/path#name"),
        ("/data/my files/100%off", "/data/my files/100%off"),
        ("/local/path?token=SECRET#FRAGMENT", "/local/path"),
        (
            "file:///local/path?token=SECRET#FRAGMENT",
            "file:///local/path",
        ),
        (
            "local://USER:PASS@/local/path?token=SECRET#FRAGMENT",
            "local:///local/path",
        ),
        (
            "hdfs://USER:PASS@host/path?token=SECRET#FRAGMENT",
            "hdfs://host/path",
        ),
        ("noop:///?token=SECRET#FRAGMENT", "noop:///"),
        (
            "gs://bucket/path?credentials=SECRET#FRAGMENT",
            "gs://bucket/path",
        ),
        (
            "gcs://USER:PASS@bucket/path?credentials=SECRET#FRAGMENT",
            "gcs://bucket/path",
        ),
        ("GCS://bucket/path?credentials=SECRET", "gcs://bucket/path"),
        (
            "gcs://bucket/path?credentials=SECRET%zz",
            "gcs://bucket/path",
        ),
        ("local:///local/path?", "local:///local/path"),
        ("https://host/path?", "https://host/path"),
        ("s3://bucket/prefix", "s3://bucket/prefix"),
        (
            "s3://bucket/prefix?sse-kms-context=%7B%22tenant%22%3A%22context-value%22%7D",
            "s3://bucket/prefix?sse-kms-context=xxxxxx",
        ),
        (
            r##"s3://bucket/prefix?sse-kms-context={"tenant":"#context-value"}"##,
            "s3://bucket/prefix",
        ),
        (
            r#"s3://bucket/prefix?sse=aws:kms&sse-kms-context={"tenant":"a&private-context-value"}"#,
            "s3://bucket/prefix",
        ),
        (
            r#"s3://bucket/prefix?sse-kms-context={"tenant":"a&region=private-context-value"}"#,
            "s3://bucket/prefix",
        ),
        (
            r#"s3://bucket/prefix?sse-kms-context=%7B%7D&sse-kms-context={"tenant":"a&region=private-context-value"}"#,
            "s3://bucket/prefix",
        ),
        (
            r#"s3://bucket/prefix?sse=aws:kms&sse-kms-context={"tenant":"a;x&private-context-value"}"#,
            "s3://bucket/prefix",
        ),
        (
            r#"s3://bucket/prefix?sse=aws:kms&sse-kms-context={"tenant":"a%zz&private-context-value"}"#,
            "s3://bucket/prefix",
        ),
        (
            "s3://bucket/prefix?access-key=AKID&secret-access-key=SKEY&region=us",
            "s3://bucket/prefix?access-key=xxxxxx&region=us&secret-access-key=xxxxxx",
        ),
        (
            "s3://bucket/prefix?endpoint=https%3A%2F%2FUSER%3APASS%40host%2F%3Fsig%3DSECRET",
            "s3://bucket/prefix?endpoint=xxxxxx",
        ),
        (
            "ks3://bucket/prefix?access-key=AKID&region=us",
            "ks3://bucket/prefix?access-key=xxxxxx&region=us",
        ),
        (
            "oss://bucket/prefix?access-key=AKID&region=us",
            "oss://bucket/prefix?access-key=xxxxxx&region=us",
        ),
        (
            "azure://bucket/prefix?sas-token=SAS",
            "azure://bucket/prefix?sas-token=xxxxxx",
        ),
        (
            "azblob://bucket/prefix?account-name=a&sas-token=SAS",
            "azblob://bucket/prefix?account-name=a&sas-token=xxxxxx",
        ),
        (
            "azure://c/p?account-name=a&sas-token=sv=2020-08-04&ss=b&sig=SECRETSIG",
            "azure://c/p?account-name=a&sas-token=xxxxxx",
        ),
        (
            "s3://bucket:port/prefix?access-key=AKID&secret-access-key=SKEY",
            INVALID_URL_PLACEHOLDER,
        ),
        (
            "s3://bucket%zz/prefix?access-key=AKID&secret-access-key=SKEY",
            INVALID_URL_PLACEHOLDER,
        ),
        (
            "s3a://bucket/prefix?access-key=AKID&secret-access-key=SKEY",
            "s3a://bucket/prefix",
        ),
        (
            "s3://AKID:SKEY@bucket/prefix?access-key=AKID",
            "s3://bucket/prefix?access-key=xxxxxx",
        ),
        ("s3a://AKID:SKEY@bucket/prefix", "s3a://bucket/prefix"),
        (
            "https://account.blob.core.windows.net/?sv=2022&sig=SAS",
            "https://account.blob.core.windows.net/",
        ),
    ] {
        assert_eq!(redact_url(raw), expected, "{raw}");
    }
}

/// Go `TestParseRawURLErrorDoesNotLeakURL`.
#[test]
fn test_parse_raw_url_error_does_not_leak_url() {
    for raw in [
        "s3://bucket:port/prefix?access-key=AKID&secret-access-key=SKEY",
        "s3://bucket%zz/prefix?access-key=AKID&secret-access-key=SKEY",
        "s3://AKID:SKEY/bucket/prefix",
    ] {
        for err in [
            parse_raw_url(raw).unwrap_err(),
            parse_backend(raw, None).unwrap_err(),
        ] {
            assert!(
                !err.contains("AKID") && !err.contains("SKEY"),
                "{raw}: {err}"
            );
        }
    }
}

/// Go `TestParseBackendMissingBucketDoesNotLeakUserinfo`.
#[test]
fn test_parse_backend_missing_bucket_does_not_leak_userinfo() {
    for raw in [
        "s3://AKID:SKEY@/prefix",
        "gcs://AKID:SKEY@/prefix",
        "azblob://AKID:SKEY@/prefix",
    ] {
        let err = parse_backend(raw, None).unwrap_err();
        assert!(err.contains("please specify the bucket"), "{err}");
        assert!(!err.contains("AKID") && !err.contains("SKEY"), "{err}");
    }
}

/// Go `TestBackendOptionsRedacted`.
#[test]
fn test_backend_options_redacted() {
    use tidb_ast::REDACTED_VALUE;
    let mut opts = BackendOptions::default();
    opts.s3.access_key = "AKID".into();
    opts.s3.secret_access_key = "SKEY".into();
    opts.s3.session_token = "TOKEN".into();
    opts.s3.sse_kms_context = r#"{"tenant":"context-value"}"#.into();
    opts.s3.region = "us-east-1".into();
    opts.azblob.account_key = "AZKEY".into();
    opts.azblob.sas_token = "SAS".into();
    opts.azblob.encryption_key = "ENC".into();
    opts.azblob.account_name = "account".into();
    opts.azblob.endpoint = "https://account.blob.core.windows.net/?sig=SAS".into();
    opts.s3.endpoint = "https://AKID:SKEY@s3.example.com".into();
    let redacted = opts.redacted();
    for masked in [
        &redacted.s3.access_key,
        &redacted.s3.secret_access_key,
        &redacted.s3.session_token,
        &redacted.s3.sse_kms_context,
        &redacted.azblob.account_key,
        &redacted.azblob.sas_token,
        &redacted.azblob.encryption_key,
    ] {
        assert_eq!(masked, REDACTED_VALUE);
    }
    assert_eq!(opts.s3.sse_kms_context, r#"{"tenant":"context-value"}"#);
    assert_eq!(redacted.s3.region, "us-east-1");
    assert_eq!(redacted.azblob.account_name, "account");
    assert_eq!(
        redacted.azblob.endpoint,
        "https://account.blob.core.windows.net/"
    );
    assert_eq!(redacted.s3.endpoint, "https://s3.example.com");
    assert_eq!(opts.s3.access_key, "AKID");
    assert!(BackendOptions::default().redacted() == BackendOptions::default());
    for (endpoint, expected) in [
        ("", ""),
        ("https://host:9000/path", "https://host:9000/path"),
        (
            "http://FAKE_USER:FAKE_PASS@host:9000/path?sig=FAKE_TOKEN#FAKE_FRAGMENT",
            "http://host:9000/path",
        ),
        ("FAKE_USER:FAKE_PASS@host:9000", INVALID_URL_PLACEHOLDER),
        ("//FAKE_USER:FAKE_PASS@host:9000", INVALID_URL_PLACEHOLDER),
        (
            "https:FAKE_USER:FAKE_PASS@host:9000",
            INVALID_URL_PLACEHOLDER,
        ),
        (
            "https:///FAKE_USER:FAKE_PASS@host:9000",
            INVALID_URL_PLACEHOLDER,
        ),
        (
            "https://FAKE_USER:FAKE_PASS@host:port",
            INVALID_URL_PLACEHOLDER,
        ),
        (
            "https://FAKE_USER:FAKE_PASS@host%zz",
            INVALID_URL_PLACEHOLDER,
        ),
        ("/local/path#FAKE_FRAGMENT", INVALID_URL_PLACEHOLDER),
    ] {
        let mut opts = BackendOptions::default();
        opts.s3.endpoint = endpoint.into();
        opts.gcs.endpoint = endpoint.into();
        opts.azblob.endpoint = endpoint.into();
        let original = opts.clone();
        let redacted = opts.redacted();
        assert_eq!(redacted.s3.endpoint, expected, "{endpoint}");
        assert_eq!(redacted.gcs.endpoint, expected, "{endpoint}");
        assert_eq!(redacted.azblob.endpoint, expected, "{endpoint}");
        assert!(original == opts);
    }
}

/// Go `TestS3SSEKMSContextURL`.
#[test]
fn test_s3_sse_kms_context_url() {
    use base64::Engine as _;
    let b64 = |s: &str| base64::engine::general_purpose::STANDARD.encode(s);
    let context_json = r#"{"kk":"¾","tenant":"租户😀","value":"a+b / = & ?"}"#;
    let normalized = r#"{"kk":"¾","tenant":"租户😀","value":"a+b / = \u0026 ?"}"#;
    let encoded = b64(normalized);
    assert!(encoded.contains('+'));
    // ParseRawURL preserves literal plus signs for existing storage parameters.
    let query = String::from_utf8(tidb_go_url::query_escape(context_json.as_bytes()))
        .unwrap()
        .replace('+', "%20");
    for key in ["sse-kms-context", "SSE_KMS_CONTEXT"] {
        let options = BackendOptions {
            s3: S3BackendOptions {
                sse: "aws:kms".into(),
                sse_kms_context: r#"{"tenant":"from-options"}"#.into(),
                ..Default::default()
            },
            ..Default::default()
        };
        let backend = s3_of(
            parse_backend(&format!("s3://bucket/prefix?{key}={query}"), Some(&options)).unwrap(),
        );
        assert_eq!(backend.sse_kms_encryption_context, encoded);
        assert_eq!(backend.sse, "aws:kms");
        assert!(backend.sse_kms_key_id.is_empty());
        // Parsing a URL overrides options for this backend, without mutating them.
        let from_options = s3_of(parse_backend("s3://bucket/prefix", Some(&options)).unwrap());
        assert_eq!(
            from_options.sse_kms_encryption_context,
            b64(r#"{"tenant":"from-options"}"#)
        );
        let unset =
            s3_of(parse_backend(&format!("s3://bucket/prefix?{key}="), Some(&options)).unwrap());
        assert!(unset.sse_kms_encryption_context.is_empty());
        let mut parsed = tidb_go_url::parse(format!("s3://bucket/prefix?{key}={query}")).unwrap();
        let from_url = s3_of(parse_backend_from_url(&mut parsed, Some(&options)).unwrap());
        assert_eq!(from_url.sse_kms_encryption_context, encoded);
    }
    // The encoded value survives snapshot request serialization.
    use prost::Message as _;
    let backend = parse_backend(
        &format!("s3://bucket/prefix?sse=aws:kms&sse-kms-context={query}"),
        None,
    )
    .unwrap();
    let request = tidb_proto::backup::BackupRequest {
        storage_backend: Some(backend),
        ..Default::default()
    };
    let restored =
        tidb_proto::backup::BackupRequest::decode(request.encode_to_vec().as_slice()).unwrap();
    assert_eq!(
        s3_of(restored.storage_backend.unwrap()).sse_kms_encryption_context,
        encoded
    );
    for input in [
        r#"{"tenant":null}"#,
        r#"{"tenant":"private-context","tenant":"other"}"#,
        encoded.as_str(),
    ] {
        let raw = format!(
            "s3://bucket/prefix?sse-kms-context={}",
            String::from_utf8(tidb_go_url::query_escape(input.as_bytes())).unwrap()
        );
        let err = parse_backend(&raw, None).unwrap_err();
        assert!(
            err.contains("sse-kms-context") && !err.contains("private-context"),
            "{err}"
        );
    }
    // A query that cannot be fully decoded must not silently lose the context.
    for raw_query in [
        r#"{"tenant":"a;private-context"}"#,
        r#"{"tenant":"private-context%zz"}"#,
    ] {
        let err = parse_backend(
            &format!("s3://bucket/prefix?sse=aws:kms&sse-kms-context={raw_query}"),
            None,
        )
        .unwrap_err();
        assert!(
            err.contains("parse storage URL query failed") && !err.contains("private-context"),
            "{err}"
        );
    }
}

/// Go `s3like.Storage.CopyFrom` against a live S3 service: a server-side
/// copy from a storage in one bucket to a storage in another, each under
/// its prefix. `TIDB_TEST_S3_ENDPOINT` names a MinIO (or S3) endpoint whose
/// `minioadmin` account owns the buckets `copy-src` and `copy-dst`.
#[test]
#[ignore = "needs a live S3 endpoint"]
fn s3_copy_from_another_bucket() {
    let endpoint = std::env::var("TIDB_TEST_S3_ENDPOINT").expect("TIDB_TEST_S3_ENDPOINT");
    let ctx = tidb_gocontext::Context::background();
    let url = |bucket: &str, prefix: &str| {
        format!(
            "s3://{bucket}/{prefix}?endpoint={endpoint}&access-key=minioadmin&secret-access-key=minioadmin&force-path-style=true&region=us-east-1"
        )
    };
    let src = crate::new_from_url(&ctx, &url("copy-src", "from")).unwrap();
    let dst = crate::new_from_url(&ctx, &url("copy-dst", "to")).unwrap();
    src.write_file(&ctx, "dir/a b+c.sst", b"payload").unwrap();
    let copier = dst.as_copier().expect("an S3 storage is a Copier");
    copier
        .copy_from(&ctx, &*src, &crate::storeapi::CopySpec { from: "dir/a b+c.sst".into(), to: "out/copied.sst".into() })
        .unwrap();
    assert_eq!(dst.read_file(&ctx, "out/copied.sst").unwrap(), b"payload");
    // Within one bucket, the same request.
    let same = crate::new_from_url(&ctx, &url("copy-src", "again")).unwrap();
    same.as_copier()
        .unwrap()
        .copy_from(&ctx, &*src, &crate::storeapi::CopySpec { from: "dir/a b+c.sst".into(), to: "x.sst".into() })
        .unwrap();
    assert_eq!(same.read_file(&ctx, "x.sst").unwrap(), b"payload");
    // A missing source is the service's error.
    let missing = copier
        .copy_from(&ctx, &*src, &crate::storeapi::CopySpec { from: "nope".into(), to: "y.sst".into() })
        .unwrap_err()
        .to_string();
    assert!(missing.contains("NoSuchKey"), "{missing}");
}
