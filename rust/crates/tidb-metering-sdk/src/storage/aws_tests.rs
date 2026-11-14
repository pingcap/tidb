// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
use super::*;
use std::io::{BufRead, BufReader};
/// Go `zzprobe/o15_meter_sdk.TestMeterSDKAWSDefaultCredentialProcess`.
#[test]
fn test_meter_sdk_aws_default_credential_process() {
    fixture(false);
}
/// Go `zzprobe/o15_meter_sdk.TestMeterSDKAWSAssumeRole`.
#[test]
fn test_meter_sdk_aws_assume_role() {
    fixture(true);
}
fn fixture(role: bool) {
    if std::env::var_os("O15_METER_AWS_CHILD").is_none() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("process"),r#"printf '%s' '{"Version":1,"AccessKeyId":"process_fixture","SecretAccessKey":"process_fixture"}'"#).unwrap();
        std::fs::write(
            root.path().join("config"),
            format!(
                "[profile process]\nregion = us-west-2\ncredential_process = /bin/sh {}\n",
                root.path().join("process").display()
            ),
        )
        .unwrap();
        let name = if role {
            "test_meter_sdk_aws_assume_role"
        } else {
            "test_meter_sdk_aws_default_credential_process"
        };
        let result = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                &format!("storage::aws_tests::{name}"),
                "--nocapture",
            ])
            .env("O15_METER_AWS_CHILD", "1")
            .env("AWS_CONFIG_FILE", root.path().join("config"))
            .env("AWS_SHARED_CREDENTIALS_FILE", root.path().join("empty"))
            .env("AWS_PROFILE", "process")
            .env("AWS_EC2_METADATA_DISABLED", "true")
            .env_remove("AWS_ACCESS_KEY_ID")
            .env_remove("AWS_SECRET_ACCESS_KEY")
            .env_remove("AWS_SESSION_TOKEN")
            .env_remove("AWS_WEB_IDENTITY_TOKEN_FILE")
            .env_remove("AWS_ROLE_ARN")
            .env_remove("AWS_REGION")
            .env_remove("AWS_DEFAULT_REGION")
            .status()
            .unwrap();
        assert!(result.success());
        return;
    }
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        for index in 0..if role { 2 } else { 1 } {
            let (mut socket, _) = listener.accept().unwrap();
            socket
                .set_read_timeout(Some(std::time::Duration::from_secs(10)))
                .unwrap();
            let mut reader = BufReader::new(socket.try_clone().unwrap());
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            let request = line.clone();
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
            let mut data = vec![
                0;
                headers
                    .get("content-length")
                    .map_or(0, |v| v.parse::<usize>().unwrap())
            ];
            reader.read_exact(&mut data).unwrap();
            let body = if role && index == 0 {
                assert!(request.starts_with("POST / "));
                let form: std::collections::HashMap<_, _> =
                    url::form_urlencoded::parse(&data).collect();
                assert_eq!(form["Action"], "AssumeRole");
                assert_eq!(form["DurationSeconds"], "900");
                assert_eq!(form["RoleArn"], "arn:aws:iam::123456789012:role/fixture");
                assert!(form["RoleSessionName"]
                    .strip_prefix("aws-go-sdk-")
                    .unwrap_or("")
                    .parse::<i64>()
                    .is_ok());
                assert!(headers["authorization"].contains("Credential=process_fixture/"));
                r#"<AssumeRoleResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/"><AssumeRoleResult><Credentials><AccessKeyId>assumed_fixture</AccessKeyId><SecretAccessKey>assumed_fixture</SecretAccessKey><SessionToken>assumed_token_fixture</SessionToken><Expiration>2099-01-01T00:00:00Z</Expiration></Credentials><AssumedRoleUser><Arn>arn:aws:sts::123456789012:assumed-role/fixture/session</Arn><AssumedRoleId>fixture:session</AssumedRoleId></AssumedRoleUser></AssumeRoleResult></AssumeRoleResponse>"#
            } else {
                assert!(request.starts_with("PUT /bucket/capture/object "));
                let wanted = if role {
                    "assumed_fixture"
                } else {
                    "process_fixture"
                };
                assert!(headers["authorization"].contains(&format!("Credential={wanted}/")));
                assert!(headers["authorization"].contains("/us-west-2/s3/"));
                assert_eq!(data, b"payload");
                ""
            };
            write!(socket,"HTTP/1.1 200 OK\r\nContent-Type: text/xml\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).unwrap();
        }
    });
    let role_option = if role {
        "&assume-role-arn=arn:aws:iam::123456789012:role/fixture"
    } else {
        ""
    };
    let config = crate::config::new_from_uri(&format!(
        "s3://bucket/capture?endpoint=http://{addr}&force-path-style=true{role_option}"
    ))
    .unwrap();
    let p = new_object_storage_provider(&config).unwrap();
    p.upload(&Context::background(), "object", b"payload")
        .unwrap();
    server.join().unwrap();
}
