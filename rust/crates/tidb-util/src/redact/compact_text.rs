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

//! The exact gogo/protobuf compact-text surface used by Go
//! `proto.CompactTextString(StreamBackupTaskInfo)`.

use std::fmt::Write as _;

use tidb_proto::{backup, encryptionpb};

pub(super) fn stream_backup_task_info(info: &backup::StreamBackupTaskInfo) -> String {
    let mut out = String::new();
    if let Some(storage) = &info.storage {
        message_field(&mut out, "storage", |out| storage_backend(out, storage));
    }
    uint64_field(&mut out, "start_ts", info.start_ts);
    uint64_field(&mut out, "end_ts", info.end_ts);
    string_field(&mut out, "name", &info.name);
    for filter in &info.table_filter {
        string_field_present(&mut out, "table_filter", filter);
    }
    enum_field(
        &mut out,
        "compression_type",
        info.compression_type,
        compression_type_name,
    );
    if let Some(security) = &info.security_config {
        message_field(&mut out, "security_config", |out| {
            stream_backup_task_security_config(out, security);
        });
    }
    out
}

fn storage_backend(out: &mut String, storage: &backup::StorageBackend) {
    use backup::storage_backend::Backend;

    match &storage.backend {
        Some(Backend::Noop(_)) => message_field(out, "noop", |_| {}),
        Some(Backend::Local(local)) => {
            message_field(out, "local", |out| string_field(out, "path", &local.path));
        }
        Some(Backend::S3(s3)) => message_field(out, "s3", |out| s3_fields(out, s3)),
        Some(Backend::Gcs(gcs)) => message_field(out, "gcs", |out| gcs_fields(out, gcs)),
        Some(Backend::CloudDynamic(cloud)) => {
            message_field(out, "cloud_dynamic", |out| cloud_dynamic_fields(out, cloud));
        }
        Some(Backend::Hdfs(hdfs)) => message_field(out, "hdfs", |out| {
            string_field(out, "remote", &hdfs.remote);
        }),
        Some(Backend::AzureBlobStorage(azure)) => message_field(out, "azure_blob_storage", |out| {
            azure_blob_storage_fields(out, azure)
        }),
        None => {}
    }
}

fn s3_fields(out: &mut String, s3: &backup::S3) {
    string_field(out, "endpoint", &s3.endpoint);
    string_field(out, "region", &s3.region);
    string_field(out, "bucket", &s3.bucket);
    string_field(out, "prefix", &s3.prefix);
    string_field(out, "storage_class", &s3.storage_class);
    string_field(out, "sse", &s3.sse);
    string_field(out, "acl", &s3.acl);
    string_field(out, "access_key", &s3.access_key);
    string_field(out, "secret_access_key", &s3.secret_access_key);
    bool_field(out, "force_path_style", s3.force_path_style);
    string_field(out, "sse_kms_key_id", &s3.sse_kms_key_id);
    string_field(out, "role_arn", &s3.role_arn);
    string_field(out, "external_id", &s3.external_id);
    bool_field(out, "object_lock_enabled", s3.object_lock_enabled);
    string_field(out, "session_token", &s3.session_token);
    string_field(out, "provider", &s3.provider);
    string_field(out, "profile", &s3.profile);
}

fn gcs_fields(out: &mut String, gcs: &backup::Gcs) {
    string_field(out, "endpoint", &gcs.endpoint);
    string_field(out, "bucket", &gcs.bucket);
    string_field(out, "prefix", &gcs.prefix);
    string_field(out, "storage_class", &gcs.storage_class);
    string_field(out, "predefined_acl", &gcs.predefined_acl);
    string_field(out, "credentials_blob", &gcs.credentials_blob);
}

fn azure_blob_storage_fields(out: &mut String, azure: &backup::AzureBlobStorage) {
    string_field(out, "endpoint", &azure.endpoint);
    string_field(out, "bucket", &azure.bucket);
    string_field(out, "prefix", &azure.prefix);
    string_field(out, "storage_class", &azure.storage_class);
    string_field(out, "account_name", &azure.account_name);
    string_field(out, "shared_key", &azure.shared_key);
    string_field(out, "access_sig", &azure.access_sig);
    string_field(out, "encryption_scope", &azure.encryption_scope);
    if let Some(key) = &azure.encryption_key {
        message_field(out, "encryption_key", |out| {
            string_field(out, "encryption_key", &key.encryption_key);
            string_field(out, "encryption_key_sha256", &key.encryption_key_sha256);
        });
    }
}

fn cloud_dynamic_fields(out: &mut String, cloud: &backup::CloudDynamic) {
    if let Some(bucket) = &cloud.bucket {
        message_field(out, "bucket", |out| {
            string_field(out, "endpoint", &bucket.endpoint);
            string_field(out, "region", &bucket.region);
            string_field(out, "bucket", &bucket.bucket);
            string_field(out, "prefix", &bucket.prefix);
            string_field(out, "storage_class", &bucket.storage_class);
        });
    }
    string_field(out, "provider_name", &cloud.provider_name);

    let mut attrs: Vec<_> = cloud.attrs.iter().collect();
    attrs.sort_unstable_by_key(|(key, _)| *key);
    for (key, value) in attrs {
        message_field(out, "attrs", |out| {
            string_field_present(out, "key", key);
            string_field_present(out, "value", value);
        });
    }
}

fn stream_backup_task_security_config(
    out: &mut String,
    security: &backup::StreamBackupTaskSecurityConfig,
) {
    use backup::stream_backup_task_security_config::Encryption;

    match &security.encryption {
        Some(Encryption::PlaintextDataKey(cipher)) => {
            message_field(out, "plaintext_data_key", |out| cipher_info(out, cipher));
        }
        Some(Encryption::MasterKeyConfig(config)) => {
            message_field(out, "master_key_config", |out| {
                master_key_config(out, config);
            });
        }
        None => {}
    }
}

fn cipher_info(out: &mut String, cipher: &backup::CipherInfo) {
    enum_field(
        out,
        "cipher_type",
        cipher.cipher_type,
        encryption_method_name,
    );
    bytes_field(out, "cipher_key", &cipher.cipher_key);
}

fn master_key_config(out: &mut String, config: &backup::MasterKeyConfig) {
    enum_field(
        out,
        "encryption_type",
        config.encryption_type,
        encryption_method_name,
    );
    for master_key in &config.master_keys {
        message_field(out, "master_keys", |out| master_key_fields(out, master_key));
    }
}

fn master_key_fields(out: &mut String, key: &encryptionpb::MasterKey) {
    use encryptionpb::master_key::Backend;

    match &key.backend {
        Some(Backend::Plaintext(_)) => message_field(out, "plaintext", |_| {}),
        Some(Backend::File(file)) => message_field(out, "file", |out| {
            string_field(out, "path", &file.path);
        }),
        Some(Backend::Kms(kms)) => message_field(out, "kms", |out| master_key_kms(out, kms)),
        None => {}
    }
}

fn master_key_kms(out: &mut String, kms: &encryptionpb::MasterKeyKms) {
    string_field(out, "vendor", &kms.vendor);
    string_field(out, "key_id", &kms.key_id);
    string_field(out, "region", &kms.region);
    string_field(out, "endpoint", &kms.endpoint);
    if let Some(azure) = &kms.azure_kms {
        message_field(out, "azure_kms", |out| azure_kms(out, azure));
    }
    if let Some(gcp) = &kms.gcp_kms {
        message_field(out, "gcp_kms", |out| {
            string_field(out, "credential", &gcp.credential);
        });
    }
    if let Some(aws) = &kms.aws_kms {
        message_field(out, "aws_kms", |out| {
            string_field(out, "access_key", &aws.access_key);
            string_field(out, "secret_access_key", &aws.secret_access_key);
        });
    }
}

fn azure_kms(out: &mut String, azure: &encryptionpb::AzureKms) {
    string_field(out, "tenant_id", &azure.tenant_id);
    string_field(out, "client_id", &azure.client_id);
    string_field(out, "client_secret", &azure.client_secret);
    string_field(out, "key_vault_url", &azure.key_vault_url);
    string_field(out, "hsm_name", &azure.hsm_name);
    string_field(out, "hsm_url", &azure.hsm_url);
    string_field(out, "client_certificate", &azure.client_certificate);
    string_field(
        out,
        "client_certificate_path",
        &azure.client_certificate_path,
    );
    string_field(
        out,
        "client_certificate_password",
        &azure.client_certificate_password,
    );
}

fn compression_type_name(value: i32) -> String {
    match backup::CompressionType::try_from(value) {
        Ok(value) => value.as_str_name().to_owned(),
        Err(_) => value.to_string(),
    }
}

fn encryption_method_name(value: i32) -> String {
    match encryptionpb::EncryptionMethod::try_from(value) {
        Ok(value) => value.as_str_name().to_owned(),
        Err(_) => value.to_string(),
    }
}

fn message_field(out: &mut String, name: &str, fields: impl FnOnce(&mut String)) {
    out.push_str(name);
    out.push_str(":<");
    fields(out);
    out.push_str("> ");
}

fn string_field(out: &mut String, name: &str, value: &str) {
    if !value.is_empty() {
        string_field_present(out, name, value);
    }
}

fn string_field_present(out: &mut String, name: &str, value: &str) {
    out.push_str(name);
    out.push(':');
    quote_bytes(out, value.as_bytes());
    out.push(' ');
}

fn bytes_field(out: &mut String, name: &str, value: &[u8]) {
    if value.is_empty() {
        return;
    }
    out.push_str(name);
    out.push(':');
    quote_bytes(out, value);
    out.push(' ');
}

fn uint64_field(out: &mut String, name: &str, value: u64) {
    if value != 0 {
        let _ = write!(out, "{name}:{value} ");
    }
}

fn bool_field(out: &mut String, name: &str, value: bool) {
    if value {
        let _ = write!(out, "{name}:true ");
    }
}

fn enum_field(out: &mut String, name: &str, value: i32, enum_name: fn(i32) -> String) {
    if value == 0 {
        return;
    }
    let value = enum_name(value);
    // gogo/protobuf v1.3.2's text_gogo.go writes the value twice when its
    // enum registry lookup misses. The pinned kvproto task types exercise
    // that path; Go oracle probes produce `ZSTDZSTD` and
    // `AES256_CTRAES256_CTR`.
    let _ = write!(out, "{name}:{value}{value} ");
}

fn quote_bytes(out: &mut String, value: &[u8]) {
    out.push('"');
    for &byte in value {
        match byte {
            b'\n' => out.push_str("\\n"),
            b'\r' => out.push_str("\\r"),
            b'\t' => out.push_str("\\t"),
            b'"' => out.push_str("\\\""),
            b'\\' => out.push_str("\\\\"),
            0x20..=0x7e => out.push(char::from(byte)),
            _ => {
                let _ = write!(out, "\\{byte:03o}");
            }
        }
    }
    out.push('"');
}
