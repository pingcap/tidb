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

// aggregate-test: standalone

#![allow(dead_code, missing_docs)]

#[path = "../src/auth_identity.rs"]
mod auth_identity;
#[path = "../src/configured_user_store.rs"]
mod configured_user_store;
#[path = "../src/native_password.rs"]
mod native_password;

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use configured_user_store::{ConfiguredUserStore, ConfiguredUserStoreError};
use sha1::{Digest, Sha1};

const ABC_HASH: &str = "*0D3CED9BEC10A777AEC23CCC353A8C08A633045E";
const SOURCE_SALT: [u8; 20] = [
    85, 92, 45, 22, 58, 79, 107, 6, 122, 125, 58, 80, 12, 90, 103, 32, 90, 10, 74, 82,
];

#[test]
fn strict_file_load_authenticates_the_most_specific_canonical_host() {
    let file = AuthFile::new(&format!(
        "alice\t%\tmysql_native_password\t{ABC_HASH}\n\
         alice\tlocalhost\tmysql_native_password\t{ABC_HASH}\n\
         alice\t127.0.0.%\tmysql_native_password\t{ABC_HASH}\n"
    ));
    let store = ConfiguredUserStore::load(file.path()).expect("strict auth file");
    assert_eq!(store.len(), 3);
    assert!(!store.is_empty());

    let response = scramble(b"abc", &SOURCE_SALT);
    let loopback = store
        .authenticate_native("alice", "127.0.0.1", &SOURCE_SALT, &response)
        .expect("canonical loopback account");
    assert_eq!(loopback.username(), "alice");
    assert_eq!(loopback.host(), "localhost");
    assert_eq!(loopback.auth_plugin(), "mysql_native_password");
    assert_eq!(loopback.matched_identity().host(), "localhost");

    let wildcard = store
        .authenticate_native("alice", "192.0.2.9", &SOURCE_SALT, &response)
        .expect("fallback account");
    assert_eq!(wildcard.host(), "%");
}

#[test]
fn wrong_password_unknown_user_and_unknown_host_all_deny_authentication() {
    let store = ConfiguredUserStore::parse(&format!(
        "alice\t10.0.0.%\tmysql_native_password\t{ABC_HASH}\n"
    ))
    .expect("catalog");
    let correct = scramble(b"abc", &SOURCE_SALT);
    let wrong = scramble(b"wrong", &SOURCE_SALT);

    assert!(store
        .authenticate_native("alice", "10.0.0.7", &SOURCE_SALT, &correct)
        .is_ok());
    assert!(store
        .authenticate_native("alice", "10.0.0.7", &SOURCE_SALT, &wrong)
        .is_err());
    assert!(store
        .authenticate_native("unknown", "10.0.0.7", &SOURCE_SALT, &correct)
        .is_err());
    assert!(store
        .authenticate_native("alice", "192.0.2.7", &SOURCE_SALT, &correct)
        .is_err());
}

#[test]
fn parser_rejects_empty_malformed_unsupported_duplicate_and_invalid_hash_rows() {
    let cases = [
        ("", "contains no accounts"),
        ("\n", "record 1 is malformed"),
        ("alice\t%\tmysql_native_password\n", "record 1 is malformed"),
        (
            "\t%\tmysql_native_password\t*0000000000000000000000000000000000000000\n",
            "record 1 is malformed",
        ),
        (
            "alice\t\tmysql_native_password\t*0000000000000000000000000000000000000000\n",
            "record 1 is malformed",
        ),
        (
            "alice\t%\tcaching_sha2_password\t*0000000000000000000000000000000000000000\n",
            "record 1 uses an unsupported plugin",
        ),
        (
            "alice\t%\tmysql_native_password\t*not-a-hash\n",
            "record 1 has an invalid password hash",
        ),
        (
            "alice\t%\tmysql_native_password\t*0000000000000000000000000000000000000000\n\
             alice\t%\tmysql_native_password\t*1111111111111111111111111111111111111111\n",
            "record 2 duplicates an identity",
        ),
        (
            "alice\t%\tmysql_native_password\t*0000000000000000000000000000000000000000\textra\n",
            "record 1 is malformed",
        ),
    ];

    for (contents, message) in cases {
        let error = ConfiguredUserStore::parse(contents).expect_err("invalid catalog");
        assert_eq!(error.to_string(), format!("authentication file {message}"));
        assert!(!error.to_string().contains("0000000000"));
    }
}

#[test]
fn diagnostics_never_render_password_equivalent_material() {
    let store =
        ConfiguredUserStore::parse(&format!("alice\t%\tmysql_native_password\t{ABC_HASH}\n"))
            .expect("catalog");
    let rendered = format!("{store:?}");
    assert_eq!(rendered, "ConfiguredUserStore { account_count: 1, .. }");
    assert!(!rendered.contains(ABC_HASH));

    let invalid = format!("alice\t%\tmysql_native_password\t{ABC_HASH}Z\n");
    let error = ConfiguredUserStore::parse(&invalid).expect_err("invalid hash");
    assert!(!format!("{error:?}").contains(ABC_HASH));
    assert!(!error.to_string().contains(ABC_HASH));
}

#[cfg(unix)]
#[test]
fn file_permissions_must_be_exactly_0600() {
    use std::os::unix::fs::PermissionsExt;

    let file = AuthFile::new(&format!("alice\t%\tmysql_native_password\t{ABC_HASH}\n"));
    fs::set_permissions(file.path(), fs::Permissions::from_mode(0o640)).expect("chmod");
    assert!(matches!(
        ConfiguredUserStore::load(file.path()),
        Err(ConfiguredUserStoreError::InvalidPermissions)
    ));

    fs::set_permissions(file.path(), fs::Permissions::from_mode(0o1600)).expect("chmod");
    assert!(matches!(
        ConfiguredUserStore::load(file.path()),
        Err(ConfiguredUserStoreError::InvalidPermissions)
    ));

    fs::set_permissions(file.path(), fs::Permissions::from_mode(0o600)).expect("chmod");
    ConfiguredUserStore::load(file.path()).expect("exact mode");
}

fn scramble(password: &[u8], salt: &[u8]) -> [u8; 20] {
    let stage_one = Sha1::digest(password);
    let stage_two = Sha1::digest(stage_one);
    let mut hasher = Sha1::new();
    hasher.update(salt);
    hasher.update(stage_two);
    let challenge = hasher.finalize();
    let mut response = [0; 20];
    for ((destination, stage_one), challenge) in response
        .iter_mut()
        .zip(stage_one.iter())
        .zip(challenge.iter())
    {
        *destination = stage_one ^ challenge;
    }
    response
}

struct AuthFile {
    path: PathBuf,
}

impl AuthFile {
    fn new(contents: &str) -> Self {
        static NEXT_ID: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "tidb-rust-auth-{}-{}",
            std::process::id(),
            NEXT_ID.fetch_add(1, Ordering::Relaxed)
        ));
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&path).expect("create auth file");
        file.write_all(contents.as_bytes())
            .expect("write auth file");
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for AuthFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}
