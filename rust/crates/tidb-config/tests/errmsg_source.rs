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

//! Semantic boundary tests for accepted Go package `pkg/util/errmsg`.

use std::thread;

use tidb_config::config_tree::config::{
    get_error_message_extensions, get_global_config, store_global_config,
};
use tidb_config::config_tree::{
    extend_error_message, extended_error_message, Config, ErrorMessageExtension,
};

struct ConfigRestore(Option<Config>);

impl Drop for ConfigRestore {
    fn drop(&mut self) {
        if let Some(config) = self.0.take() {
            store_global_config(config);
        }
    }
}

fn extension(pattern: &str, suffix: &str) -> ErrorMessageExtension {
    ErrorMessageExtension {
        pattern: pattern.to_owned(),
        suffix: suffix.to_owned(),
    }
}

#[test]
fn configured_suffix_selection_and_concurrent_publication_match_source() {
    let _restore = ConfigRestore(Some(get_global_config()));

    let mut config = get_global_config();
    config.error_message_extensions = vec![
        extension("^Access denied", "generic access denied message"),
        extension(
            "^Access denied for user '.+'@'.+' \\(using password: (YES|NO)\\)$",
            "specific user prefix message",
        ),
        extension("^Error message\\.$", "suffix."),
        extension("^Error message without period$", "suffix"),
        extension(
            "^Error message with multiple periods\\.\\.\\.$",
            "suffix...",
        ),
        extension("^Error message with empty suffix$", ""),
        extension("[", "invalid regex"),
    ];
    store_global_config(config.clone());

    let cases = [
        (
            "Access denied for user 'root.foo'@'127.0.0.1' (using password: YES)",
            "Access denied for user 'root.foo'@'127.0.0.1' (using password: YES), specific user prefix message.",
        ),
        ("Access denied by policy", "Access denied by policy, generic access denied message."),
        ("Table 'test.t' doesn't exist", "Table 'test.t' doesn't exist"),
        ("Error message.", "Error message, suffix."),
        ("Error message without period", "Error message without period, suffix."),
        ("Error message with multiple periods...", "Error message with multiple periods, suffix."),
        ("Error message with empty suffix", "Error message with empty suffix"),
    ];
    for (message, expected) in cases {
        let mut actual = message.to_owned();
        extend_error_message(&mut actual);
        assert_eq!(actual, expected);
    }

    let prepared = get_error_message_extensions();
    assert_eq!(prepared.len(), 6, "the invalid regexp is not published");
    assert!(prepared[0].pattern.len() >= prepared[1].pattern.len());

    let published = config;
    let writer = thread::spawn(move || {
        for _ in 0..1_000 {
            store_global_config(published.clone());
        }
    });
    let reader = thread::spawn(|| {
        for _ in 0..1_000 {
            assert_eq!(
                extended_error_message("Access denied by policy").as_deref(),
                Some("Access denied by policy, generic access denied message.")
            );
        }
    });
    writer.join().unwrap();
    reader.join().unwrap();

    let mut empty = get_global_config();
    empty.error_message_extensions.clear();
    store_global_config(empty);
    assert_eq!(extended_error_message("Access denied by policy"), None);
}
