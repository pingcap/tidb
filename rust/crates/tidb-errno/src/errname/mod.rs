// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");

//! Direct message and redaction catalog from `pkg/errno/errname.go`.

use tidb_mysql::errors::{CatalogEntry, ErrMessage};

// The messages and `CATALOG` are generated from Go's errname.go by
// rust/scripts/generate-errno.py.
mod catalog_generated;

pub use catalog_generated::*;

/// Finds the source entry registered for `code`.
#[must_use]
pub fn entry_by_code(code: u16) -> Option<&'static CatalogEntry> {
    CATALOG.iter().find(|entry| entry.code == code)
}

/// Finds the source message registered for `code`.
#[must_use]
pub fn message_by_code(code: u16) -> Option<&'static ErrMessage> {
    entry_by_code(code).map(|entry| &entry.message)
}

#[cfg(test)]
mod tests {
    /// The verbs of a Go format string, as Go's
    /// `%[-+# 0]*(?:[0-9]+|\*)?(?:\.(?:[0-9]+|\*))?([a-zA-Z%])` finds them.
    fn verbs(raw: &str) -> Vec<char> {
        let bytes = raw.as_bytes();
        let mut verbs = Vec::new();
        let mut i = 0;
        while i < bytes.len() {
            if bytes[i] != b'%' {
                i += 1;
                continue;
            }
            let mut j = i + 1;
            while j < bytes.len() && b"-+# 0".contains(&bytes[j]) {
                j += 1;
            }
            let width = |j: &mut usize| {
                if *j < bytes.len() && bytes[*j] == b'*' {
                    *j += 1;
                } else {
                    while *j < bytes.len() && bytes[*j].is_ascii_digit() {
                        *j += 1;
                    }
                }
            };
            width(&mut j);
            if j < bytes.len() && bytes[j] == b'.' {
                let dot = j;
                j += 1;
                let before = j;
                width(&mut j);
                if j == before {
                    j = dot;
                }
            }
            if j < bytes.len() && (bytes[j].is_ascii_alphabetic() || bytes[j] == b'%') {
                if bytes[j] != b'%' {
                    verbs.push(bytes[j] as char);
                }
                i = j + 1;
            } else {
                i += 1;
            }
        }
        verbs
    }

    /// Go `TestRedactArgsUseStringVerbs` (b3215c36538): a redacted argument
    /// is replaced with a string placeholder, so it must be formatted with a
    /// verb that can print a string.
    #[test]
    fn test_redact_args_use_string_verbs() {
        for entry in super::CATALOG {
            let message = &entry.message;
            if message.redact_arg_pos.is_empty() {
                continue;
            }
            let verbs = verbs(message.raw);
            for &pos in message.redact_arg_pos {
                assert!(pos < verbs.len(), "ErrCode {}: redacted argument {pos} out of range in {:?}", entry.code, message.raw);
                assert!(
                    matches!(verbs[pos], 's' | 'v' | 'q'),
                    "ErrCode {}: redacted argument {pos} uses %{} in {:?}",
                    entry.code,
                    verbs[pos],
                    message.raw
                );
            }
        }
    }
}
