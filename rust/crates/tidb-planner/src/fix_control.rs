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

//! Optimizer fix-control parsing from `pkg/planner/util/fixcontrol/set.go`.
//!
//! This leaf owns only the source's text-to-map parser. Session variables,
//! warning plumbing, and typed fix-control getters remain outside this
//! dependency-closed Rust boundary.

use std::{collections::BTreeMap, fmt};

/// Parsed fix-control assignments and duplicate-key warnings.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParsedFixControls {
    /// The final value for each fix-control number.
    pub values: BTreeMap<u64, String>,
    /// Warnings emitted when a key is assigned different values repeatedly.
    pub warnings: Vec<String>,
}

/// Errors emitted by the source-shaped fix-control parser.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ParseError {
    /// An assignment did not contain a colon.
    MissingColon,
    /// The text before a colon was not an unsigned decimal key.
    InvalidKey,
    /// A quoted value did not contain its closing quote.
    MissingQuote,
}

impl fmt::Display for ParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::MissingColon => "invalid fix control: expected colon not found",
            Self::InvalidKey => "invalid fix control: invalid key",
            Self::MissingQuote => "invalid fix control: expected quote not found",
        };
        formatter.write_str(message)
    }
}

impl std::error::Error for ParseError {}

/// Parses comma-separated optimizer fix-control assignments.
///
/// This follows the source's intentionally small grammar: values may be
/// unquoted (trimmed through the next comma) or quoted with either quote
/// character, and a repeated key replaces the previous value while warning
/// only when the value changed.
pub fn parse_to_map(input: &str) -> Result<ParsedFixControls, ParseError> {
    let mut values = BTreeMap::new();
    let mut warnings = Vec::new();
    let mut remaining = input;

    while !remaining.is_empty() {
        let colon = remaining.find(':').ok_or(ParseError::MissingColon)?;
        let key_text = remaining[..colon].trim();
        let key = key_text
            .strip_prefix('+')
            .unwrap_or(key_text)
            .parse::<u64>()
            .map_err(|_| ParseError::InvalidKey)?;
        remaining = remaining[colon + 1..].trim();

        let mut value = String::new();
        if let Some(quote) = remaining
            .as_bytes()
            .first()
            .copied()
            .filter(|byte| *byte == b'\'' || *byte == b'"')
        {
            let quote = char::from(quote);
            let closing = remaining[1..].find(quote).ok_or(ParseError::MissingQuote)?;
            let end = closing + 1;
            value.push_str(&remaining[1..end]);
            remaining = &remaining[end + 1..];
        }

        let end = remaining.find(',').unwrap_or(remaining.len());
        let next = remaining
            .find(',')
            .map_or(remaining.len(), |comma| comma + 1);
        if value.is_empty() {
            value.push_str(remaining[..end].trim());
        }

        if let Some(previous) = values.insert(key, value.clone()) {
            if previous != value {
                warnings.push(format!(
                    "repeated assignment for fix control: {key}. existing value: {previous:?}. new value: {value:?}."
                ));
            }
        }
        remaining = remaining[next..].trim();
    }

    Ok(ParsedFixControls { values, warnings })
}
