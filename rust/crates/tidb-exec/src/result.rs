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

//! Result rows and byte-safe differential labels.

use tidb_datatype::Datum;

/// One result row: a value per projected column.
pub type Row = Vec<Datum>;

/// A query result: zero or more rows.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ResultSet {
    /// The result rows.
    pub rows: Vec<Row>,
    /// Whether the row order is significant (an `ORDER BY` was applied). When
    /// false, the label sorts rows so comparison is order-independent.
    pub ordered: bool,
}

/// The outcome of running one statement.
#[derive(Debug, Clone, PartialEq)]
pub enum Outcome {
    /// A statement with no result set.
    Done,
    /// A query and its result set.
    Rows(ResultSet),
}

impl ResultSet {
    /// Renders the result in the `gorun` oracle's byte-safe differential
    /// format: `RS:<row>;<row>;...`, each row `c1|c2|...` of MySQL string
    /// values. Valid UTF-8 cells keep their historical bytes. Invalid UTF-8
    /// uses `BYTES_HEX:<UPPERCASE HEX>`; a valid cell beginning with
    /// `BYTES_HEX:` or `TEXT:` is prefixed with `TEXT:` so the encoding is
    /// unambiguous. Rows are sorted (order-independent) unless an `ORDER BY`
    /// made the produced order significant.
    pub fn label(&self) -> String {
        let mut rows: Vec<String> = self
            .rows
            .iter()
            .map(|row| {
                row.iter()
                    .map(result_cell_label)
                    .collect::<Vec<_>>()
                    .join("|")
            })
            .collect();
        if !self.ordered {
            rows.sort();
        }
        format!("RS:{}", rows.join(";"))
    }
}

const RESULT_BYTES_HEX_PREFIX: &str = "BYTES_HEX:";
const RESULT_TEXT_ESCAPE_PREFIX: &str = "TEXT:";

fn result_cell_label(value: &Datum) -> String {
    if value.is_null() {
        return "<nil>".to_owned();
    }
    let Some(bytes) = value.as_raw_bytes() else {
        return value
            .sql_string()
            .expect("non-string Datum SQL rendering is always valid UTF-8");
    };
    match std::str::from_utf8(bytes) {
        Ok(text) if text.contains(['\r', '\n']) => {
            format!("{RESULT_BYTES_HEX_PREFIX}{}", uppercase_hex(bytes))
        }
        Ok(text)
            if text.starts_with(RESULT_BYTES_HEX_PREFIX)
                || text.starts_with(RESULT_TEXT_ESCAPE_PREFIX) =>
        {
            format!("{RESULT_TEXT_ESCAPE_PREFIX}{text}")
        }
        Ok(text) => text.to_string(),
        Err(_) => format!("{RESULT_BYTES_HEX_PREFIX}{}", uppercase_hex(bytes)),
    }
}

fn uppercase_hex(bytes: &[u8]) -> String {
    use std::fmt::Write;

    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(encoded, "{byte:02X}").expect("writing to String cannot fail");
    }
    encoded
}

#[cfg(test)]
mod tests {
    use super::{result_cell_label, ResultSet};
    use tidb_datatype::Datum;

    /// These are the exact vectors from `rust/difftests/gorun/main_test.go`.
    /// Keep this transport test next to the result owner: SQL coercion belongs
    /// to `tidb-expr`, while this layer must only preserve the already-produced
    /// bytes and keep marker-shaped text unambiguous.
    #[test]
    fn go_format_cell_vectors_preserve_and_escape_bytes() {
        let cases = [
            (Datum::Null, "<nil>"),
            (Datum::new_string("TiDB"), "TiDB"),
            (Datum::new_string(vec![b'a', 0, b'b']), "a\0b"),
            (Datum::new_string(vec![0xff, 0, b'A']), "BYTES_HEX:FF0041"),
            (Datum::new_string("a\nb"), "BYTES_HEX:610A62"),
            (Datum::new_string("a\rb"), "BYTES_HEX:610D62"),
            (Datum::new_string("BYTES_HEX:FF"), "TEXT:BYTES_HEX:FF"),
            (Datum::new_string("TEXT:value"), "TEXT:TEXT:value"),
        ];

        for (value, expected) in cases {
            assert_eq!(result_cell_label(&value), expected);
        }
    }

    #[test]
    fn result_set_label_sorts_only_unordered_rows() {
        let rows = vec![vec![Datum::new_string("z")], vec![Datum::new_string("a")]];
        assert_eq!(
            ResultSet {
                rows: rows.clone(),
                ordered: false,
            }
            .label(),
            "RS:a;z"
        );
        assert_eq!(
            ResultSet {
                rows,
                ordered: true,
            }
            .label(),
            "RS:z;a"
        );
    }
}
