use super::*;
use base64::Engine;
use serde_json::Value;

/// Go `parser_test.go`: each named test's inputs, captured through the real
/// `ChunkParser` in both SQL modes by zzprobe/e05.TestSQLParserCapture.
fn captured_cases(name: &str) {
    let mut count = 0;
    let mut errors = Vec::new();
    for line in include_str!("../testdata/sql_parser_go.jsonl").lines() {
        let c: Value = serde_json::from_str(line).unwrap();
        if c["test"] != name {
            continue;
        }
        count += 1;
        let bytes = |v: &Value| {
            v.as_str()
                .map(|s| base64::engine::general_purpose::STANDARD.decode(s).unwrap())
                .unwrap_or_default()
        };
        let mut parser = ChunkParser::with_block_size(
            bytes(&c["input"]),
            c["no_backslash"].as_bool().unwrap(),
            c["block_size"].as_u64().unwrap() as usize,
        );
        if let Some(rows) = c["rows"].as_array() {
            for r in rows {
                parser.read_row().unwrap_or_else(|e| panic!("{c}: {e}"));
                assert_eq!(
                    parser.pos(),
                    (r["offset"].as_i64().unwrap(), r["id"].as_i64().unwrap()),
                    "{c}"
                );
                assert_eq!(
                    parser.last_row().length,
                    r["length"].as_u64().unwrap() as usize,
                    "{c}"
                );
                let columns: Vec<String> = r["columns"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .map(|s| s.as_str().unwrap().to_owned())
                    .collect();
                assert_eq!(parser.columns(), columns, "{c}");
                let values: Vec<Datum> = r["values"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .map(|v| {
                        let b = bytes(&v["bytes"]);
                        match v["kind"].as_u64().unwrap() {
                            0 => Datum::Null,
                            1 => Datum::Int(String::from_utf8(b).unwrap().parse().unwrap()),
                            2 => Datum::UInt(String::from_utf8(b).unwrap().parse().unwrap()),
                            5 => Datum::new_string_datum(b),
                            7 => Datum::new_binary_literal(tidb_datatype::BinaryLiteral::from(b)),
                            kind => panic!("unexpected kind {kind}"),
                        }
                    })
                    .collect();
                assert_eq!(parser.last_row().row, values, "{c}");
            }
        }
        let result = parser
            .read_row()
            .map(|_| "unexpected row".to_owned())
            .unwrap_or_else(|e| e.to_string());
        if result != c["error"].as_str().unwrap() {
            errors.push(format!(
                "input {:?}, no_backslash {}: Rust {:?}, Go {}",
                String::from_utf8_lossy(&bytes(&c["input"])),
                c["no_backslash"],
                result,
                c["error"]
            ));
        }
    }
    assert!(errors.is_empty(), "{}", errors.join("\n"));
    assert!(count > 0, "no capture for {name}");
}

#[test]
fn test_read_row() {
    captured_cases("TestReadRow");
}
#[test]
fn test_read_chunks() {
    captured_cases("TestReadChunks");
    let source=b"\n\t\tINSERT foo VALUES (1,2,3,4),(5,6,7,8),(9,10,11,12);\n\t\tINSERT foo VALUES (13,14,15,16),(17,18,19,20),(21,22,23,24),(25,26,27,28);\n\t\tINSERT foo VALUES (29,30,31,32),(33,34,35,36);\n\t";
    let chunks = read_chunks(&mut ChunkParser::new(source.to_vec(), false), 32).unwrap();
    assert_eq!(
        chunks
            .iter()
            .map(|c| (c.offset, c.end_offset, c.prev_row_id_max, c.row_id_max))
            .collect::<Vec<_>>(),
        vec![
            (0, 40, 0, 2),
            (40, 88, 2, 4),
            (88, 130, 4, 7),
            (130, 165, 7, 8),
            (165, 179, 8, 9)
        ]
    );
}
#[test]
fn test_nested_row() {
    captured_cases("TestNestedRow");
}
#[test]
fn test_various_syntax() {
    captured_cases("TestVariousSyntax");
}
#[test]
fn test_continuation() {
    captured_cases("TestContinuation");
}
#[test]
fn test_pseudo_keywords() {
    captured_cases("TestPseudoKeywords");
}
#[test]
fn test_syntax_error() {
    captured_cases("TestSyntaxError");
}
#[test]
fn test_more_syntax_error() {
    captured_cases("TestMoreSyntaxError");
}
#[test]
fn test_more_empty_files() {
    captured_cases("TestMoreEmptyFiles");
}
