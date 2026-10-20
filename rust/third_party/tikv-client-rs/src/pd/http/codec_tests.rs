// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use super::*;

// Go `TestBytesCodec`.
#[test]
fn test_bytes_codec() {
    let inputs: Vec<(Vec<u8>, Vec<u8>)> = vec![
        (vec![], vec![0, 0, 0, 0, 0, 0, 0, 0, 247]),
        (vec![0], vec![0, 0, 0, 0, 0, 0, 0, 0, 248]),
        (vec![1, 2, 3], vec![1, 2, 3, 0, 0, 0, 0, 0, 250]),
        (vec![1, 2, 3, 0], vec![1, 2, 3, 0, 0, 0, 0, 0, 251]),
        (vec![1, 2, 3, 4, 5, 6, 7], vec![1, 2, 3, 4, 5, 6, 7, 0, 254]),
        (
            vec![0, 0, 0, 0, 0, 0, 0, 0],
            vec![0, 0, 0, 0, 0, 0, 0, 0, 255, 0, 0, 0, 0, 0, 0, 0, 0, 247],
        ),
        (
            vec![1, 2, 3, 4, 5, 6, 7, 8],
            vec![1, 2, 3, 4, 5, 6, 7, 8, 255, 0, 0, 0, 0, 0, 0, 0, 0, 247],
        ),
        (
            vec![1, 2, 3, 4, 5, 6, 7, 8, 9],
            vec![1, 2, 3, 4, 5, 6, 7, 8, 255, 9, 0, 0, 0, 0, 0, 0, 0, 248],
        ),
    ];

    for (enc, dec) in inputs {
        let b = encode_bytes(&enc);
        assert_eq!(dec, b);

        let d = decode_bytes(&b).unwrap();
        assert_eq!(enc, d);
    }

    // Test error decode.
    let err_inputs: Vec<Vec<u8>> = vec![
        vec![1, 2, 3, 4],
        vec![0, 0, 0, 0, 0, 0, 0, 247],
        vec![0, 0, 0, 0, 0, 0, 0, 0, 246],
        vec![0, 0, 0, 0, 0, 0, 0, 1, 247],
        vec![1, 2, 3, 4, 5, 6, 7, 8, 0],
        vec![1, 2, 3, 4, 5, 6, 7, 8, 255, 1],
        vec![1, 2, 3, 4, 5, 6, 7, 8, 255, 1, 2, 3, 4, 5, 6, 7, 8],
        vec![1, 2, 3, 4, 5, 6, 7, 8, 255, 1, 2, 3, 4, 5, 6, 7, 8, 255],
        vec![1, 2, 3, 4, 5, 6, 7, 8, 255, 1, 2, 3, 4, 5, 6, 7, 8, 0],
    ];

    for input in err_inputs {
        assert!(decode_bytes(&input).is_err(), "{input:?}");
    }
}

// The Go standard-library encoders this package hand-rolls have no Go test
// in this package; pin the exact Go outputs they replace.
#[test]
fn test_go_stdlib_encoders() {
    assert_eq!(query_escape(b"a b+c/~*_.-"), "a+b%2Bc%2F~%2A_.-");
    assert_eq!(query_escape(&[0xff, 0x00]), "%FF%00");
    assert_eq!(hex_encode(&[0x00, 0xab, 0x10]), "00ab10");
    assert_eq!(hex_decode("00AB10").unwrap(), vec![0x00, 0xab, 0x10]);
    assert_eq!(
        hex_decode("abc").unwrap_err().to_string(),
        "encoding/hex: odd length hex string"
    );
    assert_eq!(
        hex_decode("zz").unwrap_err().to_string(),
        "encoding/hex: invalid byte: U+007A 'z'"
    );
    let raws: [&[u8]; 6] = [b"", b"f", b"fo", b"foo", b"foob", b"\x00\xff\x10"];
    for raw in raws {
        assert_eq!(base64_decode(&base64_encode(raw)).unwrap(), raw);
    }
    assert_eq!(base64_encode(b"fo"), "Zm8=");
    assert!(base64_decode("Zm8").is_err());
    assert_eq!(go_quote(&[1, b'a', b'"', 0xff]), "\"\\x01a\\\"\\xff\"");
}
