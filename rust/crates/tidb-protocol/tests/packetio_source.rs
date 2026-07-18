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

#![allow(missing_docs)]

use std::io::Cursor;

use tidb_protocol::{
    CompressedHeader, CompressedReader, CompressedWriter, CompressionAlgorithm, PacketError,
    PacketIoReader, PacketIoWriter, PacketReader, PacketWriter, DEFAULT_MAX_ALLOWED_PACKET,
    MAX_COMPRESSED_BATCH_SIZE, MAX_PAYLOAD_LEN, MIN_COMPRESS_LENGTH,
};

const QUERY_PAYLOAD: &[u8] =
    b"\x03SELECT 1 /* abcdefghijklmnopqrstuvwxyz_abcdefghijklmnopqrstuvwxyz */";

fn compressed_long_expected() -> Vec<u8> {
    let mut expected = b"\x03SELECT \"".to_vec();
    expected.resize(151, b'A');
    expected.push(b'"');
    expected
}

fn read_all_compressed(bytes: Vec<u8>, algorithm: CompressionAlgorithm, length: usize) -> Vec<u8> {
    let mut reader = CompressedReader::new(Cursor::new(bytes), algorithm).unwrap();
    let mut decoded = vec![0; length];
    let mut position = 0;
    while position < decoded.len() {
        position += reader.read_bytes(&mut decoded[position..]).unwrap();
    }
    decoded
}

#[test]
fn test_packet_io_write() {
    let mut writer =
        PacketIoWriter::new(Cursor::new(Vec::new()), CompressionAlgorithm::None).unwrap();
    writer.write_packet_buffer(&[0, 0, 0, 0, 1, 2, 3]).unwrap();
    writer.flush().unwrap();
    assert_eq!(writer.into_inner().into_inner(), vec![3, 0, 0, 0, 1, 2, 3]);

    let mut writer =
        PacketIoWriter::new(Cursor::new(Vec::new()), CompressionAlgorithm::None).unwrap();
    writer
        .write_packet_buffer(&vec![0; MAX_PAYLOAD_LEN + 4])
        .unwrap();
    writer.flush().unwrap();
    let encoded = writer.into_inner().into_inner();
    assert_eq!(&encoded[..4], &[0xff, 0xff, 0xff, 0]);
    assert_eq!(&encoded[MAX_PAYLOAD_LEN + 4..], &[0, 0, 0, 1]);
}

#[test]
fn test_packet_io_write_compressed_batches_at_one_mib() {
    let mut writer =
        PacketIoWriter::new(Cursor::new(Vec::new()), CompressionAlgorithm::Zlib).unwrap();
    writer
        .write_packet_buffer(&vec![b'A'; 16 * 1024 * 1024])
        .unwrap();
    writer.flush().unwrap();
    let encoded = writer.into_inner().into_inner();
    let header = CompressedHeader::decode(encoded[..7].try_into().unwrap());
    assert_eq!(header.sequence, 0);
    assert_eq!(header.uncompressed_len, MAX_COMPRESSED_BATCH_SIZE);
    assert!(header.compressed_len < header.uncompressed_len);
    assert!(encoded.len() >= 7 + header.compressed_len);
}

#[test]
fn test_packet_io_read_uncompressed() {
    let mut reader =
        PacketIoReader::new(Cursor::new(vec![1, 0, 0, 0, 1]), CompressionAlgorithm::None).unwrap();
    assert_eq!(reader.read_packet().unwrap(), vec![1]);
    assert_eq!(reader.sequence(), 1);

    let mut encoded = vec![0; MAX_PAYLOAD_LEN + 9];
    encoded[..4].copy_from_slice(&[0xff, 0xff, 0xff, 0]);
    encoded[MAX_PAYLOAD_LEN + 4..MAX_PAYLOAD_LEN + 8].copy_from_slice(&[1, 0, 0, 1]);
    encoded[MAX_PAYLOAD_LEN + 8] = 0x0a;
    let mut reader = PacketIoReader::new(Cursor::new(encoded), CompressionAlgorithm::None).unwrap();
    let payload = reader.read_packet().unwrap();
    assert_eq!(reader.sequence(), 2);
    assert_eq!(payload.len(), MAX_PAYLOAD_LEN + 1);
    assert_eq!(payload[MAX_PAYLOAD_LEN], 0x0a);
}

#[test]
fn test_packet_io_read_compressed_short() {
    let bytes = vec![
        0x27, 0, 0, 0, 0, 0, 0, 0x23, 0, 0, 0, 0x03, 0, 0x01, 0x73, 0x65, 0x6c, 0x65, 0x63, 0x74,
        0x20, 0x40, 0x40, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x5f, 0x63, 0x6f, 0x6d, 0x6d,
        0x65, 0x6e, 0x74, 0x20, 0x6c, 0x69, 0x6d, 0x69, 0x74, 0x20, 0x31,
    ];
    let mut reader = PacketIoReader::new(Cursor::new(bytes), CompressionAlgorithm::Zlib).unwrap();
    assert_eq!(
        reader.read_packet().unwrap(),
        b"\x03\x00\x01select @@version_comment limit 1"
    );
    assert_eq!(
        (reader.sequence(), reader.compressed_sequence()),
        (1, Some(1))
    );
}

#[test]
fn test_packet_io_read_zlib() {
    let bytes = vec![
        0x39, 0, 0, 0, 0x49, 0, 0, 0x78, 0x5e, 0x73, 0x65, 0x60, 0x60, 0x60, 0x0e, 0x76, 0xf5,
        0x71, 0x75, 0x0e, 0x51, 0x30, 0x54, 0xd0, 0xd7, 0x52, 0x48, 0x4c, 0x4a, 0x4e, 0x49, 0x4d,
        0x4b, 0xcf, 0xc8, 0xcc, 0xca, 0xce, 0xc9, 0xcd, 0xcb, 0x2f, 0x28, 0x2c, 0x2a, 0x2e, 0x29,
        0x2d, 0x2b, 0xaf, 0xa8, 0xac, 0x8a, 0xc7, 0x2d, 0xa5, 0xa0, 0xa5, 0x0f, 0, 0x59, 0xd8,
        0x1a, 0x09,
    ];
    let mut reader = PacketIoReader::new(Cursor::new(bytes), CompressionAlgorithm::Zlib).unwrap();
    assert_eq!(reader.read_packet().unwrap(), QUERY_PAYLOAD);
}

#[test]
fn test_packet_io_read_zstd() {
    let bytes = vec![
        0x40, 0, 0, 0, 0x49, 0, 0, 0x28, 0xb5, 0x2f, 0xfd, 0x20, 0x49, 0xbd, 0x01, 0, 0xf4, 0x02,
        0x45, 0, 0, 0, 0x03, 0x53, 0x45, 0x4c, 0x45, 0x43, 0x54, 0x20, 0x31, 0x20, 0x2f, 0x2a,
        0x20, 0x61, 0x62, 0x63, 0x64, 0x65, 0x66, 0x67, 0x68, 0x69, 0x6a, 0x6b, 0x6c, 0x6d, 0x6e,
        0x6f, 0x70, 0x71, 0x72, 0x73, 0x74, 0x75, 0x76, 0x77, 0x78, 0x79, 0x7a, 0x5f, 0x20, 0x2a,
        0x2f, 0x01, 0, 0x74, 0x7b, 0x96, 0x01,
    ];
    let mut reader = PacketIoReader::new(Cursor::new(bytes), CompressionAlgorithm::Zstd).unwrap();
    assert_eq!(reader.read_packet().unwrap(), QUERY_PAYLOAD);
}

#[test]
fn test_compressed_writer_short_and_threshold() {
    for length in [10, MIN_COMPRESS_LENGTH] {
        let payload = vec![b'x'; length];
        let mut writer =
            CompressedWriter::new(Cursor::new(Vec::new()), CompressionAlgorithm::Zlib).unwrap();
        writer.write_bytes(&payload).unwrap();
        writer.flush_envelope().unwrap();
        let encoded = writer.into_inner().into_inner();
        assert_eq!(
            CompressedHeader::decode(encoded[..7].try_into().unwrap()).uncompressed_len,
            0
        );
        assert_eq!(&encoded[7..], payload);
    }

    let payload = vec![b'x'; MIN_COMPRESS_LENGTH + 1];
    let mut writer =
        CompressedWriter::new(Cursor::new(Vec::new()), CompressionAlgorithm::Zlib).unwrap();
    writer.write_bytes(&payload).unwrap();
    writer.flush_envelope().unwrap();
    let encoded = writer.into_inner().into_inner();
    assert_eq!(
        CompressedHeader::decode(encoded[..7].try_into().unwrap()).uncompressed_len,
        51
    );
}

#[test]
fn compressed_batch_flushes_only_when_input_crosses_one_mib() {
    let mut writer =
        CompressedWriter::new(Cursor::new(Vec::new()), CompressionAlgorithm::Zlib).unwrap();
    writer
        .write_bytes(&vec![b'x'; MAX_COMPRESSED_BATCH_SIZE])
        .unwrap();
    assert!(writer.get_ref().get_ref().is_empty());
    writer.write_bytes(b"y").unwrap();
    assert!(!writer.get_ref().get_ref().is_empty());
    assert_eq!(writer.compressed_sequence(), 1);
}

#[test]
fn test_compressed_writer_long_zlib_and_zstd() {
    for (algorithm, payload, expected_len) in [
        (
            CompressionAlgorithm::Zlib,
            b"test_zlib test_zlib test_zlib test_zlib test_zlib test_zlib test_zlib".as_slice(),
            None,
        ),
        (
            CompressionAlgorithm::Zstd,
            b"test_zstd test_zstd test_zstd test_zstd test_zstd test_zstd test_zstd".as_slice(),
            None,
        ),
    ] {
        let mut writer = CompressedWriter::new(Cursor::new(Vec::new()), algorithm).unwrap();
        writer.write_bytes(payload).unwrap();
        writer.flush_envelope().unwrap();
        let encoded = writer.into_inner().into_inner();
        let header = CompressedHeader::decode(encoded[..7].try_into().unwrap());
        assert_eq!(header.sequence, 0);
        assert_eq!(header.uncompressed_len, 69);
        assert_eq!(header.compressed_len, encoded.len() - 7);
        assert!(header.compressed_len < header.uncompressed_len);
        if let Some(expected_len) = expected_len {
            assert_eq!(header.compressed_len, expected_len);
        }
        assert_eq!(
            read_all_compressed(encoded, algorithm, payload.len()),
            payload
        );
    }
}

#[test]
fn test_compressed_reader_short() {
    let bytes = vec![
        0x25, 0, 0, 0, 0, 0, 0, 0x21, 0, 0, 0, 0x03, 0x73, 0x65, 0x6c, 0x65, 0x63, 0x74, 0x20,
        0x40, 0x40, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x5f, 0x63, 0x6f, 0x6d, 0x6d, 0x65,
        0x6e, 0x74, 0x20, 0x6c, 0x69, 0x6d, 0x69, 0x74, 0x20, 0x31,
    ];
    let decoded = read_all_compressed(bytes, CompressionAlgorithm::Zlib, 37);
    assert_eq!(&decoded[..4], &[0x21, 0, 0, 0]);
    assert_eq!(&decoded[4..], b"\x03select @@version_comment limit 1");
}

#[test]
fn test_compressed_reader_long_zlib_and_zstd() {
    let zlib = vec![
        0x19, 0, 0, 0, 0x9c, 0, 0, 0x78, 0x5e, 0x9b, 0xc1, 0xc0, 0xc0, 0xc0, 0x1c, 0xec, 0xea,
        0xe3, 0xea, 0x1c, 0xa2, 0xa0, 0xe4, 0x38, 0xa8, 0x80, 0x12, 0, 0xbe, 0xe6, 0x26, 0xce,
    ];
    let zstd = vec![
        0x1f, 0, 0, 0, 0x9c, 0, 0, 0x28, 0xb5, 0x2f, 0xfd, 0x20, 0x9c, 0xb5, 0, 0, 0x78, 0x98, 0,
        0, 0, 0x03, 0x53, 0x45, 0x4c, 0x45, 0x43, 0x54, 0x20, 0x22, 0x41, 0x22, 0x01, 0, 0x0a,
        0x0a, 0x28, 0x01,
    ];
    for (algorithm, bytes) in [
        (CompressionAlgorithm::Zlib, zlib),
        (CompressionAlgorithm::Zstd, zstd),
    ] {
        let decoded = read_all_compressed(bytes, algorithm, 156);
        assert_eq!(&decoded[..4], &[0x98, 0, 0, 0]);
        assert_eq!(&decoded[4..], compressed_long_expected());
    }
}

#[test]
fn test_inner_and_compressed_sequences_are_independent_then_flush_syncs() {
    let mut writer =
        PacketIoWriter::new(Cursor::new(Vec::new()), CompressionAlgorithm::Zlib).unwrap();
    writer.set_sequence(7);
    writer.set_compressed_sequence(23);
    writer.write_packet(b"select 1").unwrap();
    assert_eq!(
        (writer.sequence(), writer.compressed_sequence()),
        (8, Some(23))
    );
    writer.flush().unwrap();
    assert_eq!(
        (writer.sequence(), writer.compressed_sequence()),
        (24, Some(24))
    );
    assert_eq!(writer.get_ref().get_ref()[3], 23);
}

#[test]
fn test_sub_header_with_wrong_sequence_number() {
    let bytes = vec![
        0x0e, 0, 0, 0, 0, 0, 0, 0x0a, 0, 0, 0x01, 0x03, 0x73, 0x65, 0x6c, 0x65, 0x63, 0x74, 0x20,
        0x31, 0x3b,
    ];
    let mut reader = PacketIoReader::new(Cursor::new(bytes), CompressionAlgorithm::Zlib).unwrap();
    assert_eq!(reader.read_packet().unwrap(), b"\x03select 1;");
    assert_eq!(
        (reader.sequence(), reader.compressed_sequence()),
        (1, Some(1))
    );
}

#[test]
fn uncompressed_inner_and_compressed_outer_sequence_mismatches_remain_errors() {
    let mut reader = PacketIoReader::new(
        Cursor::new(vec![1, 0, 0, 1, b'x']),
        CompressionAlgorithm::None,
    )
    .unwrap();
    assert!(matches!(
        reader.read_packet(),
        Err(PacketError::InvalidSequence {
            expected: 0,
            received: 1
        })
    ));

    let mut reader = PacketIoReader::new(
        Cursor::new(vec![0, 0, 0, 1, 0, 0, 0]),
        CompressionAlgorithm::Zlib,
    )
    .unwrap();
    assert!(matches!(
        reader.read_packet(),
        Err(PacketError::InvalidCompressedSequence {
            expected: 0,
            received: 1
        })
    ));
}

#[test]
fn empty_envelope_does_not_end_the_compressed_inner_stream() {
    let bytes = vec![
        // Empty outer envelope, sequence 0.
        0, 0, 0, 0, 0, 0, 0,
        // Verbatim outer envelope containing an empty inner packet, sequence 1.
        4, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0,
    ];
    let mut reader = PacketIoReader::new(Cursor::new(bytes), CompressionAlgorithm::Zlib).unwrap();
    assert_eq!(reader.read_packet().unwrap(), Vec::<u8>::new());
    assert_eq!(
        (reader.sequence(), reader.compressed_sequence()),
        (1, Some(2))
    );
}

#[test]
fn clean_eof_and_truncated_headers_remain_distinct() {
    let mut reader =
        PacketIoReader::new(Cursor::new(Vec::new()), CompressionAlgorithm::None).unwrap();
    assert!(matches!(
        reader.read_packet(),
        Err(PacketError::EndOfStream)
    ));

    let mut reader =
        PacketIoReader::new(Cursor::new(vec![1, 0]), CompressionAlgorithm::None).unwrap();
    assert!(matches!(
        reader.read_packet(),
        Err(PacketError::Io(error)) if error.kind() == std::io::ErrorKind::UnexpectedEof
    ));

    let mut reader =
        PacketIoReader::new(Cursor::new(Vec::new()), CompressionAlgorithm::Zlib).unwrap();
    assert!(matches!(
        reader.read_packet(),
        Err(PacketError::EndOfStream)
    ));

    let mut reader =
        PacketIoReader::new(Cursor::new(vec![1, 0]), CompressionAlgorithm::Zlib).unwrap();
    assert!(matches!(
        reader.read_packet(),
        Err(PacketError::Io(error)) if error.kind() == std::io::ErrorKind::UnexpectedEof
    ));
}

#[test]
fn packet_limit_is_checked_after_header_sequence_and_before_payload_read() {
    let mut reader =
        PacketIoReader::new(Cursor::new(vec![3, 0, 0, 0]), CompressionAlgorithm::None).unwrap();
    reader.set_max_allowed_packet(2);
    assert!(matches!(
        reader.read_packet(),
        Err(PacketError::PacketTooLarge {
            accumulated: 3,
            max_allowed: 2
        })
    ));
    assert_eq!(reader.sequence(), 1);
}

#[test]
fn legacy_uncompressed_facades_and_sequence_wrap_remain_compatible() {
    let mut writer = PacketWriter::with_sequence(Cursor::new(Vec::new()), u8::MAX);
    writer.write_packet(b"ok").unwrap();
    assert_eq!(writer.sequence(), 0);
    let encoded = writer.into_inner().into_inner();
    let mut reader = PacketReader::new(Cursor::new(encoded));
    reader.set_sequence(u8::MAX);
    assert_eq!(reader.read_packet().unwrap(), b"ok");
    assert_eq!(reader.sequence(), 0);
    assert_eq!(DEFAULT_MAX_ALLOWED_PACKET, 64 << 20);
    assert_eq!(MAX_COMPRESSED_BATCH_SIZE, 1 << 20);
}
