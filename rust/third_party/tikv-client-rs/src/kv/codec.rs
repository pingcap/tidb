use std::io::Write;
use std::ptr;

use crate::internal_err;
use crate::Error;
use crate::Result;

const ENC_GROUP_SIZE: usize = 8;
const ENC_MARKER: u8 = 0xff;
const ENC_ASC_PADDING: [u8; ENC_GROUP_SIZE] = [0; ENC_GROUP_SIZE];
const ENC_DESC_PADDING: [u8; ENC_GROUP_SIZE] = [!0; ENC_GROUP_SIZE];
const SIGN_MASK: u64 = 0x8000_0000_0000_0000;
const NEGATIVE_TAG_END: u8 = 8;
const POSITIVE_TAG_START: u8 = 0xff - 8;

/// Returns the maximum encoded bytes size.
///
/// Duplicate from components/tikv_util/src/codec/bytes.rs.
pub fn max_encoded_bytes_size(n: usize) -> usize {
    (n / ENC_GROUP_SIZE + 1) * (ENC_GROUP_SIZE + 1)
}

pub trait BytesEncoder: Write {
    /// Refer: <https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format#memcomparable-format>
    ///
    /// Duplicate from components/tikv_util/src/codec/bytes.rs.
    fn encode_bytes(&mut self, key: &[u8], desc: bool) -> Result<()> {
        let len = key.len();
        let mut index = 0;
        let mut buf = [0; ENC_GROUP_SIZE];
        while index <= len {
            let remain = len - index;
            let mut pad: usize = 0;
            if remain > ENC_GROUP_SIZE {
                self.write_all(adjust_bytes_order(
                    &key[index..index + ENC_GROUP_SIZE],
                    desc,
                    &mut buf,
                ))?;
            } else {
                pad = ENC_GROUP_SIZE - remain;
                self.write_all(adjust_bytes_order(&key[index..], desc, &mut buf))?;
                if desc {
                    self.write_all(&ENC_DESC_PADDING[..pad])?;
                } else {
                    self.write_all(&ENC_ASC_PADDING[..pad])?;
                }
            }
            self.write_all(adjust_bytes_order(
                &[ENC_MARKER - (pad as u8)],
                desc,
                &mut buf,
            ))?;
            index += ENC_GROUP_SIZE;
        }
        Ok(())
    }
}

impl<T: Write> BytesEncoder for T {}

/// Appends the ascending memory-comparable encoding of `data` to `buffer`.
pub fn encode_bytes(buffer: &mut Vec<u8>, data: &[u8]) {
    buffer.reserve(max_encoded_bytes_size(data.len()));
    buffer
        .encode_bytes(data, false)
        .expect("writing to a Vec cannot fail");
}

/// Appends the descending memory-comparable encoding of `data` to `buffer`.
pub fn encode_bytes_desc(buffer: &mut Vec<u8>, data: &[u8]) {
    buffer.reserve(max_encoded_bytes_size(data.len()));
    buffer
        .encode_bytes(data, true)
        .expect("writing to a Vec cannot fail");
}

/// Decodes one ascending memory-comparable byte string into `output`.
///
/// `output` is cleared before decoding and the unconsumed input is returned.
pub fn decode_bytes<'a>(input: &'a [u8], output: &mut Vec<u8>) -> Result<&'a [u8]> {
    decode_order_bytes(input, output, false)
}

/// Decodes one descending memory-comparable byte string into `output`.
///
/// `output` is cleared before decoding and the unconsumed input is returned.
pub fn decode_bytes_desc<'a>(input: &'a [u8], output: &mut Vec<u8>) -> Result<&'a [u8]> {
    decode_order_bytes(input, output, true)
}

fn decode_order_bytes<'a>(input: &'a [u8], output: &mut Vec<u8>, desc: bool) -> Result<&'a [u8]> {
    output.clear();
    let mut offset = 0;
    loop {
        if input.len().saturating_sub(offset) < ENC_GROUP_SIZE + 1 {
            return Err(codec_error("insufficient bytes to decode value"));
        }

        let group = &input[offset..offset + ENC_GROUP_SIZE];
        let marker = input[offset + ENC_GROUP_SIZE];
        let pad_count = if desc {
            marker as usize
        } else {
            (ENC_MARKER - marker) as usize
        };
        if pad_count > ENC_GROUP_SIZE {
            return Err(codec_error("invalid marker byte"));
        }

        let real_group_size = ENC_GROUP_SIZE - pad_count;
        if desc {
            output.extend(group[..real_group_size].iter().map(|byte| !byte));
        } else {
            output.extend_from_slice(&group[..real_group_size]);
        }
        offset += ENC_GROUP_SIZE + 1;

        if pad_count != 0 {
            let expected_pad = if desc { ENC_MARKER } else { 0 };
            if group[real_group_size..]
                .iter()
                .any(|byte| *byte != expected_pad)
            {
                return Err(codec_error("invalid padding byte"));
            }
            return Ok(&input[offset..]);
        }
    }
}

fn adjust_bytes_order<'a>(bs: &'a [u8], desc: bool, buf: &'a mut [u8]) -> &'a [u8] {
    if desc {
        let mut buf_idx = 0;
        for &b in bs {
            buf[buf_idx] = !b;
            buf_idx += 1;
        }
        &buf[..buf_idx]
    } else {
        bs
    }
}

/// Decodes bytes which are encoded by `encode_bytes` before just in place without malloc.
///
/// Duplicate from components/tikv_util/src/codec/bytes.rs.
pub fn decode_bytes_in_place(data: &mut Vec<u8>, desc: bool) -> Result<()> {
    if data.is_empty() {
        return Ok(());
    }
    let mut write_offset = 0;
    let mut read_offset = 0;
    loop {
        let marker_offset = read_offset + ENC_GROUP_SIZE;
        if marker_offset >= data.len() {
            return Err(internal_err!("unexpected EOF, original key = {:?}", data));
        };

        unsafe {
            // it is semantically equivalent to C's memmove()
            // and the src and dest may overlap
            // if src == dest do nothing
            ptr::copy(
                data.as_ptr().add(read_offset),
                data.as_mut_ptr().add(write_offset),
                ENC_GROUP_SIZE,
            );
        }
        write_offset += ENC_GROUP_SIZE;
        // everytime make ENC_GROUP_SIZE + 1 elements as a decode unit
        read_offset += ENC_GROUP_SIZE + 1;

        // the last byte in decode unit is for marker which indicates pad size
        let marker = data[marker_offset];
        let pad_size = if desc {
            marker as usize
        } else {
            (ENC_MARKER - marker) as usize
        };

        if pad_size > 0 {
            if pad_size > ENC_GROUP_SIZE {
                return Err(internal_err!("invalid key padding"));
            }

            // check the padding pattern whether validate or not
            let padding_slice = if desc {
                &ENC_DESC_PADDING[..pad_size]
            } else {
                &ENC_ASC_PADDING[..pad_size]
            };
            if &data[write_offset - pad_size..write_offset] != padding_slice {
                return Err(internal_err!("invalid key padding"));
            }
            unsafe {
                data.set_len(write_offset - pad_size);
            }
            if desc {
                for k in data {
                    *k = !*k;
                }
            }
            return Ok(());
        }
    }
}

/// Maps a signed integer to the unsigned domain while preserving its ordering.
#[inline]
pub fn encode_int_to_cmp_uint(value: i64) -> u64 {
    (value as u64) ^ SIGN_MASK
}

/// Reverses [`encode_int_to_cmp_uint`].
#[inline]
pub fn decode_cmp_uint_to_int(value: u64) -> i64 {
    (value ^ SIGN_MASK) as i64
}

/// Appends the ascending memory-comparable encoding of `value` to `buffer`.
pub fn encode_int(buffer: &mut Vec<u8>, value: i64) {
    buffer.extend_from_slice(&encode_int_to_cmp_uint(value).to_be_bytes());
}

/// Appends the descending memory-comparable encoding of `value` to `buffer`.
pub fn encode_int_desc(buffer: &mut Vec<u8>, value: i64) {
    buffer.extend_from_slice(&(!encode_int_to_cmp_uint(value)).to_be_bytes());
}

/// Decodes one ascending memory-comparable signed integer.
pub fn decode_int(input: &[u8]) -> Result<(&[u8], i64)> {
    let (leftover, value) = decode_fixed_u64(input)?;
    Ok((leftover, decode_cmp_uint_to_int(value)))
}

/// Decodes one descending memory-comparable signed integer.
pub fn decode_int_desc(input: &[u8]) -> Result<(&[u8], i64)> {
    let (leftover, value) = decode_fixed_u64(input)?;
    Ok((leftover, decode_cmp_uint_to_int(!value)))
}

/// Appends the ascending memory-comparable encoding of `value` to `buffer`.
pub fn encode_uint(buffer: &mut Vec<u8>, value: u64) {
    buffer.extend_from_slice(&value.to_be_bytes());
}

/// Appends the descending memory-comparable encoding of `value` to `buffer`.
pub fn encode_uint_desc(buffer: &mut Vec<u8>, value: u64) {
    buffer.extend_from_slice(&(!value).to_be_bytes());
}

/// Decodes one ascending memory-comparable unsigned integer.
pub fn decode_uint(input: &[u8]) -> Result<(&[u8], u64)> {
    decode_fixed_u64(input)
}

/// Decodes one descending memory-comparable unsigned integer.
pub fn decode_uint_desc(input: &[u8]) -> Result<(&[u8], u64)> {
    let (leftover, value) = decode_fixed_u64(input)?;
    Ok((leftover, !value))
}

fn decode_fixed_u64(input: &[u8]) -> Result<(&[u8], u64)> {
    let encoded = input
        .get(..8)
        .ok_or_else(|| codec_error("insufficient bytes to decode value"))?;
    let value = u64::from_be_bytes(encoded.try_into().expect("slice length is checked"));
    Ok((&input[8..], value))
}

/// Appends the non-memory-comparable varint encoding of `value` to `buffer`.
pub fn encode_varint(buffer: &mut Vec<u8>, value: i64) {
    let mut unsigned = (value as u64) << 1;
    if value < 0 {
        unsigned = !unsigned;
    }
    encode_uvarint(buffer, unsigned);
}

/// Decodes one non-memory-comparable signed varint.
pub fn decode_varint(input: &[u8]) -> Result<(&[u8], i64)> {
    let (leftover, unsigned) = decode_uvarint(input)?;
    let mut value = (unsigned >> 1) as i64;
    if unsigned & 1 != 0 {
        value = !value;
    }
    Ok((leftover, value))
}

/// Appends the non-memory-comparable unsigned varint encoding of `value` to `buffer`.
pub fn encode_uvarint(buffer: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        buffer.push(value as u8 | 0x80);
        value >>= 7;
    }
    buffer.push(value as u8);
}

/// Decodes one non-memory-comparable unsigned varint.
pub fn decode_uvarint(input: &[u8]) -> Result<(&[u8], u64)> {
    let mut value = 0_u64;
    let mut shift = 0;
    for (index, byte) in input.iter().copied().enumerate() {
        if index == 10 {
            return Err(codec_error("value larger than 64 bits"));
        }
        if byte < 0x80 {
            if index == 9 && byte > 1 {
                return Err(codec_error("value larger than 64 bits"));
            }
            value |= u64::from(byte) << shift;
            return Ok((&input[index + 1..], value));
        }
        value |= u64::from(byte & 0x7f) << shift;
        shift += 7;
    }
    Err(codec_error("insufficient bytes to decode value"))
}

/// Appends the memory-comparable varint encoding of `value` to `buffer`.
pub fn encode_comparable_varint(buffer: &mut Vec<u8>, value: i64) {
    if value >= 0 {
        encode_comparable_uvarint(buffer, value as u64);
        return;
    }

    let value128 = i128::from(value);
    let length = (1..=8)
        .find(|length| value128 >= -((1_i128 << (length * 8)) - 1))
        .expect("every i64 fits in eight bytes");
    buffer.push(NEGATIVE_TAG_END - length as u8);
    buffer.extend_from_slice(&value.to_be_bytes()[8 - length..]);
}

/// Appends the memory-comparable unsigned varint encoding of `value` to `buffer`.
pub fn encode_comparable_uvarint(buffer: &mut Vec<u8>, value: u64) {
    if value <= u64::from(POSITIVE_TAG_START - NEGATIVE_TAG_END) {
        buffer.push(value as u8 + NEGATIVE_TAG_END);
        return;
    }

    let encoded = value.to_be_bytes();
    let length = encoded
        .iter()
        .position(|byte| *byte != 0)
        .map_or(1, |i| 8 - i);
    buffer.push(POSITIVE_TAG_START + length as u8);
    buffer.extend_from_slice(&encoded[8 - length..]);
}

/// Decodes one memory-comparable unsigned varint.
pub fn decode_comparable_uvarint(input: &[u8]) -> Result<(&[u8], u64)> {
    let first = *input
        .first()
        .ok_or_else(|| codec_error("insufficient bytes to decode value"))?;
    if first < NEGATIVE_TAG_END {
        return Err(codec_error("invalid bytes to decode value"));
    }
    if first <= POSITIVE_TAG_START {
        return Ok((&input[1..], u64::from(first - NEGATIVE_TAG_END)));
    }

    let length = usize::from(first - POSITIVE_TAG_START);
    let encoded = input
        .get(1..1 + length)
        .ok_or_else(|| codec_error("insufficient bytes to decode value"))?;
    let value = encoded
        .iter()
        .fold(0_u64, |value, byte| (value << 8) | u64::from(*byte));
    Ok((&input[1 + length..], value))
}

/// Decodes one memory-comparable signed varint.
///
/// For the single-byte non-negative form, the returned leftover intentionally starts at the
/// encoded byte. This matches the pinned client-go source behavior.
pub fn decode_comparable_varint(input: &[u8]) -> Result<(&[u8], i64)> {
    let first = *input
        .first()
        .ok_or_else(|| codec_error("insufficient bytes to decode value"))?;
    if (NEGATIVE_TAG_END..=POSITIVE_TAG_START).contains(&first) {
        return Ok((input, i64::from(first - NEGATIVE_TAG_END)));
    }

    let (length, initial) = if first < NEGATIVE_TAG_END {
        (usize::from(NEGATIVE_TAG_END - first), u64::MAX)
    } else {
        (usize::from(first - POSITIVE_TAG_START), 0)
    };
    let encoded = input
        .get(1..1 + length)
        .ok_or_else(|| codec_error("insufficient bytes to decode value"))?;
    let value = encoded
        .iter()
        .fold(initial, |value, byte| (value << 8) | u64::from(*byte));
    if (first > POSITIVE_TAG_START && value > i64::MAX as u64)
        || (first < NEGATIVE_TAG_END && value <= i64::MAX as u64)
    {
        return Err(codec_error("invalid bytes to decode value"));
    }
    Ok((&input[1 + length..], value as i64))
}

fn codec_error(message: &str) -> Error {
    Error::InternalError {
        message: message.to_owned(),
    }
}

#[cfg(test)]
pub mod test {
    use super::*;

    fn encoded_bytes(bs: &[u8]) -> Vec<u8> {
        encode_order_bytes(bs, false)
    }

    fn encoded_bytes_desc(bs: &[u8]) -> Vec<u8> {
        encode_order_bytes(bs, true)
    }

    fn encode_order_bytes(bs: &[u8], desc: bool) -> Vec<u8> {
        let cap = max_encoded_bytes_size(bs.len());
        let mut encoded = Vec::with_capacity(cap);
        encoded.encode_bytes(bs, desc).unwrap();
        encoded
    }

    #[test]
    fn test_enc_dec_bytes() {
        let pairs = vec![
            (
                vec![],
                vec![0, 0, 0, 0, 0, 0, 0, 0, 247],
                vec![255, 255, 255, 255, 255, 255, 255, 255, 8],
            ),
            (
                vec![0],
                vec![0, 0, 0, 0, 0, 0, 0, 0, 248],
                vec![255, 255, 255, 255, 255, 255, 255, 255, 7],
            ),
            (
                vec![1, 2, 3],
                vec![1, 2, 3, 0, 0, 0, 0, 0, 250],
                vec![254, 253, 252, 255, 255, 255, 255, 255, 5],
            ),
            (
                vec![1, 2, 3, 0],
                vec![1, 2, 3, 0, 0, 0, 0, 0, 251],
                vec![254, 253, 252, 255, 255, 255, 255, 255, 4],
            ),
            (
                vec![1, 2, 3, 4, 5, 6, 7],
                vec![1, 2, 3, 4, 5, 6, 7, 0, 254],
                vec![254, 253, 252, 251, 250, 249, 248, 255, 1],
            ),
            (
                vec![0, 0, 0, 0, 0, 0, 0, 0],
                vec![0, 0, 0, 0, 0, 0, 0, 0, 255, 0, 0, 0, 0, 0, 0, 0, 0, 247],
                vec![
                    255, 255, 255, 255, 255, 255, 255, 255, 0, 255, 255, 255, 255, 255, 255, 255,
                    255, 8,
                ],
            ),
            (
                vec![1, 2, 3, 4, 5, 6, 7, 8],
                vec![1, 2, 3, 4, 5, 6, 7, 8, 255, 0, 0, 0, 0, 0, 0, 0, 0, 247],
                vec![
                    254, 253, 252, 251, 250, 249, 248, 247, 0, 255, 255, 255, 255, 255, 255, 255,
                    255, 8,
                ],
            ),
            (
                vec![1, 2, 3, 4, 5, 6, 7, 8, 9],
                vec![1, 2, 3, 4, 5, 6, 7, 8, 255, 9, 0, 0, 0, 0, 0, 0, 0, 248],
                vec![
                    254, 253, 252, 251, 250, 249, 248, 247, 0, 246, 255, 255, 255, 255, 255, 255,
                    255, 7,
                ],
            ),
        ];

        for (source, mut asc, mut desc) in pairs {
            assert_eq!(encoded_bytes(&source), asc);
            assert_eq!(encoded_bytes_desc(&source), desc);
            decode_bytes_in_place(&mut asc, false).unwrap();
            assert_eq!(source, asc);
            decode_bytes_in_place(&mut desc, true).unwrap();
            assert_eq!(source, desc);
        }
    }

    #[test]
    fn test_bytes_append_leftover_and_invalid_inputs() {
        let mut encoded = vec![42];
        encode_bytes(&mut encoded, b"12345678");
        encoded.extend_from_slice(b"leftover");
        assert_eq!(encoded[0], 42);

        let mut decoded = vec![99];
        let leftover = decode_bytes(&encoded[1..], &mut decoded).unwrap();
        assert_eq!(decoded, b"12345678");
        assert_eq!(leftover, b"leftover");

        let mut output = Vec::new();
        assert!(decode_bytes(&[], &mut output).is_err());
        assert!(decode_bytes(&[0; 9], &mut output).is_err());
        let mut invalid_padding = vec![1, 2, 3, 9, 0, 0, 0, 0, 250];
        assert!(decode_bytes(&invalid_padding, &mut output).is_err());

        invalid_padding = vec![!1, !2, !3, 0, 0xff, 0xff, 0xff, 0xff, 5];
        assert!(decode_bytes_desc(&invalid_padding, &mut output).is_err());
    }

    #[test]
    fn test_fixed_width_number_codec() {
        let signed = [i64::MIN, -1, 0, 1, i64::MAX];
        let mut previous = None;
        for value in signed {
            let mut encoded = vec![7];
            encode_int(&mut encoded, value);
            encoded.push(9);
            let (leftover, decoded) = decode_int(&encoded[1..]).unwrap();
            assert_eq!(decoded, value);
            assert_eq!(leftover, [9]);
            if let Some(previous) = previous.replace(encoded[1..9].to_vec()) {
                assert!(previous.as_slice() < &encoded[1..9]);
            }

            let mut desc = Vec::new();
            encode_int_desc(&mut desc, value);
            assert_eq!(decode_int_desc(&desc).unwrap().1, value);
        }
        assert!(decode_int(&[0; 7]).is_err());

        for value in [0, 1, u64::MAX] {
            let mut encoded = Vec::new();
            encode_uint(&mut encoded, value);
            assert_eq!(decode_uint(&encoded).unwrap().1, value);
            let mut desc = Vec::new();
            encode_uint_desc(&mut desc, value);
            assert_eq!(decode_uint_desc(&desc).unwrap().1, value);
        }
    }

    #[test]
    fn test_non_comparable_varints() {
        for value in [i64::MIN, -256, -1, 0, 1, 256, i64::MAX] {
            let mut encoded = vec![3];
            encode_varint(&mut encoded, value);
            encoded.push(4);
            let (leftover, decoded) = decode_varint(&encoded[1..]).unwrap();
            assert_eq!(decoded, value);
            assert_eq!(leftover, [4]);
        }
        let mut negative_one = Vec::new();
        encode_varint(&mut negative_one, -1);
        assert_eq!(negative_one, [1]);

        for value in [0, 127, 128, 256, u64::MAX] {
            let mut encoded = Vec::new();
            encode_uvarint(&mut encoded, value);
            assert_eq!(decode_uvarint(&encoded).unwrap().1, value);
        }
        assert!(decode_uvarint(&[0x80]).is_err());
        assert!(decode_uvarint(&[0x80; 11]).is_err());
        let oversized = [0xff; 9].into_iter().chain([2]).collect::<Vec<_>>();
        assert!(decode_uvarint(&oversized).is_err());
    }

    #[test]
    fn test_comparable_varints() {
        let signed = [
            i64::MIN,
            -0x1_0000_0000_0000,
            -0x1_0000_0000,
            -0x1_0000,
            -256,
            -255,
            -1,
            0,
            1,
            239,
            240,
            255,
            256,
            i64::MAX,
        ];
        let mut encodings = Vec::new();
        for value in signed {
            let mut encoded = Vec::new();
            encode_comparable_varint(&mut encoded, value);
            let (leftover, decoded) = decode_comparable_varint(&encoded).unwrap();
            assert_eq!(decoded, value);
            if (0..=239).contains(&value) {
                assert_eq!(leftover, encoded.as_slice());
            } else {
                assert!(leftover.is_empty());
            }
            encodings.push(encoded);
        }
        assert!(encodings.windows(2).all(|pair| pair[0] < pair[1]));
        assert_eq!(encodings[5], [7, 1]);
        assert_eq!(encodings[6], [7, 255]);
        assert_eq!(encodings[9], [247]);
        assert_eq!(encodings[10], [248, 240]);

        for value in [0, 239, 240, 255, 256, u64::MAX] {
            let mut encoded = Vec::new();
            encode_comparable_uvarint(&mut encoded, value);
            assert_eq!(decode_comparable_uvarint(&encoded).unwrap().1, value);
        }

        assert!(decode_comparable_uvarint(&[]).is_err());
        assert!(decode_comparable_uvarint(&[7]).is_err());
        assert!(decode_comparable_uvarint(&[248]).is_err());
        assert!(decode_comparable_varint(&[0; 9]).is_err());
        assert!(decode_comparable_varint(&[255, 0x80, 0, 0, 0, 0, 0, 0, 0]).is_err());
    }
}
