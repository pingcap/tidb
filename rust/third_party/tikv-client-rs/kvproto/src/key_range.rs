// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use prost::bytes::{Buf, BufMut};
use prost::encoding::{self, DecodeContext, WireType};
use prost::{DecodeError, Message};

use crate::coprocessor::KeyRange;

impl Message for KeyRange {
    #[inline]
    fn encode_raw(&self, buf: &mut impl BufMut) {
        encode_bound(1, &self.start, buf);
        encode_bound(2, &self.end, buf);
    }

    fn merge_field(
        &mut self,
        tag: u32,
        wire_type: WireType,
        buf: &mut impl Buf,
        ctx: DecodeContext,
    ) -> Result<(), DecodeError> {
        let (field, name) = match tag {
            1 => (&mut self.start, "start"),
            2 => (&mut self.end, "end"),
            _ => return encoding::skip_field(wire_type, tag, buf, ctx),
        };
        encoding::bytes::merge(wire_type, field, buf, ctx).map_err(|mut error| {
            error.push("KeyRange", name);
            error
        })
    }

    #[inline]
    fn encoded_len(&self) -> usize {
        bound_len(self.start.len()) + bound_len(self.end.len())
    }

    fn clear(&mut self) {
        self.start.clear();
        self.end.clear();
    }
}

#[inline]
fn bound_len(len: usize) -> usize {
    if len < 128 {
        // Both field tags and a short length fit in one byte each.
        len + 2 * usize::from(len != 0)
    } else {
        len + 1 + encoding::encoded_len_varint(len as u64)
    }
}

#[inline]
fn encode_bound(tag: u8, key: &[u8], buf: &mut impl BufMut) {
    if !key.is_empty() {
        if key.len() < 128 {
            buf.put_slice(&[
                (tag << 3) | WireType::LengthDelimited as u8,
                key.len() as u8,
            ]);
        } else {
            encoding::encode_key(u32::from(tag), WireType::LengthDelimited, buf);
            encoding::encode_varint(key.len() as u64, buf);
        }
        // prost's Bytes adapter clones the value before copying it into the
        // encoder. Write the borrowed slice directly.
        buf.put_slice(key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::bytes::Bytes;

    #[derive(Clone, PartialEq, Message)]
    struct DerivedRange {
        #[prost(bytes = "bytes", tag = "1")]
        start: Bytes,
        #[prost(bytes = "bytes", tag = "2")]
        end: Bytes,
    }

    #[test]
    fn key_range_matches_derived_protobuf() {
        for width in [0, 1, 127, 128, 16_384] {
            let bytes = Bytes::from(vec![0xfe; width]);
            for (start, end) in [
                (bytes.clone(), Bytes::new()),
                (Bytes::new(), bytes.clone()),
                (bytes.clone(), bytes),
            ] {
                let derived = DerivedRange {
                    start: start.clone(),
                    end: end.clone(),
                };
                let range = KeyRange { start, end };
                let wire = derived.encode_to_vec();
                assert_eq!(range.encoded_len(), wire.len());
                assert_eq!(range.encode_to_vec(), wire);
                assert_eq!(KeyRange::decode(wire.as_slice()).unwrap(), range);
            }
        }
    }

    #[test]
    fn key_range_preserves_default_duplicate_unknown_and_invalid_fields() {
        let wire = b"\x0a\x01a\x18\x03\x12\x01z\x0a\x01b";
        let mut range = KeyRange::decode(wire.as_slice()).unwrap();
        assert_eq!(&range.start[..], b"b");
        assert_eq!(&range.end[..], b"z");
        range.clear();
        assert_eq!(range, KeyRange::default());

        for invalid in [b"\x08\x01".as_slice(), b"\x12\x02z"] {
            let expected = DerivedRange::decode(invalid).unwrap_err().to_string();
            let actual = KeyRange::decode(invalid).unwrap_err().to_string();
            assert_eq!(actual, expected.replace("DerivedRange", "KeyRange"));
        }
    }
}
