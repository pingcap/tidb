// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `codec.go`: the range property and its encoding in a stat file.

/// Go `RangeProperty`: the first and last key, start offset, size and key
/// count of one range of a data file.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RangeProperty {
    /// Go `FirstKey`.
    pub first_key: Vec<u8>,
    /// Go `LastKey`.
    pub last_key: Vec<u8>,
    /// Go `Offset`.
    pub offset: u64,
    /// Go `Size`.
    pub size: u64,
    /// Go `Keys`.
    pub keys: u64,
}

/// Go `propertyLengthExceptKeys`: the two key lengths, size, keys, offset.
pub(crate) const PROPERTY_LENGTH_EXCEPT_KEYS: usize = 4 * 2 + 8 + 8 + 8;

fn read_u32(data: &[u8], at: usize) -> usize {
    u32::from_be_bytes(data[at..at + 4].try_into().expect("4 bytes")) as usize
}

fn read_u64(data: &[u8], at: usize) -> u64 {
    u64::from_be_bytes(data[at..at + 8].try_into().expect("8 bytes"))
}

/// Go `decodeProp`.
#[must_use]
pub(crate) fn decode_prop(data: &[u8]) -> RangeProperty {
    let mut n = 0;
    let key_len = read_u32(data, n);
    n += 4;
    let first_key = data[n..n + key_len].to_vec();
    n += key_len;
    let key_len = read_u32(data, n);
    n += 4;
    let last_key = data[n..n + key_len].to_vec();
    n += key_len;
    let size = read_u64(data, n);
    n += 8;
    let keys = read_u64(data, n);
    n += 8;
    let offset = read_u64(data, n);
    RangeProperty {
        first_key,
        last_key,
        offset,
        size,
        keys,
    }
}

/// Go `decodeMultiProps`.
#[cfg(test)]
pub(crate) fn decode_multi_props(mut data: &[u8]) -> Vec<RangeProperty> {
    let mut ret = Vec::new();
    while !data.is_empty() {
        let prop_len = read_u32(data, 0);
        ret.push(decode_prop(&data[4..4 + prop_len]));
        data = &data[4 + prop_len..];
    }
    ret
}

/// Go `encodeMultiProps`.
pub(crate) fn encode_multi_props(buf: &mut Vec<u8>, props: &[RangeProperty]) {
    for p in props {
        let len = PROPERTY_LENGTH_EXCEPT_KEYS + p.first_key.len() + p.last_key.len();
        buf.extend_from_slice(&(len as u32).to_be_bytes());
        encode_prop(buf, p);
    }
}

/// Go `encodeProp`.
pub(crate) fn encode_prop(buf: &mut Vec<u8>, r: &RangeProperty) {
    buf.extend_from_slice(&(r.first_key.len() as u32).to_be_bytes());
    buf.extend_from_slice(&r.first_key);
    buf.extend_from_slice(&(r.last_key.len() as u32).to_be_bytes());
    buf.extend_from_slice(&r.last_key);
    buf.extend_from_slice(&r.size.to_be_bytes());
    buf.extend_from_slice(&r.keys.to_be_bytes());
    buf.extend_from_slice(&r.offset.to_be_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Go `codec_test.go:TestRangePropertyCodec`.
    #[test]
    fn test_range_property_codec() {
        let prop = RangeProperty {
            first_key: b"key".to_vec(),
            last_key: b"key2".to_vec(),
            offset: 1,
            size: 2,
            keys: 3,
        };
        let mut buf = Vec::new();
        encode_prop(&mut buf, &prop);
        assert_eq!(decode_prop(&buf), prop);

        let props: Vec<RangeProperty> = (0..3u64)
            .map(|i| RangeProperty {
                first_key: format!("key{i}").into_bytes(),
                last_key: format!("key{i}9").into_bytes(),
                offset: 10 * i,
                size: 20 * i,
                keys: 30 * i,
            })
            .collect();
        let mut buf = Vec::new();
        encode_multi_props(&mut buf, &props);
        assert_eq!(decode_multi_props(&buf), props);
    }

    /// Go `codec_test.go:TestPropertyLengthExceptKeys`.
    #[test]
    fn test_property_length_except_keys() {
        let mut buf = Vec::new();
        encode_prop(&mut buf, &RangeProperty::default());
        assert_eq!(buf.len(), PROPERTY_LENGTH_EXCEPT_KEYS);
    }
}
