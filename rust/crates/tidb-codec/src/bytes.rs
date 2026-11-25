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

use crate::number::{decode_varint, encode_varint};
use crate::CodecError;

const GROUP_SIZE: usize = 8;
const MARKER: u8 = 0xff;

/// Returns the exact byte length produced by [`encode_bytes`].
pub const fn encoded_bytes_len(input_len: usize) -> usize {
    input_len + (GROUP_SIZE - input_len % GROUP_SIZE) + 1 + input_len / GROUP_SIZE
}

/// Appends TiDB's ascending mem-comparable bytes representation.
pub fn encode_bytes(buffer: &mut Vec<u8>, input: &[u8]) {
    encode_bytes_inner::<false>(buffer, input);
}

fn encode_bytes_inner<const REVERSE: bool>(buffer: &mut Vec<u8>, input: &[u8]) {
    let (source, tail) = input.as_chunks::<GROUP_SIZE>();
    let mut terminal: [u8; GROUP_SIZE + 1] = [0; GROUP_SIZE + 1];
    terminal[..tail.len()].copy_from_slice(tail);
    terminal[GROUP_SIZE] = MARKER - (GROUP_SIZE - tail.len()) as u8;
    if REVERSE {
        invert_bytes(&mut terminal);
    }
    if source.is_empty() {
        buffer.extend_from_slice(&terminal);
        return;
    }

    let start = buffer.len();
    // Initialize all full-group markers together. Only the final marker
    // counts padding, including when the input ends on a group boundary.
    let full_marker = if REVERSE { 0 } else { MARKER };
    buffer.resize(start + encoded_bytes_len(input.len()), full_marker);
    let (last, groups) = buffer[start..]
        .as_chunks_mut::<{ GROUP_SIZE + 1 }>()
        .0
        .split_last_mut()
        .unwrap();
    for (out, bytes) in groups.iter_mut().zip(source) {
        let bytes = if REVERSE {
            (!u64::from_ne_bytes(*bytes)).to_ne_bytes()
        } else {
            *bytes
        };
        out[..GROUP_SIZE].copy_from_slice(&bytes);
    }
    *last = terminal;
}

/// Appends raw bytes for RawKV, or TiDB's ascending mem-comparable bytes
/// representation for transactional keys.
pub fn encode_bytes_ext(buffer: &mut Vec<u8>, input: &[u8], is_raw_kv: bool) {
    if is_raw_kv {
        buffer.extend_from_slice(input);
    } else {
        encode_bytes(buffer, input);
    }
}

/// Decodes TiDB's ascending mem-comparable bytes representation.
pub fn decode_bytes(input: &[u8]) -> Result<(&[u8], Vec<u8>), CodecError> {
    decode_bytes_inner::<false>(input)
}

/// Validates and skips one ascending mem-comparable byte string.
pub fn skip_bytes(input: &[u8]) -> Result<&[u8], CodecError> {
    validated_bytes_len(input, false).map(|length| &input[length..])
}

/// Splits the ascending bytes payload of a datum bounded by [`crate::cut_one`].
/// Pass the bytes after the datum flag. The cutter has checked the continuation
/// markers. This validates the terminal group and returns the encoded prefix
/// and the terminal group's data bytes.
pub fn split_cut_bytes(input: &[u8]) -> Result<(&[u8], &[u8]), CodecError> {
    let (prefix, terminal) = input
        .split_last_chunk::<{ GROUP_SIZE + 1 }>()
        .ok_or(CodecError::InsufficientBytes)?;
    validated_bytes_len(terminal, false)?;
    let padding = usize::from(MARKER - terminal[GROUP_SIZE]);
    Ok((prefix, &terminal[..GROUP_SIZE - padding]))
}

/// Appends TiDB's descending mem-comparable bytes representation.
pub fn encode_bytes_desc(buffer: &mut Vec<u8>, input: &[u8]) {
    encode_bytes_inner::<true>(buffer, input);
}

/// Decodes TiDB's descending mem-comparable bytes representation.
pub fn decode_bytes_desc(input: &[u8]) -> Result<(&[u8], Vec<u8>), CodecError> {
    decode_bytes_inner::<true>(input)
}

/// Appends TiDB's length-prefixed byte representation.
///
/// Unlike [`encode_bytes`], the result is compact but not mem-comparable.
pub fn encode_compact_bytes(buffer: &mut Vec<u8>, input: &[u8]) {
    let length = input.len() as i64;
    if buffer.capacity() - buffer.len() < 10 + input.len() {
        let prefix_size = crate::value_size_of_signed_int(length) - 1;
        buffer.reserve(prefix_size + input.len());
    }
    encode_varint(buffer, length);
    buffer.extend_from_slice(input);
}

/// Decodes one length-prefixed byte representation and returns its remainder.
pub fn decode_compact_bytes(input: &[u8]) -> Result<(&[u8], &[u8]), CodecError> {
    let (payload, declared_len) = decode_varint(input)?;
    let length = usize::try_from(declared_len).map_err(|_| CodecError::InsufficientBytes)?;
    let value = payload.get(..length).ok_or(CodecError::InsufficientBytes)?;
    Ok((&payload[length..], value))
}

/// Returns the encoded byte-value length without allocating its decoded payload.
pub(crate) fn peek_bytes_len(input: &[u8], reverse: bool) -> Result<usize, CodecError> {
    let full_marker = if reverse { 0 } else { MARKER };
    let (groups, _) = input.as_chunks::<{ GROUP_SIZE + 1 }>();
    groups
        .iter()
        .position(|group| group[GROUP_SIZE] != full_marker)
        .map(|index| (index + 1) * (GROUP_SIZE + 1))
        .ok_or(CodecError::InsufficientBytes)
}

fn validated_large_bytes_len(input: &[u8], reverse: bool) -> Result<usize, CodecError> {
    let full_marker = if reverse { 0 } else { MARKER };
    let (groups, _) = input.as_chunks::<{ GROUP_SIZE + 1 }>();
    let (blocks, tail) = groups.as_chunks::<4>();
    let full_markers = u32::from_le_bytes([full_marker; 4]);
    // Find the earliest non-continuation marker four groups at a time.
    let terminal = blocks.iter().enumerate().find_map(|(index, [a, b, c, d])| {
        let markers =
            u32::from_le_bytes([a[GROUP_SIZE], b[GROUP_SIZE], c[GROUP_SIZE], d[GROUP_SIZE]]);
        let changed = markers ^ full_markers;
        (changed != 0).then(|| index * 4 + changed.trailing_zeros() as usize / 8)
    });
    let terminal = terminal.or_else(|| {
        tail.iter()
            .position(|group| group[GROUP_SIZE] != full_marker)
            .map(|index| blocks.len() * 4 + index)
    });
    let terminal = terminal.ok_or(CodecError::InsufficientBytes)?;
    validate_terminal_padding(&groups[terminal], reverse)?;
    Ok((terminal + 1) * (GROUP_SIZE + 1))
}

#[inline]
fn validate_terminal_padding(
    group: &[u8; GROUP_SIZE + 1],
    reverse: bool,
) -> Result<usize, CodecError> {
    let marker = group[GROUP_SIZE];
    let padding = usize::from(if reverse { marker } else { MARKER - marker });
    if padding > GROUP_SIZE {
        return Err(CodecError::InvalidEncoding("invalid bytes marker"));
    }
    // Little-endian order puts the trailing padding at the high end of the word.
    if u64::from_le_bytes(decoded_group(group, reverse)).leading_zeros() < 8 * padding as u32 {
        return Err(CodecError::InvalidEncoding("invalid bytes padding"));
    }
    Ok(padding)
}

fn validated_bytes_len(input: &[u8], reverse: bool) -> Result<usize, CodecError> {
    if input.len() >= 8 * (GROUP_SIZE + 1) {
        return validated_large_bytes_len(input, reverse);
    }
    let full_marker = if reverse { 0 } else { MARKER };
    let (groups, _) = input.as_chunks::<{ GROUP_SIZE + 1 }>();
    for (index, group) in groups.iter().enumerate() {
        if group[GROUP_SIZE] != full_marker {
            validate_terminal_padding(group, reverse)?;
            return Ok((index + 1) * (GROUP_SIZE + 1));
        }
    }
    Err(CodecError::InsufficientBytes)
}

fn decode_bytes_groups(
    input: &[u8],
    reverse: bool,
) -> Result<(&[u8], &[[u8; GROUP_SIZE + 1]], &[u8]), CodecError> {
    let encoded_len = validated_bytes_len(input, reverse)?;
    // Validation established that only the terminal group has padding.
    let groups = input[..encoded_len].as_chunks::<{ GROUP_SIZE + 1 }>().0;
    let (last, full) = groups.split_last().unwrap();
    let padding = if reverse {
        last[GROUP_SIZE]
    } else {
        MARKER - last[GROUP_SIZE]
    };
    Ok((
        &input[encoded_len..],
        full,
        &last[..GROUP_SIZE - usize::from(padding)],
    ))
}

fn collect_byte_groups(full: &[[u8; GROUP_SIZE + 1]], tail: &[u8]) -> Vec<u8> {
    let full_len = full.len() * GROUP_SIZE;
    let mut output = vec![0; full_len + tail.len()];
    let (output_full, output_tail) = output.split_at_mut(full_len);
    for (bytes, group) in output_full
        .as_chunks_mut::<GROUP_SIZE>()
        .0
        .iter_mut()
        .zip(full)
    {
        bytes.copy_from_slice(&group[..GROUP_SIZE]);
    }
    output_tail.copy_from_slice(tail);
    output
}

/// Decodes one ascending value for an immediate consumer.
#[inline]
pub(crate) fn with_decoded_bytes<'a>(
    input: &'a [u8],
    consume: impl FnOnce(&[u8]),
) -> Result<&'a [u8], CodecError> {
    const STACK_GROUPS: usize = 4;
    let (remain, groups, tail) = decode_bytes_groups(input, false)?;
    if groups.is_empty() {
        consume(tail);
    } else if groups.len() < STACK_GROUPS {
        let mut buffer = [0; STACK_GROUPS * GROUP_SIZE];
        for (out, group) in buffer
            .as_chunks_mut::<GROUP_SIZE>()
            .0
            .iter_mut()
            .zip(groups)
        {
            out.copy_from_slice(&group[..GROUP_SIZE]);
        }
        let start = groups.len() * GROUP_SIZE;
        let end = start + tail.len();
        buffer[start..end].copy_from_slice(tail);
        consume(&buffer[..end]);
    } else {
        consume(&collect_byte_groups(groups, tail));
    }
    Ok(remain)
}

#[inline]
fn decoded_group(group: &[u8; GROUP_SIZE + 1], reverse: bool) -> [u8; GROUP_SIZE] {
    let bytes = *group.first_chunk().unwrap();
    if reverse {
        (!u64::from_ne_bytes(bytes)).to_ne_bytes()
    } else {
        bytes
    }
}

fn decode_bytes_inner<const REVERSE: bool>(input: &[u8]) -> Result<(&[u8], Vec<u8>), CodecError> {
    let full_marker = if REVERSE { 0 } else { MARKER };
    let (first, rest) = input
        .split_first_chunk::<{ GROUP_SIZE + 1 }>()
        .ok_or(CodecError::InsufficientBytes)?;
    if first[GROUP_SIZE] != full_marker {
        let padding = validate_terminal_padding(first, REVERSE)?;
        if padding == GROUP_SIZE {
            return Ok((rest, Vec::new()));
        }
        // Copy one full word and expose only the unpadded bytes.
        let mut output = decoded_group(first, REVERSE).to_vec();
        output.truncate(GROUP_SIZE - padding);
        return Ok((rest, output));
    }
    let (second, rest) = rest
        .split_first_chunk::<{ GROUP_SIZE + 1 }>()
        .ok_or(CodecError::InsufficientBytes)?;
    if second[GROUP_SIZE] != full_marker {
        let padding = validate_terminal_padding(second, REVERSE)?;
        let first = decoded_group(first, REVERSE);
        if padding == GROUP_SIZE {
            return Ok((rest, first.to_vec()));
        }
        let groups = [first, decoded_group(second, REVERSE)];
        let mut output = groups.as_flattened().to_vec();
        output.truncate(GROUP_SIZE * 2 - padding);
        return Ok((rest, output));
    }

    let (remain, full, tail) = decode_bytes_groups(input, REVERSE)?;
    let mut output = collect_byte_groups(full, tail);
    if REVERSE {
        invert_bytes(&mut output);
    }
    Ok((remain, output))
}

fn invert_bytes(bytes: &mut [u8]) {
    for byte in bytes {
        *byte = !*byte;
    }
}

#[cfg(test)]
mod tests {
    use super::invert_bytes;

    #[test]
    fn safe_inversion_replaces_go_runtime_specific_fast_path() {
        let original = [1, 2, 3, 4, 5, 6, 7, 8, 255, 0, 0, 0, 0, 0, 0, 0, 0, 247];
        let mut bytes = original;
        invert_bytes(&mut bytes);
        assert_eq!(
            bytes,
            [
                254, 253, 252, 251, 250, 249, 248, 247, 0, 255, 255, 255, 255, 255, 255, 255, 255,
                8,
            ]
        );
        invert_bytes(&mut bytes);
        assert_eq!(bytes, original);
    }
}
