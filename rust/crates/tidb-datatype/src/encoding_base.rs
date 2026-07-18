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

//! Shared byte-preserving policy from TiDB's `encodingBase.Transform`.
//!
//! This module intentionally knows nothing about a charset registry or a
//! decoder.  Encoding leaves supply source and converted groups and an error
//! constructor; this module owns the operation bits, first-error behavior,
//! replacement, truncation, and source/converted collection policy.

use std::ops::{BitOr, BitOrAssign};

/// The operation bits consumed by the source `encodingBase.Transform`.
///
/// The values mirror `pkg/parser/charset/encoding.go::Op`, so a later
/// encoding can share the policy without creating a second flag vocabulary.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TransformOp(u16);

impl TransformOp {
    /// Interpret input as UTF-8 source bytes.
    pub const FROM_UTF8: Self = Self(1 << 0);
    /// Interpret output as UTF-8 bytes.
    pub const TO_UTF8: Self = Self(1 << 1);
    /// Stop before the first invalid group.
    pub const TRUNCATE_TRIM: Self = Self(1 << 2);
    /// Replace each invalid group with `?`.
    pub const TRUNCATE_REPLACE: Self = Self(1 << 3);
    /// Collect the source group.
    pub const COLLECT_FROM: Self = Self(1 << 4);
    /// Collect the converted group.
    pub const COLLECT_TO: Self = Self(1 << 5);
    /// Suppress the first invalid-group error.
    pub const SKIP_ERROR: Self = Self(1 << 6);

    /// Go's `OpReplaceNoErr`.
    pub const REPLACE_NO_ERR: Self = Self(
        Self::FROM_UTF8.0 | Self::TRUNCATE_REPLACE.0 | Self::COLLECT_FROM.0 | Self::SKIP_ERROR.0,
    );
    /// Go's `OpReplace`.
    pub const REPLACE: Self =
        Self(Self::FROM_UTF8.0 | Self::TRUNCATE_REPLACE.0 | Self::COLLECT_FROM.0);
    /// Go's `OpEncode`.
    pub const ENCODE: Self = Self(Self::FROM_UTF8.0 | Self::TRUNCATE_TRIM.0 | Self::COLLECT_TO.0);
    /// Go's `OpEncodeNoErr`.
    pub const ENCODE_NO_ERR: Self = Self(Self::ENCODE.0 | Self::SKIP_ERROR.0);
    /// Go's `OpEncodeReplace`.
    pub const ENCODE_REPLACE: Self =
        Self(Self::FROM_UTF8.0 | Self::TRUNCATE_REPLACE.0 | Self::COLLECT_TO.0);
    /// Go's `OpDecode`.
    pub const DECODE: Self = Self(Self::TO_UTF8.0 | Self::TRUNCATE_TRIM.0 | Self::COLLECT_TO.0);
    /// Go's `OpDecodeNoErr`.
    pub const DECODE_NO_ERR: Self = Self(Self::DECODE.0 | Self::SKIP_ERROR.0);
    /// Go's `OpDecodeReplace`.
    pub const DECODE_REPLACE: Self =
        Self(Self::TO_UTF8.0 | Self::TRUNCATE_REPLACE.0 | Self::COLLECT_TO.0);

    pub(crate) const fn contains(self, other: Self) -> bool {
        self.0 & other.0 != 0
    }
}

impl BitOr for TransformOp {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self::Output {
        Self(self.0 | rhs.0)
    }
}

impl BitOrAssign for TransformOp {
    fn bitor_assign(&mut self, rhs: Self) {
        self.0 |= rhs.0;
    }
}

/// Bytes and the optional first invalid-group error returned by Transform.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransformResult<E> {
    pub(crate) bytes: Vec<u8>,
    pub(crate) error: Option<E>,
}

impl<E> TransformResult<E> {
    pub(crate) fn new(bytes: Vec<u8>, error: Option<E>) -> Self {
        Self { bytes, error }
    }

    /// Returns transformed bytes, including replacement bytes.
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Returns the first invalid-group error when suppression was disabled.
    pub fn error(&self) -> Option<&E> {
        self.error.as_ref()
    }

    /// Splits the result into owned bytes and an optional error.
    pub fn into_parts(self) -> (Vec<u8>, Option<E>) {
        (self.bytes, self.error)
    }
}

/// Stateful source-shaped operation policy for one `Transform` call.
///
/// The caller invokes [`TransformPolicy::push`] in source order.  Returning
/// `false` means the caller must stop visiting groups (the trim policy); all
/// other modes return `true`.  The first error is retained even when a
/// replacement byte is emitted, matching Go's `(bytes, error)` result.
pub(crate) struct TransformPolicy<E, F>
where
    F: Fn(&[u8]) -> E,
{
    op: TransformOp,
    bytes: Vec<u8>,
    first_error: Option<E>,
    make_error: F,
}

impl<E, F> TransformPolicy<E, F>
where
    F: Fn(&[u8]) -> E,
{
    pub(crate) fn new(capacity: usize, op: TransformOp, make_error: F) -> Self {
        Self {
            op,
            bytes: Vec::with_capacity(capacity),
            first_error: None,
            make_error,
        }
    }

    /// Consumes one `(from, to, valid)` group and returns whether to continue.
    pub(crate) fn push(&mut self, from: &[u8], to: &[u8], valid: bool) -> bool {
        if !valid {
            if self.first_error.is_none() && !self.op.contains(TransformOp::SKIP_ERROR) {
                self.first_error = Some((self.make_error)(from));
            }
            if self.op.contains(TransformOp::TRUNCATE_TRIM) {
                return false;
            }
            if self.op.contains(TransformOp::TRUNCATE_REPLACE) {
                self.bytes.push(b'?');
                return true;
            }
        }

        // Keep the source's precedence when callers combine both collection
        // bits: `collectFrom` wins over `collectTo`.
        if self.op.contains(TransformOp::COLLECT_FROM) {
            self.bytes.extend_from_slice(from);
        } else if self.op.contains(TransformOp::COLLECT_TO) {
            self.bytes.extend_from_slice(to);
        }
        true
    }

    pub(crate) fn finish(self) -> TransformResult<E> {
        TransformResult::new(self.bytes, self.first_error)
    }
}

#[cfg(test)]
mod tests {
    use super::{TransformOp, TransformPolicy};

    fn run(op: TransformOp) -> (Vec<u8>, Option<Vec<u8>>) {
        let mut policy = TransformPolicy::new(3, op, |invalid| invalid.to_vec());
        for (from, to, valid) in [
            (&b"a"[..], &b"A"[..], true),
            (&b"!"[..], &b"?"[..], false),
            (&b"b"[..], &b"B"[..], true),
        ] {
            if !policy.push(from, to, valid) {
                break;
            }
        }
        let result = policy.finish();
        (result.bytes().to_vec(), result.error().cloned())
    }

    #[test]
    fn source_modes_preserve_bytes_error_and_truncation() {
        assert_eq!(run(TransformOp::REPLACE_NO_ERR), (b"a?b".to_vec(), None));
        assert_eq!(
            run(TransformOp::REPLACE),
            (b"a?b".to_vec(), Some(b"!".to_vec()))
        );
        assert_eq!(
            run(TransformOp::ENCODE),
            (b"A".to_vec(), Some(b"!".to_vec()))
        );
        assert_eq!(run(TransformOp::DECODE_NO_ERR), (b"A".to_vec(), None));
    }

    #[test]
    fn source_collection_precedence_matches_encoding_base() {
        let mut policy = TransformPolicy::new(
            1,
            TransformOp::COLLECT_FROM | TransformOp::COLLECT_TO,
            |bytes| bytes.to_vec(),
        );
        assert!(policy.push(b"from", b"to", true));
        let result = policy.finish();
        assert_eq!(result.bytes(), b"from");
    }
}
