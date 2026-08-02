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

//! A decimal key never takes a signed-vs-unsigned fork, and the bytes sort.
//!
//! The suspicion this pins down is the project's most-repeated corruption
//! shape: a value encoded so its BYTE order disagrees with its LOGICAL order,
//! which makes both index order and row order wrong while every individual
//! value still reads back correctly. It is worth pinning for decimals
//! specifically because a `DECIMAL UNSIGNED` column is the one place where a
//! plausible-looking `if unsigned { EncodeUint } else { EncodeInt }` fork could
//! be introduced by someone porting the integer path by analogy.
//!
//! Go does not have that fork. `pkg/util/codec/codec.go::Encoder::encode`
//! dispatches on the datum's KIND, and `types.KindMysqlDecimal` has exactly one
//! arm -- `decimalFlag` then `EncodeDecimal` -- with no reference to
//! `mysql.UnsignedFlag`. The column's UNSIGNED flag changes the ADMISSION rule
//! (which values may be stored) and never the encoding of a value that was
//! admitted. `MyDecimal::ToBin` is already sign-magnitude and order-preserving
//! across zero, so the unsigned flag has nothing left to correct.
//!
//! These are the bytes Go writes, captured from `codec.EncodeKey` over
//! `types.Datum::SetMysqlDecimal` (integers shown for contrast, since THEY do
//! fork and carry different flag bytes, 0x03 signed vs 0x04 unsigned):
//!
//! ```text
//! decimal -1  key = 06 01 00 7e
//! decimal  0  key = 06 01 00 80
//! decimal  1  key = 06 01 00 81
//! decimal 10  key = 06 02 00 8a
//! int64   -1  key = 03 7f ff ff ff ff ff ff ff
//! int64    0  key = 03 80 00 00 00 00 00 00 00
//! uint64   0  key = 04 00 00 00 00 00 00 00 00
//! uint64   1  key = 04 00 00 00 00 00 00 00 01
//! ```
//!
//! Note what the integer rows say about the failure mode: flag `0x03` sorts
//! BELOW flag `0x04`, so a value that took the signed path where its peers took
//! the unsigned one would sort below ALL of them regardless of magnitude. That
//! is the corruption. The decimal rows show it cannot happen there -- one flag,
//! `0x06`, for every decimal, signed or not.
//!
//! # Where a decimal key IS order-preserving, and where it is not
//!
//! Probing this turned up a sharper fact than the one being looked for, and it
//! is pinned below because it is easy to assert the wrong invariant here.
//! `EncodeDecimal` frames the mem-comparable payload with a `(precision,
//! scale)` HEADER, and the header is compared before the payload. At NATURAL
//! precision -- Go `Datum.Length == 0`, each value describing only itself --
//! two values of the same column get different headers, and byte order stops
//! agreeing with value order:
//!
//! ```text
//! -10 = 06 02 00 75      -1 = 06 01 00 7e     <- 02 > 01, so -10 keys ABOVE -1
//! ```
//!
//! That is Go's own output, captured from `codec.EncodeKey`, and this crate
//! reproduces it byte for byte. It is not a bug in either: natural precision is
//! for standalone values, and an INDEX never uses it. An index column carries
//! the schema's `(Length, Frac)` on every datum, so the header is a constant
//! across the whole index and only the order-preserving payload varies:
//!
//! ```text
//! decimal(11,1)  -10 = 06 0b 01 7f ff ff ff f5 ff
//!                 -1 = 06 0b 01 7f ff ff ff fe ff
//!               -0.5 = 06 0b 01 7f ff ff ff ff fa
//!                  0 = 06 0b 01 80 00 00 00 00 00
//!                0.5 = 06 0b 01 80 00 00 00 00 05
//!                  1 = 06 0b 01 80 00 00 00 01 00
//!                 10 = 06 0b 01 80 00 00 00 0a 00
//! ```
//!
//! So the real invariant to defend is not "decimal keys sort" but "an index
//! column's decimals are all keyed at ONE precision". Both halves are asserted
//! below, the non-monotonic half deliberately, so that a future reader who
//! finds natural-precision keys out of order recognises it as the documented
//! shape rather than rediscovering it as a corruption.

use tidb_codec::{encode_decimal_fixed, encode_key};
use tidb_datatype::{Datum, Decimal};

fn decimal(literal: &str) -> Decimal {
    match literal.strip_prefix('-') {
        Some(magnitude) => Decimal::from_literal(magnitude).negate(),
        None => Decimal::from_literal(literal),
    }
}

fn key(literal: &str) -> Vec<u8> {
    encode_key(&[Datum::new_decimal(decimal(literal))]).unwrap()
}

/// The key an INDEX column builds: one schema-fixed precision for every row.
fn column_key(literal: &str, precision: usize, scale: usize) -> Vec<u8> {
    let mut buffer = vec![0x06];
    encode_decimal_fixed(&mut buffer, &decimal(literal), precision, scale).unwrap();
    buffer
}

/// The exact Go bytes above, so a signed/unsigned fork cannot be added silently.
#[test]
fn decimal_keys_are_the_go_bytes_with_no_unsigned_fork() {
    assert_eq!(key("-1"), vec![0x06, 0x01, 0x00, 0x7e]);
    assert_eq!(key("0"), vec![0x06, 0x01, 0x00, 0x80]);
    assert_eq!(key("1"), vec![0x06, 0x01, 0x00, 0x81]);
    assert_eq!(key("10"), vec![0x06, 0x02, 0x00, 0x8a]);
}

/// At the ONE precision an index column uses, byte order agrees with value
/// order across zero -- the property that fails first when a value is routed
/// through the wrong encoder.
#[test]
fn column_precision_decimal_keys_sort_in_value_order_across_zero() {
    let ascending = ["-10", "-1", "-0.5", "0", "0.5", "1", "10"];
    for pair in ascending.windows(2) {
        let lower = column_key(pair[0], 11, 1);
        let upper = column_key(pair[1], 11, 1);
        assert!(
            lower < upper,
            "{} keyed as {:02x?} does not sort below {} keyed as {:02x?}",
            pair[0],
            lower,
            pair[1],
            upper,
        );
    }
}

/// The exact Go `decimal(11,1)` bytes for the same ladder, so the sort above
/// cannot be satisfied by some other encoding that merely happens to order.
#[test]
fn column_precision_decimal_keys_are_the_go_bytes() {
    assert_eq!(
        column_key("-10", 11, 1),
        vec![0x06, 0x0b, 0x01, 0x7f, 0xff, 0xff, 0xff, 0xf5, 0xff],
    );
    assert_eq!(
        column_key("-0.5", 11, 1),
        vec![0x06, 0x0b, 0x01, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xfa],
    );
    assert_eq!(
        column_key("0", 11, 1),
        vec![0x06, 0x0b, 0x01, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00],
    );
    assert_eq!(
        column_key("10", 11, 1),
        vec![0x06, 0x0b, 0x01, 0x80, 0x00, 0x00, 0x00, 0x0a, 0x00],
    );
}

/// The deliberate negative: at NATURAL precision the header varies per value
/// and byte order does NOT track value order. Go does this too. Pinned so the
/// shape is recognised rather than rediscovered, and so that anyone who "fixes"
/// natural-precision keys to sort has to confront that Go disagrees.
#[test]
fn natural_precision_decimal_keys_deliberately_do_not_sort() {
    let smaller_value = key("-10");
    let larger_value = key("-1");
    assert_eq!(smaller_value, vec![0x06, 0x02, 0x00, 0x75]);
    assert_eq!(larger_value, vec![0x06, 0x01, 0x00, 0x7e]);
    assert!(
        smaller_value > larger_value,
        "natural-precision keys are framed by a per-value precision header, so -10 keys ABOVE -1",
    );
}

/// The control this probe needs: integer keys DO fork, and the flag byte alone
/// decides the order. If this ever stops holding, the contrast the module doc
/// above draws is stale and the decimal assertions lose their meaning.
#[test]
fn integer_keys_do_fork_on_signedness_and_the_flag_byte_orders_them() {
    let signed_zero = encode_key(&[Datum::Int(0)]).unwrap();
    let unsigned_max = encode_key(&[Datum::UInt(u64::MAX)]).unwrap();
    assert_eq!(signed_zero[0], 0x03);
    assert_eq!(unsigned_max[0], 0x04);
    assert!(
        signed_zero < unsigned_max,
        "the signed flag must sort below the unsigned one for the contrast to hold",
    );
}
