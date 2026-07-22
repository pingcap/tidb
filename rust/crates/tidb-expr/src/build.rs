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

//! Typed expression construction for the first source-backed string family.
//!
//! Go TiDB chooses `CHAR_LENGTH`'s binary or UTF-8 implementation from the
//! argument's `FieldType` in `charLengthFunctionClass.getFunction`; the
//! runtime datum is not consulted. This module preserves that phase boundary:
//! construction records one immutable signature, then evaluation follows it.

use tidb_ast::{CastType, Expr};
use tidb_datatype::{Collation, Datum, FieldType, FieldTypeCode};

use crate::coerce::coerce_str;
use crate::EvalError;

/// The two function classes supported by this bounded build seam.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StringLengthFunction {
    /// `LENGTH` / `OCTET_LENGTH`; always counts evaluated string bytes.
    Length,
    /// `CHAR_LENGTH` / `CHARACTER_LENGTH`; signature depends on `FieldType`.
    CharLength,
}

/// The immutable evaluator selected while building the function.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StringLengthSignature {
    /// Go `builtinLengthSig`.
    Length,
    /// Go `builtinCharLengthBinarySig`.
    CharLengthBinary,
    /// Go `builtinCharLengthUTF8Sig`.
    CharLengthUtf8,
}

/// The source-backed subset of Go's expression `BuildContext`.
///
/// Only connection charset/collation belongs here today. Evaluation policy,
/// warnings, SQL mode, time, and variables remain deliberately absent until
/// the separate `EvalContext` milestone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BuildContext {
    default_collation: Collation,
}

impl BuildContext {
    /// Creates a build context with the connection's registered collation.
    pub const fn new(default_collation: Collation) -> Self {
        Self { default_collation }
    }

    /// Returns the connection collation assigned to source string expressions
    /// that do not declare a charset or collation of their own.
    pub const fn default_collation(self) -> Collation {
        self.default_collation
    }

    /// Builds one typed `LENGTH` or `CHAR_LENGTH` expression.
    ///
    /// `argument_type` is explicit and authoritative. In particular, passing
    /// a binary `FieldType` with a `Datum::String`, or a UTF-8 `FieldType` with
    /// a `Datum::Bytes`, still follows the field type selected here.
    pub fn build_string_length(
        self,
        function: StringLengthFunction,
        argument_type: FieldType,
    ) -> BuiltStringLength {
        let signature = match function {
            StringLengthFunction::Length => StringLengthSignature::Length,
            StringLengthFunction::CharLength if argument_type.is_binary_string() => {
                StringLengthSignature::CharLengthBinary
            }
            StringLengthFunction::CharLength => StringLengthSignature::CharLengthUtf8,
        };
        BuiltStringLength { signature }
    }

    /// Builds a string-length expression from source-visible AST type facts.
    ///
    /// Go's parser and function builders assign binary string types to bit and
    /// hex literals, `CAST AS BINARY`, `UNHEX`, no-`USING` `CHAR`, and other
    /// explicitly listed binary-returning forms below. This selection happens
    /// before the argument is evaluated; the resulting [`Datum`] variant is
    /// never used as a substitute for expression type metadata.
    pub fn build_string_length_for_expr(
        self,
        function: StringLengthFunction,
        argument: &Expr,
    ) -> Result<BuiltStringLength, EvalError> {
        // LENGTH has one Go signature and never needs argument type metadata.
        if function == StringLengthFunction::Length {
            return Ok(self.build_string_length(function, FieldType::new(FieldTypeCode::Null)));
        }
        Ok(self.build_string_length(function, self.string_length_argument_type(argument)?))
    }

    /// Produces the argument `FieldType` needed by Go's string-length builder.
    ///
    /// This is deliberately narrower than general SQL type inference: only
    /// binary-vs-character identity affects these signatures. Every known AST
    /// form that can produce a binary string is handled explicitly. Schema and
    /// parameter metadata are not represented by this seed evaluator yet.
    /// Unknown row-dependent or function forms fail honestly instead of
    /// guessing from either the connection default or the runtime datum.
    fn string_length_argument_type(self, argument: &Expr) -> Result<FieldType, EvalError> {
        match argument {
            // `DefaultTypeForValue` assigns both literal families a binary
            // string type; their runtime value kind is not consulted here.
            Expr::Hex(_) | Expr::Bit(_) => Ok(binary_string_type()),
            Expr::String(_) | Expr::RawString(_) => Ok(self.character_string_type()),
            Expr::CharsetString { charset, .. } if charset.eq_ignore_ascii_case("binary") => {
                Ok(binary_string_type())
            }
            Expr::CharsetString { .. } => Ok(self.character_string_type()),
            Expr::Paren(inner) => self.string_length_argument_type(inner),
            Expr::Cast(cast) => Ok(match &cast.cast_type {
                CastType::Binary { .. } => binary_string_type(),
                CastType::Char {
                    charset: Some(charset),
                    ..
                } if charset.eq_ignore_ascii_case("binary") => binary_string_type(),
                CastType::Char { .. } => self.character_string_type(),
                CastType::Signed | CastType::Unsigned | CastType::Year => {
                    FieldType::new(FieldTypeCode::LongLong)
                }
                CastType::Decimal { .. } => FieldType::new(FieldTypeCode::NewDecimal),
                CastType::Double | CastType::Float => FieldType::new(FieldTypeCode::Double),
                CastType::Date
                | CastType::DateTime { .. }
                | CastType::Time { .. }
                | CastType::Json => self.character_string_type(),
            }),
            Expr::ConvertUsing { charset, .. } if charset.eq_ignore_ascii_case("binary") => {
                Ok(binary_string_type())
            }
            Expr::ConvertUsing { .. } => Ok(self.character_string_type()),
            Expr::Collate { collation, .. } if collation.eq_ignore_ascii_case("binary") => {
                Ok(binary_string_type())
            }
            Expr::Collate { .. } => Ok(self.character_string_type()),
            Expr::Func { name, args, .. }
                if name.eq_ignore_ascii_case("UNHEX")
                    || name.eq_ignore_ascii_case("FROM_BASE64")
                    || (name.eq_ignore_ascii_case("CHAR_FUNC")
                        && char_function_returns_binary(args)) =>
            {
                Ok(binary_string_type())
            }
            Expr::Func { name, args, .. } if name.eq_ignore_ascii_case("CHAR_FUNC") => {
                match args.last() {
                    Some(Expr::RawString(_)) => Ok(self.character_string_type()),
                    _ => unresolved_string_length_type(),
                }
            }
            // Go's ELT result is binary if any selectable value has binary
            // string type, independent of the runtime selector value.
            Expr::Func { name, args, .. } if name.eq_ignore_ascii_case("ELT") => {
                let mut unresolved = false;
                for argument in args.iter().skip(1) {
                    match self.string_length_argument_type(argument) {
                        Ok(field_type) if field_type.is_binary_string() => {
                            return Ok(binary_string_type());
                        }
                        Ok(_) => {}
                        Err(_) => unresolved = true,
                    }
                }
                if unresolved {
                    unresolved_string_length_type()
                } else {
                    Ok(self.character_string_type())
                }
            }
            Expr::Int(_) | Expr::Bool(_) => Ok(FieldType::new(FieldTypeCode::LongLong)),
            Expr::Decimal(_) => Ok(FieldType::new(FieldTypeCode::NewDecimal)),
            Expr::Float(_) => Ok(FieldType::new(FieldTypeCode::Double)),
            Expr::Null => Ok(FieldType::new(FieldTypeCode::Null)),
            _ => unresolved_string_length_type(),
        }
    }

    fn character_string_type(self) -> FieldType {
        FieldType::new(FieldTypeCode::VarString).with_collation(self.default_collation)
    }
}

fn binary_string_type() -> FieldType {
    FieldType::new(FieldTypeCode::VarString).with_collation(Collation::Binary)
}

fn char_function_returns_binary(args: &[Expr]) -> bool {
    matches!(args.last(), Some(Expr::Null))
        || matches!(args.last(), Some(Expr::RawString(charset)) if charset.eq_ignore_ascii_case("binary"))
}

fn unresolved_string_length_type() -> Result<FieldType, EvalError> {
    Err(EvalError::Unsupported(
        "unresolved CHAR_LENGTH argument FieldType",
    ))
}

impl Default for BuildContext {
    fn default() -> Self {
        Self::new(Collation::DEFAULT)
    }
}

/// An immutable, typed string-length expression ready for evaluation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BuiltStringLength {
    signature: StringLengthSignature,
}

impl BuiltStringLength {
    /// Returns the signature selected during construction.
    pub const fn signature(self) -> StringLengthSignature {
        self.signature
    }

    /// Evaluates the already-selected signature against one scalar argument.
    pub fn eval(self, argument: &Datum) -> Result<Datum, EvalError> {
        let count = match self.signature {
            StringLengthSignature::Length | StringLengthSignature::CharLengthBinary => {
                match argument.as_raw_bytes() {
                    Some(bytes) => Some(bytes.len()),
                    None => coerce_str(argument)?.map(|text| text.len()),
                }
            }
            StringLengthSignature::CharLengthUtf8 => match argument.as_raw_bytes() {
                Some(bytes) => Some(go_utf8_rune_count(bytes)),
                None => coerce_str(argument)?.map(|text| text.chars().count()),
            },
        };

        Ok(count.map_or(Datum::Null, |count| Datum::Int(count as i64)))
    }
}

/// Counts runes with Go `unicode/utf8.RuneCountInString` semantics.
///
/// Rust's lossy decoder may group malformed subsequences differently. Go's
/// `DecodeRuneInString` instead returns one `RuneError` of width one for each
/// invalid encoding and each byte of an incomplete suffix, so the loop must
/// advance exactly one byte after every valid prefix.
fn go_utf8_rune_count(mut bytes: &[u8]) -> usize {
    let mut count = 0;
    while !bytes.is_empty() {
        match std::str::from_utf8(bytes) {
            Ok(_) => {
                count += bytes
                    .iter()
                    .filter(|byte| **byte & 0b1100_0000 != 0b1000_0000)
                    .count();
                break;
            }
            Err(error) => {
                let valid = error.valid_up_to();
                count += bytes[..valid]
                    .iter()
                    .filter(|byte| **byte & 0b1100_0000 != 0b1000_0000)
                    .count();
                count += 1;
                bytes = &bytes[valid + 1..];
            }
        }
    }
    count
}

#[cfg(test)]
mod tests {
    use super::{BuildContext, StringLengthFunction, StringLengthSignature};
    use crate::Datum;
    use tidb_datatype::{Collation, FieldType, FieldTypeCode};

    fn binary_type() -> FieldType {
        FieldType::new(FieldTypeCode::VarString).with_collation(Collation::Binary)
    }

    fn utf8_type() -> FieldType {
        FieldType::new(FieldTypeCode::VarString).with_collation(Collation::Utf8Mb4Bin)
    }

    /// Sources:
    /// - `pkg/expression/builtin_string.go::lengthFunctionClass.getFunction`
    /// - `pkg/expression/builtin_string.go::builtinLengthSig.evalInt`
    /// - `pkg/expression/builtin_string_test.go::TestLengthAndOctetLength`
    #[test]
    fn length_always_counts_raw_bytes() {
        let context = BuildContext::default();
        let utf8 = context.build_string_length(StringLengthFunction::Length, utf8_type());
        let binary = context.build_string_length(StringLengthFunction::Length, binary_type());

        assert_eq!(utf8.signature(), StringLengthSignature::Length);
        assert_eq!(binary.signature(), StringLengthSignature::Length);
        assert_eq!(utf8.eval(&Datum::new_string("你好")), Ok(Datum::Int(6)));
        assert_eq!(
            binary.eval(&Datum::new_bytes(vec![0xff, 0, b'a'])),
            Ok(Datum::Int(3))
        );
        assert_eq!(utf8.eval(&Datum::new_string("a\0b")), Ok(Datum::Int(3)));
        assert_eq!(utf8.eval(&Datum::Null), Ok(Datum::Null));
    }

    /// Sources:
    /// - `pkg/expression/builtin_string.go::charLengthFunctionClass.getFunction`
    /// - `builtinCharLengthBinarySig.evalInt`
    /// - `builtinCharLengthUTF8Sig.evalInt`
    /// - `pkg/types/etc.go::IsBinaryStr`
    /// - `pkg/expression/builtin_string_test.go::TestCharLength`
    #[test]
    fn char_length_signature_is_selected_only_from_field_type() {
        let context = BuildContext::default();
        let binary = context.build_string_length(StringLengthFunction::CharLength, binary_type());
        let utf8 = context.build_string_length(StringLengthFunction::CharLength, utf8_type());
        let raw_multibyte = Datum::new_bytes("你好".as_bytes().to_vec());

        assert_eq!(binary.signature(), StringLengthSignature::CharLengthBinary);
        assert_eq!(utf8.signature(), StringLengthSignature::CharLengthUtf8);
        assert_eq!(binary.eval(&raw_multibyte), Ok(Datum::Int(6)));
        assert_eq!(utf8.eval(&raw_multibyte), Ok(Datum::Int(2)));

        // Runtime metadata must not re-select the signature: a character
        // datum follows the explicitly binary field type.
        assert_eq!(binary.eval(&Datum::new_string("33")), Ok(Datum::Int(2)));
        assert_eq!(binary.eval(&Datum::new_string("CAFÉ")), Ok(Datum::Int(5)));
        assert_eq!(binary.eval(&Datum::new_string("")), Ok(Datum::Int(0)));
        assert_eq!(binary.eval(&Datum::Null), Ok(Datum::Null));
    }

    /// Source boundary: Go's UTF-8 signature converts the raw Go string to
    /// runes. `utf8.DecodeRuneInString` counts one replacement rune of width
    /// one for every invalid encoding byte; this is semantics, not a warning
    /// or error-policy decision deferred to `EvalContext`.
    #[test]
    fn char_length_invalid_utf8_and_embedded_nul_boundaries() {
        let context = BuildContext::default();
        let binary = context.build_string_length(StringLengthFunction::CharLength, binary_type());
        let utf8 = context.build_string_length(StringLengthFunction::CharLength, utf8_type());

        for (raw, rune_count) in [
            (vec![0xff], 1),
            (vec![0xf0, 0x28, 0x8c, 0x28], 4),
            (vec![b'a', 0xe4, 0xbd], 3),
            (vec![b'a', 0xe4, 0xb8, 0xad, 0xff], 3),
        ] {
            let value = Datum::new_bytes(raw.clone());
            assert_eq!(binary.eval(&value), Ok(Datum::Int(raw.len() as i64)));
            assert_eq!(utf8.eval(&value), Ok(Datum::Int(rune_count)));
        }

        let embedded_nul = Datum::new_bytes(b"a\0b".to_vec());
        assert_eq!(binary.eval(&embedded_nul), Ok(Datum::Int(3)));
        assert_eq!(utf8.eval(&embedded_nul), Ok(Datum::Int(3)));
    }

    /// `IsBinaryStr` requires a string SQL type as well as `binary`
    /// collation. A numeric field type with binary metadata therefore selects
    /// the UTF-8 signature, matching Go's build-time rule.
    #[test]
    fn binary_collation_on_non_string_type_does_not_select_binary_signature() {
        let built = BuildContext::default().build_string_length(
            StringLengthFunction::CharLength,
            FieldType::new(FieldTypeCode::LongLong),
        );
        assert_eq!(built.signature(), StringLengthSignature::CharLengthUtf8);
        assert_eq!(built.eval(&Datum::Int(33)), Ok(Datum::Int(2)));
    }
}
