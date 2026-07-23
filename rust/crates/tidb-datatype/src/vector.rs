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

//! Complete `pkg/types/vector.go` and `vector_functions.go` transcreation.

use std::cmp::Ordering;
use std::fmt;

/// Maximum dimension accepted by TiDB's VECTOR type.
pub const MAX_VECTOR_DIMENSION: usize = 16_383;

/// Vector construction, parsing, serialization, or arithmetic error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorError(String);

impl VectorError {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for VectorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for VectorError {}

/// A finite little-endian TiDB `VECTOR<FLOAT32>`.
///
/// Go stores the serialized header and values in one byte slice. Rust stores
/// aligned `f32` elements and produces the same wire image, eliminating the
/// unsafe byte-to-float aliasing seam.
#[derive(Clone, Default, PartialEq)]
pub struct VectorFloat32 {
    elements: Vec<f32>,
}

impl fmt::Debug for VectorFloat32 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("VectorFloat32")
            .field(&self.elements)
            .finish()
    }
}

impl fmt::Display for VectorFloat32 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("[")?;
        for (index, value) in self.elements.iter().enumerate() {
            if index != 0 {
                formatter.write_str(",")?;
            }
            formatter.write_str(&format_f32_fixed_shortest(*value))?;
        }
        formatter.write_str("]")
    }
}

impl VectorFloat32 {
    /// Creates a vector after rejecting NaN and infinity.
    pub fn create(elements: impl Into<Vec<f32>>) -> Result<Self, VectorError> {
        let elements = elements.into();
        check_vector_dim_valid(elements.len() as isize)?;
        for value in &elements {
            if value.is_nan() {
                return Err(VectorError::new("NaN not allowed in vector"));
            }
            if value.is_infinite() {
                return Err(VectorError::new("infinite value not allowed in vector"));
            }
        }
        Ok(Self { elements })
    }

    /// Creates a vector and panics on an invalid value.
    pub fn must_create(elements: impl Into<Vec<f32>>) -> Self {
        Self::create(elements).unwrap_or_else(|error| panic!("{error}"))
    }

    /// Creates a zero-filled vector of the given dimension.
    pub fn init(dimensions: usize) -> Self {
        Self {
            elements: vec![0.0; dimensions],
        }
    }

    /// Returns the vector dimension.
    pub fn len(&self) -> usize {
        self.elements.len()
    }

    /// Returns whether this vector has no elements.
    pub fn is_empty(&self) -> bool {
        self.elements.is_empty()
    }

    /// Borrows the elements.
    pub fn elements(&self) -> &[f32] {
        &self.elements
    }

    /// Mutably borrows the elements, matching Go `Elements`.
    ///
    /// Callers must preserve the finite-value invariant, as in the source.
    pub fn elements_mut(&mut self) -> &mut [f32] {
        &mut self.elements
    }

    /// Checks the dimension declared by a column. `None` is unspecified.
    pub fn check_dims_fit_column(&self, expected: Option<usize>) -> Result<(), VectorError> {
        if let Some(expected) = expected {
            if self.len() != expected {
                return Err(VectorError::new(format!(
                    "vector has {} dimensions, does not fit VECTOR({expected})",
                    self.len()
                )));
            }
        }
        Ok(())
    }

    /// Returns the source log/EXPLAIN representation.
    pub fn truncated_string(&self) -> String {
        const DISPLAY: usize = 5;
        let displayed = self.elements.len().min(DISPLAY);
        let mut output = String::from("[");
        for (index, value) in self.elements[..displayed].iter().enumerate() {
            if index != 0 {
                output.push(',');
            }
            output.push_str(&format_f32_general_two_digits(*value));
        }
        if self.elements.len() > DISPLAY {
            output.push_str(&format!(",({} more)...", self.elements.len() - DISPLAY));
        }
        output.push(']');
        output
    }

    /// Appends the exact little-endian source wire representation.
    pub fn serialize_to(&self, destination: &mut Vec<u8>) {
        destination.extend_from_slice(&(self.len() as u32).to_le_bytes());
        for value in &self.elements {
            destination.extend_from_slice(&value.to_bits().to_le_bytes());
        }
    }

    /// Returns the exact little-endian source wire representation.
    pub fn serialize(&self) -> Vec<u8> {
        let mut result = Vec::with_capacity(self.serialized_size());
        self.serialize_to(&mut result);
        result
    }

    /// Returns the serialized byte length.
    pub const fn serialized_size(&self) -> usize {
        4 + self.elements.len() * 4
    }

    /// Returns Go's estimated memory usage on a 64-bit target.
    pub const fn estimated_mem_usage(&self) -> usize {
        std::mem::size_of::<Self>() + self.serialized_size()
    }

    /// Parses the JSON-array text accepted by TiDB.
    pub fn parse(text: &str) -> Result<Self, VectorError> {
        if text.trim() == "null" {
            return Err(VectorError::new(format!("Invalid vector text: {text}")));
        }
        let values: Vec<f64> = serde_json::from_str(text)
            .map_err(|_| VectorError::new(format!("Invalid vector text: {text}")))?;
        check_vector_dim_valid(values.len() as isize)?;
        let mut elements = Vec::with_capacity(values.len());
        for value in values {
            if value.is_nan() {
                return Err(VectorError::new("NaN not allowed in vector"));
            }
            if value.is_infinite() {
                return Err(VectorError::new("infinite value not allowed in vector"));
            }
            if !(-f64::from(f32::MAX)..=f64::from(f32::MAX)).contains(&value) {
                return Err(VectorError::new(format!(
                    "value {} out of range for float32",
                    format_go_exponent(value)
                )));
            }
            elements.push(value as f32);
        }
        Ok(Self { elements })
    }

    /// Returns whether this is the source zero value.
    pub fn is_zero_value(&self) -> bool {
        self.is_empty()
    }

    /// Lexicographically compares elements, then dimensions.
    pub fn compare(&self, other: &Self) -> Ordering {
        for (left, right) in self.elements.iter().zip(&other.elements) {
            match left.partial_cmp(right).expect("vectors contain no NaN") {
                Ordering::Equal => {}
                ordering => return ordering,
            }
        }
        self.len().cmp(&other.len())
    }

    fn check_identical_dims(&self, other: &Self) -> Result<(), VectorError> {
        if self.len() != other.len() {
            return Err(VectorError::new(format!(
                "vectors have different dimensions: {} and {}",
                self.len(),
                other.len()
            )));
        }
        Ok(())
    }

    /// Squared L2 distance with source float32 accumulation.
    pub fn l2_squared_distance(&self, other: &Self) -> Result<f64, VectorError> {
        self.check_identical_dims(other)?;
        let distance =
            self.elements
                .iter()
                .zip(&other.elements)
                .fold(0.0_f32, |distance, (left, right)| {
                    let difference = left - right;
                    distance + difference * difference
                });
        Ok(f64::from(distance))
    }

    /// Euclidean distance.
    pub fn l2_distance(&self, other: &Self) -> Result<f64, VectorError> {
        Ok(self.l2_squared_distance(other)?.sqrt())
    }

    /// Inner product with source float32 accumulation.
    pub fn inner_product(&self, other: &Self) -> Result<f64, VectorError> {
        self.check_identical_dims(other)?;
        Ok(f64::from(
            self.elements
                .iter()
                .zip(&other.elements)
                .fold(0.0_f32, |result, (left, right)| result + left * right),
        ))
    }

    /// Negative inner product.
    pub fn negative_inner_product(&self, other: &Self) -> Result<f64, VectorError> {
        Ok(-self.inner_product(other)?)
    }

    /// Cosine distance with source clamping and zero-vector NaN behavior.
    pub fn cosine_distance(&self, other: &Self) -> Result<f64, VectorError> {
        self.check_identical_dims(other)?;
        let (dot, left_norm, right_norm) = self.elements.iter().zip(&other.elements).fold(
            (0.0_f32, 0.0_f32, 0.0_f32),
            |(dot, left_norm, right_norm), (left, right)| {
                (
                    dot + left * right,
                    left_norm + left * left,
                    right_norm + right * right,
                )
            },
        );
        let similarity = f64::from(dot) / (f64::from(left_norm) * f64::from(right_norm)).sqrt();
        if similarity.is_nan() {
            return Ok(f64::NAN);
        }
        Ok(1.0 - similarity.clamp(-1.0, 1.0))
    }

    /// Manhattan distance with source float32 accumulation.
    pub fn l1_distance(&self, other: &Self) -> Result<f64, VectorError> {
        self.check_identical_dims(other)?;
        Ok(f64::from(
            self.elements
                .iter()
                .zip(&other.elements)
                .fold(0.0_f32, |distance, (left, right)| {
                    distance + (left - right).abs()
                }),
        ))
    }

    /// L2 norm with the source's intentional float64 accumulation.
    pub fn l2_norm(&self) -> f64 {
        self.elements
            .iter()
            .fold(0.0, |norm, value| norm + f64::from(*value).powi(2))
            .sqrt()
    }

    /// Element-wise addition.
    pub fn add(&self, other: &Self) -> Result<Self, VectorError> {
        self.elementwise(other, |left, right| left + right)
    }

    /// Element-wise subtraction.
    pub fn sub(&self, other: &Self) -> Result<Self, VectorError> {
        self.elementwise(other, |left, right| left - right)
    }

    /// Element-wise multiplication.
    pub fn mul(&self, other: &Self) -> Result<Self, VectorError> {
        self.elementwise(other, |left, right| left * right)
    }

    fn elementwise(
        &self,
        other: &Self,
        operation: impl Fn(f32, f32) -> f32,
    ) -> Result<Self, VectorError> {
        self.check_identical_dims(other)?;
        let elements: Vec<_> = self
            .elements
            .iter()
            .copied()
            .zip(other.elements.iter().copied())
            .map(|(left, right)| operation(left, right))
            .collect();
        for value in &elements {
            if value.is_infinite() {
                return Err(VectorError::new("value out of range: overflow"));
            }
            if value.is_nan() {
                return Err(VectorError::new("value out of range: NaN"));
            }
        }
        Ok(Self { elements })
    }
}

/// Checks source dimension bounds.
pub fn check_vector_dim_valid(dimensions: isize) -> Result<(), VectorError> {
    if dimensions < 0 {
        return Err(VectorError::new(
            "dimensions for type vector must be at least 0",
        ));
    }
    if dimensions as usize > MAX_VECTOR_DIMENSION {
        return Err(VectorError::new(format!(
            "vector cannot have more than {MAX_VECTOR_DIMENSION} dimensions"
        )));
    }
    Ok(())
}

fn format_go_exponent(value: f64) -> String {
    let scientific = format!("{value:e}");
    let Some((mantissa, exponent)) = scientific.split_once('e') else {
        return scientific;
    };
    let exponent: i32 = exponent.parse().expect("Rust exponent is numeric");
    format!("{mantissa}e{exponent:+}")
}

fn format_f32_fixed_shortest(value: f32) -> String {
    let shortest = value.to_string();
    let Some((mantissa, exponent)) = shortest
        .split_once('e')
        .or_else(|| shortest.split_once('E'))
    else {
        return shortest;
    };
    let exponent: i32 = exponent.parse().expect("Rust exponent is numeric");
    let unsigned = mantissa.trim_start_matches('-');
    let negative = mantissa.starts_with('-');
    let digits: String = unsigned
        .chars()
        .filter(|character| *character != '.')
        .collect();
    let decimal = unsigned.find('.').map_or(1_i32, |index| index as i32);
    let point = decimal + exponent;
    let mut output = String::new();
    if negative {
        output.push('-');
    }
    if point <= 0 {
        output.push_str("0.");
        output.extend(std::iter::repeat_n('0', (-point) as usize));
        output.push_str(&digits);
    } else if point as usize >= digits.len() {
        output.push_str(&digits);
        output.extend(std::iter::repeat_n('0', point as usize - digits.len()));
    } else {
        output.push_str(&digits[..point as usize]);
        output.push('.');
        output.push_str(&digits[point as usize..]);
    }
    output
}

fn format_f32_general_two_digits(value: f32) -> String {
    if value == 0.0 {
        return if value.is_sign_negative() {
            "-0".to_owned()
        } else {
            "0".to_owned()
        };
    }
    let exponent = value.abs().log10().floor() as i32;
    if !(-4..2).contains(&exponent) {
        let scientific = format!("{value:.1e}");
        let (mantissa, exponent) = scientific
            .split_once('e')
            .expect("scientific format has exponent");
        let mantissa = mantissa.trim_end_matches('0').trim_end_matches('.');
        let exponent: i32 = exponent.parse().expect("Rust exponent is numeric");
        format!("{mantissa}e{exponent:+03}")
    } else {
        let decimals = (1 - exponent).max(0) as usize;
        let fixed = format!("{value:.decimals$}");
        fixed.trim_end_matches('0').trim_end_matches('.').to_owned()
    }
}

/// Returns the number of bytes occupied by the first serialized vector.
pub fn peek_vector_float32(bytes: &[u8]) -> Result<usize, VectorError> {
    let header = bytes.get(..4).ok_or_else(|| {
        VectorError::new(format!(
            "bad VectorFloat32 value header (len={})",
            bytes.len()
        ))
    })?;
    let dimensions = u32::from_le_bytes(header.try_into().expect("fixed vector header"));
    let expected = dimensions
        .checked_mul(4)
        .and_then(|bytes| bytes.checked_add(4))
        .ok_or_else(|| VectorError::new("bad VectorFloat32 value size overflow"))?;
    if bytes.len() < expected as usize {
        return Err(VectorError::new(format!(
            "bad VectorFloat32 value (len={}, expected={expected})",
            bytes.len()
        )));
    }
    Ok(expected as usize)
}

/// Deserializes the first vector and returns it with the unconsumed suffix.
pub fn deserialize_vector_float32(bytes: &[u8]) -> Result<(VectorFloat32, &[u8]), VectorError> {
    let length = peek_vector_float32(bytes)?;
    let mut elements = Vec::with_capacity((length - 4) / 4);
    for chunk in bytes[4..length].chunks_exact(4) {
        elements.push(f32::from_bits(u32::from_le_bytes(
            chunk.try_into().expect("fixed vector element"),
        )));
    }
    Ok((VectorFloat32::create(elements)?, &bytes[length..]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn original_vector_endianness_zero_parse_compare_serialize_and_datum_rows() {
        let mut vector = VectorFloat32::init(2);
        vector.elements_mut().copy_from_slice(&[1.1, 2.2]);
        assert_eq!(
            vector.serialize(),
            [2, 0, 0, 0, 0xCD, 0xCC, 0x8C, 0x3F, 0xCD, 0xCC, 0x0C, 0x40]
        );

        let zero = VectorFloat32::default();
        assert!(zero.is_zero_value());
        assert_eq!(zero.compare(&zero), Ordering::Equal);
        assert_eq!(zero.serialize(), [0, 0, 0, 0]);
        assert_eq!(zero.serialized_size(), 4);
        assert_eq!(zero.to_string(), "[]");

        for invalid in [
            "abc",
            "null",
            "\"json_str\"",
            "123",
            "[123",
            "123]",
            "[123,]",
        ] {
            assert!(VectorFloat32::parse(invalid).is_err(), "{invalid}");
        }
        assert_eq!(VectorFloat32::parse("[]").unwrap(), zero);
        let parsed = VectorFloat32::parse("[1.1, 2.2, 3.3]").unwrap();
        assert_eq!(parsed.to_string(), "[1.1,2.2,3.3]");
        assert_eq!(parsed.compare(&zero), Ordering::Greater);
        assert_eq!(
            VectorFloat32::parse("[-1e39, 1e39]")
                .unwrap_err()
                .to_string(),
            "value -1e+39 out of range for float32"
        );
        assert!(check_vector_dim_valid(-1).is_err());
        for invalid in ["[1,2,3,4.4]ddddddddddddfasfa", "[1,2,3]extra"] {
            assert!(VectorFloat32::parse(invalid).is_err());
        }

        let other = VectorFloat32::parse("[-1.1, 4.2]").unwrap();
        assert_eq!(parsed.compare(&other), Ordering::Greater);
        let other = VectorFloat32::parse("[1.1, 4.2]").unwrap();
        assert_eq!(parsed.compare(&other), Ordering::Less);

        let mut serialized = parsed.serialize();
        serialized.extend_from_slice(&[1, 2, 3, 4]);
        let (round_trip, remaining) = deserialize_vector_float32(&serialized).unwrap();
        assert_eq!(round_trip, parsed);
        assert_eq!(remaining, [1, 2, 3, 4]);
        assert!(deserialize_vector_float32(&[0xF1, 0xFC]).is_err());
    }

    #[test]
    fn vector_functions_cover_source_precision_errors_and_edge_cases() {
        let left = VectorFloat32::must_create(vec![1.0, 2.0, 3.0]);
        let right = VectorFloat32::must_create(vec![4.0, 5.0, 6.0]);
        assert_eq!(left.l2_squared_distance(&right).unwrap(), 27.0);
        assert_eq!(left.l2_distance(&right).unwrap(), 27_f64.sqrt());
        assert_eq!(left.inner_product(&right).unwrap(), 32.0);
        assert_eq!(left.negative_inner_product(&right).unwrap(), -32.0);
        assert_eq!(left.l1_distance(&right).unwrap(), 9.0);
        assert_eq!(left.add(&right).unwrap().elements(), [5.0, 7.0, 9.0]);
        assert_eq!(right.sub(&left).unwrap().elements(), [3.0, 3.0, 3.0]);
        assert_eq!(left.mul(&right).unwrap().elements(), [4.0, 10.0, 18.0]);
        assert!(left.add(&VectorFloat32::must_create(vec![1.0])).is_err());
        assert!(VectorFloat32::must_create(vec![f32::MAX])
            .add(&VectorFloat32::must_create(vec![f32::MAX]))
            .is_err());
        assert!(VectorFloat32::default()
            .cosine_distance(&VectorFloat32::default())
            .unwrap()
            .is_nan());
    }
}
