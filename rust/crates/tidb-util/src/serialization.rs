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

//! Native spill serialization used by TiDB aggregate partial results.
//!
//! This format is deliberately architecture-native. It is an in-process spill
//! format, not a portable storage or network format.

use std::error::Error;
use std::fmt;
use std::mem::size_of;

use tidb_datatype::{
    BinaryJSON, GoString, MyDecimal, MySqlDuration, MysqlEnum, MysqlSet, Opaque, Time,
    MYDECIMAL_STRUCT_SIZE,
};

/// Runtime tag for a boolean interface value.
pub const BOOL_TYPE: u8 = 0;
/// Runtime tag for a signed-integer interface value.
pub const INT64_TYPE: u8 = 1;
/// Runtime tag for an unsigned-integer interface value.
pub const UINT64_TYPE: u8 = 2;
/// Runtime tag for a floating-point interface value.
pub const FLOAT_TYPE: u8 = 3;
/// Runtime tag for a byte-preserving string interface value.
pub const STRING_TYPE: u8 = 4;
/// Runtime tag for a BinaryJSON interface value.
pub const BINARY_JSON_TYPE: u8 = 5;
/// Runtime tag for an opaque JSON interface value.
pub const OPAQUE_TYPE: u8 = 6;
/// Runtime tag for a packed MySQL time interface value.
pub const TIME_TYPE: u8 = 7;
/// Runtime tag for a MySQL duration interface value.
pub const DURATION_TYPE: u8 = 8;

/// Native serialized width of one Go/Rust word.
pub const INT_LEN: usize = size_of::<isize>();
/// Serialized width of one interface tag.
pub const INTERFACE_TYPE_CODE_LEN: usize = 1;
/// Serialized width of one JSON type code.
pub const JSON_TYPE_CODE_LEN: usize = 1;
/// Serialized width of one boolean.
pub const BOOL_LEN: usize = 1;
/// Serialized width of one byte.
pub const BYTE_LEN: usize = 1;
/// Serialized width of one signed byte.
pub const INT8_LEN: usize = 1;
/// Serialized width of one unsigned byte.
pub const UINT8_LEN: usize = 1;
/// Serialized width of one signed 32-bit integer.
pub const INT32_LEN: usize = size_of::<i32>();
/// Serialized width of one unsigned 32-bit integer.
pub const UINT32_LEN: usize = size_of::<u32>();
/// Serialized width of one signed 64-bit integer.
pub const INT64_LEN: usize = size_of::<i64>();
/// Serialized width of one unsigned 64-bit integer.
pub const UINT64_LEN: usize = size_of::<u64>();
/// Serialized width of one 32-bit float.
pub const FLOAT32_LEN: usize = size_of::<f32>();
/// Serialized width of one 64-bit float.
pub const FLOAT64_LEN: usize = size_of::<f64>();
/// Serialized width of one exact Go `types.MyDecimal` value.
pub const MY_DECIMAL_LEN: usize = MYDECIMAL_STRUCT_SIZE;
/// Serialized width of one packed Go `types.Time` value.
pub const TIME_LEN: usize = size_of::<u64>();
/// Serialized width of one signed Go `time.Duration`.
pub const TIME_DURATION_LEN: usize = size_of::<i64>();
/// Native pointer width recorded by the source package.
pub const UNSAFE_POINTER_LEN: usize = size_of::<*const ()>();

/// A malformed native spill row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SerializationError {
    /// The row ended before the requested value was complete.
    Truncated,
    /// A length prefix was negative or did not fit the remaining row.
    InvalidLength(isize),
    /// A boolean byte was outside the source value domain.
    InvalidBool(u8),
    /// An interface tag was not one of the source-supported values.
    InvalidInterfaceType(u8),
    /// Raw decimal bytes did not describe a valid `MyDecimal`.
    InvalidDecimal(&'static str),
    /// Raw packed-time bits did not describe a valid `Time`.
    InvalidTime(String),
}

impl fmt::Display for SerializationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Truncated => formatter.write_str("truncated native spill row"),
            Self::InvalidLength(length) => {
                write!(formatter, "invalid spill buffer length {length}")
            }
            Self::InvalidBool(value) => write!(formatter, "invalid spill boolean byte {value}"),
            Self::InvalidInterfaceType(value) => {
                write!(formatter, "invalid spill interface type {value}")
            }
            Self::InvalidDecimal(message) => formatter.write_str(message),
            Self::InvalidTime(message) => formatter.write_str(message),
        }
    }
}

impl Error for SerializationError {}

/// Source-supported values of Go's aggregate-spill `any` boundary.
#[derive(Clone, Debug, PartialEq)]
pub enum InterfaceValue {
    /// Boolean value.
    Bool(bool),
    /// Signed integer value.
    Int64(i64),
    /// Unsigned integer value.
    Uint64(u64),
    /// Floating-point value.
    Float64(f64),
    /// Arbitrary Go string bytes.
    String(GoString),
    /// Binary JSON value.
    BinaryJSON(BinaryJSON),
    /// Opaque JSON value.
    Opaque(Opaque),
    /// Packed MySQL time value.
    Time(Time),
    /// MySQL duration value, including its raw FSP metadata.
    Duration(MySqlDuration),
}

macro_rules! serializer {
    ($name:ident, $type:ty) => {
        #[doc = concat!("Appends one native-endian `", stringify!($type), "` value.")]
        pub fn $name(value: $type, output: &mut Vec<u8>) {
            output.extend_from_slice(&value.to_ne_bytes());
        }
    };
}

/// Appends one byte.
pub fn serialize_byte(value: u8, output: &mut Vec<u8>) {
    output.push(value);
}

/// Appends one source boolean byte.
pub fn serialize_bool(value: bool, output: &mut Vec<u8>) {
    output.push(u8::from(value));
}

serializer!(serialize_int, isize);
serializer!(serialize_i8, i8);
serializer!(serialize_u8, u8);
serializer!(serialize_i32, i32);
serializer!(serialize_u32, u32);
serializer!(serialize_u64, u64);
serializer!(serialize_i64, i64);
serializer!(serialize_f32, f32);
serializer!(serialize_f64, f64);

/// Appends a native-width length followed by the exact bytes.
pub fn serialize_buffer(value: &[u8], output: &mut Vec<u8>) {
    let length = isize::try_from(value.len()).expect("a Vec cannot exceed isize::MAX bytes");
    serialize_int(length, output);
    output.extend_from_slice(value);
}

/// Appends a bytes-buffer's currently visible bytes.
pub fn serialize_bytes_buffer(value: &[u8], output: &mut Vec<u8>) {
    serialize_buffer(value, output);
}

/// Appends the exact 40-byte Go `MyDecimal` layout.
pub fn serialize_my_decimal(value: &MyDecimal, output: &mut Vec<u8>) {
    output.extend_from_slice(&value.to_raw_bytes());
}

/// Appends the exact packed Go `types.Time` word.
pub fn serialize_time(value: Time, output: &mut Vec<u8>) {
    serialize_u64(value.go_raw(), output);
}

/// Appends a signed Go `time.Duration` nanosecond count.
pub fn serialize_go_duration(nanoseconds: i64, output: &mut Vec<u8>) {
    serialize_i64(nanoseconds, output);
}

/// Appends a TiDB duration's nanoseconds and native-width raw FSP.
pub fn serialize_duration(value: MySqlDuration, output: &mut Vec<u8>) {
    serialize_i64(value.nanoseconds(), output);
    serialize_int(value.fsp() as isize, output);
}

/// Appends one BinaryJSON type code.
pub fn serialize_json_type_code(value: u8, output: &mut Vec<u8>) {
    output.push(value);
}

/// Appends a BinaryJSON type code and length-prefixed value bytes.
pub fn serialize_binary_json(value: &BinaryJSON, output: &mut Vec<u8>) {
    serialize_json_type_code(value.type_code(), output);
    serialize_buffer(value.value(), output);
}

/// Appends a SET bit mask and byte-preserving name.
pub fn serialize_set(value: &MysqlSet, output: &mut Vec<u8>) {
    serialize_u64(value.value(), output);
    serialize_buffer(value.name_bytes(), output);
}

/// Appends an ENUM index and byte-preserving name.
pub fn serialize_enum(value: &MysqlEnum, output: &mut Vec<u8>) {
    serialize_u64(value.value(), output);
    serialize_buffer(value.name_bytes(), output);
}

/// Appends an opaque JSON type code and payload.
pub fn serialize_opaque(value: &Opaque, output: &mut Vec<u8>) {
    serialize_byte(value.type_code, output);
    serialize_buffer(&value.bytes, output);
}

/// Appends arbitrary Go string bytes.
pub fn serialize_string(value: &GoString, output: &mut Vec<u8>) {
    serialize_buffer(value.as_bytes(), output);
}

/// Appends a source-supported interface tag and payload.
pub fn serialize_interface(value: &InterfaceValue, output: &mut Vec<u8>) {
    match value {
        InterfaceValue::Bool(value) => {
            output.push(BOOL_TYPE);
            serialize_bool(*value, output);
        }
        InterfaceValue::Int64(value) => {
            output.push(INT64_TYPE);
            serialize_i64(*value, output);
        }
        InterfaceValue::Uint64(value) => {
            output.push(UINT64_TYPE);
            serialize_u64(*value, output);
        }
        InterfaceValue::Float64(value) => {
            output.push(FLOAT_TYPE);
            serialize_f64(*value, output);
        }
        InterfaceValue::String(value) => {
            output.push(STRING_TYPE);
            serialize_string(value, output);
        }
        InterfaceValue::BinaryJSON(value) => {
            output.push(BINARY_JSON_TYPE);
            serialize_binary_json(value, output);
        }
        InterfaceValue::Opaque(value) => {
            output.push(OPAQUE_TYPE);
            serialize_opaque(value, output);
        }
        InterfaceValue::Time(value) => {
            output.push(TIME_TYPE);
            serialize_time(*value, output);
        }
        InterfaceValue::Duration(value) => {
            output.push(DURATION_TYPE);
            serialize_duration(*value, output);
        }
    }
}

/// Borrowed positional decoder corresponding to Go's `PosAndBuf`.
#[derive(Clone, Debug)]
pub struct Cursor<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Cursor<'a> {
    /// Starts decoding at byte zero.
    #[must_use]
    pub const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    /// Reuses the decoder for another spill cell.
    pub fn reset(&mut self, bytes: &'a [u8]) {
        self.bytes = bytes;
        self.position = 0;
    }

    /// Returns the current byte position.
    #[must_use]
    pub const fn position(&self) -> usize {
        self.position
    }

    /// Returns the unread byte count.
    #[must_use]
    pub const fn remaining(&self) -> usize {
        self.bytes.len() - self.position
    }

    fn read_array<const N: usize>(&mut self) -> Result<[u8; N], SerializationError> {
        let end = self
            .position
            .checked_add(N)
            .ok_or(SerializationError::Truncated)?;
        let source = self
            .bytes
            .get(self.position..end)
            .ok_or(SerializationError::Truncated)?;
        let mut value = [0; N];
        value.copy_from_slice(source);
        self.position = end;
        Ok(value)
    }

    /// Reads one byte.
    pub fn read_byte(&mut self) -> Result<u8, SerializationError> {
        Ok(self.read_array::<1>()?[0])
    }

    /// Reads one source boolean byte.
    pub fn read_bool(&mut self) -> Result<bool, SerializationError> {
        match self.read_byte()? {
            0 => Ok(false),
            1 => Ok(true),
            value => Err(SerializationError::InvalidBool(value)),
        }
    }

    /// Reads one native-width signed word.
    pub fn read_int(&mut self) -> Result<isize, SerializationError> {
        Ok(isize::from_ne_bytes(self.read_array()?))
    }

    /// Reads one signed byte.
    pub fn read_i8(&mut self) -> Result<i8, SerializationError> {
        Ok(i8::from_ne_bytes(self.read_array()?))
    }

    /// Reads one unsigned byte.
    pub fn read_u8(&mut self) -> Result<u8, SerializationError> {
        self.read_byte()
    }

    /// Reads one native-endian signed 32-bit value.
    pub fn read_i32(&mut self) -> Result<i32, SerializationError> {
        Ok(i32::from_ne_bytes(self.read_array()?))
    }

    /// Reads one native-endian unsigned 32-bit value.
    pub fn read_u32(&mut self) -> Result<u32, SerializationError> {
        Ok(u32::from_ne_bytes(self.read_array()?))
    }

    /// Reads one native-endian unsigned 64-bit value.
    pub fn read_u64(&mut self) -> Result<u64, SerializationError> {
        Ok(u64::from_ne_bytes(self.read_array()?))
    }

    /// Reads one native-endian signed 64-bit value.
    pub fn read_i64(&mut self) -> Result<i64, SerializationError> {
        Ok(i64::from_ne_bytes(self.read_array()?))
    }

    /// Reads one native-endian 32-bit float.
    pub fn read_f32(&mut self) -> Result<f32, SerializationError> {
        Ok(f32::from_ne_bytes(self.read_array()?))
    }

    /// Reads one native-endian 64-bit float.
    pub fn read_f64(&mut self) -> Result<f64, SerializationError> {
        Ok(f64::from_ne_bytes(self.read_array()?))
    }

    /// Reads a native-width length and borrows the following bytes.
    pub fn read_buffer(&mut self) -> Result<&'a [u8], SerializationError> {
        let raw_length = self.read_int()?;
        let length = usize::try_from(raw_length)
            .map_err(|_| SerializationError::InvalidLength(raw_length))?;
        let end = self
            .position
            .checked_add(length)
            .ok_or(SerializationError::InvalidLength(raw_length))?;
        let value = self
            .bytes
            .get(self.position..end)
            .ok_or(SerializationError::InvalidLength(raw_length))?;
        self.position = end;
        Ok(value)
    }

    /// Reads one exact-layout `MyDecimal` value.
    pub fn read_my_decimal(&mut self) -> Result<MyDecimal, SerializationError> {
        MyDecimal::from_raw_bytes(self.read_array::<MYDECIMAL_STRUCT_SIZE>()?)
            .map_err(SerializationError::InvalidDecimal)
    }

    /// Reads one exact packed `Time` value.
    pub fn read_time(&mut self) -> Result<Time, SerializationError> {
        Time::from_go_raw(self.read_u64()?)
            .map_err(|error| SerializationError::InvalidTime(error.to_string()))
    }

    /// Reads one Go `time.Duration` value.
    pub fn read_go_duration(&mut self) -> Result<i64, SerializationError> {
        self.read_i64()
    }

    /// Reads one TiDB duration with raw FSP metadata.
    pub fn read_duration(&mut self) -> Result<MySqlDuration, SerializationError> {
        let nanoseconds = self.read_i64()?;
        let fsp = self.read_int()? as i64;
        Ok(MySqlDuration::from_raw_parts(nanoseconds, fsp))
    }

    /// Reads one BinaryJSON type code.
    pub fn read_json_type_code(&mut self) -> Result<u8, SerializationError> {
        self.read_byte()
    }

    /// Reads one BinaryJSON value and deep-copies its payload.
    pub fn read_binary_json(&mut self) -> Result<BinaryJSON, SerializationError> {
        let type_code = self.read_json_type_code()?;
        let value = self.read_buffer()?.to_vec();
        Ok(BinaryJSON::from_encoded_parts(type_code, value))
    }

    /// Reads one SET value and byte-preserving name.
    pub fn read_set(&mut self) -> Result<MysqlSet, SerializationError> {
        let value = self.read_u64()?;
        let name = GoString::from(self.read_buffer()?.to_vec());
        Ok(MysqlSet::new(name, value))
    }

    /// Reads one ENUM value and byte-preserving name.
    pub fn read_enum(&mut self) -> Result<MysqlEnum, SerializationError> {
        let value = self.read_u64()?;
        let name = GoString::from(self.read_buffer()?.to_vec());
        Ok(MysqlEnum::new(name, value))
    }

    /// Reads one opaque JSON value and deep-copies its payload.
    pub fn read_opaque(&mut self) -> Result<Opaque, SerializationError> {
        Ok(Opaque {
            type_code: self.read_byte()?,
            bytes: self.read_buffer()?.to_vec(),
        })
    }

    /// Reads one arbitrary Go string and deep-copies its bytes.
    pub fn read_string(&mut self) -> Result<GoString, SerializationError> {
        Ok(GoString::from(self.read_buffer()?.to_vec()))
    }

    /// Reads one byte buffer and deep-copies its contents.
    pub fn read_bytes_buffer(&mut self) -> Result<Vec<u8>, SerializationError> {
        Ok(self.read_buffer()?.to_vec())
    }

    /// Reads one source-supported interface tag and payload.
    pub fn read_interface(&mut self) -> Result<InterfaceValue, SerializationError> {
        match self.read_byte()? {
            BOOL_TYPE => self.read_bool().map(InterfaceValue::Bool),
            INT64_TYPE => self.read_i64().map(InterfaceValue::Int64),
            UINT64_TYPE => self.read_u64().map(InterfaceValue::Uint64),
            FLOAT_TYPE => self.read_f64().map(InterfaceValue::Float64),
            STRING_TYPE => self.read_string().map(InterfaceValue::String),
            BINARY_JSON_TYPE => self.read_binary_json().map(InterfaceValue::BinaryJSON),
            OPAQUE_TYPE => self.read_opaque().map(InterfaceValue::Opaque),
            TIME_TYPE => self.read_time().map(InterfaceValue::Time),
            DURATION_TYPE => self.read_duration().map(InterfaceValue::Duration),
            value => Err(SerializationError::InvalidInterfaceType(value)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_datatype::{CoreTime, TimeType};

    #[test]
    fn primitive_values_use_native_layout_and_advance_position() {
        let mut bytes = Vec::new();
        serialize_byte(0x5a, &mut bytes);
        serialize_bool(true, &mut bytes);
        serialize_int(-7, &mut bytes);
        serialize_i8(-8, &mut bytes);
        serialize_u8(9, &mut bytes);
        serialize_i32(-10, &mut bytes);
        serialize_u32(11, &mut bytes);
        serialize_u64(12, &mut bytes);
        serialize_i64(-13, &mut bytes);
        serialize_f32(1.25, &mut bytes);
        serialize_f64(-2.5, &mut bytes);

        let mut expected = vec![0x5a, 1];
        expected.extend_from_slice(&(-7_isize).to_ne_bytes());
        expected.extend_from_slice(&(-8_i8).to_ne_bytes());
        expected.extend_from_slice(&9_u8.to_ne_bytes());
        expected.extend_from_slice(&(-10_i32).to_ne_bytes());
        expected.extend_from_slice(&11_u32.to_ne_bytes());
        expected.extend_from_slice(&12_u64.to_ne_bytes());
        expected.extend_from_slice(&(-13_i64).to_ne_bytes());
        expected.extend_from_slice(&1.25_f32.to_ne_bytes());
        expected.extend_from_slice(&(-2.5_f64).to_ne_bytes());
        assert_eq!(bytes, expected);

        let mut cursor = Cursor::new(&bytes);
        assert_eq!(cursor.read_byte().unwrap(), 0x5a);
        assert!(cursor.read_bool().unwrap());
        assert_eq!(cursor.read_int().unwrap(), -7);
        assert_eq!(cursor.read_i8().unwrap(), -8);
        assert_eq!(cursor.read_u8().unwrap(), 9);
        assert_eq!(cursor.read_i32().unwrap(), -10);
        assert_eq!(cursor.read_u32().unwrap(), 11);
        assert_eq!(cursor.read_u64().unwrap(), 12);
        assert_eq!(cursor.read_i64().unwrap(), -13);
        assert_eq!(cursor.read_f32().unwrap(), 1.25);
        assert_eq!(cursor.read_f64().unwrap(), -2.5);
        assert_eq!(cursor.remaining(), 0);
        assert_eq!(INT_LEN, size_of::<isize>());
        assert_eq!(INTERFACE_TYPE_CODE_LEN, 1);
        assert_eq!(JSON_TYPE_CODE_LEN, 1);
        assert_eq!(BOOL_LEN, 1);
        assert_eq!(BYTE_LEN, 1);
        assert_eq!(INT8_LEN, 1);
        assert_eq!(UINT8_LEN, 1);
        assert_eq!(INT32_LEN, 4);
        assert_eq!(UINT32_LEN, 4);
        assert_eq!(INT64_LEN, 8);
        assert_eq!(UINT64_LEN, 8);
        assert_eq!(FLOAT32_LEN, 4);
        assert_eq!(FLOAT64_LEN, 8);
        assert_eq!(TIME_LEN, 8);
        assert_eq!(TIME_DURATION_LEN, 8);
        assert_eq!(UNSAFE_POINTER_LEN, size_of::<*const ()>());
    }

    #[test]
    fn typed_values_preserve_raw_bytes_and_metadata() {
        let decimal = MyDecimal::from_string(b"-123.4500").0;
        let time = Time::new(
            CoreTime::from_date(2026, 8, 12, 3, 4, 5, 600_000),
            TimeType::DateTime,
            4,
        )
        .unwrap();
        let duration = MySqlDuration::from_raw_parts(-9_876_543_210, -1);
        let json = BinaryJSON::from_encoded_parts(0x04, vec![1, 2, 0xff]);
        let enum_value = MysqlEnum::new(GoString::from(vec![b'e', 0xff]), 3);
        let set_value = MysqlSet::new(GoString::from(vec![b's', 0x00]), 5);
        let opaque = Opaque {
            type_code: 0xf5,
            bytes: vec![0, 0xff],
        };

        let mut bytes = Vec::new();
        serialize_my_decimal(&decimal, &mut bytes);
        serialize_time(time, &mut bytes);
        serialize_go_duration(-123, &mut bytes);
        serialize_duration(duration, &mut bytes);
        serialize_binary_json(&json, &mut bytes);
        serialize_enum(&enum_value, &mut bytes);
        serialize_set(&set_value, &mut bytes);
        serialize_opaque(&opaque, &mut bytes);
        serialize_string(&GoString::from(vec![0xff, b'x']), &mut bytes);
        serialize_buffer(&[7, 8], &mut bytes);

        let mut cursor = Cursor::new(&bytes);
        assert_eq!(cursor.read_my_decimal().unwrap(), decimal);
        assert_eq!(cursor.read_time().unwrap(), time);
        assert_eq!(cursor.read_go_duration().unwrap(), -123);
        assert_eq!(cursor.read_duration().unwrap(), duration);
        assert_eq!(cursor.read_binary_json().unwrap(), json);
        assert_eq!(cursor.read_enum().unwrap(), enum_value);
        assert_eq!(cursor.read_set().unwrap(), set_value);
        assert_eq!(cursor.read_opaque().unwrap(), opaque);
        assert_eq!(cursor.read_string().unwrap().as_bytes(), &[0xff, b'x']);
        assert_eq!(cursor.read_bytes_buffer().unwrap(), [7, 8]);
        assert_eq!(cursor.remaining(), 0);
    }

    #[test]
    fn every_interface_variant_round_trips() {
        let time = Time::from_go_raw(0).unwrap();
        let values = vec![
            InterfaceValue::Bool(false),
            InterfaceValue::Int64(-5),
            InterfaceValue::Uint64(u64::MAX),
            InterfaceValue::Float64(3.5),
            InterfaceValue::String(GoString::from(vec![0xff, 0])),
            InterfaceValue::BinaryJSON(BinaryJSON::from_encoded_parts(0x04, vec![1, 2])),
            InterfaceValue::Opaque(Opaque {
                type_code: 7,
                bytes: vec![8, 9],
            }),
            InterfaceValue::Time(time),
            InterfaceValue::Duration(MySqlDuration::from_raw_parts(42, 7)),
        ];
        for (expected_tag, value) in values.into_iter().enumerate() {
            let mut bytes = Vec::new();
            serialize_interface(&value, &mut bytes);
            assert_eq!(bytes[0], expected_tag as u8);
            let mut cursor = Cursor::new(&bytes);
            assert_eq!(cursor.read_interface().unwrap(), value);
            assert_eq!(cursor.remaining(), 0);
        }
    }

    #[test]
    fn reset_and_malformed_rows_are_explicit() {
        let mut cursor = Cursor::new(&[1]);
        assert!(cursor.read_bool().unwrap());
        cursor.reset(&[2]);
        assert_eq!(cursor.read_bool(), Err(SerializationError::InvalidBool(2)));
        cursor.reset(&[0xff; INT_LEN]);
        assert_eq!(
            cursor.read_buffer(),
            Err(SerializationError::InvalidLength(-1))
        );
        cursor.reset(&[99]);
        assert_eq!(
            cursor.read_interface(),
            Err(SerializationError::InvalidInterfaceType(99))
        );
        cursor.reset(&[]);
        assert_eq!(cursor.read_i64(), Err(SerializationError::Truncated));
    }
}
