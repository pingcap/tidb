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
/// Serialized width of one packed Go `types.Time` value.
pub const TIME_LEN: usize = size_of::<u64>();
/// Serialized width of one signed Go `time.Duration`.
pub const TIME_DURATION_LEN: usize = size_of::<i64>();
/// Native pointer width recorded by the source package.
pub const UNSAFE_POINTER_LEN: usize = size_of::<*const ()>();

/// Source-supported values of Go's aggregate-spill `any` boundary.
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

fn serialize_buffer(value: &[u8], output: &mut Vec<u8>) {
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
pub struct Cursor<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Cursor<'a> {
    /// Starts decoding at byte zero.
    pub const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    /// Reuses the decoder for another spill cell.
    pub fn reset(&mut self, bytes: &'a [u8]) {
        self.bytes = bytes;
        self.position = 0;
    }

    /// Returns the current byte position.
    pub const fn position(&self) -> usize {
        self.position
    }

    fn read_array<const N: usize>(&mut self) -> [u8; N] {
        let end = self.position + N;
        let source = &self.bytes[self.position..end];
        let mut value = [0; N];
        value.copy_from_slice(source);
        self.position = end;
        value
    }

    /// Reads one byte.
    pub fn read_byte(&mut self) -> u8 {
        self.read_array::<1>()[0]
    }

    /// Reads one source boolean byte.
    pub fn read_bool(&mut self) -> bool {
        self.read_byte() & 1 != 0
    }

    /// Reads one native-width signed word.
    pub fn read_int(&mut self) -> isize {
        isize::from_ne_bytes(self.read_array())
    }

    /// Reads one signed byte.
    pub fn read_i8(&mut self) -> i8 {
        i8::from_ne_bytes(self.read_array())
    }

    /// Reads one unsigned byte.
    pub fn read_u8(&mut self) -> u8 {
        self.read_byte()
    }

    /// Reads one native-endian signed 32-bit value.
    pub fn read_i32(&mut self) -> i32 {
        i32::from_ne_bytes(self.read_array())
    }

    /// Reads one native-endian unsigned 32-bit value.
    pub fn read_u32(&mut self) -> u32 {
        u32::from_ne_bytes(self.read_array())
    }

    /// Reads one native-endian unsigned 64-bit value.
    pub fn read_u64(&mut self) -> u64 {
        u64::from_ne_bytes(self.read_array())
    }

    /// Reads one native-endian signed 64-bit value.
    pub fn read_i64(&mut self) -> i64 {
        i64::from_ne_bytes(self.read_array())
    }

    /// Reads one native-endian 32-bit float.
    pub fn read_f32(&mut self) -> f32 {
        f32::from_ne_bytes(self.read_array())
    }

    /// Reads one native-endian 64-bit float.
    pub fn read_f64(&mut self) -> f64 {
        f64::from_ne_bytes(self.read_array())
    }

    fn read_buffer(&mut self) -> &'a [u8] {
        let length = self.read_int() as usize;
        let end = self.position + length;
        let value = &self.bytes[self.position..end];
        self.position = end;
        value
    }

    /// Reads one exact-layout `MyDecimal` value.
    pub fn read_my_decimal(&mut self) -> MyDecimal {
        MyDecimal::from_raw_bytes_like_go(self.read_array::<MYDECIMAL_STRUCT_SIZE>())
    }

    /// Reads one exact packed `Time` value.
    pub fn read_time(&mut self) -> Time {
        Time::from_go_raw_like_go(self.read_u64())
    }

    /// Reads one Go `time.Duration` value.
    pub fn read_go_duration(&mut self) -> i64 {
        self.read_i64()
    }

    /// Reads one TiDB duration with raw FSP metadata.
    pub fn read_duration(&mut self) -> MySqlDuration {
        let nanoseconds = self.read_i64();
        let fsp = self.read_int() as i64;
        MySqlDuration::from_raw_parts(nanoseconds, fsp)
    }

    /// Reads one BinaryJSON type code.
    pub fn read_json_type_code(&mut self) -> u8 {
        self.read_byte()
    }

    /// Reads one BinaryJSON value and deep-copies its payload.
    pub fn read_binary_json(&mut self) -> BinaryJSON {
        let type_code = self.read_json_type_code();
        let value = self.read_buffer().to_vec();
        BinaryJSON::from_encoded_parts(type_code, value)
    }

    /// Reads one SET value and byte-preserving name.
    pub fn read_set(&mut self) -> MysqlSet {
        let value = self.read_u64();
        let name = GoString::from(self.read_buffer().to_vec());
        MysqlSet::new(name, value)
    }

    /// Reads one ENUM value and byte-preserving name.
    pub fn read_enum(&mut self) -> MysqlEnum {
        let value = self.read_u64();
        let name = GoString::from(self.read_buffer().to_vec());
        MysqlEnum::new(name, value)
    }

    /// Reads one opaque JSON value and deep-copies its payload.
    pub fn read_opaque(&mut self) -> Opaque {
        Opaque {
            type_code: self.read_byte(),
            bytes: self.read_buffer().to_vec(),
        }
    }

    /// Reads one arbitrary Go string and deep-copies its bytes.
    pub fn read_string(&mut self) -> GoString {
        GoString::from(self.read_buffer().to_vec())
    }

    /// Reads one byte buffer and deep-copies its contents.
    pub fn read_bytes_buffer(&mut self) -> Vec<u8> {
        self.read_buffer().to_vec()
    }

    /// Reads one source-supported interface tag and payload.
    pub fn read_interface(&mut self) -> InterfaceValue {
        match self.read_byte() {
            BOOL_TYPE => InterfaceValue::Bool(self.read_bool()),
            INT64_TYPE => InterfaceValue::Int64(self.read_i64()),
            UINT64_TYPE => InterfaceValue::Uint64(self.read_u64()),
            FLOAT_TYPE => InterfaceValue::Float64(self.read_f64()),
            STRING_TYPE => InterfaceValue::String(self.read_string()),
            BINARY_JSON_TYPE => InterfaceValue::BinaryJSON(self.read_binary_json()),
            OPAQUE_TYPE => InterfaceValue::Opaque(self.read_opaque()),
            TIME_TYPE => InterfaceValue::Time(self.read_time()),
            DURATION_TYPE => InterfaceValue::Duration(self.read_duration()),
            _ => panic!("Invalid data type happens in agg spill deserializing!"),
        }
    }
}
