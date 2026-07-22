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

//! Source-backed MySQL packet framing, including zlib/zstd compression.
//!
//! This leaf owns the four-byte inner packet header, continuation rules, and
//! seven-byte compressed envelope from `pkg/server/internal/packetio.go`.
//! TLS, authentication, socket deadlines, metrics, and server dispatch remain
//! explicit server obligations.

mod binary_params;
mod column;
mod command;
mod compression;
mod error;
mod error_conversion;
mod error_packet;
mod packet;
mod prepared_statement;
mod result;
mod result_encoder;
mod resultset;
pub mod resultset_stream;
mod textrow;

pub use binary_params::{
    parse_binary_params, parse_length_encoded_int, BinaryParam, BinaryParamError, TYPE_NULL,
    TYPE_UNSPECIFIED,
};
pub use column::{
    dump_column, dump_column_with_default, dump_flag, dump_type, ColumnDefault, ColumnInfo,
    BINARY_DEFAULT_COLLATION_ID, BINARY_FLAG, DEFAULT_COLLATION_ID, ENUM_FLAG,
    MAX_COLUMN_NAME_SIZE, MAX_LONG_BLOB_WIDTH, SET_FLAG, TYPE_BIT, TYPE_BLOB, TYPE_ENUM, TYPE_JSON,
    TYPE_LONG_BLOB, TYPE_MEDIUM_BLOB, TYPE_NEW_DATE, TYPE_SET, TYPE_STRING,
    TYPE_TIDB_VECTOR_FLOAT32, TYPE_TINY_BLOB, TYPE_VARCHAR, TYPE_VAR_STRING,
};
pub use command::{
    decode_command, Command, CommandError, COM_FIELD_LIST, COM_INIT_DB, COM_PING, COM_QUERY,
    COM_QUIT, COM_RESET_CONNECTION, COM_SET_OPTION, COM_STMT_CLOSE, COM_STMT_EXECUTE,
    COM_STMT_FETCH, COM_STMT_PREPARE, COM_STMT_RESET,
};
pub use compression::{
    CompressedHeader, CompressedReader, CompressedWriter, CompressionAlgorithm,
    MAX_COMPRESSED_BATCH_SIZE, MIN_COMPRESS_LENGTH,
};
pub use error::PacketError;
pub use error_conversion::{
    error_packet_from_descriptor, ErrorDescriptor, ErrorKind, MYSQL_ERR_BAD_FIELD,
    MYSQL_ERR_DATA_TOO_LONG, MYSQL_ERR_DUP_ENTRY, MYSQL_ERR_DUP_KEY_NAME,
    MYSQL_ERR_NOT_SUPPORTED_YET, MYSQL_ERR_PARSE, MYSQL_ERR_UNKNOWN, MYSQL_ERR_UNKNOWN_TABLE,
    MYSQL_ERR_WARN_DATA_OUT_OF_RANGE, MYSQL_ERR_WRONG_VALUE_COUNT_ON_ROW,
};
pub use error_packet::{encode_error_packet, ErrorPacket, ERR_HEADER};
pub use packet::{
    PacketHeader, PacketIoReader, PacketIoWriter, PacketReader, PacketWriter,
    DEFAULT_MAX_ALLOWED_PACKET, MAX_PAYLOAD_LEN,
};
pub use prepared_statement::{
    decode_prepared_statement_close, decode_prepared_statement_execute, encode_binary_datetime,
    encode_binary_result_row, encode_binary_signed_longlong_row, encode_binary_time,
    encode_prepared_statement_prepare_response, is_binary_decimal_result_type,
    is_binary_float_result_type, is_binary_integer_result_type, is_binary_string_result_type,
    BinaryDateTimeType, BinaryResultCell, BinaryResultSetStream, PreparedParameterType,
    PreparedParameterTypes, PreparedStatementError, PreparedStatementExecute, PreparedValue,
    MYSQL_TYPE_LONGLONG, MYSQL_UNSIGNED_FLAG,
};
pub use result::{
    append_length_encoded_bytes, append_length_encoded_int, encode_text_row, is_string_column_type,
    NULL_MARKER,
};
pub use result_encoder::{
    is_string_column_type as is_string_result_column_type, ResultCharset, ResultEncoder,
    ResultEncoderError, ASCII_DEFAULT_COLLATION_ID, GBK_DEFAULT_COLLATION_ID,
    LATIN1_DEFAULT_COLLATION_ID, UTF8MB4_DEFAULT_COLLATION_ID, UTF8MB4_GENERAL_CI_COLLATION_ID,
    UTF8MB4_UNICODE_CI_COLLATION_ID, UTF8_DEFAULT_COLLATION_ID, UTF8_GENERAL_CI_COLLATION_ID,
    UTF8_UNICODE_CI_COLLATION_ID,
};
pub use resultset::{
    encode_eof_packet, encode_ok_packet, encode_text_result_set, EofPacket, OkPacket,
    ResultSetError, ResultSetOptions, EOF_HEADER, OK_HEADER,
};
pub use textrow::{
    append_format_float, format_text_value, TextColumn, TextFormatError, TextScalar,
    NOT_FIXED_DECIMAL, TYPE_DATE, TYPE_DATETIME, TYPE_DOUBLE, TYPE_DURATION, TYPE_FLOAT,
    TYPE_GEOMETRY, TYPE_INT24, TYPE_LONG, TYPE_LONGLONG, TYPE_NEW_DECIMAL, TYPE_SHORT,
    TYPE_TIMESTAMP, TYPE_TINY, TYPE_YEAR, UNSIGNED_FLAG,
};
