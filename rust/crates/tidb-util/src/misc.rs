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

//! Go `pkg/util/misc.go`: the statement-level helpers of the root `util`
//! package.

use prost::encoding::{encode_varint, encoded_len_varint, WireType};
use prost::Message;
use tidb_datatype::DateModes;
use tidb_goruntime::{go_runtime::GoSharedSlice, GoString};
use tidb_model::ColumnInfo;
use tidb_proto::tipb;

/// Go `GetTypeFlagsForInsert` / `ResetUpdateStmtCtx`, in the two bits that
/// decide whether a zero-in or invalid date PARSES at all.
///
/// The `IgnoreZeroInDate` expression is Go's verbatim, and its shape is the
/// point: the zero-in-date is let through unless `NO_ZERO_IN_DATE` **and**
/// `NO_ZERO_DATE` **and** strict mode are all present and
/// `ALLOW_INVALID_DATES` is absent. Any one of those missing and
/// `'2024-00-01'` reaches Go `handleZeroDatetime` as a real value instead of
/// a conversion failure -- which is what makes `NO_ZERO_IN_DATE` alone a
/// warning rather than an error.
#[must_use]
pub fn get_type_flags_for_insert(
    base: tidb_datatype::ConversionFlags,
    modes: DateModes,
    strict: bool,
) -> tidb_datatype::ConversionFlags {
    base.with_ignore_invalid_date_err(modes.allow_invalid_dates)
        .with_ignore_zero_in_date_err(
            !modes.no_zero_in_date || !modes.no_zero_date || !strict || modes.allow_invalid_dates,
        )
}

/// Go `SyntaxErrorPrefix`: the common prefix for SQL syntax errors in TiDB.
pub const SYNTAX_ERROR_PREFIX: &str = "You have an error in your SQL syntax; check the manual that corresponds to your TiDB version for the right syntax to use";

/// Go `SyntaxError` for a parser failure that is not already a
/// `*terror.Error`: `parser.ErrParse.GenWithStackByArgs(SyntaxErrorPrefix,
/// err.Error())`.
pub fn syntax_error(message: impl Into<String>) -> tidb_terror::TerrorError {
    tidb_parser::parser_errors::ERR_PARSE
        .gen_with_stack_by_args(&[SYNTAX_ERROR_PREFIX.into(), message.into().into()])
}

/// Go `util.SequenceTable`: the sequence operations a table object of a
/// `CREATE SEQUENCE` provides, so expression evaluation can drive one without
/// depending on the table layer. `ctx` is Go's session argument; the
/// database and sequence names are what its errors name.
pub trait SequenceTable: std::fmt::Debug + Send + Sync {
    /// Go `GetSequenceID`.
    fn get_sequence_id(&self) -> i64;

    /// Go `GetSequenceNextVal`.
    fn get_sequence_next_val(
        &self,
        ctx: &dyn std::any::Any,
        db_name: &str,
        seq_name: &str,
    ) -> Result<i64, tidb_terror::TerrorError>;

    /// Go `SetSequenceVal`: the value set, and whether the counter was
    /// already at or past `new_val` (Go's `alreadySatisfied`, nothing set).
    fn set_sequence_val(
        &self,
        ctx: &dyn std::any::Any,
        new_val: i64,
        db_name: &str,
        seq_name: &str,
    ) -> Result<(i64, bool), tidb_terror::TerrorError>;
}

/// Go `util.GetSequenceByName` over a statement's infoschema: the
/// sequences its `NEXTVAL` / `LASTVAL` / `SETVAL` resolve by name.
pub trait SequenceSchema: std::fmt::Debug + Send + Sync {
    /// The session's current database, lowercase: the schema of a bare name.
    fn current_db(&self) -> &str;

    /// Go `infoschema.GetSequenceByName`: the sequence `schema.sequence`
    /// names, or the infoschema's error for a missing object or an object
    /// that is not a sequence.
    fn get_sequence_by_name(
        &self,
        schema: &str,
        sequence: &str,
    ) -> Result<&dyn SequenceTable, tidb_terror::TerrorError>;

    /// The `(database, name)` a one- or two-part sequence path names.
    fn schema_and_name<'a>(&'a self, path: &'a [String]) -> Option<(&'a str, &'a str)> {
        match path {
            [name] => Some((self.current_db(), name.as_str())),
            [database, name] => Some((database.as_str(), name.as_str())),
            _ => None,
        }
    }

    /// The lowercase `(database, name)` the lookup and its errors name for
    /// `path`.
    fn key(&self, path: &[String]) -> (String, String) {
        use tidb_hack::GoToLower;
        match path {
            [name] => (self.current_db().to_string(), name.go_to_lower()),
            [database, name] => (database.go_to_lower(), name.go_to_lower()),
            [] => (String::new(), String::new()),
            [leading @ .., name] => (
                leading
                    .iter()
                    .map(|part| part.go_to_lower())
                    .collect::<Vec<_>>()
                    .join("."),
                name.go_to_lower(),
            ),
        }
    }
}

/// The sequences of a context with no infoschema: none, so every name is
/// Go's `infoschema.ErrTableNotExists` (`ClassSchema.NewStd(ErrNoSuchTable)`).
#[derive(Debug)]
pub struct NoSequences;

impl SequenceSchema for NoSequences {
    fn current_db(&self) -> &str {
        ""
    }

    fn get_sequence_by_name(
        &self,
        schema: &str,
        sequence: &str,
    ) -> Result<&dyn SequenceTable, tidb_terror::TerrorError> {
        Err(tidb_dbterror::catalog_support::std_err(
            tidb_dbterror::catalog_support::Class::Schema,
            tidb_errno::errcode::ErrNoSuchTable,
        )
        .gen_with_stack_by_args(&[schema.into(), sequence.into()]))
    }
}

/// Go `GetLocalIP`: the first global-unicast address among the host's
/// interface addresses (`net.InterfaceAddrs`), or `""` when there is none.
#[must_use]
pub fn get_local_ip() -> String {
    let Ok(addresses) = nix::ifaddrs::getifaddrs() else {
        return String::new();
    };
    for interface in addresses {
        let Some(address) = interface.address.as_ref() else {
            continue;
        };
        let ip = if let Some(v4) = address.as_sockaddr_in() {
            std::net::IpAddr::V4(v4.ip())
        } else if let Some(v6) = address.as_sockaddr_in6() {
            std::net::IpAddr::V6(v6.ip())
        } else {
            continue;
        };
        if is_global_unicast(ip) {
            return ip.to_string();
        }
    }
    String::new()
}

/// Go `net.IP.IsGlobalUnicast`: neither the IPv4 limited broadcast address,
/// unspecified, loopback, multicast nor link-local unicast. Private ranges
/// are global unicast.
#[must_use]
pub fn is_global_unicast(ip: std::net::IpAddr) -> bool {
    match ip {
        std::net::IpAddr::V4(v4) => {
            v4 != std::net::Ipv4Addr::BROADCAST
                && !v4.is_unspecified()
                && !v4.is_loopback()
                && !v4.is_multicast()
                && !v4.is_link_local()
        }
        std::net::IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(v4) => is_global_unicast(std::net::IpAddr::V4(v4)),
            None => {
                !v6.is_unspecified()
                    && !v6.is_loopback()
                    && !v6.is_multicast()
                    && (v6.segments()[0] & 0xffc0) != 0xfe80
            }
        },
    }
}

#[cfg(test)]
mod local_ip_tests {
    use super::is_global_unicast;

    /// Go `net`'s `TestIPAttrs` rows for `IsGlobalUnicast`.
    #[test]
    fn global_unicast_follows_go_net() {
        for (ip, global) in [
            ("0.0.0.0", false),
            ("255.255.255.255", false),
            ("127.0.0.1", false),
            ("10.0.0.1", true),
            ("192.168.1.219", true),
            ("169.254.0.1", false),
            ("224.0.0.1", false),
            ("8.8.8.8", true),
            ("::", false),
            ("::1", false),
            ("fe80::1", false),
            ("ff02::1", false),
            ("2001:db8::1", true),
            ("::ffff:192.168.1.1", true),
            ("::ffff:127.0.0.1", false),
        ] {
            assert_eq!(is_global_unicast(ip.parse().unwrap()), global, "{ip}");
        }
    }
}

/// Go `ColumnsToProto`: the coprocessor descriptors of `columns`, a column
/// marked `PkHandle` when it is the int handle (`pk_is_handle` and a
/// primary-key column) or `_tidb_rowid`.
#[must_use]
pub fn columns_to_proto(
    columns: &[tidb_model::GoShared<tidb_model::ColumnInfo>],
    pk_is_handle: bool,
    for_index: bool,
    is_tiflash_store: bool,
) -> Vec<tidb_proto::tipb::ColumnInfo> {
    columns
        .iter()
        .map(|c| {
            let c = c.read();
            let mut col = column_to_proto(&c, for_index, is_tiflash_store);
            // TODO: Here `PkHandle`'s meaning is changed, we will change it to `IsHandle` when tikv's old select logic
            // is abandoned.
            col.pk_handle = Some(column_is_handle(&c, pk_is_handle));
            col
        })
        .collect()
}

/// Go `ColumnToProto`: converts `model.ColumnInfo` to `tipb.ColumnInfo`.
/// Go reads the collation id from `mysql.CollationNames`; a column's
/// collation is always one TiDB supports, which the collation registry
/// answers alike.
#[must_use]
pub fn column_to_proto(c: &tidb_model::ColumnInfo, for_index: bool, is_tiflash_store: bool) -> tidb_proto::tipb::ColumnInfo {
    let mut fields = column_proto_fields(c, for_index, is_tiflash_store);
    fields.elems = c.field_type.with_elems_visible(column_elements_to_proto);
    fields
}

fn column_is_handle(column: &ColumnInfo, pk_is_handle: bool) -> bool {
    (pk_is_handle && tidb_mysql::types::has_pri_key_flag(column.field_type.flags() as usize))
        || column.id == tidb_model::column::EXTRA_HANDLE_ID
}

#[inline]
fn column_proto_fields(c: &ColumnInfo, for_index: bool, is_tiflash_store: bool) -> tipb::ColumnInfo {
    let field_type = &c.field_type;
    let code = if for_index {
        field_type.array_element_code()
    } else {
        field_type.code()
    };
    let collation = if for_index && field_type.is_array() {
        // An index reads the element type of a multi-valued column with
        // binary collation, including older metadata with another collation.
        tidb_datatype::collation_name_to_id("binary")
    } else {
        tidb_datatype::collation_to_proto(field_type.collation_name())
    };
    let mut flag = field_type.flags() as i32;
    if is_tiflash_store && c.is_virtual_generated() {
        flag |= tidb_mysql::types::GeneratedColumnFlag as i32;
    }
    tidb_proto::tipb::ColumnInfo {
        column_id: Some(c.id),
        tp: Some(i32::from(code.mysql_type())),
        collation: Some(collation),
        column_len: Some(field_type.flen() as i32),
        decimal: Some(field_type.decimal() as i32),
        flag: Some(flag),
        // gogoproto's non-nullable fields are always sent, zero included.
        pk_handle: Some(false),
        array: Some(false),
        ..tidb_proto::tipb::ColumnInfo::default()
    }
}

/// A TiPB column descriptor retaining the source ENUM and SET label slice.
#[derive(Clone, Debug)]
pub struct ColumnProjection {
    fields: tipb::ColumnInfo,
    labels: GoSharedSlice<GoString>,
}

impl ColumnProjection {
    /// Separates the generated metadata fields and the shared label slice.
    pub fn into_parts(self) -> (tipb::ColumnInfo, GoSharedSlice<GoString>) {
        (self.fields, self.labels)
    }

    /// Appends the encoded `ColumnInfo`, including its visible labels.
    pub fn encode(&self, output: &mut Vec<u8>) {
        self.labels.with_visible(|labels| {
            let (label_size, packed_size) = column_label_size(labels);
            output.reserve(self.fields.encoded_len() + label_size);
            encode_projected_column(&self.fields, labels, packed_size, output);
        });
    }
}

/// Go `ColumnToProto` for a caller that encodes directly to the wire.
#[must_use]
#[inline]
pub fn column_to_proto_shared(c: &ColumnInfo, for_index: bool, is_tiflash_store: bool) -> ColumnProjection {
    ColumnProjection {
        fields: column_proto_fields(c, for_index, is_tiflash_store),
        labels: c.field_type.elems(),
    }
}

/// Go `ColumnsToProto` for a caller that encodes directly to the wire.
#[must_use]
pub fn columns_to_proto_shared(
    columns: &[tidb_model::GoShared<ColumnInfo>],
    pk_is_handle: bool,
    for_index: bool,
    is_tiflash_store: bool,
) -> Vec<ColumnProjection> {
    columns.iter().map(|column| {
        let column = column.read();
        let mut result = column_to_proto_shared(&column, for_index, is_tiflash_store);
        result.fields.pk_handle = Some(column_is_handle(&column, pk_is_handle));
        result
    }).collect()
}

/// Appends generated `ColumnInfo` metadata with labels from the source slice.
/// The metadata must have an empty `elems` field.
pub fn encode_projected_column(
    fields: &tipb::ColumnInfo,
    labels: &[GoString],
    packed_size: Option<usize>,
    output: &mut Vec<u8>,
) {
    debug_assert!(fields.elems.is_empty());
    if labels.is_empty() {
        fields.encode_raw(output);
        return;
    }
    let mut prefix = fields.clone();
    let suffix = tipb::ColumnInfo {
        default_val: prefix.default_val.take(),
        pk_handle: prefix.pk_handle.take(),
        array: prefix.array.take(),
        ..Default::default()
    };
    prefix.encode_raw(output);
    encode_column_labels(labels, packed_size, output);
    suffix.encode_raw(output);
}

/// Copies the visible ENUM and SET labels into the strings used by TiPB.
#[inline]
pub fn column_elements_to_proto(elements: &[tidb_goruntime::GoString]) -> Vec<String> {
    if elements.is_empty() {
        return Vec::new();
    }
    let mut labels = Vec::with_capacity(elements.len());
    visit_column_proto_elements(elements, |label| labels.push(label.to_owned()));
    labels
}

/// Visits the UTF-8 labels that TiPB will serialize.
#[inline]
pub fn visit_column_proto_elements(
    elements: &[tidb_goruntime::GoString],
    mut visit: impl FnMut(&str),
) {
    const LABELS_PER_BATCH: usize = 16;
    const MAX_BATCHED_LABEL_BYTES: usize = 63;

    if elements.len() >= LABELS_PER_BATCH
        && elements[0].len() <= MAX_BATCHED_LABEL_BYTES
        && elements[0].known_utf8().is_none()
        && !elements[0].as_bytes().is_ascii()
        && elements.iter().all(|label| label.len() <= MAX_BATCHED_LABEL_BYTES)
    {
        let mut packed = Vec::with_capacity(LABELS_PER_BATCH * (MAX_BATCHED_LABEL_BYTES + 1));
        for batch in elements.chunks(LABELS_PER_BATCH) {
            packed.clear();
            // An ASCII separator prevents a partial UTF-8 sequence
            // in one label from consuming continuation bytes in another.
            for label in batch {
                packed.extend_from_slice(label.as_bytes());
                packed.push(0);
            }
            if let Ok(mut text) = simdutf8::basic::from_utf8(&packed) {
                for label in batch {
                    let (value, remainder) = text.split_at(label.len());
                    visit(value);
                    text = &remainder[1..];
                }
            } else {
                for label in batch {
                    visit(&label.to_utf8_lossy_go());
                }
            }
        }
        return;
    }
    for element in elements {
        if let Some(text) = element.known_utf8() {
            visit(text);
            continue;
        }
        match simdutf8::basic::from_utf8(element.as_bytes()) {
            Ok(text) => visit(text),
            Err(_) => visit(&element.to_utf8_lossy_go()),
        }
    }
}

/// Estimates the encoded size of the column labels and recognizes groups
/// eligible for direct writes into the output buffer.
#[doc(hidden)]
pub fn column_label_size(labels: &[GoString]) -> (usize, Option<usize>) {
    let (width_sum, width_bits) = labels.iter().fold((0, 0), |(sum, width_bits), label| {
        let width = label.len();
        (sum + width, width_bits | width)
    });
    if width_bits < 128 {
        let length = width_sum + 2 * labels.len();
        (length, Some(length))
    } else if width_bits < 16_384 && width_bits.checked_mul(labels.len()) == Some(width_sum) {
        // Each width is a subset of the accumulated bits. The sum proves
        // that every width equals that upper bound.
        let length = width_sum + 3 * labels.len();
        (length, (width_bits <= MAX_INLINED_LABEL_WIDTH).then_some(length))
    } else {
        (labels.iter().map(|label| 1 + encoded_len_varint(label.len() as u64) + label.len()).sum(), None)
    }
}

const COLUMN_ELEMENT_TAG: u32 = 7; // ColumnInfo.elems
// Beyond the inlined word copies, append the payload with one slice copy.
const MAX_INLINED_LABEL_WIDTH: usize = 256;

/// Appends the repeated `ColumnInfo.elems` fields.
#[doc(hidden)]
#[inline]
pub fn encode_column_labels(
    labels: &[GoString],
    packed_size: Option<usize>,
    output: &mut Vec<u8>,
) {
    let packed = packed_size.is_some_and(|length| {
        match labels.first().map(GoString::len) {
            Some(width @ 128..=MAX_INLINED_LABEL_WIDTH) => {
                try_encode_equal_width_labels(labels, width, length, output)
            }
            _ => try_encode_short_column_labels(labels, length, output),
        }
    });
    if !packed {
        visit_column_proto_elements(labels, |label| {
            encode_column_label(label, output);
        });
    }
}

fn try_encode_equal_width_labels(
    labels: &[GoString],
    width: usize,
    estimated_length: usize,
    output: &mut Vec<u8>,
) -> bool {
    let field_width = width + 3;
    if labels.len() < 16 || field_width.checked_mul(labels.len()) != Some(estimated_length) {
        return false;
    }
    let start = output.len();
    output.resize(start + estimated_length, 0);
    let key = ((COLUMN_ELEMENT_TAG << 3) | WireType::LengthDelimited as u32) as u8;
    let header = [key, (width as u8) | 0x80, (width >> 7) as u8];
    for (label, field) in labels.iter().zip(output[start..].chunks_exact_mut(field_width)) {
        let Some(text) = label.known_utf8().filter(|text| text.len() == width) else {
            output.truncate(start);
            return false;
        };
        field[..3].copy_from_slice(&header);
        copy_short_label(&mut field[3..], text.as_bytes());
    }
    true
}

fn try_encode_short_column_labels(
    labels: &[GoString],
    estimated_length: usize,
    output: &mut Vec<u8>,
) -> bool {
    if labels.len() < 16 {
        return false;
    }
    let start = output.len();
    output.resize(start + estimated_length, 0);
    let mut remaining = &mut output[start..];
    let key = ((COLUMN_ELEMENT_TAG << 3) | WireType::LengthDelimited as u32) as u8;
    let mut known_utf8 = true;
    for label in labels {
        let bytes = if let Some(text) = label.known_utf8() {
            text.as_bytes()
        } else {
            known_utf8 = false;
            label.as_bytes()
        };
        if bytes.len() >= 128 {
            output.truncate(start);
            return false;
        }
        // Another slice header can replace a label after the sizing pass.
        let Some((field, tail)) = remaining.split_at_mut_checked(2 + bytes.len()) else {
            output.truncate(start);
            return false;
        };
        field[0] = key;
        field[1] = bytes.len() as u8;
        copy_short_label(&mut field[2..], bytes);
        remaining = tail;
    }
    let written = estimated_length - remaining.len();
    output.truncate(start + written);
    // The key and length bytes are ASCII. They separate the labels, so no
    // invalid UTF-8 sequences can become valid across two adjacent fields.
    if known_utf8 || simdutf8::basic::from_utf8(&output[start..]).is_ok() {
        true
    } else {
        output.truncate(start);
        false
    }
}

#[inline]
fn copy_short_label(output: &mut [u8], bytes: &[u8]) {
    #[inline]
    fn copy_ends<const WIDTH: usize>(output: &mut [u8], bytes: &[u8]) {
        *output.first_chunk_mut::<WIDTH>().expect("label width") =
            *bytes.first_chunk::<WIDTH>().expect("label width");
        *output.last_chunk_mut::<WIDTH>().expect("label width") =
            *bytes.last_chunk::<WIDTH>().expect("label width");
    }

    match bytes.len() {
        8..=16 => copy_ends::<8>(output, bytes),
        17..=32 => copy_ends::<16>(output, bytes),
        33..=64 => copy_ends::<32>(output, bytes),
        65..=128 => copy_ends::<64>(output, bytes),
        129..=MAX_INLINED_LABEL_WIDTH => copy_ends::<128>(output, bytes),
        _ => output.copy_from_slice(bytes),
    }
}

fn encode_column_label(label: &str, output: &mut Vec<u8>) {
    let key = ((COLUMN_ELEMENT_TAG << 3) | WireType::LengthDelimited as u32) as u8;
    let length = label.len();
    if length < 128 {
        output.extend_from_slice(&[key, length as u8]);
    } else if length < 16_384 {
        output.extend_from_slice(&[key, length as u8 | 0x80, (length >> 7) as u8]);
    } else {
        output.push(key);
        encode_varint(length as u64, output);
    }
    output.extend_from_slice(label.as_bytes());
}

#[cfg(test)]
mod column_to_proto_tests {
    use prost::Message;
    use tidb_datatype::{set_new_collation_enabled_for_test, FieldType, FieldTypeBuilder, FieldTypeCode};
    use tidb_model::{ColumnInfo, GoShared};
    use tidb_mysql::types::GeneratedColumnFlag;
    use tidb_proto::tipb;

    use super::{
        column_label_size, column_to_proto, column_to_proto_shared, columns_to_proto,
        encode_column_label, encode_column_labels, try_encode_short_column_labels,
    };

    #[test]
    fn equal_width_column_labels_recheck_sizes_after_sizing() {
        let text = "界".repeat(43);
        let mut labels = vec![tidb_goruntime::GoString::from(text.clone()); 16];
        let (size, packed_size) = column_label_size(&labels);
        assert_eq!(packed_size, Some(size));
        let mut output = Vec::new();
        encode_column_labels(&labels, packed_size, &mut output);
        let mut expected = Vec::new();
        for _ in &labels {
            encode_column_label(&text, &mut expected);
        }
        assert_eq!(output, expected);

        labels[15] = "changed length".into();
        output.clear();
        encode_column_labels(&labels, packed_size, &mut output);
        expected.truncate(15 * (text.len() + 3));
        encode_column_label("changed length", &mut expected);
        assert_eq!(output, expected);
    }

    #[test]
    fn short_column_labels_handle_size_changes_after_sizing() {
        let labels = vec![tidb_goruntime::GoString::from("界\0"); 16];
        let mut expected = vec![1, 2, 3];
        for _ in &labels {
            encode_column_label("界\0", &mut expected);
        }
        let mut output = vec![1, 2, 3];
        let label_length = expected.len() - output.len();
        assert!(!try_encode_short_column_labels(&labels, label_length - 1, &mut output));
        assert_eq!(output, [1, 2, 3]);
        assert!(try_encode_short_column_labels(&labels, label_length + 1, &mut output));
        assert_eq!(output, expected);
    }

    #[test]
    fn streamed_short_labels_check_unknown_utf8_after_copying_known_text() {
        let text = tidb_goruntime::GoString::from("界".repeat(8));
        let mut labels = vec![text.clone(); 16];
        let mut output = Vec::new();
        let mut expected = Vec::new();
        for _ in &labels {
            encode_column_label(text.known_utf8().unwrap(), &mut expected);
        }
        assert!(try_encode_short_column_labels(&labels, expected.len(), &mut output));
        assert_eq!(output, expected);

        labels[15] = text.slice(1..);
        let previous = output.len();
        assert!(!try_encode_short_column_labels(&labels, expected.len() - 1, &mut output));
        assert_eq!(output.len(), previous);
        assert_eq!(output, expected);
    }



    fn field_type(tp: u8) -> FieldType {
        FieldType::new(FieldTypeCode::from_mysql_type(tp))
    }

    fn column(field_type: FieldType) -> ColumnInfo {
        ColumnInfo { field_type, ..ColumnInfo::default() }
    }

    #[test]
    fn shared_projection_keeps_the_label_slice_and_matches_owned_wire() {
        let field = FieldType::new(FieldTypeCode::Enum).with_elems([
            "雪".as_bytes().to_vec(),
            vec![0xff],
            b"zero\0byte".to_vec(),
        ]);
        let mut source = column(field);
        let projected = column_to_proto_shared(&source, true, false);
        source.field_type.set_elem(0, "changed");
        let expected = column_to_proto(&source, true, false).encode_to_vec();
        source.field_type.set_elems(vec![tidb_goruntime::GoString::from("replaced")]);

        let mut output = Vec::new();
        projected.encode(&mut output);
        assert_eq!(output, expected);
    }

    fn pb(tp: i32, collation: i32, column_len: i32, flag: i32) -> tipb::ColumnInfo {
        tipb::ColumnInfo {
            column_id: Some(0),
            tp: Some(tp),
            collation: Some(collation),
            column_len: Some(column_len),
            decimal: Some(0),
            flag: Some(flag),
            pk_handle: Some(false),
            array: Some(false),
            ..tipb::ColumnInfo::default()
        }
    }

    /// Go `TestColumnToProto` (`pkg/planner/core/plan_to_pb_test.go`).
    #[test]
    fn test_column_to_proto() {
        // Make sure the flag is set in tipb.ColumnInfo
        set_new_collation_enabled_for_test(false);
        let mut tp = field_type(3);
        tp.set_flag(10);
        tp.set_collation_name("utf8_bin");
        let col = column(tp);
        assert_eq!(column_to_proto(&col, false, false), pb(3, 83, 11, 10));

        let cols = vec![GoShared::new(col.clone()), GoShared::new(col.clone())];
        for v in columns_to_proto(&cols, false, false, false) {
            assert_eq!(v.flag, Some(10));
        }
        for v in columns_to_proto(&cols, true, false, false) {
            assert_eq!(v.flag, Some(10));
        }

        // Make sure the collation ID is successfully set.
        let mut tp = field_type(15);
        tp.set_flag(10);
        tp.set_collation_name("latin1_swedish_ci");
        let col1 = column(tp);
        assert_eq!(column_to_proto(&col1, false, false).collation, Some(8));

        set_new_collation_enabled_for_test(true);

        assert_eq!(column_to_proto(&col, false, false), pb(3, -83, 11, 10));
        for v in columns_to_proto(&cols, true, false, false) {
            assert_eq!(v.collation, Some(-83));
        }
        assert_eq!(column_to_proto(&col1, false, false).collation, Some(-8));

        let mut tp = field_type(0xf7);
        tp.set_flag(10);
        tp.set_elems(vec!["a".into(), "b".into()]);
        let col2 = column(tp);
        assert_eq!(column_to_proto(&col2, false, false).elems.len(), 2);

        let tp = FieldTypeBuilder::new()
            .with_code(FieldTypeCode::from_mysql_type(0xfe))
            .charset_set("utf8mb4")
            .collation_set("utf8mb4_bin")
            .flen_set(100)
            .flags_set(10)
            .array(true)
            .build();
        let col3 = column(tp);
        assert_eq!(column_to_proto(&col3, true, false), pb(0xfe, 63, 100, 10));
    }

    #[test]
    fn column_projection_preserves_go_lossy_enum_labels() {
        let field = field_type(247).with_elems([
            "café".as_bytes().to_vec(),
            vec![b'a', 0xe2, 0x82, b'b'],
            vec![],
        ]);
        let projected = column_to_proto(&column(field), false, false);
        assert_eq!(projected.elems, ["café", "a\u{fffd}\u{fffd}b", ""]);

        let mut labels = vec!["界".as_bytes().to_vec(); 32];
        labels[2].clear();
        labels[3] = b"a\0b".to_vec();
        // Consecutive labels must be validated independently even when their
        // adjacent bytes could form a single valid character.
        labels[14] = vec![0xe2, 0x82];
        labels[15] = vec![0xac];
        let field = field_type(247).with_elems(labels);
        let projected = column_to_proto(&column(field), false, false);
        assert_eq!(projected.elems.len(), 32);
        assert_eq!(&projected.elems[..4], ["界", "界", "", "a\0b"]);
        assert_eq!(
            &projected.elems[13..17],
            ["界", "\u{fffd}\u{fffd}", "\u{fffd}", "界"]
        );
    }

    /// Go `TestGeneratedColumnFlagForTiFlash`'s `ColumnToProto` /
    /// `ColumnsToProto` half (issue 59831).
    #[test]
    fn test_generated_column_flag_for_tiflash() {
        set_new_collation_enabled_for_test(false);
        let mut tp = field_type(15);
        tp.set_collation_name("utf8_bin");

        let gen_col = ColumnInfo {
            id: 1,
            generated_expr_string: "lower(c1)".to_owned(),
            generated_stored: false,
            ..column(tp.clone())
        };
        assert!(gen_col.is_virtual_generated());
        let normal_col = ColumnInfo { id: 2, ..column(tp) };

        let gen_flag = GeneratedColumnFlag as i32;

        // When isTiFlashStore=true, GeneratedColumnFlag should be set on virtual generated columns.
        assert_ne!(column_to_proto(&gen_col, false, true).flag.unwrap() & gen_flag, 0);
        assert_eq!(column_to_proto(&gen_col, false, false).flag.unwrap() & gen_flag, 0);
        assert_eq!(column_to_proto(&normal_col, false, true).flag.unwrap() & gen_flag, 0);

        // Test via ColumnsToProto with isTiFlashStore=true.
        let pcs = columns_to_proto(&[GoShared::new(gen_col), GoShared::new(normal_col)], false, false, true);
        assert_ne!(pcs[0].flag.unwrap() & gen_flag, 0);
        assert_eq!(pcs[1].flag.unwrap() & gen_flag, 0);
    }

    /// Go `TestToPB` (`pkg/util/misc_test.go`): `column_id:1 collation:-45
    /// columnLen:-1 decimal:-1`.
    #[test]
    fn test_to_pb() {
        set_new_collation_enabled_for_test(true);
        let mut column = ColumnInfo { id: 1, hidden: true, ..column(field_type(0)) };
        column.field_type.set_collation_name("utf8mb4_general_ci");
        let mut column2 = column.clone();
        column2.field_type.set_collation_name("utf8mb4_bin");

        let expect = tipb::ColumnInfo {
            column_id: Some(1),
            tp: Some(0),
            collation: Some(-45),
            column_len: Some(-1),
            decimal: Some(-1),
            flag: Some(0),
            pk_handle: Some(false),
            array: Some(false),
            ..tipb::ColumnInfo::default()
        };
        assert_eq!(column_to_proto(&column, false, false), expect);
        assert_eq!(columns_to_proto(&[GoShared::new(column), GoShared::new(column2)], false, false, false)[0], expect);
    }
}

/// Go `RetryInterval`: the retry interval in milliseconds.
pub const RETRY_INTERVAL: u64 = 500;

/// Go `RunWithRetry`: runs `f` up to `retry_cnt` times while it fails with
/// an error it marks retryable, sleeping `backoff * i` milliseconds after the
/// `i`-th failure.
///
/// # Errors
/// `f`'s last error.
pub fn run_with_retry<E: std::fmt::Display>(
    retry_cnt: usize,
    backoff: u64,
    mut f: impl FnMut() -> (bool, Result<(), E>),
) -> Result<(), E> {
    let mut result = Ok(());
    for i in 1..=retry_cnt {
        let (retry_able, attempt) = f();
        let err = match attempt {
            Ok(()) => return Ok(()),
            Err(err) if !retry_able => return Err(err),
            Err(err) => err,
        };
        tidb_metrics::ddl::RETRYABLE_ERROR_COUNT.with_label_values(&[&err.to_string()]).inc();
        result = Err(err);
        std::thread::sleep(std::time::Duration::from_millis(backoff * i as u64));
    }
    result
}

#[cfg(test)]
mod run_with_retry_tests {
    use super::run_with_retry;

    /// Go `TestRunWithRetry`.
    #[test]
    fn test_run_with_retry() {
        // success
        let mut cnt = 0;
        let err = run_with_retry(3, 1, || {
            cnt += 1;
            if cnt < 2 {
                return (true, Err("err"));
            }
            (true, Ok(()))
        });
        assert_eq!(err, Ok(()));
        assert_eq!(cnt, 2);

        // retry exceeds
        let mut cnt = 0;
        let err = run_with_retry(3, 1, || {
            cnt += 1;
            if cnt < 4 {
                return (true, Err("err"));
            }
            (true, Ok(()))
        });
        assert!(err.is_err());
        assert_eq!(cnt, 3);

        // failed result
        let mut cnt = 0;
        let err = run_with_retry(3, 1, || {
            cnt += 1;
            if cnt < 2 {
                return (false, Err("err"));
            }
            (true, Ok(()))
        });
        assert!(err.is_err());
        assert_eq!(cnt, 1);
    }
}
