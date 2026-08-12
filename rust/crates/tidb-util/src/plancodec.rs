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

//! Encoding and display of TiDB's textual and protobuf execution plans.

use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::num::ParseIntError;
use std::sync::LazyLock;

use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use prost::Message;
use snap::raw::{Decoder as SnappyDecoder, Encoder as SnappyEncoder};
use tidb_proto::tipb::access_object::AccessObject as AccessObjectKind;
use tidb_proto::tipb::{
    AccessObject, DynamicPartitionAccessObject, ExplainData, ExplainOperator, OperatorLabel,
    StoreType, TaskType,
};

use crate::memory::format_bytes;
use crate::texttree::{
    indent_4_child, pretty_identifier, TREE_BODY, TREE_LAST_NODE, TREE_MIDDLE_NODE,
    TREE_NODE_IDENTIFIER,
};

/// Encoded sentinel used when a textual plan exceeded TiDB's size limit.
pub const PLAN_DISCARDED_ENCODED: &str = "[discard]";
/// Human-readable spelling of a discarded textual or binary plan.
pub const PLAN_DISCARDED_DECODED: &str = "(plan discarded because too long)";

/// Protobuf/Snappy/base64 form of a discarded binary plan.
pub static BINARY_PLAN_DISCARDED_ENCODED: LazyLock<String> = LazyLock::new(|| {
    let data = ExplainData {
        discarded_due_to_too_long: true,
        ..Default::default()
    };
    compress(&data.encode_to_vec())
});

/// A malformed encoded plan.
#[derive(Debug)]
pub enum PlanCodecError {
    /// Invalid base64 text.
    Base64(base64::DecodeError),
    /// Invalid Snappy block data.
    Snappy(snap::Error),
    /// Invalid ExplainData protobuf data.
    Protobuf(prost::DecodeError),
    /// A textual plan integer was malformed.
    Integer(ParseIntError),
    /// A textual plan violated the tree encoding contract.
    InvalidPlan(String),
}

impl fmt::Display for PlanCodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Base64(base64::DecodeError::InvalidByte(offset, _))
            | Self::Base64(base64::DecodeError::InvalidLastSymbol(offset, _)) => {
                write!(f, "illegal base64 data at input byte {offset}")
            }
            Self::Base64(base64::DecodeError::InvalidLength(length)) => {
                write!(f, "illegal base64 data at input byte {length}")
            }
            Self::Base64(base64::DecodeError::InvalidPadding) => {
                f.write_str("illegal base64 data at input byte 0")
            }
            Self::Snappy(_) => f.write_str("snappy: corrupt input"),
            Self::Protobuf(error) => write!(f, "{}", format_protobuf_error(error)),
            Self::Integer(error) => write!(f, "invalid integer in encoded plan: {error}"),
            Self::InvalidPlan(error) => f.write_str(error),
        }
    }
}

impl Error for PlanCodecError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Base64(error) => Some(error),
            Self::Snappy(error) => Some(error),
            Self::Protobuf(error) => Some(error),
            Self::Integer(error) => Some(error),
            Self::InvalidPlan(_) => None,
        }
    }
}

impl From<base64::DecodeError> for PlanCodecError {
    fn from(error: base64::DecodeError) -> Self {
        Self::Base64(error)
    }
}

impl From<snap::Error> for PlanCodecError {
    fn from(error: snap::Error) -> Self {
        Self::Snappy(error)
    }
}

impl From<prost::DecodeError> for PlanCodecError {
    fn from(error: prost::DecodeError) -> Self {
        Self::Protobuf(error)
    }
}

impl From<ParseIntError> for PlanCodecError {
    fn from(error: ParseIntError) -> Self {
        Self::Integer(error)
    }
}

/// Result type returned by plan decoders.
pub type Result<T> = std::result::Result<T, PlanCodecError>;

macro_rules! plan_types {
    ($(($constant:ident, $name:literal)),+ $(,)?) => {
        $(
            #[doc = concat!("Stable encoded plan type `", $name, "`.")]
            pub const $constant: &str = $name;
        )+

        const PLAN_TYPES: [&str; 63] = [$($constant),+];
    };
}

plan_types!(
    (TYPE_SELECTION, "Selection"),
    (TYPE_SET, "Set"),
    (TYPE_PROJECTION, "Projection"),
    (TYPE_AGGREGATION, "Aggregation"),
    (TYPE_STREAM_AGG, "StreamAgg"),
    (TYPE_HASH_AGG, "HashAgg"),
    (TYPE_SHOW, "Show"),
    (TYPE_JOIN, "Join"),
    (TYPE_UNION, "Union"),
    (TYPE_TABLE_SCAN, "TableScan"),
    (TYPE_MEM_TABLE_SCAN, "MemTableScan"),
    (TYPE_UNION_SCAN, "UnionScan"),
    (TYPE_INDEX_SCAN, "IndexScan"),
    (TYPE_SORT, "Sort"),
    (TYPE_TOP_N, "TopN"),
    (TYPE_LIMIT, "Limit"),
    (TYPE_HASH_JOIN, "HashJoin"),
    (TYPE_MERGE_JOIN, "MergeJoin"),
    (TYPE_INDEX_JOIN, "IndexJoin"),
    (TYPE_INDEX_MERGE_JOIN, "IndexMergeJoin"),
    (TYPE_INDEX_HASH_JOIN, "IndexHashJoin"),
    (TYPE_APPLY, "Apply"),
    (TYPE_MAX_ONE_ROW, "MaxOneRow"),
    (TYPE_EXISTS, "Exists"),
    (TYPE_TABLE_DUAL, "TableDual"),
    (TYPE_SELECT_LOCK, "SelectLock"),
    (TYPE_INSERT, "Insert"),
    (TYPE_UPDATE, "Update"),
    (TYPE_DELETE, "Delete"),
    (TYPE_INDEX_LOOK_UP, "IndexLookUp"),
    (TYPE_TABLE_READER, "TableReader"),
    (TYPE_INDEX_READER, "IndexReader"),
    (TYPE_WINDOW, "Window"),
    (TYPE_TIKV_SINGLE_GATHER, "TiKVSingleGather"),
    (TYPE_INDEX_MERGE, "IndexMerge"),
    (TYPE_POINT_GET, "Point_Get"),
    (TYPE_SHOW_DDL_JOBS, "ShowDDLJobs"),
    (TYPE_BATCH_POINT_GET, "Batch_Point_Get"),
    (TYPE_CLUSTER_MEM_TABLE_READER, "ClusterMemTableReader"),
    (TYPE_DATA_SOURCE, "DataSource"),
    (TYPE_LOAD_DATA, "LoadData"),
    (TYPE_TABLE_SAMPLE, "TableSample"),
    (TYPE_TABLE_FULL_SCAN, "TableFullScan"),
    (TYPE_TABLE_RANGE_SCAN, "TableRangeScan"),
    (TYPE_TABLE_ROW_ID_SCAN, "TableRowIDScan"),
    (TYPE_INDEX_FULL_SCAN, "IndexFullScan"),
    (TYPE_INDEX_RANGE_SCAN, "IndexRangeScan"),
    (TYPE_EXCHANGE_RECEIVER, "ExchangeReceiver"),
    (TYPE_EXCHANGE_SENDER, "ExchangeSender"),
    (TYPE_CTE_FULL_SCAN, "CTEFullScan"),
    (TYPE_CTE, "CTE"),
    (TYPE_CTE_TABLE, "CTETable"),
    (TYPE_PARTITION_UNION, "PartitionUnion"),
    (TYPE_SHUFFLE, "Shuffle"),
    (TYPE_SHUFFLE_RECEIVER, "ShuffleReceiver"),
    (TYPE_FOREIGN_KEY_CHECK, "Foreign_Key_Check"),
    (TYPE_FOREIGN_KEY_CASCADE, "Foreign_Key_Cascade"),
    (TYPE_EXPAND, "Expand"),
    (TYPE_IMPORT_INTO, "ImportInto"),
    (TYPE_SCALAR_SUBQUERY, "ScalarSubQuery"),
    (TYPE_LOCAL_INDEX_LOOK_UP, "LocalIndexLookUp"),
    (TYPE_PHYSICAL_CTE_SINK, "PhysicalCTESink"),
    (TYPE_PHYSICAL_CTE_SOURCE, "PhysicalCTESource"),
);

/// A plan type which intentionally has no stable physical ID.
pub const TYPE_SEQUENCE: &str = "Sequence";
/// Stable physical ID of [`TYPE_SCALAR_SUBQUERY`].
pub const TYPE_SCALAR_SUBQUERY_ID: i32 = 60;

/// Converts a plan type string to its stable physical ID, or zero if unknown.
#[must_use]
pub fn type_string_to_physical_id(plan_type: &str) -> i32 {
    PLAN_TYPES
        .iter()
        .position(|candidate| *candidate == plan_type)
        .map_or(0, |index| {
            i32::try_from(index + 1).expect("63 plan types fit i32")
        })
}

/// Converts a physical ID to its plan type string.
#[must_use]
pub fn physical_id_to_type_string(id: i32) -> String {
    usize::try_from(id - 1)
        .ok()
        .and_then(|index| PLAN_TYPES.get(index))
        .map_or_else(|| format!("UnknownPlanID{id}"), |value| (*value).to_owned())
}

/// Storage engine encoded into textual plan task fields.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum PlanStoreType {
    /// TiKV coprocessor.
    TiKv = 0,
    /// TiFlash coprocessor.
    TiFlash = 1,
    /// A TiDB instance serving cluster memory tables.
    TiDb = 2,
    /// Unknown engine.
    Unspecified = 255,
}

impl PlanStoreType {
    const fn name_for_code(code: i64) -> &'static str {
        match code {
            0 => "tikv",
            1 => "tiflash",
            2 => "tidb",
            _ => "unspecified",
        }
    }
}

/// Encodes root/cop task ownership.
#[must_use]
pub fn encode_task_type(is_root: bool, store_type: PlanStoreType) -> String {
    if is_root {
        "0".to_owned()
    } else {
        format!("1_{}", store_type as u8)
    }
}

/// Encodes task ownership for normalized plans, omitting TiKV's default engine.
#[must_use]
pub fn encode_task_type_for_normalize(is_root: bool, store_type: PlanStoreType) -> String {
    if is_root {
        "0".to_owned()
    } else if store_type == PlanStoreType::TiKv {
        "1".to_owned()
    } else {
        format!("1_{}", store_type as u8)
    }
}

/// Snappy-compresses and standard-base64 encodes bytes.
#[must_use]
pub fn compress(input: &[u8]) -> String {
    let compressed = SnappyEncoder::new()
        .compress_vec(input)
        .expect("a byte slice always fits a Snappy block");
    STANDARD.encode(compressed)
}

/// Standard-base64 decodes and Snappy-decompresses a block.
pub fn decompress(input: &[u8]) -> Result<Vec<u8>> {
    let compressed = STANDARD.decode(input)?;
    Ok(SnappyDecoder::new().decompress_vec(&compressed)?)
}

/// Decodes a compressed textual plan into its aligned tree display.
///
/// The result is bytes because Go strings, including plan fields, are not
/// required to be UTF-8.
pub fn decode_plan(plan: impl AsRef<[u8]>) -> Result<Vec<u8>> {
    let plan = plan.as_ref();
    if plan.is_empty() {
        return Ok(Vec::new());
    }
    let raw = match decompress(plan) {
        Ok(raw) => raw,
        Err(_) if plan == PLAN_DISCARDED_ENCODED.as_bytes() => {
            return Ok(PLAN_DISCARDED_DECODED.as_bytes().to_vec());
        }
        Err(error) => return Err(error),
    };
    build_plan_tree(&raw, true)
}

/// Decodes an uncompressed normalized-plan stream into its aligned tree display.
pub fn decode_normalized_plan(plan: impl AsRef<[u8]>) -> Result<Vec<u8>> {
    let plan = plan.as_ref();
    if plan.is_empty() {
        return Ok(Vec::new());
    }
    build_plan_tree(plan, false)
}

#[derive(Debug)]
struct PlanInfo {
    depth: usize,
    fields: Vec<Vec<u8>>,
}

fn build_plan_tree(plan: &[u8], add_header: bool) -> Result<Vec<u8>> {
    let mut infos = Vec::new();
    for node in plan.split(|byte| *byte == b'\n') {
        if let Some(info) = decode_plan_info(node)? {
            infos.push(info);
        }
    }
    if add_header && !infos.is_empty() {
        let mut fields = vec![
            "id",
            "task",
            "estRows",
            "operator info",
            "actRows",
            "execution info",
            "memory",
            "disk",
        ];
        fields.truncate(fields.len().min(infos[0].fields.len()));
        infos.insert(
            0,
            PlanInfo {
                depth: 0,
                fields: fields
                    .into_iter()
                    .map(|field| field.as_bytes().to_vec())
                    .collect(),
            },
        );
    }

    let depths: Vec<usize> = infos.iter().map(|info| info.depth).collect();
    let mut indents = Vec::with_capacity(depths.len());
    for depth in &depths {
        let len = depth.checked_mul(2).ok_or_else(|| {
            PlanCodecError::InvalidPlan("encoded plan depth is too large".to_owned())
        })?;
        let mut indent = vec!['\0'; len];
        if len > 0 {
            indent[..len - 2].fill(' ');
            indent[len - 2] = TREE_LAST_NODE;
            indent[len - 1] = TREE_NODE_IDENTIFIER;
        }
        indents.push(indent);
    }

    let mut parent_cache = HashMap::new();
    for child in 1..depths.len() {
        parent_cache.insert(depths[child], child);
        let parent = find_parent_index(&depths, child, &mut parent_cache);
        fill_indent(&mut indents, &depths, parent, child)?;
    }

    align_fields(&mut infos, &indents);
    let mut output = Vec::new();
    for (row, info) in infos.iter().enumerate() {
        if row > 0 {
            output.push(b'\n');
        }
        output.push(b'\t');
        for character in &indents[row] {
            let mut encoded = [0; 4];
            output.extend_from_slice(character.encode_utf8(&mut encoded).as_bytes());
        }
        for (field, value) in info.fields.iter().enumerate() {
            if field > 0 {
                output.push(b'\t');
            }
            output.extend_from_slice(value);
        }
    }
    Ok(output)
}

fn find_parent_index(depths: &[usize], child: usize, cache: &mut HashMap<usize, usize>) -> usize {
    let Some(parent_depth) = depths[child].checked_sub(1) else {
        return 0;
    };
    if let Some(parent) = cache.get(&parent_depth) {
        return *parent;
    }
    for index in (1..child).rev() {
        if depths[index] == parent_depth {
            cache.insert(parent_depth, index);
            return index;
        }
    }
    0
}

fn fill_indent(
    indents: &mut [Vec<char>],
    depths: &[usize],
    parent: usize,
    child: usize,
) -> Result<()> {
    let depth = depths[child];
    if depth == 0 {
        return Ok(());
    }
    let column = depth * 2 - 2;
    for index in (parent + 1..child).rev() {
        let value = indents[index].get_mut(column).ok_or_else(|| {
            PlanCodecError::InvalidPlan(format!("encoded plan depth jumps before row {child}"))
        })?;
        if *value == TREE_LAST_NODE {
            *value = TREE_MIDDLE_NODE;
            break;
        }
        *value = TREE_BODY;
    }
    Ok(())
}

fn align_fields(infos: &mut [PlanInfo], indents: &[Vec<char>]) {
    let Some(max_fields) = infos.iter().map(|info| info.fields.len()).max() else {
        return;
    };
    for info in infos.iter_mut() {
        info.fields.resize(max_fields, Vec::new());
    }
    for column in 0..max_fields.saturating_sub(1) {
        let max_len = infos
            .iter()
            .enumerate()
            .map(|(row, info)| field_len(row, column, info, indents))
            .max()
            .unwrap_or(0);
        for (row, info) in infos.iter_mut().enumerate() {
            let fill = max_len - field_len(row, column, info, indents);
            info.fields[column].extend(std::iter::repeat_n(b' ', fill));
        }
    }
}

fn field_len(row: usize, column: usize, info: &PlanInfo, indents: &[Vec<char>]) -> usize {
    if column == 0 {
        info.fields[0].len() + indents[row].len()
    } else {
        info.fields[column].len()
    }
}

fn decode_plan_info(value: &[u8]) -> Result<Option<PlanInfo>> {
    let values: Vec<&[u8]> = value.split(|byte| *byte == b'\t').collect();
    if values.len() < 2 {
        return Ok(None);
    }
    let depth_text = std::str::from_utf8(values[0]).map_err(|error| {
        PlanCodecError::InvalidPlan(format!("invalid encoded plan depth: {error}"))
    })?;
    let depth = depth_text.parse::<isize>()?;
    let depth = usize::try_from(depth).map_err(|_| {
        PlanCodecError::InvalidPlan(format!("negative encoded plan depth: {depth}"))
    })?;
    let ids: Vec<&[u8]> = values[1].split(|byte| *byte == b'_').collect();
    if !matches!(ids.len(), 1 | 2) {
        return Err(PlanCodecError::InvalidPlan(format!(
            "invalid encoded plan id: {}",
            String::from_utf8_lossy(values[1])
        )));
    }
    let id = std::str::from_utf8(ids[0])
        .map_err(|error| PlanCodecError::InvalidPlan(format!("invalid encoded plan id: {error}")))?
        .parse::<i32>()?;
    let mut fields = vec![physical_id_to_type_string(id).into_bytes()];
    if ids.len() == 2 {
        fields[0].push(b'_');
        fields[0].extend_from_slice(ids[1]);
    }
    if let Some(task) = values.get(2) {
        fields.push(decode_task_type_bytes(task)?);
    }
    fields.extend(values.iter().skip(3).map(|field| field.to_vec()));
    Ok(Some(PlanInfo { depth, fields }))
}

fn decode_task_type_bytes(value: &[u8]) -> Result<Vec<u8>> {
    let segments: Vec<&[u8]> = value.split(|byte| *byte == b'_').collect();
    if segments.first() == Some(&b"0".as_slice()) {
        return Ok(b"root".to_vec());
    }
    if segments.len() == 1 {
        return Ok(b"cop".to_vec());
    }
    let store = std::str::from_utf8(segments[1])
        .map_err(|error| PlanCodecError::InvalidPlan(format!("invalid task type: {error}")))?
        .parse::<i64>()?;
    Ok(format!("cop[{}]", PlanStoreType::name_for_code(store)).into_bytes())
}

/// Appends one textual plan node to an encoded plan stream.
#[allow(clippy::too_many_arguments)]
pub fn encode_plan_node(
    depth: usize,
    id: &str,
    plan_type: &str,
    row_count: f64,
    task_type: &str,
    explain_info: &str,
    act_rows: &str,
    analyze_info: &str,
    memory_info: &str,
    disk_info: &str,
    output: &mut String,
) {
    output.push_str(&depth.to_string());
    output.push('\t');
    output.push_str(&type_string_to_physical_id(plan_type).to_string());
    output.push('_');
    output.push_str(id);
    output.push('\t');
    output.push_str(task_type);
    output.push('\t');
    output.push_str(&format_plan_float(
        row_count,
        row_count.round() == row_count,
    ));
    output.push('\t');
    output.push_str(&escape_string(explain_info));
    if !act_rows.is_empty()
        || !analyze_info.is_empty()
        || !memory_info.is_empty()
        || !disk_info.is_empty()
    {
        for value in [act_rows, analyze_info, memory_info, disk_info] {
            output.push('\t');
            output.push_str(value);
        }
    }
    output.push('\n');
}

/// Appends one node to an uncompressed normalized plan stream.
pub fn normalize_plan_node(
    depth: usize,
    plan_type: &str,
    task_type: &str,
    explain_info: &str,
    output: &mut String,
) {
    output.push_str(&depth.to_string());
    output.push('\t');
    output.push_str(&type_string_to_physical_id(plan_type).to_string());
    output.push('\t');
    output.push_str(task_type);
    output.push('\t');
    output.push_str(explain_info);
    output.push('\n');
}

fn escape_string(value: &str) -> String {
    value.replace('\t', "\\t").replace('\n', "\\n")
}

fn format_plan_float(value: f64, integral: bool) -> String {
    if value.is_nan() {
        return "NaN".to_owned();
    }
    if value == f64::INFINITY {
        return "+Inf".to_owned();
    }
    if value == f64::NEG_INFINITY {
        return "-Inf".to_owned();
    }
    if integral {
        format!("{value:.0}")
    } else {
        format!("{value:.2}")
    }
}

const NO_RUNTIME_TITLES: [&str; 6] = [
    "id",
    "estRows",
    "estCost",
    "task",
    "access object",
    "operator info",
];
const FULL_TITLES: [&str; 10] = [
    "id",
    "estRows",
    "estCost",
    "actRows",
    "task",
    "access object",
    "execution info",
    "operator info",
    "memory",
    "disk",
];

/// Decodes a protobuf binary plan into EXPLAIN ANALYZE's tabular display.
pub fn decode_binary_plan(plan: impl AsRef<[u8]>) -> Result<String> {
    let data = decode_explain_data(plan.as_ref())?;
    if data.discarded_due_to_too_long {
        return Ok(PLAN_DISCARDED_DECODED.to_owned());
    }
    let mut rows = Vec::new();
    if let Some(main) = data.main.as_ref() {
        decode_binary_operator(main, "", true, data.with_runtime_stats, false, &mut rows);
    }
    for operator in &data.ctes {
        decode_binary_operator(
            operator,
            "",
            true,
            data.with_runtime_stats,
            false,
            &mut rows,
        );
    }
    for operator in &data.subqueries {
        decode_binary_operator(
            operator,
            "",
            true,
            data.with_runtime_stats,
            false,
            &mut rows,
        );
    }
    if rows.is_empty() {
        return Ok(String::new());
    }

    let titles: &[&str] = if data.with_runtime_stats {
        &FULL_TITLES
    } else {
        &NO_RUNTIME_TITLES
    };
    let widths = calculate_max_field_lens(&rows, titles);
    let mut output = String::from("\n");
    write_binary_row(&mut output, titles.iter().copied(), &widths);
    for row in &rows {
        write_binary_row(&mut output, row.iter().map(String::as_str), &widths);
    }
    Ok(output)
}

/// Decodes rows selected by an EXPLAIN FOR CONNECTION output format.
pub fn decode_binary_plan_for_connection(
    plan: impl AsRef<[u8]>,
    format: &str,
    for_top_sql: bool,
) -> Result<Vec<Vec<String>>> {
    let data = decode_explain_data(plan.as_ref())?;
    if data.discarded_due_to_too_long {
        return Ok(Vec::new());
    }
    let brief = format == "brief";
    let mut rows = Vec::new();
    if let Some(main) = data.main.as_ref() {
        decode_binary_operator(main, "", true, data.with_runtime_stats, brief, &mut rows);
    }
    for operator in &data.ctes {
        decode_binary_operator(
            operator,
            "",
            true,
            data.with_runtime_stats,
            brief,
            &mut rows,
        );
    }
    if rows.is_empty() {
        return Ok(Vec::new());
    }

    let indices: &[usize] = match (data.with_runtime_stats && !for_top_sql, format) {
        (true, "brief" | "row") => &[0, 1, 3, 4, 5, 6, 7, 8, 9],
        (true, "plan_tree") => &[0, 2, 3, 4, 5, 6, 7, 8],
        (true, "verbose") => &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
        (false, "brief" | "row") => &[0, 1, 3, 4, 5],
        (false, "plan_tree") => &[0, 2, 3, 4, 5],
        (false, "verbose") => &[0, 1, 2, 3, 4, 5],
        _ => &[],
    };
    Ok(rows
        .into_iter()
        .map(|row| indices.iter().map(|index| row[*index].clone()).collect())
        .collect())
}

fn decode_explain_data(plan: &[u8]) -> Result<ExplainData> {
    let bytes = decompress(plan)?;
    Ok(ExplainData::decode(bytes.as_slice())?)
}

fn format_protobuf_error(error: &prost::DecodeError) -> String {
    let message = error.to_string();
    let detail = message
        .strip_prefix("failed to decode Protobuf message: ")
        .unwrap_or(&message);
    if let Some(value) = detail.strip_prefix("invalid wire type value: ") {
        return format!("proto: illegal wireType {value}");
    }
    if matches!(detail, "buffer underflow" | "invalid varint") {
        return "unexpected EOF".to_owned();
    }
    format!("proto: {detail}")
}

fn calculate_max_field_lens(rows: &[Vec<String>], titles: &[&str]) -> Vec<usize> {
    let mut widths = vec![0; rows[0].len()];
    for row in rows {
        for (column, value) in row.iter().enumerate() {
            widths[column] = widths[column].max(value.chars().count());
        }
    }
    for (column, title) in titles.iter().enumerate() {
        widths[column] = widths[column].max(title.chars().count());
    }
    widths
}

fn write_binary_row<'a>(
    output: &mut String,
    fields: impl Iterator<Item = &'a str>,
    widths: &[usize],
) {
    for (column, value) in fields.enumerate() {
        output.push_str("| ");
        output.push_str(value);
        output.extend(std::iter::repeat_n(
            ' ',
            widths[column] - value.chars().count(),
        ));
        output.push(' ');
        if column + 1 == widths.len() {
            output.push_str("|\n");
        }
    }
}

fn decode_binary_operator(
    operator: &ExplainOperator,
    indent: &str,
    last_child: bool,
    runtime: bool,
    brief: bool,
    output: &mut Vec<Vec<String>>,
) {
    let labels = print_driver_side(&operator.labels);
    let name = if brief {
        &operator.brief_name
    } else {
        &operator.name
    };
    let id = pretty_identifier(&format!("{name}{labels}"), indent, last_child);
    let mut row = vec![
        id,
        format_plan_float(operator.est_rows, false),
        format_plan_float(operator.cost, false),
    ];
    if runtime {
        row.push(operator.act_rows.to_string());
    }
    let mut task = TaskType::try_from(operator.task_type).map_or_else(
        |_| operator.task_type.to_string(),
        |task_type| task_type.as_str_name().to_owned(),
    );
    if operator.task_type != TaskType::Unknown as i32 && operator.task_type != TaskType::Root as i32
    {
        let store = StoreType::try_from(operator.store_type).map_or_else(
            |_| operator.store_type.to_string(),
            |store_type| store_type.as_str_name().to_owned(),
        );
        task.push('[');
        task.push_str(&store);
        task.push(']');
    }
    row.push(task);
    row.push(print_access_object(&operator.access_objects));
    if runtime {
        let mut execution = operator.root_basic_exec_info.clone();
        for value in [
            operator.root_group_exec_info.join(", "),
            operator.cop_exec_info.clone(),
        ] {
            if value.is_empty() {
                continue;
            }
            if !execution.is_empty() {
                execution.push_str(", ");
            }
            execution.push_str(&value);
        }
        row.push(execution);
    }
    row.push(if brief && !operator.brief_operator_info.is_empty() {
        operator.brief_operator_info.clone()
    } else {
        operator.operator_info.clone()
    });
    if runtime {
        row.push(if operator.memory_bytes < 0 {
            "N/A".to_owned()
        } else {
            format_bytes(operator.memory_bytes)
        });
        row.push(if operator.disk_bytes < 0 {
            "N/A".to_owned()
        } else {
            format_bytes(operator.disk_bytes)
        });
    }
    output.push(row);

    let mut children: Vec<&ExplainOperator> = operator.children.iter().collect();
    if children.len() == 2
        && children[0].labels.first() == Some(&(OperatorLabel::ProbeSide as i32))
        && children[1].labels.first() == Some(&(OperatorLabel::BuildSide as i32))
    {
        children.swap(0, 1);
    }
    let child_indent = indent_4_child(indent, last_child);
    let last = children.len().saturating_sub(1);
    for (index, child) in children.into_iter().enumerate() {
        decode_binary_operator(child, &child_indent, index == last, runtime, brief, output);
    }
}

fn print_driver_side(labels: &[i32]) -> String {
    let mut output = String::new();
    for label in labels {
        match OperatorLabel::try_from(*label) {
            Ok(OperatorLabel::Empty) => {}
            Ok(OperatorLabel::BuildSide) => output.push_str("(Build)"),
            Ok(OperatorLabel::ProbeSide) => output.push_str("(Probe)"),
            Ok(OperatorLabel::SeedPart) => output.push_str("(Seed Part)"),
            Ok(OperatorLabel::RecursivePart) => output.push_str("(Recursive Part)"),
            Err(_) => {}
        }
    }
    output
}

fn print_dynamic_partition_object(object: &DynamicPartitionAccessObject) -> String {
    if object.all_partitions {
        "partition:all".to_owned()
    } else if object.partitions.is_empty() {
        "partition:dual".to_owned()
    } else {
        format!("partition:{}", object.partitions.join(","))
    }
}

fn print_access_object(objects: &[AccessObject]) -> String {
    let mut values = Vec::new();
    for object in objects {
        match object.access_object.as_ref() {
            Some(AccessObjectKind::DynamicPartitionObjects(dynamic)) => {
                if dynamic.objects.is_empty() {
                    return String::new();
                }
                if dynamic.objects.len() == 1 {
                    return print_dynamic_partition_object(&dynamic.objects[0]);
                }
                let value = dynamic
                    .objects
                    .iter()
                    .map(|access| {
                        format!(
                            "{} of {}",
                            print_dynamic_partition_object(access),
                            access.table
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                values.push(value);
            }
            Some(AccessObjectKind::ScanObject(scan)) => {
                let mut value = String::new();
                if !scan.table.is_empty() {
                    value.push_str("table:");
                    value.push_str(&scan.table);
                }
                if !scan.partitions.is_empty() {
                    value.push_str(", partition:");
                    value.push_str(&scan.partitions.join(","));
                }
                for index in &scan.indexes {
                    if index.is_clustered_index {
                        value.push_str(", clustered index:");
                    } else {
                        value.push_str(", index:");
                    }
                    value.push_str(&index.name);
                    value.push('(');
                    value.push_str(&index.cols.join(", "));
                    value.push(')');
                }
                values.push(value);
            }
            Some(AccessObjectKind::OtherObject(other)) => values.push(other.clone()),
            None => {}
        }
    }
    values.concat()
}

#[cfg(test)]
mod tests {
    use tidb_proto::tipb::access_object::AccessObject as AccessObjectKind;
    use tidb_proto::tipb::{
        AccessObject, DynamicPartitionAccessObject, DynamicPartitionAccessObjects, ExplainData,
        ExplainOperator, IndexAccess, OperatorLabel, ScanAccessObject, StoreType, TaskType,
    };

    use super::*;

    #[test]
    fn stable_plan_ids_round_trip() {
        assert_eq!(PLAN_TYPES.len(), 63);
        for id in 1..=63 {
            let plan_type = physical_id_to_type_string(id);
            assert_eq!(type_string_to_physical_id(&plan_type), id);
        }
        assert_eq!(type_string_to_physical_id("Sequence"), 0);
        assert_eq!(physical_id_to_type_string(64), "UnknownPlanID64");
    }

    #[test]
    fn task_types_match_upstream_vectors() {
        let cases = [
            (true, PlanStoreType::Unspecified, "0", "root"),
            (false, PlanStoreType::TiKv, "1_0", "cop[tikv]"),
            (false, PlanStoreType::TiFlash, "1_1", "cop[tiflash]"),
            (false, PlanStoreType::TiDb, "1_2", "cop[tidb]"),
        ];
        for (root, store, encoded, decoded) in cases {
            assert_eq!(encode_task_type(root, store), encoded);
            assert_eq!(
                decode_task_type_bytes(encoded.as_bytes()).unwrap(),
                decoded.as_bytes()
            );
        }
        assert_eq!(decode_task_type_bytes(b"1").unwrap(), b"cop");
        assert!(decode_task_type_bytes(b"1_x").is_err());
        assert_eq!(
            decode_task_type_bytes(b"1_255").unwrap(),
            b"cop[unspecified]"
        );
        assert_eq!(
            encode_task_type_for_normalize(false, PlanStoreType::TiKv),
            "1"
        );
        assert_eq!(
            encode_task_type_for_normalize(false, PlanStoreType::TiFlash),
            "1_1"
        );
    }

    #[test]
    fn textual_plan_encodes_decodes_and_discards() {
        let mut raw = String::new();
        encode_plan_node(
            0,
            "1",
            TYPE_HASH_JOIN,
            3.0,
            &encode_task_type(true, PlanStoreType::Unspecified),
            "equal:[eq(a\tb, c\nd)]",
            "2",
            "time:1ms",
            "1 KB",
            "N/A",
            &mut raw,
        );
        encode_plan_node(
            1,
            "2",
            TYPE_TABLE_SCAN,
            1.25,
            &encode_task_type(false, PlanStoreType::TiKv),
            "table:t",
            "",
            "",
            "",
            "",
            &mut raw,
        );
        let encoded = compress(raw.as_bytes());
        let decoded = String::from_utf8(decode_plan(&encoded).unwrap()).unwrap();
        assert!(decoded.starts_with("\tid"));
        assert!(decoded.contains("HashJoin_1"));
        assert!(decoded.contains("└─TableScan_2"));
        assert!(decoded.contains("equal:[eq(a\\tb, c\\nd)]"));
        assert!(decoded.contains("cop[tikv]"));
        assert_eq!(
            decode_plan(PLAN_DISCARDED_ENCODED).unwrap(),
            PLAN_DISCARDED_DECODED.as_bytes()
        );
        assert_eq!(decode_plan("").unwrap(), b"");
        assert!(decode_plan("not base64").is_err());
    }

    #[test]
    fn textual_plan_preserves_non_utf8_go_string_bytes() {
        let encoded = compress(b"0\t1\t0\t1\t\xff\n");
        assert_eq!(
            decode_plan(encoded).unwrap(),
            b"\tid       \ttask\testRows\toperator info\n\tSelection\troot\t1      \t\xff"
        );
        assert_eq!(
            decode_normalized_plan(b"0\t1\t0\t\xff\n").unwrap(),
            b"\tSelection\troot\t\xff"
        );
    }

    #[test]
    fn normalized_plan_has_no_header() {
        let mut raw = String::new();
        normalize_plan_node(0, TYPE_SELECTION, "0", "eq(a, 1)", &mut raw);
        normalize_plan_node(1, TYPE_TABLE_SCAN, "1", "table:t", &mut raw);
        let decoded = String::from_utf8(decode_normalized_plan(&raw).unwrap()).unwrap();
        assert!(!decoded.contains("\tid\t"));
        assert!(decoded.starts_with("\tSelection"));
        assert!(decoded.contains("└─TableScan"));
    }

    #[test]
    fn compression_is_raw_snappy_plus_standard_base64() {
        let input = b"plan\0bytes";
        let encoded = compress(input);
        assert_eq!(decompress(encoded.as_bytes()).unwrap(), input);
        assert!(decompress(b"***").is_err());
    }

    fn sample_binary_plan(runtime: bool) -> ExplainData {
        let scan = ExplainOperator {
            name: "TableScan_2".to_owned(),
            brief_name: "TableScan".to_owned(),
            labels: vec![OperatorLabel::BuildSide as i32],
            est_rows: 1.0,
            cost: 2.0,
            act_rows: 1,
            task_type: TaskType::Cop as i32,
            store_type: StoreType::Tikv as i32,
            access_objects: vec![AccessObject {
                access_object: Some(AccessObjectKind::ScanObject(ScanAccessObject {
                    table: "t".to_owned(),
                    partitions: vec!["p0".to_owned()],
                    indexes: vec![IndexAccess {
                        name: "idx".to_owned(),
                        cols: vec!["a".to_owned(), "b".to_owned()],
                        ..Default::default()
                    }],
                    ..Default::default()
                })),
            }],
            operator_info: "keep order:false".to_owned(),
            brief_operator_info: "table:t".to_owned(),
            root_basic_exec_info: "time:1ms".to_owned(),
            root_group_exec_info: vec!["loops:1".to_owned()],
            cop_exec_info: "cop_task:num:1".to_owned(),
            memory_bytes: 1024,
            disk_bytes: -1,
            ..Default::default()
        };
        let probe = ExplainOperator {
            name: "Selection_3".to_owned(),
            brief_name: "Selection".to_owned(),
            labels: vec![OperatorLabel::ProbeSide as i32],
            est_rows: 2.0,
            cost: 3.0,
            task_type: TaskType::Root as i32,
            operator_info: "gt(a, 0)".to_owned(),
            brief_operator_info: "gt(a, 0)".to_owned(),
            memory_bytes: -1,
            disk_bytes: -1,
            ..Default::default()
        };
        ExplainData {
            main: Some(ExplainOperator {
                name: "HashJoin_1".to_owned(),
                brief_name: "HashJoin".to_owned(),
                children: vec![probe, scan],
                est_rows: 2.0,
                cost: 1.25,
                act_rows: 2,
                task_type: TaskType::Root as i32,
                operator_info: "equal:[eq(a, b)]".to_owned(),
                brief_operator_info: "inner join".to_owned(),
                memory_bytes: 2048,
                disk_bytes: 0,
                ..Default::default()
            }),
            with_runtime_stats: runtime,
            ..Default::default()
        }
    }

    #[test]
    fn binary_plan_preserves_tree_order_access_and_runtime_fields() {
        let encoded = compress(&sample_binary_plan(true).encode_to_vec());
        let decoded = decode_binary_plan(&encoded).unwrap();
        assert!(decoded.starts_with("\n| id"));
        assert!(decoded.contains("HashJoin_1"));
        let build = decoded.find("TableScan_2(Build)").unwrap();
        let probe = decoded.find("Selection_3(Probe)").unwrap();
        assert!(build < probe, "build child must print before probe child");
        assert!(decoded.contains("table:t, partition:p0, index:idx(a, b)"));
        assert!(decoded.contains("time:1ms, loops:1, cop_task:num:1"));
        assert!(decoded.contains("1024 Bytes"));
        assert!(decoded.contains("N/A"));

        let brief = decode_binary_plan_for_connection(&encoded, "brief", false).unwrap();
        assert_eq!(brief.len(), 3);
        assert_eq!(brief[0][0], "HashJoin");
        assert_eq!(brief[0][6], "inner join");
        assert_eq!(brief[1][0], "├─TableScan(Build)");

        let top_sql = decode_binary_plan_for_connection(&encoded, "row", true).unwrap();
        assert_eq!(top_sql[0].len(), 5);
        assert_eq!(top_sql[0][3], "root");
    }

    #[test]
    fn connection_formats_select_source_columns() {
        let encoded = compress(&sample_binary_plan(false).encode_to_vec());
        let row = decode_binary_plan_for_connection(&encoded, "row", false).unwrap();
        assert_eq!(
            row[0],
            ["HashJoin_1", "2.00", "root", "", "equal:[eq(a, b)]"]
        );

        let tree = decode_binary_plan_for_connection(&encoded, "plan_tree", false).unwrap();
        assert_eq!(
            tree[0],
            ["HashJoin_1", "1.25", "root", "", "equal:[eq(a, b)]"]
        );

        let verbose = decode_binary_plan_for_connection(&encoded, "verbose", false).unwrap();
        assert_eq!(
            verbose[0],
            ["HashJoin_1", "2.00", "1.25", "root", "", "equal:[eq(a, b)]"]
        );

        let unknown = decode_binary_plan_for_connection(&encoded, "unknown", false).unwrap();
        assert_eq!(unknown.len(), 3);
        assert!(unknown.iter().all(Vec::is_empty));
        let decoded = decode_binary_plan(&encoded).unwrap();
        assert!(decoded.contains("| id"));
        assert!(!decoded.contains("actRows"));
    }

    #[test]
    fn binary_plan_and_connection_use_their_distinct_root_sets() {
        let mut plan = sample_binary_plan(false);
        plan.ctes.push(ExplainOperator {
            name: "CTE_4".to_owned(),
            task_type: TaskType::Root as i32,
            ..Default::default()
        });
        plan.subqueries.push(ExplainOperator {
            name: "ScalarSubQuery_5".to_owned(),
            task_type: TaskType::Root as i32,
            ..Default::default()
        });
        let encoded = compress(&plan.encode_to_vec());
        let decoded = decode_binary_plan(&encoded).unwrap();
        assert!(decoded.contains("CTE_4"));
        assert!(decoded.contains("ScalarSubQuery_5"));
        let connection = decode_binary_plan_for_connection(&encoded, "verbose", false).unwrap();
        assert!(connection.iter().any(|row| row[0] == "CTE_4"));
        assert!(connection.iter().all(|row| row[0] != "ScalarSubQuery_5"));

        let empty = compress(&ExplainData::default().encode_to_vec());
        assert_eq!(decode_binary_plan(&empty).unwrap(), "");
        assert!(decode_binary_plan_for_connection(&empty, "row", false)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn access_objects_and_operator_labels_match_source_rendering() {
        assert_eq!(
            print_driver_side(&[
                OperatorLabel::BuildSide as i32,
                OperatorLabel::SeedPart as i32,
                OperatorLabel::RecursivePart as i32,
                99,
            ]),
            "(Build)(Seed Part)(Recursive Part)"
        );

        let dynamic = |objects| AccessObject {
            access_object: Some(AccessObjectKind::DynamicPartitionObjects(
                DynamicPartitionAccessObjects { objects },
            )),
        };
        assert_eq!(
            print_access_object(&[dynamic(vec![DynamicPartitionAccessObject {
                all_partitions: true,
                ..Default::default()
            }])]),
            "partition:all"
        );
        assert_eq!(
            print_access_object(&[dynamic(vec![DynamicPartitionAccessObject::default()])]),
            "partition:dual"
        );
        assert_eq!(
            print_access_object(&[dynamic(vec![
                DynamicPartitionAccessObject {
                    table: "t0".to_owned(),
                    partitions: vec!["p0".to_owned(), "p1".to_owned()],
                    ..Default::default()
                },
                DynamicPartitionAccessObject {
                    table: "t1".to_owned(),
                    all_partitions: true,
                    ..Default::default()
                },
            ])]),
            "partition:p0,p1 of t0, partition:all of t1"
        );
        assert_eq!(
            print_access_object(&[
                AccessObject {
                    access_object: Some(AccessObjectKind::OtherObject("range:[1,2]".to_owned())),
                },
                AccessObject {
                    access_object: Some(AccessObjectKind::OtherObject(
                        "keep order:true".to_owned()
                    )),
                },
            ]),
            "range:[1,2]keep order:true"
        );

        let mut rows = Vec::new();
        decode_binary_operator(
            &ExplainOperator {
                name: "FutureOperator_1".to_owned(),
                task_type: 99,
                store_type: 98,
                ..Default::default()
            },
            "",
            true,
            false,
            false,
            &mut rows,
        );
        assert_eq!(rows[0][3], "99[98]");
    }

    #[test]
    fn binary_discard_sentinel_round_trips() {
        assert_eq!(
            decode_binary_plan(BINARY_PLAN_DISCARDED_ENCODED.as_str()).unwrap(),
            PLAN_DISCARDED_DECODED
        );
        assert!(decode_binary_plan_for_connection(
            BINARY_PLAN_DISCARDED_ENCODED.as_str(),
            "row",
            false,
        )
        .unwrap()
        .is_empty());
    }

    #[test]
    fn binary_decode_errors_match_sql_warning_vectors() {
        let cases = [
            ("some random bytes", "illegal base64 data at input byte 4"),
            ("c29tZSByYW5kb20gYnl0ZXM=", "snappy: corrupt input"),
            ("EUBzb21lIHJhbmRvbSBieXRlcw==", "proto: illegal wireType 7"),
        ];
        for (input, expected) in cases {
            assert_eq!(decode_binary_plan(input).unwrap_err().to_string(), expected);
        }
        assert_eq!(
            decode_binary_plan([0xff]).unwrap_err().to_string(),
            "illegal base64 data at input byte 0"
        );
    }
}
