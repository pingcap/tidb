// This file is generated. Do not edit
// @generated

// https://github.com/Manishearth/rust-clippy/issues/702
#![allow(unknown_lints)]
#![allow(clippy)]

#![cfg_attr(rustfmt, rustfmt_skip)]

#![allow(box_pointers)]
#![allow(dead_code)]
#![allow(missing_docs)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(trivial_casts)]
#![allow(unsafe_code)]
#![allow(unused_imports)]
#![allow(unused_results)]

use protobuf::Message as Message_imported_for_functions;
use protobuf::ProtobufEnum as ProtobufEnum_imported_for_functions;

#[derive(PartialEq,Clone,Default)]
pub struct SelectRequest {
    // message fields
    start_ts: ::std::option::Option<u64>,
    table_info: ::protobuf::SingularPtrField<super::schema::TableInfo>,
    index_info: ::protobuf::SingularPtrField<super::schema::IndexInfo>,
    fields: ::protobuf::RepeatedField<super::expression::Expr>,
    ranges: ::protobuf::RepeatedField<super::schema::KeyRange>,
    distinct: ::std::option::Option<bool>,
    field_where: ::protobuf::SingularPtrField<super::expression::Expr>,
    group_by: ::protobuf::RepeatedField<super::expression::ByItem>,
    having: ::protobuf::SingularPtrField<super::expression::Expr>,
    order_by: ::protobuf::RepeatedField<super::expression::ByItem>,
    limit: ::std::option::Option<i64>,
    aggregates: ::protobuf::RepeatedField<super::expression::Expr>,
    time_zone_offset: ::std::option::Option<i64>,
    flags: ::std::option::Option<u64>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for SelectRequest {}

impl SelectRequest {
    pub fn new() -> SelectRequest {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static SelectRequest {
        static mut instance: ::protobuf::lazy::Lazy<SelectRequest> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const SelectRequest,
        };
        unsafe {
            instance.get(SelectRequest::new)
        }
    }

    // optional uint64 start_ts = 1;

    pub fn clear_start_ts(&mut self) {
        self.start_ts = ::std::option::Option::None;
    }

    pub fn has_start_ts(&self) -> bool {
        self.start_ts.is_some()
    }

    // Param is passed by value, moved
    pub fn set_start_ts(&mut self, v: u64) {
        self.start_ts = ::std::option::Option::Some(v);
    }

    pub fn get_start_ts(&self) -> u64 {
        self.start_ts.unwrap_or(0)
    }

    fn get_start_ts_for_reflect(&self) -> &::std::option::Option<u64> {
        &self.start_ts
    }

    fn mut_start_ts_for_reflect(&mut self) -> &mut ::std::option::Option<u64> {
        &mut self.start_ts
    }

    // optional .tipb.TableInfo table_info = 2;

    pub fn clear_table_info(&mut self) {
        self.table_info.clear();
    }

    pub fn has_table_info(&self) -> bool {
        self.table_info.is_some()
    }

    // Param is passed by value, moved
    pub fn set_table_info(&mut self, v: super::schema::TableInfo) {
        self.table_info = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_table_info(&mut self) -> &mut super::schema::TableInfo {
        if self.table_info.is_none() {
            self.table_info.set_default();
        }
        self.table_info.as_mut().unwrap()
    }

    // Take field
    pub fn take_table_info(&mut self) -> super::schema::TableInfo {
        self.table_info.take().unwrap_or_else(|| super::schema::TableInfo::new())
    }

    pub fn get_table_info(&self) -> &super::schema::TableInfo {
        self.table_info.as_ref().unwrap_or_else(|| super::schema::TableInfo::default_instance())
    }

    fn get_table_info_for_reflect(&self) -> &::protobuf::SingularPtrField<super::schema::TableInfo> {
        &self.table_info
    }

    fn mut_table_info_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<super::schema::TableInfo> {
        &mut self.table_info
    }

    // optional .tipb.IndexInfo index_info = 3;

    pub fn clear_index_info(&mut self) {
        self.index_info.clear();
    }

    pub fn has_index_info(&self) -> bool {
        self.index_info.is_some()
    }

    // Param is passed by value, moved
    pub fn set_index_info(&mut self, v: super::schema::IndexInfo) {
        self.index_info = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_index_info(&mut self) -> &mut super::schema::IndexInfo {
        if self.index_info.is_none() {
            self.index_info.set_default();
        }
        self.index_info.as_mut().unwrap()
    }

    // Take field
    pub fn take_index_info(&mut self) -> super::schema::IndexInfo {
        self.index_info.take().unwrap_or_else(|| super::schema::IndexInfo::new())
    }

    pub fn get_index_info(&self) -> &super::schema::IndexInfo {
        self.index_info.as_ref().unwrap_or_else(|| super::schema::IndexInfo::default_instance())
    }

    fn get_index_info_for_reflect(&self) -> &::protobuf::SingularPtrField<super::schema::IndexInfo> {
        &self.index_info
    }

    fn mut_index_info_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<super::schema::IndexInfo> {
        &mut self.index_info
    }

    // repeated .tipb.Expr fields = 4;

    pub fn clear_fields(&mut self) {
        self.fields.clear();
    }

    // Param is passed by value, moved
    pub fn set_fields(&mut self, v: ::protobuf::RepeatedField<super::expression::Expr>) {
        self.fields = v;
    }

    // Mutable pointer to the field.
    pub fn mut_fields(&mut self) -> &mut ::protobuf::RepeatedField<super::expression::Expr> {
        &mut self.fields
    }

    // Take field
    pub fn take_fields(&mut self) -> ::protobuf::RepeatedField<super::expression::Expr> {
        ::std::mem::replace(&mut self.fields, ::protobuf::RepeatedField::new())
    }

    pub fn get_fields(&self) -> &[super::expression::Expr] {
        &self.fields
    }

    fn get_fields_for_reflect(&self) -> &::protobuf::RepeatedField<super::expression::Expr> {
        &self.fields
    }

    fn mut_fields_for_reflect(&mut self) -> &mut ::protobuf::RepeatedField<super::expression::Expr> {
        &mut self.fields
    }

    // repeated .tipb.KeyRange ranges = 5;

    pub fn clear_ranges(&mut self) {
        self.ranges.clear();
    }

    // Param is passed by value, moved
    pub fn set_ranges(&mut self, v: ::protobuf::RepeatedField<super::schema::KeyRange>) {
        self.ranges = v;
    }

    // Mutable pointer to the field.
    pub fn mut_ranges(&mut self) -> &mut ::protobuf::RepeatedField<super::schema::KeyRange> {
        &mut self.ranges
    }

    // Take field
    pub fn take_ranges(&mut self) -> ::protobuf::RepeatedField<super::schema::KeyRange> {
        ::std::mem::replace(&mut self.ranges, ::protobuf::RepeatedField::new())
    }

    pub fn get_ranges(&self) -> &[super::schema::KeyRange] {
        &self.ranges
    }

    fn get_ranges_for_reflect(&self) -> &::protobuf::RepeatedField<super::schema::KeyRange> {
        &self.ranges
    }

    fn mut_ranges_for_reflect(&mut self) -> &mut ::protobuf::RepeatedField<super::schema::KeyRange> {
        &mut self.ranges
    }

    // optional bool distinct = 6;

    pub fn clear_distinct(&mut self) {
        self.distinct = ::std::option::Option::None;
    }

    pub fn has_distinct(&self) -> bool {
        self.distinct.is_some()
    }

    // Param is passed by value, moved
    pub fn set_distinct(&mut self, v: bool) {
        self.distinct = ::std::option::Option::Some(v);
    }

    pub fn get_distinct(&self) -> bool {
        self.distinct.unwrap_or(false)
    }

    fn get_distinct_for_reflect(&self) -> &::std::option::Option<bool> {
        &self.distinct
    }

    fn mut_distinct_for_reflect(&mut self) -> &mut ::std::option::Option<bool> {
        &mut self.distinct
    }

    // optional .tipb.Expr where = 7;

    pub fn clear_field_where(&mut self) {
        self.field_where.clear();
    }

    pub fn has_field_where(&self) -> bool {
        self.field_where.is_some()
    }

    // Param is passed by value, moved
    pub fn set_field_where(&mut self, v: super::expression::Expr) {
        self.field_where = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_field_where(&mut self) -> &mut super::expression::Expr {
        if self.field_where.is_none() {
            self.field_where.set_default();
        }
        self.field_where.as_mut().unwrap()
    }

    // Take field
    pub fn take_field_where(&mut self) -> super::expression::Expr {
        self.field_where.take().unwrap_or_else(|| super::expression::Expr::new())
    }

    pub fn get_field_where(&self) -> &super::expression::Expr {
        self.field_where.as_ref().unwrap_or_else(|| super::expression::Expr::default_instance())
    }

    fn get_field_where_for_reflect(&self) -> &::protobuf::SingularPtrField<super::expression::Expr> {
        &self.field_where
    }

    fn mut_field_where_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<super::expression::Expr> {
        &mut self.field_where
    }

    // repeated .tipb.ByItem group_by = 8;

    pub fn clear_group_by(&mut self) {
        self.group_by.clear();
    }

    // Param is passed by value, moved
    pub fn set_group_by(&mut self, v: ::protobuf::RepeatedField<super::expression::ByItem>) {
        self.group_by = v;
    }

    // Mutable pointer to the field.
    pub fn mut_group_by(&mut self) -> &mut ::protobuf::RepeatedField<super::expression::ByItem> {
        &mut self.group_by
    }

    // Take field
    pub fn take_group_by(&mut self) -> ::protobuf::RepeatedField<super::expression::ByItem> {
        ::std::mem::replace(&mut self.group_by, ::protobuf::RepeatedField::new())
    }

    pub fn get_group_by(&self) -> &[super::expression::ByItem] {
        &self.group_by
    }

    fn get_group_by_for_reflect(&self) -> &::protobuf::RepeatedField<super::expression::ByItem> {
        &self.group_by
    }

    fn mut_group_by_for_reflect(&mut self) -> &mut ::protobuf::RepeatedField<super::expression::ByItem> {
        &mut self.group_by
    }

    // optional .tipb.Expr having = 9;

    pub fn clear_having(&mut self) {
        self.having.clear();
    }

    pub fn has_having(&self) -> bool {
        self.having.is_some()
    }

    // Param is passed by value, moved
    pub fn set_having(&mut self, v: super::expression::Expr) {
        self.having = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_having(&mut self) -> &mut super::expression::Expr {
        if self.having.is_none() {
            self.having.set_default();
        }
        self.having.as_mut().unwrap()
    }

    // Take field
    pub fn take_having(&mut self) -> super::expression::Expr {
        self.having.take().unwrap_or_else(|| super::expression::Expr::new())
    }

    pub fn get_having(&self) -> &super::expression::Expr {
        self.having.as_ref().unwrap_or_else(|| super::expression::Expr::default_instance())
    }

    fn get_having_for_reflect(&self) -> &::protobuf::SingularPtrField<super::expression::Expr> {
        &self.having
    }

    fn mut_having_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<super::expression::Expr> {
        &mut self.having
    }

    // repeated .tipb.ByItem order_by = 10;

    pub fn clear_order_by(&mut self) {
        self.order_by.clear();
    }

    // Param is passed by value, moved
    pub fn set_order_by(&mut self, v: ::protobuf::RepeatedField<super::expression::ByItem>) {
        self.order_by = v;
    }

    // Mutable pointer to the field.
    pub fn mut_order_by(&mut self) -> &mut ::protobuf::RepeatedField<super::expression::ByItem> {
        &mut self.order_by
    }

    // Take field
    pub fn take_order_by(&mut self) -> ::protobuf::RepeatedField<super::expression::ByItem> {
        ::std::mem::replace(&mut self.order_by, ::protobuf::RepeatedField::new())
    }

    pub fn get_order_by(&self) -> &[super::expression::ByItem] {
        &self.order_by
    }

    fn get_order_by_for_reflect(&self) -> &::protobuf::RepeatedField<super::expression::ByItem> {
        &self.order_by
    }

    fn mut_order_by_for_reflect(&mut self) -> &mut ::protobuf::RepeatedField<super::expression::ByItem> {
        &mut self.order_by
    }

    // optional int64 limit = 12;

    pub fn clear_limit(&mut self) {
        self.limit = ::std::option::Option::None;
    }

    pub fn has_limit(&self) -> bool {
        self.limit.is_some()
    }

    // Param is passed by value, moved
    pub fn set_limit(&mut self, v: i64) {
        self.limit = ::std::option::Option::Some(v);
    }

    pub fn get_limit(&self) -> i64 {
        self.limit.unwrap_or(0)
    }

    fn get_limit_for_reflect(&self) -> &::std::option::Option<i64> {
        &self.limit
    }

    fn mut_limit_for_reflect(&mut self) -> &mut ::std::option::Option<i64> {
        &mut self.limit
    }

    // repeated .tipb.Expr aggregates = 13;

    pub fn clear_aggregates(&mut self) {
        self.aggregates.clear();
    }

    // Param is passed by value, moved
    pub fn set_aggregates(&mut self, v: ::protobuf::RepeatedField<super::expression::Expr>) {
        self.aggregates = v;
    }

    // Mutable pointer to the field.
    pub fn mut_aggregates(&mut self) -> &mut ::protobuf::RepeatedField<super::expression::Expr> {
        &mut self.aggregates
    }

    // Take field
    pub fn take_aggregates(&mut self) -> ::protobuf::RepeatedField<super::expression::Expr> {
        ::std::mem::replace(&mut self.aggregates, ::protobuf::RepeatedField::new())
    }

    pub fn get_aggregates(&self) -> &[super::expression::Expr] {
        &self.aggregates
    }

    fn get_aggregates_for_reflect(&self) -> &::protobuf::RepeatedField<super::expression::Expr> {
        &self.aggregates
    }

    fn mut_aggregates_for_reflect(&mut self) -> &mut ::protobuf::RepeatedField<super::expression::Expr> {
        &mut self.aggregates
    }

    // optional int64 time_zone_offset = 14;

    pub fn clear_time_zone_offset(&mut self) {
        self.time_zone_offset = ::std::option::Option::None;
    }

    pub fn has_time_zone_offset(&self) -> bool {
        self.time_zone_offset.is_some()
    }

    // Param is passed by value, moved
    pub fn set_time_zone_offset(&mut self, v: i64) {
        self.time_zone_offset = ::std::option::Option::Some(v);
    }

    pub fn get_time_zone_offset(&self) -> i64 {
        self.time_zone_offset.unwrap_or(0)
    }

    fn get_time_zone_offset_for_reflect(&self) -> &::std::option::Option<i64> {
        &self.time_zone_offset
    }

    fn mut_time_zone_offset_for_reflect(&mut self) -> &mut ::std::option::Option<i64> {
        &mut self.time_zone_offset
    }

    // optional uint64 flags = 15;

    pub fn clear_flags(&mut self) {
        self.flags = ::std::option::Option::None;
    }

    pub fn has_flags(&self) -> bool {
        self.flags.is_some()
    }

    // Param is passed by value, moved
    pub fn set_flags(&mut self, v: u64) {
        self.flags = ::std::option::Option::Some(v);
    }

    pub fn get_flags(&self) -> u64 {
        self.flags.unwrap_or(0)
    }

    fn get_flags_for_reflect(&self) -> &::std::option::Option<u64> {
        &self.flags
    }

    fn mut_flags_for_reflect(&mut self) -> &mut ::std::option::Option<u64> {
        &mut self.flags
    }
}

impl ::protobuf::Message for SelectRequest {
    fn is_initialized(&self) -> bool {
        for v in &self.table_info {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.index_info {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.fields {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.ranges {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.field_where {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.group_by {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.having {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.order_by {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.aggregates {
            if !v.is_initialized() {
                return false;
            }
        };
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !is.eof()? {
            let (field_number, wire_type) = is.read_tag_unpack()?;
            match field_number {
                1 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_uint64()?;
                    self.start_ts = ::std::option::Option::Some(tmp);
                },
                2 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.table_info)?;
                },
                3 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.index_info)?;
                },
                4 => {
                    ::protobuf::rt::read_repeated_message_into(wire_type, is, &mut self.fields)?;
                },
                5 => {
                    ::protobuf::rt::read_repeated_message_into(wire_type, is, &mut self.ranges)?;
                },
                6 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_bool()?;
                    self.distinct = ::std::option::Option::Some(tmp);
                },
                7 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.field_where)?;
                },
                8 => {
                    ::protobuf::rt::read_repeated_message_into(wire_type, is, &mut self.group_by)?;
                },
                9 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.having)?;
                },
                10 => {
                    ::protobuf::rt::read_repeated_message_into(wire_type, is, &mut self.order_by)?;
                },
                12 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_int64()?;
                    self.limit = ::std::option::Option::Some(tmp);
                },
                13 => {
                    ::protobuf::rt::read_repeated_message_into(wire_type, is, &mut self.aggregates)?;
                },
                14 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_int64()?;
                    self.time_zone_offset = ::std::option::Option::Some(tmp);
                },
                15 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_uint64()?;
                    self.flags = ::std::option::Option::Some(tmp);
                },
                _ => {
                    ::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields())?;
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        if let Some(v) = self.start_ts {
            my_size += ::protobuf::rt::value_size(1, v, ::protobuf::wire_format::WireTypeVarint);
        }
        if let Some(ref v) = self.table_info.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if let Some(ref v) = self.index_info.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        for value in &self.fields {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        for value in &self.ranges {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        if let Some(v) = self.distinct {
            my_size += 2;
        }
        if let Some(ref v) = self.field_where.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        for value in &self.group_by {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        if let Some(ref v) = self.having.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        for value in &self.order_by {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        if let Some(v) = self.limit {
            my_size += ::protobuf::rt::value_size(12, v, ::protobuf::wire_format::WireTypeVarint);
        }
        for value in &self.aggregates {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        if let Some(v) = self.time_zone_offset {
            my_size += ::protobuf::rt::value_size(14, v, ::protobuf::wire_format::WireTypeVarint);
        }
        if let Some(v) = self.flags {
            my_size += ::protobuf::rt::value_size(15, v, ::protobuf::wire_format::WireTypeVarint);
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.start_ts {
            os.write_uint64(1, v)?;
        }
        if let Some(ref v) = self.table_info.as_ref() {
            os.write_tag(2, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if let Some(ref v) = self.index_info.as_ref() {
            os.write_tag(3, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        for v in &self.fields {
            os.write_tag(4, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        };
        for v in &self.ranges {
            os.write_tag(5, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        };
        if let Some(v) = self.distinct {
            os.write_bool(6, v)?;
        }
        if let Some(ref v) = self.field_where.as_ref() {
            os.write_tag(7, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        for v in &self.group_by {
            os.write_tag(8, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        };
        if let Some(ref v) = self.having.as_ref() {
            os.write_tag(9, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        for v in &self.order_by {
            os.write_tag(10, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        };
        if let Some(v) = self.limit {
            os.write_int64(12, v)?;
        }
        for v in &self.aggregates {
            os.write_tag(13, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        };
        if let Some(v) = self.time_zone_offset {
            os.write_int64(14, v)?;
        }
        if let Some(v) = self.flags {
            os.write_uint64(15, v)?;
        }
        os.write_unknown_fields(self.get_unknown_fields())?;
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }
    fn as_any_mut(&mut self) -> &mut ::std::any::Any {
        self as &mut ::std::any::Any
    }
    fn into_any(self: Box<Self>) -> ::std::boxed::Box<::std::any::Any> {
        self
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for SelectRequest {
    fn new() -> SelectRequest {
        SelectRequest::new()
    }

    fn descriptor_static(_: ::std::option::Option<SelectRequest>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "start_ts",
                    SelectRequest::get_start_ts_for_reflect,
                    SelectRequest::mut_start_ts_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<super::schema::TableInfo>>(
                    "table_info",
                    SelectRequest::get_table_info_for_reflect,
                    SelectRequest::mut_table_info_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<super::schema::IndexInfo>>(
                    "index_info",
                    SelectRequest::get_index_info_for_reflect,
                    SelectRequest::mut_index_info_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_repeated_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<super::expression::Expr>>(
                    "fields",
                    SelectRequest::get_fields_for_reflect,
                    SelectRequest::mut_fields_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_repeated_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<super::schema::KeyRange>>(
                    "ranges",
                    SelectRequest::get_ranges_for_reflect,
                    SelectRequest::mut_ranges_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeBool>(
                    "distinct",
                    SelectRequest::get_distinct_for_reflect,
                    SelectRequest::mut_distinct_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<super::expression::Expr>>(
                    "where",
                    SelectRequest::get_field_where_for_reflect,
                    SelectRequest::mut_field_where_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_repeated_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<super::expression::ByItem>>(
                    "group_by",
                    SelectRequest::get_group_by_for_reflect,
                    SelectRequest::mut_group_by_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<super::expression::Expr>>(
                    "having",
                    SelectRequest::get_having_for_reflect,
                    SelectRequest::mut_having_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_repeated_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<super::expression::ByItem>>(
                    "order_by",
                    SelectRequest::get_order_by_for_reflect,
                    SelectRequest::mut_order_by_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeInt64>(
                    "limit",
                    SelectRequest::get_limit_for_reflect,
                    SelectRequest::mut_limit_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_repeated_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<super::expression::Expr>>(
                    "aggregates",
                    SelectRequest::get_aggregates_for_reflect,
                    SelectRequest::mut_aggregates_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeInt64>(
                    "time_zone_offset",
                    SelectRequest::get_time_zone_offset_for_reflect,
                    SelectRequest::mut_time_zone_offset_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "flags",
                    SelectRequest::get_flags_for_reflect,
                    SelectRequest::mut_flags_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<SelectRequest>(
                    "SelectRequest",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for SelectRequest {
    fn clear(&mut self) {
        self.clear_start_ts();
        self.clear_table_info();
        self.clear_index_info();
        self.clear_fields();
        self.clear_ranges();
        self.clear_distinct();
        self.clear_field_where();
        self.clear_group_by();
        self.clear_having();
        self.clear_order_by();
        self.clear_limit();
        self.clear_aggregates();
        self.clear_time_zone_offset();
        self.clear_flags();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for SelectRequest {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for SelectRequest {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct Row {
    // message fields
    handle: ::protobuf::SingularField<::std::vec::Vec<u8>>,
    data: ::protobuf::SingularField<::std::vec::Vec<u8>>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for Row {}

impl Row {
    pub fn new() -> Row {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static Row {
        static mut instance: ::protobuf::lazy::Lazy<Row> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const Row,
        };
        unsafe {
            instance.get(Row::new)
        }
    }

    // optional bytes handle = 1;

    pub fn clear_handle(&mut self) {
        self.handle.clear();
    }

    pub fn has_handle(&self) -> bool {
        self.handle.is_some()
    }

    // Param is passed by value, moved
    pub fn set_handle(&mut self, v: ::std::vec::Vec<u8>) {
        self.handle = ::protobuf::SingularField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_handle(&mut self) -> &mut ::std::vec::Vec<u8> {
        if self.handle.is_none() {
            self.handle.set_default();
        }
        self.handle.as_mut().unwrap()
    }

    // Take field
    pub fn take_handle(&mut self) -> ::std::vec::Vec<u8> {
        self.handle.take().unwrap_or_else(|| ::std::vec::Vec::new())
    }

    pub fn get_handle(&self) -> &[u8] {
        match self.handle.as_ref() {
            Some(v) => &v,
            None => &[],
        }
    }

    fn get_handle_for_reflect(&self) -> &::protobuf::SingularField<::std::vec::Vec<u8>> {
        &self.handle
    }

    fn mut_handle_for_reflect(&mut self) -> &mut ::protobuf::SingularField<::std::vec::Vec<u8>> {
        &mut self.handle
    }

    // optional bytes data = 2;

    pub fn clear_data(&mut self) {
        self.data.clear();
    }

    pub fn has_data(&self) -> bool {
        self.data.is_some()
    }

    // Param is passed by value, moved
    pub fn set_data(&mut self, v: ::std::vec::Vec<u8>) {
        self.data = ::protobuf::SingularField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_data(&mut self) -> &mut ::std::vec::Vec<u8> {
        if self.data.is_none() {
            self.data.set_default();
        }
        self.data.as_mut().unwrap()
    }

    // Take field
    pub fn take_data(&mut self) -> ::std::vec::Vec<u8> {
        self.data.take().unwrap_or_else(|| ::std::vec::Vec::new())
    }

    pub fn get_data(&self) -> &[u8] {
        match self.data.as_ref() {
            Some(v) => &v,
            None => &[],
        }
    }

    fn get_data_for_reflect(&self) -> &::protobuf::SingularField<::std::vec::Vec<u8>> {
        &self.data
    }

    fn mut_data_for_reflect(&mut self) -> &mut ::protobuf::SingularField<::std::vec::Vec<u8>> {
        &mut self.data
    }
}

impl ::protobuf::Message for Row {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !is.eof()? {
            let (field_number, wire_type) = is.read_tag_unpack()?;
            match field_number {
                1 => {
                    ::protobuf::rt::read_singular_bytes_into(wire_type, is, &mut self.handle)?;
                },
                2 => {
                    ::protobuf::rt::read_singular_bytes_into(wire_type, is, &mut self.data)?;
                },
                _ => {
                    ::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields())?;
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        if let Some(ref v) = self.handle.as_ref() {
            my_size += ::protobuf::rt::bytes_size(1, &v);
        }
        if let Some(ref v) = self.data.as_ref() {
            my_size += ::protobuf::rt::bytes_size(2, &v);
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(ref v) = self.handle.as_ref() {
            os.write_bytes(1, &v)?;
        }
        if let Some(ref v) = self.data.as_ref() {
            os.write_bytes(2, &v)?;
        }
        os.write_unknown_fields(self.get_unknown_fields())?;
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }
    fn as_any_mut(&mut self) -> &mut ::std::any::Any {
        self as &mut ::std::any::Any
    }
    fn into_any(self: Box<Self>) -> ::std::boxed::Box<::std::any::Any> {
        self
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for Row {
    fn new() -> Row {
        Row::new()
    }

    fn descriptor_static(_: ::std::option::Option<Row>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_field_accessor::<_, ::protobuf::types::ProtobufTypeBytes>(
                    "handle",
                    Row::get_handle_for_reflect,
                    Row::mut_handle_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_field_accessor::<_, ::protobuf::types::ProtobufTypeBytes>(
                    "data",
                    Row::get_data_for_reflect,
                    Row::mut_data_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<Row>(
                    "Row",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for Row {
    fn clear(&mut self) {
        self.clear_handle();
        self.clear_data();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for Row {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for Row {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct Error {
    // message fields
    code: ::std::option::Option<i32>,
    msg: ::protobuf::SingularField<::std::string::String>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for Error {}

impl Error {
    pub fn new() -> Error {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static Error {
        static mut instance: ::protobuf::lazy::Lazy<Error> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const Error,
        };
        unsafe {
            instance.get(Error::new)
        }
    }

    // optional int32 code = 1;

    pub fn clear_code(&mut self) {
        self.code = ::std::option::Option::None;
    }

    pub fn has_code(&self) -> bool {
        self.code.is_some()
    }

    // Param is passed by value, moved
    pub fn set_code(&mut self, v: i32) {
        self.code = ::std::option::Option::Some(v);
    }

    pub fn get_code(&self) -> i32 {
        self.code.unwrap_or(0)
    }

    fn get_code_for_reflect(&self) -> &::std::option::Option<i32> {
        &self.code
    }

    fn mut_code_for_reflect(&mut self) -> &mut ::std::option::Option<i32> {
        &mut self.code
    }

    // optional string msg = 2;

    pub fn clear_msg(&mut self) {
        self.msg.clear();
    }

    pub fn has_msg(&self) -> bool {
        self.msg.is_some()
    }

    // Param is passed by value, moved
    pub fn set_msg(&mut self, v: ::std::string::String) {
        self.msg = ::protobuf::SingularField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_msg(&mut self) -> &mut ::std::string::String {
        if self.msg.is_none() {
            self.msg.set_default();
        }
        self.msg.as_mut().unwrap()
    }

    // Take field
    pub fn take_msg(&mut self) -> ::std::string::String {
        self.msg.take().unwrap_or_else(|| ::std::string::String::new())
    }

    pub fn get_msg(&self) -> &str {
        match self.msg.as_ref() {
            Some(v) => &v,
            None => "",
        }
    }

    fn get_msg_for_reflect(&self) -> &::protobuf::SingularField<::std::string::String> {
        &self.msg
    }

    fn mut_msg_for_reflect(&mut self) -> &mut ::protobuf::SingularField<::std::string::String> {
        &mut self.msg
    }
}

impl ::protobuf::Message for Error {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !is.eof()? {
            let (field_number, wire_type) = is.read_tag_unpack()?;
            match field_number {
                1 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_int32()?;
                    self.code = ::std::option::Option::Some(tmp);
                },
                2 => {
                    ::protobuf::rt::read_singular_string_into(wire_type, is, &mut self.msg)?;
                },
                _ => {
                    ::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields())?;
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        if let Some(v) = self.code {
            my_size += ::protobuf::rt::value_size(1, v, ::protobuf::wire_format::WireTypeVarint);
        }
        if let Some(ref v) = self.msg.as_ref() {
            my_size += ::protobuf::rt::string_size(2, &v);
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.code {
            os.write_int32(1, v)?;
        }
        if let Some(ref v) = self.msg.as_ref() {
            os.write_string(2, &v)?;
        }
        os.write_unknown_fields(self.get_unknown_fields())?;
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }
    fn as_any_mut(&mut self) -> &mut ::std::any::Any {
        self as &mut ::std::any::Any
    }
    fn into_any(self: Box<Self>) -> ::std::boxed::Box<::std::any::Any> {
        self
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for Error {
    fn new() -> Error {
        Error::new()
    }

    fn descriptor_static(_: ::std::option::Option<Error>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeInt32>(
                    "code",
                    Error::get_code_for_reflect,
                    Error::mut_code_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_field_accessor::<_, ::protobuf::types::ProtobufTypeString>(
                    "msg",
                    Error::get_msg_for_reflect,
                    Error::mut_msg_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<Error>(
                    "Error",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for Error {
    fn clear(&mut self) {
        self.clear_code();
        self.clear_msg();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for Error {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for Error {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct SelectResponse {
    // message fields
    error: ::protobuf::SingularPtrField<Error>,
    rows: ::protobuf::RepeatedField<Row>,
    chunks: ::protobuf::RepeatedField<Chunk>,
    warnings: ::protobuf::RepeatedField<Error>,
    output_counts: ::std::vec::Vec<i64>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for SelectResponse {}

impl SelectResponse {
    pub fn new() -> SelectResponse {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static SelectResponse {
        static mut instance: ::protobuf::lazy::Lazy<SelectResponse> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const SelectResponse,
        };
        unsafe {
            instance.get(SelectResponse::new)
        }
    }

    // optional .tipb.Error error = 1;

    pub fn clear_error(&mut self) {
        self.error.clear();
    }

    pub fn has_error(&self) -> bool {
        self.error.is_some()
    }

    // Param is passed by value, moved
    pub fn set_error(&mut self, v: Error) {
        self.error = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_error(&mut self) -> &mut Error {
        if self.error.is_none() {
            self.error.set_default();
        }
        self.error.as_mut().unwrap()
    }

    // Take field
    pub fn take_error(&mut self) -> Error {
        self.error.take().unwrap_or_else(|| Error::new())
    }

    pub fn get_error(&self) -> &Error {
        self.error.as_ref().unwrap_or_else(|| Error::default_instance())
    }

    fn get_error_for_reflect(&self) -> &::protobuf::SingularPtrField<Error> {
        &self.error
    }

    fn mut_error_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<Error> {
        &mut self.error
    }

    // repeated .tipb.Row rows = 2;

    pub fn clear_rows(&mut self) {
        self.rows.clear();
    }

    // Param is passed by value, moved
    pub fn set_rows(&mut self, v: ::protobuf::RepeatedField<Row>) {
        self.rows = v;
    }

    // Mutable pointer to the field.
    pub fn mut_rows(&mut self) -> &mut ::protobuf::RepeatedField<Row> {
        &mut self.rows
    }

    // Take field
    pub fn take_rows(&mut self) -> ::protobuf::RepeatedField<Row> {
        ::std::mem::replace(&mut self.rows, ::protobuf::RepeatedField::new())
    }

    pub fn get_rows(&self) -> &[Row] {
        &self.rows
    }

    fn get_rows_for_reflect(&self) -> &::protobuf::RepeatedField<Row> {
        &self.rows
    }

    fn mut_rows_for_reflect(&mut self) -> &mut ::protobuf::RepeatedField<Row> {
        &mut self.rows
    }

    // repeated .tipb.Chunk chunks = 3;

    pub fn clear_chunks(&mut self) {
        self.chunks.clear();
    }

    // Param is passed by value, moved
    pub fn set_chunks(&mut self, v: ::protobuf::RepeatedField<Chunk>) {
        self.chunks = v;
    }

    // Mutable pointer to the field.
    pub fn mut_chunks(&mut self) -> &mut ::protobuf::RepeatedField<Chunk> {
        &mut self.chunks
    }

    // Take field
    pub fn take_chunks(&mut self) -> ::protobuf::RepeatedField<Chunk> {
        ::std::mem::replace(&mut self.chunks, ::protobuf::RepeatedField::new())
    }

    pub fn get_chunks(&self) -> &[Chunk] {
        &self.chunks
    }

    fn get_chunks_for_reflect(&self) -> &::protobuf::RepeatedField<Chunk> {
        &self.chunks
    }

    fn mut_chunks_for_reflect(&mut self) -> &mut ::protobuf::RepeatedField<Chunk> {
        &mut self.chunks
    }

    // repeated .tipb.Error warnings = 4;

    pub fn clear_warnings(&mut self) {
        self.warnings.clear();
    }

    // Param is passed by value, moved
    pub fn set_warnings(&mut self, v: ::protobuf::RepeatedField<Error>) {
        self.warnings = v;
    }

    // Mutable pointer to the field.
    pub fn mut_warnings(&mut self) -> &mut ::protobuf::RepeatedField<Error> {
        &mut self.warnings
    }

    // Take field
    pub fn take_warnings(&mut self) -> ::protobuf::RepeatedField<Error> {
        ::std::mem::replace(&mut self.warnings, ::protobuf::RepeatedField::new())
    }

    pub fn get_warnings(&self) -> &[Error] {
        &self.warnings
    }

    fn get_warnings_for_reflect(&self) -> &::protobuf::RepeatedField<Error> {
        &self.warnings
    }

    fn mut_warnings_for_reflect(&mut self) -> &mut ::protobuf::RepeatedField<Error> {
        &mut self.warnings
    }

    // repeated int64 output_counts = 5;

    pub fn clear_output_counts(&mut self) {
        self.output_counts.clear();
    }

    // Param is passed by value, moved
    pub fn set_output_counts(&mut self, v: ::std::vec::Vec<i64>) {
        self.output_counts = v;
    }

    // Mutable pointer to the field.
    pub fn mut_output_counts(&mut self) -> &mut ::std::vec::Vec<i64> {
        &mut self.output_counts
    }

    // Take field
    pub fn take_output_counts(&mut self) -> ::std::vec::Vec<i64> {
        ::std::mem::replace(&mut self.output_counts, ::std::vec::Vec::new())
    }

    pub fn get_output_counts(&self) -> &[i64] {
        &self.output_counts
    }

    fn get_output_counts_for_reflect(&self) -> &::std::vec::Vec<i64> {
        &self.output_counts
    }

    fn mut_output_counts_for_reflect(&mut self) -> &mut ::std::vec::Vec<i64> {
        &mut self.output_counts
    }
}

impl ::protobuf::Message for SelectResponse {
    fn is_initialized(&self) -> bool {
        for v in &self.error {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.rows {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.chunks {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.warnings {
            if !v.is_initialized() {
                return false;
            }
        };
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !is.eof()? {
            let (field_number, wire_type) = is.read_tag_unpack()?;
            match field_number {
                1 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.error)?;
                },
                2 => {
                    ::protobuf::rt::read_repeated_message_into(wire_type, is, &mut self.rows)?;
                },
                3 => {
                    ::protobuf::rt::read_repeated_message_into(wire_type, is, &mut self.chunks)?;
                },
                4 => {
                    ::protobuf::rt::read_repeated_message_into(wire_type, is, &mut self.warnings)?;
                },
                5 => {
                    ::protobuf::rt::read_repeated_int64_into(wire_type, is, &mut self.output_counts)?;
                },
                _ => {
                    ::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields())?;
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        if let Some(ref v) = self.error.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        for value in &self.rows {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        for value in &self.chunks {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        for value in &self.warnings {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        for value in &self.output_counts {
            my_size += ::protobuf::rt::value_size(5, *value, ::protobuf::wire_format::WireTypeVarint);
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(ref v) = self.error.as_ref() {
            os.write_tag(1, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        for v in &self.rows {
            os.write_tag(2, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        };
        for v in &self.chunks {
            os.write_tag(3, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        };
        for v in &self.warnings {
            os.write_tag(4, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        };
        for v in &self.output_counts {
            os.write_int64(5, *v)?;
        };
        os.write_unknown_fields(self.get_unknown_fields())?;
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }
    fn as_any_mut(&mut self) -> &mut ::std::any::Any {
        self as &mut ::std::any::Any
    }
    fn into_any(self: Box<Self>) -> ::std::boxed::Box<::std::any::Any> {
        self
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for SelectResponse {
    fn new() -> SelectResponse {
        SelectResponse::new()
    }

    fn descriptor_static(_: ::std::option::Option<SelectResponse>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<Error>>(
                    "error",
                    SelectResponse::get_error_for_reflect,
                    SelectResponse::mut_error_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_repeated_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<Row>>(
                    "rows",
                    SelectResponse::get_rows_for_reflect,
                    SelectResponse::mut_rows_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_repeated_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<Chunk>>(
                    "chunks",
                    SelectResponse::get_chunks_for_reflect,
                    SelectResponse::mut_chunks_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_repeated_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<Error>>(
                    "warnings",
                    SelectResponse::get_warnings_for_reflect,
                    SelectResponse::mut_warnings_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_vec_accessor::<_, ::protobuf::types::ProtobufTypeInt64>(
                    "output_counts",
                    SelectResponse::get_output_counts_for_reflect,
                    SelectResponse::mut_output_counts_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<SelectResponse>(
                    "SelectResponse",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for SelectResponse {
    fn clear(&mut self) {
        self.clear_error();
        self.clear_rows();
        self.clear_chunks();
        self.clear_warnings();
        self.clear_output_counts();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for SelectResponse {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for SelectResponse {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct Chunk {
    // message fields
    rows_data: ::protobuf::SingularField<::std::vec::Vec<u8>>,
    rows_meta: ::protobuf::RepeatedField<RowMeta>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for Chunk {}

impl Chunk {
    pub fn new() -> Chunk {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static Chunk {
        static mut instance: ::protobuf::lazy::Lazy<Chunk> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const Chunk,
        };
        unsafe {
            instance.get(Chunk::new)
        }
    }

    // optional bytes rows_data = 3;

    pub fn clear_rows_data(&mut self) {
        self.rows_data.clear();
    }

    pub fn has_rows_data(&self) -> bool {
        self.rows_data.is_some()
    }

    // Param is passed by value, moved
    pub fn set_rows_data(&mut self, v: ::std::vec::Vec<u8>) {
        self.rows_data = ::protobuf::SingularField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_rows_data(&mut self) -> &mut ::std::vec::Vec<u8> {
        if self.rows_data.is_none() {
            self.rows_data.set_default();
        }
        self.rows_data.as_mut().unwrap()
    }

    // Take field
    pub fn take_rows_data(&mut self) -> ::std::vec::Vec<u8> {
        self.rows_data.take().unwrap_or_else(|| ::std::vec::Vec::new())
    }

    pub fn get_rows_data(&self) -> &[u8] {
        match self.rows_data.as_ref() {
            Some(v) => &v,
            None => &[],
        }
    }

    fn get_rows_data_for_reflect(&self) -> &::protobuf::SingularField<::std::vec::Vec<u8>> {
        &self.rows_data
    }

    fn mut_rows_data_for_reflect(&mut self) -> &mut ::protobuf::SingularField<::std::vec::Vec<u8>> {
        &mut self.rows_data
    }

    // repeated .tipb.RowMeta rows_meta = 4;

    pub fn clear_rows_meta(&mut self) {
        self.rows_meta.clear();
    }

    // Param is passed by value, moved
    pub fn set_rows_meta(&mut self, v: ::protobuf::RepeatedField<RowMeta>) {
        self.rows_meta = v;
    }

    // Mutable pointer to the field.
    pub fn mut_rows_meta(&mut self) -> &mut ::protobuf::RepeatedField<RowMeta> {
        &mut self.rows_meta
    }

    // Take field
    pub fn take_rows_meta(&mut self) -> ::protobuf::RepeatedField<RowMeta> {
        ::std::mem::replace(&mut self.rows_meta, ::protobuf::RepeatedField::new())
    }

    pub fn get_rows_meta(&self) -> &[RowMeta] {
        &self.rows_meta
    }

    fn get_rows_meta_for_reflect(&self) -> &::protobuf::RepeatedField<RowMeta> {
        &self.rows_meta
    }

    fn mut_rows_meta_for_reflect(&mut self) -> &mut ::protobuf::RepeatedField<RowMeta> {
        &mut self.rows_meta
    }
}

impl ::protobuf::Message for Chunk {
    fn is_initialized(&self) -> bool {
        for v in &self.rows_meta {
            if !v.is_initialized() {
                return false;
            }
        };
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !is.eof()? {
            let (field_number, wire_type) = is.read_tag_unpack()?;
            match field_number {
                3 => {
                    ::protobuf::rt::read_singular_bytes_into(wire_type, is, &mut self.rows_data)?;
                },
                4 => {
                    ::protobuf::rt::read_repeated_message_into(wire_type, is, &mut self.rows_meta)?;
                },
                _ => {
                    ::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields())?;
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        if let Some(ref v) = self.rows_data.as_ref() {
            my_size += ::protobuf::rt::bytes_size(3, &v);
        }
        for value in &self.rows_meta {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(ref v) = self.rows_data.as_ref() {
            os.write_bytes(3, &v)?;
        }
        for v in &self.rows_meta {
            os.write_tag(4, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        };
        os.write_unknown_fields(self.get_unknown_fields())?;
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }
    fn as_any_mut(&mut self) -> &mut ::std::any::Any {
        self as &mut ::std::any::Any
    }
    fn into_any(self: Box<Self>) -> ::std::boxed::Box<::std::any::Any> {
        self
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for Chunk {
    fn new() -> Chunk {
        Chunk::new()
    }

    fn descriptor_static(_: ::std::option::Option<Chunk>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_field_accessor::<_, ::protobuf::types::ProtobufTypeBytes>(
                    "rows_data",
                    Chunk::get_rows_data_for_reflect,
                    Chunk::mut_rows_data_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_repeated_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<RowMeta>>(
                    "rows_meta",
                    Chunk::get_rows_meta_for_reflect,
                    Chunk::mut_rows_meta_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<Chunk>(
                    "Chunk",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for Chunk {
    fn clear(&mut self) {
        self.clear_rows_data();
        self.clear_rows_meta();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for Chunk {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for Chunk {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct RowMeta {
    // message fields
    handle: ::std::option::Option<i64>,
    length: ::std::option::Option<i64>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for RowMeta {}

impl RowMeta {
    pub fn new() -> RowMeta {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static RowMeta {
        static mut instance: ::protobuf::lazy::Lazy<RowMeta> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const RowMeta,
        };
        unsafe {
            instance.get(RowMeta::new)
        }
    }

    // optional int64 handle = 1;

    pub fn clear_handle(&mut self) {
        self.handle = ::std::option::Option::None;
    }

    pub fn has_handle(&self) -> bool {
        self.handle.is_some()
    }

    // Param is passed by value, moved
    pub fn set_handle(&mut self, v: i64) {
        self.handle = ::std::option::Option::Some(v);
    }

    pub fn get_handle(&self) -> i64 {
        self.handle.unwrap_or(0)
    }

    fn get_handle_for_reflect(&self) -> &::std::option::Option<i64> {
        &self.handle
    }

    fn mut_handle_for_reflect(&mut self) -> &mut ::std::option::Option<i64> {
        &mut self.handle
    }

    // optional int64 length = 2;

    pub fn clear_length(&mut self) {
        self.length = ::std::option::Option::None;
    }

    pub fn has_length(&self) -> bool {
        self.length.is_some()
    }

    // Param is passed by value, moved
    pub fn set_length(&mut self, v: i64) {
        self.length = ::std::option::Option::Some(v);
    }

    pub fn get_length(&self) -> i64 {
        self.length.unwrap_or(0)
    }

    fn get_length_for_reflect(&self) -> &::std::option::Option<i64> {
        &self.length
    }

    fn mut_length_for_reflect(&mut self) -> &mut ::std::option::Option<i64> {
        &mut self.length
    }
}

impl ::protobuf::Message for RowMeta {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !is.eof()? {
            let (field_number, wire_type) = is.read_tag_unpack()?;
            match field_number {
                1 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_int64()?;
                    self.handle = ::std::option::Option::Some(tmp);
                },
                2 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_int64()?;
                    self.length = ::std::option::Option::Some(tmp);
                },
                _ => {
                    ::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields())?;
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        if let Some(v) = self.handle {
            my_size += ::protobuf::rt::value_size(1, v, ::protobuf::wire_format::WireTypeVarint);
        }
        if let Some(v) = self.length {
            my_size += ::protobuf::rt::value_size(2, v, ::protobuf::wire_format::WireTypeVarint);
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.handle {
            os.write_int64(1, v)?;
        }
        if let Some(v) = self.length {
            os.write_int64(2, v)?;
        }
        os.write_unknown_fields(self.get_unknown_fields())?;
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }
    fn as_any_mut(&mut self) -> &mut ::std::any::Any {
        self as &mut ::std::any::Any
    }
    fn into_any(self: Box<Self>) -> ::std::boxed::Box<::std::any::Any> {
        self
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for RowMeta {
    fn new() -> RowMeta {
        RowMeta::new()
    }

    fn descriptor_static(_: ::std::option::Option<RowMeta>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeInt64>(
                    "handle",
                    RowMeta::get_handle_for_reflect,
                    RowMeta::mut_handle_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeInt64>(
                    "length",
                    RowMeta::get_length_for_reflect,
                    RowMeta::mut_length_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<RowMeta>(
                    "RowMeta",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for RowMeta {
    fn clear(&mut self) {
        self.clear_handle();
        self.clear_length();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for RowMeta {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for RowMeta {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct DAGRequest {
    // message fields
    start_ts: ::std::option::Option<u64>,
    executors: ::protobuf::RepeatedField<super::executor::Executor>,
    time_zone_offset: ::std::option::Option<i64>,
    flags: ::std::option::Option<u64>,
    output_offsets: ::std::vec::Vec<u32>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for DAGRequest {}

impl DAGRequest {
    pub fn new() -> DAGRequest {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static DAGRequest {
        static mut instance: ::protobuf::lazy::Lazy<DAGRequest> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const DAGRequest,
        };
        unsafe {
            instance.get(DAGRequest::new)
        }
    }

    // optional uint64 start_ts = 1;

    pub fn clear_start_ts(&mut self) {
        self.start_ts = ::std::option::Option::None;
    }

    pub fn has_start_ts(&self) -> bool {
        self.start_ts.is_some()
    }

    // Param is passed by value, moved
    pub fn set_start_ts(&mut self, v: u64) {
        self.start_ts = ::std::option::Option::Some(v);
    }

    pub fn get_start_ts(&self) -> u64 {
        self.start_ts.unwrap_or(0)
    }

    fn get_start_ts_for_reflect(&self) -> &::std::option::Option<u64> {
        &self.start_ts
    }

    fn mut_start_ts_for_reflect(&mut self) -> &mut ::std::option::Option<u64> {
        &mut self.start_ts
    }

    // repeated .tipb.Executor executors = 2;

    pub fn clear_executors(&mut self) {
        self.executors.clear();
    }

    // Param is passed by value, moved
    pub fn set_executors(&mut self, v: ::protobuf::RepeatedField<super::executor::Executor>) {
        self.executors = v;
    }

    // Mutable pointer to the field.
    pub fn mut_executors(&mut self) -> &mut ::protobuf::RepeatedField<super::executor::Executor> {
        &mut self.executors
    }

    // Take field
    pub fn take_executors(&mut self) -> ::protobuf::RepeatedField<super::executor::Executor> {
        ::std::mem::replace(&mut self.executors, ::protobuf::RepeatedField::new())
    }

    pub fn get_executors(&self) -> &[super::executor::Executor] {
        &self.executors
    }

    fn get_executors_for_reflect(&self) -> &::protobuf::RepeatedField<super::executor::Executor> {
        &self.executors
    }

    fn mut_executors_for_reflect(&mut self) -> &mut ::protobuf::RepeatedField<super::executor::Executor> {
        &mut self.executors
    }

    // optional int64 time_zone_offset = 3;

    pub fn clear_time_zone_offset(&mut self) {
        self.time_zone_offset = ::std::option::Option::None;
    }

    pub fn has_time_zone_offset(&self) -> bool {
        self.time_zone_offset.is_some()
    }

    // Param is passed by value, moved
    pub fn set_time_zone_offset(&mut self, v: i64) {
        self.time_zone_offset = ::std::option::Option::Some(v);
    }

    pub fn get_time_zone_offset(&self) -> i64 {
        self.time_zone_offset.unwrap_or(0)
    }

    fn get_time_zone_offset_for_reflect(&self) -> &::std::option::Option<i64> {
        &self.time_zone_offset
    }

    fn mut_time_zone_offset_for_reflect(&mut self) -> &mut ::std::option::Option<i64> {
        &mut self.time_zone_offset
    }

    // optional uint64 flags = 4;

    pub fn clear_flags(&mut self) {
        self.flags = ::std::option::Option::None;
    }

    pub fn has_flags(&self) -> bool {
        self.flags.is_some()
    }

    // Param is passed by value, moved
    pub fn set_flags(&mut self, v: u64) {
        self.flags = ::std::option::Option::Some(v);
    }

    pub fn get_flags(&self) -> u64 {
        self.flags.unwrap_or(0)
    }

    fn get_flags_for_reflect(&self) -> &::std::option::Option<u64> {
        &self.flags
    }

    fn mut_flags_for_reflect(&mut self) -> &mut ::std::option::Option<u64> {
        &mut self.flags
    }

    // repeated uint32 output_offsets = 5;

    pub fn clear_output_offsets(&mut self) {
        self.output_offsets.clear();
    }

    // Param is passed by value, moved
    pub fn set_output_offsets(&mut self, v: ::std::vec::Vec<u32>) {
        self.output_offsets = v;
    }

    // Mutable pointer to the field.
    pub fn mut_output_offsets(&mut self) -> &mut ::std::vec::Vec<u32> {
        &mut self.output_offsets
    }

    // Take field
    pub fn take_output_offsets(&mut self) -> ::std::vec::Vec<u32> {
        ::std::mem::replace(&mut self.output_offsets, ::std::vec::Vec::new())
    }

    pub fn get_output_offsets(&self) -> &[u32] {
        &self.output_offsets
    }

    fn get_output_offsets_for_reflect(&self) -> &::std::vec::Vec<u32> {
        &self.output_offsets
    }

    fn mut_output_offsets_for_reflect(&mut self) -> &mut ::std::vec::Vec<u32> {
        &mut self.output_offsets
    }
}

impl ::protobuf::Message for DAGRequest {
    fn is_initialized(&self) -> bool {
        for v in &self.executors {
            if !v.is_initialized() {
                return false;
            }
        };
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !is.eof()? {
            let (field_number, wire_type) = is.read_tag_unpack()?;
            match field_number {
                1 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_uint64()?;
                    self.start_ts = ::std::option::Option::Some(tmp);
                },
                2 => {
                    ::protobuf::rt::read_repeated_message_into(wire_type, is, &mut self.executors)?;
                },
                3 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_int64()?;
                    self.time_zone_offset = ::std::option::Option::Some(tmp);
                },
                4 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_uint64()?;
                    self.flags = ::std::option::Option::Some(tmp);
                },
                5 => {
                    ::protobuf::rt::read_repeated_uint32_into(wire_type, is, &mut self.output_offsets)?;
                },
                _ => {
                    ::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields())?;
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        if let Some(v) = self.start_ts {
            my_size += ::protobuf::rt::value_size(1, v, ::protobuf::wire_format::WireTypeVarint);
        }
        for value in &self.executors {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        if let Some(v) = self.time_zone_offset {
            my_size += ::protobuf::rt::value_size(3, v, ::protobuf::wire_format::WireTypeVarint);
        }
        if let Some(v) = self.flags {
            my_size += ::protobuf::rt::value_size(4, v, ::protobuf::wire_format::WireTypeVarint);
        }
        for value in &self.output_offsets {
            my_size += ::protobuf::rt::value_size(5, *value, ::protobuf::wire_format::WireTypeVarint);
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.start_ts {
            os.write_uint64(1, v)?;
        }
        for v in &self.executors {
            os.write_tag(2, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        };
        if let Some(v) = self.time_zone_offset {
            os.write_int64(3, v)?;
        }
        if let Some(v) = self.flags {
            os.write_uint64(4, v)?;
        }
        for v in &self.output_offsets {
            os.write_uint32(5, *v)?;
        };
        os.write_unknown_fields(self.get_unknown_fields())?;
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }
    fn as_any_mut(&mut self) -> &mut ::std::any::Any {
        self as &mut ::std::any::Any
    }
    fn into_any(self: Box<Self>) -> ::std::boxed::Box<::std::any::Any> {
        self
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for DAGRequest {
    fn new() -> DAGRequest {
        DAGRequest::new()
    }

    fn descriptor_static(_: ::std::option::Option<DAGRequest>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "start_ts",
                    DAGRequest::get_start_ts_for_reflect,
                    DAGRequest::mut_start_ts_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_repeated_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<super::executor::Executor>>(
                    "executors",
                    DAGRequest::get_executors_for_reflect,
                    DAGRequest::mut_executors_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeInt64>(
                    "time_zone_offset",
                    DAGRequest::get_time_zone_offset_for_reflect,
                    DAGRequest::mut_time_zone_offset_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "flags",
                    DAGRequest::get_flags_for_reflect,
                    DAGRequest::mut_flags_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_vec_accessor::<_, ::protobuf::types::ProtobufTypeUint32>(
                    "output_offsets",
                    DAGRequest::get_output_offsets_for_reflect,
                    DAGRequest::mut_output_offsets_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<DAGRequest>(
                    "DAGRequest",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for DAGRequest {
    fn clear(&mut self) {
        self.clear_start_ts();
        self.clear_executors();
        self.clear_time_zone_offset();
        self.clear_flags();
        self.clear_output_offsets();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for DAGRequest {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for DAGRequest {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct StreamResponse {
    // message fields
    error: ::protobuf::SingularPtrField<Error>,
    encode_type: ::std::option::Option<EncodeType>,
    data: ::protobuf::SingularField<::std::vec::Vec<u8>>,
    warnings: ::protobuf::RepeatedField<Error>,
    output_counts: ::std::vec::Vec<i64>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for StreamResponse {}

impl StreamResponse {
    pub fn new() -> StreamResponse {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static StreamResponse {
        static mut instance: ::protobuf::lazy::Lazy<StreamResponse> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const StreamResponse,
        };
        unsafe {
            instance.get(StreamResponse::new)
        }
    }

    // optional .tipb.Error error = 1;

    pub fn clear_error(&mut self) {
        self.error.clear();
    }

    pub fn has_error(&self) -> bool {
        self.error.is_some()
    }

    // Param is passed by value, moved
    pub fn set_error(&mut self, v: Error) {
        self.error = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_error(&mut self) -> &mut Error {
        if self.error.is_none() {
            self.error.set_default();
        }
        self.error.as_mut().unwrap()
    }

    // Take field
    pub fn take_error(&mut self) -> Error {
        self.error.take().unwrap_or_else(|| Error::new())
    }

    pub fn get_error(&self) -> &Error {
        self.error.as_ref().unwrap_or_else(|| Error::default_instance())
    }

    fn get_error_for_reflect(&self) -> &::protobuf::SingularPtrField<Error> {
        &self.error
    }

    fn mut_error_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<Error> {
        &mut self.error
    }

    // optional .tipb.EncodeType encode_type = 2;

    pub fn clear_encode_type(&mut self) {
        self.encode_type = ::std::option::Option::None;
    }

    pub fn has_encode_type(&self) -> bool {
        self.encode_type.is_some()
    }

    // Param is passed by value, moved
    pub fn set_encode_type(&mut self, v: EncodeType) {
        self.encode_type = ::std::option::Option::Some(v);
    }

    pub fn get_encode_type(&self) -> EncodeType {
        self.encode_type.unwrap_or(EncodeType::TypeDefault)
    }

    fn get_encode_type_for_reflect(&self) -> &::std::option::Option<EncodeType> {
        &self.encode_type
    }

    fn mut_encode_type_for_reflect(&mut self) -> &mut ::std::option::Option<EncodeType> {
        &mut self.encode_type
    }

    // optional bytes data = 3;

    pub fn clear_data(&mut self) {
        self.data.clear();
    }

    pub fn has_data(&self) -> bool {
        self.data.is_some()
    }

    // Param is passed by value, moved
    pub fn set_data(&mut self, v: ::std::vec::Vec<u8>) {
        self.data = ::protobuf::SingularField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_data(&mut self) -> &mut ::std::vec::Vec<u8> {
        if self.data.is_none() {
            self.data.set_default();
        }
        self.data.as_mut().unwrap()
    }

    // Take field
    pub fn take_data(&mut self) -> ::std::vec::Vec<u8> {
        self.data.take().unwrap_or_else(|| ::std::vec::Vec::new())
    }

    pub fn get_data(&self) -> &[u8] {
        match self.data.as_ref() {
            Some(v) => &v,
            None => &[],
        }
    }

    fn get_data_for_reflect(&self) -> &::protobuf::SingularField<::std::vec::Vec<u8>> {
        &self.data
    }

    fn mut_data_for_reflect(&mut self) -> &mut ::protobuf::SingularField<::std::vec::Vec<u8>> {
        &mut self.data
    }

    // repeated .tipb.Error warnings = 4;

    pub fn clear_warnings(&mut self) {
        self.warnings.clear();
    }

    // Param is passed by value, moved
    pub fn set_warnings(&mut self, v: ::protobuf::RepeatedField<Error>) {
        self.warnings = v;
    }

    // Mutable pointer to the field.
    pub fn mut_warnings(&mut self) -> &mut ::protobuf::RepeatedField<Error> {
        &mut self.warnings
    }

    // Take field
    pub fn take_warnings(&mut self) -> ::protobuf::RepeatedField<Error> {
        ::std::mem::replace(&mut self.warnings, ::protobuf::RepeatedField::new())
    }

    pub fn get_warnings(&self) -> &[Error] {
        &self.warnings
    }

    fn get_warnings_for_reflect(&self) -> &::protobuf::RepeatedField<Error> {
        &self.warnings
    }

    fn mut_warnings_for_reflect(&mut self) -> &mut ::protobuf::RepeatedField<Error> {
        &mut self.warnings
    }

    // repeated int64 output_counts = 5;

    pub fn clear_output_counts(&mut self) {
        self.output_counts.clear();
    }

    // Param is passed by value, moved
    pub fn set_output_counts(&mut self, v: ::std::vec::Vec<i64>) {
        self.output_counts = v;
    }

    // Mutable pointer to the field.
    pub fn mut_output_counts(&mut self) -> &mut ::std::vec::Vec<i64> {
        &mut self.output_counts
    }

    // Take field
    pub fn take_output_counts(&mut self) -> ::std::vec::Vec<i64> {
        ::std::mem::replace(&mut self.output_counts, ::std::vec::Vec::new())
    }

    pub fn get_output_counts(&self) -> &[i64] {
        &self.output_counts
    }

    fn get_output_counts_for_reflect(&self) -> &::std::vec::Vec<i64> {
        &self.output_counts
    }

    fn mut_output_counts_for_reflect(&mut self) -> &mut ::std::vec::Vec<i64> {
        &mut self.output_counts
    }
}

impl ::protobuf::Message for StreamResponse {
    fn is_initialized(&self) -> bool {
        for v in &self.error {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.warnings {
            if !v.is_initialized() {
                return false;
            }
        };
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !is.eof()? {
            let (field_number, wire_type) = is.read_tag_unpack()?;
            match field_number {
                1 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.error)?;
                },
                2 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_enum()?;
                    self.encode_type = ::std::option::Option::Some(tmp);
                },
                3 => {
                    ::protobuf::rt::read_singular_bytes_into(wire_type, is, &mut self.data)?;
                },
                4 => {
                    ::protobuf::rt::read_repeated_message_into(wire_type, is, &mut self.warnings)?;
                },
                5 => {
                    ::protobuf::rt::read_repeated_int64_into(wire_type, is, &mut self.output_counts)?;
                },
                _ => {
                    ::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields())?;
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        if let Some(ref v) = self.error.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if let Some(v) = self.encode_type {
            my_size += ::protobuf::rt::enum_size(2, v);
        }
        if let Some(ref v) = self.data.as_ref() {
            my_size += ::protobuf::rt::bytes_size(3, &v);
        }
        for value in &self.warnings {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        for value in &self.output_counts {
            my_size += ::protobuf::rt::value_size(5, *value, ::protobuf::wire_format::WireTypeVarint);
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(ref v) = self.error.as_ref() {
            os.write_tag(1, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if let Some(v) = self.encode_type {
            os.write_enum(2, v.value())?;
        }
        if let Some(ref v) = self.data.as_ref() {
            os.write_bytes(3, &v)?;
        }
        for v in &self.warnings {
            os.write_tag(4, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        };
        for v in &self.output_counts {
            os.write_int64(5, *v)?;
        };
        os.write_unknown_fields(self.get_unknown_fields())?;
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }
    fn as_any_mut(&mut self) -> &mut ::std::any::Any {
        self as &mut ::std::any::Any
    }
    fn into_any(self: Box<Self>) -> ::std::boxed::Box<::std::any::Any> {
        self
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for StreamResponse {
    fn new() -> StreamResponse {
        StreamResponse::new()
    }

    fn descriptor_static(_: ::std::option::Option<StreamResponse>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<Error>>(
                    "error",
                    StreamResponse::get_error_for_reflect,
                    StreamResponse::mut_error_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeEnum<EncodeType>>(
                    "encode_type",
                    StreamResponse::get_encode_type_for_reflect,
                    StreamResponse::mut_encode_type_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_field_accessor::<_, ::protobuf::types::ProtobufTypeBytes>(
                    "data",
                    StreamResponse::get_data_for_reflect,
                    StreamResponse::mut_data_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_repeated_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<Error>>(
                    "warnings",
                    StreamResponse::get_warnings_for_reflect,
                    StreamResponse::mut_warnings_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_vec_accessor::<_, ::protobuf::types::ProtobufTypeInt64>(
                    "output_counts",
                    StreamResponse::get_output_counts_for_reflect,
                    StreamResponse::mut_output_counts_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<StreamResponse>(
                    "StreamResponse",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for StreamResponse {
    fn clear(&mut self) {
        self.clear_error();
        self.clear_encode_type();
        self.clear_data();
        self.clear_warnings();
        self.clear_output_counts();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for StreamResponse {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for StreamResponse {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(Clone,PartialEq,Eq,Debug,Hash)]
pub enum EncodeType {
    TypeDefault = 0,
    TypeArrow = 1,
}

impl ::protobuf::ProtobufEnum for EncodeType {
    fn value(&self) -> i32 {
        *self as i32
    }

    fn from_i32(value: i32) -> ::std::option::Option<EncodeType> {
        match value {
            0 => ::std::option::Option::Some(EncodeType::TypeDefault),
            1 => ::std::option::Option::Some(EncodeType::TypeArrow),
            _ => ::std::option::Option::None
        }
    }

    fn values() -> &'static [Self] {
        static values: &'static [EncodeType] = &[
            EncodeType::TypeDefault,
            EncodeType::TypeArrow,
        ];
        values
    }

    fn enum_descriptor_static(_: ::std::option::Option<EncodeType>) -> &'static ::protobuf::reflect::EnumDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::EnumDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::EnumDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                ::protobuf::reflect::EnumDescriptor::new("EncodeType", file_descriptor_proto())
            })
        }
    }
}

impl ::std::marker::Copy for EncodeType {
}

impl ::protobuf::reflect::ProtobufValue for EncodeType {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Enum(self.descriptor())
    }
}

static file_descriptor_proto_data: &'static [u8] = b"\
    \n\x0cselect.proto\x12\x04tipb\x1a\x0eexecutor.proto\x1a\x10expression.p\
    roto\x1a\x0cschema.proto\x1a\x14gogoproto/gogo.proto\"\xa4\x04\n\rSelect\
    Request\x12\x1f\n\x08start_ts\x18\x01\x20\x01(\x04R\x07startTsB\x04\xc8\
    \xde\x1f\0\x12.\n\ntable_info\x18\x02\x20\x01(\x0b2\x0f.tipb.TableInfoR\
    \ttableInfo\x12.\n\nindex_info\x18\x03\x20\x01(\x0b2\x0f.tipb.IndexInfoR\
    \tindexInfo\x12\"\n\x06fields\x18\x04\x20\x03(\x0b2\n.tipb.ExprR\x06fiel\
    ds\x12&\n\x06ranges\x18\x05\x20\x03(\x0b2\x0e.tipb.KeyRangeR\x06ranges\
    \x12\x20\n\x08distinct\x18\x06\x20\x01(\x08R\x08distinctB\x04\xc8\xde\
    \x1f\0\x12\x20\n\x05where\x18\x07\x20\x01(\x0b2\n.tipb.ExprR\x05where\
    \x12'\n\x08group_by\x18\x08\x20\x03(\x0b2\x0c.tipb.ByItemR\x07groupBy\
    \x12\"\n\x06having\x18\t\x20\x01(\x0b2\n.tipb.ExprR\x06having\x12'\n\x08\
    order_by\x18\n\x20\x03(\x0b2\x0c.tipb.ByItemR\x07orderBy\x12\x14\n\x05li\
    mit\x18\x0c\x20\x01(\x03R\x05limit\x12*\n\naggregates\x18\r\x20\x03(\x0b\
    2\n.tipb.ExprR\naggregates\x12.\n\x10time_zone_offset\x18\x0e\x20\x01(\
    \x03R\x0etimeZoneOffsetB\x04\xc8\xde\x1f\0\x12\x1a\n\x05flags\x18\x0f\
    \x20\x01(\x04R\x05flagsB\x04\xc8\xde\x1f\0\"1\n\x03Row\x12\x16\n\x06hand\
    le\x18\x01\x20\x01(\x0cR\x06handle\x12\x12\n\x04data\x18\x02\x20\x01(\
    \x0cR\x04data\"9\n\x05Error\x12\x18\n\x04code\x18\x01\x20\x01(\x05R\x04c\
    odeB\x04\xc8\xde\x1f\0\x12\x16\n\x03msg\x18\x02\x20\x01(\tR\x03msgB\x04\
    \xc8\xde\x1f\0\"\xcb\x01\n\x0eSelectResponse\x12!\n\x05error\x18\x01\x20\
    \x01(\x0b2\x0b.tipb.ErrorR\x05error\x12\x1d\n\x04rows\x18\x02\x20\x03(\
    \x0b2\t.tipb.RowR\x04rows\x12)\n\x06chunks\x18\x03\x20\x03(\x0b2\x0b.tip\
    b.ChunkR\x06chunksB\x04\xc8\xde\x1f\0\x12'\n\x08warnings\x18\x04\x20\x03\
    (\x0b2\x0b.tipb.ErrorR\x08warnings\x12#\n\routput_counts\x18\x05\x20\x03\
    (\x03R\x0coutputCounts\"\x8f\x01\n\x05Chunk\x12T\n\trows_data\x18\x03\
    \x20\x01(\x0cR\x08rowsDataB7\xc8\xde\x1f\0\xda\xde\x1f/github.com/pingca\
    p/tipb/sharedbytes.SharedBytes\x120\n\trows_meta\x18\x04\x20\x03(\x0b2\r\
    .tipb.RowMetaR\x08rowsMetaB\x04\xc8\xde\x1f\0\"E\n\x07RowMeta\x12\x1c\n\
    \x06handle\x18\x01\x20\x01(\x03R\x06handleB\x04\xc8\xde\x1f\0\x12\x1c\n\
    \x06length\x18\x02\x20\x01(\x03R\x06lengthB\x04\xc8\xde\x1f\0\"\xce\x01\
    \n\nDAGRequest\x12\x1f\n\x08start_ts\x18\x01\x20\x01(\x04R\x07startTsB\
    \x04\xc8\xde\x1f\0\x12,\n\texecutors\x18\x02\x20\x03(\x0b2\x0e.tipb.Exec\
    utorR\texecutors\x12.\n\x10time_zone_offset\x18\x03\x20\x01(\x03R\x0etim\
    eZoneOffsetB\x04\xc8\xde\x1f\0\x12\x1a\n\x05flags\x18\x04\x20\x01(\x04R\
    \x05flagsB\x04\xc8\xde\x1f\0\x12%\n\x0eoutput_offsets\x18\x05\x20\x03(\r\
    R\routputOffsets\"\x87\x02\n\x0eStreamResponse\x12!\n\x05error\x18\x01\
    \x20\x01(\x0b2\x0b.tipb.ErrorR\x05error\x127\n\x0bencode_type\x18\x02\
    \x20\x01(\x0e2\x10.tipb.EncodeTypeR\nencodeTypeB\x04\xc8\xde\x1f\0\x12K\
    \n\x04data\x18\x03\x20\x01(\x0cR\x04dataB7\xda\xde\x1f/github.com/pingca\
    p/tipb/sharedbytes.SharedBytes\xc8\xde\x1f\0\x12'\n\x08warnings\x18\x04\
    \x20\x03(\x0b2\x0b.tipb.ErrorR\x08warnings\x12#\n\routput_counts\x18\x05\
    \x20\x03(\x03R\x0coutputCounts*,\n\nEncodeType\x12\x0f\n\x0bTypeDefault\
    \x10\0\x12\r\n\tTypeArrow\x10\x01B%\n\x15com.pingcap.tidb.tipbP\x01\xd0\
    \xe2\x1e\x01\xc8\xe2\x1e\x01\xe0\xe2\x1e\x01J\xf67\n\x07\x12\x05\0\0\x94\
    \x01\x01\n\x08\n\x01\x0c\x12\x03\0\0\x12\n\x08\n\x01\x02\x12\x03\x02\x08\
    \x0c\n\x08\n\x01\x08\x12\x03\x04\0\"\n\x0b\n\x04\x08\xe7\x07\0\x12\x03\
    \x04\0\"\n\x0c\n\x05\x08\xe7\x07\0\x02\x12\x03\x04\x07\x1a\n\r\n\x06\x08\
    \xe7\x07\0\x02\0\x12\x03\x04\x07\x1a\n\x0e\n\x07\x08\xe7\x07\0\x02\0\x01\
    \x12\x03\x04\x07\x1a\n\x0c\n\x05\x08\xe7\x07\0\x03\x12\x03\x04\x1d!\n\
    \x08\n\x01\x08\x12\x03\x05\0.\n\x0b\n\x04\x08\xe7\x07\x01\x12\x03\x05\0.\
    \n\x0c\n\x05\x08\xe7\x07\x01\x02\x12\x03\x05\x07\x13\n\r\n\x06\x08\xe7\
    \x07\x01\x02\0\x12\x03\x05\x07\x13\n\x0e\n\x07\x08\xe7\x07\x01\x02\0\x01\
    \x12\x03\x05\x07\x13\n\x0c\n\x05\x08\xe7\x07\x01\x07\x12\x03\x05\x16-\n\
    \t\n\x02\x03\0\x12\x03\x07\x07\x17\n\t\n\x02\x03\x01\x12\x03\x08\x07\x19\
    \n\t\n\x02\x03\x02\x12\x03\t\x07\x15\n\t\n\x02\x03\x03\x12\x03\n\x07\x1d\
    \n\x08\n\x01\x08\x12\x03\x0c\0(\n\x0b\n\x04\x08\xe7\x07\x02\x12\x03\x0c\
    \0(\n\x0c\n\x05\x08\xe7\x07\x02\x02\x12\x03\x0c\x07\x20\n\r\n\x06\x08\
    \xe7\x07\x02\x02\0\x12\x03\x0c\x07\x20\n\x0e\n\x07\x08\xe7\x07\x02\x02\0\
    \x01\x12\x03\x0c\x08\x1f\n\x0c\n\x05\x08\xe7\x07\x02\x03\x12\x03\x0c#'\n\
    \x08\n\x01\x08\x12\x03\r\0$\n\x0b\n\x04\x08\xe7\x07\x03\x12\x03\r\0$\n\
    \x0c\n\x05\x08\xe7\x07\x03\x02\x12\x03\r\x07\x1c\n\r\n\x06\x08\xe7\x07\
    \x03\x02\0\x12\x03\r\x07\x1c\n\x0e\n\x07\x08\xe7\x07\x03\x02\0\x01\x12\
    \x03\r\x08\x1b\n\x0c\n\x05\x08\xe7\x07\x03\x03\x12\x03\r\x1f#\n\x08\n\
    \x01\x08\x12\x03\x0e\0*\n\x0b\n\x04\x08\xe7\x07\x04\x12\x03\x0e\0*\n\x0c\
    \n\x05\x08\xe7\x07\x04\x02\x12\x03\x0e\x07\"\n\r\n\x06\x08\xe7\x07\x04\
    \x02\0\x12\x03\x0e\x07\"\n\x0e\n\x07\x08\xe7\x07\x04\x02\0\x01\x12\x03\
    \x0e\x08!\n\x0c\n\x05\x08\xe7\x07\x04\x03\x12\x03\x0e%)\nE\n\x02\x04\0\
    \x12\x04\x11\0B\x01\x1a9\x20SelectRequest\x20works\x20like\x20a\x20simpl\
    ified\x20select\x20statement.\n\n\n\n\x03\x04\0\x01\x12\x03\x11\x08\x15\
    \n+\n\x04\x04\0\x02\0\x12\x03\x13\x08D\x1a\x1e\x20transaction\x20start\
    \x20timestamp.\n\n\x0c\n\x05\x04\0\x02\0\x04\x12\x03\x13\x08\x10\n\x0c\n\
    \x05\x04\0\x02\0\x05\x12\x03\x13\x11\x17\n\x0c\n\x05\x04\0\x02\0\x01\x12\
    \x03\x13\x18\x20\n\x0c\n\x05\x04\0\x02\0\x03\x12\x03\x13#$\n\x0c\n\x05\
    \x04\0\x02\0\x08\x12\x03\x13%C\n\x0f\n\x08\x04\0\x02\0\x08\xe7\x07\0\x12\
    \x03\x13&B\n\x10\n\t\x04\0\x02\0\x08\xe7\x07\0\x02\x12\x03\x13&:\n\x11\n\
    \n\x04\0\x02\0\x08\xe7\x07\0\x02\0\x12\x03\x13&:\n\x12\n\x0b\x04\0\x02\0\
    \x08\xe7\x07\0\x02\0\x01\x12\x03\x13'9\n\x10\n\t\x04\0\x02\0\x08\xe7\x07\
    \0\x03\x12\x03\x13=B\n_\n\x04\x04\0\x02\x01\x12\x03\x16\x08*\x1aR\x20If\
    \x20table_info\x20is\x20not\x20null,\x20it\x20represents\x20a\x20table\
    \x20scan,\x20index_info\x20would\x20be\x20null.\n\n\x0c\n\x05\x04\0\x02\
    \x01\x04\x12\x03\x16\x08\x10\n\x0c\n\x05\x04\0\x02\x01\x06\x12\x03\x16\
    \x11\x1a\n\x0c\n\x05\x04\0\x02\x01\x01\x12\x03\x16\x1b%\n\x0c\n\x05\x04\
    \0\x02\x01\x03\x12\x03\x16()\n`\n\x04\x04\0\x02\x02\x12\x03\x19\x08*\x1a\
    S\x20If\x20index_info\x20is\x20not\x20null,\x20it\x20represents\x20an\
    \x20index\x20scan,\x20table_info\x20would\x20be\x20null.\n\n\x0c\n\x05\
    \x04\0\x02\x02\x04\x12\x03\x19\x08\x10\n\x0c\n\x05\x04\0\x02\x02\x06\x12\
    \x03\x19\x11\x1a\n\x0c\n\x05\x04\0\x02\x02\x01\x12\x03\x19\x1b%\n\x0c\n\
    \x05\x04\0\x02\x02\x03\x12\x03\x19()\n\xab\x01\n\x04\x04\0\x02\x03\x12\
    \x03\x1d\x08!\x1a\x9d\x01\x20fields\x20to\x20be\x20selected,\x20fields\
    \x20type\x20can\x20be\x20column\x20reference\x20for\x20simple\x20scan.\n\
    \x20or\x20aggregation\x20function.\x20If\x20no\x20fields\x20specified,\
    \x20only\x20handle\x20will\x20be\x20returned.\n\n\x0c\n\x05\x04\0\x02\
    \x03\x04\x12\x03\x1d\x08\x10\n\x0c\n\x05\x04\0\x02\x03\x06\x12\x03\x1d\
    \x11\x15\n\x0c\n\x05\x04\0\x02\x03\x01\x12\x03\x1d\x16\x1c\n\x0c\n\x05\
    \x04\0\x02\x03\x03\x12\x03\x1d\x1f\x20\n4\n\x04\x04\0\x02\x04\x12\x03\
    \x20\x08%\x1a'\x20disjoint\x20handle\x20ranges\x20to\x20be\x20scanned.\n\
    \n\x0c\n\x05\x04\0\x02\x04\x04\x12\x03\x20\x08\x10\n\x0c\n\x05\x04\0\x02\
    \x04\x06\x12\x03\x20\x11\x19\n\x0c\n\x05\x04\0\x02\x04\x01\x12\x03\x20\
    \x1a\x20\n\x0c\n\x05\x04\0\x02\x04\x03\x12\x03\x20#$\n\x1f\n\x04\x04\0\
    \x02\x05\x12\x03#\x08B\x1a\x12\x20distinct\x20result.\n\n\x0c\n\x05\x04\
    \0\x02\x05\x04\x12\x03#\x08\x10\n\x0c\n\x05\x04\0\x02\x05\x05\x12\x03#\
    \x11\x15\n\x0c\n\x05\x04\0\x02\x05\x01\x12\x03#\x16\x1e\n\x0c\n\x05\x04\
    \0\x02\x05\x03\x12\x03#!\"\n\x0c\n\x05\x04\0\x02\x05\x08\x12\x03##A\n\
    \x0f\n\x08\x04\0\x02\x05\x08\xe7\x07\0\x12\x03#$@\n\x10\n\t\x04\0\x02\
    \x05\x08\xe7\x07\0\x02\x12\x03#$8\n\x11\n\n\x04\0\x02\x05\x08\xe7\x07\0\
    \x02\0\x12\x03#$8\n\x12\n\x0b\x04\0\x02\x05\x08\xe7\x07\0\x02\0\x01\x12\
    \x03#%7\n\x10\n\t\x04\0\x02\x05\x08\xe7\x07\0\x03\x12\x03#;@\n\x1f\n\x04\
    \x04\0\x02\x06\x12\x03&\x08\x20\x1a\x12\x20where\x20condition.\n\n\x0c\n\
    \x05\x04\0\x02\x06\x04\x12\x03&\x08\x10\n\x0c\n\x05\x04\0\x02\x06\x06\
    \x12\x03&\x11\x15\n\x0c\n\x05\x04\0\x02\x06\x01\x12\x03&\x16\x1b\n\x0c\n\
    \x05\x04\0\x02\x06\x03\x12\x03&\x1e\x1f\n\x1f\n\x04\x04\0\x02\x07\x12\
    \x03)\x08%\x1a\x12\x20group\x20by\x20clause.\n\n\x0c\n\x05\x04\0\x02\x07\
    \x04\x12\x03)\x08\x10\n\x0c\n\x05\x04\0\x02\x07\x06\x12\x03)\x11\x17\n\
    \x0c\n\x05\x04\0\x02\x07\x01\x12\x03)\x18\x20\n\x0c\n\x05\x04\0\x02\x07\
    \x03\x12\x03)#$\n\x1d\n\x04\x04\0\x02\x08\x12\x03,\x08!\x1a\x10\x20havin\
    g\x20clause.\n\n\x0c\n\x05\x04\0\x02\x08\x04\x12\x03,\x08\x10\n\x0c\n\
    \x05\x04\0\x02\x08\x06\x12\x03,\x11\x15\n\x0c\n\x05\x04\0\x02\x08\x01\
    \x12\x03,\x16\x1c\n\x0c\n\x05\x04\0\x02\x08\x03\x12\x03,\x1f\x20\n\x1f\n\
    \x04\x04\0\x02\t\x12\x03/\x08&\x1a\x12\x20order\x20by\x20clause.\n\n\x0c\
    \n\x05\x04\0\x02\t\x04\x12\x03/\x08\x10\n\x0c\n\x05\x04\0\x02\t\x06\x12\
    \x03/\x11\x17\n\x0c\n\x05\x04\0\x02\t\x01\x12\x03/\x18\x20\n\x0c\n\x05\
    \x04\0\x02\t\x03\x12\x03/#%\n/\n\x04\x04\0\x02\n\x12\x032\x08\"\x1a\"\
    \x20limit\x20the\x20result\x20to\x20be\x20returned.\n\n\x0c\n\x05\x04\0\
    \x02\n\x04\x12\x032\x08\x10\n\x0c\n\x05\x04\0\x02\n\x05\x12\x032\x11\x16\
    \n\x0c\n\x05\x04\0\x02\n\x01\x12\x032\x17\x1c\n\x0c\n\x05\x04\0\x02\n\
    \x03\x12\x032\x1f!\n\"\n\x04\x04\0\x02\x0b\x12\x035\x08&\x1a\x15\x20aggr\
    egate\x20functions\n\n\x0c\n\x05\x04\0\x02\x0b\x04\x12\x035\x08\x10\n\
    \x0c\n\x05\x04\0\x02\x0b\x06\x12\x035\x11\x15\n\x0c\n\x05\x04\0\x02\x0b\
    \x01\x12\x035\x16\x20\n\x0c\n\x05\x04\0\x02\x0b\x03\x12\x035#%\n*\n\x04\
    \x04\0\x02\x0c\x12\x038\x08L\x1a\x1d\x20time\x20zone\x20offset\x20in\x20\
    seconds\n\n\x0c\n\x05\x04\0\x02\x0c\x04\x12\x038\x08\x10\n\x0c\n\x05\x04\
    \0\x02\x0c\x05\x12\x038\x11\x16\n\x0c\n\x05\x04\0\x02\x0c\x01\x12\x038\
    \x17'\n\x0c\n\x05\x04\0\x02\x0c\x03\x12\x038*,\n\x0c\n\x05\x04\0\x02\x0c\
    \x08\x12\x038-K\n\x0f\n\x08\x04\0\x02\x0c\x08\xe7\x07\0\x12\x038.J\n\x10\
    \n\t\x04\0\x02\x0c\x08\xe7\x07\0\x02\x12\x038.B\n\x11\n\n\x04\0\x02\x0c\
    \x08\xe7\x07\0\x02\0\x12\x038.B\n\x12\n\x0b\x04\0\x02\x0c\x08\xe7\x07\0\
    \x02\0\x01\x12\x038/A\n\x10\n\t\x04\0\x02\x0c\x08\xe7\x07\0\x03\x12\x038\
    EJ\n\xab\x02\n\x04\x04\0\x02\r\x12\x03A\x08B\x1a\x9d\x02\x20flags\x20is\
    \x20used\x20to\x20store\x20flags\x20that\x20change\x20the\x20execution\
    \x20mode,\x20it\x20contains:\n\tignore_truncate\x20=\x201\n\t\ttruncate\
    \x20error\x20should\x20be\x20ignore\x20if\x20set.\n\ttruncate_as_warning\
    \x20=\x201\x20<<\x201\n\t\twhen\x20ignored_truncate\x20is\x20not\x20set,\
    \x20return\x20warning\x20instead\x20of\x20error\x20if\x20this\x20flag\
    \x20is\x20set.\n\t...\n\tadd\x20more\x20when\x20needed.\n\n\x0c\n\x05\
    \x04\0\x02\r\x04\x12\x03A\x08\x10\n\x0c\n\x05\x04\0\x02\r\x05\x12\x03A\
    \x11\x17\n\x0c\n\x05\x04\0\x02\r\x01\x12\x03A\x18\x1d\n\x0c\n\x05\x04\0\
    \x02\r\x03\x12\x03A\x20\"\n\x0c\n\x05\x04\0\x02\r\x08\x12\x03A#A\n\x0f\n\
    \x08\x04\0\x02\r\x08\xe7\x07\0\x12\x03A$@\n\x10\n\t\x04\0\x02\r\x08\xe7\
    \x07\0\x02\x12\x03A$8\n\x11\n\n\x04\0\x02\r\x08\xe7\x07\0\x02\0\x12\x03A\
    $8\n\x12\n\x0b\x04\0\x02\r\x08\xe7\x07\0\x02\0\x01\x12\x03A%7\n\x10\n\t\
    \x04\0\x02\r\x08\xe7\x07\0\x03\x12\x03A;@\n,\n\x02\x04\x01\x12\x04E\0H\
    \x01\x1a\x20\x20values\x20are\x20all\x20in\x20text\x20format.\n\n\n\n\
    \x03\x04\x01\x01\x12\x03E\x08\x0b\n\x0b\n\x04\x04\x01\x02\0\x12\x03F\x08\
    \"\n\x0c\n\x05\x04\x01\x02\0\x04\x12\x03F\x08\x10\n\x0c\n\x05\x04\x01\
    \x02\0\x05\x12\x03F\x11\x16\n\x0c\n\x05\x04\x01\x02\0\x01\x12\x03F\x17\
    \x1d\n\x0c\n\x05\x04\x01\x02\0\x03\x12\x03F\x20!\n\x0b\n\x04\x04\x01\x02\
    \x01\x12\x03G\x08\x20\n\x0c\n\x05\x04\x01\x02\x01\x04\x12\x03G\x08\x10\n\
    \x0c\n\x05\x04\x01\x02\x01\x05\x12\x03G\x11\x16\n\x0c\n\x05\x04\x01\x02\
    \x01\x01\x12\x03G\x17\x1b\n\x0c\n\x05\x04\x01\x02\x01\x03\x12\x03G\x1e\
    \x1f\n\n\n\x02\x04\x02\x12\x04J\0N\x01\n\n\n\x03\x04\x02\x01\x12\x03J\
    \x08\r\n\x0b\n\x04\x04\x02\x02\0\x12\x03K\x08?\n\x0c\n\x05\x04\x02\x02\0\
    \x04\x12\x03K\x08\x10\n\x0c\n\x05\x04\x02\x02\0\x05\x12\x03K\x11\x16\n\
    \x0c\n\x05\x04\x02\x02\0\x01\x12\x03K\x17\x1b\n\x0c\n\x05\x04\x02\x02\0\
    \x03\x12\x03K\x1e\x1f\n\x0c\n\x05\x04\x02\x02\0\x08\x12\x03K\x20>\n\x0f\
    \n\x08\x04\x02\x02\0\x08\xe7\x07\0\x12\x03K!=\n\x10\n\t\x04\x02\x02\0\
    \x08\xe7\x07\0\x02\x12\x03K!5\n\x11\n\n\x04\x02\x02\0\x08\xe7\x07\0\x02\
    \0\x12\x03K!5\n\x12\n\x0b\x04\x02\x02\0\x08\xe7\x07\0\x02\0\x01\x12\x03K\
    \"4\n\x10\n\t\x04\x02\x02\0\x08\xe7\x07\0\x03\x12\x03K8=\n\x0b\n\x04\x04\
    \x02\x02\x01\x12\x03M\x08?\n\x0c\n\x05\x04\x02\x02\x01\x04\x12\x03M\x08\
    \x10\n\x0c\n\x05\x04\x02\x02\x01\x05\x12\x03M\x11\x17\n\x0c\n\x05\x04\
    \x02\x02\x01\x01\x12\x03M\x18\x1b\n\x0c\n\x05\x04\x02\x02\x01\x03\x12\
    \x03M\x1e\x1f\n\x0c\n\x05\x04\x02\x02\x01\x08\x12\x03M\x20>\n\x0f\n\x08\
    \x04\x02\x02\x01\x08\xe7\x07\0\x12\x03M!=\n\x10\n\t\x04\x02\x02\x01\x08\
    \xe7\x07\0\x02\x12\x03M!5\n\x11\n\n\x04\x02\x02\x01\x08\xe7\x07\0\x02\0\
    \x12\x03M!5\n\x12\n\x0b\x04\x02\x02\x01\x08\xe7\x07\0\x02\0\x01\x12\x03M\
    \"4\n\x10\n\t\x04\x02\x02\x01\x08\xe7\x07\0\x03\x12\x03M8=\n)\n\x02\x04\
    \x03\x12\x04Q\0^\x01\x1a\x1d\x20Response\x20for\x20SelectRequest.\n\n\n\
    \n\x03\x04\x03\x01\x12\x03Q\x08\x16\n\x0b\n\x04\x04\x03\x02\0\x12\x03R\
    \x08!\n\x0c\n\x05\x04\x03\x02\0\x04\x12\x03R\x08\x10\n\x0c\n\x05\x04\x03\
    \x02\0\x06\x12\x03R\x11\x16\n\x0c\n\x05\x04\x03\x02\0\x01\x12\x03R\x17\
    \x1c\n\x0c\n\x05\x04\x03\x02\0\x03\x12\x03R\x1f\x20\n\x1b\n\x04\x04\x03\
    \x02\x01\x12\x03U\x08\x1e\x1a\x0e\x20Result\x20rows.\n\n\x0c\n\x05\x04\
    \x03\x02\x01\x04\x12\x03U\x08\x10\n\x0c\n\x05\x04\x03\x02\x01\x06\x12\
    \x03U\x11\x14\n\x0c\n\x05\x04\x03\x02\x01\x01\x12\x03U\x15\x19\n\x0c\n\
    \x05\x04\x03\x02\x01\x03\x12\x03U\x1c\x1d\nm\n\x04\x04\x03\x02\x02\x12\
    \x03Y\x08A\x1a`\x20Use\x20multiple\x20chunks\x20to\x20reduce\x20memory\
    \x20allocation\x20and\n\x20avoid\x20allocating\x20large\x20contiguous\
    \x20memory.\n\n\x0c\n\x05\x04\x03\x02\x02\x04\x12\x03Y\x08\x10\n\x0c\n\
    \x05\x04\x03\x02\x02\x06\x12\x03Y\x11\x16\n\x0c\n\x05\x04\x03\x02\x02\
    \x01\x12\x03Y\x17\x1d\n\x0c\n\x05\x04\x03\x02\x02\x03\x12\x03Y\x20!\n\
    \x0c\n\x05\x04\x03\x02\x02\x08\x12\x03Y\"@\n\x0f\n\x08\x04\x03\x02\x02\
    \x08\xe7\x07\0\x12\x03Y#?\n\x10\n\t\x04\x03\x02\x02\x08\xe7\x07\0\x02\
    \x12\x03Y#7\n\x11\n\n\x04\x03\x02\x02\x08\xe7\x07\0\x02\0\x12\x03Y#7\n\
    \x12\n\x0b\x04\x03\x02\x02\x08\xe7\x07\0\x02\0\x01\x12\x03Y$6\n\x10\n\t\
    \x04\x03\x02\x02\x08\xe7\x07\0\x03\x12\x03Y:?\n\x0b\n\x04\x04\x03\x02\
    \x03\x12\x03[\x08$\n\x0c\n\x05\x04\x03\x02\x03\x04\x12\x03[\x08\x10\n\
    \x0c\n\x05\x04\x03\x02\x03\x06\x12\x03[\x11\x16\n\x0c\n\x05\x04\x03\x02\
    \x03\x01\x12\x03[\x17\x1f\n\x0c\n\x05\x04\x03\x02\x03\x03\x12\x03[\"#\n\
    \x0b\n\x04\x04\x03\x02\x04\x12\x03]\x08)\n\x0c\n\x05\x04\x03\x02\x04\x04\
    \x12\x03]\x08\x10\n\x0c\n\x05\x04\x03\x02\x04\x05\x12\x03]\x11\x16\n\x0c\
    \n\x05\x04\x03\x02\x04\x01\x12\x03]\x17$\n\x0c\n\x05\x04\x03\x02\x04\x03\
    \x12\x03]'(\n>\n\x02\x04\x04\x12\x04a\0g\x01\x1a2\x20Chunk\x20contains\
    \x20multiple\x20rows\x20data\x20and\x20rows\x20meta.\n\n\n\n\x03\x04\x04\
    \x01\x12\x03a\x08\r\n/\n\x04\x04\x04\x02\0\x12\x04c\x08\x90\x01\x1a!\x20\
    Data\x20for\x20all\x20rows\x20in\x20the\x20chunk.\n\n\x0c\n\x05\x04\x04\
    \x02\0\x04\x12\x03c\x08\x10\n\x0c\n\x05\x04\x04\x02\0\x05\x12\x03c\x11\
    \x16\n\x0c\n\x05\x04\x04\x02\0\x01\x12\x03c\x17\x20\n\x0c\n\x05\x04\x04\
    \x02\0\x03\x12\x03c#$\n\r\n\x05\x04\x04\x02\0\x08\x12\x04c%\x8f\x01\n\
    \x0f\n\x08\x04\x04\x02\0\x08\xe7\x07\0\x12\x03c&p\n\x10\n\t\x04\x04\x02\
    \0\x08\xe7\x07\0\x02\x12\x03c&<\n\x11\n\n\x04\x04\x02\0\x08\xe7\x07\0\
    \x02\0\x12\x03c&<\n\x12\n\x0b\x04\x04\x02\0\x08\xe7\x07\0\x02\0\x01\x12\
    \x03c';\n\x10\n\t\x04\x04\x02\0\x08\xe7\x07\0\x07\x12\x03c?p\n\x10\n\x08\
    \x04\x04\x02\0\x08\xe7\x07\x01\x12\x04cr\x8e\x01\n\x11\n\t\x04\x04\x02\0\
    \x08\xe7\x07\x01\x02\x12\x04cr\x86\x01\n\x12\n\n\x04\x04\x02\0\x08\xe7\
    \x07\x01\x02\0\x12\x04cr\x86\x01\n\x13\n\x0b\x04\x04\x02\0\x08\xe7\x07\
    \x01\x02\0\x01\x12\x04cs\x85\x01\n\x12\n\t\x04\x04\x02\0\x08\xe7\x07\x01\
    \x03\x12\x05c\x89\x01\x8e\x01\n'\n\x04\x04\x04\x02\x01\x12\x03f\x08F\x1a\
    \x1a\x20Meta\x20data\x20for\x20every\x20row.\n\n\x0c\n\x05\x04\x04\x02\
    \x01\x04\x12\x03f\x08\x10\n\x0c\n\x05\x04\x04\x02\x01\x06\x12\x03f\x11\
    \x18\n\x0c\n\x05\x04\x04\x02\x01\x01\x12\x03f\x19\"\n\x0c\n\x05\x04\x04\
    \x02\x01\x03\x12\x03f%&\n\x0c\n\x05\x04\x04\x02\x01\x08\x12\x03f'E\n\x0f\
    \n\x08\x04\x04\x02\x01\x08\xe7\x07\0\x12\x03f(D\n\x10\n\t\x04\x04\x02\
    \x01\x08\xe7\x07\0\x02\x12\x03f(<\n\x11\n\n\x04\x04\x02\x01\x08\xe7\x07\
    \0\x02\0\x12\x03f(<\n\x12\n\x0b\x04\x04\x02\x01\x08\xe7\x07\0\x02\0\x01\
    \x12\x03f);\n\x10\n\t\x04\x04\x02\x01\x08\xe7\x07\0\x03\x12\x03f?D\n>\n\
    \x02\x04\x05\x12\x04j\0m\x01\x1a2\x20RowMeta\x20contains\x20row\x20handl\
    e\x20and\x20length\x20of\x20a\x20row.\n\n\n\n\x03\x04\x05\x01\x12\x03j\
    \x08\x0f\n\x0b\n\x04\x04\x05\x02\0\x12\x03k\x08A\n\x0c\n\x05\x04\x05\x02\
    \0\x04\x12\x03k\x08\x10\n\x0c\n\x05\x04\x05\x02\0\x05\x12\x03k\x11\x16\n\
    \x0c\n\x05\x04\x05\x02\0\x01\x12\x03k\x17\x1d\n\x0c\n\x05\x04\x05\x02\0\
    \x03\x12\x03k\x20!\n\x0c\n\x05\x04\x05\x02\0\x08\x12\x03k\"@\n\x0f\n\x08\
    \x04\x05\x02\0\x08\xe7\x07\0\x12\x03k#?\n\x10\n\t\x04\x05\x02\0\x08\xe7\
    \x07\0\x02\x12\x03k#7\n\x11\n\n\x04\x05\x02\0\x08\xe7\x07\0\x02\0\x12\
    \x03k#7\n\x12\n\x0b\x04\x05\x02\0\x08\xe7\x07\0\x02\0\x01\x12\x03k$6\n\
    \x10\n\t\x04\x05\x02\0\x08\xe7\x07\0\x03\x12\x03k:?\n\x0b\n\x04\x04\x05\
    \x02\x01\x12\x03l\x08A\n\x0c\n\x05\x04\x05\x02\x01\x04\x12\x03l\x08\x10\
    \n\x0c\n\x05\x04\x05\x02\x01\x05\x12\x03l\x11\x16\n\x0c\n\x05\x04\x05\
    \x02\x01\x01\x12\x03l\x17\x1d\n\x0c\n\x05\x04\x05\x02\x01\x03\x12\x03l\
    \x20!\n\x0c\n\x05\x04\x05\x02\x01\x08\x12\x03l\"@\n\x0f\n\x08\x04\x05\
    \x02\x01\x08\xe7\x07\0\x12\x03l#?\n\x10\n\t\x04\x05\x02\x01\x08\xe7\x07\
    \0\x02\x12\x03l#7\n\x11\n\n\x04\x05\x02\x01\x08\xe7\x07\0\x02\0\x12\x03l\
    #7\n\x12\n\x0b\x04\x05\x02\x01\x08\xe7\x07\0\x02\0\x01\x12\x03l$6\n\x10\
    \n\t\x04\x05\x02\x01\x08\xe7\x07\0\x03\x12\x03l:?\nT\n\x02\x04\x06\x12\
    \x05p\0\x85\x01\x01\x1aG\x20DAGRequest\x20represents\x20the\x20request\
    \x20that\x20will\x20be\x20handled\x20with\x20DAG\x20mode.\n\n\n\n\x03\
    \x04\x06\x01\x12\x03p\x08\x12\n+\n\x04\x04\x06\x02\0\x12\x03r\x08D\x1a\
    \x1e\x20Transaction\x20start\x20timestamp.\n\n\x0c\n\x05\x04\x06\x02\0\
    \x04\x12\x03r\x08\x10\n\x0c\n\x05\x04\x06\x02\0\x05\x12\x03r\x11\x17\n\
    \x0c\n\x05\x04\x06\x02\0\x01\x12\x03r\x18\x20\n\x0c\n\x05\x04\x06\x02\0\
    \x03\x12\x03r#$\n\x0c\n\x05\x04\x06\x02\0\x08\x12\x03r%C\n\x0f\n\x08\x04\
    \x06\x02\0\x08\xe7\x07\0\x12\x03r&B\n\x10\n\t\x04\x06\x02\0\x08\xe7\x07\
    \0\x02\x12\x03r&:\n\x11\n\n\x04\x06\x02\0\x08\xe7\x07\0\x02\0\x12\x03r&:\
    \n\x12\n\x0b\x04\x06\x02\0\x08\xe7\x07\0\x02\0\x01\x12\x03r'9\n\x10\n\t\
    \x04\x06\x02\0\x08\xe7\x07\0\x03\x12\x03r=B\n1\n\x04\x04\x06\x02\x01\x12\
    \x03u\x08(\x1a$\x20It\x20represents\x20push\x20down\x20Executors.\n\n\
    \x0c\n\x05\x04\x06\x02\x01\x04\x12\x03u\x08\x10\n\x0c\n\x05\x04\x06\x02\
    \x01\x06\x12\x03u\x11\x19\n\x0c\n\x05\x04\x06\x02\x01\x01\x12\x03u\x1a#\
    \n\x0c\n\x05\x04\x06\x02\x01\x03\x12\x03u&'\n*\n\x04\x04\x06\x02\x02\x12\
    \x03x\x08K\x1a\x1d\x20time\x20zone\x20offset\x20in\x20seconds\n\n\x0c\n\
    \x05\x04\x06\x02\x02\x04\x12\x03x\x08\x10\n\x0c\n\x05\x04\x06\x02\x02\
    \x05\x12\x03x\x11\x16\n\x0c\n\x05\x04\x06\x02\x02\x01\x12\x03x\x17'\n\
    \x0c\n\x05\x04\x06\x02\x02\x03\x12\x03x*+\n\x0c\n\x05\x04\x06\x02\x02\
    \x08\x12\x03x,J\n\x0f\n\x08\x04\x06\x02\x02\x08\xe7\x07\0\x12\x03x-I\n\
    \x10\n\t\x04\x06\x02\x02\x08\xe7\x07\0\x02\x12\x03x-A\n\x11\n\n\x04\x06\
    \x02\x02\x08\xe7\x07\0\x02\0\x12\x03x-A\n\x12\n\x0b\x04\x06\x02\x02\x08\
    \xe7\x07\0\x02\0\x01\x12\x03x.@\n\x10\n\t\x04\x06\x02\x02\x08\xe7\x07\0\
    \x03\x12\x03xDI\n\xad\x02\n\x04\x04\x06\x02\x03\x12\x04\x81\x01\x08A\x1a\
    \x9e\x02\x20flags\x20are\x20used\x20to\x20store\x20flags\x20that\x20chan\
    ge\x20the\x20execution\x20mode,\x20it\x20contains:\n\tignore_truncate\
    \x20=\x201\n\t\ttruncate\x20error\x20should\x20be\x20ignore\x20if\x20set\
    .\n\ttruncate_as_warning\x20=\x201\x20<<\x201\n\t\twhen\x20ignored_trunc\
    ate\x20is\x20not\x20set,\x20return\x20warning\x20instead\x20of\x20error\
    \x20if\x20this\x20flag\x20is\x20set.\n\t...\n\tadd\x20more\x20when\x20ne\
    eded.\n\n\r\n\x05\x04\x06\x02\x03\x04\x12\x04\x81\x01\x08\x10\n\r\n\x05\
    \x04\x06\x02\x03\x05\x12\x04\x81\x01\x11\x17\n\r\n\x05\x04\x06\x02\x03\
    \x01\x12\x04\x81\x01\x18\x1d\n\r\n\x05\x04\x06\x02\x03\x03\x12\x04\x81\
    \x01\x20!\n\r\n\x05\x04\x06\x02\x03\x08\x12\x04\x81\x01\"@\n\x10\n\x08\
    \x04\x06\x02\x03\x08\xe7\x07\0\x12\x04\x81\x01#?\n\x11\n\t\x04\x06\x02\
    \x03\x08\xe7\x07\0\x02\x12\x04\x81\x01#7\n\x12\n\n\x04\x06\x02\x03\x08\
    \xe7\x07\0\x02\0\x12\x04\x81\x01#7\n\x13\n\x0b\x04\x06\x02\x03\x08\xe7\
    \x07\0\x02\0\x01\x12\x04\x81\x01$6\n\x11\n\t\x04\x06\x02\x03\x08\xe7\x07\
    \0\x03\x12\x04\x81\x01:?\n=\n\x04\x04\x06\x02\x04\x12\x04\x84\x01\x08+\
    \x1a/\x20It\x20represents\x20which\x20columns\x20we\x20should\x20output.\
    \n\n\r\n\x05\x04\x06\x02\x04\x04\x12\x04\x84\x01\x08\x10\n\r\n\x05\x04\
    \x06\x02\x04\x05\x12\x04\x84\x01\x11\x17\n\r\n\x05\x04\x06\x02\x04\x01\
    \x12\x04\x84\x01\x18&\n\r\n\x05\x04\x06\x02\x04\x03\x12\x04\x84\x01)*\n\
    \x0c\n\x02\x05\0\x12\x06\x87\x01\0\x8a\x01\x01\n\x0b\n\x03\x05\0\x01\x12\
    \x04\x87\x01\x05\x0f\n\x0c\n\x04\x05\0\x02\0\x12\x04\x88\x01\x08\x18\n\r\
    \n\x05\x05\0\x02\0\x01\x12\x04\x88\x01\x08\x13\n\r\n\x05\x05\0\x02\0\x02\
    \x12\x04\x88\x01\x16\x17\n\x0c\n\x04\x05\0\x02\x01\x12\x04\x89\x01\x08\
    \x16\n\r\n\x05\x05\0\x02\x01\x01\x12\x04\x89\x01\x08\x11\n\r\n\x05\x05\0\
    \x02\x01\x02\x12\x04\x89\x01\x14\x15\n\x0c\n\x02\x04\x07\x12\x06\x8c\x01\
    \0\x94\x01\x01\n\x0b\n\x03\x04\x07\x01\x12\x04\x8c\x01\x08\x16\n\x0c\n\
    \x04\x04\x07\x02\0\x12\x04\x8d\x01\x08!\n\r\n\x05\x04\x07\x02\0\x04\x12\
    \x04\x8d\x01\x08\x10\n\r\n\x05\x04\x07\x02\0\x06\x12\x04\x8d\x01\x11\x16\
    \n\r\n\x05\x04\x07\x02\0\x01\x12\x04\x8d\x01\x17\x1c\n\r\n\x05\x04\x07\
    \x02\0\x03\x12\x04\x8d\x01\x1f\x20\n\x0c\n\x04\x04\x07\x02\x01\x12\x04\
    \x8e\x01\x08K\n\r\n\x05\x04\x07\x02\x01\x04\x12\x04\x8e\x01\x08\x10\n\r\
    \n\x05\x04\x07\x02\x01\x06\x12\x04\x8e\x01\x11\x1b\n\r\n\x05\x04\x07\x02\
    \x01\x01\x12\x04\x8e\x01\x1c'\n\r\n\x05\x04\x07\x02\x01\x03\x12\x04\x8e\
    \x01*+\n\r\n\x05\x04\x07\x02\x01\x08\x12\x04\x8e\x01,J\n\x10\n\x08\x04\
    \x07\x02\x01\x08\xe7\x07\0\x12\x04\x8e\x01-I\n\x11\n\t\x04\x07\x02\x01\
    \x08\xe7\x07\0\x02\x12\x04\x8e\x01-A\n\x12\n\n\x04\x07\x02\x01\x08\xe7\
    \x07\0\x02\0\x12\x04\x8e\x01-A\n\x13\n\x0b\x04\x07\x02\x01\x08\xe7\x07\0\
    \x02\0\x01\x12\x04\x8e\x01.@\n\x11\n\t\x04\x07\x02\x01\x08\xe7\x07\0\x03\
    \x12\x04\x8e\x01DI\n\"\n\x04\x04\x07\x02\x02\x12\x05\x90\x01\x08\x8b\x01\
    \x1a\x13\x20Data\x20for\x20all\x20rows\n\n\r\n\x05\x04\x07\x02\x02\x04\
    \x12\x04\x90\x01\x08\x10\n\r\n\x05\x04\x07\x02\x02\x05\x12\x04\x90\x01\
    \x11\x16\n\r\n\x05\x04\x07\x02\x02\x01\x12\x04\x90\x01\x17\x1b\n\r\n\x05\
    \x04\x07\x02\x02\x03\x12\x04\x90\x01\x1e\x1f\n\x0e\n\x05\x04\x07\x02\x02\
    \x08\x12\x05\x90\x01\x20\x8a\x01\n\x10\n\x08\x04\x07\x02\x02\x08\xe7\x07\
    \0\x12\x04\x90\x01!k\n\x11\n\t\x04\x07\x02\x02\x08\xe7\x07\0\x02\x12\x04\
    \x90\x01!7\n\x12\n\n\x04\x07\x02\x02\x08\xe7\x07\0\x02\0\x12\x04\x90\x01\
    !7\n\x13\n\x0b\x04\x07\x02\x02\x08\xe7\x07\0\x02\0\x01\x12\x04\x90\x01\"\
    6\n\x11\n\t\x04\x07\x02\x02\x08\xe7\x07\0\x07\x12\x04\x90\x01:k\n\x11\n\
    \x08\x04\x07\x02\x02\x08\xe7\x07\x01\x12\x05\x90\x01m\x89\x01\n\x12\n\t\
    \x04\x07\x02\x02\x08\xe7\x07\x01\x02\x12\x05\x90\x01m\x81\x01\n\x13\n\n\
    \x04\x07\x02\x02\x08\xe7\x07\x01\x02\0\x12\x05\x90\x01m\x81\x01\n\x14\n\
    \x0b\x04\x07\x02\x02\x08\xe7\x07\x01\x02\0\x01\x12\x05\x90\x01n\x80\x01\
    \n\x13\n\t\x04\x07\x02\x02\x08\xe7\x07\x01\x03\x12\x06\x90\x01\x84\x01\
    \x89\x01\n\x0c\n\x04\x04\x07\x02\x03\x12\x04\x91\x01\x08$\n\r\n\x05\x04\
    \x07\x02\x03\x04\x12\x04\x91\x01\x08\x10\n\r\n\x05\x04\x07\x02\x03\x06\
    \x12\x04\x91\x01\x11\x16\n\r\n\x05\x04\x07\x02\x03\x01\x12\x04\x91\x01\
    \x17\x1f\n\r\n\x05\x04\x07\x02\x03\x03\x12\x04\x91\x01\"#\n2\n\x04\x04\
    \x07\x02\x04\x12\x04\x93\x01\x08)\x1a$\x20output\x20row\x20count\x20for\
    \x20each\x20executor\n\n\r\n\x05\x04\x07\x02\x04\x04\x12\x04\x93\x01\x08\
    \x10\n\r\n\x05\x04\x07\x02\x04\x05\x12\x04\x93\x01\x11\x16\n\r\n\x05\x04\
    \x07\x02\x04\x01\x12\x04\x93\x01\x17$\n\r\n\x05\x04\x07\x02\x04\x03\x12\
    \x04\x93\x01'(\
";

static mut file_descriptor_proto_lazy: ::protobuf::lazy::Lazy<::protobuf::descriptor::FileDescriptorProto> = ::protobuf::lazy::Lazy {
    lock: ::protobuf::lazy::ONCE_INIT,
    ptr: 0 as *const ::protobuf::descriptor::FileDescriptorProto,
};

fn parse_descriptor_proto() -> ::protobuf::descriptor::FileDescriptorProto {
    ::protobuf::parse_from_bytes(file_descriptor_proto_data).unwrap()
}

pub fn file_descriptor_proto() -> &'static ::protobuf::descriptor::FileDescriptorProto {
    unsafe {
        file_descriptor_proto_lazy.get(|| {
            parse_descriptor_proto()
        })
    }
}
