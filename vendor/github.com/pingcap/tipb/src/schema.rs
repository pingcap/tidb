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
pub struct TableInfo {
    // message fields
    table_id: ::std::option::Option<i64>,
    columns: ::protobuf::RepeatedField<ColumnInfo>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for TableInfo {}

impl TableInfo {
    pub fn new() -> TableInfo {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static TableInfo {
        static mut instance: ::protobuf::lazy::Lazy<TableInfo> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const TableInfo,
        };
        unsafe {
            instance.get(TableInfo::new)
        }
    }

    // optional int64 table_id = 1;

    pub fn clear_table_id(&mut self) {
        self.table_id = ::std::option::Option::None;
    }

    pub fn has_table_id(&self) -> bool {
        self.table_id.is_some()
    }

    // Param is passed by value, moved
    pub fn set_table_id(&mut self, v: i64) {
        self.table_id = ::std::option::Option::Some(v);
    }

    pub fn get_table_id(&self) -> i64 {
        self.table_id.unwrap_or(0)
    }

    fn get_table_id_for_reflect(&self) -> &::std::option::Option<i64> {
        &self.table_id
    }

    fn mut_table_id_for_reflect(&mut self) -> &mut ::std::option::Option<i64> {
        &mut self.table_id
    }

    // repeated .tipb.ColumnInfo columns = 2;

    pub fn clear_columns(&mut self) {
        self.columns.clear();
    }

    // Param is passed by value, moved
    pub fn set_columns(&mut self, v: ::protobuf::RepeatedField<ColumnInfo>) {
        self.columns = v;
    }

    // Mutable pointer to the field.
    pub fn mut_columns(&mut self) -> &mut ::protobuf::RepeatedField<ColumnInfo> {
        &mut self.columns
    }

    // Take field
    pub fn take_columns(&mut self) -> ::protobuf::RepeatedField<ColumnInfo> {
        ::std::mem::replace(&mut self.columns, ::protobuf::RepeatedField::new())
    }

    pub fn get_columns(&self) -> &[ColumnInfo] {
        &self.columns
    }

    fn get_columns_for_reflect(&self) -> &::protobuf::RepeatedField<ColumnInfo> {
        &self.columns
    }

    fn mut_columns_for_reflect(&mut self) -> &mut ::protobuf::RepeatedField<ColumnInfo> {
        &mut self.columns
    }
}

impl ::protobuf::Message for TableInfo {
    fn is_initialized(&self) -> bool {
        for v in &self.columns {
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
                    let tmp = is.read_int64()?;
                    self.table_id = ::std::option::Option::Some(tmp);
                },
                2 => {
                    ::protobuf::rt::read_repeated_message_into(wire_type, is, &mut self.columns)?;
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
        if let Some(v) = self.table_id {
            my_size += ::protobuf::rt::value_size(1, v, ::protobuf::wire_format::WireTypeVarint);
        }
        for value in &self.columns {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.table_id {
            os.write_int64(1, v)?;
        }
        for v in &self.columns {
            os.write_tag(2, ::protobuf::wire_format::WireTypeLengthDelimited)?;
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

impl ::protobuf::MessageStatic for TableInfo {
    fn new() -> TableInfo {
        TableInfo::new()
    }

    fn descriptor_static(_: ::std::option::Option<TableInfo>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeInt64>(
                    "table_id",
                    TableInfo::get_table_id_for_reflect,
                    TableInfo::mut_table_id_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_repeated_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<ColumnInfo>>(
                    "columns",
                    TableInfo::get_columns_for_reflect,
                    TableInfo::mut_columns_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<TableInfo>(
                    "TableInfo",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for TableInfo {
    fn clear(&mut self) {
        self.clear_table_id();
        self.clear_columns();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for TableInfo {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for TableInfo {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct ColumnInfo {
    // message fields
    column_id: ::std::option::Option<i64>,
    tp: ::std::option::Option<i32>,
    collation: ::std::option::Option<i32>,
    columnLen: ::std::option::Option<i32>,
    decimal: ::std::option::Option<i32>,
    flag: ::std::option::Option<i32>,
    elems: ::protobuf::RepeatedField<::std::string::String>,
    default_val: ::protobuf::SingularField<::std::vec::Vec<u8>>,
    pk_handle: ::std::option::Option<bool>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for ColumnInfo {}

impl ColumnInfo {
    pub fn new() -> ColumnInfo {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static ColumnInfo {
        static mut instance: ::protobuf::lazy::Lazy<ColumnInfo> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ColumnInfo,
        };
        unsafe {
            instance.get(ColumnInfo::new)
        }
    }

    // optional int64 column_id = 1;

    pub fn clear_column_id(&mut self) {
        self.column_id = ::std::option::Option::None;
    }

    pub fn has_column_id(&self) -> bool {
        self.column_id.is_some()
    }

    // Param is passed by value, moved
    pub fn set_column_id(&mut self, v: i64) {
        self.column_id = ::std::option::Option::Some(v);
    }

    pub fn get_column_id(&self) -> i64 {
        self.column_id.unwrap_or(0)
    }

    fn get_column_id_for_reflect(&self) -> &::std::option::Option<i64> {
        &self.column_id
    }

    fn mut_column_id_for_reflect(&mut self) -> &mut ::std::option::Option<i64> {
        &mut self.column_id
    }

    // optional int32 tp = 2;

    pub fn clear_tp(&mut self) {
        self.tp = ::std::option::Option::None;
    }

    pub fn has_tp(&self) -> bool {
        self.tp.is_some()
    }

    // Param is passed by value, moved
    pub fn set_tp(&mut self, v: i32) {
        self.tp = ::std::option::Option::Some(v);
    }

    pub fn get_tp(&self) -> i32 {
        self.tp.unwrap_or(0)
    }

    fn get_tp_for_reflect(&self) -> &::std::option::Option<i32> {
        &self.tp
    }

    fn mut_tp_for_reflect(&mut self) -> &mut ::std::option::Option<i32> {
        &mut self.tp
    }

    // optional int32 collation = 3;

    pub fn clear_collation(&mut self) {
        self.collation = ::std::option::Option::None;
    }

    pub fn has_collation(&self) -> bool {
        self.collation.is_some()
    }

    // Param is passed by value, moved
    pub fn set_collation(&mut self, v: i32) {
        self.collation = ::std::option::Option::Some(v);
    }

    pub fn get_collation(&self) -> i32 {
        self.collation.unwrap_or(0)
    }

    fn get_collation_for_reflect(&self) -> &::std::option::Option<i32> {
        &self.collation
    }

    fn mut_collation_for_reflect(&mut self) -> &mut ::std::option::Option<i32> {
        &mut self.collation
    }

    // optional int32 columnLen = 4;

    pub fn clear_columnLen(&mut self) {
        self.columnLen = ::std::option::Option::None;
    }

    pub fn has_columnLen(&self) -> bool {
        self.columnLen.is_some()
    }

    // Param is passed by value, moved
    pub fn set_columnLen(&mut self, v: i32) {
        self.columnLen = ::std::option::Option::Some(v);
    }

    pub fn get_columnLen(&self) -> i32 {
        self.columnLen.unwrap_or(0)
    }

    fn get_columnLen_for_reflect(&self) -> &::std::option::Option<i32> {
        &self.columnLen
    }

    fn mut_columnLen_for_reflect(&mut self) -> &mut ::std::option::Option<i32> {
        &mut self.columnLen
    }

    // optional int32 decimal = 5;

    pub fn clear_decimal(&mut self) {
        self.decimal = ::std::option::Option::None;
    }

    pub fn has_decimal(&self) -> bool {
        self.decimal.is_some()
    }

    // Param is passed by value, moved
    pub fn set_decimal(&mut self, v: i32) {
        self.decimal = ::std::option::Option::Some(v);
    }

    pub fn get_decimal(&self) -> i32 {
        self.decimal.unwrap_or(0)
    }

    fn get_decimal_for_reflect(&self) -> &::std::option::Option<i32> {
        &self.decimal
    }

    fn mut_decimal_for_reflect(&mut self) -> &mut ::std::option::Option<i32> {
        &mut self.decimal
    }

    // optional int32 flag = 6;

    pub fn clear_flag(&mut self) {
        self.flag = ::std::option::Option::None;
    }

    pub fn has_flag(&self) -> bool {
        self.flag.is_some()
    }

    // Param is passed by value, moved
    pub fn set_flag(&mut self, v: i32) {
        self.flag = ::std::option::Option::Some(v);
    }

    pub fn get_flag(&self) -> i32 {
        self.flag.unwrap_or(0)
    }

    fn get_flag_for_reflect(&self) -> &::std::option::Option<i32> {
        &self.flag
    }

    fn mut_flag_for_reflect(&mut self) -> &mut ::std::option::Option<i32> {
        &mut self.flag
    }

    // repeated string elems = 7;

    pub fn clear_elems(&mut self) {
        self.elems.clear();
    }

    // Param is passed by value, moved
    pub fn set_elems(&mut self, v: ::protobuf::RepeatedField<::std::string::String>) {
        self.elems = v;
    }

    // Mutable pointer to the field.
    pub fn mut_elems(&mut self) -> &mut ::protobuf::RepeatedField<::std::string::String> {
        &mut self.elems
    }

    // Take field
    pub fn take_elems(&mut self) -> ::protobuf::RepeatedField<::std::string::String> {
        ::std::mem::replace(&mut self.elems, ::protobuf::RepeatedField::new())
    }

    pub fn get_elems(&self) -> &[::std::string::String] {
        &self.elems
    }

    fn get_elems_for_reflect(&self) -> &::protobuf::RepeatedField<::std::string::String> {
        &self.elems
    }

    fn mut_elems_for_reflect(&mut self) -> &mut ::protobuf::RepeatedField<::std::string::String> {
        &mut self.elems
    }

    // optional bytes default_val = 8;

    pub fn clear_default_val(&mut self) {
        self.default_val.clear();
    }

    pub fn has_default_val(&self) -> bool {
        self.default_val.is_some()
    }

    // Param is passed by value, moved
    pub fn set_default_val(&mut self, v: ::std::vec::Vec<u8>) {
        self.default_val = ::protobuf::SingularField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_default_val(&mut self) -> &mut ::std::vec::Vec<u8> {
        if self.default_val.is_none() {
            self.default_val.set_default();
        }
        self.default_val.as_mut().unwrap()
    }

    // Take field
    pub fn take_default_val(&mut self) -> ::std::vec::Vec<u8> {
        self.default_val.take().unwrap_or_else(|| ::std::vec::Vec::new())
    }

    pub fn get_default_val(&self) -> &[u8] {
        match self.default_val.as_ref() {
            Some(v) => &v,
            None => &[],
        }
    }

    fn get_default_val_for_reflect(&self) -> &::protobuf::SingularField<::std::vec::Vec<u8>> {
        &self.default_val
    }

    fn mut_default_val_for_reflect(&mut self) -> &mut ::protobuf::SingularField<::std::vec::Vec<u8>> {
        &mut self.default_val
    }

    // optional bool pk_handle = 21;

    pub fn clear_pk_handle(&mut self) {
        self.pk_handle = ::std::option::Option::None;
    }

    pub fn has_pk_handle(&self) -> bool {
        self.pk_handle.is_some()
    }

    // Param is passed by value, moved
    pub fn set_pk_handle(&mut self, v: bool) {
        self.pk_handle = ::std::option::Option::Some(v);
    }

    pub fn get_pk_handle(&self) -> bool {
        self.pk_handle.unwrap_or(false)
    }

    fn get_pk_handle_for_reflect(&self) -> &::std::option::Option<bool> {
        &self.pk_handle
    }

    fn mut_pk_handle_for_reflect(&mut self) -> &mut ::std::option::Option<bool> {
        &mut self.pk_handle
    }
}

impl ::protobuf::Message for ColumnInfo {
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
                    self.column_id = ::std::option::Option::Some(tmp);
                },
                2 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_int32()?;
                    self.tp = ::std::option::Option::Some(tmp);
                },
                3 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_int32()?;
                    self.collation = ::std::option::Option::Some(tmp);
                },
                4 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_int32()?;
                    self.columnLen = ::std::option::Option::Some(tmp);
                },
                5 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_int32()?;
                    self.decimal = ::std::option::Option::Some(tmp);
                },
                6 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_int32()?;
                    self.flag = ::std::option::Option::Some(tmp);
                },
                7 => {
                    ::protobuf::rt::read_repeated_string_into(wire_type, is, &mut self.elems)?;
                },
                8 => {
                    ::protobuf::rt::read_singular_bytes_into(wire_type, is, &mut self.default_val)?;
                },
                21 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_bool()?;
                    self.pk_handle = ::std::option::Option::Some(tmp);
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
        if let Some(v) = self.column_id {
            my_size += ::protobuf::rt::value_size(1, v, ::protobuf::wire_format::WireTypeVarint);
        }
        if let Some(v) = self.tp {
            my_size += ::protobuf::rt::value_size(2, v, ::protobuf::wire_format::WireTypeVarint);
        }
        if let Some(v) = self.collation {
            my_size += ::protobuf::rt::value_size(3, v, ::protobuf::wire_format::WireTypeVarint);
        }
        if let Some(v) = self.columnLen {
            my_size += ::protobuf::rt::value_size(4, v, ::protobuf::wire_format::WireTypeVarint);
        }
        if let Some(v) = self.decimal {
            my_size += ::protobuf::rt::value_size(5, v, ::protobuf::wire_format::WireTypeVarint);
        }
        if let Some(v) = self.flag {
            my_size += ::protobuf::rt::value_size(6, v, ::protobuf::wire_format::WireTypeVarint);
        }
        for value in &self.elems {
            my_size += ::protobuf::rt::string_size(7, &value);
        };
        if let Some(ref v) = self.default_val.as_ref() {
            my_size += ::protobuf::rt::bytes_size(8, &v);
        }
        if let Some(v) = self.pk_handle {
            my_size += 3;
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.column_id {
            os.write_int64(1, v)?;
        }
        if let Some(v) = self.tp {
            os.write_int32(2, v)?;
        }
        if let Some(v) = self.collation {
            os.write_int32(3, v)?;
        }
        if let Some(v) = self.columnLen {
            os.write_int32(4, v)?;
        }
        if let Some(v) = self.decimal {
            os.write_int32(5, v)?;
        }
        if let Some(v) = self.flag {
            os.write_int32(6, v)?;
        }
        for v in &self.elems {
            os.write_string(7, &v)?;
        };
        if let Some(ref v) = self.default_val.as_ref() {
            os.write_bytes(8, &v)?;
        }
        if let Some(v) = self.pk_handle {
            os.write_bool(21, v)?;
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

impl ::protobuf::MessageStatic for ColumnInfo {
    fn new() -> ColumnInfo {
        ColumnInfo::new()
    }

    fn descriptor_static(_: ::std::option::Option<ColumnInfo>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeInt64>(
                    "column_id",
                    ColumnInfo::get_column_id_for_reflect,
                    ColumnInfo::mut_column_id_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeInt32>(
                    "tp",
                    ColumnInfo::get_tp_for_reflect,
                    ColumnInfo::mut_tp_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeInt32>(
                    "collation",
                    ColumnInfo::get_collation_for_reflect,
                    ColumnInfo::mut_collation_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeInt32>(
                    "columnLen",
                    ColumnInfo::get_columnLen_for_reflect,
                    ColumnInfo::mut_columnLen_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeInt32>(
                    "decimal",
                    ColumnInfo::get_decimal_for_reflect,
                    ColumnInfo::mut_decimal_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeInt32>(
                    "flag",
                    ColumnInfo::get_flag_for_reflect,
                    ColumnInfo::mut_flag_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_repeated_field_accessor::<_, ::protobuf::types::ProtobufTypeString>(
                    "elems",
                    ColumnInfo::get_elems_for_reflect,
                    ColumnInfo::mut_elems_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_field_accessor::<_, ::protobuf::types::ProtobufTypeBytes>(
                    "default_val",
                    ColumnInfo::get_default_val_for_reflect,
                    ColumnInfo::mut_default_val_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeBool>(
                    "pk_handle",
                    ColumnInfo::get_pk_handle_for_reflect,
                    ColumnInfo::mut_pk_handle_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<ColumnInfo>(
                    "ColumnInfo",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for ColumnInfo {
    fn clear(&mut self) {
        self.clear_column_id();
        self.clear_tp();
        self.clear_collation();
        self.clear_columnLen();
        self.clear_decimal();
        self.clear_flag();
        self.clear_elems();
        self.clear_default_val();
        self.clear_pk_handle();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for ColumnInfo {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for ColumnInfo {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct IndexInfo {
    // message fields
    table_id: ::std::option::Option<i64>,
    index_id: ::std::option::Option<i64>,
    columns: ::protobuf::RepeatedField<ColumnInfo>,
    unique: ::std::option::Option<bool>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for IndexInfo {}

impl IndexInfo {
    pub fn new() -> IndexInfo {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static IndexInfo {
        static mut instance: ::protobuf::lazy::Lazy<IndexInfo> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const IndexInfo,
        };
        unsafe {
            instance.get(IndexInfo::new)
        }
    }

    // optional int64 table_id = 1;

    pub fn clear_table_id(&mut self) {
        self.table_id = ::std::option::Option::None;
    }

    pub fn has_table_id(&self) -> bool {
        self.table_id.is_some()
    }

    // Param is passed by value, moved
    pub fn set_table_id(&mut self, v: i64) {
        self.table_id = ::std::option::Option::Some(v);
    }

    pub fn get_table_id(&self) -> i64 {
        self.table_id.unwrap_or(0)
    }

    fn get_table_id_for_reflect(&self) -> &::std::option::Option<i64> {
        &self.table_id
    }

    fn mut_table_id_for_reflect(&mut self) -> &mut ::std::option::Option<i64> {
        &mut self.table_id
    }

    // optional int64 index_id = 2;

    pub fn clear_index_id(&mut self) {
        self.index_id = ::std::option::Option::None;
    }

    pub fn has_index_id(&self) -> bool {
        self.index_id.is_some()
    }

    // Param is passed by value, moved
    pub fn set_index_id(&mut self, v: i64) {
        self.index_id = ::std::option::Option::Some(v);
    }

    pub fn get_index_id(&self) -> i64 {
        self.index_id.unwrap_or(0)
    }

    fn get_index_id_for_reflect(&self) -> &::std::option::Option<i64> {
        &self.index_id
    }

    fn mut_index_id_for_reflect(&mut self) -> &mut ::std::option::Option<i64> {
        &mut self.index_id
    }

    // repeated .tipb.ColumnInfo columns = 3;

    pub fn clear_columns(&mut self) {
        self.columns.clear();
    }

    // Param is passed by value, moved
    pub fn set_columns(&mut self, v: ::protobuf::RepeatedField<ColumnInfo>) {
        self.columns = v;
    }

    // Mutable pointer to the field.
    pub fn mut_columns(&mut self) -> &mut ::protobuf::RepeatedField<ColumnInfo> {
        &mut self.columns
    }

    // Take field
    pub fn take_columns(&mut self) -> ::protobuf::RepeatedField<ColumnInfo> {
        ::std::mem::replace(&mut self.columns, ::protobuf::RepeatedField::new())
    }

    pub fn get_columns(&self) -> &[ColumnInfo] {
        &self.columns
    }

    fn get_columns_for_reflect(&self) -> &::protobuf::RepeatedField<ColumnInfo> {
        &self.columns
    }

    fn mut_columns_for_reflect(&mut self) -> &mut ::protobuf::RepeatedField<ColumnInfo> {
        &mut self.columns
    }

    // optional bool unique = 4;

    pub fn clear_unique(&mut self) {
        self.unique = ::std::option::Option::None;
    }

    pub fn has_unique(&self) -> bool {
        self.unique.is_some()
    }

    // Param is passed by value, moved
    pub fn set_unique(&mut self, v: bool) {
        self.unique = ::std::option::Option::Some(v);
    }

    pub fn get_unique(&self) -> bool {
        self.unique.unwrap_or(false)
    }

    fn get_unique_for_reflect(&self) -> &::std::option::Option<bool> {
        &self.unique
    }

    fn mut_unique_for_reflect(&mut self) -> &mut ::std::option::Option<bool> {
        &mut self.unique
    }
}

impl ::protobuf::Message for IndexInfo {
    fn is_initialized(&self) -> bool {
        for v in &self.columns {
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
                    let tmp = is.read_int64()?;
                    self.table_id = ::std::option::Option::Some(tmp);
                },
                2 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_int64()?;
                    self.index_id = ::std::option::Option::Some(tmp);
                },
                3 => {
                    ::protobuf::rt::read_repeated_message_into(wire_type, is, &mut self.columns)?;
                },
                4 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_bool()?;
                    self.unique = ::std::option::Option::Some(tmp);
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
        if let Some(v) = self.table_id {
            my_size += ::protobuf::rt::value_size(1, v, ::protobuf::wire_format::WireTypeVarint);
        }
        if let Some(v) = self.index_id {
            my_size += ::protobuf::rt::value_size(2, v, ::protobuf::wire_format::WireTypeVarint);
        }
        for value in &self.columns {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        if let Some(v) = self.unique {
            my_size += 2;
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.table_id {
            os.write_int64(1, v)?;
        }
        if let Some(v) = self.index_id {
            os.write_int64(2, v)?;
        }
        for v in &self.columns {
            os.write_tag(3, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        };
        if let Some(v) = self.unique {
            os.write_bool(4, v)?;
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

impl ::protobuf::MessageStatic for IndexInfo {
    fn new() -> IndexInfo {
        IndexInfo::new()
    }

    fn descriptor_static(_: ::std::option::Option<IndexInfo>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeInt64>(
                    "table_id",
                    IndexInfo::get_table_id_for_reflect,
                    IndexInfo::mut_table_id_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeInt64>(
                    "index_id",
                    IndexInfo::get_index_id_for_reflect,
                    IndexInfo::mut_index_id_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_repeated_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<ColumnInfo>>(
                    "columns",
                    IndexInfo::get_columns_for_reflect,
                    IndexInfo::mut_columns_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeBool>(
                    "unique",
                    IndexInfo::get_unique_for_reflect,
                    IndexInfo::mut_unique_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<IndexInfo>(
                    "IndexInfo",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for IndexInfo {
    fn clear(&mut self) {
        self.clear_table_id();
        self.clear_index_id();
        self.clear_columns();
        self.clear_unique();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for IndexInfo {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for IndexInfo {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct KeyRange {
    // message fields
    low: ::protobuf::SingularField<::std::vec::Vec<u8>>,
    high: ::protobuf::SingularField<::std::vec::Vec<u8>>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for KeyRange {}

impl KeyRange {
    pub fn new() -> KeyRange {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static KeyRange {
        static mut instance: ::protobuf::lazy::Lazy<KeyRange> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const KeyRange,
        };
        unsafe {
            instance.get(KeyRange::new)
        }
    }

    // optional bytes low = 1;

    pub fn clear_low(&mut self) {
        self.low.clear();
    }

    pub fn has_low(&self) -> bool {
        self.low.is_some()
    }

    // Param is passed by value, moved
    pub fn set_low(&mut self, v: ::std::vec::Vec<u8>) {
        self.low = ::protobuf::SingularField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_low(&mut self) -> &mut ::std::vec::Vec<u8> {
        if self.low.is_none() {
            self.low.set_default();
        }
        self.low.as_mut().unwrap()
    }

    // Take field
    pub fn take_low(&mut self) -> ::std::vec::Vec<u8> {
        self.low.take().unwrap_or_else(|| ::std::vec::Vec::new())
    }

    pub fn get_low(&self) -> &[u8] {
        match self.low.as_ref() {
            Some(v) => &v,
            None => &[],
        }
    }

    fn get_low_for_reflect(&self) -> &::protobuf::SingularField<::std::vec::Vec<u8>> {
        &self.low
    }

    fn mut_low_for_reflect(&mut self) -> &mut ::protobuf::SingularField<::std::vec::Vec<u8>> {
        &mut self.low
    }

    // optional bytes high = 2;

    pub fn clear_high(&mut self) {
        self.high.clear();
    }

    pub fn has_high(&self) -> bool {
        self.high.is_some()
    }

    // Param is passed by value, moved
    pub fn set_high(&mut self, v: ::std::vec::Vec<u8>) {
        self.high = ::protobuf::SingularField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_high(&mut self) -> &mut ::std::vec::Vec<u8> {
        if self.high.is_none() {
            self.high.set_default();
        }
        self.high.as_mut().unwrap()
    }

    // Take field
    pub fn take_high(&mut self) -> ::std::vec::Vec<u8> {
        self.high.take().unwrap_or_else(|| ::std::vec::Vec::new())
    }

    pub fn get_high(&self) -> &[u8] {
        match self.high.as_ref() {
            Some(v) => &v,
            None => &[],
        }
    }

    fn get_high_for_reflect(&self) -> &::protobuf::SingularField<::std::vec::Vec<u8>> {
        &self.high
    }

    fn mut_high_for_reflect(&mut self) -> &mut ::protobuf::SingularField<::std::vec::Vec<u8>> {
        &mut self.high
    }
}

impl ::protobuf::Message for KeyRange {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !is.eof()? {
            let (field_number, wire_type) = is.read_tag_unpack()?;
            match field_number {
                1 => {
                    ::protobuf::rt::read_singular_bytes_into(wire_type, is, &mut self.low)?;
                },
                2 => {
                    ::protobuf::rt::read_singular_bytes_into(wire_type, is, &mut self.high)?;
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
        if let Some(ref v) = self.low.as_ref() {
            my_size += ::protobuf::rt::bytes_size(1, &v);
        }
        if let Some(ref v) = self.high.as_ref() {
            my_size += ::protobuf::rt::bytes_size(2, &v);
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(ref v) = self.low.as_ref() {
            os.write_bytes(1, &v)?;
        }
        if let Some(ref v) = self.high.as_ref() {
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

impl ::protobuf::MessageStatic for KeyRange {
    fn new() -> KeyRange {
        KeyRange::new()
    }

    fn descriptor_static(_: ::std::option::Option<KeyRange>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_field_accessor::<_, ::protobuf::types::ProtobufTypeBytes>(
                    "low",
                    KeyRange::get_low_for_reflect,
                    KeyRange::mut_low_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_field_accessor::<_, ::protobuf::types::ProtobufTypeBytes>(
                    "high",
                    KeyRange::get_high_for_reflect,
                    KeyRange::mut_high_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<KeyRange>(
                    "KeyRange",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for KeyRange {
    fn clear(&mut self) {
        self.clear_low();
        self.clear_high();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for KeyRange {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for KeyRange {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

static file_descriptor_proto_data: &'static [u8] = b"\
    \n\x0cschema.proto\x12\x04tipb\x1a\x14gogoproto/gogo.proto\"X\n\tTableIn\
    fo\x12\x1f\n\x08table_id\x18\x01\x20\x01(\x03R\x07tableIdB\x04\xc8\xde\
    \x1f\0\x12*\n\x07columns\x18\x02\x20\x03(\x0b2\x10.tipb.ColumnInfoR\x07c\
    olumns\"\xa1\x02\n\nColumnInfo\x12!\n\tcolumn_id\x18\x01\x20\x01(\x03R\
    \x08columnIdB\x04\xc8\xde\x1f\0\x12\x14\n\x02tp\x18\x02\x20\x01(\x05R\
    \x02tpB\x04\xc8\xde\x1f\0\x12\"\n\tcollation\x18\x03\x20\x01(\x05R\tcoll\
    ationB\x04\xc8\xde\x1f\0\x12\"\n\tcolumnLen\x18\x04\x20\x01(\x05R\tcolum\
    nLenB\x04\xc8\xde\x1f\0\x12\x1e\n\x07decimal\x18\x05\x20\x01(\x05R\x07de\
    cimalB\x04\xc8\xde\x1f\0\x12\x18\n\x04flag\x18\x06\x20\x01(\x05R\x04flag\
    B\x04\xc8\xde\x1f\0\x12\x14\n\x05elems\x18\x07\x20\x03(\tR\x05elems\x12\
    \x1f\n\x0bdefault_val\x18\x08\x20\x01(\x0cR\ndefaultVal\x12!\n\tpk_handl\
    e\x18\x15\x20\x01(\x08R\x08pkHandleB\x04\xc8\xde\x1f\0\"\x97\x01\n\tInde\
    xInfo\x12\x1f\n\x08table_id\x18\x01\x20\x01(\x03R\x07tableIdB\x04\xc8\
    \xde\x1f\0\x12\x1f\n\x08index_id\x18\x02\x20\x01(\x03R\x07indexIdB\x04\
    \xc8\xde\x1f\0\x12*\n\x07columns\x18\x03\x20\x03(\x0b2\x10.tipb.ColumnIn\
    foR\x07columns\x12\x1c\n\x06unique\x18\x04\x20\x01(\x08R\x06uniqueB\x04\
    \xc8\xde\x1f\0\"0\n\x08KeyRange\x12\x10\n\x03low\x18\x01\x20\x01(\x0cR\
    \x03low\x12\x12\n\x04high\x18\x02\x20\x01(\x0cR\x04highB%\n\x15com.pingc\
    ap.tidb.tipbP\x01\xe0\xe2\x1e\x01\xd0\xe2\x1e\x01\xc8\xe2\x1e\x01J\xe9\
    \x17\n\x06\x12\x04\0\0)\x01\n\x08\n\x01\x0c\x12\x03\0\0\x12\n\x08\n\x01\
    \x02\x12\x03\x02\x08\x0c\n\x08\n\x01\x08\x12\x03\x04\0\"\n\x0b\n\x04\x08\
    \xe7\x07\0\x12\x03\x04\0\"\n\x0c\n\x05\x08\xe7\x07\0\x02\x12\x03\x04\x07\
    \x1a\n\r\n\x06\x08\xe7\x07\0\x02\0\x12\x03\x04\x07\x1a\n\x0e\n\x07\x08\
    \xe7\x07\0\x02\0\x01\x12\x03\x04\x07\x1a\n\x0c\n\x05\x08\xe7\x07\0\x03\
    \x12\x03\x04\x1d!\n\x08\n\x01\x08\x12\x03\x05\0.\n\x0b\n\x04\x08\xe7\x07\
    \x01\x12\x03\x05\0.\n\x0c\n\x05\x08\xe7\x07\x01\x02\x12\x03\x05\x07\x13\
    \n\r\n\x06\x08\xe7\x07\x01\x02\0\x12\x03\x05\x07\x13\n\x0e\n\x07\x08\xe7\
    \x07\x01\x02\0\x01\x12\x03\x05\x07\x13\n\x0c\n\x05\x08\xe7\x07\x01\x07\
    \x12\x03\x05\x16-\n\t\n\x02\x03\0\x12\x03\x07\x07\x1d\n\x08\n\x01\x08\
    \x12\x03\t\0(\n\x0b\n\x04\x08\xe7\x07\x02\x12\x03\t\0(\n\x0c\n\x05\x08\
    \xe7\x07\x02\x02\x12\x03\t\x07\x20\n\r\n\x06\x08\xe7\x07\x02\x02\0\x12\
    \x03\t\x07\x20\n\x0e\n\x07\x08\xe7\x07\x02\x02\0\x01\x12\x03\t\x08\x1f\n\
    \x0c\n\x05\x08\xe7\x07\x02\x03\x12\x03\t#'\n\x08\n\x01\x08\x12\x03\n\0$\
    \n\x0b\n\x04\x08\xe7\x07\x03\x12\x03\n\0$\n\x0c\n\x05\x08\xe7\x07\x03\
    \x02\x12\x03\n\x07\x1c\n\r\n\x06\x08\xe7\x07\x03\x02\0\x12\x03\n\x07\x1c\
    \n\x0e\n\x07\x08\xe7\x07\x03\x02\0\x01\x12\x03\n\x08\x1b\n\x0c\n\x05\x08\
    \xe7\x07\x03\x03\x12\x03\n\x1f#\n\x08\n\x01\x08\x12\x03\x0b\0*\n\x0b\n\
    \x04\x08\xe7\x07\x04\x12\x03\x0b\0*\n\x0c\n\x05\x08\xe7\x07\x04\x02\x12\
    \x03\x0b\x07\"\n\r\n\x06\x08\xe7\x07\x04\x02\0\x12\x03\x0b\x07\"\n\x0e\n\
    \x07\x08\xe7\x07\x04\x02\0\x01\x12\x03\x0b\x08!\n\x0c\n\x05\x08\xe7\x07\
    \x04\x03\x12\x03\x0b%)\n\n\n\x02\x04\0\x12\x04\r\0\x10\x01\n\n\n\x03\x04\
    \0\x01\x12\x03\r\x08\x11\n\x0b\n\x04\x04\0\x02\0\x12\x03\x0e\x08C\n\x0c\
    \n\x05\x04\0\x02\0\x04\x12\x03\x0e\x08\x10\n\x0c\n\x05\x04\0\x02\0\x05\
    \x12\x03\x0e\x11\x16\n\x0c\n\x05\x04\0\x02\0\x01\x12\x03\x0e\x17\x1f\n\
    \x0c\n\x05\x04\0\x02\0\x03\x12\x03\x0e\"#\n\x0c\n\x05\x04\0\x02\0\x08\
    \x12\x03\x0e$B\n\x0f\n\x08\x04\0\x02\0\x08\xe7\x07\0\x12\x03\x0e%A\n\x10\
    \n\t\x04\0\x02\0\x08\xe7\x07\0\x02\x12\x03\x0e%9\n\x11\n\n\x04\0\x02\0\
    \x08\xe7\x07\0\x02\0\x12\x03\x0e%9\n\x12\n\x0b\x04\0\x02\0\x08\xe7\x07\0\
    \x02\0\x01\x12\x03\x0e&8\n\x10\n\t\x04\0\x02\0\x08\xe7\x07\0\x03\x12\x03\
    \x0e<A\n\x0b\n\x04\x04\0\x02\x01\x12\x03\x0f\x08(\n\x0c\n\x05\x04\0\x02\
    \x01\x04\x12\x03\x0f\x08\x10\n\x0c\n\x05\x04\0\x02\x01\x06\x12\x03\x0f\
    \x11\x1b\n\x0c\n\x05\x04\0\x02\x01\x01\x12\x03\x0f\x1c#\n\x0c\n\x05\x04\
    \0\x02\x01\x03\x12\x03\x0f&'\n\n\n\x02\x04\x01\x12\x04\x12\0\x1c\x01\n\n\
    \n\x03\x04\x01\x01\x12\x03\x12\x08\x12\n\x0b\n\x04\x04\x01\x02\0\x12\x03\
    \x13\x08D\n\x0c\n\x05\x04\x01\x02\0\x04\x12\x03\x13\x08\x10\n\x0c\n\x05\
    \x04\x01\x02\0\x05\x12\x03\x13\x11\x16\n\x0c\n\x05\x04\x01\x02\0\x01\x12\
    \x03\x13\x17\x20\n\x0c\n\x05\x04\x01\x02\0\x03\x12\x03\x13#$\n\x0c\n\x05\
    \x04\x01\x02\0\x08\x12\x03\x13%C\n\x0f\n\x08\x04\x01\x02\0\x08\xe7\x07\0\
    \x12\x03\x13&B\n\x10\n\t\x04\x01\x02\0\x08\xe7\x07\0\x02\x12\x03\x13&:\n\
    \x11\n\n\x04\x01\x02\0\x08\xe7\x07\0\x02\0\x12\x03\x13&:\n\x12\n\x0b\x04\
    \x01\x02\0\x08\xe7\x07\0\x02\0\x01\x12\x03\x13'9\n\x10\n\t\x04\x01\x02\0\
    \x08\xe7\x07\0\x03\x12\x03\x13=B\n\x1a\n\x04\x04\x01\x02\x01\x12\x03\x14\
    \x08=\"\r\x20MySQL\x20type.\n\n\x0c\n\x05\x04\x01\x02\x01\x04\x12\x03\
    \x14\x08\x10\n\x0c\n\x05\x04\x01\x02\x01\x05\x12\x03\x14\x11\x16\n\x0c\n\
    \x05\x04\x01\x02\x01\x01\x12\x03\x14\x17\x19\n\x0c\n\x05\x04\x01\x02\x01\
    \x03\x12\x03\x14\x1c\x1d\n\x0c\n\x05\x04\x01\x02\x01\x08\x12\x03\x14\x1e\
    <\n\x0f\n\x08\x04\x01\x02\x01\x08\xe7\x07\0\x12\x03\x14\x1f;\n\x10\n\t\
    \x04\x01\x02\x01\x08\xe7\x07\0\x02\x12\x03\x14\x1f3\n\x11\n\n\x04\x01\
    \x02\x01\x08\xe7\x07\0\x02\0\x12\x03\x14\x1f3\n\x12\n\x0b\x04\x01\x02\
    \x01\x08\xe7\x07\0\x02\0\x01\x12\x03\x14\x202\n\x10\n\t\x04\x01\x02\x01\
    \x08\xe7\x07\0\x03\x12\x03\x146;\n\x0b\n\x04\x04\x01\x02\x02\x12\x03\x15\
    \x08D\n\x0c\n\x05\x04\x01\x02\x02\x04\x12\x03\x15\x08\x10\n\x0c\n\x05\
    \x04\x01\x02\x02\x05\x12\x03\x15\x11\x16\n\x0c\n\x05\x04\x01\x02\x02\x01\
    \x12\x03\x15\x17\x20\n\x0c\n\x05\x04\x01\x02\x02\x03\x12\x03\x15#$\n\x0c\
    \n\x05\x04\x01\x02\x02\x08\x12\x03\x15%C\n\x0f\n\x08\x04\x01\x02\x02\x08\
    \xe7\x07\0\x12\x03\x15&B\n\x10\n\t\x04\x01\x02\x02\x08\xe7\x07\0\x02\x12\
    \x03\x15&:\n\x11\n\n\x04\x01\x02\x02\x08\xe7\x07\0\x02\0\x12\x03\x15&:\n\
    \x12\n\x0b\x04\x01\x02\x02\x08\xe7\x07\0\x02\0\x01\x12\x03\x15'9\n\x10\n\
    \t\x04\x01\x02\x02\x08\xe7\x07\0\x03\x12\x03\x15=B\n\x0b\n\x04\x04\x01\
    \x02\x03\x12\x03\x16\x08D\n\x0c\n\x05\x04\x01\x02\x03\x04\x12\x03\x16\
    \x08\x10\n\x0c\n\x05\x04\x01\x02\x03\x05\x12\x03\x16\x11\x16\n\x0c\n\x05\
    \x04\x01\x02\x03\x01\x12\x03\x16\x17\x20\n\x0c\n\x05\x04\x01\x02\x03\x03\
    \x12\x03\x16#$\n\x0c\n\x05\x04\x01\x02\x03\x08\x12\x03\x16%C\n\x0f\n\x08\
    \x04\x01\x02\x03\x08\xe7\x07\0\x12\x03\x16&B\n\x10\n\t\x04\x01\x02\x03\
    \x08\xe7\x07\0\x02\x12\x03\x16&:\n\x11\n\n\x04\x01\x02\x03\x08\xe7\x07\0\
    \x02\0\x12\x03\x16&:\n\x12\n\x0b\x04\x01\x02\x03\x08\xe7\x07\0\x02\0\x01\
    \x12\x03\x16'9\n\x10\n\t\x04\x01\x02\x03\x08\xe7\x07\0\x03\x12\x03\x16=B\
    \n\x0b\n\x04\x04\x01\x02\x04\x12\x03\x17\x08B\n\x0c\n\x05\x04\x01\x02\
    \x04\x04\x12\x03\x17\x08\x10\n\x0c\n\x05\x04\x01\x02\x04\x05\x12\x03\x17\
    \x11\x16\n\x0c\n\x05\x04\x01\x02\x04\x01\x12\x03\x17\x17\x1e\n\x0c\n\x05\
    \x04\x01\x02\x04\x03\x12\x03\x17!\"\n\x0c\n\x05\x04\x01\x02\x04\x08\x12\
    \x03\x17#A\n\x0f\n\x08\x04\x01\x02\x04\x08\xe7\x07\0\x12\x03\x17$@\n\x10\
    \n\t\x04\x01\x02\x04\x08\xe7\x07\0\x02\x12\x03\x17$8\n\x11\n\n\x04\x01\
    \x02\x04\x08\xe7\x07\0\x02\0\x12\x03\x17$8\n\x12\n\x0b\x04\x01\x02\x04\
    \x08\xe7\x07\0\x02\0\x01\x12\x03\x17%7\n\x10\n\t\x04\x01\x02\x04\x08\xe7\
    \x07\0\x03\x12\x03\x17;@\n\x0b\n\x04\x04\x01\x02\x05\x12\x03\x18\x08?\n\
    \x0c\n\x05\x04\x01\x02\x05\x04\x12\x03\x18\x08\x10\n\x0c\n\x05\x04\x01\
    \x02\x05\x05\x12\x03\x18\x11\x16\n\x0c\n\x05\x04\x01\x02\x05\x01\x12\x03\
    \x18\x17\x1b\n\x0c\n\x05\x04\x01\x02\x05\x03\x12\x03\x18\x1e\x1f\n\x0c\n\
    \x05\x04\x01\x02\x05\x08\x12\x03\x18\x20>\n\x0f\n\x08\x04\x01\x02\x05\
    \x08\xe7\x07\0\x12\x03\x18!=\n\x10\n\t\x04\x01\x02\x05\x08\xe7\x07\0\x02\
    \x12\x03\x18!5\n\x11\n\n\x04\x01\x02\x05\x08\xe7\x07\0\x02\0\x12\x03\x18\
    !5\n\x12\n\x0b\x04\x01\x02\x05\x08\xe7\x07\0\x02\0\x01\x12\x03\x18\"4\n\
    \x10\n\t\x04\x01\x02\x05\x08\xe7\x07\0\x03\x12\x03\x188=\n\x0b\n\x04\x04\
    \x01\x02\x06\x12\x03\x19\x08\"\n\x0c\n\x05\x04\x01\x02\x06\x04\x12\x03\
    \x19\x08\x10\n\x0c\n\x05\x04\x01\x02\x06\x05\x12\x03\x19\x11\x17\n\x0c\n\
    \x05\x04\x01\x02\x06\x01\x12\x03\x19\x18\x1d\n\x0c\n\x05\x04\x01\x02\x06\
    \x03\x12\x03\x19\x20!\n\x1d\n\x04\x04\x01\x02\x07\x12\x03\x1a\x08'\"\x10\
    \x20Encoded\x20datum.\n\n\x0c\n\x05\x04\x01\x02\x07\x04\x12\x03\x1a\x08\
    \x10\n\x0c\n\x05\x04\x01\x02\x07\x05\x12\x03\x1a\x11\x16\n\x0c\n\x05\x04\
    \x01\x02\x07\x01\x12\x03\x1a\x17\"\n\x0c\n\x05\x04\x01\x02\x07\x03\x12\
    \x03\x1a%&\n4\n\x04\x04\x01\x02\x08\x12\x03\x1b\x08D\"'\x20PK\x20handle\
    \x20column\x20value\x20is\x20row\x20handle.\n\n\x0c\n\x05\x04\x01\x02\
    \x08\x04\x12\x03\x1b\x08\x10\n\x0c\n\x05\x04\x01\x02\x08\x05\x12\x03\x1b\
    \x11\x15\n\x0c\n\x05\x04\x01\x02\x08\x01\x12\x03\x1b\x16\x1f\n\x0c\n\x05\
    \x04\x01\x02\x08\x03\x12\x03\x1b\"$\n\x0c\n\x05\x04\x01\x02\x08\x08\x12\
    \x03\x1b%C\n\x0f\n\x08\x04\x01\x02\x08\x08\xe7\x07\0\x12\x03\x1b&B\n\x10\
    \n\t\x04\x01\x02\x08\x08\xe7\x07\0\x02\x12\x03\x1b&:\n\x11\n\n\x04\x01\
    \x02\x08\x08\xe7\x07\0\x02\0\x12\x03\x1b&:\n\x12\n\x0b\x04\x01\x02\x08\
    \x08\xe7\x07\0\x02\0\x01\x12\x03\x1b'9\n\x10\n\t\x04\x01\x02\x08\x08\xe7\
    \x07\0\x03\x12\x03\x1b=B\n\n\n\x02\x04\x02\x12\x04\x1e\0#\x01\n\n\n\x03\
    \x04\x02\x01\x12\x03\x1e\x08\x11\n\x0b\n\x04\x04\x02\x02\0\x12\x03\x1f\
    \x08C\n\x0c\n\x05\x04\x02\x02\0\x04\x12\x03\x1f\x08\x10\n\x0c\n\x05\x04\
    \x02\x02\0\x05\x12\x03\x1f\x11\x16\n\x0c\n\x05\x04\x02\x02\0\x01\x12\x03\
    \x1f\x17\x1f\n\x0c\n\x05\x04\x02\x02\0\x03\x12\x03\x1f\"#\n\x0c\n\x05\
    \x04\x02\x02\0\x08\x12\x03\x1f$B\n\x0f\n\x08\x04\x02\x02\0\x08\xe7\x07\0\
    \x12\x03\x1f%A\n\x10\n\t\x04\x02\x02\0\x08\xe7\x07\0\x02\x12\x03\x1f%9\n\
    \x11\n\n\x04\x02\x02\0\x08\xe7\x07\0\x02\0\x12\x03\x1f%9\n\x12\n\x0b\x04\
    \x02\x02\0\x08\xe7\x07\0\x02\0\x01\x12\x03\x1f&8\n\x10\n\t\x04\x02\x02\0\
    \x08\xe7\x07\0\x03\x12\x03\x1f<A\n\x0b\n\x04\x04\x02\x02\x01\x12\x03\x20\
    \x08C\n\x0c\n\x05\x04\x02\x02\x01\x04\x12\x03\x20\x08\x10\n\x0c\n\x05\
    \x04\x02\x02\x01\x05\x12\x03\x20\x11\x16\n\x0c\n\x05\x04\x02\x02\x01\x01\
    \x12\x03\x20\x17\x1f\n\x0c\n\x05\x04\x02\x02\x01\x03\x12\x03\x20\"#\n\
    \x0c\n\x05\x04\x02\x02\x01\x08\x12\x03\x20$B\n\x0f\n\x08\x04\x02\x02\x01\
    \x08\xe7\x07\0\x12\x03\x20%A\n\x10\n\t\x04\x02\x02\x01\x08\xe7\x07\0\x02\
    \x12\x03\x20%9\n\x11\n\n\x04\x02\x02\x01\x08\xe7\x07\0\x02\0\x12\x03\x20\
    %9\n\x12\n\x0b\x04\x02\x02\x01\x08\xe7\x07\0\x02\0\x01\x12\x03\x20&8\n\
    \x10\n\t\x04\x02\x02\x01\x08\xe7\x07\0\x03\x12\x03\x20<A\n\x0b\n\x04\x04\
    \x02\x02\x02\x12\x03!\x08(\n\x0c\n\x05\x04\x02\x02\x02\x04\x12\x03!\x08\
    \x10\n\x0c\n\x05\x04\x02\x02\x02\x06\x12\x03!\x11\x1b\n\x0c\n\x05\x04\
    \x02\x02\x02\x01\x12\x03!\x1c#\n\x0c\n\x05\x04\x02\x02\x02\x03\x12\x03!&\
    '\n\x0b\n\x04\x04\x02\x02\x03\x12\x03\"\x08@\n\x0c\n\x05\x04\x02\x02\x03\
    \x04\x12\x03\"\x08\x10\n\x0c\n\x05\x04\x02\x02\x03\x05\x12\x03\"\x11\x15\
    \n\x0c\n\x05\x04\x02\x02\x03\x01\x12\x03\"\x16\x1c\n\x0c\n\x05\x04\x02\
    \x02\x03\x03\x12\x03\"\x1f\x20\n\x0c\n\x05\x04\x02\x02\x03\x08\x12\x03\"\
    !?\n\x0f\n\x08\x04\x02\x02\x03\x08\xe7\x07\0\x12\x03\"\">\n\x10\n\t\x04\
    \x02\x02\x03\x08\xe7\x07\0\x02\x12\x03\"\"6\n\x11\n\n\x04\x02\x02\x03\
    \x08\xe7\x07\0\x02\0\x12\x03\"\"6\n\x12\n\x0b\x04\x02\x02\x03\x08\xe7\
    \x07\0\x02\0\x01\x12\x03\"#5\n\x10\n\t\x04\x02\x02\x03\x08\xe7\x07\0\x03\
    \x12\x03\"9>\ne\n\x02\x04\x03\x12\x04&\0)\x01\x1aY\x20KeyRange\x20is\x20\
    the\x20encoded\x20index\x20key\x20range,\x20low\x20is\x20closed,\x20high\
    \x20is\x20open.\x20(low\x20<=\x20x\x20<\x20high)\n\n\n\n\x03\x04\x03\x01\
    \x12\x03&\x08\x10\n\x0b\n\x04\x04\x03\x02\0\x12\x03'\x08\x1f\n\x0c\n\x05\
    \x04\x03\x02\0\x04\x12\x03'\x08\x10\n\x0c\n\x05\x04\x03\x02\0\x05\x12\
    \x03'\x11\x16\n\x0c\n\x05\x04\x03\x02\0\x01\x12\x03'\x17\x1a\n\x0c\n\x05\
    \x04\x03\x02\0\x03\x12\x03'\x1d\x1e\n\x0b\n\x04\x04\x03\x02\x01\x12\x03(\
    \x08\x20\n\x0c\n\x05\x04\x03\x02\x01\x04\x12\x03(\x08\x10\n\x0c\n\x05\
    \x04\x03\x02\x01\x05\x12\x03(\x11\x16\n\x0c\n\x05\x04\x03\x02\x01\x01\
    \x12\x03(\x17\x1b\n\x0c\n\x05\x04\x03\x02\x01\x03\x12\x03(\x1e\x1f\
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
