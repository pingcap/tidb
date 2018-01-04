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
pub struct Executor {
    // message fields
    tp: ::std::option::Option<ExecType>,
    tbl_scan: ::protobuf::SingularPtrField<TableScan>,
    idx_scan: ::protobuf::SingularPtrField<IndexScan>,
    selection: ::protobuf::SingularPtrField<Selection>,
    aggregation: ::protobuf::SingularPtrField<Aggregation>,
    topN: ::protobuf::SingularPtrField<TopN>,
    limit: ::protobuf::SingularPtrField<Limit>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for Executor {}

impl Executor {
    pub fn new() -> Executor {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static Executor {
        static mut instance: ::protobuf::lazy::Lazy<Executor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const Executor,
        };
        unsafe {
            instance.get(Executor::new)
        }
    }

    // optional .tipb.ExecType tp = 1;

    pub fn clear_tp(&mut self) {
        self.tp = ::std::option::Option::None;
    }

    pub fn has_tp(&self) -> bool {
        self.tp.is_some()
    }

    // Param is passed by value, moved
    pub fn set_tp(&mut self, v: ExecType) {
        self.tp = ::std::option::Option::Some(v);
    }

    pub fn get_tp(&self) -> ExecType {
        self.tp.unwrap_or(ExecType::TypeTableScan)
    }

    fn get_tp_for_reflect(&self) -> &::std::option::Option<ExecType> {
        &self.tp
    }

    fn mut_tp_for_reflect(&mut self) -> &mut ::std::option::Option<ExecType> {
        &mut self.tp
    }

    // optional .tipb.TableScan tbl_scan = 2;

    pub fn clear_tbl_scan(&mut self) {
        self.tbl_scan.clear();
    }

    pub fn has_tbl_scan(&self) -> bool {
        self.tbl_scan.is_some()
    }

    // Param is passed by value, moved
    pub fn set_tbl_scan(&mut self, v: TableScan) {
        self.tbl_scan = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_tbl_scan(&mut self) -> &mut TableScan {
        if self.tbl_scan.is_none() {
            self.tbl_scan.set_default();
        }
        self.tbl_scan.as_mut().unwrap()
    }

    // Take field
    pub fn take_tbl_scan(&mut self) -> TableScan {
        self.tbl_scan.take().unwrap_or_else(|| TableScan::new())
    }

    pub fn get_tbl_scan(&self) -> &TableScan {
        self.tbl_scan.as_ref().unwrap_or_else(|| TableScan::default_instance())
    }

    fn get_tbl_scan_for_reflect(&self) -> &::protobuf::SingularPtrField<TableScan> {
        &self.tbl_scan
    }

    fn mut_tbl_scan_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<TableScan> {
        &mut self.tbl_scan
    }

    // optional .tipb.IndexScan idx_scan = 3;

    pub fn clear_idx_scan(&mut self) {
        self.idx_scan.clear();
    }

    pub fn has_idx_scan(&self) -> bool {
        self.idx_scan.is_some()
    }

    // Param is passed by value, moved
    pub fn set_idx_scan(&mut self, v: IndexScan) {
        self.idx_scan = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_idx_scan(&mut self) -> &mut IndexScan {
        if self.idx_scan.is_none() {
            self.idx_scan.set_default();
        }
        self.idx_scan.as_mut().unwrap()
    }

    // Take field
    pub fn take_idx_scan(&mut self) -> IndexScan {
        self.idx_scan.take().unwrap_or_else(|| IndexScan::new())
    }

    pub fn get_idx_scan(&self) -> &IndexScan {
        self.idx_scan.as_ref().unwrap_or_else(|| IndexScan::default_instance())
    }

    fn get_idx_scan_for_reflect(&self) -> &::protobuf::SingularPtrField<IndexScan> {
        &self.idx_scan
    }

    fn mut_idx_scan_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<IndexScan> {
        &mut self.idx_scan
    }

    // optional .tipb.Selection selection = 4;

    pub fn clear_selection(&mut self) {
        self.selection.clear();
    }

    pub fn has_selection(&self) -> bool {
        self.selection.is_some()
    }

    // Param is passed by value, moved
    pub fn set_selection(&mut self, v: Selection) {
        self.selection = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_selection(&mut self) -> &mut Selection {
        if self.selection.is_none() {
            self.selection.set_default();
        }
        self.selection.as_mut().unwrap()
    }

    // Take field
    pub fn take_selection(&mut self) -> Selection {
        self.selection.take().unwrap_or_else(|| Selection::new())
    }

    pub fn get_selection(&self) -> &Selection {
        self.selection.as_ref().unwrap_or_else(|| Selection::default_instance())
    }

    fn get_selection_for_reflect(&self) -> &::protobuf::SingularPtrField<Selection> {
        &self.selection
    }

    fn mut_selection_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<Selection> {
        &mut self.selection
    }

    // optional .tipb.Aggregation aggregation = 5;

    pub fn clear_aggregation(&mut self) {
        self.aggregation.clear();
    }

    pub fn has_aggregation(&self) -> bool {
        self.aggregation.is_some()
    }

    // Param is passed by value, moved
    pub fn set_aggregation(&mut self, v: Aggregation) {
        self.aggregation = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_aggregation(&mut self) -> &mut Aggregation {
        if self.aggregation.is_none() {
            self.aggregation.set_default();
        }
        self.aggregation.as_mut().unwrap()
    }

    // Take field
    pub fn take_aggregation(&mut self) -> Aggregation {
        self.aggregation.take().unwrap_or_else(|| Aggregation::new())
    }

    pub fn get_aggregation(&self) -> &Aggregation {
        self.aggregation.as_ref().unwrap_or_else(|| Aggregation::default_instance())
    }

    fn get_aggregation_for_reflect(&self) -> &::protobuf::SingularPtrField<Aggregation> {
        &self.aggregation
    }

    fn mut_aggregation_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<Aggregation> {
        &mut self.aggregation
    }

    // optional .tipb.TopN topN = 6;

    pub fn clear_topN(&mut self) {
        self.topN.clear();
    }

    pub fn has_topN(&self) -> bool {
        self.topN.is_some()
    }

    // Param is passed by value, moved
    pub fn set_topN(&mut self, v: TopN) {
        self.topN = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_topN(&mut self) -> &mut TopN {
        if self.topN.is_none() {
            self.topN.set_default();
        }
        self.topN.as_mut().unwrap()
    }

    // Take field
    pub fn take_topN(&mut self) -> TopN {
        self.topN.take().unwrap_or_else(|| TopN::new())
    }

    pub fn get_topN(&self) -> &TopN {
        self.topN.as_ref().unwrap_or_else(|| TopN::default_instance())
    }

    fn get_topN_for_reflect(&self) -> &::protobuf::SingularPtrField<TopN> {
        &self.topN
    }

    fn mut_topN_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<TopN> {
        &mut self.topN
    }

    // optional .tipb.Limit limit = 7;

    pub fn clear_limit(&mut self) {
        self.limit.clear();
    }

    pub fn has_limit(&self) -> bool {
        self.limit.is_some()
    }

    // Param is passed by value, moved
    pub fn set_limit(&mut self, v: Limit) {
        self.limit = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_limit(&mut self) -> &mut Limit {
        if self.limit.is_none() {
            self.limit.set_default();
        }
        self.limit.as_mut().unwrap()
    }

    // Take field
    pub fn take_limit(&mut self) -> Limit {
        self.limit.take().unwrap_or_else(|| Limit::new())
    }

    pub fn get_limit(&self) -> &Limit {
        self.limit.as_ref().unwrap_or_else(|| Limit::default_instance())
    }

    fn get_limit_for_reflect(&self) -> &::protobuf::SingularPtrField<Limit> {
        &self.limit
    }

    fn mut_limit_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<Limit> {
        &mut self.limit
    }
}

impl ::protobuf::Message for Executor {
    fn is_initialized(&self) -> bool {
        for v in &self.tbl_scan {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.idx_scan {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.selection {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.aggregation {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.topN {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.limit {
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
                    let tmp = is.read_enum()?;
                    self.tp = ::std::option::Option::Some(tmp);
                },
                2 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.tbl_scan)?;
                },
                3 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.idx_scan)?;
                },
                4 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.selection)?;
                },
                5 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.aggregation)?;
                },
                6 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.topN)?;
                },
                7 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.limit)?;
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
        if let Some(v) = self.tp {
            my_size += ::protobuf::rt::enum_size(1, v);
        }
        if let Some(ref v) = self.tbl_scan.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if let Some(ref v) = self.idx_scan.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if let Some(ref v) = self.selection.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if let Some(ref v) = self.aggregation.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if let Some(ref v) = self.topN.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if let Some(ref v) = self.limit.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.tp {
            os.write_enum(1, v.value())?;
        }
        if let Some(ref v) = self.tbl_scan.as_ref() {
            os.write_tag(2, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if let Some(ref v) = self.idx_scan.as_ref() {
            os.write_tag(3, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if let Some(ref v) = self.selection.as_ref() {
            os.write_tag(4, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if let Some(ref v) = self.aggregation.as_ref() {
            os.write_tag(5, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if let Some(ref v) = self.topN.as_ref() {
            os.write_tag(6, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if let Some(ref v) = self.limit.as_ref() {
            os.write_tag(7, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
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

impl ::protobuf::MessageStatic for Executor {
    fn new() -> Executor {
        Executor::new()
    }

    fn descriptor_static(_: ::std::option::Option<Executor>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeEnum<ExecType>>(
                    "tp",
                    Executor::get_tp_for_reflect,
                    Executor::mut_tp_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<TableScan>>(
                    "tbl_scan",
                    Executor::get_tbl_scan_for_reflect,
                    Executor::mut_tbl_scan_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<IndexScan>>(
                    "idx_scan",
                    Executor::get_idx_scan_for_reflect,
                    Executor::mut_idx_scan_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<Selection>>(
                    "selection",
                    Executor::get_selection_for_reflect,
                    Executor::mut_selection_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<Aggregation>>(
                    "aggregation",
                    Executor::get_aggregation_for_reflect,
                    Executor::mut_aggregation_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<TopN>>(
                    "topN",
                    Executor::get_topN_for_reflect,
                    Executor::mut_topN_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<Limit>>(
                    "limit",
                    Executor::get_limit_for_reflect,
                    Executor::mut_limit_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<Executor>(
                    "Executor",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for Executor {
    fn clear(&mut self) {
        self.clear_tp();
        self.clear_tbl_scan();
        self.clear_idx_scan();
        self.clear_selection();
        self.clear_aggregation();
        self.clear_topN();
        self.clear_limit();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for Executor {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for Executor {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct TableScan {
    // message fields
    table_id: ::std::option::Option<i64>,
    columns: ::protobuf::RepeatedField<super::schema::ColumnInfo>,
    desc: ::std::option::Option<bool>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for TableScan {}

impl TableScan {
    pub fn new() -> TableScan {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static TableScan {
        static mut instance: ::protobuf::lazy::Lazy<TableScan> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const TableScan,
        };
        unsafe {
            instance.get(TableScan::new)
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
    pub fn set_columns(&mut self, v: ::protobuf::RepeatedField<super::schema::ColumnInfo>) {
        self.columns = v;
    }

    // Mutable pointer to the field.
    pub fn mut_columns(&mut self) -> &mut ::protobuf::RepeatedField<super::schema::ColumnInfo> {
        &mut self.columns
    }

    // Take field
    pub fn take_columns(&mut self) -> ::protobuf::RepeatedField<super::schema::ColumnInfo> {
        ::std::mem::replace(&mut self.columns, ::protobuf::RepeatedField::new())
    }

    pub fn get_columns(&self) -> &[super::schema::ColumnInfo] {
        &self.columns
    }

    fn get_columns_for_reflect(&self) -> &::protobuf::RepeatedField<super::schema::ColumnInfo> {
        &self.columns
    }

    fn mut_columns_for_reflect(&mut self) -> &mut ::protobuf::RepeatedField<super::schema::ColumnInfo> {
        &mut self.columns
    }

    // optional bool desc = 3;

    pub fn clear_desc(&mut self) {
        self.desc = ::std::option::Option::None;
    }

    pub fn has_desc(&self) -> bool {
        self.desc.is_some()
    }

    // Param is passed by value, moved
    pub fn set_desc(&mut self, v: bool) {
        self.desc = ::std::option::Option::Some(v);
    }

    pub fn get_desc(&self) -> bool {
        self.desc.unwrap_or(false)
    }

    fn get_desc_for_reflect(&self) -> &::std::option::Option<bool> {
        &self.desc
    }

    fn mut_desc_for_reflect(&mut self) -> &mut ::std::option::Option<bool> {
        &mut self.desc
    }
}

impl ::protobuf::Message for TableScan {
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
                3 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_bool()?;
                    self.desc = ::std::option::Option::Some(tmp);
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
        if let Some(v) = self.desc {
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
        for v in &self.columns {
            os.write_tag(2, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        };
        if let Some(v) = self.desc {
            os.write_bool(3, v)?;
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

impl ::protobuf::MessageStatic for TableScan {
    fn new() -> TableScan {
        TableScan::new()
    }

    fn descriptor_static(_: ::std::option::Option<TableScan>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeInt64>(
                    "table_id",
                    TableScan::get_table_id_for_reflect,
                    TableScan::mut_table_id_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_repeated_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<super::schema::ColumnInfo>>(
                    "columns",
                    TableScan::get_columns_for_reflect,
                    TableScan::mut_columns_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeBool>(
                    "desc",
                    TableScan::get_desc_for_reflect,
                    TableScan::mut_desc_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<TableScan>(
                    "TableScan",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for TableScan {
    fn clear(&mut self) {
        self.clear_table_id();
        self.clear_columns();
        self.clear_desc();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for TableScan {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for TableScan {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct IndexScan {
    // message fields
    table_id: ::std::option::Option<i64>,
    index_id: ::std::option::Option<i64>,
    columns: ::protobuf::RepeatedField<super::schema::ColumnInfo>,
    desc: ::std::option::Option<bool>,
    unique: ::std::option::Option<bool>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for IndexScan {}

impl IndexScan {
    pub fn new() -> IndexScan {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static IndexScan {
        static mut instance: ::protobuf::lazy::Lazy<IndexScan> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const IndexScan,
        };
        unsafe {
            instance.get(IndexScan::new)
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
    pub fn set_columns(&mut self, v: ::protobuf::RepeatedField<super::schema::ColumnInfo>) {
        self.columns = v;
    }

    // Mutable pointer to the field.
    pub fn mut_columns(&mut self) -> &mut ::protobuf::RepeatedField<super::schema::ColumnInfo> {
        &mut self.columns
    }

    // Take field
    pub fn take_columns(&mut self) -> ::protobuf::RepeatedField<super::schema::ColumnInfo> {
        ::std::mem::replace(&mut self.columns, ::protobuf::RepeatedField::new())
    }

    pub fn get_columns(&self) -> &[super::schema::ColumnInfo] {
        &self.columns
    }

    fn get_columns_for_reflect(&self) -> &::protobuf::RepeatedField<super::schema::ColumnInfo> {
        &self.columns
    }

    fn mut_columns_for_reflect(&mut self) -> &mut ::protobuf::RepeatedField<super::schema::ColumnInfo> {
        &mut self.columns
    }

    // optional bool desc = 4;

    pub fn clear_desc(&mut self) {
        self.desc = ::std::option::Option::None;
    }

    pub fn has_desc(&self) -> bool {
        self.desc.is_some()
    }

    // Param is passed by value, moved
    pub fn set_desc(&mut self, v: bool) {
        self.desc = ::std::option::Option::Some(v);
    }

    pub fn get_desc(&self) -> bool {
        self.desc.unwrap_or(false)
    }

    fn get_desc_for_reflect(&self) -> &::std::option::Option<bool> {
        &self.desc
    }

    fn mut_desc_for_reflect(&mut self) -> &mut ::std::option::Option<bool> {
        &mut self.desc
    }

    // optional bool unique = 5;

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

impl ::protobuf::Message for IndexScan {
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
                    self.desc = ::std::option::Option::Some(tmp);
                },
                5 => {
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
        if let Some(v) = self.desc {
            my_size += 2;
        }
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
        if let Some(v) = self.desc {
            os.write_bool(4, v)?;
        }
        if let Some(v) = self.unique {
            os.write_bool(5, v)?;
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

impl ::protobuf::MessageStatic for IndexScan {
    fn new() -> IndexScan {
        IndexScan::new()
    }

    fn descriptor_static(_: ::std::option::Option<IndexScan>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeInt64>(
                    "table_id",
                    IndexScan::get_table_id_for_reflect,
                    IndexScan::mut_table_id_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeInt64>(
                    "index_id",
                    IndexScan::get_index_id_for_reflect,
                    IndexScan::mut_index_id_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_repeated_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<super::schema::ColumnInfo>>(
                    "columns",
                    IndexScan::get_columns_for_reflect,
                    IndexScan::mut_columns_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeBool>(
                    "desc",
                    IndexScan::get_desc_for_reflect,
                    IndexScan::mut_desc_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeBool>(
                    "unique",
                    IndexScan::get_unique_for_reflect,
                    IndexScan::mut_unique_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<IndexScan>(
                    "IndexScan",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for IndexScan {
    fn clear(&mut self) {
        self.clear_table_id();
        self.clear_index_id();
        self.clear_columns();
        self.clear_desc();
        self.clear_unique();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for IndexScan {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for IndexScan {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct Selection {
    // message fields
    conditions: ::protobuf::RepeatedField<super::expression::Expr>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for Selection {}

impl Selection {
    pub fn new() -> Selection {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static Selection {
        static mut instance: ::protobuf::lazy::Lazy<Selection> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const Selection,
        };
        unsafe {
            instance.get(Selection::new)
        }
    }

    // repeated .tipb.Expr conditions = 1;

    pub fn clear_conditions(&mut self) {
        self.conditions.clear();
    }

    // Param is passed by value, moved
    pub fn set_conditions(&mut self, v: ::protobuf::RepeatedField<super::expression::Expr>) {
        self.conditions = v;
    }

    // Mutable pointer to the field.
    pub fn mut_conditions(&mut self) -> &mut ::protobuf::RepeatedField<super::expression::Expr> {
        &mut self.conditions
    }

    // Take field
    pub fn take_conditions(&mut self) -> ::protobuf::RepeatedField<super::expression::Expr> {
        ::std::mem::replace(&mut self.conditions, ::protobuf::RepeatedField::new())
    }

    pub fn get_conditions(&self) -> &[super::expression::Expr] {
        &self.conditions
    }

    fn get_conditions_for_reflect(&self) -> &::protobuf::RepeatedField<super::expression::Expr> {
        &self.conditions
    }

    fn mut_conditions_for_reflect(&mut self) -> &mut ::protobuf::RepeatedField<super::expression::Expr> {
        &mut self.conditions
    }
}

impl ::protobuf::Message for Selection {
    fn is_initialized(&self) -> bool {
        for v in &self.conditions {
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
                    ::protobuf::rt::read_repeated_message_into(wire_type, is, &mut self.conditions)?;
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
        for value in &self.conditions {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        for v in &self.conditions {
            os.write_tag(1, ::protobuf::wire_format::WireTypeLengthDelimited)?;
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

impl ::protobuf::MessageStatic for Selection {
    fn new() -> Selection {
        Selection::new()
    }

    fn descriptor_static(_: ::std::option::Option<Selection>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_repeated_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<super::expression::Expr>>(
                    "conditions",
                    Selection::get_conditions_for_reflect,
                    Selection::mut_conditions_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<Selection>(
                    "Selection",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for Selection {
    fn clear(&mut self) {
        self.clear_conditions();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for Selection {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for Selection {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct Projection {
    // message fields
    exprs: ::protobuf::RepeatedField<super::expression::Expr>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for Projection {}

impl Projection {
    pub fn new() -> Projection {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static Projection {
        static mut instance: ::protobuf::lazy::Lazy<Projection> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const Projection,
        };
        unsafe {
            instance.get(Projection::new)
        }
    }

    // repeated .tipb.Expr exprs = 1;

    pub fn clear_exprs(&mut self) {
        self.exprs.clear();
    }

    // Param is passed by value, moved
    pub fn set_exprs(&mut self, v: ::protobuf::RepeatedField<super::expression::Expr>) {
        self.exprs = v;
    }

    // Mutable pointer to the field.
    pub fn mut_exprs(&mut self) -> &mut ::protobuf::RepeatedField<super::expression::Expr> {
        &mut self.exprs
    }

    // Take field
    pub fn take_exprs(&mut self) -> ::protobuf::RepeatedField<super::expression::Expr> {
        ::std::mem::replace(&mut self.exprs, ::protobuf::RepeatedField::new())
    }

    pub fn get_exprs(&self) -> &[super::expression::Expr] {
        &self.exprs
    }

    fn get_exprs_for_reflect(&self) -> &::protobuf::RepeatedField<super::expression::Expr> {
        &self.exprs
    }

    fn mut_exprs_for_reflect(&mut self) -> &mut ::protobuf::RepeatedField<super::expression::Expr> {
        &mut self.exprs
    }
}

impl ::protobuf::Message for Projection {
    fn is_initialized(&self) -> bool {
        for v in &self.exprs {
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
                    ::protobuf::rt::read_repeated_message_into(wire_type, is, &mut self.exprs)?;
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
        for value in &self.exprs {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        for v in &self.exprs {
            os.write_tag(1, ::protobuf::wire_format::WireTypeLengthDelimited)?;
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

impl ::protobuf::MessageStatic for Projection {
    fn new() -> Projection {
        Projection::new()
    }

    fn descriptor_static(_: ::std::option::Option<Projection>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_repeated_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<super::expression::Expr>>(
                    "exprs",
                    Projection::get_exprs_for_reflect,
                    Projection::mut_exprs_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<Projection>(
                    "Projection",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for Projection {
    fn clear(&mut self) {
        self.clear_exprs();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for Projection {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for Projection {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct Aggregation {
    // message fields
    group_by: ::protobuf::RepeatedField<super::expression::Expr>,
    agg_func: ::protobuf::RepeatedField<super::expression::Expr>,
    streamed: ::std::option::Option<bool>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for Aggregation {}

impl Aggregation {
    pub fn new() -> Aggregation {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static Aggregation {
        static mut instance: ::protobuf::lazy::Lazy<Aggregation> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const Aggregation,
        };
        unsafe {
            instance.get(Aggregation::new)
        }
    }

    // repeated .tipb.Expr group_by = 1;

    pub fn clear_group_by(&mut self) {
        self.group_by.clear();
    }

    // Param is passed by value, moved
    pub fn set_group_by(&mut self, v: ::protobuf::RepeatedField<super::expression::Expr>) {
        self.group_by = v;
    }

    // Mutable pointer to the field.
    pub fn mut_group_by(&mut self) -> &mut ::protobuf::RepeatedField<super::expression::Expr> {
        &mut self.group_by
    }

    // Take field
    pub fn take_group_by(&mut self) -> ::protobuf::RepeatedField<super::expression::Expr> {
        ::std::mem::replace(&mut self.group_by, ::protobuf::RepeatedField::new())
    }

    pub fn get_group_by(&self) -> &[super::expression::Expr] {
        &self.group_by
    }

    fn get_group_by_for_reflect(&self) -> &::protobuf::RepeatedField<super::expression::Expr> {
        &self.group_by
    }

    fn mut_group_by_for_reflect(&mut self) -> &mut ::protobuf::RepeatedField<super::expression::Expr> {
        &mut self.group_by
    }

    // repeated .tipb.Expr agg_func = 2;

    pub fn clear_agg_func(&mut self) {
        self.agg_func.clear();
    }

    // Param is passed by value, moved
    pub fn set_agg_func(&mut self, v: ::protobuf::RepeatedField<super::expression::Expr>) {
        self.agg_func = v;
    }

    // Mutable pointer to the field.
    pub fn mut_agg_func(&mut self) -> &mut ::protobuf::RepeatedField<super::expression::Expr> {
        &mut self.agg_func
    }

    // Take field
    pub fn take_agg_func(&mut self) -> ::protobuf::RepeatedField<super::expression::Expr> {
        ::std::mem::replace(&mut self.agg_func, ::protobuf::RepeatedField::new())
    }

    pub fn get_agg_func(&self) -> &[super::expression::Expr] {
        &self.agg_func
    }

    fn get_agg_func_for_reflect(&self) -> &::protobuf::RepeatedField<super::expression::Expr> {
        &self.agg_func
    }

    fn mut_agg_func_for_reflect(&mut self) -> &mut ::protobuf::RepeatedField<super::expression::Expr> {
        &mut self.agg_func
    }

    // optional bool streamed = 3;

    pub fn clear_streamed(&mut self) {
        self.streamed = ::std::option::Option::None;
    }

    pub fn has_streamed(&self) -> bool {
        self.streamed.is_some()
    }

    // Param is passed by value, moved
    pub fn set_streamed(&mut self, v: bool) {
        self.streamed = ::std::option::Option::Some(v);
    }

    pub fn get_streamed(&self) -> bool {
        self.streamed.unwrap_or(false)
    }

    fn get_streamed_for_reflect(&self) -> &::std::option::Option<bool> {
        &self.streamed
    }

    fn mut_streamed_for_reflect(&mut self) -> &mut ::std::option::Option<bool> {
        &mut self.streamed
    }
}

impl ::protobuf::Message for Aggregation {
    fn is_initialized(&self) -> bool {
        for v in &self.group_by {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.agg_func {
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
                    ::protobuf::rt::read_repeated_message_into(wire_type, is, &mut self.group_by)?;
                },
                2 => {
                    ::protobuf::rt::read_repeated_message_into(wire_type, is, &mut self.agg_func)?;
                },
                3 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_bool()?;
                    self.streamed = ::std::option::Option::Some(tmp);
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
        for value in &self.group_by {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        for value in &self.agg_func {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        if let Some(v) = self.streamed {
            my_size += 2;
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        for v in &self.group_by {
            os.write_tag(1, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        };
        for v in &self.agg_func {
            os.write_tag(2, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        };
        if let Some(v) = self.streamed {
            os.write_bool(3, v)?;
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

impl ::protobuf::MessageStatic for Aggregation {
    fn new() -> Aggregation {
        Aggregation::new()
    }

    fn descriptor_static(_: ::std::option::Option<Aggregation>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_repeated_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<super::expression::Expr>>(
                    "group_by",
                    Aggregation::get_group_by_for_reflect,
                    Aggregation::mut_group_by_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_repeated_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<super::expression::Expr>>(
                    "agg_func",
                    Aggregation::get_agg_func_for_reflect,
                    Aggregation::mut_agg_func_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeBool>(
                    "streamed",
                    Aggregation::get_streamed_for_reflect,
                    Aggregation::mut_streamed_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<Aggregation>(
                    "Aggregation",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for Aggregation {
    fn clear(&mut self) {
        self.clear_group_by();
        self.clear_agg_func();
        self.clear_streamed();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for Aggregation {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for Aggregation {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct TopN {
    // message fields
    order_by: ::protobuf::RepeatedField<super::expression::ByItem>,
    limit: ::std::option::Option<u64>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for TopN {}

impl TopN {
    pub fn new() -> TopN {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static TopN {
        static mut instance: ::protobuf::lazy::Lazy<TopN> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const TopN,
        };
        unsafe {
            instance.get(TopN::new)
        }
    }

    // repeated .tipb.ByItem order_by = 1;

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

    // optional uint64 limit = 2;

    pub fn clear_limit(&mut self) {
        self.limit = ::std::option::Option::None;
    }

    pub fn has_limit(&self) -> bool {
        self.limit.is_some()
    }

    // Param is passed by value, moved
    pub fn set_limit(&mut self, v: u64) {
        self.limit = ::std::option::Option::Some(v);
    }

    pub fn get_limit(&self) -> u64 {
        self.limit.unwrap_or(0)
    }

    fn get_limit_for_reflect(&self) -> &::std::option::Option<u64> {
        &self.limit
    }

    fn mut_limit_for_reflect(&mut self) -> &mut ::std::option::Option<u64> {
        &mut self.limit
    }
}

impl ::protobuf::Message for TopN {
    fn is_initialized(&self) -> bool {
        for v in &self.order_by {
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
                    ::protobuf::rt::read_repeated_message_into(wire_type, is, &mut self.order_by)?;
                },
                2 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_uint64()?;
                    self.limit = ::std::option::Option::Some(tmp);
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
        for value in &self.order_by {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        if let Some(v) = self.limit {
            my_size += ::protobuf::rt::value_size(2, v, ::protobuf::wire_format::WireTypeVarint);
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        for v in &self.order_by {
            os.write_tag(1, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        };
        if let Some(v) = self.limit {
            os.write_uint64(2, v)?;
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

impl ::protobuf::MessageStatic for TopN {
    fn new() -> TopN {
        TopN::new()
    }

    fn descriptor_static(_: ::std::option::Option<TopN>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_repeated_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<super::expression::ByItem>>(
                    "order_by",
                    TopN::get_order_by_for_reflect,
                    TopN::mut_order_by_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "limit",
                    TopN::get_limit_for_reflect,
                    TopN::mut_limit_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<TopN>(
                    "TopN",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for TopN {
    fn clear(&mut self) {
        self.clear_order_by();
        self.clear_limit();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for TopN {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for TopN {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct Limit {
    // message fields
    limit: ::std::option::Option<u64>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for Limit {}

impl Limit {
    pub fn new() -> Limit {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static Limit {
        static mut instance: ::protobuf::lazy::Lazy<Limit> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const Limit,
        };
        unsafe {
            instance.get(Limit::new)
        }
    }

    // optional uint64 limit = 1;

    pub fn clear_limit(&mut self) {
        self.limit = ::std::option::Option::None;
    }

    pub fn has_limit(&self) -> bool {
        self.limit.is_some()
    }

    // Param is passed by value, moved
    pub fn set_limit(&mut self, v: u64) {
        self.limit = ::std::option::Option::Some(v);
    }

    pub fn get_limit(&self) -> u64 {
        self.limit.unwrap_or(0)
    }

    fn get_limit_for_reflect(&self) -> &::std::option::Option<u64> {
        &self.limit
    }

    fn mut_limit_for_reflect(&mut self) -> &mut ::std::option::Option<u64> {
        &mut self.limit
    }
}

impl ::protobuf::Message for Limit {
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
                    let tmp = is.read_uint64()?;
                    self.limit = ::std::option::Option::Some(tmp);
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
        if let Some(v) = self.limit {
            my_size += ::protobuf::rt::value_size(1, v, ::protobuf::wire_format::WireTypeVarint);
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.limit {
            os.write_uint64(1, v)?;
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

impl ::protobuf::MessageStatic for Limit {
    fn new() -> Limit {
        Limit::new()
    }

    fn descriptor_static(_: ::std::option::Option<Limit>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "limit",
                    Limit::get_limit_for_reflect,
                    Limit::mut_limit_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<Limit>(
                    "Limit",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for Limit {
    fn clear(&mut self) {
        self.clear_limit();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for Limit {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for Limit {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(Clone,PartialEq,Eq,Debug,Hash)]
pub enum ExecType {
    TypeTableScan = 0,
    TypeIndexScan = 1,
    TypeSelection = 2,
    TypeAggregation = 3,
    TypeTopN = 4,
    TypeLimit = 5,
}

impl ::protobuf::ProtobufEnum for ExecType {
    fn value(&self) -> i32 {
        *self as i32
    }

    fn from_i32(value: i32) -> ::std::option::Option<ExecType> {
        match value {
            0 => ::std::option::Option::Some(ExecType::TypeTableScan),
            1 => ::std::option::Option::Some(ExecType::TypeIndexScan),
            2 => ::std::option::Option::Some(ExecType::TypeSelection),
            3 => ::std::option::Option::Some(ExecType::TypeAggregation),
            4 => ::std::option::Option::Some(ExecType::TypeTopN),
            5 => ::std::option::Option::Some(ExecType::TypeLimit),
            _ => ::std::option::Option::None
        }
    }

    fn values() -> &'static [Self] {
        static values: &'static [ExecType] = &[
            ExecType::TypeTableScan,
            ExecType::TypeIndexScan,
            ExecType::TypeSelection,
            ExecType::TypeAggregation,
            ExecType::TypeTopN,
            ExecType::TypeLimit,
        ];
        values
    }

    fn enum_descriptor_static(_: ::std::option::Option<ExecType>) -> &'static ::protobuf::reflect::EnumDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::EnumDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::EnumDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                ::protobuf::reflect::EnumDescriptor::new("ExecType", file_descriptor_proto())
            })
        }
    }
}

impl ::std::marker::Copy for ExecType {
}

impl ::protobuf::reflect::ProtobufValue for ExecType {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Enum(self.descriptor())
    }
}

static file_descriptor_proto_data: &'static [u8] = b"\
    \n\x0eexecutor.proto\x12\x04tipb\x1a\x10expression.proto\x1a\x0cschema.p\
    roto\x1a\x14gogoproto/gogo.proto\"\xaf\x02\n\x08Executor\x12$\n\x02tp\
    \x18\x01\x20\x01(\x0e2\x0e.tipb.ExecTypeR\x02tpB\x04\xc8\xde\x1f\0\x12*\
    \n\x08tbl_scan\x18\x02\x20\x01(\x0b2\x0f.tipb.TableScanR\x07tblScan\x12*\
    \n\x08idx_scan\x18\x03\x20\x01(\x0b2\x0f.tipb.IndexScanR\x07idxScan\x12-\
    \n\tselection\x18\x04\x20\x01(\x0b2\x0f.tipb.SelectionR\tselection\x123\
    \n\x0baggregation\x18\x05\x20\x01(\x0b2\x11.tipb.AggregationR\x0baggrega\
    tion\x12\x1e\n\x04topN\x18\x06\x20\x01(\x0b2\n.tipb.TopNR\x04topN\x12!\n\
    \x05limit\x18\x07\x20\x01(\x0b2\x0b.tipb.LimitR\x05limit\"r\n\tTableScan\
    \x12\x1f\n\x08table_id\x18\x01\x20\x01(\x03R\x07tableIdB\x04\xc8\xde\x1f\
    \0\x12*\n\x07columns\x18\x02\x20\x03(\x0b2\x10.tipb.ColumnInfoR\x07colum\
    ns\x12\x18\n\x04desc\x18\x03\x20\x01(\x08R\x04descB\x04\xc8\xde\x1f\0\"\
    \xab\x01\n\tIndexScan\x12\x1f\n\x08table_id\x18\x01\x20\x01(\x03R\x07tab\
    leIdB\x04\xc8\xde\x1f\0\x12\x1f\n\x08index_id\x18\x02\x20\x01(\x03R\x07i\
    ndexIdB\x04\xc8\xde\x1f\0\x12*\n\x07columns\x18\x03\x20\x03(\x0b2\x10.ti\
    pb.ColumnInfoR\x07columns\x12\x18\n\x04desc\x18\x04\x20\x01(\x08R\x04des\
    cB\x04\xc8\xde\x1f\0\x12\x16\n\x06unique\x18\x05\x20\x01(\x08R\x06unique\
    \"7\n\tSelection\x12*\n\nconditions\x18\x01\x20\x03(\x0b2\n.tipb.ExprR\n\
    conditions\".\n\nProjection\x12\x20\n\x05exprs\x18\x01\x20\x03(\x0b2\n.t\
    ipb.ExprR\x05exprs\"}\n\x0bAggregation\x12%\n\x08group_by\x18\x01\x20\
    \x03(\x0b2\n.tipb.ExprR\x07groupBy\x12%\n\x08agg_func\x18\x02\x20\x03(\
    \x0b2\n.tipb.ExprR\x07aggFunc\x12\x20\n\x08streamed\x18\x03\x20\x01(\x08\
    R\x08streamedB\x04\xc8\xde\x1f\0\"K\n\x04TopN\x12'\n\x08order_by\x18\x01\
    \x20\x03(\x0b2\x0c.tipb.ByItemR\x07orderBy\x12\x1a\n\x05limit\x18\x02\
    \x20\x01(\x04R\x05limitB\x04\xc8\xde\x1f\0\"#\n\x05Limit\x12\x1a\n\x05li\
    mit\x18\x01\x20\x01(\x04R\x05limitB\x04\xc8\xde\x1f\0*u\n\x08ExecType\
    \x12\x11\n\rTypeTableScan\x10\0\x12\x11\n\rTypeIndexScan\x10\x01\x12\x11\
    \n\rTypeSelection\x10\x02\x12\x13\n\x0fTypeAggregation\x10\x03\x12\x0c\n\
    \x08TypeTopN\x10\x04\x12\r\n\tTypeLimit\x10\x05B%\n\x15com.pingcap.tidb.\
    tipbP\x01\xc8\xe2\x1e\x01\xe0\xe2\x1e\x01\xd0\xe2\x1e\x01J\x8c\x1d\n\x06\
    \x12\x04\0\0M\x01\n\x08\n\x01\x0c\x12\x03\0\0\x12\n\x08\n\x01\x02\x12\
    \x03\x02\x08\x0c\n\x08\n\x01\x08\x12\x03\x04\0\"\n\x0b\n\x04\x08\xe7\x07\
    \0\x12\x03\x04\0\"\n\x0c\n\x05\x08\xe7\x07\0\x02\x12\x03\x04\x07\x1a\n\r\
    \n\x06\x08\xe7\x07\0\x02\0\x12\x03\x04\x07\x1a\n\x0e\n\x07\x08\xe7\x07\0\
    \x02\0\x01\x12\x03\x04\x07\x1a\n\x0c\n\x05\x08\xe7\x07\0\x03\x12\x03\x04\
    \x1d!\n\x08\n\x01\x08\x12\x03\x05\0.\n\x0b\n\x04\x08\xe7\x07\x01\x12\x03\
    \x05\0.\n\x0c\n\x05\x08\xe7\x07\x01\x02\x12\x03\x05\x07\x13\n\r\n\x06\
    \x08\xe7\x07\x01\x02\0\x12\x03\x05\x07\x13\n\x0e\n\x07\x08\xe7\x07\x01\
    \x02\0\x01\x12\x03\x05\x07\x13\n\x0c\n\x05\x08\xe7\x07\x01\x07\x12\x03\
    \x05\x16-\n\t\n\x02\x03\0\x12\x03\x07\x07\x19\n\t\n\x02\x03\x01\x12\x03\
    \x08\x07\x15\n\t\n\x02\x03\x02\x12\x03\t\x07\x1d\n\x08\n\x01\x08\x12\x03\
    \x0b\0(\n\x0b\n\x04\x08\xe7\x07\x02\x12\x03\x0b\0(\n\x0c\n\x05\x08\xe7\
    \x07\x02\x02\x12\x03\x0b\x07\x20\n\r\n\x06\x08\xe7\x07\x02\x02\0\x12\x03\
    \x0b\x07\x20\n\x0e\n\x07\x08\xe7\x07\x02\x02\0\x01\x12\x03\x0b\x08\x1f\n\
    \x0c\n\x05\x08\xe7\x07\x02\x03\x12\x03\x0b#'\n\x08\n\x01\x08\x12\x03\x0c\
    \0$\n\x0b\n\x04\x08\xe7\x07\x03\x12\x03\x0c\0$\n\x0c\n\x05\x08\xe7\x07\
    \x03\x02\x12\x03\x0c\x07\x1c\n\r\n\x06\x08\xe7\x07\x03\x02\0\x12\x03\x0c\
    \x07\x1c\n\x0e\n\x07\x08\xe7\x07\x03\x02\0\x01\x12\x03\x0c\x08\x1b\n\x0c\
    \n\x05\x08\xe7\x07\x03\x03\x12\x03\x0c\x1f#\n\x08\n\x01\x08\x12\x03\r\0*\
    \n\x0b\n\x04\x08\xe7\x07\x04\x12\x03\r\0*\n\x0c\n\x05\x08\xe7\x07\x04\
    \x02\x12\x03\r\x07\"\n\r\n\x06\x08\xe7\x07\x04\x02\0\x12\x03\r\x07\"\n\
    \x0e\n\x07\x08\xe7\x07\x04\x02\0\x01\x12\x03\r\x08!\n\x0c\n\x05\x08\xe7\
    \x07\x04\x03\x12\x03\r%)\n\n\n\x02\x05\0\x12\x04\x0f\0\x16\x01\n\n\n\x03\
    \x05\0\x01\x12\x03\x0f\x05\r\n\x0b\n\x04\x05\0\x02\0\x12\x03\x10\x08\x1a\
    \n\x0c\n\x05\x05\0\x02\0\x01\x12\x03\x10\x08\x15\n\x0c\n\x05\x05\0\x02\0\
    \x02\x12\x03\x10\x18\x19\n\x0b\n\x04\x05\0\x02\x01\x12\x03\x11\x08\x1a\n\
    \x0c\n\x05\x05\0\x02\x01\x01\x12\x03\x11\x08\x15\n\x0c\n\x05\x05\0\x02\
    \x01\x02\x12\x03\x11\x18\x19\n\x0b\n\x04\x05\0\x02\x02\x12\x03\x12\x08\
    \x1a\n\x0c\n\x05\x05\0\x02\x02\x01\x12\x03\x12\x08\x15\n\x0c\n\x05\x05\0\
    \x02\x02\x02\x12\x03\x12\x18\x19\n\x0b\n\x04\x05\0\x02\x03\x12\x03\x13\
    \x08\x1c\n\x0c\n\x05\x05\0\x02\x03\x01\x12\x03\x13\x08\x17\n\x0c\n\x05\
    \x05\0\x02\x03\x02\x12\x03\x13\x1a\x1b\n\x0b\n\x04\x05\0\x02\x04\x12\x03\
    \x14\x08\x15\n\x0c\n\x05\x05\0\x02\x04\x01\x12\x03\x14\x08\x10\n\x0c\n\
    \x05\x05\0\x02\x04\x02\x12\x03\x14\x13\x14\n\x0b\n\x04\x05\0\x02\x05\x12\
    \x03\x15\x08\x16\n\x0c\n\x05\x05\0\x02\x05\x01\x12\x03\x15\x08\x11\n\x0c\
    \n\x05\x05\0\x02\x05\x02\x12\x03\x15\x14\x15\n'\n\x02\x04\0\x12\x04\x19\
    \0!\x01\x1a\x1b\x20It\x20represents\x20a\x20Executor.\n\n\n\n\x03\x04\0\
    \x01\x12\x03\x19\x08\x10\n\x0b\n\x04\x04\0\x02\0\x12\x03\x1a\x08@\n\x0c\
    \n\x05\x04\0\x02\0\x04\x12\x03\x1a\x08\x10\n\x0c\n\x05\x04\0\x02\0\x06\
    \x12\x03\x1a\x11\x19\n\x0c\n\x05\x04\0\x02\0\x01\x12\x03\x1a\x1a\x1c\n\
    \x0c\n\x05\x04\0\x02\0\x03\x12\x03\x1a\x1f\x20\n\x0c\n\x05\x04\0\x02\0\
    \x08\x12\x03\x1a!?\n\x0f\n\x08\x04\0\x02\0\x08\xe7\x07\0\x12\x03\x1a\">\
    \n\x10\n\t\x04\0\x02\0\x08\xe7\x07\0\x02\x12\x03\x1a\"6\n\x11\n\n\x04\0\
    \x02\0\x08\xe7\x07\0\x02\0\x12\x03\x1a\"6\n\x12\n\x0b\x04\0\x02\0\x08\
    \xe7\x07\0\x02\0\x01\x12\x03\x1a#5\n\x10\n\t\x04\0\x02\0\x08\xe7\x07\0\
    \x03\x12\x03\x1a9>\n\x0b\n\x04\x04\0\x02\x01\x12\x03\x1b\x08(\n\x0c\n\
    \x05\x04\0\x02\x01\x04\x12\x03\x1b\x08\x10\n\x0c\n\x05\x04\0\x02\x01\x06\
    \x12\x03\x1b\x11\x1a\n\x0c\n\x05\x04\0\x02\x01\x01\x12\x03\x1b\x1b#\n\
    \x0c\n\x05\x04\0\x02\x01\x03\x12\x03\x1b&'\n\x0b\n\x04\x04\0\x02\x02\x12\
    \x03\x1c\x08(\n\x0c\n\x05\x04\0\x02\x02\x04\x12\x03\x1c\x08\x10\n\x0c\n\
    \x05\x04\0\x02\x02\x06\x12\x03\x1c\x11\x1a\n\x0c\n\x05\x04\0\x02\x02\x01\
    \x12\x03\x1c\x1b#\n\x0c\n\x05\x04\0\x02\x02\x03\x12\x03\x1c&'\n\x0b\n\
    \x04\x04\0\x02\x03\x12\x03\x1d\x08)\n\x0c\n\x05\x04\0\x02\x03\x04\x12\
    \x03\x1d\x08\x10\n\x0c\n\x05\x04\0\x02\x03\x06\x12\x03\x1d\x11\x1a\n\x0c\
    \n\x05\x04\0\x02\x03\x01\x12\x03\x1d\x1b$\n\x0c\n\x05\x04\0\x02\x03\x03\
    \x12\x03\x1d'(\n\x0b\n\x04\x04\0\x02\x04\x12\x03\x1e\x08-\n\x0c\n\x05\
    \x04\0\x02\x04\x04\x12\x03\x1e\x08\x10\n\x0c\n\x05\x04\0\x02\x04\x06\x12\
    \x03\x1e\x11\x1c\n\x0c\n\x05\x04\0\x02\x04\x01\x12\x03\x1e\x1d(\n\x0c\n\
    \x05\x04\0\x02\x04\x03\x12\x03\x1e+,\n\x0b\n\x04\x04\0\x02\x05\x12\x03\
    \x1f\x08\x1f\n\x0c\n\x05\x04\0\x02\x05\x04\x12\x03\x1f\x08\x10\n\x0c\n\
    \x05\x04\0\x02\x05\x06\x12\x03\x1f\x11\x15\n\x0c\n\x05\x04\0\x02\x05\x01\
    \x12\x03\x1f\x16\x1a\n\x0c\n\x05\x04\0\x02\x05\x03\x12\x03\x1f\x1d\x1e\n\
    \x0b\n\x04\x04\0\x02\x06\x12\x03\x20\x08!\n\x0c\n\x05\x04\0\x02\x06\x04\
    \x12\x03\x20\x08\x10\n\x0c\n\x05\x04\0\x02\x06\x06\x12\x03\x20\x11\x16\n\
    \x0c\n\x05\x04\0\x02\x06\x01\x12\x03\x20\x17\x1c\n\x0c\n\x05\x04\0\x02\
    \x06\x03\x12\x03\x20\x1f\x20\n\n\n\x02\x04\x01\x12\x04#\0'\x01\n\n\n\x03\
    \x04\x01\x01\x12\x03#\x08\x11\n\x0b\n\x04\x04\x01\x02\0\x12\x03$\x08C\n\
    \x0c\n\x05\x04\x01\x02\0\x04\x12\x03$\x08\x10\n\x0c\n\x05\x04\x01\x02\0\
    \x05\x12\x03$\x11\x16\n\x0c\n\x05\x04\x01\x02\0\x01\x12\x03$\x17\x1f\n\
    \x0c\n\x05\x04\x01\x02\0\x03\x12\x03$\"#\n\x0c\n\x05\x04\x01\x02\0\x08\
    \x12\x03$$B\n\x0f\n\x08\x04\x01\x02\0\x08\xe7\x07\0\x12\x03$%A\n\x10\n\t\
    \x04\x01\x02\0\x08\xe7\x07\0\x02\x12\x03$%9\n\x11\n\n\x04\x01\x02\0\x08\
    \xe7\x07\0\x02\0\x12\x03$%9\n\x12\n\x0b\x04\x01\x02\0\x08\xe7\x07\0\x02\
    \0\x01\x12\x03$&8\n\x10\n\t\x04\x01\x02\0\x08\xe7\x07\0\x03\x12\x03$<A\n\
    \x0b\n\x04\x04\x01\x02\x01\x12\x03%\x08(\n\x0c\n\x05\x04\x01\x02\x01\x04\
    \x12\x03%\x08\x10\n\x0c\n\x05\x04\x01\x02\x01\x06\x12\x03%\x11\x1b\n\x0c\
    \n\x05\x04\x01\x02\x01\x01\x12\x03%\x1c#\n\x0c\n\x05\x04\x01\x02\x01\x03\
    \x12\x03%&'\n\x0b\n\x04\x04\x01\x02\x02\x12\x03&\x08>\n\x0c\n\x05\x04\
    \x01\x02\x02\x04\x12\x03&\x08\x10\n\x0c\n\x05\x04\x01\x02\x02\x05\x12\
    \x03&\x11\x15\n\x0c\n\x05\x04\x01\x02\x02\x01\x12\x03&\x16\x1a\n\x0c\n\
    \x05\x04\x01\x02\x02\x03\x12\x03&\x1d\x1e\n\x0c\n\x05\x04\x01\x02\x02\
    \x08\x12\x03&\x1f=\n\x0f\n\x08\x04\x01\x02\x02\x08\xe7\x07\0\x12\x03&\
    \x20<\n\x10\n\t\x04\x01\x02\x02\x08\xe7\x07\0\x02\x12\x03&\x204\n\x11\n\
    \n\x04\x01\x02\x02\x08\xe7\x07\0\x02\0\x12\x03&\x204\n\x12\n\x0b\x04\x01\
    \x02\x02\x08\xe7\x07\0\x02\0\x01\x12\x03&!3\n\x10\n\t\x04\x01\x02\x02\
    \x08\xe7\x07\0\x03\x12\x03&7<\n\n\n\x02\x04\x02\x12\x04)\0/\x01\n\n\n\
    \x03\x04\x02\x01\x12\x03)\x08\x11\n\x0b\n\x04\x04\x02\x02\0\x12\x03*\x08\
    C\n\x0c\n\x05\x04\x02\x02\0\x04\x12\x03*\x08\x10\n\x0c\n\x05\x04\x02\x02\
    \0\x05\x12\x03*\x11\x16\n\x0c\n\x05\x04\x02\x02\0\x01\x12\x03*\x17\x1f\n\
    \x0c\n\x05\x04\x02\x02\0\x03\x12\x03*\"#\n\x0c\n\x05\x04\x02\x02\0\x08\
    \x12\x03*$B\n\x0f\n\x08\x04\x02\x02\0\x08\xe7\x07\0\x12\x03*%A\n\x10\n\t\
    \x04\x02\x02\0\x08\xe7\x07\0\x02\x12\x03*%9\n\x11\n\n\x04\x02\x02\0\x08\
    \xe7\x07\0\x02\0\x12\x03*%9\n\x12\n\x0b\x04\x02\x02\0\x08\xe7\x07\0\x02\
    \0\x01\x12\x03*&8\n\x10\n\t\x04\x02\x02\0\x08\xe7\x07\0\x03\x12\x03*<A\n\
    \x0b\n\x04\x04\x02\x02\x01\x12\x03+\x08C\n\x0c\n\x05\x04\x02\x02\x01\x04\
    \x12\x03+\x08\x10\n\x0c\n\x05\x04\x02\x02\x01\x05\x12\x03+\x11\x16\n\x0c\
    \n\x05\x04\x02\x02\x01\x01\x12\x03+\x17\x1f\n\x0c\n\x05\x04\x02\x02\x01\
    \x03\x12\x03+\"#\n\x0c\n\x05\x04\x02\x02\x01\x08\x12\x03+$B\n\x0f\n\x08\
    \x04\x02\x02\x01\x08\xe7\x07\0\x12\x03+%A\n\x10\n\t\x04\x02\x02\x01\x08\
    \xe7\x07\0\x02\x12\x03+%9\n\x11\n\n\x04\x02\x02\x01\x08\xe7\x07\0\x02\0\
    \x12\x03+%9\n\x12\n\x0b\x04\x02\x02\x01\x08\xe7\x07\0\x02\0\x01\x12\x03+\
    &8\n\x10\n\t\x04\x02\x02\x01\x08\xe7\x07\0\x03\x12\x03+<A\n\x0b\n\x04\
    \x04\x02\x02\x02\x12\x03,\x08(\n\x0c\n\x05\x04\x02\x02\x02\x04\x12\x03,\
    \x08\x10\n\x0c\n\x05\x04\x02\x02\x02\x06\x12\x03,\x11\x1b\n\x0c\n\x05\
    \x04\x02\x02\x02\x01\x12\x03,\x1c#\n\x0c\n\x05\x04\x02\x02\x02\x03\x12\
    \x03,&'\n\x0b\n\x04\x04\x02\x02\x03\x12\x03-\x08>\n\x0c\n\x05\x04\x02\
    \x02\x03\x04\x12\x03-\x08\x10\n\x0c\n\x05\x04\x02\x02\x03\x05\x12\x03-\
    \x11\x15\n\x0c\n\x05\x04\x02\x02\x03\x01\x12\x03-\x16\x1a\n\x0c\n\x05\
    \x04\x02\x02\x03\x03\x12\x03-\x1d\x1e\n\x0c\n\x05\x04\x02\x02\x03\x08\
    \x12\x03-\x1f=\n\x0f\n\x08\x04\x02\x02\x03\x08\xe7\x07\0\x12\x03-\x20<\n\
    \x10\n\t\x04\x02\x02\x03\x08\xe7\x07\0\x02\x12\x03-\x204\n\x11\n\n\x04\
    \x02\x02\x03\x08\xe7\x07\0\x02\0\x12\x03-\x204\n\x12\n\x0b\x04\x02\x02\
    \x03\x08\xe7\x07\0\x02\0\x01\x12\x03-!3\n\x10\n\t\x04\x02\x02\x03\x08\
    \xe7\x07\0\x03\x12\x03-7<\n2\n\x04\x04\x02\x02\x04\x12\x03.\x08!\"%\x20c\
    heck\x20whether\x20it\x20is\x20a\x20unique\x20index.\n\n\x0c\n\x05\x04\
    \x02\x02\x04\x04\x12\x03.\x08\x10\n\x0c\n\x05\x04\x02\x02\x04\x05\x12\
    \x03.\x11\x15\n\x0c\n\x05\x04\x02\x02\x04\x01\x12\x03.\x16\x1c\n\x0c\n\
    \x05\x04\x02\x02\x04\x03\x12\x03.\x1f\x20\n\n\n\x02\x04\x03\x12\x041\04\
    \x01\n\n\n\x03\x04\x03\x01\x12\x031\x08\x11\n\x20\n\x04\x04\x03\x02\0\
    \x12\x033\x08%\x1a\x13\x20Where\x20conditions.\n\n\x0c\n\x05\x04\x03\x02\
    \0\x04\x12\x033\x08\x10\n\x0c\n\x05\x04\x03\x02\0\x06\x12\x033\x11\x15\n\
    \x0c\n\x05\x04\x03\x02\0\x01\x12\x033\x16\x20\n\x0c\n\x05\x04\x03\x02\0\
    \x03\x12\x033#$\n\n\n\x02\x04\x04\x12\x046\09\x01\n\n\n\x03\x04\x04\x01\
    \x12\x036\x08\x12\n&\n\x04\x04\x04\x02\0\x12\x038\x08\x20\x1a\x19\x20Pro\
    jection\x20expressions.\n\n\x0c\n\x05\x04\x04\x02\0\x04\x12\x038\x08\x10\
    \n\x0c\n\x05\x04\x04\x02\0\x06\x12\x038\x11\x15\n\x0c\n\x05\x04\x04\x02\
    \0\x01\x12\x038\x16\x1b\n\x0c\n\x05\x04\x04\x02\0\x03\x12\x038\x1e\x1f\n\
    \n\n\x02\x04\x05\x12\x04;\0B\x01\n\n\n\x03\x04\x05\x01\x12\x03;\x08\x13\
    \n\x1f\n\x04\x04\x05\x02\0\x12\x03=\x08#\x1a\x12\x20Group\x20by\x20claus\
    e.\n\n\x0c\n\x05\x04\x05\x02\0\x04\x12\x03=\x08\x10\n\x0c\n\x05\x04\x05\
    \x02\0\x06\x12\x03=\x11\x15\n\x0c\n\x05\x04\x05\x02\0\x01\x12\x03=\x16\
    \x1e\n\x0c\n\x05\x04\x05\x02\0\x03\x12\x03=!\"\n#\n\x04\x04\x05\x02\x01\
    \x12\x03?\x08#\x1a\x16\x20Aggregate\x20functions.\n\n\x0c\n\x05\x04\x05\
    \x02\x01\x04\x12\x03?\x08\x10\n\x0c\n\x05\x04\x05\x02\x01\x06\x12\x03?\
    \x11\x15\n\x0c\n\x05\x04\x05\x02\x01\x01\x12\x03?\x16\x1e\n\x0c\n\x05\
    \x04\x05\x02\x01\x03\x12\x03?!\"\n-\n\x04\x04\x05\x02\x02\x12\x03A\x08B\
    \x1a\x20\x20If\x20it\x20is\x20a\x20stream\x20aggregation.\n\n\x0c\n\x05\
    \x04\x05\x02\x02\x04\x12\x03A\x08\x10\n\x0c\n\x05\x04\x05\x02\x02\x05\
    \x12\x03A\x11\x15\n\x0c\n\x05\x04\x05\x02\x02\x01\x12\x03A\x16\x1e\n\x0c\
    \n\x05\x04\x05\x02\x02\x03\x12\x03A!\"\n\x0c\n\x05\x04\x05\x02\x02\x08\
    \x12\x03A#A\n\x0f\n\x08\x04\x05\x02\x02\x08\xe7\x07\0\x12\x03A$@\n\x10\n\
    \t\x04\x05\x02\x02\x08\xe7\x07\0\x02\x12\x03A$8\n\x11\n\n\x04\x05\x02\
    \x02\x08\xe7\x07\0\x02\0\x12\x03A$8\n\x12\n\x0b\x04\x05\x02\x02\x08\xe7\
    \x07\0\x02\0\x01\x12\x03A%7\n\x10\n\t\x04\x05\x02\x02\x08\xe7\x07\0\x03\
    \x12\x03A;@\n\n\n\x02\x04\x06\x12\x04D\0H\x01\n\n\n\x03\x04\x06\x01\x12\
    \x03D\x08\x0c\n\x1f\n\x04\x04\x06\x02\0\x12\x03F\x08%\x1a\x12\x20Order\
    \x20by\x20clause.\n\n\x0c\n\x05\x04\x06\x02\0\x04\x12\x03F\x08\x10\n\x0c\
    \n\x05\x04\x06\x02\0\x06\x12\x03F\x11\x17\n\x0c\n\x05\x04\x06\x02\0\x01\
    \x12\x03F\x18\x20\n\x0c\n\x05\x04\x06\x02\0\x03\x12\x03F#$\n\x0b\n\x04\
    \x04\x06\x02\x01\x12\x03G\x08A\n\x0c\n\x05\x04\x06\x02\x01\x04\x12\x03G\
    \x08\x10\n\x0c\n\x05\x04\x06\x02\x01\x05\x12\x03G\x11\x17\n\x0c\n\x05\
    \x04\x06\x02\x01\x01\x12\x03G\x18\x1d\n\x0c\n\x05\x04\x06\x02\x01\x03\
    \x12\x03G\x20!\n\x0c\n\x05\x04\x06\x02\x01\x08\x12\x03G\"@\n\x0f\n\x08\
    \x04\x06\x02\x01\x08\xe7\x07\0\x12\x03G#?\n\x10\n\t\x04\x06\x02\x01\x08\
    \xe7\x07\0\x02\x12\x03G#7\n\x11\n\n\x04\x06\x02\x01\x08\xe7\x07\0\x02\0\
    \x12\x03G#7\n\x12\n\x0b\x04\x06\x02\x01\x08\xe7\x07\0\x02\0\x01\x12\x03G\
    $6\n\x10\n\t\x04\x06\x02\x01\x08\xe7\x07\0\x03\x12\x03G:?\n\n\n\x02\x04\
    \x07\x12\x04J\0M\x01\n\n\n\x03\x04\x07\x01\x12\x03J\x08\r\n/\n\x04\x04\
    \x07\x02\0\x12\x03L\x08A\x1a\"\x20Limit\x20the\x20result\x20to\x20be\x20\
    returned.\n\n\x0c\n\x05\x04\x07\x02\0\x04\x12\x03L\x08\x10\n\x0c\n\x05\
    \x04\x07\x02\0\x05\x12\x03L\x11\x17\n\x0c\n\x05\x04\x07\x02\0\x01\x12\
    \x03L\x18\x1d\n\x0c\n\x05\x04\x07\x02\0\x03\x12\x03L\x20!\n\x0c\n\x05\
    \x04\x07\x02\0\x08\x12\x03L\"@\n\x0f\n\x08\x04\x07\x02\0\x08\xe7\x07\0\
    \x12\x03L#?\n\x10\n\t\x04\x07\x02\0\x08\xe7\x07\0\x02\x12\x03L#7\n\x11\n\
    \n\x04\x07\x02\0\x08\xe7\x07\0\x02\0\x12\x03L#7\n\x12\n\x0b\x04\x07\x02\
    \0\x08\xe7\x07\0\x02\0\x01\x12\x03L$6\n\x10\n\t\x04\x07\x02\0\x08\xe7\
    \x07\0\x03\x12\x03L:?\
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
