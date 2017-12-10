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
pub struct AnalyzeReq {
    // message fields
    tp: ::std::option::Option<AnalyzeType>,
    start_ts: ::std::option::Option<u64>,
    flags: ::std::option::Option<u64>,
    time_zone_offset: ::std::option::Option<i64>,
    idx_req: ::protobuf::SingularPtrField<AnalyzeIndexReq>,
    col_req: ::protobuf::SingularPtrField<AnalyzeColumnsReq>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for AnalyzeReq {}

impl AnalyzeReq {
    pub fn new() -> AnalyzeReq {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static AnalyzeReq {
        static mut instance: ::protobuf::lazy::Lazy<AnalyzeReq> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const AnalyzeReq,
        };
        unsafe {
            instance.get(AnalyzeReq::new)
        }
    }

    // optional .tipb.AnalyzeType tp = 1;

    pub fn clear_tp(&mut self) {
        self.tp = ::std::option::Option::None;
    }

    pub fn has_tp(&self) -> bool {
        self.tp.is_some()
    }

    // Param is passed by value, moved
    pub fn set_tp(&mut self, v: AnalyzeType) {
        self.tp = ::std::option::Option::Some(v);
    }

    pub fn get_tp(&self) -> AnalyzeType {
        self.tp.unwrap_or(AnalyzeType::TypeIndex)
    }

    fn get_tp_for_reflect(&self) -> &::std::option::Option<AnalyzeType> {
        &self.tp
    }

    fn mut_tp_for_reflect(&mut self) -> &mut ::std::option::Option<AnalyzeType> {
        &mut self.tp
    }

    // optional uint64 start_ts = 2;

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

    // optional uint64 flags = 3;

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

    // optional int64 time_zone_offset = 4;

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

    // optional .tipb.AnalyzeIndexReq idx_req = 5;

    pub fn clear_idx_req(&mut self) {
        self.idx_req.clear();
    }

    pub fn has_idx_req(&self) -> bool {
        self.idx_req.is_some()
    }

    // Param is passed by value, moved
    pub fn set_idx_req(&mut self, v: AnalyzeIndexReq) {
        self.idx_req = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_idx_req(&mut self) -> &mut AnalyzeIndexReq {
        if self.idx_req.is_none() {
            self.idx_req.set_default();
        }
        self.idx_req.as_mut().unwrap()
    }

    // Take field
    pub fn take_idx_req(&mut self) -> AnalyzeIndexReq {
        self.idx_req.take().unwrap_or_else(|| AnalyzeIndexReq::new())
    }

    pub fn get_idx_req(&self) -> &AnalyzeIndexReq {
        self.idx_req.as_ref().unwrap_or_else(|| AnalyzeIndexReq::default_instance())
    }

    fn get_idx_req_for_reflect(&self) -> &::protobuf::SingularPtrField<AnalyzeIndexReq> {
        &self.idx_req
    }

    fn mut_idx_req_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<AnalyzeIndexReq> {
        &mut self.idx_req
    }

    // optional .tipb.AnalyzeColumnsReq col_req = 6;

    pub fn clear_col_req(&mut self) {
        self.col_req.clear();
    }

    pub fn has_col_req(&self) -> bool {
        self.col_req.is_some()
    }

    // Param is passed by value, moved
    pub fn set_col_req(&mut self, v: AnalyzeColumnsReq) {
        self.col_req = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_col_req(&mut self) -> &mut AnalyzeColumnsReq {
        if self.col_req.is_none() {
            self.col_req.set_default();
        }
        self.col_req.as_mut().unwrap()
    }

    // Take field
    pub fn take_col_req(&mut self) -> AnalyzeColumnsReq {
        self.col_req.take().unwrap_or_else(|| AnalyzeColumnsReq::new())
    }

    pub fn get_col_req(&self) -> &AnalyzeColumnsReq {
        self.col_req.as_ref().unwrap_or_else(|| AnalyzeColumnsReq::default_instance())
    }

    fn get_col_req_for_reflect(&self) -> &::protobuf::SingularPtrField<AnalyzeColumnsReq> {
        &self.col_req
    }

    fn mut_col_req_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<AnalyzeColumnsReq> {
        &mut self.col_req
    }
}

impl ::protobuf::Message for AnalyzeReq {
    fn is_initialized(&self) -> bool {
        for v in &self.idx_req {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.col_req {
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
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_uint64()?;
                    self.start_ts = ::std::option::Option::Some(tmp);
                },
                3 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_uint64()?;
                    self.flags = ::std::option::Option::Some(tmp);
                },
                4 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_int64()?;
                    self.time_zone_offset = ::std::option::Option::Some(tmp);
                },
                5 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.idx_req)?;
                },
                6 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.col_req)?;
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
        if let Some(v) = self.start_ts {
            my_size += ::protobuf::rt::value_size(2, v, ::protobuf::wire_format::WireTypeVarint);
        }
        if let Some(v) = self.flags {
            my_size += ::protobuf::rt::value_size(3, v, ::protobuf::wire_format::WireTypeVarint);
        }
        if let Some(v) = self.time_zone_offset {
            my_size += ::protobuf::rt::value_size(4, v, ::protobuf::wire_format::WireTypeVarint);
        }
        if let Some(ref v) = self.idx_req.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if let Some(ref v) = self.col_req.as_ref() {
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
        if let Some(v) = self.start_ts {
            os.write_uint64(2, v)?;
        }
        if let Some(v) = self.flags {
            os.write_uint64(3, v)?;
        }
        if let Some(v) = self.time_zone_offset {
            os.write_int64(4, v)?;
        }
        if let Some(ref v) = self.idx_req.as_ref() {
            os.write_tag(5, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if let Some(ref v) = self.col_req.as_ref() {
            os.write_tag(6, ::protobuf::wire_format::WireTypeLengthDelimited)?;
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

impl ::protobuf::MessageStatic for AnalyzeReq {
    fn new() -> AnalyzeReq {
        AnalyzeReq::new()
    }

    fn descriptor_static(_: ::std::option::Option<AnalyzeReq>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeEnum<AnalyzeType>>(
                    "tp",
                    AnalyzeReq::get_tp_for_reflect,
                    AnalyzeReq::mut_tp_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "start_ts",
                    AnalyzeReq::get_start_ts_for_reflect,
                    AnalyzeReq::mut_start_ts_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "flags",
                    AnalyzeReq::get_flags_for_reflect,
                    AnalyzeReq::mut_flags_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeInt64>(
                    "time_zone_offset",
                    AnalyzeReq::get_time_zone_offset_for_reflect,
                    AnalyzeReq::mut_time_zone_offset_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<AnalyzeIndexReq>>(
                    "idx_req",
                    AnalyzeReq::get_idx_req_for_reflect,
                    AnalyzeReq::mut_idx_req_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<AnalyzeColumnsReq>>(
                    "col_req",
                    AnalyzeReq::get_col_req_for_reflect,
                    AnalyzeReq::mut_col_req_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<AnalyzeReq>(
                    "AnalyzeReq",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for AnalyzeReq {
    fn clear(&mut self) {
        self.clear_tp();
        self.clear_start_ts();
        self.clear_flags();
        self.clear_time_zone_offset();
        self.clear_idx_req();
        self.clear_col_req();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for AnalyzeReq {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for AnalyzeReq {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct AnalyzeIndexReq {
    // message fields
    bucket_size: ::std::option::Option<i64>,
    num_columns: ::std::option::Option<i32>,
    cmsketch_depth: ::std::option::Option<i32>,
    cmsketch_width: ::std::option::Option<i32>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for AnalyzeIndexReq {}

impl AnalyzeIndexReq {
    pub fn new() -> AnalyzeIndexReq {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static AnalyzeIndexReq {
        static mut instance: ::protobuf::lazy::Lazy<AnalyzeIndexReq> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const AnalyzeIndexReq,
        };
        unsafe {
            instance.get(AnalyzeIndexReq::new)
        }
    }

    // optional int64 bucket_size = 1;

    pub fn clear_bucket_size(&mut self) {
        self.bucket_size = ::std::option::Option::None;
    }

    pub fn has_bucket_size(&self) -> bool {
        self.bucket_size.is_some()
    }

    // Param is passed by value, moved
    pub fn set_bucket_size(&mut self, v: i64) {
        self.bucket_size = ::std::option::Option::Some(v);
    }

    pub fn get_bucket_size(&self) -> i64 {
        self.bucket_size.unwrap_or(0)
    }

    fn get_bucket_size_for_reflect(&self) -> &::std::option::Option<i64> {
        &self.bucket_size
    }

    fn mut_bucket_size_for_reflect(&mut self) -> &mut ::std::option::Option<i64> {
        &mut self.bucket_size
    }

    // optional int32 num_columns = 2;

    pub fn clear_num_columns(&mut self) {
        self.num_columns = ::std::option::Option::None;
    }

    pub fn has_num_columns(&self) -> bool {
        self.num_columns.is_some()
    }

    // Param is passed by value, moved
    pub fn set_num_columns(&mut self, v: i32) {
        self.num_columns = ::std::option::Option::Some(v);
    }

    pub fn get_num_columns(&self) -> i32 {
        self.num_columns.unwrap_or(0)
    }

    fn get_num_columns_for_reflect(&self) -> &::std::option::Option<i32> {
        &self.num_columns
    }

    fn mut_num_columns_for_reflect(&mut self) -> &mut ::std::option::Option<i32> {
        &mut self.num_columns
    }

    // optional int32 cmsketch_depth = 3;

    pub fn clear_cmsketch_depth(&mut self) {
        self.cmsketch_depth = ::std::option::Option::None;
    }

    pub fn has_cmsketch_depth(&self) -> bool {
        self.cmsketch_depth.is_some()
    }

    // Param is passed by value, moved
    pub fn set_cmsketch_depth(&mut self, v: i32) {
        self.cmsketch_depth = ::std::option::Option::Some(v);
    }

    pub fn get_cmsketch_depth(&self) -> i32 {
        self.cmsketch_depth.unwrap_or(0)
    }

    fn get_cmsketch_depth_for_reflect(&self) -> &::std::option::Option<i32> {
        &self.cmsketch_depth
    }

    fn mut_cmsketch_depth_for_reflect(&mut self) -> &mut ::std::option::Option<i32> {
        &mut self.cmsketch_depth
    }

    // optional int32 cmsketch_width = 4;

    pub fn clear_cmsketch_width(&mut self) {
        self.cmsketch_width = ::std::option::Option::None;
    }

    pub fn has_cmsketch_width(&self) -> bool {
        self.cmsketch_width.is_some()
    }

    // Param is passed by value, moved
    pub fn set_cmsketch_width(&mut self, v: i32) {
        self.cmsketch_width = ::std::option::Option::Some(v);
    }

    pub fn get_cmsketch_width(&self) -> i32 {
        self.cmsketch_width.unwrap_or(0)
    }

    fn get_cmsketch_width_for_reflect(&self) -> &::std::option::Option<i32> {
        &self.cmsketch_width
    }

    fn mut_cmsketch_width_for_reflect(&mut self) -> &mut ::std::option::Option<i32> {
        &mut self.cmsketch_width
    }
}

impl ::protobuf::Message for AnalyzeIndexReq {
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
                    self.bucket_size = ::std::option::Option::Some(tmp);
                },
                2 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_int32()?;
                    self.num_columns = ::std::option::Option::Some(tmp);
                },
                3 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_int32()?;
                    self.cmsketch_depth = ::std::option::Option::Some(tmp);
                },
                4 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_int32()?;
                    self.cmsketch_width = ::std::option::Option::Some(tmp);
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
        if let Some(v) = self.bucket_size {
            my_size += ::protobuf::rt::value_size(1, v, ::protobuf::wire_format::WireTypeVarint);
        }
        if let Some(v) = self.num_columns {
            my_size += ::protobuf::rt::value_size(2, v, ::protobuf::wire_format::WireTypeVarint);
        }
        if let Some(v) = self.cmsketch_depth {
            my_size += ::protobuf::rt::value_size(3, v, ::protobuf::wire_format::WireTypeVarint);
        }
        if let Some(v) = self.cmsketch_width {
            my_size += ::protobuf::rt::value_size(4, v, ::protobuf::wire_format::WireTypeVarint);
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.bucket_size {
            os.write_int64(1, v)?;
        }
        if let Some(v) = self.num_columns {
            os.write_int32(2, v)?;
        }
        if let Some(v) = self.cmsketch_depth {
            os.write_int32(3, v)?;
        }
        if let Some(v) = self.cmsketch_width {
            os.write_int32(4, v)?;
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

impl ::protobuf::MessageStatic for AnalyzeIndexReq {
    fn new() -> AnalyzeIndexReq {
        AnalyzeIndexReq::new()
    }

    fn descriptor_static(_: ::std::option::Option<AnalyzeIndexReq>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeInt64>(
                    "bucket_size",
                    AnalyzeIndexReq::get_bucket_size_for_reflect,
                    AnalyzeIndexReq::mut_bucket_size_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeInt32>(
                    "num_columns",
                    AnalyzeIndexReq::get_num_columns_for_reflect,
                    AnalyzeIndexReq::mut_num_columns_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeInt32>(
                    "cmsketch_depth",
                    AnalyzeIndexReq::get_cmsketch_depth_for_reflect,
                    AnalyzeIndexReq::mut_cmsketch_depth_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeInt32>(
                    "cmsketch_width",
                    AnalyzeIndexReq::get_cmsketch_width_for_reflect,
                    AnalyzeIndexReq::mut_cmsketch_width_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<AnalyzeIndexReq>(
                    "AnalyzeIndexReq",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for AnalyzeIndexReq {
    fn clear(&mut self) {
        self.clear_bucket_size();
        self.clear_num_columns();
        self.clear_cmsketch_depth();
        self.clear_cmsketch_width();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for AnalyzeIndexReq {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for AnalyzeIndexReq {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct AnalyzeColumnsReq {
    // message fields
    bucket_size: ::std::option::Option<i64>,
    sample_size: ::std::option::Option<i64>,
    sketch_size: ::std::option::Option<i64>,
    columns_info: ::protobuf::RepeatedField<super::schema::ColumnInfo>,
    cmsketch_depth: ::std::option::Option<i32>,
    cmsketch_width: ::std::option::Option<i32>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for AnalyzeColumnsReq {}

impl AnalyzeColumnsReq {
    pub fn new() -> AnalyzeColumnsReq {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static AnalyzeColumnsReq {
        static mut instance: ::protobuf::lazy::Lazy<AnalyzeColumnsReq> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const AnalyzeColumnsReq,
        };
        unsafe {
            instance.get(AnalyzeColumnsReq::new)
        }
    }

    // optional int64 bucket_size = 1;

    pub fn clear_bucket_size(&mut self) {
        self.bucket_size = ::std::option::Option::None;
    }

    pub fn has_bucket_size(&self) -> bool {
        self.bucket_size.is_some()
    }

    // Param is passed by value, moved
    pub fn set_bucket_size(&mut self, v: i64) {
        self.bucket_size = ::std::option::Option::Some(v);
    }

    pub fn get_bucket_size(&self) -> i64 {
        self.bucket_size.unwrap_or(0)
    }

    fn get_bucket_size_for_reflect(&self) -> &::std::option::Option<i64> {
        &self.bucket_size
    }

    fn mut_bucket_size_for_reflect(&mut self) -> &mut ::std::option::Option<i64> {
        &mut self.bucket_size
    }

    // optional int64 sample_size = 2;

    pub fn clear_sample_size(&mut self) {
        self.sample_size = ::std::option::Option::None;
    }

    pub fn has_sample_size(&self) -> bool {
        self.sample_size.is_some()
    }

    // Param is passed by value, moved
    pub fn set_sample_size(&mut self, v: i64) {
        self.sample_size = ::std::option::Option::Some(v);
    }

    pub fn get_sample_size(&self) -> i64 {
        self.sample_size.unwrap_or(0)
    }

    fn get_sample_size_for_reflect(&self) -> &::std::option::Option<i64> {
        &self.sample_size
    }

    fn mut_sample_size_for_reflect(&mut self) -> &mut ::std::option::Option<i64> {
        &mut self.sample_size
    }

    // optional int64 sketch_size = 3;

    pub fn clear_sketch_size(&mut self) {
        self.sketch_size = ::std::option::Option::None;
    }

    pub fn has_sketch_size(&self) -> bool {
        self.sketch_size.is_some()
    }

    // Param is passed by value, moved
    pub fn set_sketch_size(&mut self, v: i64) {
        self.sketch_size = ::std::option::Option::Some(v);
    }

    pub fn get_sketch_size(&self) -> i64 {
        self.sketch_size.unwrap_or(0)
    }

    fn get_sketch_size_for_reflect(&self) -> &::std::option::Option<i64> {
        &self.sketch_size
    }

    fn mut_sketch_size_for_reflect(&mut self) -> &mut ::std::option::Option<i64> {
        &mut self.sketch_size
    }

    // repeated .tipb.ColumnInfo columns_info = 4;

    pub fn clear_columns_info(&mut self) {
        self.columns_info.clear();
    }

    // Param is passed by value, moved
    pub fn set_columns_info(&mut self, v: ::protobuf::RepeatedField<super::schema::ColumnInfo>) {
        self.columns_info = v;
    }

    // Mutable pointer to the field.
    pub fn mut_columns_info(&mut self) -> &mut ::protobuf::RepeatedField<super::schema::ColumnInfo> {
        &mut self.columns_info
    }

    // Take field
    pub fn take_columns_info(&mut self) -> ::protobuf::RepeatedField<super::schema::ColumnInfo> {
        ::std::mem::replace(&mut self.columns_info, ::protobuf::RepeatedField::new())
    }

    pub fn get_columns_info(&self) -> &[super::schema::ColumnInfo] {
        &self.columns_info
    }

    fn get_columns_info_for_reflect(&self) -> &::protobuf::RepeatedField<super::schema::ColumnInfo> {
        &self.columns_info
    }

    fn mut_columns_info_for_reflect(&mut self) -> &mut ::protobuf::RepeatedField<super::schema::ColumnInfo> {
        &mut self.columns_info
    }

    // optional int32 cmsketch_depth = 5;

    pub fn clear_cmsketch_depth(&mut self) {
        self.cmsketch_depth = ::std::option::Option::None;
    }

    pub fn has_cmsketch_depth(&self) -> bool {
        self.cmsketch_depth.is_some()
    }

    // Param is passed by value, moved
    pub fn set_cmsketch_depth(&mut self, v: i32) {
        self.cmsketch_depth = ::std::option::Option::Some(v);
    }

    pub fn get_cmsketch_depth(&self) -> i32 {
        self.cmsketch_depth.unwrap_or(0)
    }

    fn get_cmsketch_depth_for_reflect(&self) -> &::std::option::Option<i32> {
        &self.cmsketch_depth
    }

    fn mut_cmsketch_depth_for_reflect(&mut self) -> &mut ::std::option::Option<i32> {
        &mut self.cmsketch_depth
    }

    // optional int32 cmsketch_width = 6;

    pub fn clear_cmsketch_width(&mut self) {
        self.cmsketch_width = ::std::option::Option::None;
    }

    pub fn has_cmsketch_width(&self) -> bool {
        self.cmsketch_width.is_some()
    }

    // Param is passed by value, moved
    pub fn set_cmsketch_width(&mut self, v: i32) {
        self.cmsketch_width = ::std::option::Option::Some(v);
    }

    pub fn get_cmsketch_width(&self) -> i32 {
        self.cmsketch_width.unwrap_or(0)
    }

    fn get_cmsketch_width_for_reflect(&self) -> &::std::option::Option<i32> {
        &self.cmsketch_width
    }

    fn mut_cmsketch_width_for_reflect(&mut self) -> &mut ::std::option::Option<i32> {
        &mut self.cmsketch_width
    }
}

impl ::protobuf::Message for AnalyzeColumnsReq {
    fn is_initialized(&self) -> bool {
        for v in &self.columns_info {
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
                    self.bucket_size = ::std::option::Option::Some(tmp);
                },
                2 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_int64()?;
                    self.sample_size = ::std::option::Option::Some(tmp);
                },
                3 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_int64()?;
                    self.sketch_size = ::std::option::Option::Some(tmp);
                },
                4 => {
                    ::protobuf::rt::read_repeated_message_into(wire_type, is, &mut self.columns_info)?;
                },
                5 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_int32()?;
                    self.cmsketch_depth = ::std::option::Option::Some(tmp);
                },
                6 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_int32()?;
                    self.cmsketch_width = ::std::option::Option::Some(tmp);
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
        if let Some(v) = self.bucket_size {
            my_size += ::protobuf::rt::value_size(1, v, ::protobuf::wire_format::WireTypeVarint);
        }
        if let Some(v) = self.sample_size {
            my_size += ::protobuf::rt::value_size(2, v, ::protobuf::wire_format::WireTypeVarint);
        }
        if let Some(v) = self.sketch_size {
            my_size += ::protobuf::rt::value_size(3, v, ::protobuf::wire_format::WireTypeVarint);
        }
        for value in &self.columns_info {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        if let Some(v) = self.cmsketch_depth {
            my_size += ::protobuf::rt::value_size(5, v, ::protobuf::wire_format::WireTypeVarint);
        }
        if let Some(v) = self.cmsketch_width {
            my_size += ::protobuf::rt::value_size(6, v, ::protobuf::wire_format::WireTypeVarint);
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.bucket_size {
            os.write_int64(1, v)?;
        }
        if let Some(v) = self.sample_size {
            os.write_int64(2, v)?;
        }
        if let Some(v) = self.sketch_size {
            os.write_int64(3, v)?;
        }
        for v in &self.columns_info {
            os.write_tag(4, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        };
        if let Some(v) = self.cmsketch_depth {
            os.write_int32(5, v)?;
        }
        if let Some(v) = self.cmsketch_width {
            os.write_int32(6, v)?;
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

impl ::protobuf::MessageStatic for AnalyzeColumnsReq {
    fn new() -> AnalyzeColumnsReq {
        AnalyzeColumnsReq::new()
    }

    fn descriptor_static(_: ::std::option::Option<AnalyzeColumnsReq>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeInt64>(
                    "bucket_size",
                    AnalyzeColumnsReq::get_bucket_size_for_reflect,
                    AnalyzeColumnsReq::mut_bucket_size_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeInt64>(
                    "sample_size",
                    AnalyzeColumnsReq::get_sample_size_for_reflect,
                    AnalyzeColumnsReq::mut_sample_size_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeInt64>(
                    "sketch_size",
                    AnalyzeColumnsReq::get_sketch_size_for_reflect,
                    AnalyzeColumnsReq::mut_sketch_size_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_repeated_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<super::schema::ColumnInfo>>(
                    "columns_info",
                    AnalyzeColumnsReq::get_columns_info_for_reflect,
                    AnalyzeColumnsReq::mut_columns_info_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeInt32>(
                    "cmsketch_depth",
                    AnalyzeColumnsReq::get_cmsketch_depth_for_reflect,
                    AnalyzeColumnsReq::mut_cmsketch_depth_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeInt32>(
                    "cmsketch_width",
                    AnalyzeColumnsReq::get_cmsketch_width_for_reflect,
                    AnalyzeColumnsReq::mut_cmsketch_width_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<AnalyzeColumnsReq>(
                    "AnalyzeColumnsReq",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for AnalyzeColumnsReq {
    fn clear(&mut self) {
        self.clear_bucket_size();
        self.clear_sample_size();
        self.clear_sketch_size();
        self.clear_columns_info();
        self.clear_cmsketch_depth();
        self.clear_cmsketch_width();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for AnalyzeColumnsReq {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for AnalyzeColumnsReq {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct AnalyzeColumnsResp {
    // message fields
    collectors: ::protobuf::RepeatedField<SampleCollector>,
    pk_hist: ::protobuf::SingularPtrField<Histogram>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for AnalyzeColumnsResp {}

impl AnalyzeColumnsResp {
    pub fn new() -> AnalyzeColumnsResp {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static AnalyzeColumnsResp {
        static mut instance: ::protobuf::lazy::Lazy<AnalyzeColumnsResp> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const AnalyzeColumnsResp,
        };
        unsafe {
            instance.get(AnalyzeColumnsResp::new)
        }
    }

    // repeated .tipb.SampleCollector collectors = 1;

    pub fn clear_collectors(&mut self) {
        self.collectors.clear();
    }

    // Param is passed by value, moved
    pub fn set_collectors(&mut self, v: ::protobuf::RepeatedField<SampleCollector>) {
        self.collectors = v;
    }

    // Mutable pointer to the field.
    pub fn mut_collectors(&mut self) -> &mut ::protobuf::RepeatedField<SampleCollector> {
        &mut self.collectors
    }

    // Take field
    pub fn take_collectors(&mut self) -> ::protobuf::RepeatedField<SampleCollector> {
        ::std::mem::replace(&mut self.collectors, ::protobuf::RepeatedField::new())
    }

    pub fn get_collectors(&self) -> &[SampleCollector] {
        &self.collectors
    }

    fn get_collectors_for_reflect(&self) -> &::protobuf::RepeatedField<SampleCollector> {
        &self.collectors
    }

    fn mut_collectors_for_reflect(&mut self) -> &mut ::protobuf::RepeatedField<SampleCollector> {
        &mut self.collectors
    }

    // optional .tipb.Histogram pk_hist = 2;

    pub fn clear_pk_hist(&mut self) {
        self.pk_hist.clear();
    }

    pub fn has_pk_hist(&self) -> bool {
        self.pk_hist.is_some()
    }

    // Param is passed by value, moved
    pub fn set_pk_hist(&mut self, v: Histogram) {
        self.pk_hist = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_pk_hist(&mut self) -> &mut Histogram {
        if self.pk_hist.is_none() {
            self.pk_hist.set_default();
        }
        self.pk_hist.as_mut().unwrap()
    }

    // Take field
    pub fn take_pk_hist(&mut self) -> Histogram {
        self.pk_hist.take().unwrap_or_else(|| Histogram::new())
    }

    pub fn get_pk_hist(&self) -> &Histogram {
        self.pk_hist.as_ref().unwrap_or_else(|| Histogram::default_instance())
    }

    fn get_pk_hist_for_reflect(&self) -> &::protobuf::SingularPtrField<Histogram> {
        &self.pk_hist
    }

    fn mut_pk_hist_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<Histogram> {
        &mut self.pk_hist
    }
}

impl ::protobuf::Message for AnalyzeColumnsResp {
    fn is_initialized(&self) -> bool {
        for v in &self.collectors {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.pk_hist {
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
                    ::protobuf::rt::read_repeated_message_into(wire_type, is, &mut self.collectors)?;
                },
                2 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.pk_hist)?;
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
        for value in &self.collectors {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        if let Some(ref v) = self.pk_hist.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        for v in &self.collectors {
            os.write_tag(1, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        };
        if let Some(ref v) = self.pk_hist.as_ref() {
            os.write_tag(2, ::protobuf::wire_format::WireTypeLengthDelimited)?;
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

impl ::protobuf::MessageStatic for AnalyzeColumnsResp {
    fn new() -> AnalyzeColumnsResp {
        AnalyzeColumnsResp::new()
    }

    fn descriptor_static(_: ::std::option::Option<AnalyzeColumnsResp>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_repeated_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<SampleCollector>>(
                    "collectors",
                    AnalyzeColumnsResp::get_collectors_for_reflect,
                    AnalyzeColumnsResp::mut_collectors_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<Histogram>>(
                    "pk_hist",
                    AnalyzeColumnsResp::get_pk_hist_for_reflect,
                    AnalyzeColumnsResp::mut_pk_hist_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<AnalyzeColumnsResp>(
                    "AnalyzeColumnsResp",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for AnalyzeColumnsResp {
    fn clear(&mut self) {
        self.clear_collectors();
        self.clear_pk_hist();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for AnalyzeColumnsResp {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for AnalyzeColumnsResp {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct AnalyzeIndexResp {
    // message fields
    hist: ::protobuf::SingularPtrField<Histogram>,
    cms: ::protobuf::SingularPtrField<CMSketch>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for AnalyzeIndexResp {}

impl AnalyzeIndexResp {
    pub fn new() -> AnalyzeIndexResp {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static AnalyzeIndexResp {
        static mut instance: ::protobuf::lazy::Lazy<AnalyzeIndexResp> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const AnalyzeIndexResp,
        };
        unsafe {
            instance.get(AnalyzeIndexResp::new)
        }
    }

    // optional .tipb.Histogram hist = 1;

    pub fn clear_hist(&mut self) {
        self.hist.clear();
    }

    pub fn has_hist(&self) -> bool {
        self.hist.is_some()
    }

    // Param is passed by value, moved
    pub fn set_hist(&mut self, v: Histogram) {
        self.hist = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_hist(&mut self) -> &mut Histogram {
        if self.hist.is_none() {
            self.hist.set_default();
        }
        self.hist.as_mut().unwrap()
    }

    // Take field
    pub fn take_hist(&mut self) -> Histogram {
        self.hist.take().unwrap_or_else(|| Histogram::new())
    }

    pub fn get_hist(&self) -> &Histogram {
        self.hist.as_ref().unwrap_or_else(|| Histogram::default_instance())
    }

    fn get_hist_for_reflect(&self) -> &::protobuf::SingularPtrField<Histogram> {
        &self.hist
    }

    fn mut_hist_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<Histogram> {
        &mut self.hist
    }

    // optional .tipb.CMSketch cms = 2;

    pub fn clear_cms(&mut self) {
        self.cms.clear();
    }

    pub fn has_cms(&self) -> bool {
        self.cms.is_some()
    }

    // Param is passed by value, moved
    pub fn set_cms(&mut self, v: CMSketch) {
        self.cms = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_cms(&mut self) -> &mut CMSketch {
        if self.cms.is_none() {
            self.cms.set_default();
        }
        self.cms.as_mut().unwrap()
    }

    // Take field
    pub fn take_cms(&mut self) -> CMSketch {
        self.cms.take().unwrap_or_else(|| CMSketch::new())
    }

    pub fn get_cms(&self) -> &CMSketch {
        self.cms.as_ref().unwrap_or_else(|| CMSketch::default_instance())
    }

    fn get_cms_for_reflect(&self) -> &::protobuf::SingularPtrField<CMSketch> {
        &self.cms
    }

    fn mut_cms_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<CMSketch> {
        &mut self.cms
    }
}

impl ::protobuf::Message for AnalyzeIndexResp {
    fn is_initialized(&self) -> bool {
        for v in &self.hist {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.cms {
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
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.hist)?;
                },
                2 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.cms)?;
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
        if let Some(ref v) = self.hist.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if let Some(ref v) = self.cms.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(ref v) = self.hist.as_ref() {
            os.write_tag(1, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if let Some(ref v) = self.cms.as_ref() {
            os.write_tag(2, ::protobuf::wire_format::WireTypeLengthDelimited)?;
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

impl ::protobuf::MessageStatic for AnalyzeIndexResp {
    fn new() -> AnalyzeIndexResp {
        AnalyzeIndexResp::new()
    }

    fn descriptor_static(_: ::std::option::Option<AnalyzeIndexResp>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<Histogram>>(
                    "hist",
                    AnalyzeIndexResp::get_hist_for_reflect,
                    AnalyzeIndexResp::mut_hist_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<CMSketch>>(
                    "cms",
                    AnalyzeIndexResp::get_cms_for_reflect,
                    AnalyzeIndexResp::mut_cms_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<AnalyzeIndexResp>(
                    "AnalyzeIndexResp",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for AnalyzeIndexResp {
    fn clear(&mut self) {
        self.clear_hist();
        self.clear_cms();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for AnalyzeIndexResp {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for AnalyzeIndexResp {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct Bucket {
    // message fields
    count: ::std::option::Option<i64>,
    lower_bound: ::protobuf::SingularField<::std::vec::Vec<u8>>,
    upper_bound: ::protobuf::SingularField<::std::vec::Vec<u8>>,
    repeats: ::std::option::Option<i64>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for Bucket {}

impl Bucket {
    pub fn new() -> Bucket {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static Bucket {
        static mut instance: ::protobuf::lazy::Lazy<Bucket> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const Bucket,
        };
        unsafe {
            instance.get(Bucket::new)
        }
    }

    // optional int64 count = 1;

    pub fn clear_count(&mut self) {
        self.count = ::std::option::Option::None;
    }

    pub fn has_count(&self) -> bool {
        self.count.is_some()
    }

    // Param is passed by value, moved
    pub fn set_count(&mut self, v: i64) {
        self.count = ::std::option::Option::Some(v);
    }

    pub fn get_count(&self) -> i64 {
        self.count.unwrap_or(0)
    }

    fn get_count_for_reflect(&self) -> &::std::option::Option<i64> {
        &self.count
    }

    fn mut_count_for_reflect(&mut self) -> &mut ::std::option::Option<i64> {
        &mut self.count
    }

    // optional bytes lower_bound = 2;

    pub fn clear_lower_bound(&mut self) {
        self.lower_bound.clear();
    }

    pub fn has_lower_bound(&self) -> bool {
        self.lower_bound.is_some()
    }

    // Param is passed by value, moved
    pub fn set_lower_bound(&mut self, v: ::std::vec::Vec<u8>) {
        self.lower_bound = ::protobuf::SingularField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_lower_bound(&mut self) -> &mut ::std::vec::Vec<u8> {
        if self.lower_bound.is_none() {
            self.lower_bound.set_default();
        }
        self.lower_bound.as_mut().unwrap()
    }

    // Take field
    pub fn take_lower_bound(&mut self) -> ::std::vec::Vec<u8> {
        self.lower_bound.take().unwrap_or_else(|| ::std::vec::Vec::new())
    }

    pub fn get_lower_bound(&self) -> &[u8] {
        match self.lower_bound.as_ref() {
            Some(v) => &v,
            None => &[],
        }
    }

    fn get_lower_bound_for_reflect(&self) -> &::protobuf::SingularField<::std::vec::Vec<u8>> {
        &self.lower_bound
    }

    fn mut_lower_bound_for_reflect(&mut self) -> &mut ::protobuf::SingularField<::std::vec::Vec<u8>> {
        &mut self.lower_bound
    }

    // optional bytes upper_bound = 3;

    pub fn clear_upper_bound(&mut self) {
        self.upper_bound.clear();
    }

    pub fn has_upper_bound(&self) -> bool {
        self.upper_bound.is_some()
    }

    // Param is passed by value, moved
    pub fn set_upper_bound(&mut self, v: ::std::vec::Vec<u8>) {
        self.upper_bound = ::protobuf::SingularField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_upper_bound(&mut self) -> &mut ::std::vec::Vec<u8> {
        if self.upper_bound.is_none() {
            self.upper_bound.set_default();
        }
        self.upper_bound.as_mut().unwrap()
    }

    // Take field
    pub fn take_upper_bound(&mut self) -> ::std::vec::Vec<u8> {
        self.upper_bound.take().unwrap_or_else(|| ::std::vec::Vec::new())
    }

    pub fn get_upper_bound(&self) -> &[u8] {
        match self.upper_bound.as_ref() {
            Some(v) => &v,
            None => &[],
        }
    }

    fn get_upper_bound_for_reflect(&self) -> &::protobuf::SingularField<::std::vec::Vec<u8>> {
        &self.upper_bound
    }

    fn mut_upper_bound_for_reflect(&mut self) -> &mut ::protobuf::SingularField<::std::vec::Vec<u8>> {
        &mut self.upper_bound
    }

    // optional int64 repeats = 4;

    pub fn clear_repeats(&mut self) {
        self.repeats = ::std::option::Option::None;
    }

    pub fn has_repeats(&self) -> bool {
        self.repeats.is_some()
    }

    // Param is passed by value, moved
    pub fn set_repeats(&mut self, v: i64) {
        self.repeats = ::std::option::Option::Some(v);
    }

    pub fn get_repeats(&self) -> i64 {
        self.repeats.unwrap_or(0)
    }

    fn get_repeats_for_reflect(&self) -> &::std::option::Option<i64> {
        &self.repeats
    }

    fn mut_repeats_for_reflect(&mut self) -> &mut ::std::option::Option<i64> {
        &mut self.repeats
    }
}

impl ::protobuf::Message for Bucket {
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
                    self.count = ::std::option::Option::Some(tmp);
                },
                2 => {
                    ::protobuf::rt::read_singular_bytes_into(wire_type, is, &mut self.lower_bound)?;
                },
                3 => {
                    ::protobuf::rt::read_singular_bytes_into(wire_type, is, &mut self.upper_bound)?;
                },
                4 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_int64()?;
                    self.repeats = ::std::option::Option::Some(tmp);
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
        if let Some(v) = self.count {
            my_size += ::protobuf::rt::value_size(1, v, ::protobuf::wire_format::WireTypeVarint);
        }
        if let Some(ref v) = self.lower_bound.as_ref() {
            my_size += ::protobuf::rt::bytes_size(2, &v);
        }
        if let Some(ref v) = self.upper_bound.as_ref() {
            my_size += ::protobuf::rt::bytes_size(3, &v);
        }
        if let Some(v) = self.repeats {
            my_size += ::protobuf::rt::value_size(4, v, ::protobuf::wire_format::WireTypeVarint);
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.count {
            os.write_int64(1, v)?;
        }
        if let Some(ref v) = self.lower_bound.as_ref() {
            os.write_bytes(2, &v)?;
        }
        if let Some(ref v) = self.upper_bound.as_ref() {
            os.write_bytes(3, &v)?;
        }
        if let Some(v) = self.repeats {
            os.write_int64(4, v)?;
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

impl ::protobuf::MessageStatic for Bucket {
    fn new() -> Bucket {
        Bucket::new()
    }

    fn descriptor_static(_: ::std::option::Option<Bucket>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeInt64>(
                    "count",
                    Bucket::get_count_for_reflect,
                    Bucket::mut_count_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_field_accessor::<_, ::protobuf::types::ProtobufTypeBytes>(
                    "lower_bound",
                    Bucket::get_lower_bound_for_reflect,
                    Bucket::mut_lower_bound_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_field_accessor::<_, ::protobuf::types::ProtobufTypeBytes>(
                    "upper_bound",
                    Bucket::get_upper_bound_for_reflect,
                    Bucket::mut_upper_bound_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeInt64>(
                    "repeats",
                    Bucket::get_repeats_for_reflect,
                    Bucket::mut_repeats_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<Bucket>(
                    "Bucket",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for Bucket {
    fn clear(&mut self) {
        self.clear_count();
        self.clear_lower_bound();
        self.clear_upper_bound();
        self.clear_repeats();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for Bucket {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for Bucket {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct Histogram {
    // message fields
    ndv: ::std::option::Option<i64>,
    buckets: ::protobuf::RepeatedField<Bucket>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for Histogram {}

impl Histogram {
    pub fn new() -> Histogram {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static Histogram {
        static mut instance: ::protobuf::lazy::Lazy<Histogram> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const Histogram,
        };
        unsafe {
            instance.get(Histogram::new)
        }
    }

    // optional int64 ndv = 1;

    pub fn clear_ndv(&mut self) {
        self.ndv = ::std::option::Option::None;
    }

    pub fn has_ndv(&self) -> bool {
        self.ndv.is_some()
    }

    // Param is passed by value, moved
    pub fn set_ndv(&mut self, v: i64) {
        self.ndv = ::std::option::Option::Some(v);
    }

    pub fn get_ndv(&self) -> i64 {
        self.ndv.unwrap_or(0)
    }

    fn get_ndv_for_reflect(&self) -> &::std::option::Option<i64> {
        &self.ndv
    }

    fn mut_ndv_for_reflect(&mut self) -> &mut ::std::option::Option<i64> {
        &mut self.ndv
    }

    // repeated .tipb.Bucket buckets = 2;

    pub fn clear_buckets(&mut self) {
        self.buckets.clear();
    }

    // Param is passed by value, moved
    pub fn set_buckets(&mut self, v: ::protobuf::RepeatedField<Bucket>) {
        self.buckets = v;
    }

    // Mutable pointer to the field.
    pub fn mut_buckets(&mut self) -> &mut ::protobuf::RepeatedField<Bucket> {
        &mut self.buckets
    }

    // Take field
    pub fn take_buckets(&mut self) -> ::protobuf::RepeatedField<Bucket> {
        ::std::mem::replace(&mut self.buckets, ::protobuf::RepeatedField::new())
    }

    pub fn get_buckets(&self) -> &[Bucket] {
        &self.buckets
    }

    fn get_buckets_for_reflect(&self) -> &::protobuf::RepeatedField<Bucket> {
        &self.buckets
    }

    fn mut_buckets_for_reflect(&mut self) -> &mut ::protobuf::RepeatedField<Bucket> {
        &mut self.buckets
    }
}

impl ::protobuf::Message for Histogram {
    fn is_initialized(&self) -> bool {
        for v in &self.buckets {
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
                    self.ndv = ::std::option::Option::Some(tmp);
                },
                2 => {
                    ::protobuf::rt::read_repeated_message_into(wire_type, is, &mut self.buckets)?;
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
        if let Some(v) = self.ndv {
            my_size += ::protobuf::rt::value_size(1, v, ::protobuf::wire_format::WireTypeVarint);
        }
        for value in &self.buckets {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.ndv {
            os.write_int64(1, v)?;
        }
        for v in &self.buckets {
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

impl ::protobuf::MessageStatic for Histogram {
    fn new() -> Histogram {
        Histogram::new()
    }

    fn descriptor_static(_: ::std::option::Option<Histogram>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeInt64>(
                    "ndv",
                    Histogram::get_ndv_for_reflect,
                    Histogram::mut_ndv_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_repeated_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<Bucket>>(
                    "buckets",
                    Histogram::get_buckets_for_reflect,
                    Histogram::mut_buckets_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<Histogram>(
                    "Histogram",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for Histogram {
    fn clear(&mut self) {
        self.clear_ndv();
        self.clear_buckets();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for Histogram {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for Histogram {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct FMSketch {
    // message fields
    mask: ::std::option::Option<u64>,
    hashset: ::std::vec::Vec<u64>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for FMSketch {}

impl FMSketch {
    pub fn new() -> FMSketch {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static FMSketch {
        static mut instance: ::protobuf::lazy::Lazy<FMSketch> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const FMSketch,
        };
        unsafe {
            instance.get(FMSketch::new)
        }
    }

    // optional uint64 mask = 1;

    pub fn clear_mask(&mut self) {
        self.mask = ::std::option::Option::None;
    }

    pub fn has_mask(&self) -> bool {
        self.mask.is_some()
    }

    // Param is passed by value, moved
    pub fn set_mask(&mut self, v: u64) {
        self.mask = ::std::option::Option::Some(v);
    }

    pub fn get_mask(&self) -> u64 {
        self.mask.unwrap_or(0)
    }

    fn get_mask_for_reflect(&self) -> &::std::option::Option<u64> {
        &self.mask
    }

    fn mut_mask_for_reflect(&mut self) -> &mut ::std::option::Option<u64> {
        &mut self.mask
    }

    // repeated uint64 hashset = 2;

    pub fn clear_hashset(&mut self) {
        self.hashset.clear();
    }

    // Param is passed by value, moved
    pub fn set_hashset(&mut self, v: ::std::vec::Vec<u64>) {
        self.hashset = v;
    }

    // Mutable pointer to the field.
    pub fn mut_hashset(&mut self) -> &mut ::std::vec::Vec<u64> {
        &mut self.hashset
    }

    // Take field
    pub fn take_hashset(&mut self) -> ::std::vec::Vec<u64> {
        ::std::mem::replace(&mut self.hashset, ::std::vec::Vec::new())
    }

    pub fn get_hashset(&self) -> &[u64] {
        &self.hashset
    }

    fn get_hashset_for_reflect(&self) -> &::std::vec::Vec<u64> {
        &self.hashset
    }

    fn mut_hashset_for_reflect(&mut self) -> &mut ::std::vec::Vec<u64> {
        &mut self.hashset
    }
}

impl ::protobuf::Message for FMSketch {
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
                    self.mask = ::std::option::Option::Some(tmp);
                },
                2 => {
                    ::protobuf::rt::read_repeated_uint64_into(wire_type, is, &mut self.hashset)?;
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
        if let Some(v) = self.mask {
            my_size += ::protobuf::rt::value_size(1, v, ::protobuf::wire_format::WireTypeVarint);
        }
        for value in &self.hashset {
            my_size += ::protobuf::rt::value_size(2, *value, ::protobuf::wire_format::WireTypeVarint);
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.mask {
            os.write_uint64(1, v)?;
        }
        for v in &self.hashset {
            os.write_uint64(2, *v)?;
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

impl ::protobuf::MessageStatic for FMSketch {
    fn new() -> FMSketch {
        FMSketch::new()
    }

    fn descriptor_static(_: ::std::option::Option<FMSketch>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "mask",
                    FMSketch::get_mask_for_reflect,
                    FMSketch::mut_mask_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_vec_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "hashset",
                    FMSketch::get_hashset_for_reflect,
                    FMSketch::mut_hashset_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<FMSketch>(
                    "FMSketch",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for FMSketch {
    fn clear(&mut self) {
        self.clear_mask();
        self.clear_hashset();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for FMSketch {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for FMSketch {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct SampleCollector {
    // message fields
    samples: ::protobuf::RepeatedField<::std::vec::Vec<u8>>,
    null_count: ::std::option::Option<i64>,
    count: ::std::option::Option<i64>,
    fm_sketch: ::protobuf::SingularPtrField<FMSketch>,
    cm_sketch: ::protobuf::SingularPtrField<CMSketch>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for SampleCollector {}

impl SampleCollector {
    pub fn new() -> SampleCollector {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static SampleCollector {
        static mut instance: ::protobuf::lazy::Lazy<SampleCollector> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const SampleCollector,
        };
        unsafe {
            instance.get(SampleCollector::new)
        }
    }

    // repeated bytes samples = 1;

    pub fn clear_samples(&mut self) {
        self.samples.clear();
    }

    // Param is passed by value, moved
    pub fn set_samples(&mut self, v: ::protobuf::RepeatedField<::std::vec::Vec<u8>>) {
        self.samples = v;
    }

    // Mutable pointer to the field.
    pub fn mut_samples(&mut self) -> &mut ::protobuf::RepeatedField<::std::vec::Vec<u8>> {
        &mut self.samples
    }

    // Take field
    pub fn take_samples(&mut self) -> ::protobuf::RepeatedField<::std::vec::Vec<u8>> {
        ::std::mem::replace(&mut self.samples, ::protobuf::RepeatedField::new())
    }

    pub fn get_samples(&self) -> &[::std::vec::Vec<u8>] {
        &self.samples
    }

    fn get_samples_for_reflect(&self) -> &::protobuf::RepeatedField<::std::vec::Vec<u8>> {
        &self.samples
    }

    fn mut_samples_for_reflect(&mut self) -> &mut ::protobuf::RepeatedField<::std::vec::Vec<u8>> {
        &mut self.samples
    }

    // optional int64 null_count = 2;

    pub fn clear_null_count(&mut self) {
        self.null_count = ::std::option::Option::None;
    }

    pub fn has_null_count(&self) -> bool {
        self.null_count.is_some()
    }

    // Param is passed by value, moved
    pub fn set_null_count(&mut self, v: i64) {
        self.null_count = ::std::option::Option::Some(v);
    }

    pub fn get_null_count(&self) -> i64 {
        self.null_count.unwrap_or(0)
    }

    fn get_null_count_for_reflect(&self) -> &::std::option::Option<i64> {
        &self.null_count
    }

    fn mut_null_count_for_reflect(&mut self) -> &mut ::std::option::Option<i64> {
        &mut self.null_count
    }

    // optional int64 count = 3;

    pub fn clear_count(&mut self) {
        self.count = ::std::option::Option::None;
    }

    pub fn has_count(&self) -> bool {
        self.count.is_some()
    }

    // Param is passed by value, moved
    pub fn set_count(&mut self, v: i64) {
        self.count = ::std::option::Option::Some(v);
    }

    pub fn get_count(&self) -> i64 {
        self.count.unwrap_or(0)
    }

    fn get_count_for_reflect(&self) -> &::std::option::Option<i64> {
        &self.count
    }

    fn mut_count_for_reflect(&mut self) -> &mut ::std::option::Option<i64> {
        &mut self.count
    }

    // optional .tipb.FMSketch fm_sketch = 4;

    pub fn clear_fm_sketch(&mut self) {
        self.fm_sketch.clear();
    }

    pub fn has_fm_sketch(&self) -> bool {
        self.fm_sketch.is_some()
    }

    // Param is passed by value, moved
    pub fn set_fm_sketch(&mut self, v: FMSketch) {
        self.fm_sketch = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_fm_sketch(&mut self) -> &mut FMSketch {
        if self.fm_sketch.is_none() {
            self.fm_sketch.set_default();
        }
        self.fm_sketch.as_mut().unwrap()
    }

    // Take field
    pub fn take_fm_sketch(&mut self) -> FMSketch {
        self.fm_sketch.take().unwrap_or_else(|| FMSketch::new())
    }

    pub fn get_fm_sketch(&self) -> &FMSketch {
        self.fm_sketch.as_ref().unwrap_or_else(|| FMSketch::default_instance())
    }

    fn get_fm_sketch_for_reflect(&self) -> &::protobuf::SingularPtrField<FMSketch> {
        &self.fm_sketch
    }

    fn mut_fm_sketch_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<FMSketch> {
        &mut self.fm_sketch
    }

    // optional .tipb.CMSketch cm_sketch = 5;

    pub fn clear_cm_sketch(&mut self) {
        self.cm_sketch.clear();
    }

    pub fn has_cm_sketch(&self) -> bool {
        self.cm_sketch.is_some()
    }

    // Param is passed by value, moved
    pub fn set_cm_sketch(&mut self, v: CMSketch) {
        self.cm_sketch = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_cm_sketch(&mut self) -> &mut CMSketch {
        if self.cm_sketch.is_none() {
            self.cm_sketch.set_default();
        }
        self.cm_sketch.as_mut().unwrap()
    }

    // Take field
    pub fn take_cm_sketch(&mut self) -> CMSketch {
        self.cm_sketch.take().unwrap_or_else(|| CMSketch::new())
    }

    pub fn get_cm_sketch(&self) -> &CMSketch {
        self.cm_sketch.as_ref().unwrap_or_else(|| CMSketch::default_instance())
    }

    fn get_cm_sketch_for_reflect(&self) -> &::protobuf::SingularPtrField<CMSketch> {
        &self.cm_sketch
    }

    fn mut_cm_sketch_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<CMSketch> {
        &mut self.cm_sketch
    }
}

impl ::protobuf::Message for SampleCollector {
    fn is_initialized(&self) -> bool {
        for v in &self.fm_sketch {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.cm_sketch {
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
                    ::protobuf::rt::read_repeated_bytes_into(wire_type, is, &mut self.samples)?;
                },
                2 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_int64()?;
                    self.null_count = ::std::option::Option::Some(tmp);
                },
                3 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_int64()?;
                    self.count = ::std::option::Option::Some(tmp);
                },
                4 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.fm_sketch)?;
                },
                5 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.cm_sketch)?;
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
        for value in &self.samples {
            my_size += ::protobuf::rt::bytes_size(1, &value);
        };
        if let Some(v) = self.null_count {
            my_size += ::protobuf::rt::value_size(2, v, ::protobuf::wire_format::WireTypeVarint);
        }
        if let Some(v) = self.count {
            my_size += ::protobuf::rt::value_size(3, v, ::protobuf::wire_format::WireTypeVarint);
        }
        if let Some(ref v) = self.fm_sketch.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if let Some(ref v) = self.cm_sketch.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        for v in &self.samples {
            os.write_bytes(1, &v)?;
        };
        if let Some(v) = self.null_count {
            os.write_int64(2, v)?;
        }
        if let Some(v) = self.count {
            os.write_int64(3, v)?;
        }
        if let Some(ref v) = self.fm_sketch.as_ref() {
            os.write_tag(4, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if let Some(ref v) = self.cm_sketch.as_ref() {
            os.write_tag(5, ::protobuf::wire_format::WireTypeLengthDelimited)?;
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

impl ::protobuf::MessageStatic for SampleCollector {
    fn new() -> SampleCollector {
        SampleCollector::new()
    }

    fn descriptor_static(_: ::std::option::Option<SampleCollector>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_repeated_field_accessor::<_, ::protobuf::types::ProtobufTypeBytes>(
                    "samples",
                    SampleCollector::get_samples_for_reflect,
                    SampleCollector::mut_samples_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeInt64>(
                    "null_count",
                    SampleCollector::get_null_count_for_reflect,
                    SampleCollector::mut_null_count_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeInt64>(
                    "count",
                    SampleCollector::get_count_for_reflect,
                    SampleCollector::mut_count_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<FMSketch>>(
                    "fm_sketch",
                    SampleCollector::get_fm_sketch_for_reflect,
                    SampleCollector::mut_fm_sketch_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<CMSketch>>(
                    "cm_sketch",
                    SampleCollector::get_cm_sketch_for_reflect,
                    SampleCollector::mut_cm_sketch_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<SampleCollector>(
                    "SampleCollector",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for SampleCollector {
    fn clear(&mut self) {
        self.clear_samples();
        self.clear_null_count();
        self.clear_count();
        self.clear_fm_sketch();
        self.clear_cm_sketch();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for SampleCollector {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for SampleCollector {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct CMSketchRow {
    // message fields
    counters: ::std::vec::Vec<u32>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for CMSketchRow {}

impl CMSketchRow {
    pub fn new() -> CMSketchRow {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static CMSketchRow {
        static mut instance: ::protobuf::lazy::Lazy<CMSketchRow> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const CMSketchRow,
        };
        unsafe {
            instance.get(CMSketchRow::new)
        }
    }

    // repeated uint32 counters = 1;

    pub fn clear_counters(&mut self) {
        self.counters.clear();
    }

    // Param is passed by value, moved
    pub fn set_counters(&mut self, v: ::std::vec::Vec<u32>) {
        self.counters = v;
    }

    // Mutable pointer to the field.
    pub fn mut_counters(&mut self) -> &mut ::std::vec::Vec<u32> {
        &mut self.counters
    }

    // Take field
    pub fn take_counters(&mut self) -> ::std::vec::Vec<u32> {
        ::std::mem::replace(&mut self.counters, ::std::vec::Vec::new())
    }

    pub fn get_counters(&self) -> &[u32] {
        &self.counters
    }

    fn get_counters_for_reflect(&self) -> &::std::vec::Vec<u32> {
        &self.counters
    }

    fn mut_counters_for_reflect(&mut self) -> &mut ::std::vec::Vec<u32> {
        &mut self.counters
    }
}

impl ::protobuf::Message for CMSketchRow {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !is.eof()? {
            let (field_number, wire_type) = is.read_tag_unpack()?;
            match field_number {
                1 => {
                    ::protobuf::rt::read_repeated_uint32_into(wire_type, is, &mut self.counters)?;
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
        for value in &self.counters {
            my_size += ::protobuf::rt::value_size(1, *value, ::protobuf::wire_format::WireTypeVarint);
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        for v in &self.counters {
            os.write_uint32(1, *v)?;
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

impl ::protobuf::MessageStatic for CMSketchRow {
    fn new() -> CMSketchRow {
        CMSketchRow::new()
    }

    fn descriptor_static(_: ::std::option::Option<CMSketchRow>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_vec_accessor::<_, ::protobuf::types::ProtobufTypeUint32>(
                    "counters",
                    CMSketchRow::get_counters_for_reflect,
                    CMSketchRow::mut_counters_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<CMSketchRow>(
                    "CMSketchRow",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for CMSketchRow {
    fn clear(&mut self) {
        self.clear_counters();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for CMSketchRow {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for CMSketchRow {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct CMSketch {
    // message fields
    rows: ::protobuf::RepeatedField<CMSketchRow>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for CMSketch {}

impl CMSketch {
    pub fn new() -> CMSketch {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static CMSketch {
        static mut instance: ::protobuf::lazy::Lazy<CMSketch> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const CMSketch,
        };
        unsafe {
            instance.get(CMSketch::new)
        }
    }

    // repeated .tipb.CMSketchRow rows = 1;

    pub fn clear_rows(&mut self) {
        self.rows.clear();
    }

    // Param is passed by value, moved
    pub fn set_rows(&mut self, v: ::protobuf::RepeatedField<CMSketchRow>) {
        self.rows = v;
    }

    // Mutable pointer to the field.
    pub fn mut_rows(&mut self) -> &mut ::protobuf::RepeatedField<CMSketchRow> {
        &mut self.rows
    }

    // Take field
    pub fn take_rows(&mut self) -> ::protobuf::RepeatedField<CMSketchRow> {
        ::std::mem::replace(&mut self.rows, ::protobuf::RepeatedField::new())
    }

    pub fn get_rows(&self) -> &[CMSketchRow] {
        &self.rows
    }

    fn get_rows_for_reflect(&self) -> &::protobuf::RepeatedField<CMSketchRow> {
        &self.rows
    }

    fn mut_rows_for_reflect(&mut self) -> &mut ::protobuf::RepeatedField<CMSketchRow> {
        &mut self.rows
    }
}

impl ::protobuf::Message for CMSketch {
    fn is_initialized(&self) -> bool {
        for v in &self.rows {
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
                    ::protobuf::rt::read_repeated_message_into(wire_type, is, &mut self.rows)?;
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
        for value in &self.rows {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        for v in &self.rows {
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

impl ::protobuf::MessageStatic for CMSketch {
    fn new() -> CMSketch {
        CMSketch::new()
    }

    fn descriptor_static(_: ::std::option::Option<CMSketch>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_repeated_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<CMSketchRow>>(
                    "rows",
                    CMSketch::get_rows_for_reflect,
                    CMSketch::mut_rows_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<CMSketch>(
                    "CMSketch",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for CMSketch {
    fn clear(&mut self) {
        self.clear_rows();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for CMSketch {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for CMSketch {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(Clone,PartialEq,Eq,Debug,Hash)]
pub enum AnalyzeType {
    TypeIndex = 0,
    TypeColumn = 1,
}

impl ::protobuf::ProtobufEnum for AnalyzeType {
    fn value(&self) -> i32 {
        *self as i32
    }

    fn from_i32(value: i32) -> ::std::option::Option<AnalyzeType> {
        match value {
            0 => ::std::option::Option::Some(AnalyzeType::TypeIndex),
            1 => ::std::option::Option::Some(AnalyzeType::TypeColumn),
            _ => ::std::option::Option::None
        }
    }

    fn values() -> &'static [Self] {
        static values: &'static [AnalyzeType] = &[
            AnalyzeType::TypeIndex,
            AnalyzeType::TypeColumn,
        ];
        values
    }

    fn enum_descriptor_static(_: ::std::option::Option<AnalyzeType>) -> &'static ::protobuf::reflect::EnumDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::EnumDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::EnumDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                ::protobuf::reflect::EnumDescriptor::new("AnalyzeType", file_descriptor_proto())
            })
        }
    }
}

impl ::std::marker::Copy for AnalyzeType {
}

impl ::protobuf::reflect::ProtobufValue for AnalyzeType {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Enum(self.descriptor())
    }
}

static file_descriptor_proto_data: &'static [u8] = b"\
    \n\ranalyze.proto\x12\x04tipb\x1a\x0cschema.proto\x1a\x14gogoproto/gogo.\
    proto\"\x84\x02\n\nAnalyzeReq\x12'\n\x02tp\x18\x01\x20\x01(\x0e2\x11.tip\
    b.AnalyzeTypeR\x02tpB\x04\xc8\xde\x1f\0\x12\x1f\n\x08start_ts\x18\x02\
    \x20\x01(\x04R\x07startTsB\x04\xc8\xde\x1f\0\x12\x1a\n\x05flags\x18\x03\
    \x20\x01(\x04R\x05flagsB\x04\xc8\xde\x1f\0\x12.\n\x10time_zone_offset\
    \x18\x04\x20\x01(\x03R\x0etimeZoneOffsetB\x04\xc8\xde\x1f\0\x12.\n\x07id\
    x_req\x18\x05\x20\x01(\x0b2\x15.tipb.AnalyzeIndexReqR\x06idxReq\x120\n\
    \x07col_req\x18\x06\x20\x01(\x0b2\x17.tipb.AnalyzeColumnsReqR\x06colReq\
    \"\xad\x01\n\x0fAnalyzeIndexReq\x12%\n\x0bbucket_size\x18\x01\x20\x01(\
    \x03R\nbucketSizeB\x04\xc8\xde\x1f\0\x12%\n\x0bnum_columns\x18\x02\x20\
    \x01(\x05R\nnumColumnsB\x04\xc8\xde\x1f\0\x12%\n\x0ecmsketch_depth\x18\
    \x03\x20\x01(\x05R\rcmsketchDepth\x12%\n\x0ecmsketch_width\x18\x04\x20\
    \x01(\x05R\rcmsketchWidth\"\x8b\x02\n\x11AnalyzeColumnsReq\x12%\n\x0bbuc\
    ket_size\x18\x01\x20\x01(\x03R\nbucketSizeB\x04\xc8\xde\x1f\0\x12%\n\x0b\
    sample_size\x18\x02\x20\x01(\x03R\nsampleSizeB\x04\xc8\xde\x1f\0\x12%\n\
    \x0bsketch_size\x18\x03\x20\x01(\x03R\nsketchSizeB\x04\xc8\xde\x1f\0\x12\
    3\n\x0ccolumns_info\x18\x04\x20\x03(\x0b2\x10.tipb.ColumnInfoR\x0bcolumn\
    sInfo\x12%\n\x0ecmsketch_depth\x18\x05\x20\x01(\x05R\rcmsketchDepth\x12%\
    \n\x0ecmsketch_width\x18\x06\x20\x01(\x05R\rcmsketchWidth\"u\n\x12Analyz\
    eColumnsResp\x125\n\ncollectors\x18\x01\x20\x03(\x0b2\x15.tipb.SampleCol\
    lectorR\ncollectors\x12(\n\x07pk_hist\x18\x02\x20\x01(\x0b2\x0f.tipb.His\
    togramR\x06pkHist\"Y\n\x10AnalyzeIndexResp\x12#\n\x04hist\x18\x01\x20\
    \x01(\x0b2\x0f.tipb.HistogramR\x04hist\x12\x20\n\x03cms\x18\x02\x20\x01(\
    \x0b2\x0e.tipb.CMSketchR\x03cms\"\x86\x01\n\x06Bucket\x12\x1a\n\x05count\
    \x18\x01\x20\x01(\x03R\x05countB\x04\xc8\xde\x1f\0\x12\x1f\n\x0blower_bo\
    und\x18\x02\x20\x01(\x0cR\nlowerBound\x12\x1f\n\x0bupper_bound\x18\x03\
    \x20\x01(\x0cR\nupperBound\x12\x1e\n\x07repeats\x18\x04\x20\x01(\x03R\
    \x07repeatsB\x04\xc8\xde\x1f\0\"K\n\tHistogram\x12\x16\n\x03ndv\x18\x01\
    \x20\x01(\x03R\x03ndvB\x04\xc8\xde\x1f\0\x12&\n\x07buckets\x18\x02\x20\
    \x03(\x0b2\x0c.tipb.BucketR\x07buckets\">\n\x08FMSketch\x12\x18\n\x04mas\
    k\x18\x01\x20\x01(\x04R\x04maskB\x04\xc8\xde\x1f\0\x12\x18\n\x07hashset\
    \x18\x02\x20\x03(\x04R\x07hashset\"\xc6\x01\n\x0fSampleCollector\x12\x18\
    \n\x07samples\x18\x01\x20\x03(\x0cR\x07samples\x12#\n\nnull_count\x18\
    \x02\x20\x01(\x03R\tnullCountB\x04\xc8\xde\x1f\0\x12\x1a\n\x05count\x18\
    \x03\x20\x01(\x03R\x05countB\x04\xc8\xde\x1f\0\x12+\n\tfm_sketch\x18\x04\
    \x20\x01(\x0b2\x0e.tipb.FMSketchR\x08fmSketch\x12+\n\tcm_sketch\x18\x05\
    \x20\x01(\x0b2\x0e.tipb.CMSketchR\x08cmSketch\")\n\x0bCMSketchRow\x12\
    \x1a\n\x08counters\x18\x01\x20\x03(\rR\x08counters\"1\n\x08CMSketch\x12%\
    \n\x04rows\x18\x01\x20\x03(\x0b2\x11.tipb.CMSketchRowR\x04rows*,\n\x0bAn\
    alyzeType\x12\r\n\tTypeIndex\x10\0\x12\x0e\n\nTypeColumn\x10\x01B%\n\x15\
    com.pingcap.tidb.tipbP\x01\xd0\xe2\x1e\x01\xc8\xe2\x1e\x01\xe0\xe2\x1e\
    \x01J\x90,\n\x06\x12\x04\0\0m\x01\n\x08\n\x01\x0c\x12\x03\0\0\x12\n\x08\
    \n\x01\x02\x12\x03\x02\x08\x0c\n\x08\n\x01\x08\x12\x03\x04\0\"\n\x0b\n\
    \x04\x08\xe7\x07\0\x12\x03\x04\0\"\n\x0c\n\x05\x08\xe7\x07\0\x02\x12\x03\
    \x04\x07\x1a\n\r\n\x06\x08\xe7\x07\0\x02\0\x12\x03\x04\x07\x1a\n\x0e\n\
    \x07\x08\xe7\x07\0\x02\0\x01\x12\x03\x04\x07\x1a\n\x0c\n\x05\x08\xe7\x07\
    \0\x03\x12\x03\x04\x1d!\n\x08\n\x01\x08\x12\x03\x05\0.\n\x0b\n\x04\x08\
    \xe7\x07\x01\x12\x03\x05\0.\n\x0c\n\x05\x08\xe7\x07\x01\x02\x12\x03\x05\
    \x07\x13\n\r\n\x06\x08\xe7\x07\x01\x02\0\x12\x03\x05\x07\x13\n\x0e\n\x07\
    \x08\xe7\x07\x01\x02\0\x01\x12\x03\x05\x07\x13\n\x0c\n\x05\x08\xe7\x07\
    \x01\x07\x12\x03\x05\x16-\n\t\n\x02\x03\0\x12\x03\x07\x07\x15\n\t\n\x02\
    \x03\x01\x12\x03\x08\x07\x1d\n\x08\n\x01\x08\x12\x03\n\0(\n\x0b\n\x04\
    \x08\xe7\x07\x02\x12\x03\n\0(\n\x0c\n\x05\x08\xe7\x07\x02\x02\x12\x03\n\
    \x07\x20\n\r\n\x06\x08\xe7\x07\x02\x02\0\x12\x03\n\x07\x20\n\x0e\n\x07\
    \x08\xe7\x07\x02\x02\0\x01\x12\x03\n\x08\x1f\n\x0c\n\x05\x08\xe7\x07\x02\
    \x03\x12\x03\n#'\n\x08\n\x01\x08\x12\x03\x0b\0$\n\x0b\n\x04\x08\xe7\x07\
    \x03\x12\x03\x0b\0$\n\x0c\n\x05\x08\xe7\x07\x03\x02\x12\x03\x0b\x07\x1c\
    \n\r\n\x06\x08\xe7\x07\x03\x02\0\x12\x03\x0b\x07\x1c\n\x0e\n\x07\x08\xe7\
    \x07\x03\x02\0\x01\x12\x03\x0b\x08\x1b\n\x0c\n\x05\x08\xe7\x07\x03\x03\
    \x12\x03\x0b\x1f#\n\x08\n\x01\x08\x12\x03\x0c\0*\n\x0b\n\x04\x08\xe7\x07\
    \x04\x12\x03\x0c\0*\n\x0c\n\x05\x08\xe7\x07\x04\x02\x12\x03\x0c\x07\"\n\
    \r\n\x06\x08\xe7\x07\x04\x02\0\x12\x03\x0c\x07\"\n\x0e\n\x07\x08\xe7\x07\
    \x04\x02\0\x01\x12\x03\x0c\x08!\n\x0c\n\x05\x08\xe7\x07\x04\x03\x12\x03\
    \x0c%)\n\n\n\x02\x05\0\x12\x04\x0e\0\x11\x01\n\n\n\x03\x05\0\x01\x12\x03\
    \x0e\x05\x10\n\x0b\n\x04\x05\0\x02\0\x12\x03\x0f\x04\x12\n\x0c\n\x05\x05\
    \0\x02\0\x01\x12\x03\x0f\x04\r\n\x0c\n\x05\x05\0\x02\0\x02\x12\x03\x0f\
    \x10\x11\n\x0b\n\x04\x05\0\x02\x01\x12\x03\x10\x04\x13\n\x0c\n\x05\x05\0\
    \x02\x01\x01\x12\x03\x10\x04\x0e\n\x0c\n\x05\x05\0\x02\x01\x02\x12\x03\
    \x10\x11\x12\n\n\n\x02\x04\0\x12\x04\x13\0\x1a\x01\n\n\n\x03\x04\0\x01\
    \x12\x03\x13\x08\x12\n\x0b\n\x04\x04\0\x02\0\x12\x03\x14\x04?\n\x0c\n\
    \x05\x04\0\x02\0\x04\x12\x03\x14\x04\x0c\n\x0c\n\x05\x04\0\x02\0\x06\x12\
    \x03\x14\r\x18\n\x0c\n\x05\x04\0\x02\0\x01\x12\x03\x14\x19\x1b\n\x0c\n\
    \x05\x04\0\x02\0\x03\x12\x03\x14\x1e\x1f\n\x0c\n\x05\x04\0\x02\0\x08\x12\
    \x03\x14\x20>\n\x0f\n\x08\x04\0\x02\0\x08\xe7\x07\0\x12\x03\x14!=\n\x10\
    \n\t\x04\0\x02\0\x08\xe7\x07\0\x02\x12\x03\x14!5\n\x11\n\n\x04\0\x02\0\
    \x08\xe7\x07\0\x02\0\x12\x03\x14!5\n\x12\n\x0b\x04\0\x02\0\x08\xe7\x07\0\
    \x02\0\x01\x12\x03\x14\"4\n\x10\n\t\x04\0\x02\0\x08\xe7\x07\0\x03\x12\
    \x03\x148=\n\x0b\n\x04\x04\0\x02\x01\x12\x03\x15\x04@\n\x0c\n\x05\x04\0\
    \x02\x01\x04\x12\x03\x15\x04\x0c\n\x0c\n\x05\x04\0\x02\x01\x05\x12\x03\
    \x15\r\x13\n\x0c\n\x05\x04\0\x02\x01\x01\x12\x03\x15\x14\x1c\n\x0c\n\x05\
    \x04\0\x02\x01\x03\x12\x03\x15\x1f\x20\n\x0c\n\x05\x04\0\x02\x01\x08\x12\
    \x03\x15!?\n\x0f\n\x08\x04\0\x02\x01\x08\xe7\x07\0\x12\x03\x15\">\n\x10\
    \n\t\x04\0\x02\x01\x08\xe7\x07\0\x02\x12\x03\x15\"6\n\x11\n\n\x04\0\x02\
    \x01\x08\xe7\x07\0\x02\0\x12\x03\x15\"6\n\x12\n\x0b\x04\0\x02\x01\x08\
    \xe7\x07\0\x02\0\x01\x12\x03\x15#5\n\x10\n\t\x04\0\x02\x01\x08\xe7\x07\0\
    \x03\x12\x03\x159>\n\x0b\n\x04\x04\0\x02\x02\x12\x03\x16\x04=\n\x0c\n\
    \x05\x04\0\x02\x02\x04\x12\x03\x16\x04\x0c\n\x0c\n\x05\x04\0\x02\x02\x05\
    \x12\x03\x16\r\x13\n\x0c\n\x05\x04\0\x02\x02\x01\x12\x03\x16\x14\x19\n\
    \x0c\n\x05\x04\0\x02\x02\x03\x12\x03\x16\x1c\x1d\n\x0c\n\x05\x04\0\x02\
    \x02\x08\x12\x03\x16\x1e<\n\x0f\n\x08\x04\0\x02\x02\x08\xe7\x07\0\x12\
    \x03\x16\x1f;\n\x10\n\t\x04\0\x02\x02\x08\xe7\x07\0\x02\x12\x03\x16\x1f3\
    \n\x11\n\n\x04\0\x02\x02\x08\xe7\x07\0\x02\0\x12\x03\x16\x1f3\n\x12\n\
    \x0b\x04\0\x02\x02\x08\xe7\x07\0\x02\0\x01\x12\x03\x16\x202\n\x10\n\t\
    \x04\0\x02\x02\x08\xe7\x07\0\x03\x12\x03\x166;\n\x0b\n\x04\x04\0\x02\x03\
    \x12\x03\x17\x04G\n\x0c\n\x05\x04\0\x02\x03\x04\x12\x03\x17\x04\x0c\n\
    \x0c\n\x05\x04\0\x02\x03\x05\x12\x03\x17\r\x12\n\x0c\n\x05\x04\0\x02\x03\
    \x01\x12\x03\x17\x13#\n\x0c\n\x05\x04\0\x02\x03\x03\x12\x03\x17&'\n\x0c\
    \n\x05\x04\0\x02\x03\x08\x12\x03\x17(F\n\x0f\n\x08\x04\0\x02\x03\x08\xe7\
    \x07\0\x12\x03\x17)E\n\x10\n\t\x04\0\x02\x03\x08\xe7\x07\0\x02\x12\x03\
    \x17)=\n\x11\n\n\x04\0\x02\x03\x08\xe7\x07\0\x02\0\x12\x03\x17)=\n\x12\n\
    \x0b\x04\0\x02\x03\x08\xe7\x07\0\x02\0\x01\x12\x03\x17*<\n\x10\n\t\x04\0\
    \x02\x03\x08\xe7\x07\0\x03\x12\x03\x17@E\n\x0b\n\x04\x04\0\x02\x04\x12\
    \x03\x18\x04)\n\x0c\n\x05\x04\0\x02\x04\x04\x12\x03\x18\x04\x0c\n\x0c\n\
    \x05\x04\0\x02\x04\x06\x12\x03\x18\r\x1c\n\x0c\n\x05\x04\0\x02\x04\x01\
    \x12\x03\x18\x1d$\n\x0c\n\x05\x04\0\x02\x04\x03\x12\x03\x18'(\n\x0b\n\
    \x04\x04\0\x02\x05\x12\x03\x19\x04+\n\x0c\n\x05\x04\0\x02\x05\x04\x12\
    \x03\x19\x04\x0c\n\x0c\n\x05\x04\0\x02\x05\x06\x12\x03\x19\r\x1e\n\x0c\n\
    \x05\x04\0\x02\x05\x01\x12\x03\x19\x1f&\n\x0c\n\x05\x04\0\x02\x05\x03\
    \x12\x03\x19)*\n\n\n\x02\x04\x01\x12\x04\x1c\0&\x01\n\n\n\x03\x04\x01\
    \x01\x12\x03\x1c\x08\x17\n=\n\x04\x04\x01\x02\0\x12\x03\x1e\x04B\x1a0\
    \x20bucket_size\x20is\x20the\x20max\x20histograms\x20bucket\x20size.\n\n\
    \x0c\n\x05\x04\x01\x02\0\x04\x12\x03\x1e\x04\x0c\n\x0c\n\x05\x04\x01\x02\
    \0\x05\x12\x03\x1e\r\x12\n\x0c\n\x05\x04\x01\x02\0\x01\x12\x03\x1e\x13\
    \x1e\n\x0c\n\x05\x04\x01\x02\0\x03\x12\x03\x1e!\"\n\x0c\n\x05\x04\x01\
    \x02\0\x08\x12\x03\x1e#A\n\x0f\n\x08\x04\x01\x02\0\x08\xe7\x07\0\x12\x03\
    \x1e$@\n\x10\n\t\x04\x01\x02\0\x08\xe7\x07\0\x02\x12\x03\x1e$8\n\x11\n\n\
    \x04\x01\x02\0\x08\xe7\x07\0\x02\0\x12\x03\x1e$8\n\x12\n\x0b\x04\x01\x02\
    \0\x08\xe7\x07\0\x02\0\x01\x12\x03\x1e%7\n\x10\n\t\x04\x01\x02\0\x08\xe7\
    \x07\0\x03\x12\x03\x1e;@\nA\n\x04\x04\x01\x02\x01\x12\x03!\x04B\x1a4\x20\
    num_columns\x20is\x20the\x20number\x20of\x20columns\x20in\x20the\x20inde\
    x.\n\n\x0c\n\x05\x04\x01\x02\x01\x04\x12\x03!\x04\x0c\n\x0c\n\x05\x04\
    \x01\x02\x01\x05\x12\x03!\r\x12\n\x0c\n\x05\x04\x01\x02\x01\x01\x12\x03!\
    \x13\x1e\n\x0c\n\x05\x04\x01\x02\x01\x03\x12\x03!!\"\n\x0c\n\x05\x04\x01\
    \x02\x01\x08\x12\x03!#A\n\x0f\n\x08\x04\x01\x02\x01\x08\xe7\x07\0\x12\
    \x03!$@\n\x10\n\t\x04\x01\x02\x01\x08\xe7\x07\0\x02\x12\x03!$8\n\x11\n\n\
    \x04\x01\x02\x01\x08\xe7\x07\0\x02\0\x12\x03!$8\n\x12\n\x0b\x04\x01\x02\
    \x01\x08\xe7\x07\0\x02\0\x01\x12\x03!%7\n\x10\n\t\x04\x01\x02\x01\x08\
    \xe7\x07\0\x03\x12\x03!;@\n\x0b\n\x04\x04\x01\x02\x02\x12\x03#\x04&\n\
    \x0c\n\x05\x04\x01\x02\x02\x04\x12\x03#\x04\x0c\n\x0c\n\x05\x04\x01\x02\
    \x02\x05\x12\x03#\r\x12\n\x0c\n\x05\x04\x01\x02\x02\x01\x12\x03#\x13!\n\
    \x0c\n\x05\x04\x01\x02\x02\x03\x12\x03#$%\n\x0b\n\x04\x04\x01\x02\x03\
    \x12\x03%\x04&\n\x0c\n\x05\x04\x01\x02\x03\x04\x12\x03%\x04\x0c\n\x0c\n\
    \x05\x04\x01\x02\x03\x05\x12\x03%\r\x12\n\x0c\n\x05\x04\x01\x02\x03\x01\
    \x12\x03%\x13!\n\x0c\n\x05\x04\x01\x02\x03\x03\x12\x03%$%\n\n\n\x02\x04\
    \x02\x12\x04(\09\x01\n\n\n\x03\x04\x02\x01\x12\x03(\x08\x19\n\x96\x01\n\
    \x04\x04\x02\x02\0\x12\x03+\x04B\x1a\x88\x01\x20bucket_size\x20is\x20the\
    \x20max\x20histograms\x20bucket\x20size,\x20we\x20need\x20this\x20becaus\
    e\x20when\x20primary\x20key\x20is\x20handle,\n\x20the\x20histogram\x20wi\
    ll\x20be\x20directly\x20built.\n\n\x0c\n\x05\x04\x02\x02\0\x04\x12\x03+\
    \x04\x0c\n\x0c\n\x05\x04\x02\x02\0\x05\x12\x03+\r\x12\n\x0c\n\x05\x04\
    \x02\x02\0\x01\x12\x03+\x13\x1e\n\x0c\n\x05\x04\x02\x02\0\x03\x12\x03+!\
    \"\n\x0c\n\x05\x04\x02\x02\0\x08\x12\x03+#A\n\x0f\n\x08\x04\x02\x02\0\
    \x08\xe7\x07\0\x12\x03+$@\n\x10\n\t\x04\x02\x02\0\x08\xe7\x07\0\x02\x12\
    \x03+$8\n\x11\n\n\x04\x02\x02\0\x08\xe7\x07\0\x02\0\x12\x03+$8\n\x12\n\
    \x0b\x04\x02\x02\0\x08\xe7\x07\0\x02\0\x01\x12\x03+%7\n\x10\n\t\x04\x02\
    \x02\0\x08\xe7\x07\0\x03\x12\x03+;@\nO\n\x04\x04\x02\x02\x01\x12\x03.\
    \x04B\x1aB\x20sample_size\x20is\x20the\x20max\x20number\x20of\x20samples\
    \x20that\x20will\x20be\x20collected.\n\n\x0c\n\x05\x04\x02\x02\x01\x04\
    \x12\x03.\x04\x0c\n\x0c\n\x05\x04\x02\x02\x01\x05\x12\x03.\r\x12\n\x0c\n\
    \x05\x04\x02\x02\x01\x01\x12\x03.\x13\x1e\n\x0c\n\x05\x04\x02\x02\x01\
    \x03\x12\x03.!\"\n\x0c\n\x05\x04\x02\x02\x01\x08\x12\x03.#A\n\x0f\n\x08\
    \x04\x02\x02\x01\x08\xe7\x07\0\x12\x03.$@\n\x10\n\t\x04\x02\x02\x01\x08\
    \xe7\x07\0\x02\x12\x03.$8\n\x11\n\n\x04\x02\x02\x01\x08\xe7\x07\0\x02\0\
    \x12\x03.$8\n\x12\n\x0b\x04\x02\x02\x01\x08\xe7\x07\0\x02\0\x01\x12\x03.\
    %7\n\x10\n\t\x04\x02\x02\x01\x08\xe7\x07\0\x03\x12\x03.;@\n2\n\x04\x04\
    \x02\x02\x02\x12\x031\x04B\x1a%\x20sketch_size\x20is\x20the\x20max\x20sk\
    etch\x20size.\n\n\x0c\n\x05\x04\x02\x02\x02\x04\x12\x031\x04\x0c\n\x0c\n\
    \x05\x04\x02\x02\x02\x05\x12\x031\r\x12\n\x0c\n\x05\x04\x02\x02\x02\x01\
    \x12\x031\x13\x1e\n\x0c\n\x05\x04\x02\x02\x02\x03\x12\x031!\"\n\x0c\n\
    \x05\x04\x02\x02\x02\x08\x12\x031#A\n\x0f\n\x08\x04\x02\x02\x02\x08\xe7\
    \x07\0\x12\x031$@\n\x10\n\t\x04\x02\x02\x02\x08\xe7\x07\0\x02\x12\x031$8\
    \n\x11\n\n\x04\x02\x02\x02\x08\xe7\x07\0\x02\0\x12\x031$8\n\x12\n\x0b\
    \x04\x02\x02\x02\x08\xe7\x07\0\x02\0\x01\x12\x031%7\n\x10\n\t\x04\x02\
    \x02\x02\x08\xe7\x07\0\x03\x12\x031;@\nU\n\x04\x04\x02\x02\x03\x12\x034\
    \x04)\x1aH\x20columns_info\x20is\x20the\x20info\x20of\x20all\x20the\x20c\
    olumns\x20that\x20needs\x20to\x20be\x20analyzed.\n\n\x0c\n\x05\x04\x02\
    \x02\x03\x04\x12\x034\x04\x0c\n\x0c\n\x05\x04\x02\x02\x03\x06\x12\x034\r\
    \x17\n\x0c\n\x05\x04\x02\x02\x03\x01\x12\x034\x18$\n\x0c\n\x05\x04\x02\
    \x02\x03\x03\x12\x034'(\n\x0b\n\x04\x04\x02\x02\x04\x12\x036\x04&\n\x0c\
    \n\x05\x04\x02\x02\x04\x04\x12\x036\x04\x0c\n\x0c\n\x05\x04\x02\x02\x04\
    \x05\x12\x036\r\x12\n\x0c\n\x05\x04\x02\x02\x04\x01\x12\x036\x13!\n\x0c\
    \n\x05\x04\x02\x02\x04\x03\x12\x036$%\n\x0b\n\x04\x04\x02\x02\x05\x12\
    \x038\x04&\n\x0c\n\x05\x04\x02\x02\x05\x04\x12\x038\x04\x0c\n\x0c\n\x05\
    \x04\x02\x02\x05\x05\x12\x038\r\x12\n\x0c\n\x05\x04\x02\x02\x05\x01\x12\
    \x038\x13!\n\x0c\n\x05\x04\x02\x02\x05\x03\x12\x038$%\n\n\n\x02\x04\x03\
    \x12\x04;\0A\x01\n\n\n\x03\x04\x03\x01\x12\x03;\x08\x1a\n?\n\x04\x04\x03\
    \x02\0\x12\x03=\x04,\x1a2\x20collectors\x20is\x20the\x20sample\x20collec\
    tors\x20for\x20columns.\n\n\x0c\n\x05\x04\x03\x02\0\x04\x12\x03=\x04\x0c\
    \n\x0c\n\x05\x04\x03\x02\0\x06\x12\x03=\r\x1c\n\x0c\n\x05\x04\x03\x02\0\
    \x01\x12\x03=\x1d'\n\x0c\n\x05\x04\x03\x02\0\x03\x12\x03=*+\nN\n\x04\x04\
    \x03\x02\x01\x12\x03@\x04#\x1aA\x20pk_hist\x20is\x20the\x20histogram\x20\
    for\x20primary\x20key\x20when\x20it\x20is\x20the\x20handle.\n\n\x0c\n\
    \x05\x04\x03\x02\x01\x04\x12\x03@\x04\x0c\n\x0c\n\x05\x04\x03\x02\x01\
    \x06\x12\x03@\r\x16\n\x0c\n\x05\x04\x03\x02\x01\x01\x12\x03@\x17\x1e\n\
    \x0c\n\x05\x04\x03\x02\x01\x03\x12\x03@!\"\n\n\n\x02\x04\x04\x12\x04C\0F\
    \x01\n\n\n\x03\x04\x04\x01\x12\x03C\x08\x18\n\x0b\n\x04\x04\x04\x02\0\
    \x12\x03D\x04\x20\n\x0c\n\x05\x04\x04\x02\0\x04\x12\x03D\x04\x0c\n\x0c\n\
    \x05\x04\x04\x02\0\x06\x12\x03D\r\x16\n\x0c\n\x05\x04\x04\x02\0\x01\x12\
    \x03D\x17\x1b\n\x0c\n\x05\x04\x04\x02\0\x03\x12\x03D\x1e\x1f\n\x0b\n\x04\
    \x04\x04\x02\x01\x12\x03E\x04\x1e\n\x0c\n\x05\x04\x04\x02\x01\x04\x12\
    \x03E\x04\x0c\n\x0c\n\x05\x04\x04\x02\x01\x06\x12\x03E\r\x15\n\x0c\n\x05\
    \x04\x04\x02\x01\x01\x12\x03E\x16\x19\n\x0c\n\x05\x04\x04\x02\x01\x03\
    \x12\x03E\x1c\x1d\n0\n\x02\x04\x05\x12\x04I\0N\x01\x1a$\x20Bucket\x20is\
    \x20an\x20element\x20of\x20histogram.\n\n\n\n\x03\x04\x05\x01\x12\x03I\
    \x08\x0e\n\x0b\n\x04\x04\x05\x02\0\x12\x03J\x04<\n\x0c\n\x05\x04\x05\x02\
    \0\x04\x12\x03J\x04\x0c\n\x0c\n\x05\x04\x05\x02\0\x05\x12\x03J\r\x12\n\
    \x0c\n\x05\x04\x05\x02\0\x01\x12\x03J\x13\x18\n\x0c\n\x05\x04\x05\x02\0\
    \x03\x12\x03J\x1b\x1c\n\x0c\n\x05\x04\x05\x02\0\x08\x12\x03J\x1d;\n\x0f\
    \n\x08\x04\x05\x02\0\x08\xe7\x07\0\x12\x03J\x1e:\n\x10\n\t\x04\x05\x02\0\
    \x08\xe7\x07\0\x02\x12\x03J\x1e2\n\x11\n\n\x04\x05\x02\0\x08\xe7\x07\0\
    \x02\0\x12\x03J\x1e2\n\x12\n\x0b\x04\x05\x02\0\x08\xe7\x07\0\x02\0\x01\
    \x12\x03J\x1f1\n\x10\n\t\x04\x05\x02\0\x08\xe7\x07\0\x03\x12\x03J5:\n\
    \x0b\n\x04\x04\x05\x02\x01\x12\x03K\x04#\n\x0c\n\x05\x04\x05\x02\x01\x04\
    \x12\x03K\x04\x0c\n\x0c\n\x05\x04\x05\x02\x01\x05\x12\x03K\r\x12\n\x0c\n\
    \x05\x04\x05\x02\x01\x01\x12\x03K\x13\x1e\n\x0c\n\x05\x04\x05\x02\x01\
    \x03\x12\x03K!\"\n\x0b\n\x04\x04\x05\x02\x02\x12\x03L\x04#\n\x0c\n\x05\
    \x04\x05\x02\x02\x04\x12\x03L\x04\x0c\n\x0c\n\x05\x04\x05\x02\x02\x05\
    \x12\x03L\r\x12\n\x0c\n\x05\x04\x05\x02\x02\x01\x12\x03L\x13\x1e\n\x0c\n\
    \x05\x04\x05\x02\x02\x03\x12\x03L!\"\n\x0b\n\x04\x04\x05\x02\x03\x12\x03\
    M\x04>\n\x0c\n\x05\x04\x05\x02\x03\x04\x12\x03M\x04\x0c\n\x0c\n\x05\x04\
    \x05\x02\x03\x05\x12\x03M\r\x12\n\x0c\n\x05\x04\x05\x02\x03\x01\x12\x03M\
    \x13\x1a\n\x0c\n\x05\x04\x05\x02\x03\x03\x12\x03M\x1d\x1e\n\x0c\n\x05\
    \x04\x05\x02\x03\x08\x12\x03M\x1f=\n\x0f\n\x08\x04\x05\x02\x03\x08\xe7\
    \x07\0\x12\x03M\x20<\n\x10\n\t\x04\x05\x02\x03\x08\xe7\x07\0\x02\x12\x03\
    M\x204\n\x11\n\n\x04\x05\x02\x03\x08\xe7\x07\0\x02\0\x12\x03M\x204\n\x12\
    \n\x0b\x04\x05\x02\x03\x08\xe7\x07\0\x02\0\x01\x12\x03M!3\n\x10\n\t\x04\
    \x05\x02\x03\x08\xe7\x07\0\x03\x12\x03M7<\n\n\n\x02\x04\x06\x12\x04P\0V\
    \x01\n\n\n\x03\x04\x06\x01\x12\x03P\x08\x11\n4\n\x04\x04\x06\x02\0\x12\
    \x03R\x04:\x1a'\x20ndv\x20is\x20the\x20number\x20of\x20distinct\x20value\
    s.\n\n\x0c\n\x05\x04\x06\x02\0\x04\x12\x03R\x04\x0c\n\x0c\n\x05\x04\x06\
    \x02\0\x05\x12\x03R\r\x12\n\x0c\n\x05\x04\x06\x02\0\x01\x12\x03R\x13\x16\
    \n\x0c\n\x05\x04\x06\x02\0\x03\x12\x03R\x19\x1a\n\x0c\n\x05\x04\x06\x02\
    \0\x08\x12\x03R\x1b9\n\x0f\n\x08\x04\x06\x02\0\x08\xe7\x07\0\x12\x03R\
    \x1c8\n\x10\n\t\x04\x06\x02\0\x08\xe7\x07\0\x02\x12\x03R\x1c0\n\x11\n\n\
    \x04\x06\x02\0\x08\xe7\x07\0\x02\0\x12\x03R\x1c0\n\x12\n\x0b\x04\x06\x02\
    \0\x08\xe7\x07\0\x02\0\x01\x12\x03R\x1d/\n\x10\n\t\x04\x06\x02\0\x08\xe7\
    \x07\0\x03\x12\x03R38\n2\n\x04\x04\x06\x02\x01\x12\x03U\x04\x20\x1a%\x20\
    buckets\x20represents\x20all\x20the\x20buckets.\n\n\x0c\n\x05\x04\x06\
    \x02\x01\x04\x12\x03U\x04\x0c\n\x0c\n\x05\x04\x06\x02\x01\x06\x12\x03U\r\
    \x13\n\x0c\n\x05\x04\x06\x02\x01\x01\x12\x03U\x14\x1b\n\x0c\n\x05\x04\
    \x06\x02\x01\x03\x12\x03U\x1e\x1f\nD\n\x02\x04\x07\x12\x04Y\0\\\x01\x1a8\
    \x20FMSketch\x20is\x20used\x20to\x20count\x20distinct\x20values\x20for\
    \x20columns.\n\n\n\n\x03\x04\x07\x01\x12\x03Y\x08\x10\n\x0b\n\x04\x04\
    \x07\x02\0\x12\x03Z\x04<\n\x0c\n\x05\x04\x07\x02\0\x04\x12\x03Z\x04\x0c\
    \n\x0c\n\x05\x04\x07\x02\0\x05\x12\x03Z\r\x13\n\x0c\n\x05\x04\x07\x02\0\
    \x01\x12\x03Z\x14\x18\n\x0c\n\x05\x04\x07\x02\0\x03\x12\x03Z\x1b\x1c\n\
    \x0c\n\x05\x04\x07\x02\0\x08\x12\x03Z\x1d;\n\x0f\n\x08\x04\x07\x02\0\x08\
    \xe7\x07\0\x12\x03Z\x1e:\n\x10\n\t\x04\x07\x02\0\x08\xe7\x07\0\x02\x12\
    \x03Z\x1e2\n\x11\n\n\x04\x07\x02\0\x08\xe7\x07\0\x02\0\x12\x03Z\x1e2\n\
    \x12\n\x0b\x04\x07\x02\0\x08\xe7\x07\0\x02\0\x01\x12\x03Z\x1f1\n\x10\n\t\
    \x04\x07\x02\0\x08\xe7\x07\0\x03\x12\x03Z5:\n\x0b\n\x04\x04\x07\x02\x01\
    \x12\x03[\x04\x20\n\x0c\n\x05\x04\x07\x02\x01\x04\x12\x03[\x04\x0c\n\x0c\
    \n\x05\x04\x07\x02\x01\x05\x12\x03[\r\x13\n\x0c\n\x05\x04\x07\x02\x01\
    \x01\x12\x03[\x14\x1b\n\x0c\n\x05\x04\x07\x02\x01\x03\x12\x03[\x1e\x1f\n\
    g\n\x02\x04\x08\x12\x04_\0e\x01\x1a[\x20SampleCollector\x20is\x20used\
    \x20for\x20collect\x20samples\x20and\x20calculate\x20the\x20count\x20and\
    \x20ndv\x20of\x20an\x20column.\n\n\n\n\x03\x04\x08\x01\x12\x03_\x08\x17\
    \n\x0b\n\x04\x04\x08\x02\0\x12\x03`\x04\x1f\n\x0c\n\x05\x04\x08\x02\0\
    \x04\x12\x03`\x04\x0c\n\x0c\n\x05\x04\x08\x02\0\x05\x12\x03`\r\x12\n\x0c\
    \n\x05\x04\x08\x02\0\x01\x12\x03`\x13\x1a\n\x0c\n\x05\x04\x08\x02\0\x03\
    \x12\x03`\x1d\x1e\n\x0b\n\x04\x04\x08\x02\x01\x12\x03a\x04A\n\x0c\n\x05\
    \x04\x08\x02\x01\x04\x12\x03a\x04\x0c\n\x0c\n\x05\x04\x08\x02\x01\x05\
    \x12\x03a\r\x12\n\x0c\n\x05\x04\x08\x02\x01\x01\x12\x03a\x13\x1d\n\x0c\n\
    \x05\x04\x08\x02\x01\x03\x12\x03a\x20!\n\x0c\n\x05\x04\x08\x02\x01\x08\
    \x12\x03a\"@\n\x0f\n\x08\x04\x08\x02\x01\x08\xe7\x07\0\x12\x03a#?\n\x10\
    \n\t\x04\x08\x02\x01\x08\xe7\x07\0\x02\x12\x03a#7\n\x11\n\n\x04\x08\x02\
    \x01\x08\xe7\x07\0\x02\0\x12\x03a#7\n\x12\n\x0b\x04\x08\x02\x01\x08\xe7\
    \x07\0\x02\0\x01\x12\x03a$6\n\x10\n\t\x04\x08\x02\x01\x08\xe7\x07\0\x03\
    \x12\x03a:?\n\x0b\n\x04\x04\x08\x02\x02\x12\x03b\x04<\n\x0c\n\x05\x04\
    \x08\x02\x02\x04\x12\x03b\x04\x0c\n\x0c\n\x05\x04\x08\x02\x02\x05\x12\
    \x03b\r\x12\n\x0c\n\x05\x04\x08\x02\x02\x01\x12\x03b\x13\x18\n\x0c\n\x05\
    \x04\x08\x02\x02\x03\x12\x03b\x1b\x1c\n\x0c\n\x05\x04\x08\x02\x02\x08\
    \x12\x03b\x1d;\n\x0f\n\x08\x04\x08\x02\x02\x08\xe7\x07\0\x12\x03b\x1e:\n\
    \x10\n\t\x04\x08\x02\x02\x08\xe7\x07\0\x02\x12\x03b\x1e2\n\x11\n\n\x04\
    \x08\x02\x02\x08\xe7\x07\0\x02\0\x12\x03b\x1e2\n\x12\n\x0b\x04\x08\x02\
    \x02\x08\xe7\x07\0\x02\0\x01\x12\x03b\x1f1\n\x10\n\t\x04\x08\x02\x02\x08\
    \xe7\x07\0\x03\x12\x03b5:\n\x0b\n\x04\x04\x08\x02\x03\x12\x03c\x04$\n\
    \x0c\n\x05\x04\x08\x02\x03\x04\x12\x03c\x04\x0c\n\x0c\n\x05\x04\x08\x02\
    \x03\x06\x12\x03c\r\x15\n\x0c\n\x05\x04\x08\x02\x03\x01\x12\x03c\x16\x1f\
    \n\x0c\n\x05\x04\x08\x02\x03\x03\x12\x03c\"#\n\x0b\n\x04\x04\x08\x02\x04\
    \x12\x03d\x04$\n\x0c\n\x05\x04\x08\x02\x04\x04\x12\x03d\x04\x0c\n\x0c\n\
    \x05\x04\x08\x02\x04\x06\x12\x03d\r\x15\n\x0c\n\x05\x04\x08\x02\x04\x01\
    \x12\x03d\x16\x1f\n\x0c\n\x05\x04\x08\x02\x04\x03\x12\x03d\"#\n\n\n\x02\
    \x04\t\x12\x04g\0i\x01\n\n\n\x03\x04\t\x01\x12\x03g\x08\x13\n\x0b\n\x04\
    \x04\t\x02\0\x12\x03h\x04!\n\x0c\n\x05\x04\t\x02\0\x04\x12\x03h\x04\x0c\
    \n\x0c\n\x05\x04\t\x02\0\x05\x12\x03h\r\x13\n\x0c\n\x05\x04\t\x02\0\x01\
    \x12\x03h\x14\x1c\n\x0c\n\x05\x04\t\x02\0\x03\x12\x03h\x1f\x20\n\n\n\x02\
    \x04\n\x12\x04k\0m\x01\n\n\n\x03\x04\n\x01\x12\x03k\x08\x10\n\x0b\n\x04\
    \x04\n\x02\0\x12\x03l\x04\"\n\x0c\n\x05\x04\n\x02\0\x04\x12\x03l\x04\x0c\
    \n\x0c\n\x05\x04\n\x02\0\x06\x12\x03l\r\x18\n\x0c\n\x05\x04\n\x02\0\x01\
    \x12\x03l\x19\x1d\n\x0c\n\x05\x04\n\x02\0\x03\x12\x03l\x20!\
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
