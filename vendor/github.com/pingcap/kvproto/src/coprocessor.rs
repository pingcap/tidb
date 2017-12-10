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
pub struct KeyRange {
    // message fields
    pub start: ::std::vec::Vec<u8>,
    pub end: ::std::vec::Vec<u8>,
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

    // bytes start = 1;

    pub fn clear_start(&mut self) {
        self.start.clear();
    }

    // Param is passed by value, moved
    pub fn set_start(&mut self, v: ::std::vec::Vec<u8>) {
        self.start = v;
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_start(&mut self) -> &mut ::std::vec::Vec<u8> {
        &mut self.start
    }

    // Take field
    pub fn take_start(&mut self) -> ::std::vec::Vec<u8> {
        ::std::mem::replace(&mut self.start, ::std::vec::Vec::new())
    }

    pub fn get_start(&self) -> &[u8] {
        &self.start
    }

    fn get_start_for_reflect(&self) -> &::std::vec::Vec<u8> {
        &self.start
    }

    fn mut_start_for_reflect(&mut self) -> &mut ::std::vec::Vec<u8> {
        &mut self.start
    }

    // bytes end = 2;

    pub fn clear_end(&mut self) {
        self.end.clear();
    }

    // Param is passed by value, moved
    pub fn set_end(&mut self, v: ::std::vec::Vec<u8>) {
        self.end = v;
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_end(&mut self) -> &mut ::std::vec::Vec<u8> {
        &mut self.end
    }

    // Take field
    pub fn take_end(&mut self) -> ::std::vec::Vec<u8> {
        ::std::mem::replace(&mut self.end, ::std::vec::Vec::new())
    }

    pub fn get_end(&self) -> &[u8] {
        &self.end
    }

    fn get_end_for_reflect(&self) -> &::std::vec::Vec<u8> {
        &self.end
    }

    fn mut_end_for_reflect(&mut self) -> &mut ::std::vec::Vec<u8> {
        &mut self.end
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
                    ::protobuf::rt::read_singular_proto3_bytes_into(wire_type, is, &mut self.start)?;
                },
                2 => {
                    ::protobuf::rt::read_singular_proto3_bytes_into(wire_type, is, &mut self.end)?;
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
        if !self.start.is_empty() {
            my_size += ::protobuf::rt::bytes_size(1, &self.start);
        }
        if !self.end.is_empty() {
            my_size += ::protobuf::rt::bytes_size(2, &self.end);
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if !self.start.is_empty() {
            os.write_bytes(1, &self.start)?;
        }
        if !self.end.is_empty() {
            os.write_bytes(2, &self.end)?;
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
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeBytes>(
                    "start",
                    KeyRange::get_start_for_reflect,
                    KeyRange::mut_start_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeBytes>(
                    "end",
                    KeyRange::get_end_for_reflect,
                    KeyRange::mut_end_for_reflect,
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
        self.clear_start();
        self.clear_end();
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

#[derive(PartialEq,Clone,Default)]
pub struct Request {
    // message fields
    pub context: ::protobuf::SingularPtrField<super::kvrpcpb::Context>,
    pub tp: i64,
    pub data: ::std::vec::Vec<u8>,
    pub ranges: ::protobuf::RepeatedField<KeyRange>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for Request {}

impl Request {
    pub fn new() -> Request {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static Request {
        static mut instance: ::protobuf::lazy::Lazy<Request> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const Request,
        };
        unsafe {
            instance.get(Request::new)
        }
    }

    // .kvrpcpb.Context context = 1;

    pub fn clear_context(&mut self) {
        self.context.clear();
    }

    pub fn has_context(&self) -> bool {
        self.context.is_some()
    }

    // Param is passed by value, moved
    pub fn set_context(&mut self, v: super::kvrpcpb::Context) {
        self.context = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_context(&mut self) -> &mut super::kvrpcpb::Context {
        if self.context.is_none() {
            self.context.set_default();
        }
        self.context.as_mut().unwrap()
    }

    // Take field
    pub fn take_context(&mut self) -> super::kvrpcpb::Context {
        self.context.take().unwrap_or_else(|| super::kvrpcpb::Context::new())
    }

    pub fn get_context(&self) -> &super::kvrpcpb::Context {
        self.context.as_ref().unwrap_or_else(|| super::kvrpcpb::Context::default_instance())
    }

    fn get_context_for_reflect(&self) -> &::protobuf::SingularPtrField<super::kvrpcpb::Context> {
        &self.context
    }

    fn mut_context_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<super::kvrpcpb::Context> {
        &mut self.context
    }

    // int64 tp = 2;

    pub fn clear_tp(&mut self) {
        self.tp = 0;
    }

    // Param is passed by value, moved
    pub fn set_tp(&mut self, v: i64) {
        self.tp = v;
    }

    pub fn get_tp(&self) -> i64 {
        self.tp
    }

    fn get_tp_for_reflect(&self) -> &i64 {
        &self.tp
    }

    fn mut_tp_for_reflect(&mut self) -> &mut i64 {
        &mut self.tp
    }

    // bytes data = 3;

    pub fn clear_data(&mut self) {
        self.data.clear();
    }

    // Param is passed by value, moved
    pub fn set_data(&mut self, v: ::std::vec::Vec<u8>) {
        self.data = v;
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_data(&mut self) -> &mut ::std::vec::Vec<u8> {
        &mut self.data
    }

    // Take field
    pub fn take_data(&mut self) -> ::std::vec::Vec<u8> {
        ::std::mem::replace(&mut self.data, ::std::vec::Vec::new())
    }

    pub fn get_data(&self) -> &[u8] {
        &self.data
    }

    fn get_data_for_reflect(&self) -> &::std::vec::Vec<u8> {
        &self.data
    }

    fn mut_data_for_reflect(&mut self) -> &mut ::std::vec::Vec<u8> {
        &mut self.data
    }

    // repeated .coprocessor.KeyRange ranges = 4;

    pub fn clear_ranges(&mut self) {
        self.ranges.clear();
    }

    // Param is passed by value, moved
    pub fn set_ranges(&mut self, v: ::protobuf::RepeatedField<KeyRange>) {
        self.ranges = v;
    }

    // Mutable pointer to the field.
    pub fn mut_ranges(&mut self) -> &mut ::protobuf::RepeatedField<KeyRange> {
        &mut self.ranges
    }

    // Take field
    pub fn take_ranges(&mut self) -> ::protobuf::RepeatedField<KeyRange> {
        ::std::mem::replace(&mut self.ranges, ::protobuf::RepeatedField::new())
    }

    pub fn get_ranges(&self) -> &[KeyRange] {
        &self.ranges
    }

    fn get_ranges_for_reflect(&self) -> &::protobuf::RepeatedField<KeyRange> {
        &self.ranges
    }

    fn mut_ranges_for_reflect(&mut self) -> &mut ::protobuf::RepeatedField<KeyRange> {
        &mut self.ranges
    }
}

impl ::protobuf::Message for Request {
    fn is_initialized(&self) -> bool {
        for v in &self.context {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.ranges {
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
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.context)?;
                },
                2 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_int64()?;
                    self.tp = tmp;
                },
                3 => {
                    ::protobuf::rt::read_singular_proto3_bytes_into(wire_type, is, &mut self.data)?;
                },
                4 => {
                    ::protobuf::rt::read_repeated_message_into(wire_type, is, &mut self.ranges)?;
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
        if let Some(ref v) = self.context.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if self.tp != 0 {
            my_size += ::protobuf::rt::value_size(2, self.tp, ::protobuf::wire_format::WireTypeVarint);
        }
        if !self.data.is_empty() {
            my_size += ::protobuf::rt::bytes_size(3, &self.data);
        }
        for value in &self.ranges {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(ref v) = self.context.as_ref() {
            os.write_tag(1, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if self.tp != 0 {
            os.write_int64(2, self.tp)?;
        }
        if !self.data.is_empty() {
            os.write_bytes(3, &self.data)?;
        }
        for v in &self.ranges {
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

impl ::protobuf::MessageStatic for Request {
    fn new() -> Request {
        Request::new()
    }

    fn descriptor_static(_: ::std::option::Option<Request>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<super::kvrpcpb::Context>>(
                    "context",
                    Request::get_context_for_reflect,
                    Request::mut_context_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeInt64>(
                    "tp",
                    Request::get_tp_for_reflect,
                    Request::mut_tp_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeBytes>(
                    "data",
                    Request::get_data_for_reflect,
                    Request::mut_data_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_repeated_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<KeyRange>>(
                    "ranges",
                    Request::get_ranges_for_reflect,
                    Request::mut_ranges_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<Request>(
                    "Request",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for Request {
    fn clear(&mut self) {
        self.clear_context();
        self.clear_tp();
        self.clear_data();
        self.clear_ranges();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for Request {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for Request {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct Response {
    // message fields
    pub data: ::std::vec::Vec<u8>,
    pub region_error: ::protobuf::SingularPtrField<super::errorpb::Error>,
    pub locked: ::protobuf::SingularPtrField<super::kvrpcpb::LockInfo>,
    pub other_error: ::std::string::String,
    pub range: ::protobuf::SingularPtrField<KeyRange>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for Response {}

impl Response {
    pub fn new() -> Response {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static Response {
        static mut instance: ::protobuf::lazy::Lazy<Response> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const Response,
        };
        unsafe {
            instance.get(Response::new)
        }
    }

    // bytes data = 1;

    pub fn clear_data(&mut self) {
        self.data.clear();
    }

    // Param is passed by value, moved
    pub fn set_data(&mut self, v: ::std::vec::Vec<u8>) {
        self.data = v;
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_data(&mut self) -> &mut ::std::vec::Vec<u8> {
        &mut self.data
    }

    // Take field
    pub fn take_data(&mut self) -> ::std::vec::Vec<u8> {
        ::std::mem::replace(&mut self.data, ::std::vec::Vec::new())
    }

    pub fn get_data(&self) -> &[u8] {
        &self.data
    }

    fn get_data_for_reflect(&self) -> &::std::vec::Vec<u8> {
        &self.data
    }

    fn mut_data_for_reflect(&mut self) -> &mut ::std::vec::Vec<u8> {
        &mut self.data
    }

    // .errorpb.Error region_error = 2;

    pub fn clear_region_error(&mut self) {
        self.region_error.clear();
    }

    pub fn has_region_error(&self) -> bool {
        self.region_error.is_some()
    }

    // Param is passed by value, moved
    pub fn set_region_error(&mut self, v: super::errorpb::Error) {
        self.region_error = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_region_error(&mut self) -> &mut super::errorpb::Error {
        if self.region_error.is_none() {
            self.region_error.set_default();
        }
        self.region_error.as_mut().unwrap()
    }

    // Take field
    pub fn take_region_error(&mut self) -> super::errorpb::Error {
        self.region_error.take().unwrap_or_else(|| super::errorpb::Error::new())
    }

    pub fn get_region_error(&self) -> &super::errorpb::Error {
        self.region_error.as_ref().unwrap_or_else(|| super::errorpb::Error::default_instance())
    }

    fn get_region_error_for_reflect(&self) -> &::protobuf::SingularPtrField<super::errorpb::Error> {
        &self.region_error
    }

    fn mut_region_error_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<super::errorpb::Error> {
        &mut self.region_error
    }

    // .kvrpcpb.LockInfo locked = 3;

    pub fn clear_locked(&mut self) {
        self.locked.clear();
    }

    pub fn has_locked(&self) -> bool {
        self.locked.is_some()
    }

    // Param is passed by value, moved
    pub fn set_locked(&mut self, v: super::kvrpcpb::LockInfo) {
        self.locked = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_locked(&mut self) -> &mut super::kvrpcpb::LockInfo {
        if self.locked.is_none() {
            self.locked.set_default();
        }
        self.locked.as_mut().unwrap()
    }

    // Take field
    pub fn take_locked(&mut self) -> super::kvrpcpb::LockInfo {
        self.locked.take().unwrap_or_else(|| super::kvrpcpb::LockInfo::new())
    }

    pub fn get_locked(&self) -> &super::kvrpcpb::LockInfo {
        self.locked.as_ref().unwrap_or_else(|| super::kvrpcpb::LockInfo::default_instance())
    }

    fn get_locked_for_reflect(&self) -> &::protobuf::SingularPtrField<super::kvrpcpb::LockInfo> {
        &self.locked
    }

    fn mut_locked_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<super::kvrpcpb::LockInfo> {
        &mut self.locked
    }

    // string other_error = 4;

    pub fn clear_other_error(&mut self) {
        self.other_error.clear();
    }

    // Param is passed by value, moved
    pub fn set_other_error(&mut self, v: ::std::string::String) {
        self.other_error = v;
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_other_error(&mut self) -> &mut ::std::string::String {
        &mut self.other_error
    }

    // Take field
    pub fn take_other_error(&mut self) -> ::std::string::String {
        ::std::mem::replace(&mut self.other_error, ::std::string::String::new())
    }

    pub fn get_other_error(&self) -> &str {
        &self.other_error
    }

    fn get_other_error_for_reflect(&self) -> &::std::string::String {
        &self.other_error
    }

    fn mut_other_error_for_reflect(&mut self) -> &mut ::std::string::String {
        &mut self.other_error
    }

    // .coprocessor.KeyRange range = 5;

    pub fn clear_range(&mut self) {
        self.range.clear();
    }

    pub fn has_range(&self) -> bool {
        self.range.is_some()
    }

    // Param is passed by value, moved
    pub fn set_range(&mut self, v: KeyRange) {
        self.range = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_range(&mut self) -> &mut KeyRange {
        if self.range.is_none() {
            self.range.set_default();
        }
        self.range.as_mut().unwrap()
    }

    // Take field
    pub fn take_range(&mut self) -> KeyRange {
        self.range.take().unwrap_or_else(|| KeyRange::new())
    }

    pub fn get_range(&self) -> &KeyRange {
        self.range.as_ref().unwrap_or_else(|| KeyRange::default_instance())
    }

    fn get_range_for_reflect(&self) -> &::protobuf::SingularPtrField<KeyRange> {
        &self.range
    }

    fn mut_range_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<KeyRange> {
        &mut self.range
    }
}

impl ::protobuf::Message for Response {
    fn is_initialized(&self) -> bool {
        for v in &self.region_error {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.locked {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.range {
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
                    ::protobuf::rt::read_singular_proto3_bytes_into(wire_type, is, &mut self.data)?;
                },
                2 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.region_error)?;
                },
                3 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.locked)?;
                },
                4 => {
                    ::protobuf::rt::read_singular_proto3_string_into(wire_type, is, &mut self.other_error)?;
                },
                5 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.range)?;
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
        if !self.data.is_empty() {
            my_size += ::protobuf::rt::bytes_size(1, &self.data);
        }
        if let Some(ref v) = self.region_error.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if let Some(ref v) = self.locked.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if !self.other_error.is_empty() {
            my_size += ::protobuf::rt::string_size(4, &self.other_error);
        }
        if let Some(ref v) = self.range.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if !self.data.is_empty() {
            os.write_bytes(1, &self.data)?;
        }
        if let Some(ref v) = self.region_error.as_ref() {
            os.write_tag(2, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if let Some(ref v) = self.locked.as_ref() {
            os.write_tag(3, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if !self.other_error.is_empty() {
            os.write_string(4, &self.other_error)?;
        }
        if let Some(ref v) = self.range.as_ref() {
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

impl ::protobuf::MessageStatic for Response {
    fn new() -> Response {
        Response::new()
    }

    fn descriptor_static(_: ::std::option::Option<Response>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeBytes>(
                    "data",
                    Response::get_data_for_reflect,
                    Response::mut_data_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<super::errorpb::Error>>(
                    "region_error",
                    Response::get_region_error_for_reflect,
                    Response::mut_region_error_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<super::kvrpcpb::LockInfo>>(
                    "locked",
                    Response::get_locked_for_reflect,
                    Response::mut_locked_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeString>(
                    "other_error",
                    Response::get_other_error_for_reflect,
                    Response::mut_other_error_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<KeyRange>>(
                    "range",
                    Response::get_range_for_reflect,
                    Response::mut_range_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<Response>(
                    "Response",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for Response {
    fn clear(&mut self) {
        self.clear_data();
        self.clear_region_error();
        self.clear_locked();
        self.clear_other_error();
        self.clear_range();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for Response {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for Response {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

static file_descriptor_proto_data: &'static [u8] = b"\
    \n\x11coprocessor.proto\x12\x0bcoprocessor\x1a\rerrorpb.proto\x1a\rkvrpc\
    pb.proto\x1a\x14gogoproto/gogo.proto\"2\n\x08KeyRange\x12\x14\n\x05start\
    \x18\x01\x20\x01(\x0cR\x05start\x12\x10\n\x03end\x18\x02\x20\x01(\x0cR\
    \x03end\"\x88\x01\n\x07Request\x12*\n\x07context\x18\x01\x20\x01(\x0b2\
    \x10.kvrpcpb.ContextR\x07context\x12\x0e\n\x02tp\x18\x02\x20\x01(\x03R\
    \x02tp\x12\x12\n\x04data\x18\x03\x20\x01(\x0cR\x04data\x12-\n\x06ranges\
    \x18\x04\x20\x03(\x0b2\x15.coprocessor.KeyRangeR\x06ranges\"\x83\x02\n\
    \x08Response\x12K\n\x04data\x18\x01\x20\x01(\x0cR\x04dataB7\xc8\xde\x1f\
    \0\xda\xde\x1f/github.com/pingcap/tipb/sharedbytes.SharedBytes\x121\n\
    \x0cregion_error\x18\x02\x20\x01(\x0b2\x0e.errorpb.ErrorR\x0bregionError\
    \x12)\n\x06locked\x18\x03\x20\x01(\x0b2\x11.kvrpcpb.LockInfoR\x06locked\
    \x12\x1f\n\x0bother_error\x18\x04\x20\x01(\tR\notherError\x12+\n\x05rang\
    e\x18\x05\x20\x01(\x0b2\x15.coprocessor.KeyRangeR\x05rangeB&\n\x18com.pi\
    ngcap.tikv.kvproto\xc8\xe2\x1e\x01\xe0\xe2\x1e\x01\xd0\xe2\x1e\x01J\xa4\
    \x0b\n\x06\x12\x04\0\0\x20\x01\n\x08\n\x01\x0c\x12\x03\0\0\x12\n\x08\n\
    \x01\x02\x12\x03\x01\x08\x13\n\t\n\x02\x03\0\x12\x03\x03\x07\x16\n\t\n\
    \x02\x03\x01\x12\x03\x04\x07\x16\n\t\n\x02\x03\x02\x12\x03\x05\x07\x1d\n\
    \x08\n\x01\x08\x12\x03\x07\0(\n\x0b\n\x04\x08\xe7\x07\0\x12\x03\x07\0(\n\
    \x0c\n\x05\x08\xe7\x07\0\x02\x12\x03\x07\x07\x20\n\r\n\x06\x08\xe7\x07\0\
    \x02\0\x12\x03\x07\x07\x20\n\x0e\n\x07\x08\xe7\x07\0\x02\0\x01\x12\x03\
    \x07\x08\x1f\n\x0c\n\x05\x08\xe7\x07\0\x03\x12\x03\x07#'\n\x08\n\x01\x08\
    \x12\x03\x08\0$\n\x0b\n\x04\x08\xe7\x07\x01\x12\x03\x08\0$\n\x0c\n\x05\
    \x08\xe7\x07\x01\x02\x12\x03\x08\x07\x1c\n\r\n\x06\x08\xe7\x07\x01\x02\0\
    \x12\x03\x08\x07\x1c\n\x0e\n\x07\x08\xe7\x07\x01\x02\0\x01\x12\x03\x08\
    \x08\x1b\n\x0c\n\x05\x08\xe7\x07\x01\x03\x12\x03\x08\x1f#\n\x08\n\x01\
    \x08\x12\x03\t\0*\n\x0b\n\x04\x08\xe7\x07\x02\x12\x03\t\0*\n\x0c\n\x05\
    \x08\xe7\x07\x02\x02\x12\x03\t\x07\"\n\r\n\x06\x08\xe7\x07\x02\x02\0\x12\
    \x03\t\x07\"\n\x0e\n\x07\x08\xe7\x07\x02\x02\0\x01\x12\x03\t\x08!\n\x0c\
    \n\x05\x08\xe7\x07\x02\x03\x12\x03\t%)\n\x08\n\x01\x08\x12\x03\x0b\01\n\
    \x0b\n\x04\x08\xe7\x07\x03\x12\x03\x0b\01\n\x0c\n\x05\x08\xe7\x07\x03\
    \x02\x12\x03\x0b\x07\x13\n\r\n\x06\x08\xe7\x07\x03\x02\0\x12\x03\x0b\x07\
    \x13\n\x0e\n\x07\x08\xe7\x07\x03\x02\0\x01\x12\x03\x0b\x07\x13\n\x0c\n\
    \x05\x08\xe7\x07\x03\x07\x12\x03\x0b\x160\n\x1a\n\x02\x04\0\x12\x04\x0e\
    \0\x11\x01\x1a\x0e\x20[start,\x20end)\n\n\n\n\x03\x04\0\x01\x12\x03\x0e\
    \x08\x10\n\x0b\n\x04\x04\0\x02\0\x12\x03\x0f\x04\x14\n\r\n\x05\x04\0\x02\
    \0\x04\x12\x04\x0f\x04\x0e\x12\n\x0c\n\x05\x04\0\x02\0\x05\x12\x03\x0f\
    \x04\t\n\x0c\n\x05\x04\0\x02\0\x01\x12\x03\x0f\n\x0f\n\x0c\n\x05\x04\0\
    \x02\0\x03\x12\x03\x0f\x12\x13\n\x0b\n\x04\x04\0\x02\x01\x12\x03\x10\x04\
    \x12\n\r\n\x05\x04\0\x02\x01\x04\x12\x04\x10\x04\x0f\x14\n\x0c\n\x05\x04\
    \0\x02\x01\x05\x12\x03\x10\x04\t\n\x0c\n\x05\x04\0\x02\x01\x01\x12\x03\
    \x10\n\r\n\x0c\n\x05\x04\0\x02\x01\x03\x12\x03\x10\x10\x11\n\n\n\x02\x04\
    \x01\x12\x04\x13\0\x18\x01\n\n\n\x03\x04\x01\x01\x12\x03\x13\x08\x0f\n\
    \x0b\n\x04\x04\x01\x02\0\x12\x03\x14\x04\x20\n\r\n\x05\x04\x01\x02\0\x04\
    \x12\x04\x14\x04\x13\x11\n\x0c\n\x05\x04\x01\x02\0\x06\x12\x03\x14\x04\
    \x13\n\x0c\n\x05\x04\x01\x02\0\x01\x12\x03\x14\x14\x1b\n\x0c\n\x05\x04\
    \x01\x02\0\x03\x12\x03\x14\x1e\x1f\n\x0b\n\x04\x04\x01\x02\x01\x12\x03\
    \x15\x04\x11\n\r\n\x05\x04\x01\x02\x01\x04\x12\x04\x15\x04\x14\x20\n\x0c\
    \n\x05\x04\x01\x02\x01\x05\x12\x03\x15\x04\t\n\x0c\n\x05\x04\x01\x02\x01\
    \x01\x12\x03\x15\n\x0c\n\x0c\n\x05\x04\x01\x02\x01\x03\x12\x03\x15\x0f\
    \x10\n\x0b\n\x04\x04\x01\x02\x02\x12\x03\x16\x04\x13\n\r\n\x05\x04\x01\
    \x02\x02\x04\x12\x04\x16\x04\x15\x11\n\x0c\n\x05\x04\x01\x02\x02\x05\x12\
    \x03\x16\x04\t\n\x0c\n\x05\x04\x01\x02\x02\x01\x12\x03\x16\n\x0e\n\x0c\n\
    \x05\x04\x01\x02\x02\x03\x12\x03\x16\x11\x12\n\x0b\n\x04\x04\x01\x02\x03\
    \x12\x03\x17\x04!\n\x0c\n\x05\x04\x01\x02\x03\x04\x12\x03\x17\x04\x0c\n\
    \x0c\n\x05\x04\x01\x02\x03\x06\x12\x03\x17\r\x15\n\x0c\n\x05\x04\x01\x02\
    \x03\x01\x12\x03\x17\x16\x1c\n\x0c\n\x05\x04\x01\x02\x03\x03\x12\x03\x17\
    \x1f\x20\n\n\n\x02\x04\x02\x12\x04\x1a\0\x20\x01\n\n\n\x03\x04\x02\x01\
    \x12\x03\x1a\x08\x10\n\x0b\n\x04\x04\x02\x02\0\x12\x03\x1b\x04~\n\r\n\
    \x05\x04\x02\x02\0\x04\x12\x04\x1b\x04\x1a\x12\n\x0c\n\x05\x04\x02\x02\0\
    \x05\x12\x03\x1b\x04\t\n\x0c\n\x05\x04\x02\x02\0\x01\x12\x03\x1b\n\x0e\n\
    \x0c\n\x05\x04\x02\x02\0\x03\x12\x03\x1b\x11\x12\n\x0c\n\x05\x04\x02\x02\
    \0\x08\x12\x03\x1b\x13}\n\x0f\n\x08\x04\x02\x02\0\x08\xe7\x07\0\x12\x03\
    \x1b\x14^\n\x10\n\t\x04\x02\x02\0\x08\xe7\x07\0\x02\x12\x03\x1b\x14*\n\
    \x11\n\n\x04\x02\x02\0\x08\xe7\x07\0\x02\0\x12\x03\x1b\x14*\n\x12\n\x0b\
    \x04\x02\x02\0\x08\xe7\x07\0\x02\0\x01\x12\x03\x1b\x15)\n\x10\n\t\x04\
    \x02\x02\0\x08\xe7\x07\0\x07\x12\x03\x1b-^\n\x0f\n\x08\x04\x02\x02\0\x08\
    \xe7\x07\x01\x12\x03\x1b`|\n\x10\n\t\x04\x02\x02\0\x08\xe7\x07\x01\x02\
    \x12\x03\x1b`t\n\x11\n\n\x04\x02\x02\0\x08\xe7\x07\x01\x02\0\x12\x03\x1b\
    `t\n\x12\n\x0b\x04\x02\x02\0\x08\xe7\x07\x01\x02\0\x01\x12\x03\x1bas\n\
    \x10\n\t\x04\x02\x02\0\x08\xe7\x07\x01\x03\x12\x03\x1bw|\n\x0b\n\x04\x04\
    \x02\x02\x01\x12\x03\x1c\x04#\n\r\n\x05\x04\x02\x02\x01\x04\x12\x04\x1c\
    \x04\x1b~\n\x0c\n\x05\x04\x02\x02\x01\x06\x12\x03\x1c\x04\x11\n\x0c\n\
    \x05\x04\x02\x02\x01\x01\x12\x03\x1c\x12\x1e\n\x0c\n\x05\x04\x02\x02\x01\
    \x03\x12\x03\x1c!\"\n\x0b\n\x04\x04\x02\x02\x02\x12\x03\x1d\x04\x20\n\r\
    \n\x05\x04\x02\x02\x02\x04\x12\x04\x1d\x04\x1c#\n\x0c\n\x05\x04\x02\x02\
    \x02\x06\x12\x03\x1d\x04\x14\n\x0c\n\x05\x04\x02\x02\x02\x01\x12\x03\x1d\
    \x15\x1b\n\x0c\n\x05\x04\x02\x02\x02\x03\x12\x03\x1d\x1e\x1f\n\x0b\n\x04\
    \x04\x02\x02\x03\x12\x03\x1e\x04\x1b\n\r\n\x05\x04\x02\x02\x03\x04\x12\
    \x04\x1e\x04\x1d\x20\n\x0c\n\x05\x04\x02\x02\x03\x05\x12\x03\x1e\x04\n\n\
    \x0c\n\x05\x04\x02\x02\x03\x01\x12\x03\x1e\x0b\x16\n\x0c\n\x05\x04\x02\
    \x02\x03\x03\x12\x03\x1e\x19\x1a\n\x0b\n\x04\x04\x02\x02\x04\x12\x03\x1f\
    \x04\x17\n\r\n\x05\x04\x02\x02\x04\x04\x12\x04\x1f\x04\x1e\x1b\n\x0c\n\
    \x05\x04\x02\x02\x04\x06\x12\x03\x1f\x04\x0c\n\x0c\n\x05\x04\x02\x02\x04\
    \x01\x12\x03\x1f\r\x12\n\x0c\n\x05\x04\x02\x02\x04\x03\x12\x03\x1f\x15\
    \x16b\x06proto3\
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
