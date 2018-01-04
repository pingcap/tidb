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
pub struct RequestHeader {
    // message fields
    pub cluster_id: u64,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for RequestHeader {}

impl RequestHeader {
    pub fn new() -> RequestHeader {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static RequestHeader {
        static mut instance: ::protobuf::lazy::Lazy<RequestHeader> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const RequestHeader,
        };
        unsafe {
            instance.get(RequestHeader::new)
        }
    }

    // uint64 cluster_id = 1;

    pub fn clear_cluster_id(&mut self) {
        self.cluster_id = 0;
    }

    // Param is passed by value, moved
    pub fn set_cluster_id(&mut self, v: u64) {
        self.cluster_id = v;
    }

    pub fn get_cluster_id(&self) -> u64 {
        self.cluster_id
    }

    fn get_cluster_id_for_reflect(&self) -> &u64 {
        &self.cluster_id
    }

    fn mut_cluster_id_for_reflect(&mut self) -> &mut u64 {
        &mut self.cluster_id
    }
}

impl ::protobuf::Message for RequestHeader {
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
                    self.cluster_id = tmp;
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
        if self.cluster_id != 0 {
            my_size += ::protobuf::rt::value_size(1, self.cluster_id, ::protobuf::wire_format::WireTypeVarint);
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if self.cluster_id != 0 {
            os.write_uint64(1, self.cluster_id)?;
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

impl ::protobuf::MessageStatic for RequestHeader {
    fn new() -> RequestHeader {
        RequestHeader::new()
    }

    fn descriptor_static(_: ::std::option::Option<RequestHeader>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "cluster_id",
                    RequestHeader::get_cluster_id_for_reflect,
                    RequestHeader::mut_cluster_id_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<RequestHeader>(
                    "RequestHeader",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for RequestHeader {
    fn clear(&mut self) {
        self.clear_cluster_id();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for RequestHeader {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for RequestHeader {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct ResponseHeader {
    // message fields
    pub cluster_id: u64,
    pub error: ::protobuf::SingularPtrField<Error>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for ResponseHeader {}

impl ResponseHeader {
    pub fn new() -> ResponseHeader {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static ResponseHeader {
        static mut instance: ::protobuf::lazy::Lazy<ResponseHeader> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ResponseHeader,
        };
        unsafe {
            instance.get(ResponseHeader::new)
        }
    }

    // uint64 cluster_id = 1;

    pub fn clear_cluster_id(&mut self) {
        self.cluster_id = 0;
    }

    // Param is passed by value, moved
    pub fn set_cluster_id(&mut self, v: u64) {
        self.cluster_id = v;
    }

    pub fn get_cluster_id(&self) -> u64 {
        self.cluster_id
    }

    fn get_cluster_id_for_reflect(&self) -> &u64 {
        &self.cluster_id
    }

    fn mut_cluster_id_for_reflect(&mut self) -> &mut u64 {
        &mut self.cluster_id
    }

    // .pdpb.Error error = 2;

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
}

impl ::protobuf::Message for ResponseHeader {
    fn is_initialized(&self) -> bool {
        for v in &self.error {
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
                    self.cluster_id = tmp;
                },
                2 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.error)?;
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
        if self.cluster_id != 0 {
            my_size += ::protobuf::rt::value_size(1, self.cluster_id, ::protobuf::wire_format::WireTypeVarint);
        }
        if let Some(ref v) = self.error.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if self.cluster_id != 0 {
            os.write_uint64(1, self.cluster_id)?;
        }
        if let Some(ref v) = self.error.as_ref() {
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

impl ::protobuf::MessageStatic for ResponseHeader {
    fn new() -> ResponseHeader {
        ResponseHeader::new()
    }

    fn descriptor_static(_: ::std::option::Option<ResponseHeader>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "cluster_id",
                    ResponseHeader::get_cluster_id_for_reflect,
                    ResponseHeader::mut_cluster_id_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<Error>>(
                    "error",
                    ResponseHeader::get_error_for_reflect,
                    ResponseHeader::mut_error_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<ResponseHeader>(
                    "ResponseHeader",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for ResponseHeader {
    fn clear(&mut self) {
        self.clear_cluster_id();
        self.clear_error();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for ResponseHeader {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for ResponseHeader {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct Error {
    // message fields
    pub field_type: ErrorType,
    pub message: ::std::string::String,
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

    // .pdpb.ErrorType type = 1;

    pub fn clear_field_type(&mut self) {
        self.field_type = ErrorType::OK;
    }

    // Param is passed by value, moved
    pub fn set_field_type(&mut self, v: ErrorType) {
        self.field_type = v;
    }

    pub fn get_field_type(&self) -> ErrorType {
        self.field_type
    }

    fn get_field_type_for_reflect(&self) -> &ErrorType {
        &self.field_type
    }

    fn mut_field_type_for_reflect(&mut self) -> &mut ErrorType {
        &mut self.field_type
    }

    // string message = 2;

    pub fn clear_message(&mut self) {
        self.message.clear();
    }

    // Param is passed by value, moved
    pub fn set_message(&mut self, v: ::std::string::String) {
        self.message = v;
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_message(&mut self) -> &mut ::std::string::String {
        &mut self.message
    }

    // Take field
    pub fn take_message(&mut self) -> ::std::string::String {
        ::std::mem::replace(&mut self.message, ::std::string::String::new())
    }

    pub fn get_message(&self) -> &str {
        &self.message
    }

    fn get_message_for_reflect(&self) -> &::std::string::String {
        &self.message
    }

    fn mut_message_for_reflect(&mut self) -> &mut ::std::string::String {
        &mut self.message
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
                    let tmp = is.read_enum()?;
                    self.field_type = tmp;
                },
                2 => {
                    ::protobuf::rt::read_singular_proto3_string_into(wire_type, is, &mut self.message)?;
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
        if self.field_type != ErrorType::OK {
            my_size += ::protobuf::rt::enum_size(1, self.field_type);
        }
        if !self.message.is_empty() {
            my_size += ::protobuf::rt::string_size(2, &self.message);
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if self.field_type != ErrorType::OK {
            os.write_enum(1, self.field_type.value())?;
        }
        if !self.message.is_empty() {
            os.write_string(2, &self.message)?;
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
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeEnum<ErrorType>>(
                    "type",
                    Error::get_field_type_for_reflect,
                    Error::mut_field_type_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeString>(
                    "message",
                    Error::get_message_for_reflect,
                    Error::mut_message_for_reflect,
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
        self.clear_field_type();
        self.clear_message();
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
pub struct TsoRequest {
    // message fields
    pub header: ::protobuf::SingularPtrField<RequestHeader>,
    pub count: u32,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for TsoRequest {}

impl TsoRequest {
    pub fn new() -> TsoRequest {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static TsoRequest {
        static mut instance: ::protobuf::lazy::Lazy<TsoRequest> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const TsoRequest,
        };
        unsafe {
            instance.get(TsoRequest::new)
        }
    }

    // .pdpb.RequestHeader header = 1;

    pub fn clear_header(&mut self) {
        self.header.clear();
    }

    pub fn has_header(&self) -> bool {
        self.header.is_some()
    }

    // Param is passed by value, moved
    pub fn set_header(&mut self, v: RequestHeader) {
        self.header = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_header(&mut self) -> &mut RequestHeader {
        if self.header.is_none() {
            self.header.set_default();
        }
        self.header.as_mut().unwrap()
    }

    // Take field
    pub fn take_header(&mut self) -> RequestHeader {
        self.header.take().unwrap_or_else(|| RequestHeader::new())
    }

    pub fn get_header(&self) -> &RequestHeader {
        self.header.as_ref().unwrap_or_else(|| RequestHeader::default_instance())
    }

    fn get_header_for_reflect(&self) -> &::protobuf::SingularPtrField<RequestHeader> {
        &self.header
    }

    fn mut_header_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<RequestHeader> {
        &mut self.header
    }

    // uint32 count = 2;

    pub fn clear_count(&mut self) {
        self.count = 0;
    }

    // Param is passed by value, moved
    pub fn set_count(&mut self, v: u32) {
        self.count = v;
    }

    pub fn get_count(&self) -> u32 {
        self.count
    }

    fn get_count_for_reflect(&self) -> &u32 {
        &self.count
    }

    fn mut_count_for_reflect(&mut self) -> &mut u32 {
        &mut self.count
    }
}

impl ::protobuf::Message for TsoRequest {
    fn is_initialized(&self) -> bool {
        for v in &self.header {
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
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.header)?;
                },
                2 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_uint32()?;
                    self.count = tmp;
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
        if let Some(ref v) = self.header.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if self.count != 0 {
            my_size += ::protobuf::rt::value_size(2, self.count, ::protobuf::wire_format::WireTypeVarint);
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(ref v) = self.header.as_ref() {
            os.write_tag(1, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if self.count != 0 {
            os.write_uint32(2, self.count)?;
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

impl ::protobuf::MessageStatic for TsoRequest {
    fn new() -> TsoRequest {
        TsoRequest::new()
    }

    fn descriptor_static(_: ::std::option::Option<TsoRequest>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<RequestHeader>>(
                    "header",
                    TsoRequest::get_header_for_reflect,
                    TsoRequest::mut_header_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeUint32>(
                    "count",
                    TsoRequest::get_count_for_reflect,
                    TsoRequest::mut_count_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<TsoRequest>(
                    "TsoRequest",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for TsoRequest {
    fn clear(&mut self) {
        self.clear_header();
        self.clear_count();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for TsoRequest {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for TsoRequest {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct Timestamp {
    // message fields
    pub physical: i64,
    pub logical: i64,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for Timestamp {}

impl Timestamp {
    pub fn new() -> Timestamp {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static Timestamp {
        static mut instance: ::protobuf::lazy::Lazy<Timestamp> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const Timestamp,
        };
        unsafe {
            instance.get(Timestamp::new)
        }
    }

    // int64 physical = 1;

    pub fn clear_physical(&mut self) {
        self.physical = 0;
    }

    // Param is passed by value, moved
    pub fn set_physical(&mut self, v: i64) {
        self.physical = v;
    }

    pub fn get_physical(&self) -> i64 {
        self.physical
    }

    fn get_physical_for_reflect(&self) -> &i64 {
        &self.physical
    }

    fn mut_physical_for_reflect(&mut self) -> &mut i64 {
        &mut self.physical
    }

    // int64 logical = 2;

    pub fn clear_logical(&mut self) {
        self.logical = 0;
    }

    // Param is passed by value, moved
    pub fn set_logical(&mut self, v: i64) {
        self.logical = v;
    }

    pub fn get_logical(&self) -> i64 {
        self.logical
    }

    fn get_logical_for_reflect(&self) -> &i64 {
        &self.logical
    }

    fn mut_logical_for_reflect(&mut self) -> &mut i64 {
        &mut self.logical
    }
}

impl ::protobuf::Message for Timestamp {
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
                    self.physical = tmp;
                },
                2 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_int64()?;
                    self.logical = tmp;
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
        if self.physical != 0 {
            my_size += ::protobuf::rt::value_size(1, self.physical, ::protobuf::wire_format::WireTypeVarint);
        }
        if self.logical != 0 {
            my_size += ::protobuf::rt::value_size(2, self.logical, ::protobuf::wire_format::WireTypeVarint);
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if self.physical != 0 {
            os.write_int64(1, self.physical)?;
        }
        if self.logical != 0 {
            os.write_int64(2, self.logical)?;
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

impl ::protobuf::MessageStatic for Timestamp {
    fn new() -> Timestamp {
        Timestamp::new()
    }

    fn descriptor_static(_: ::std::option::Option<Timestamp>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeInt64>(
                    "physical",
                    Timestamp::get_physical_for_reflect,
                    Timestamp::mut_physical_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeInt64>(
                    "logical",
                    Timestamp::get_logical_for_reflect,
                    Timestamp::mut_logical_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<Timestamp>(
                    "Timestamp",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for Timestamp {
    fn clear(&mut self) {
        self.clear_physical();
        self.clear_logical();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for Timestamp {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for Timestamp {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct TsoResponse {
    // message fields
    pub header: ::protobuf::SingularPtrField<ResponseHeader>,
    pub count: u32,
    pub timestamp: ::protobuf::SingularPtrField<Timestamp>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for TsoResponse {}

impl TsoResponse {
    pub fn new() -> TsoResponse {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static TsoResponse {
        static mut instance: ::protobuf::lazy::Lazy<TsoResponse> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const TsoResponse,
        };
        unsafe {
            instance.get(TsoResponse::new)
        }
    }

    // .pdpb.ResponseHeader header = 1;

    pub fn clear_header(&mut self) {
        self.header.clear();
    }

    pub fn has_header(&self) -> bool {
        self.header.is_some()
    }

    // Param is passed by value, moved
    pub fn set_header(&mut self, v: ResponseHeader) {
        self.header = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_header(&mut self) -> &mut ResponseHeader {
        if self.header.is_none() {
            self.header.set_default();
        }
        self.header.as_mut().unwrap()
    }

    // Take field
    pub fn take_header(&mut self) -> ResponseHeader {
        self.header.take().unwrap_or_else(|| ResponseHeader::new())
    }

    pub fn get_header(&self) -> &ResponseHeader {
        self.header.as_ref().unwrap_or_else(|| ResponseHeader::default_instance())
    }

    fn get_header_for_reflect(&self) -> &::protobuf::SingularPtrField<ResponseHeader> {
        &self.header
    }

    fn mut_header_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<ResponseHeader> {
        &mut self.header
    }

    // uint32 count = 2;

    pub fn clear_count(&mut self) {
        self.count = 0;
    }

    // Param is passed by value, moved
    pub fn set_count(&mut self, v: u32) {
        self.count = v;
    }

    pub fn get_count(&self) -> u32 {
        self.count
    }

    fn get_count_for_reflect(&self) -> &u32 {
        &self.count
    }

    fn mut_count_for_reflect(&mut self) -> &mut u32 {
        &mut self.count
    }

    // .pdpb.Timestamp timestamp = 3;

    pub fn clear_timestamp(&mut self) {
        self.timestamp.clear();
    }

    pub fn has_timestamp(&self) -> bool {
        self.timestamp.is_some()
    }

    // Param is passed by value, moved
    pub fn set_timestamp(&mut self, v: Timestamp) {
        self.timestamp = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_timestamp(&mut self) -> &mut Timestamp {
        if self.timestamp.is_none() {
            self.timestamp.set_default();
        }
        self.timestamp.as_mut().unwrap()
    }

    // Take field
    pub fn take_timestamp(&mut self) -> Timestamp {
        self.timestamp.take().unwrap_or_else(|| Timestamp::new())
    }

    pub fn get_timestamp(&self) -> &Timestamp {
        self.timestamp.as_ref().unwrap_or_else(|| Timestamp::default_instance())
    }

    fn get_timestamp_for_reflect(&self) -> &::protobuf::SingularPtrField<Timestamp> {
        &self.timestamp
    }

    fn mut_timestamp_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<Timestamp> {
        &mut self.timestamp
    }
}

impl ::protobuf::Message for TsoResponse {
    fn is_initialized(&self) -> bool {
        for v in &self.header {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.timestamp {
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
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.header)?;
                },
                2 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_uint32()?;
                    self.count = tmp;
                },
                3 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.timestamp)?;
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
        if let Some(ref v) = self.header.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if self.count != 0 {
            my_size += ::protobuf::rt::value_size(2, self.count, ::protobuf::wire_format::WireTypeVarint);
        }
        if let Some(ref v) = self.timestamp.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(ref v) = self.header.as_ref() {
            os.write_tag(1, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if self.count != 0 {
            os.write_uint32(2, self.count)?;
        }
        if let Some(ref v) = self.timestamp.as_ref() {
            os.write_tag(3, ::protobuf::wire_format::WireTypeLengthDelimited)?;
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

impl ::protobuf::MessageStatic for TsoResponse {
    fn new() -> TsoResponse {
        TsoResponse::new()
    }

    fn descriptor_static(_: ::std::option::Option<TsoResponse>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<ResponseHeader>>(
                    "header",
                    TsoResponse::get_header_for_reflect,
                    TsoResponse::mut_header_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeUint32>(
                    "count",
                    TsoResponse::get_count_for_reflect,
                    TsoResponse::mut_count_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<Timestamp>>(
                    "timestamp",
                    TsoResponse::get_timestamp_for_reflect,
                    TsoResponse::mut_timestamp_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<TsoResponse>(
                    "TsoResponse",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for TsoResponse {
    fn clear(&mut self) {
        self.clear_header();
        self.clear_count();
        self.clear_timestamp();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for TsoResponse {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for TsoResponse {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct BootstrapRequest {
    // message fields
    pub header: ::protobuf::SingularPtrField<RequestHeader>,
    pub store: ::protobuf::SingularPtrField<super::metapb::Store>,
    pub region: ::protobuf::SingularPtrField<super::metapb::Region>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for BootstrapRequest {}

impl BootstrapRequest {
    pub fn new() -> BootstrapRequest {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static BootstrapRequest {
        static mut instance: ::protobuf::lazy::Lazy<BootstrapRequest> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const BootstrapRequest,
        };
        unsafe {
            instance.get(BootstrapRequest::new)
        }
    }

    // .pdpb.RequestHeader header = 1;

    pub fn clear_header(&mut self) {
        self.header.clear();
    }

    pub fn has_header(&self) -> bool {
        self.header.is_some()
    }

    // Param is passed by value, moved
    pub fn set_header(&mut self, v: RequestHeader) {
        self.header = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_header(&mut self) -> &mut RequestHeader {
        if self.header.is_none() {
            self.header.set_default();
        }
        self.header.as_mut().unwrap()
    }

    // Take field
    pub fn take_header(&mut self) -> RequestHeader {
        self.header.take().unwrap_or_else(|| RequestHeader::new())
    }

    pub fn get_header(&self) -> &RequestHeader {
        self.header.as_ref().unwrap_or_else(|| RequestHeader::default_instance())
    }

    fn get_header_for_reflect(&self) -> &::protobuf::SingularPtrField<RequestHeader> {
        &self.header
    }

    fn mut_header_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<RequestHeader> {
        &mut self.header
    }

    // .metapb.Store store = 2;

    pub fn clear_store(&mut self) {
        self.store.clear();
    }

    pub fn has_store(&self) -> bool {
        self.store.is_some()
    }

    // Param is passed by value, moved
    pub fn set_store(&mut self, v: super::metapb::Store) {
        self.store = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_store(&mut self) -> &mut super::metapb::Store {
        if self.store.is_none() {
            self.store.set_default();
        }
        self.store.as_mut().unwrap()
    }

    // Take field
    pub fn take_store(&mut self) -> super::metapb::Store {
        self.store.take().unwrap_or_else(|| super::metapb::Store::new())
    }

    pub fn get_store(&self) -> &super::metapb::Store {
        self.store.as_ref().unwrap_or_else(|| super::metapb::Store::default_instance())
    }

    fn get_store_for_reflect(&self) -> &::protobuf::SingularPtrField<super::metapb::Store> {
        &self.store
    }

    fn mut_store_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<super::metapb::Store> {
        &mut self.store
    }

    // .metapb.Region region = 3;

    pub fn clear_region(&mut self) {
        self.region.clear();
    }

    pub fn has_region(&self) -> bool {
        self.region.is_some()
    }

    // Param is passed by value, moved
    pub fn set_region(&mut self, v: super::metapb::Region) {
        self.region = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_region(&mut self) -> &mut super::metapb::Region {
        if self.region.is_none() {
            self.region.set_default();
        }
        self.region.as_mut().unwrap()
    }

    // Take field
    pub fn take_region(&mut self) -> super::metapb::Region {
        self.region.take().unwrap_or_else(|| super::metapb::Region::new())
    }

    pub fn get_region(&self) -> &super::metapb::Region {
        self.region.as_ref().unwrap_or_else(|| super::metapb::Region::default_instance())
    }

    fn get_region_for_reflect(&self) -> &::protobuf::SingularPtrField<super::metapb::Region> {
        &self.region
    }

    fn mut_region_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<super::metapb::Region> {
        &mut self.region
    }
}

impl ::protobuf::Message for BootstrapRequest {
    fn is_initialized(&self) -> bool {
        for v in &self.header {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.store {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.region {
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
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.header)?;
                },
                2 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.store)?;
                },
                3 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.region)?;
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
        if let Some(ref v) = self.header.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if let Some(ref v) = self.store.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if let Some(ref v) = self.region.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(ref v) = self.header.as_ref() {
            os.write_tag(1, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if let Some(ref v) = self.store.as_ref() {
            os.write_tag(2, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if let Some(ref v) = self.region.as_ref() {
            os.write_tag(3, ::protobuf::wire_format::WireTypeLengthDelimited)?;
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

impl ::protobuf::MessageStatic for BootstrapRequest {
    fn new() -> BootstrapRequest {
        BootstrapRequest::new()
    }

    fn descriptor_static(_: ::std::option::Option<BootstrapRequest>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<RequestHeader>>(
                    "header",
                    BootstrapRequest::get_header_for_reflect,
                    BootstrapRequest::mut_header_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<super::metapb::Store>>(
                    "store",
                    BootstrapRequest::get_store_for_reflect,
                    BootstrapRequest::mut_store_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<super::metapb::Region>>(
                    "region",
                    BootstrapRequest::get_region_for_reflect,
                    BootstrapRequest::mut_region_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<BootstrapRequest>(
                    "BootstrapRequest",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for BootstrapRequest {
    fn clear(&mut self) {
        self.clear_header();
        self.clear_store();
        self.clear_region();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for BootstrapRequest {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for BootstrapRequest {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct BootstrapResponse {
    // message fields
    pub header: ::protobuf::SingularPtrField<ResponseHeader>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for BootstrapResponse {}

impl BootstrapResponse {
    pub fn new() -> BootstrapResponse {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static BootstrapResponse {
        static mut instance: ::protobuf::lazy::Lazy<BootstrapResponse> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const BootstrapResponse,
        };
        unsafe {
            instance.get(BootstrapResponse::new)
        }
    }

    // .pdpb.ResponseHeader header = 1;

    pub fn clear_header(&mut self) {
        self.header.clear();
    }

    pub fn has_header(&self) -> bool {
        self.header.is_some()
    }

    // Param is passed by value, moved
    pub fn set_header(&mut self, v: ResponseHeader) {
        self.header = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_header(&mut self) -> &mut ResponseHeader {
        if self.header.is_none() {
            self.header.set_default();
        }
        self.header.as_mut().unwrap()
    }

    // Take field
    pub fn take_header(&mut self) -> ResponseHeader {
        self.header.take().unwrap_or_else(|| ResponseHeader::new())
    }

    pub fn get_header(&self) -> &ResponseHeader {
        self.header.as_ref().unwrap_or_else(|| ResponseHeader::default_instance())
    }

    fn get_header_for_reflect(&self) -> &::protobuf::SingularPtrField<ResponseHeader> {
        &self.header
    }

    fn mut_header_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<ResponseHeader> {
        &mut self.header
    }
}

impl ::protobuf::Message for BootstrapResponse {
    fn is_initialized(&self) -> bool {
        for v in &self.header {
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
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.header)?;
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
        if let Some(ref v) = self.header.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(ref v) = self.header.as_ref() {
            os.write_tag(1, ::protobuf::wire_format::WireTypeLengthDelimited)?;
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

impl ::protobuf::MessageStatic for BootstrapResponse {
    fn new() -> BootstrapResponse {
        BootstrapResponse::new()
    }

    fn descriptor_static(_: ::std::option::Option<BootstrapResponse>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<ResponseHeader>>(
                    "header",
                    BootstrapResponse::get_header_for_reflect,
                    BootstrapResponse::mut_header_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<BootstrapResponse>(
                    "BootstrapResponse",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for BootstrapResponse {
    fn clear(&mut self) {
        self.clear_header();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for BootstrapResponse {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for BootstrapResponse {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct IsBootstrappedRequest {
    // message fields
    pub header: ::protobuf::SingularPtrField<RequestHeader>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for IsBootstrappedRequest {}

impl IsBootstrappedRequest {
    pub fn new() -> IsBootstrappedRequest {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static IsBootstrappedRequest {
        static mut instance: ::protobuf::lazy::Lazy<IsBootstrappedRequest> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const IsBootstrappedRequest,
        };
        unsafe {
            instance.get(IsBootstrappedRequest::new)
        }
    }

    // .pdpb.RequestHeader header = 1;

    pub fn clear_header(&mut self) {
        self.header.clear();
    }

    pub fn has_header(&self) -> bool {
        self.header.is_some()
    }

    // Param is passed by value, moved
    pub fn set_header(&mut self, v: RequestHeader) {
        self.header = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_header(&mut self) -> &mut RequestHeader {
        if self.header.is_none() {
            self.header.set_default();
        }
        self.header.as_mut().unwrap()
    }

    // Take field
    pub fn take_header(&mut self) -> RequestHeader {
        self.header.take().unwrap_or_else(|| RequestHeader::new())
    }

    pub fn get_header(&self) -> &RequestHeader {
        self.header.as_ref().unwrap_or_else(|| RequestHeader::default_instance())
    }

    fn get_header_for_reflect(&self) -> &::protobuf::SingularPtrField<RequestHeader> {
        &self.header
    }

    fn mut_header_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<RequestHeader> {
        &mut self.header
    }
}

impl ::protobuf::Message for IsBootstrappedRequest {
    fn is_initialized(&self) -> bool {
        for v in &self.header {
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
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.header)?;
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
        if let Some(ref v) = self.header.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(ref v) = self.header.as_ref() {
            os.write_tag(1, ::protobuf::wire_format::WireTypeLengthDelimited)?;
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

impl ::protobuf::MessageStatic for IsBootstrappedRequest {
    fn new() -> IsBootstrappedRequest {
        IsBootstrappedRequest::new()
    }

    fn descriptor_static(_: ::std::option::Option<IsBootstrappedRequest>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<RequestHeader>>(
                    "header",
                    IsBootstrappedRequest::get_header_for_reflect,
                    IsBootstrappedRequest::mut_header_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<IsBootstrappedRequest>(
                    "IsBootstrappedRequest",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for IsBootstrappedRequest {
    fn clear(&mut self) {
        self.clear_header();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for IsBootstrappedRequest {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for IsBootstrappedRequest {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct IsBootstrappedResponse {
    // message fields
    pub header: ::protobuf::SingularPtrField<ResponseHeader>,
    pub bootstrapped: bool,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for IsBootstrappedResponse {}

impl IsBootstrappedResponse {
    pub fn new() -> IsBootstrappedResponse {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static IsBootstrappedResponse {
        static mut instance: ::protobuf::lazy::Lazy<IsBootstrappedResponse> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const IsBootstrappedResponse,
        };
        unsafe {
            instance.get(IsBootstrappedResponse::new)
        }
    }

    // .pdpb.ResponseHeader header = 1;

    pub fn clear_header(&mut self) {
        self.header.clear();
    }

    pub fn has_header(&self) -> bool {
        self.header.is_some()
    }

    // Param is passed by value, moved
    pub fn set_header(&mut self, v: ResponseHeader) {
        self.header = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_header(&mut self) -> &mut ResponseHeader {
        if self.header.is_none() {
            self.header.set_default();
        }
        self.header.as_mut().unwrap()
    }

    // Take field
    pub fn take_header(&mut self) -> ResponseHeader {
        self.header.take().unwrap_or_else(|| ResponseHeader::new())
    }

    pub fn get_header(&self) -> &ResponseHeader {
        self.header.as_ref().unwrap_or_else(|| ResponseHeader::default_instance())
    }

    fn get_header_for_reflect(&self) -> &::protobuf::SingularPtrField<ResponseHeader> {
        &self.header
    }

    fn mut_header_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<ResponseHeader> {
        &mut self.header
    }

    // bool bootstrapped = 2;

    pub fn clear_bootstrapped(&mut self) {
        self.bootstrapped = false;
    }

    // Param is passed by value, moved
    pub fn set_bootstrapped(&mut self, v: bool) {
        self.bootstrapped = v;
    }

    pub fn get_bootstrapped(&self) -> bool {
        self.bootstrapped
    }

    fn get_bootstrapped_for_reflect(&self) -> &bool {
        &self.bootstrapped
    }

    fn mut_bootstrapped_for_reflect(&mut self) -> &mut bool {
        &mut self.bootstrapped
    }
}

impl ::protobuf::Message for IsBootstrappedResponse {
    fn is_initialized(&self) -> bool {
        for v in &self.header {
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
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.header)?;
                },
                2 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_bool()?;
                    self.bootstrapped = tmp;
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
        if let Some(ref v) = self.header.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if self.bootstrapped != false {
            my_size += 2;
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(ref v) = self.header.as_ref() {
            os.write_tag(1, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if self.bootstrapped != false {
            os.write_bool(2, self.bootstrapped)?;
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

impl ::protobuf::MessageStatic for IsBootstrappedResponse {
    fn new() -> IsBootstrappedResponse {
        IsBootstrappedResponse::new()
    }

    fn descriptor_static(_: ::std::option::Option<IsBootstrappedResponse>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<ResponseHeader>>(
                    "header",
                    IsBootstrappedResponse::get_header_for_reflect,
                    IsBootstrappedResponse::mut_header_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeBool>(
                    "bootstrapped",
                    IsBootstrappedResponse::get_bootstrapped_for_reflect,
                    IsBootstrappedResponse::mut_bootstrapped_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<IsBootstrappedResponse>(
                    "IsBootstrappedResponse",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for IsBootstrappedResponse {
    fn clear(&mut self) {
        self.clear_header();
        self.clear_bootstrapped();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for IsBootstrappedResponse {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for IsBootstrappedResponse {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct AllocIDRequest {
    // message fields
    pub header: ::protobuf::SingularPtrField<RequestHeader>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for AllocIDRequest {}

impl AllocIDRequest {
    pub fn new() -> AllocIDRequest {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static AllocIDRequest {
        static mut instance: ::protobuf::lazy::Lazy<AllocIDRequest> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const AllocIDRequest,
        };
        unsafe {
            instance.get(AllocIDRequest::new)
        }
    }

    // .pdpb.RequestHeader header = 1;

    pub fn clear_header(&mut self) {
        self.header.clear();
    }

    pub fn has_header(&self) -> bool {
        self.header.is_some()
    }

    // Param is passed by value, moved
    pub fn set_header(&mut self, v: RequestHeader) {
        self.header = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_header(&mut self) -> &mut RequestHeader {
        if self.header.is_none() {
            self.header.set_default();
        }
        self.header.as_mut().unwrap()
    }

    // Take field
    pub fn take_header(&mut self) -> RequestHeader {
        self.header.take().unwrap_or_else(|| RequestHeader::new())
    }

    pub fn get_header(&self) -> &RequestHeader {
        self.header.as_ref().unwrap_or_else(|| RequestHeader::default_instance())
    }

    fn get_header_for_reflect(&self) -> &::protobuf::SingularPtrField<RequestHeader> {
        &self.header
    }

    fn mut_header_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<RequestHeader> {
        &mut self.header
    }
}

impl ::protobuf::Message for AllocIDRequest {
    fn is_initialized(&self) -> bool {
        for v in &self.header {
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
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.header)?;
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
        if let Some(ref v) = self.header.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(ref v) = self.header.as_ref() {
            os.write_tag(1, ::protobuf::wire_format::WireTypeLengthDelimited)?;
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

impl ::protobuf::MessageStatic for AllocIDRequest {
    fn new() -> AllocIDRequest {
        AllocIDRequest::new()
    }

    fn descriptor_static(_: ::std::option::Option<AllocIDRequest>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<RequestHeader>>(
                    "header",
                    AllocIDRequest::get_header_for_reflect,
                    AllocIDRequest::mut_header_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<AllocIDRequest>(
                    "AllocIDRequest",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for AllocIDRequest {
    fn clear(&mut self) {
        self.clear_header();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for AllocIDRequest {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for AllocIDRequest {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct AllocIDResponse {
    // message fields
    pub header: ::protobuf::SingularPtrField<ResponseHeader>,
    pub id: u64,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for AllocIDResponse {}

impl AllocIDResponse {
    pub fn new() -> AllocIDResponse {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static AllocIDResponse {
        static mut instance: ::protobuf::lazy::Lazy<AllocIDResponse> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const AllocIDResponse,
        };
        unsafe {
            instance.get(AllocIDResponse::new)
        }
    }

    // .pdpb.ResponseHeader header = 1;

    pub fn clear_header(&mut self) {
        self.header.clear();
    }

    pub fn has_header(&self) -> bool {
        self.header.is_some()
    }

    // Param is passed by value, moved
    pub fn set_header(&mut self, v: ResponseHeader) {
        self.header = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_header(&mut self) -> &mut ResponseHeader {
        if self.header.is_none() {
            self.header.set_default();
        }
        self.header.as_mut().unwrap()
    }

    // Take field
    pub fn take_header(&mut self) -> ResponseHeader {
        self.header.take().unwrap_or_else(|| ResponseHeader::new())
    }

    pub fn get_header(&self) -> &ResponseHeader {
        self.header.as_ref().unwrap_or_else(|| ResponseHeader::default_instance())
    }

    fn get_header_for_reflect(&self) -> &::protobuf::SingularPtrField<ResponseHeader> {
        &self.header
    }

    fn mut_header_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<ResponseHeader> {
        &mut self.header
    }

    // uint64 id = 2;

    pub fn clear_id(&mut self) {
        self.id = 0;
    }

    // Param is passed by value, moved
    pub fn set_id(&mut self, v: u64) {
        self.id = v;
    }

    pub fn get_id(&self) -> u64 {
        self.id
    }

    fn get_id_for_reflect(&self) -> &u64 {
        &self.id
    }

    fn mut_id_for_reflect(&mut self) -> &mut u64 {
        &mut self.id
    }
}

impl ::protobuf::Message for AllocIDResponse {
    fn is_initialized(&self) -> bool {
        for v in &self.header {
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
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.header)?;
                },
                2 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_uint64()?;
                    self.id = tmp;
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
        if let Some(ref v) = self.header.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if self.id != 0 {
            my_size += ::protobuf::rt::value_size(2, self.id, ::protobuf::wire_format::WireTypeVarint);
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(ref v) = self.header.as_ref() {
            os.write_tag(1, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if self.id != 0 {
            os.write_uint64(2, self.id)?;
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

impl ::protobuf::MessageStatic for AllocIDResponse {
    fn new() -> AllocIDResponse {
        AllocIDResponse::new()
    }

    fn descriptor_static(_: ::std::option::Option<AllocIDResponse>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<ResponseHeader>>(
                    "header",
                    AllocIDResponse::get_header_for_reflect,
                    AllocIDResponse::mut_header_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "id",
                    AllocIDResponse::get_id_for_reflect,
                    AllocIDResponse::mut_id_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<AllocIDResponse>(
                    "AllocIDResponse",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for AllocIDResponse {
    fn clear(&mut self) {
        self.clear_header();
        self.clear_id();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for AllocIDResponse {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for AllocIDResponse {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct GetStoreRequest {
    // message fields
    pub header: ::protobuf::SingularPtrField<RequestHeader>,
    pub store_id: u64,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for GetStoreRequest {}

impl GetStoreRequest {
    pub fn new() -> GetStoreRequest {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static GetStoreRequest {
        static mut instance: ::protobuf::lazy::Lazy<GetStoreRequest> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const GetStoreRequest,
        };
        unsafe {
            instance.get(GetStoreRequest::new)
        }
    }

    // .pdpb.RequestHeader header = 1;

    pub fn clear_header(&mut self) {
        self.header.clear();
    }

    pub fn has_header(&self) -> bool {
        self.header.is_some()
    }

    // Param is passed by value, moved
    pub fn set_header(&mut self, v: RequestHeader) {
        self.header = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_header(&mut self) -> &mut RequestHeader {
        if self.header.is_none() {
            self.header.set_default();
        }
        self.header.as_mut().unwrap()
    }

    // Take field
    pub fn take_header(&mut self) -> RequestHeader {
        self.header.take().unwrap_or_else(|| RequestHeader::new())
    }

    pub fn get_header(&self) -> &RequestHeader {
        self.header.as_ref().unwrap_or_else(|| RequestHeader::default_instance())
    }

    fn get_header_for_reflect(&self) -> &::protobuf::SingularPtrField<RequestHeader> {
        &self.header
    }

    fn mut_header_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<RequestHeader> {
        &mut self.header
    }

    // uint64 store_id = 2;

    pub fn clear_store_id(&mut self) {
        self.store_id = 0;
    }

    // Param is passed by value, moved
    pub fn set_store_id(&mut self, v: u64) {
        self.store_id = v;
    }

    pub fn get_store_id(&self) -> u64 {
        self.store_id
    }

    fn get_store_id_for_reflect(&self) -> &u64 {
        &self.store_id
    }

    fn mut_store_id_for_reflect(&mut self) -> &mut u64 {
        &mut self.store_id
    }
}

impl ::protobuf::Message for GetStoreRequest {
    fn is_initialized(&self) -> bool {
        for v in &self.header {
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
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.header)?;
                },
                2 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_uint64()?;
                    self.store_id = tmp;
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
        if let Some(ref v) = self.header.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if self.store_id != 0 {
            my_size += ::protobuf::rt::value_size(2, self.store_id, ::protobuf::wire_format::WireTypeVarint);
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(ref v) = self.header.as_ref() {
            os.write_tag(1, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if self.store_id != 0 {
            os.write_uint64(2, self.store_id)?;
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

impl ::protobuf::MessageStatic for GetStoreRequest {
    fn new() -> GetStoreRequest {
        GetStoreRequest::new()
    }

    fn descriptor_static(_: ::std::option::Option<GetStoreRequest>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<RequestHeader>>(
                    "header",
                    GetStoreRequest::get_header_for_reflect,
                    GetStoreRequest::mut_header_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "store_id",
                    GetStoreRequest::get_store_id_for_reflect,
                    GetStoreRequest::mut_store_id_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<GetStoreRequest>(
                    "GetStoreRequest",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for GetStoreRequest {
    fn clear(&mut self) {
        self.clear_header();
        self.clear_store_id();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for GetStoreRequest {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for GetStoreRequest {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct GetStoreResponse {
    // message fields
    pub header: ::protobuf::SingularPtrField<ResponseHeader>,
    pub store: ::protobuf::SingularPtrField<super::metapb::Store>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for GetStoreResponse {}

impl GetStoreResponse {
    pub fn new() -> GetStoreResponse {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static GetStoreResponse {
        static mut instance: ::protobuf::lazy::Lazy<GetStoreResponse> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const GetStoreResponse,
        };
        unsafe {
            instance.get(GetStoreResponse::new)
        }
    }

    // .pdpb.ResponseHeader header = 1;

    pub fn clear_header(&mut self) {
        self.header.clear();
    }

    pub fn has_header(&self) -> bool {
        self.header.is_some()
    }

    // Param is passed by value, moved
    pub fn set_header(&mut self, v: ResponseHeader) {
        self.header = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_header(&mut self) -> &mut ResponseHeader {
        if self.header.is_none() {
            self.header.set_default();
        }
        self.header.as_mut().unwrap()
    }

    // Take field
    pub fn take_header(&mut self) -> ResponseHeader {
        self.header.take().unwrap_or_else(|| ResponseHeader::new())
    }

    pub fn get_header(&self) -> &ResponseHeader {
        self.header.as_ref().unwrap_or_else(|| ResponseHeader::default_instance())
    }

    fn get_header_for_reflect(&self) -> &::protobuf::SingularPtrField<ResponseHeader> {
        &self.header
    }

    fn mut_header_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<ResponseHeader> {
        &mut self.header
    }

    // .metapb.Store store = 2;

    pub fn clear_store(&mut self) {
        self.store.clear();
    }

    pub fn has_store(&self) -> bool {
        self.store.is_some()
    }

    // Param is passed by value, moved
    pub fn set_store(&mut self, v: super::metapb::Store) {
        self.store = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_store(&mut self) -> &mut super::metapb::Store {
        if self.store.is_none() {
            self.store.set_default();
        }
        self.store.as_mut().unwrap()
    }

    // Take field
    pub fn take_store(&mut self) -> super::metapb::Store {
        self.store.take().unwrap_or_else(|| super::metapb::Store::new())
    }

    pub fn get_store(&self) -> &super::metapb::Store {
        self.store.as_ref().unwrap_or_else(|| super::metapb::Store::default_instance())
    }

    fn get_store_for_reflect(&self) -> &::protobuf::SingularPtrField<super::metapb::Store> {
        &self.store
    }

    fn mut_store_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<super::metapb::Store> {
        &mut self.store
    }
}

impl ::protobuf::Message for GetStoreResponse {
    fn is_initialized(&self) -> bool {
        for v in &self.header {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.store {
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
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.header)?;
                },
                2 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.store)?;
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
        if let Some(ref v) = self.header.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if let Some(ref v) = self.store.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(ref v) = self.header.as_ref() {
            os.write_tag(1, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if let Some(ref v) = self.store.as_ref() {
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

impl ::protobuf::MessageStatic for GetStoreResponse {
    fn new() -> GetStoreResponse {
        GetStoreResponse::new()
    }

    fn descriptor_static(_: ::std::option::Option<GetStoreResponse>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<ResponseHeader>>(
                    "header",
                    GetStoreResponse::get_header_for_reflect,
                    GetStoreResponse::mut_header_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<super::metapb::Store>>(
                    "store",
                    GetStoreResponse::get_store_for_reflect,
                    GetStoreResponse::mut_store_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<GetStoreResponse>(
                    "GetStoreResponse",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for GetStoreResponse {
    fn clear(&mut self) {
        self.clear_header();
        self.clear_store();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for GetStoreResponse {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for GetStoreResponse {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct PutStoreRequest {
    // message fields
    pub header: ::protobuf::SingularPtrField<RequestHeader>,
    pub store: ::protobuf::SingularPtrField<super::metapb::Store>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for PutStoreRequest {}

impl PutStoreRequest {
    pub fn new() -> PutStoreRequest {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static PutStoreRequest {
        static mut instance: ::protobuf::lazy::Lazy<PutStoreRequest> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const PutStoreRequest,
        };
        unsafe {
            instance.get(PutStoreRequest::new)
        }
    }

    // .pdpb.RequestHeader header = 1;

    pub fn clear_header(&mut self) {
        self.header.clear();
    }

    pub fn has_header(&self) -> bool {
        self.header.is_some()
    }

    // Param is passed by value, moved
    pub fn set_header(&mut self, v: RequestHeader) {
        self.header = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_header(&mut self) -> &mut RequestHeader {
        if self.header.is_none() {
            self.header.set_default();
        }
        self.header.as_mut().unwrap()
    }

    // Take field
    pub fn take_header(&mut self) -> RequestHeader {
        self.header.take().unwrap_or_else(|| RequestHeader::new())
    }

    pub fn get_header(&self) -> &RequestHeader {
        self.header.as_ref().unwrap_or_else(|| RequestHeader::default_instance())
    }

    fn get_header_for_reflect(&self) -> &::protobuf::SingularPtrField<RequestHeader> {
        &self.header
    }

    fn mut_header_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<RequestHeader> {
        &mut self.header
    }

    // .metapb.Store store = 2;

    pub fn clear_store(&mut self) {
        self.store.clear();
    }

    pub fn has_store(&self) -> bool {
        self.store.is_some()
    }

    // Param is passed by value, moved
    pub fn set_store(&mut self, v: super::metapb::Store) {
        self.store = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_store(&mut self) -> &mut super::metapb::Store {
        if self.store.is_none() {
            self.store.set_default();
        }
        self.store.as_mut().unwrap()
    }

    // Take field
    pub fn take_store(&mut self) -> super::metapb::Store {
        self.store.take().unwrap_or_else(|| super::metapb::Store::new())
    }

    pub fn get_store(&self) -> &super::metapb::Store {
        self.store.as_ref().unwrap_or_else(|| super::metapb::Store::default_instance())
    }

    fn get_store_for_reflect(&self) -> &::protobuf::SingularPtrField<super::metapb::Store> {
        &self.store
    }

    fn mut_store_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<super::metapb::Store> {
        &mut self.store
    }
}

impl ::protobuf::Message for PutStoreRequest {
    fn is_initialized(&self) -> bool {
        for v in &self.header {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.store {
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
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.header)?;
                },
                2 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.store)?;
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
        if let Some(ref v) = self.header.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if let Some(ref v) = self.store.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(ref v) = self.header.as_ref() {
            os.write_tag(1, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if let Some(ref v) = self.store.as_ref() {
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

impl ::protobuf::MessageStatic for PutStoreRequest {
    fn new() -> PutStoreRequest {
        PutStoreRequest::new()
    }

    fn descriptor_static(_: ::std::option::Option<PutStoreRequest>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<RequestHeader>>(
                    "header",
                    PutStoreRequest::get_header_for_reflect,
                    PutStoreRequest::mut_header_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<super::metapb::Store>>(
                    "store",
                    PutStoreRequest::get_store_for_reflect,
                    PutStoreRequest::mut_store_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<PutStoreRequest>(
                    "PutStoreRequest",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for PutStoreRequest {
    fn clear(&mut self) {
        self.clear_header();
        self.clear_store();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for PutStoreRequest {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for PutStoreRequest {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct PutStoreResponse {
    // message fields
    pub header: ::protobuf::SingularPtrField<ResponseHeader>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for PutStoreResponse {}

impl PutStoreResponse {
    pub fn new() -> PutStoreResponse {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static PutStoreResponse {
        static mut instance: ::protobuf::lazy::Lazy<PutStoreResponse> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const PutStoreResponse,
        };
        unsafe {
            instance.get(PutStoreResponse::new)
        }
    }

    // .pdpb.ResponseHeader header = 1;

    pub fn clear_header(&mut self) {
        self.header.clear();
    }

    pub fn has_header(&self) -> bool {
        self.header.is_some()
    }

    // Param is passed by value, moved
    pub fn set_header(&mut self, v: ResponseHeader) {
        self.header = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_header(&mut self) -> &mut ResponseHeader {
        if self.header.is_none() {
            self.header.set_default();
        }
        self.header.as_mut().unwrap()
    }

    // Take field
    pub fn take_header(&mut self) -> ResponseHeader {
        self.header.take().unwrap_or_else(|| ResponseHeader::new())
    }

    pub fn get_header(&self) -> &ResponseHeader {
        self.header.as_ref().unwrap_or_else(|| ResponseHeader::default_instance())
    }

    fn get_header_for_reflect(&self) -> &::protobuf::SingularPtrField<ResponseHeader> {
        &self.header
    }

    fn mut_header_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<ResponseHeader> {
        &mut self.header
    }
}

impl ::protobuf::Message for PutStoreResponse {
    fn is_initialized(&self) -> bool {
        for v in &self.header {
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
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.header)?;
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
        if let Some(ref v) = self.header.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(ref v) = self.header.as_ref() {
            os.write_tag(1, ::protobuf::wire_format::WireTypeLengthDelimited)?;
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

impl ::protobuf::MessageStatic for PutStoreResponse {
    fn new() -> PutStoreResponse {
        PutStoreResponse::new()
    }

    fn descriptor_static(_: ::std::option::Option<PutStoreResponse>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<ResponseHeader>>(
                    "header",
                    PutStoreResponse::get_header_for_reflect,
                    PutStoreResponse::mut_header_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<PutStoreResponse>(
                    "PutStoreResponse",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for PutStoreResponse {
    fn clear(&mut self) {
        self.clear_header();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for PutStoreResponse {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for PutStoreResponse {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct GetRegionRequest {
    // message fields
    pub header: ::protobuf::SingularPtrField<RequestHeader>,
    pub region_key: ::std::vec::Vec<u8>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for GetRegionRequest {}

impl GetRegionRequest {
    pub fn new() -> GetRegionRequest {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static GetRegionRequest {
        static mut instance: ::protobuf::lazy::Lazy<GetRegionRequest> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const GetRegionRequest,
        };
        unsafe {
            instance.get(GetRegionRequest::new)
        }
    }

    // .pdpb.RequestHeader header = 1;

    pub fn clear_header(&mut self) {
        self.header.clear();
    }

    pub fn has_header(&self) -> bool {
        self.header.is_some()
    }

    // Param is passed by value, moved
    pub fn set_header(&mut self, v: RequestHeader) {
        self.header = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_header(&mut self) -> &mut RequestHeader {
        if self.header.is_none() {
            self.header.set_default();
        }
        self.header.as_mut().unwrap()
    }

    // Take field
    pub fn take_header(&mut self) -> RequestHeader {
        self.header.take().unwrap_or_else(|| RequestHeader::new())
    }

    pub fn get_header(&self) -> &RequestHeader {
        self.header.as_ref().unwrap_or_else(|| RequestHeader::default_instance())
    }

    fn get_header_for_reflect(&self) -> &::protobuf::SingularPtrField<RequestHeader> {
        &self.header
    }

    fn mut_header_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<RequestHeader> {
        &mut self.header
    }

    // bytes region_key = 2;

    pub fn clear_region_key(&mut self) {
        self.region_key.clear();
    }

    // Param is passed by value, moved
    pub fn set_region_key(&mut self, v: ::std::vec::Vec<u8>) {
        self.region_key = v;
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_region_key(&mut self) -> &mut ::std::vec::Vec<u8> {
        &mut self.region_key
    }

    // Take field
    pub fn take_region_key(&mut self) -> ::std::vec::Vec<u8> {
        ::std::mem::replace(&mut self.region_key, ::std::vec::Vec::new())
    }

    pub fn get_region_key(&self) -> &[u8] {
        &self.region_key
    }

    fn get_region_key_for_reflect(&self) -> &::std::vec::Vec<u8> {
        &self.region_key
    }

    fn mut_region_key_for_reflect(&mut self) -> &mut ::std::vec::Vec<u8> {
        &mut self.region_key
    }
}

impl ::protobuf::Message for GetRegionRequest {
    fn is_initialized(&self) -> bool {
        for v in &self.header {
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
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.header)?;
                },
                2 => {
                    ::protobuf::rt::read_singular_proto3_bytes_into(wire_type, is, &mut self.region_key)?;
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
        if let Some(ref v) = self.header.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if !self.region_key.is_empty() {
            my_size += ::protobuf::rt::bytes_size(2, &self.region_key);
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(ref v) = self.header.as_ref() {
            os.write_tag(1, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if !self.region_key.is_empty() {
            os.write_bytes(2, &self.region_key)?;
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

impl ::protobuf::MessageStatic for GetRegionRequest {
    fn new() -> GetRegionRequest {
        GetRegionRequest::new()
    }

    fn descriptor_static(_: ::std::option::Option<GetRegionRequest>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<RequestHeader>>(
                    "header",
                    GetRegionRequest::get_header_for_reflect,
                    GetRegionRequest::mut_header_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeBytes>(
                    "region_key",
                    GetRegionRequest::get_region_key_for_reflect,
                    GetRegionRequest::mut_region_key_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<GetRegionRequest>(
                    "GetRegionRequest",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for GetRegionRequest {
    fn clear(&mut self) {
        self.clear_header();
        self.clear_region_key();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for GetRegionRequest {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for GetRegionRequest {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct GetRegionResponse {
    // message fields
    pub header: ::protobuf::SingularPtrField<ResponseHeader>,
    pub region: ::protobuf::SingularPtrField<super::metapb::Region>,
    pub leader: ::protobuf::SingularPtrField<super::metapb::Peer>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for GetRegionResponse {}

impl GetRegionResponse {
    pub fn new() -> GetRegionResponse {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static GetRegionResponse {
        static mut instance: ::protobuf::lazy::Lazy<GetRegionResponse> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const GetRegionResponse,
        };
        unsafe {
            instance.get(GetRegionResponse::new)
        }
    }

    // .pdpb.ResponseHeader header = 1;

    pub fn clear_header(&mut self) {
        self.header.clear();
    }

    pub fn has_header(&self) -> bool {
        self.header.is_some()
    }

    // Param is passed by value, moved
    pub fn set_header(&mut self, v: ResponseHeader) {
        self.header = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_header(&mut self) -> &mut ResponseHeader {
        if self.header.is_none() {
            self.header.set_default();
        }
        self.header.as_mut().unwrap()
    }

    // Take field
    pub fn take_header(&mut self) -> ResponseHeader {
        self.header.take().unwrap_or_else(|| ResponseHeader::new())
    }

    pub fn get_header(&self) -> &ResponseHeader {
        self.header.as_ref().unwrap_or_else(|| ResponseHeader::default_instance())
    }

    fn get_header_for_reflect(&self) -> &::protobuf::SingularPtrField<ResponseHeader> {
        &self.header
    }

    fn mut_header_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<ResponseHeader> {
        &mut self.header
    }

    // .metapb.Region region = 2;

    pub fn clear_region(&mut self) {
        self.region.clear();
    }

    pub fn has_region(&self) -> bool {
        self.region.is_some()
    }

    // Param is passed by value, moved
    pub fn set_region(&mut self, v: super::metapb::Region) {
        self.region = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_region(&mut self) -> &mut super::metapb::Region {
        if self.region.is_none() {
            self.region.set_default();
        }
        self.region.as_mut().unwrap()
    }

    // Take field
    pub fn take_region(&mut self) -> super::metapb::Region {
        self.region.take().unwrap_or_else(|| super::metapb::Region::new())
    }

    pub fn get_region(&self) -> &super::metapb::Region {
        self.region.as_ref().unwrap_or_else(|| super::metapb::Region::default_instance())
    }

    fn get_region_for_reflect(&self) -> &::protobuf::SingularPtrField<super::metapb::Region> {
        &self.region
    }

    fn mut_region_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<super::metapb::Region> {
        &mut self.region
    }

    // .metapb.Peer leader = 3;

    pub fn clear_leader(&mut self) {
        self.leader.clear();
    }

    pub fn has_leader(&self) -> bool {
        self.leader.is_some()
    }

    // Param is passed by value, moved
    pub fn set_leader(&mut self, v: super::metapb::Peer) {
        self.leader = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_leader(&mut self) -> &mut super::metapb::Peer {
        if self.leader.is_none() {
            self.leader.set_default();
        }
        self.leader.as_mut().unwrap()
    }

    // Take field
    pub fn take_leader(&mut self) -> super::metapb::Peer {
        self.leader.take().unwrap_or_else(|| super::metapb::Peer::new())
    }

    pub fn get_leader(&self) -> &super::metapb::Peer {
        self.leader.as_ref().unwrap_or_else(|| super::metapb::Peer::default_instance())
    }

    fn get_leader_for_reflect(&self) -> &::protobuf::SingularPtrField<super::metapb::Peer> {
        &self.leader
    }

    fn mut_leader_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<super::metapb::Peer> {
        &mut self.leader
    }
}

impl ::protobuf::Message for GetRegionResponse {
    fn is_initialized(&self) -> bool {
        for v in &self.header {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.region {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.leader {
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
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.header)?;
                },
                2 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.region)?;
                },
                3 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.leader)?;
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
        if let Some(ref v) = self.header.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if let Some(ref v) = self.region.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if let Some(ref v) = self.leader.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(ref v) = self.header.as_ref() {
            os.write_tag(1, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if let Some(ref v) = self.region.as_ref() {
            os.write_tag(2, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if let Some(ref v) = self.leader.as_ref() {
            os.write_tag(3, ::protobuf::wire_format::WireTypeLengthDelimited)?;
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

impl ::protobuf::MessageStatic for GetRegionResponse {
    fn new() -> GetRegionResponse {
        GetRegionResponse::new()
    }

    fn descriptor_static(_: ::std::option::Option<GetRegionResponse>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<ResponseHeader>>(
                    "header",
                    GetRegionResponse::get_header_for_reflect,
                    GetRegionResponse::mut_header_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<super::metapb::Region>>(
                    "region",
                    GetRegionResponse::get_region_for_reflect,
                    GetRegionResponse::mut_region_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<super::metapb::Peer>>(
                    "leader",
                    GetRegionResponse::get_leader_for_reflect,
                    GetRegionResponse::mut_leader_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<GetRegionResponse>(
                    "GetRegionResponse",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for GetRegionResponse {
    fn clear(&mut self) {
        self.clear_header();
        self.clear_region();
        self.clear_leader();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for GetRegionResponse {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for GetRegionResponse {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct GetRegionByIDRequest {
    // message fields
    pub header: ::protobuf::SingularPtrField<RequestHeader>,
    pub region_id: u64,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for GetRegionByIDRequest {}

impl GetRegionByIDRequest {
    pub fn new() -> GetRegionByIDRequest {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static GetRegionByIDRequest {
        static mut instance: ::protobuf::lazy::Lazy<GetRegionByIDRequest> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const GetRegionByIDRequest,
        };
        unsafe {
            instance.get(GetRegionByIDRequest::new)
        }
    }

    // .pdpb.RequestHeader header = 1;

    pub fn clear_header(&mut self) {
        self.header.clear();
    }

    pub fn has_header(&self) -> bool {
        self.header.is_some()
    }

    // Param is passed by value, moved
    pub fn set_header(&mut self, v: RequestHeader) {
        self.header = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_header(&mut self) -> &mut RequestHeader {
        if self.header.is_none() {
            self.header.set_default();
        }
        self.header.as_mut().unwrap()
    }

    // Take field
    pub fn take_header(&mut self) -> RequestHeader {
        self.header.take().unwrap_or_else(|| RequestHeader::new())
    }

    pub fn get_header(&self) -> &RequestHeader {
        self.header.as_ref().unwrap_or_else(|| RequestHeader::default_instance())
    }

    fn get_header_for_reflect(&self) -> &::protobuf::SingularPtrField<RequestHeader> {
        &self.header
    }

    fn mut_header_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<RequestHeader> {
        &mut self.header
    }

    // uint64 region_id = 2;

    pub fn clear_region_id(&mut self) {
        self.region_id = 0;
    }

    // Param is passed by value, moved
    pub fn set_region_id(&mut self, v: u64) {
        self.region_id = v;
    }

    pub fn get_region_id(&self) -> u64 {
        self.region_id
    }

    fn get_region_id_for_reflect(&self) -> &u64 {
        &self.region_id
    }

    fn mut_region_id_for_reflect(&mut self) -> &mut u64 {
        &mut self.region_id
    }
}

impl ::protobuf::Message for GetRegionByIDRequest {
    fn is_initialized(&self) -> bool {
        for v in &self.header {
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
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.header)?;
                },
                2 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_uint64()?;
                    self.region_id = tmp;
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
        if let Some(ref v) = self.header.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if self.region_id != 0 {
            my_size += ::protobuf::rt::value_size(2, self.region_id, ::protobuf::wire_format::WireTypeVarint);
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(ref v) = self.header.as_ref() {
            os.write_tag(1, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if self.region_id != 0 {
            os.write_uint64(2, self.region_id)?;
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

impl ::protobuf::MessageStatic for GetRegionByIDRequest {
    fn new() -> GetRegionByIDRequest {
        GetRegionByIDRequest::new()
    }

    fn descriptor_static(_: ::std::option::Option<GetRegionByIDRequest>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<RequestHeader>>(
                    "header",
                    GetRegionByIDRequest::get_header_for_reflect,
                    GetRegionByIDRequest::mut_header_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "region_id",
                    GetRegionByIDRequest::get_region_id_for_reflect,
                    GetRegionByIDRequest::mut_region_id_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<GetRegionByIDRequest>(
                    "GetRegionByIDRequest",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for GetRegionByIDRequest {
    fn clear(&mut self) {
        self.clear_header();
        self.clear_region_id();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for GetRegionByIDRequest {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for GetRegionByIDRequest {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct GetClusterConfigRequest {
    // message fields
    pub header: ::protobuf::SingularPtrField<RequestHeader>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for GetClusterConfigRequest {}

impl GetClusterConfigRequest {
    pub fn new() -> GetClusterConfigRequest {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static GetClusterConfigRequest {
        static mut instance: ::protobuf::lazy::Lazy<GetClusterConfigRequest> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const GetClusterConfigRequest,
        };
        unsafe {
            instance.get(GetClusterConfigRequest::new)
        }
    }

    // .pdpb.RequestHeader header = 1;

    pub fn clear_header(&mut self) {
        self.header.clear();
    }

    pub fn has_header(&self) -> bool {
        self.header.is_some()
    }

    // Param is passed by value, moved
    pub fn set_header(&mut self, v: RequestHeader) {
        self.header = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_header(&mut self) -> &mut RequestHeader {
        if self.header.is_none() {
            self.header.set_default();
        }
        self.header.as_mut().unwrap()
    }

    // Take field
    pub fn take_header(&mut self) -> RequestHeader {
        self.header.take().unwrap_or_else(|| RequestHeader::new())
    }

    pub fn get_header(&self) -> &RequestHeader {
        self.header.as_ref().unwrap_or_else(|| RequestHeader::default_instance())
    }

    fn get_header_for_reflect(&self) -> &::protobuf::SingularPtrField<RequestHeader> {
        &self.header
    }

    fn mut_header_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<RequestHeader> {
        &mut self.header
    }
}

impl ::protobuf::Message for GetClusterConfigRequest {
    fn is_initialized(&self) -> bool {
        for v in &self.header {
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
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.header)?;
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
        if let Some(ref v) = self.header.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(ref v) = self.header.as_ref() {
            os.write_tag(1, ::protobuf::wire_format::WireTypeLengthDelimited)?;
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

impl ::protobuf::MessageStatic for GetClusterConfigRequest {
    fn new() -> GetClusterConfigRequest {
        GetClusterConfigRequest::new()
    }

    fn descriptor_static(_: ::std::option::Option<GetClusterConfigRequest>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<RequestHeader>>(
                    "header",
                    GetClusterConfigRequest::get_header_for_reflect,
                    GetClusterConfigRequest::mut_header_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<GetClusterConfigRequest>(
                    "GetClusterConfigRequest",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for GetClusterConfigRequest {
    fn clear(&mut self) {
        self.clear_header();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for GetClusterConfigRequest {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for GetClusterConfigRequest {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct GetClusterConfigResponse {
    // message fields
    pub header: ::protobuf::SingularPtrField<ResponseHeader>,
    pub cluster: ::protobuf::SingularPtrField<super::metapb::Cluster>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for GetClusterConfigResponse {}

impl GetClusterConfigResponse {
    pub fn new() -> GetClusterConfigResponse {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static GetClusterConfigResponse {
        static mut instance: ::protobuf::lazy::Lazy<GetClusterConfigResponse> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const GetClusterConfigResponse,
        };
        unsafe {
            instance.get(GetClusterConfigResponse::new)
        }
    }

    // .pdpb.ResponseHeader header = 1;

    pub fn clear_header(&mut self) {
        self.header.clear();
    }

    pub fn has_header(&self) -> bool {
        self.header.is_some()
    }

    // Param is passed by value, moved
    pub fn set_header(&mut self, v: ResponseHeader) {
        self.header = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_header(&mut self) -> &mut ResponseHeader {
        if self.header.is_none() {
            self.header.set_default();
        }
        self.header.as_mut().unwrap()
    }

    // Take field
    pub fn take_header(&mut self) -> ResponseHeader {
        self.header.take().unwrap_or_else(|| ResponseHeader::new())
    }

    pub fn get_header(&self) -> &ResponseHeader {
        self.header.as_ref().unwrap_or_else(|| ResponseHeader::default_instance())
    }

    fn get_header_for_reflect(&self) -> &::protobuf::SingularPtrField<ResponseHeader> {
        &self.header
    }

    fn mut_header_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<ResponseHeader> {
        &mut self.header
    }

    // .metapb.Cluster cluster = 2;

    pub fn clear_cluster(&mut self) {
        self.cluster.clear();
    }

    pub fn has_cluster(&self) -> bool {
        self.cluster.is_some()
    }

    // Param is passed by value, moved
    pub fn set_cluster(&mut self, v: super::metapb::Cluster) {
        self.cluster = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_cluster(&mut self) -> &mut super::metapb::Cluster {
        if self.cluster.is_none() {
            self.cluster.set_default();
        }
        self.cluster.as_mut().unwrap()
    }

    // Take field
    pub fn take_cluster(&mut self) -> super::metapb::Cluster {
        self.cluster.take().unwrap_or_else(|| super::metapb::Cluster::new())
    }

    pub fn get_cluster(&self) -> &super::metapb::Cluster {
        self.cluster.as_ref().unwrap_or_else(|| super::metapb::Cluster::default_instance())
    }

    fn get_cluster_for_reflect(&self) -> &::protobuf::SingularPtrField<super::metapb::Cluster> {
        &self.cluster
    }

    fn mut_cluster_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<super::metapb::Cluster> {
        &mut self.cluster
    }
}

impl ::protobuf::Message for GetClusterConfigResponse {
    fn is_initialized(&self) -> bool {
        for v in &self.header {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.cluster {
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
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.header)?;
                },
                2 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.cluster)?;
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
        if let Some(ref v) = self.header.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if let Some(ref v) = self.cluster.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(ref v) = self.header.as_ref() {
            os.write_tag(1, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if let Some(ref v) = self.cluster.as_ref() {
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

impl ::protobuf::MessageStatic for GetClusterConfigResponse {
    fn new() -> GetClusterConfigResponse {
        GetClusterConfigResponse::new()
    }

    fn descriptor_static(_: ::std::option::Option<GetClusterConfigResponse>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<ResponseHeader>>(
                    "header",
                    GetClusterConfigResponse::get_header_for_reflect,
                    GetClusterConfigResponse::mut_header_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<super::metapb::Cluster>>(
                    "cluster",
                    GetClusterConfigResponse::get_cluster_for_reflect,
                    GetClusterConfigResponse::mut_cluster_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<GetClusterConfigResponse>(
                    "GetClusterConfigResponse",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for GetClusterConfigResponse {
    fn clear(&mut self) {
        self.clear_header();
        self.clear_cluster();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for GetClusterConfigResponse {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for GetClusterConfigResponse {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct PutClusterConfigRequest {
    // message fields
    pub header: ::protobuf::SingularPtrField<RequestHeader>,
    pub cluster: ::protobuf::SingularPtrField<super::metapb::Cluster>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for PutClusterConfigRequest {}

impl PutClusterConfigRequest {
    pub fn new() -> PutClusterConfigRequest {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static PutClusterConfigRequest {
        static mut instance: ::protobuf::lazy::Lazy<PutClusterConfigRequest> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const PutClusterConfigRequest,
        };
        unsafe {
            instance.get(PutClusterConfigRequest::new)
        }
    }

    // .pdpb.RequestHeader header = 1;

    pub fn clear_header(&mut self) {
        self.header.clear();
    }

    pub fn has_header(&self) -> bool {
        self.header.is_some()
    }

    // Param is passed by value, moved
    pub fn set_header(&mut self, v: RequestHeader) {
        self.header = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_header(&mut self) -> &mut RequestHeader {
        if self.header.is_none() {
            self.header.set_default();
        }
        self.header.as_mut().unwrap()
    }

    // Take field
    pub fn take_header(&mut self) -> RequestHeader {
        self.header.take().unwrap_or_else(|| RequestHeader::new())
    }

    pub fn get_header(&self) -> &RequestHeader {
        self.header.as_ref().unwrap_or_else(|| RequestHeader::default_instance())
    }

    fn get_header_for_reflect(&self) -> &::protobuf::SingularPtrField<RequestHeader> {
        &self.header
    }

    fn mut_header_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<RequestHeader> {
        &mut self.header
    }

    // .metapb.Cluster cluster = 2;

    pub fn clear_cluster(&mut self) {
        self.cluster.clear();
    }

    pub fn has_cluster(&self) -> bool {
        self.cluster.is_some()
    }

    // Param is passed by value, moved
    pub fn set_cluster(&mut self, v: super::metapb::Cluster) {
        self.cluster = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_cluster(&mut self) -> &mut super::metapb::Cluster {
        if self.cluster.is_none() {
            self.cluster.set_default();
        }
        self.cluster.as_mut().unwrap()
    }

    // Take field
    pub fn take_cluster(&mut self) -> super::metapb::Cluster {
        self.cluster.take().unwrap_or_else(|| super::metapb::Cluster::new())
    }

    pub fn get_cluster(&self) -> &super::metapb::Cluster {
        self.cluster.as_ref().unwrap_or_else(|| super::metapb::Cluster::default_instance())
    }

    fn get_cluster_for_reflect(&self) -> &::protobuf::SingularPtrField<super::metapb::Cluster> {
        &self.cluster
    }

    fn mut_cluster_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<super::metapb::Cluster> {
        &mut self.cluster
    }
}

impl ::protobuf::Message for PutClusterConfigRequest {
    fn is_initialized(&self) -> bool {
        for v in &self.header {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.cluster {
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
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.header)?;
                },
                2 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.cluster)?;
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
        if let Some(ref v) = self.header.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if let Some(ref v) = self.cluster.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(ref v) = self.header.as_ref() {
            os.write_tag(1, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if let Some(ref v) = self.cluster.as_ref() {
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

impl ::protobuf::MessageStatic for PutClusterConfigRequest {
    fn new() -> PutClusterConfigRequest {
        PutClusterConfigRequest::new()
    }

    fn descriptor_static(_: ::std::option::Option<PutClusterConfigRequest>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<RequestHeader>>(
                    "header",
                    PutClusterConfigRequest::get_header_for_reflect,
                    PutClusterConfigRequest::mut_header_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<super::metapb::Cluster>>(
                    "cluster",
                    PutClusterConfigRequest::get_cluster_for_reflect,
                    PutClusterConfigRequest::mut_cluster_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<PutClusterConfigRequest>(
                    "PutClusterConfigRequest",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for PutClusterConfigRequest {
    fn clear(&mut self) {
        self.clear_header();
        self.clear_cluster();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for PutClusterConfigRequest {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for PutClusterConfigRequest {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct PutClusterConfigResponse {
    // message fields
    pub header: ::protobuf::SingularPtrField<ResponseHeader>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for PutClusterConfigResponse {}

impl PutClusterConfigResponse {
    pub fn new() -> PutClusterConfigResponse {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static PutClusterConfigResponse {
        static mut instance: ::protobuf::lazy::Lazy<PutClusterConfigResponse> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const PutClusterConfigResponse,
        };
        unsafe {
            instance.get(PutClusterConfigResponse::new)
        }
    }

    // .pdpb.ResponseHeader header = 1;

    pub fn clear_header(&mut self) {
        self.header.clear();
    }

    pub fn has_header(&self) -> bool {
        self.header.is_some()
    }

    // Param is passed by value, moved
    pub fn set_header(&mut self, v: ResponseHeader) {
        self.header = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_header(&mut self) -> &mut ResponseHeader {
        if self.header.is_none() {
            self.header.set_default();
        }
        self.header.as_mut().unwrap()
    }

    // Take field
    pub fn take_header(&mut self) -> ResponseHeader {
        self.header.take().unwrap_or_else(|| ResponseHeader::new())
    }

    pub fn get_header(&self) -> &ResponseHeader {
        self.header.as_ref().unwrap_or_else(|| ResponseHeader::default_instance())
    }

    fn get_header_for_reflect(&self) -> &::protobuf::SingularPtrField<ResponseHeader> {
        &self.header
    }

    fn mut_header_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<ResponseHeader> {
        &mut self.header
    }
}

impl ::protobuf::Message for PutClusterConfigResponse {
    fn is_initialized(&self) -> bool {
        for v in &self.header {
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
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.header)?;
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
        if let Some(ref v) = self.header.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(ref v) = self.header.as_ref() {
            os.write_tag(1, ::protobuf::wire_format::WireTypeLengthDelimited)?;
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

impl ::protobuf::MessageStatic for PutClusterConfigResponse {
    fn new() -> PutClusterConfigResponse {
        PutClusterConfigResponse::new()
    }

    fn descriptor_static(_: ::std::option::Option<PutClusterConfigResponse>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<ResponseHeader>>(
                    "header",
                    PutClusterConfigResponse::get_header_for_reflect,
                    PutClusterConfigResponse::mut_header_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<PutClusterConfigResponse>(
                    "PutClusterConfigResponse",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for PutClusterConfigResponse {
    fn clear(&mut self) {
        self.clear_header();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for PutClusterConfigResponse {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for PutClusterConfigResponse {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct Member {
    // message fields
    pub name: ::std::string::String,
    pub member_id: u64,
    pub peer_urls: ::protobuf::RepeatedField<::std::string::String>,
    pub client_urls: ::protobuf::RepeatedField<::std::string::String>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for Member {}

impl Member {
    pub fn new() -> Member {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static Member {
        static mut instance: ::protobuf::lazy::Lazy<Member> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const Member,
        };
        unsafe {
            instance.get(Member::new)
        }
    }

    // string name = 1;

    pub fn clear_name(&mut self) {
        self.name.clear();
    }

    // Param is passed by value, moved
    pub fn set_name(&mut self, v: ::std::string::String) {
        self.name = v;
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_name(&mut self) -> &mut ::std::string::String {
        &mut self.name
    }

    // Take field
    pub fn take_name(&mut self) -> ::std::string::String {
        ::std::mem::replace(&mut self.name, ::std::string::String::new())
    }

    pub fn get_name(&self) -> &str {
        &self.name
    }

    fn get_name_for_reflect(&self) -> &::std::string::String {
        &self.name
    }

    fn mut_name_for_reflect(&mut self) -> &mut ::std::string::String {
        &mut self.name
    }

    // uint64 member_id = 2;

    pub fn clear_member_id(&mut self) {
        self.member_id = 0;
    }

    // Param is passed by value, moved
    pub fn set_member_id(&mut self, v: u64) {
        self.member_id = v;
    }

    pub fn get_member_id(&self) -> u64 {
        self.member_id
    }

    fn get_member_id_for_reflect(&self) -> &u64 {
        &self.member_id
    }

    fn mut_member_id_for_reflect(&mut self) -> &mut u64 {
        &mut self.member_id
    }

    // repeated string peer_urls = 3;

    pub fn clear_peer_urls(&mut self) {
        self.peer_urls.clear();
    }

    // Param is passed by value, moved
    pub fn set_peer_urls(&mut self, v: ::protobuf::RepeatedField<::std::string::String>) {
        self.peer_urls = v;
    }

    // Mutable pointer to the field.
    pub fn mut_peer_urls(&mut self) -> &mut ::protobuf::RepeatedField<::std::string::String> {
        &mut self.peer_urls
    }

    // Take field
    pub fn take_peer_urls(&mut self) -> ::protobuf::RepeatedField<::std::string::String> {
        ::std::mem::replace(&mut self.peer_urls, ::protobuf::RepeatedField::new())
    }

    pub fn get_peer_urls(&self) -> &[::std::string::String] {
        &self.peer_urls
    }

    fn get_peer_urls_for_reflect(&self) -> &::protobuf::RepeatedField<::std::string::String> {
        &self.peer_urls
    }

    fn mut_peer_urls_for_reflect(&mut self) -> &mut ::protobuf::RepeatedField<::std::string::String> {
        &mut self.peer_urls
    }

    // repeated string client_urls = 4;

    pub fn clear_client_urls(&mut self) {
        self.client_urls.clear();
    }

    // Param is passed by value, moved
    pub fn set_client_urls(&mut self, v: ::protobuf::RepeatedField<::std::string::String>) {
        self.client_urls = v;
    }

    // Mutable pointer to the field.
    pub fn mut_client_urls(&mut self) -> &mut ::protobuf::RepeatedField<::std::string::String> {
        &mut self.client_urls
    }

    // Take field
    pub fn take_client_urls(&mut self) -> ::protobuf::RepeatedField<::std::string::String> {
        ::std::mem::replace(&mut self.client_urls, ::protobuf::RepeatedField::new())
    }

    pub fn get_client_urls(&self) -> &[::std::string::String] {
        &self.client_urls
    }

    fn get_client_urls_for_reflect(&self) -> &::protobuf::RepeatedField<::std::string::String> {
        &self.client_urls
    }

    fn mut_client_urls_for_reflect(&mut self) -> &mut ::protobuf::RepeatedField<::std::string::String> {
        &mut self.client_urls
    }
}

impl ::protobuf::Message for Member {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !is.eof()? {
            let (field_number, wire_type) = is.read_tag_unpack()?;
            match field_number {
                1 => {
                    ::protobuf::rt::read_singular_proto3_string_into(wire_type, is, &mut self.name)?;
                },
                2 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_uint64()?;
                    self.member_id = tmp;
                },
                3 => {
                    ::protobuf::rt::read_repeated_string_into(wire_type, is, &mut self.peer_urls)?;
                },
                4 => {
                    ::protobuf::rt::read_repeated_string_into(wire_type, is, &mut self.client_urls)?;
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
        if !self.name.is_empty() {
            my_size += ::protobuf::rt::string_size(1, &self.name);
        }
        if self.member_id != 0 {
            my_size += ::protobuf::rt::value_size(2, self.member_id, ::protobuf::wire_format::WireTypeVarint);
        }
        for value in &self.peer_urls {
            my_size += ::protobuf::rt::string_size(3, &value);
        };
        for value in &self.client_urls {
            my_size += ::protobuf::rt::string_size(4, &value);
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if !self.name.is_empty() {
            os.write_string(1, &self.name)?;
        }
        if self.member_id != 0 {
            os.write_uint64(2, self.member_id)?;
        }
        for v in &self.peer_urls {
            os.write_string(3, &v)?;
        };
        for v in &self.client_urls {
            os.write_string(4, &v)?;
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

impl ::protobuf::MessageStatic for Member {
    fn new() -> Member {
        Member::new()
    }

    fn descriptor_static(_: ::std::option::Option<Member>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeString>(
                    "name",
                    Member::get_name_for_reflect,
                    Member::mut_name_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "member_id",
                    Member::get_member_id_for_reflect,
                    Member::mut_member_id_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_repeated_field_accessor::<_, ::protobuf::types::ProtobufTypeString>(
                    "peer_urls",
                    Member::get_peer_urls_for_reflect,
                    Member::mut_peer_urls_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_repeated_field_accessor::<_, ::protobuf::types::ProtobufTypeString>(
                    "client_urls",
                    Member::get_client_urls_for_reflect,
                    Member::mut_client_urls_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<Member>(
                    "Member",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for Member {
    fn clear(&mut self) {
        self.clear_name();
        self.clear_member_id();
        self.clear_peer_urls();
        self.clear_client_urls();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for Member {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for Member {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct GetMembersRequest {
    // message fields
    pub header: ::protobuf::SingularPtrField<RequestHeader>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for GetMembersRequest {}

impl GetMembersRequest {
    pub fn new() -> GetMembersRequest {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static GetMembersRequest {
        static mut instance: ::protobuf::lazy::Lazy<GetMembersRequest> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const GetMembersRequest,
        };
        unsafe {
            instance.get(GetMembersRequest::new)
        }
    }

    // .pdpb.RequestHeader header = 1;

    pub fn clear_header(&mut self) {
        self.header.clear();
    }

    pub fn has_header(&self) -> bool {
        self.header.is_some()
    }

    // Param is passed by value, moved
    pub fn set_header(&mut self, v: RequestHeader) {
        self.header = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_header(&mut self) -> &mut RequestHeader {
        if self.header.is_none() {
            self.header.set_default();
        }
        self.header.as_mut().unwrap()
    }

    // Take field
    pub fn take_header(&mut self) -> RequestHeader {
        self.header.take().unwrap_or_else(|| RequestHeader::new())
    }

    pub fn get_header(&self) -> &RequestHeader {
        self.header.as_ref().unwrap_or_else(|| RequestHeader::default_instance())
    }

    fn get_header_for_reflect(&self) -> &::protobuf::SingularPtrField<RequestHeader> {
        &self.header
    }

    fn mut_header_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<RequestHeader> {
        &mut self.header
    }
}

impl ::protobuf::Message for GetMembersRequest {
    fn is_initialized(&self) -> bool {
        for v in &self.header {
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
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.header)?;
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
        if let Some(ref v) = self.header.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(ref v) = self.header.as_ref() {
            os.write_tag(1, ::protobuf::wire_format::WireTypeLengthDelimited)?;
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

impl ::protobuf::MessageStatic for GetMembersRequest {
    fn new() -> GetMembersRequest {
        GetMembersRequest::new()
    }

    fn descriptor_static(_: ::std::option::Option<GetMembersRequest>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<RequestHeader>>(
                    "header",
                    GetMembersRequest::get_header_for_reflect,
                    GetMembersRequest::mut_header_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<GetMembersRequest>(
                    "GetMembersRequest",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for GetMembersRequest {
    fn clear(&mut self) {
        self.clear_header();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for GetMembersRequest {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for GetMembersRequest {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct GetMembersResponse {
    // message fields
    pub header: ::protobuf::SingularPtrField<ResponseHeader>,
    pub members: ::protobuf::RepeatedField<Member>,
    pub leader: ::protobuf::SingularPtrField<Member>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for GetMembersResponse {}

impl GetMembersResponse {
    pub fn new() -> GetMembersResponse {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static GetMembersResponse {
        static mut instance: ::protobuf::lazy::Lazy<GetMembersResponse> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const GetMembersResponse,
        };
        unsafe {
            instance.get(GetMembersResponse::new)
        }
    }

    // .pdpb.ResponseHeader header = 1;

    pub fn clear_header(&mut self) {
        self.header.clear();
    }

    pub fn has_header(&self) -> bool {
        self.header.is_some()
    }

    // Param is passed by value, moved
    pub fn set_header(&mut self, v: ResponseHeader) {
        self.header = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_header(&mut self) -> &mut ResponseHeader {
        if self.header.is_none() {
            self.header.set_default();
        }
        self.header.as_mut().unwrap()
    }

    // Take field
    pub fn take_header(&mut self) -> ResponseHeader {
        self.header.take().unwrap_or_else(|| ResponseHeader::new())
    }

    pub fn get_header(&self) -> &ResponseHeader {
        self.header.as_ref().unwrap_or_else(|| ResponseHeader::default_instance())
    }

    fn get_header_for_reflect(&self) -> &::protobuf::SingularPtrField<ResponseHeader> {
        &self.header
    }

    fn mut_header_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<ResponseHeader> {
        &mut self.header
    }

    // repeated .pdpb.Member members = 2;

    pub fn clear_members(&mut self) {
        self.members.clear();
    }

    // Param is passed by value, moved
    pub fn set_members(&mut self, v: ::protobuf::RepeatedField<Member>) {
        self.members = v;
    }

    // Mutable pointer to the field.
    pub fn mut_members(&mut self) -> &mut ::protobuf::RepeatedField<Member> {
        &mut self.members
    }

    // Take field
    pub fn take_members(&mut self) -> ::protobuf::RepeatedField<Member> {
        ::std::mem::replace(&mut self.members, ::protobuf::RepeatedField::new())
    }

    pub fn get_members(&self) -> &[Member] {
        &self.members
    }

    fn get_members_for_reflect(&self) -> &::protobuf::RepeatedField<Member> {
        &self.members
    }

    fn mut_members_for_reflect(&mut self) -> &mut ::protobuf::RepeatedField<Member> {
        &mut self.members
    }

    // .pdpb.Member leader = 3;

    pub fn clear_leader(&mut self) {
        self.leader.clear();
    }

    pub fn has_leader(&self) -> bool {
        self.leader.is_some()
    }

    // Param is passed by value, moved
    pub fn set_leader(&mut self, v: Member) {
        self.leader = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_leader(&mut self) -> &mut Member {
        if self.leader.is_none() {
            self.leader.set_default();
        }
        self.leader.as_mut().unwrap()
    }

    // Take field
    pub fn take_leader(&mut self) -> Member {
        self.leader.take().unwrap_or_else(|| Member::new())
    }

    pub fn get_leader(&self) -> &Member {
        self.leader.as_ref().unwrap_or_else(|| Member::default_instance())
    }

    fn get_leader_for_reflect(&self) -> &::protobuf::SingularPtrField<Member> {
        &self.leader
    }

    fn mut_leader_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<Member> {
        &mut self.leader
    }
}

impl ::protobuf::Message for GetMembersResponse {
    fn is_initialized(&self) -> bool {
        for v in &self.header {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.members {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.leader {
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
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.header)?;
                },
                2 => {
                    ::protobuf::rt::read_repeated_message_into(wire_type, is, &mut self.members)?;
                },
                3 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.leader)?;
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
        if let Some(ref v) = self.header.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        for value in &self.members {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        if let Some(ref v) = self.leader.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(ref v) = self.header.as_ref() {
            os.write_tag(1, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        for v in &self.members {
            os.write_tag(2, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        };
        if let Some(ref v) = self.leader.as_ref() {
            os.write_tag(3, ::protobuf::wire_format::WireTypeLengthDelimited)?;
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

impl ::protobuf::MessageStatic for GetMembersResponse {
    fn new() -> GetMembersResponse {
        GetMembersResponse::new()
    }

    fn descriptor_static(_: ::std::option::Option<GetMembersResponse>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<ResponseHeader>>(
                    "header",
                    GetMembersResponse::get_header_for_reflect,
                    GetMembersResponse::mut_header_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_repeated_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<Member>>(
                    "members",
                    GetMembersResponse::get_members_for_reflect,
                    GetMembersResponse::mut_members_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<Member>>(
                    "leader",
                    GetMembersResponse::get_leader_for_reflect,
                    GetMembersResponse::mut_leader_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<GetMembersResponse>(
                    "GetMembersResponse",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for GetMembersResponse {
    fn clear(&mut self) {
        self.clear_header();
        self.clear_members();
        self.clear_leader();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for GetMembersResponse {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for GetMembersResponse {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct PeerStats {
    // message fields
    pub peer: ::protobuf::SingularPtrField<super::metapb::Peer>,
    pub down_seconds: u64,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for PeerStats {}

impl PeerStats {
    pub fn new() -> PeerStats {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static PeerStats {
        static mut instance: ::protobuf::lazy::Lazy<PeerStats> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const PeerStats,
        };
        unsafe {
            instance.get(PeerStats::new)
        }
    }

    // .metapb.Peer peer = 1;

    pub fn clear_peer(&mut self) {
        self.peer.clear();
    }

    pub fn has_peer(&self) -> bool {
        self.peer.is_some()
    }

    // Param is passed by value, moved
    pub fn set_peer(&mut self, v: super::metapb::Peer) {
        self.peer = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_peer(&mut self) -> &mut super::metapb::Peer {
        if self.peer.is_none() {
            self.peer.set_default();
        }
        self.peer.as_mut().unwrap()
    }

    // Take field
    pub fn take_peer(&mut self) -> super::metapb::Peer {
        self.peer.take().unwrap_or_else(|| super::metapb::Peer::new())
    }

    pub fn get_peer(&self) -> &super::metapb::Peer {
        self.peer.as_ref().unwrap_or_else(|| super::metapb::Peer::default_instance())
    }

    fn get_peer_for_reflect(&self) -> &::protobuf::SingularPtrField<super::metapb::Peer> {
        &self.peer
    }

    fn mut_peer_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<super::metapb::Peer> {
        &mut self.peer
    }

    // uint64 down_seconds = 2;

    pub fn clear_down_seconds(&mut self) {
        self.down_seconds = 0;
    }

    // Param is passed by value, moved
    pub fn set_down_seconds(&mut self, v: u64) {
        self.down_seconds = v;
    }

    pub fn get_down_seconds(&self) -> u64 {
        self.down_seconds
    }

    fn get_down_seconds_for_reflect(&self) -> &u64 {
        &self.down_seconds
    }

    fn mut_down_seconds_for_reflect(&mut self) -> &mut u64 {
        &mut self.down_seconds
    }
}

impl ::protobuf::Message for PeerStats {
    fn is_initialized(&self) -> bool {
        for v in &self.peer {
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
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.peer)?;
                },
                2 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_uint64()?;
                    self.down_seconds = tmp;
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
        if let Some(ref v) = self.peer.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if self.down_seconds != 0 {
            my_size += ::protobuf::rt::value_size(2, self.down_seconds, ::protobuf::wire_format::WireTypeVarint);
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(ref v) = self.peer.as_ref() {
            os.write_tag(1, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if self.down_seconds != 0 {
            os.write_uint64(2, self.down_seconds)?;
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

impl ::protobuf::MessageStatic for PeerStats {
    fn new() -> PeerStats {
        PeerStats::new()
    }

    fn descriptor_static(_: ::std::option::Option<PeerStats>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<super::metapb::Peer>>(
                    "peer",
                    PeerStats::get_peer_for_reflect,
                    PeerStats::mut_peer_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "down_seconds",
                    PeerStats::get_down_seconds_for_reflect,
                    PeerStats::mut_down_seconds_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<PeerStats>(
                    "PeerStats",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for PeerStats {
    fn clear(&mut self) {
        self.clear_peer();
        self.clear_down_seconds();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for PeerStats {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for PeerStats {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct RegionHeartbeatRequest {
    // message fields
    pub header: ::protobuf::SingularPtrField<RequestHeader>,
    pub region: ::protobuf::SingularPtrField<super::metapb::Region>,
    pub leader: ::protobuf::SingularPtrField<super::metapb::Peer>,
    pub down_peers: ::protobuf::RepeatedField<PeerStats>,
    pub pending_peers: ::protobuf::RepeatedField<super::metapb::Peer>,
    pub bytes_written: u64,
    pub bytes_read: u64,
    pub keys_written: u64,
    pub keys_read: u64,
    pub approximate_size: u64,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for RegionHeartbeatRequest {}

impl RegionHeartbeatRequest {
    pub fn new() -> RegionHeartbeatRequest {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static RegionHeartbeatRequest {
        static mut instance: ::protobuf::lazy::Lazy<RegionHeartbeatRequest> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const RegionHeartbeatRequest,
        };
        unsafe {
            instance.get(RegionHeartbeatRequest::new)
        }
    }

    // .pdpb.RequestHeader header = 1;

    pub fn clear_header(&mut self) {
        self.header.clear();
    }

    pub fn has_header(&self) -> bool {
        self.header.is_some()
    }

    // Param is passed by value, moved
    pub fn set_header(&mut self, v: RequestHeader) {
        self.header = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_header(&mut self) -> &mut RequestHeader {
        if self.header.is_none() {
            self.header.set_default();
        }
        self.header.as_mut().unwrap()
    }

    // Take field
    pub fn take_header(&mut self) -> RequestHeader {
        self.header.take().unwrap_or_else(|| RequestHeader::new())
    }

    pub fn get_header(&self) -> &RequestHeader {
        self.header.as_ref().unwrap_or_else(|| RequestHeader::default_instance())
    }

    fn get_header_for_reflect(&self) -> &::protobuf::SingularPtrField<RequestHeader> {
        &self.header
    }

    fn mut_header_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<RequestHeader> {
        &mut self.header
    }

    // .metapb.Region region = 2;

    pub fn clear_region(&mut self) {
        self.region.clear();
    }

    pub fn has_region(&self) -> bool {
        self.region.is_some()
    }

    // Param is passed by value, moved
    pub fn set_region(&mut self, v: super::metapb::Region) {
        self.region = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_region(&mut self) -> &mut super::metapb::Region {
        if self.region.is_none() {
            self.region.set_default();
        }
        self.region.as_mut().unwrap()
    }

    // Take field
    pub fn take_region(&mut self) -> super::metapb::Region {
        self.region.take().unwrap_or_else(|| super::metapb::Region::new())
    }

    pub fn get_region(&self) -> &super::metapb::Region {
        self.region.as_ref().unwrap_or_else(|| super::metapb::Region::default_instance())
    }

    fn get_region_for_reflect(&self) -> &::protobuf::SingularPtrField<super::metapb::Region> {
        &self.region
    }

    fn mut_region_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<super::metapb::Region> {
        &mut self.region
    }

    // .metapb.Peer leader = 3;

    pub fn clear_leader(&mut self) {
        self.leader.clear();
    }

    pub fn has_leader(&self) -> bool {
        self.leader.is_some()
    }

    // Param is passed by value, moved
    pub fn set_leader(&mut self, v: super::metapb::Peer) {
        self.leader = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_leader(&mut self) -> &mut super::metapb::Peer {
        if self.leader.is_none() {
            self.leader.set_default();
        }
        self.leader.as_mut().unwrap()
    }

    // Take field
    pub fn take_leader(&mut self) -> super::metapb::Peer {
        self.leader.take().unwrap_or_else(|| super::metapb::Peer::new())
    }

    pub fn get_leader(&self) -> &super::metapb::Peer {
        self.leader.as_ref().unwrap_or_else(|| super::metapb::Peer::default_instance())
    }

    fn get_leader_for_reflect(&self) -> &::protobuf::SingularPtrField<super::metapb::Peer> {
        &self.leader
    }

    fn mut_leader_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<super::metapb::Peer> {
        &mut self.leader
    }

    // repeated .pdpb.PeerStats down_peers = 4;

    pub fn clear_down_peers(&mut self) {
        self.down_peers.clear();
    }

    // Param is passed by value, moved
    pub fn set_down_peers(&mut self, v: ::protobuf::RepeatedField<PeerStats>) {
        self.down_peers = v;
    }

    // Mutable pointer to the field.
    pub fn mut_down_peers(&mut self) -> &mut ::protobuf::RepeatedField<PeerStats> {
        &mut self.down_peers
    }

    // Take field
    pub fn take_down_peers(&mut self) -> ::protobuf::RepeatedField<PeerStats> {
        ::std::mem::replace(&mut self.down_peers, ::protobuf::RepeatedField::new())
    }

    pub fn get_down_peers(&self) -> &[PeerStats] {
        &self.down_peers
    }

    fn get_down_peers_for_reflect(&self) -> &::protobuf::RepeatedField<PeerStats> {
        &self.down_peers
    }

    fn mut_down_peers_for_reflect(&mut self) -> &mut ::protobuf::RepeatedField<PeerStats> {
        &mut self.down_peers
    }

    // repeated .metapb.Peer pending_peers = 5;

    pub fn clear_pending_peers(&mut self) {
        self.pending_peers.clear();
    }

    // Param is passed by value, moved
    pub fn set_pending_peers(&mut self, v: ::protobuf::RepeatedField<super::metapb::Peer>) {
        self.pending_peers = v;
    }

    // Mutable pointer to the field.
    pub fn mut_pending_peers(&mut self) -> &mut ::protobuf::RepeatedField<super::metapb::Peer> {
        &mut self.pending_peers
    }

    // Take field
    pub fn take_pending_peers(&mut self) -> ::protobuf::RepeatedField<super::metapb::Peer> {
        ::std::mem::replace(&mut self.pending_peers, ::protobuf::RepeatedField::new())
    }

    pub fn get_pending_peers(&self) -> &[super::metapb::Peer] {
        &self.pending_peers
    }

    fn get_pending_peers_for_reflect(&self) -> &::protobuf::RepeatedField<super::metapb::Peer> {
        &self.pending_peers
    }

    fn mut_pending_peers_for_reflect(&mut self) -> &mut ::protobuf::RepeatedField<super::metapb::Peer> {
        &mut self.pending_peers
    }

    // uint64 bytes_written = 6;

    pub fn clear_bytes_written(&mut self) {
        self.bytes_written = 0;
    }

    // Param is passed by value, moved
    pub fn set_bytes_written(&mut self, v: u64) {
        self.bytes_written = v;
    }

    pub fn get_bytes_written(&self) -> u64 {
        self.bytes_written
    }

    fn get_bytes_written_for_reflect(&self) -> &u64 {
        &self.bytes_written
    }

    fn mut_bytes_written_for_reflect(&mut self) -> &mut u64 {
        &mut self.bytes_written
    }

    // uint64 bytes_read = 7;

    pub fn clear_bytes_read(&mut self) {
        self.bytes_read = 0;
    }

    // Param is passed by value, moved
    pub fn set_bytes_read(&mut self, v: u64) {
        self.bytes_read = v;
    }

    pub fn get_bytes_read(&self) -> u64 {
        self.bytes_read
    }

    fn get_bytes_read_for_reflect(&self) -> &u64 {
        &self.bytes_read
    }

    fn mut_bytes_read_for_reflect(&mut self) -> &mut u64 {
        &mut self.bytes_read
    }

    // uint64 keys_written = 8;

    pub fn clear_keys_written(&mut self) {
        self.keys_written = 0;
    }

    // Param is passed by value, moved
    pub fn set_keys_written(&mut self, v: u64) {
        self.keys_written = v;
    }

    pub fn get_keys_written(&self) -> u64 {
        self.keys_written
    }

    fn get_keys_written_for_reflect(&self) -> &u64 {
        &self.keys_written
    }

    fn mut_keys_written_for_reflect(&mut self) -> &mut u64 {
        &mut self.keys_written
    }

    // uint64 keys_read = 9;

    pub fn clear_keys_read(&mut self) {
        self.keys_read = 0;
    }

    // Param is passed by value, moved
    pub fn set_keys_read(&mut self, v: u64) {
        self.keys_read = v;
    }

    pub fn get_keys_read(&self) -> u64 {
        self.keys_read
    }

    fn get_keys_read_for_reflect(&self) -> &u64 {
        &self.keys_read
    }

    fn mut_keys_read_for_reflect(&mut self) -> &mut u64 {
        &mut self.keys_read
    }

    // uint64 approximate_size = 10;

    pub fn clear_approximate_size(&mut self) {
        self.approximate_size = 0;
    }

    // Param is passed by value, moved
    pub fn set_approximate_size(&mut self, v: u64) {
        self.approximate_size = v;
    }

    pub fn get_approximate_size(&self) -> u64 {
        self.approximate_size
    }

    fn get_approximate_size_for_reflect(&self) -> &u64 {
        &self.approximate_size
    }

    fn mut_approximate_size_for_reflect(&mut self) -> &mut u64 {
        &mut self.approximate_size
    }
}

impl ::protobuf::Message for RegionHeartbeatRequest {
    fn is_initialized(&self) -> bool {
        for v in &self.header {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.region {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.leader {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.down_peers {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.pending_peers {
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
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.header)?;
                },
                2 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.region)?;
                },
                3 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.leader)?;
                },
                4 => {
                    ::protobuf::rt::read_repeated_message_into(wire_type, is, &mut self.down_peers)?;
                },
                5 => {
                    ::protobuf::rt::read_repeated_message_into(wire_type, is, &mut self.pending_peers)?;
                },
                6 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_uint64()?;
                    self.bytes_written = tmp;
                },
                7 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_uint64()?;
                    self.bytes_read = tmp;
                },
                8 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_uint64()?;
                    self.keys_written = tmp;
                },
                9 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_uint64()?;
                    self.keys_read = tmp;
                },
                10 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_uint64()?;
                    self.approximate_size = tmp;
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
        if let Some(ref v) = self.header.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if let Some(ref v) = self.region.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if let Some(ref v) = self.leader.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        for value in &self.down_peers {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        for value in &self.pending_peers {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        if self.bytes_written != 0 {
            my_size += ::protobuf::rt::value_size(6, self.bytes_written, ::protobuf::wire_format::WireTypeVarint);
        }
        if self.bytes_read != 0 {
            my_size += ::protobuf::rt::value_size(7, self.bytes_read, ::protobuf::wire_format::WireTypeVarint);
        }
        if self.keys_written != 0 {
            my_size += ::protobuf::rt::value_size(8, self.keys_written, ::protobuf::wire_format::WireTypeVarint);
        }
        if self.keys_read != 0 {
            my_size += ::protobuf::rt::value_size(9, self.keys_read, ::protobuf::wire_format::WireTypeVarint);
        }
        if self.approximate_size != 0 {
            my_size += ::protobuf::rt::value_size(10, self.approximate_size, ::protobuf::wire_format::WireTypeVarint);
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(ref v) = self.header.as_ref() {
            os.write_tag(1, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if let Some(ref v) = self.region.as_ref() {
            os.write_tag(2, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if let Some(ref v) = self.leader.as_ref() {
            os.write_tag(3, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        for v in &self.down_peers {
            os.write_tag(4, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        };
        for v in &self.pending_peers {
            os.write_tag(5, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        };
        if self.bytes_written != 0 {
            os.write_uint64(6, self.bytes_written)?;
        }
        if self.bytes_read != 0 {
            os.write_uint64(7, self.bytes_read)?;
        }
        if self.keys_written != 0 {
            os.write_uint64(8, self.keys_written)?;
        }
        if self.keys_read != 0 {
            os.write_uint64(9, self.keys_read)?;
        }
        if self.approximate_size != 0 {
            os.write_uint64(10, self.approximate_size)?;
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

impl ::protobuf::MessageStatic for RegionHeartbeatRequest {
    fn new() -> RegionHeartbeatRequest {
        RegionHeartbeatRequest::new()
    }

    fn descriptor_static(_: ::std::option::Option<RegionHeartbeatRequest>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<RequestHeader>>(
                    "header",
                    RegionHeartbeatRequest::get_header_for_reflect,
                    RegionHeartbeatRequest::mut_header_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<super::metapb::Region>>(
                    "region",
                    RegionHeartbeatRequest::get_region_for_reflect,
                    RegionHeartbeatRequest::mut_region_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<super::metapb::Peer>>(
                    "leader",
                    RegionHeartbeatRequest::get_leader_for_reflect,
                    RegionHeartbeatRequest::mut_leader_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_repeated_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<PeerStats>>(
                    "down_peers",
                    RegionHeartbeatRequest::get_down_peers_for_reflect,
                    RegionHeartbeatRequest::mut_down_peers_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_repeated_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<super::metapb::Peer>>(
                    "pending_peers",
                    RegionHeartbeatRequest::get_pending_peers_for_reflect,
                    RegionHeartbeatRequest::mut_pending_peers_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "bytes_written",
                    RegionHeartbeatRequest::get_bytes_written_for_reflect,
                    RegionHeartbeatRequest::mut_bytes_written_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "bytes_read",
                    RegionHeartbeatRequest::get_bytes_read_for_reflect,
                    RegionHeartbeatRequest::mut_bytes_read_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "keys_written",
                    RegionHeartbeatRequest::get_keys_written_for_reflect,
                    RegionHeartbeatRequest::mut_keys_written_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "keys_read",
                    RegionHeartbeatRequest::get_keys_read_for_reflect,
                    RegionHeartbeatRequest::mut_keys_read_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "approximate_size",
                    RegionHeartbeatRequest::get_approximate_size_for_reflect,
                    RegionHeartbeatRequest::mut_approximate_size_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<RegionHeartbeatRequest>(
                    "RegionHeartbeatRequest",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for RegionHeartbeatRequest {
    fn clear(&mut self) {
        self.clear_header();
        self.clear_region();
        self.clear_leader();
        self.clear_down_peers();
        self.clear_pending_peers();
        self.clear_bytes_written();
        self.clear_bytes_read();
        self.clear_keys_written();
        self.clear_keys_read();
        self.clear_approximate_size();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for RegionHeartbeatRequest {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for RegionHeartbeatRequest {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct ChangePeer {
    // message fields
    pub peer: ::protobuf::SingularPtrField<super::metapb::Peer>,
    pub change_type: super::eraftpb::ConfChangeType,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for ChangePeer {}

impl ChangePeer {
    pub fn new() -> ChangePeer {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static ChangePeer {
        static mut instance: ::protobuf::lazy::Lazy<ChangePeer> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ChangePeer,
        };
        unsafe {
            instance.get(ChangePeer::new)
        }
    }

    // .metapb.Peer peer = 1;

    pub fn clear_peer(&mut self) {
        self.peer.clear();
    }

    pub fn has_peer(&self) -> bool {
        self.peer.is_some()
    }

    // Param is passed by value, moved
    pub fn set_peer(&mut self, v: super::metapb::Peer) {
        self.peer = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_peer(&mut self) -> &mut super::metapb::Peer {
        if self.peer.is_none() {
            self.peer.set_default();
        }
        self.peer.as_mut().unwrap()
    }

    // Take field
    pub fn take_peer(&mut self) -> super::metapb::Peer {
        self.peer.take().unwrap_or_else(|| super::metapb::Peer::new())
    }

    pub fn get_peer(&self) -> &super::metapb::Peer {
        self.peer.as_ref().unwrap_or_else(|| super::metapb::Peer::default_instance())
    }

    fn get_peer_for_reflect(&self) -> &::protobuf::SingularPtrField<super::metapb::Peer> {
        &self.peer
    }

    fn mut_peer_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<super::metapb::Peer> {
        &mut self.peer
    }

    // .eraftpb.ConfChangeType change_type = 2;

    pub fn clear_change_type(&mut self) {
        self.change_type = super::eraftpb::ConfChangeType::AddNode;
    }

    // Param is passed by value, moved
    pub fn set_change_type(&mut self, v: super::eraftpb::ConfChangeType) {
        self.change_type = v;
    }

    pub fn get_change_type(&self) -> super::eraftpb::ConfChangeType {
        self.change_type
    }

    fn get_change_type_for_reflect(&self) -> &super::eraftpb::ConfChangeType {
        &self.change_type
    }

    fn mut_change_type_for_reflect(&mut self) -> &mut super::eraftpb::ConfChangeType {
        &mut self.change_type
    }
}

impl ::protobuf::Message for ChangePeer {
    fn is_initialized(&self) -> bool {
        for v in &self.peer {
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
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.peer)?;
                },
                2 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_enum()?;
                    self.change_type = tmp;
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
        if let Some(ref v) = self.peer.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if self.change_type != super::eraftpb::ConfChangeType::AddNode {
            my_size += ::protobuf::rt::enum_size(2, self.change_type);
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(ref v) = self.peer.as_ref() {
            os.write_tag(1, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if self.change_type != super::eraftpb::ConfChangeType::AddNode {
            os.write_enum(2, self.change_type.value())?;
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

impl ::protobuf::MessageStatic for ChangePeer {
    fn new() -> ChangePeer {
        ChangePeer::new()
    }

    fn descriptor_static(_: ::std::option::Option<ChangePeer>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<super::metapb::Peer>>(
                    "peer",
                    ChangePeer::get_peer_for_reflect,
                    ChangePeer::mut_peer_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeEnum<super::eraftpb::ConfChangeType>>(
                    "change_type",
                    ChangePeer::get_change_type_for_reflect,
                    ChangePeer::mut_change_type_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<ChangePeer>(
                    "ChangePeer",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for ChangePeer {
    fn clear(&mut self) {
        self.clear_peer();
        self.clear_change_type();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for ChangePeer {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for ChangePeer {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct TransferLeader {
    // message fields
    pub peer: ::protobuf::SingularPtrField<super::metapb::Peer>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for TransferLeader {}

impl TransferLeader {
    pub fn new() -> TransferLeader {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static TransferLeader {
        static mut instance: ::protobuf::lazy::Lazy<TransferLeader> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const TransferLeader,
        };
        unsafe {
            instance.get(TransferLeader::new)
        }
    }

    // .metapb.Peer peer = 1;

    pub fn clear_peer(&mut self) {
        self.peer.clear();
    }

    pub fn has_peer(&self) -> bool {
        self.peer.is_some()
    }

    // Param is passed by value, moved
    pub fn set_peer(&mut self, v: super::metapb::Peer) {
        self.peer = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_peer(&mut self) -> &mut super::metapb::Peer {
        if self.peer.is_none() {
            self.peer.set_default();
        }
        self.peer.as_mut().unwrap()
    }

    // Take field
    pub fn take_peer(&mut self) -> super::metapb::Peer {
        self.peer.take().unwrap_or_else(|| super::metapb::Peer::new())
    }

    pub fn get_peer(&self) -> &super::metapb::Peer {
        self.peer.as_ref().unwrap_or_else(|| super::metapb::Peer::default_instance())
    }

    fn get_peer_for_reflect(&self) -> &::protobuf::SingularPtrField<super::metapb::Peer> {
        &self.peer
    }

    fn mut_peer_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<super::metapb::Peer> {
        &mut self.peer
    }
}

impl ::protobuf::Message for TransferLeader {
    fn is_initialized(&self) -> bool {
        for v in &self.peer {
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
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.peer)?;
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
        if let Some(ref v) = self.peer.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(ref v) = self.peer.as_ref() {
            os.write_tag(1, ::protobuf::wire_format::WireTypeLengthDelimited)?;
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

impl ::protobuf::MessageStatic for TransferLeader {
    fn new() -> TransferLeader {
        TransferLeader::new()
    }

    fn descriptor_static(_: ::std::option::Option<TransferLeader>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<super::metapb::Peer>>(
                    "peer",
                    TransferLeader::get_peer_for_reflect,
                    TransferLeader::mut_peer_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<TransferLeader>(
                    "TransferLeader",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for TransferLeader {
    fn clear(&mut self) {
        self.clear_peer();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for TransferLeader {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for TransferLeader {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct RegionHeartbeatResponse {
    // message fields
    pub header: ::protobuf::SingularPtrField<ResponseHeader>,
    pub change_peer: ::protobuf::SingularPtrField<ChangePeer>,
    pub transfer_leader: ::protobuf::SingularPtrField<TransferLeader>,
    pub region_id: u64,
    pub region_epoch: ::protobuf::SingularPtrField<super::metapb::RegionEpoch>,
    pub target_peer: ::protobuf::SingularPtrField<super::metapb::Peer>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for RegionHeartbeatResponse {}

impl RegionHeartbeatResponse {
    pub fn new() -> RegionHeartbeatResponse {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static RegionHeartbeatResponse {
        static mut instance: ::protobuf::lazy::Lazy<RegionHeartbeatResponse> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const RegionHeartbeatResponse,
        };
        unsafe {
            instance.get(RegionHeartbeatResponse::new)
        }
    }

    // .pdpb.ResponseHeader header = 1;

    pub fn clear_header(&mut self) {
        self.header.clear();
    }

    pub fn has_header(&self) -> bool {
        self.header.is_some()
    }

    // Param is passed by value, moved
    pub fn set_header(&mut self, v: ResponseHeader) {
        self.header = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_header(&mut self) -> &mut ResponseHeader {
        if self.header.is_none() {
            self.header.set_default();
        }
        self.header.as_mut().unwrap()
    }

    // Take field
    pub fn take_header(&mut self) -> ResponseHeader {
        self.header.take().unwrap_or_else(|| ResponseHeader::new())
    }

    pub fn get_header(&self) -> &ResponseHeader {
        self.header.as_ref().unwrap_or_else(|| ResponseHeader::default_instance())
    }

    fn get_header_for_reflect(&self) -> &::protobuf::SingularPtrField<ResponseHeader> {
        &self.header
    }

    fn mut_header_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<ResponseHeader> {
        &mut self.header
    }

    // .pdpb.ChangePeer change_peer = 2;

    pub fn clear_change_peer(&mut self) {
        self.change_peer.clear();
    }

    pub fn has_change_peer(&self) -> bool {
        self.change_peer.is_some()
    }

    // Param is passed by value, moved
    pub fn set_change_peer(&mut self, v: ChangePeer) {
        self.change_peer = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_change_peer(&mut self) -> &mut ChangePeer {
        if self.change_peer.is_none() {
            self.change_peer.set_default();
        }
        self.change_peer.as_mut().unwrap()
    }

    // Take field
    pub fn take_change_peer(&mut self) -> ChangePeer {
        self.change_peer.take().unwrap_or_else(|| ChangePeer::new())
    }

    pub fn get_change_peer(&self) -> &ChangePeer {
        self.change_peer.as_ref().unwrap_or_else(|| ChangePeer::default_instance())
    }

    fn get_change_peer_for_reflect(&self) -> &::protobuf::SingularPtrField<ChangePeer> {
        &self.change_peer
    }

    fn mut_change_peer_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<ChangePeer> {
        &mut self.change_peer
    }

    // .pdpb.TransferLeader transfer_leader = 3;

    pub fn clear_transfer_leader(&mut self) {
        self.transfer_leader.clear();
    }

    pub fn has_transfer_leader(&self) -> bool {
        self.transfer_leader.is_some()
    }

    // Param is passed by value, moved
    pub fn set_transfer_leader(&mut self, v: TransferLeader) {
        self.transfer_leader = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_transfer_leader(&mut self) -> &mut TransferLeader {
        if self.transfer_leader.is_none() {
            self.transfer_leader.set_default();
        }
        self.transfer_leader.as_mut().unwrap()
    }

    // Take field
    pub fn take_transfer_leader(&mut self) -> TransferLeader {
        self.transfer_leader.take().unwrap_or_else(|| TransferLeader::new())
    }

    pub fn get_transfer_leader(&self) -> &TransferLeader {
        self.transfer_leader.as_ref().unwrap_or_else(|| TransferLeader::default_instance())
    }

    fn get_transfer_leader_for_reflect(&self) -> &::protobuf::SingularPtrField<TransferLeader> {
        &self.transfer_leader
    }

    fn mut_transfer_leader_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<TransferLeader> {
        &mut self.transfer_leader
    }

    // uint64 region_id = 4;

    pub fn clear_region_id(&mut self) {
        self.region_id = 0;
    }

    // Param is passed by value, moved
    pub fn set_region_id(&mut self, v: u64) {
        self.region_id = v;
    }

    pub fn get_region_id(&self) -> u64 {
        self.region_id
    }

    fn get_region_id_for_reflect(&self) -> &u64 {
        &self.region_id
    }

    fn mut_region_id_for_reflect(&mut self) -> &mut u64 {
        &mut self.region_id
    }

    // .metapb.RegionEpoch region_epoch = 5;

    pub fn clear_region_epoch(&mut self) {
        self.region_epoch.clear();
    }

    pub fn has_region_epoch(&self) -> bool {
        self.region_epoch.is_some()
    }

    // Param is passed by value, moved
    pub fn set_region_epoch(&mut self, v: super::metapb::RegionEpoch) {
        self.region_epoch = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_region_epoch(&mut self) -> &mut super::metapb::RegionEpoch {
        if self.region_epoch.is_none() {
            self.region_epoch.set_default();
        }
        self.region_epoch.as_mut().unwrap()
    }

    // Take field
    pub fn take_region_epoch(&mut self) -> super::metapb::RegionEpoch {
        self.region_epoch.take().unwrap_or_else(|| super::metapb::RegionEpoch::new())
    }

    pub fn get_region_epoch(&self) -> &super::metapb::RegionEpoch {
        self.region_epoch.as_ref().unwrap_or_else(|| super::metapb::RegionEpoch::default_instance())
    }

    fn get_region_epoch_for_reflect(&self) -> &::protobuf::SingularPtrField<super::metapb::RegionEpoch> {
        &self.region_epoch
    }

    fn mut_region_epoch_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<super::metapb::RegionEpoch> {
        &mut self.region_epoch
    }

    // .metapb.Peer target_peer = 6;

    pub fn clear_target_peer(&mut self) {
        self.target_peer.clear();
    }

    pub fn has_target_peer(&self) -> bool {
        self.target_peer.is_some()
    }

    // Param is passed by value, moved
    pub fn set_target_peer(&mut self, v: super::metapb::Peer) {
        self.target_peer = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_target_peer(&mut self) -> &mut super::metapb::Peer {
        if self.target_peer.is_none() {
            self.target_peer.set_default();
        }
        self.target_peer.as_mut().unwrap()
    }

    // Take field
    pub fn take_target_peer(&mut self) -> super::metapb::Peer {
        self.target_peer.take().unwrap_or_else(|| super::metapb::Peer::new())
    }

    pub fn get_target_peer(&self) -> &super::metapb::Peer {
        self.target_peer.as_ref().unwrap_or_else(|| super::metapb::Peer::default_instance())
    }

    fn get_target_peer_for_reflect(&self) -> &::protobuf::SingularPtrField<super::metapb::Peer> {
        &self.target_peer
    }

    fn mut_target_peer_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<super::metapb::Peer> {
        &mut self.target_peer
    }
}

impl ::protobuf::Message for RegionHeartbeatResponse {
    fn is_initialized(&self) -> bool {
        for v in &self.header {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.change_peer {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.transfer_leader {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.region_epoch {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.target_peer {
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
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.header)?;
                },
                2 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.change_peer)?;
                },
                3 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.transfer_leader)?;
                },
                4 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_uint64()?;
                    self.region_id = tmp;
                },
                5 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.region_epoch)?;
                },
                6 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.target_peer)?;
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
        if let Some(ref v) = self.header.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if let Some(ref v) = self.change_peer.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if let Some(ref v) = self.transfer_leader.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if self.region_id != 0 {
            my_size += ::protobuf::rt::value_size(4, self.region_id, ::protobuf::wire_format::WireTypeVarint);
        }
        if let Some(ref v) = self.region_epoch.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if let Some(ref v) = self.target_peer.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(ref v) = self.header.as_ref() {
            os.write_tag(1, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if let Some(ref v) = self.change_peer.as_ref() {
            os.write_tag(2, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if let Some(ref v) = self.transfer_leader.as_ref() {
            os.write_tag(3, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if self.region_id != 0 {
            os.write_uint64(4, self.region_id)?;
        }
        if let Some(ref v) = self.region_epoch.as_ref() {
            os.write_tag(5, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if let Some(ref v) = self.target_peer.as_ref() {
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

impl ::protobuf::MessageStatic for RegionHeartbeatResponse {
    fn new() -> RegionHeartbeatResponse {
        RegionHeartbeatResponse::new()
    }

    fn descriptor_static(_: ::std::option::Option<RegionHeartbeatResponse>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<ResponseHeader>>(
                    "header",
                    RegionHeartbeatResponse::get_header_for_reflect,
                    RegionHeartbeatResponse::mut_header_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<ChangePeer>>(
                    "change_peer",
                    RegionHeartbeatResponse::get_change_peer_for_reflect,
                    RegionHeartbeatResponse::mut_change_peer_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<TransferLeader>>(
                    "transfer_leader",
                    RegionHeartbeatResponse::get_transfer_leader_for_reflect,
                    RegionHeartbeatResponse::mut_transfer_leader_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "region_id",
                    RegionHeartbeatResponse::get_region_id_for_reflect,
                    RegionHeartbeatResponse::mut_region_id_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<super::metapb::RegionEpoch>>(
                    "region_epoch",
                    RegionHeartbeatResponse::get_region_epoch_for_reflect,
                    RegionHeartbeatResponse::mut_region_epoch_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<super::metapb::Peer>>(
                    "target_peer",
                    RegionHeartbeatResponse::get_target_peer_for_reflect,
                    RegionHeartbeatResponse::mut_target_peer_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<RegionHeartbeatResponse>(
                    "RegionHeartbeatResponse",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for RegionHeartbeatResponse {
    fn clear(&mut self) {
        self.clear_header();
        self.clear_change_peer();
        self.clear_transfer_leader();
        self.clear_region_id();
        self.clear_region_epoch();
        self.clear_target_peer();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for RegionHeartbeatResponse {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for RegionHeartbeatResponse {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct AskSplitRequest {
    // message fields
    pub header: ::protobuf::SingularPtrField<RequestHeader>,
    pub region: ::protobuf::SingularPtrField<super::metapb::Region>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for AskSplitRequest {}

impl AskSplitRequest {
    pub fn new() -> AskSplitRequest {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static AskSplitRequest {
        static mut instance: ::protobuf::lazy::Lazy<AskSplitRequest> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const AskSplitRequest,
        };
        unsafe {
            instance.get(AskSplitRequest::new)
        }
    }

    // .pdpb.RequestHeader header = 1;

    pub fn clear_header(&mut self) {
        self.header.clear();
    }

    pub fn has_header(&self) -> bool {
        self.header.is_some()
    }

    // Param is passed by value, moved
    pub fn set_header(&mut self, v: RequestHeader) {
        self.header = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_header(&mut self) -> &mut RequestHeader {
        if self.header.is_none() {
            self.header.set_default();
        }
        self.header.as_mut().unwrap()
    }

    // Take field
    pub fn take_header(&mut self) -> RequestHeader {
        self.header.take().unwrap_or_else(|| RequestHeader::new())
    }

    pub fn get_header(&self) -> &RequestHeader {
        self.header.as_ref().unwrap_or_else(|| RequestHeader::default_instance())
    }

    fn get_header_for_reflect(&self) -> &::protobuf::SingularPtrField<RequestHeader> {
        &self.header
    }

    fn mut_header_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<RequestHeader> {
        &mut self.header
    }

    // .metapb.Region region = 2;

    pub fn clear_region(&mut self) {
        self.region.clear();
    }

    pub fn has_region(&self) -> bool {
        self.region.is_some()
    }

    // Param is passed by value, moved
    pub fn set_region(&mut self, v: super::metapb::Region) {
        self.region = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_region(&mut self) -> &mut super::metapb::Region {
        if self.region.is_none() {
            self.region.set_default();
        }
        self.region.as_mut().unwrap()
    }

    // Take field
    pub fn take_region(&mut self) -> super::metapb::Region {
        self.region.take().unwrap_or_else(|| super::metapb::Region::new())
    }

    pub fn get_region(&self) -> &super::metapb::Region {
        self.region.as_ref().unwrap_or_else(|| super::metapb::Region::default_instance())
    }

    fn get_region_for_reflect(&self) -> &::protobuf::SingularPtrField<super::metapb::Region> {
        &self.region
    }

    fn mut_region_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<super::metapb::Region> {
        &mut self.region
    }
}

impl ::protobuf::Message for AskSplitRequest {
    fn is_initialized(&self) -> bool {
        for v in &self.header {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.region {
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
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.header)?;
                },
                2 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.region)?;
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
        if let Some(ref v) = self.header.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if let Some(ref v) = self.region.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(ref v) = self.header.as_ref() {
            os.write_tag(1, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if let Some(ref v) = self.region.as_ref() {
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

impl ::protobuf::MessageStatic for AskSplitRequest {
    fn new() -> AskSplitRequest {
        AskSplitRequest::new()
    }

    fn descriptor_static(_: ::std::option::Option<AskSplitRequest>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<RequestHeader>>(
                    "header",
                    AskSplitRequest::get_header_for_reflect,
                    AskSplitRequest::mut_header_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<super::metapb::Region>>(
                    "region",
                    AskSplitRequest::get_region_for_reflect,
                    AskSplitRequest::mut_region_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<AskSplitRequest>(
                    "AskSplitRequest",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for AskSplitRequest {
    fn clear(&mut self) {
        self.clear_header();
        self.clear_region();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for AskSplitRequest {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for AskSplitRequest {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct AskSplitResponse {
    // message fields
    pub header: ::protobuf::SingularPtrField<ResponseHeader>,
    pub new_region_id: u64,
    pub new_peer_ids: ::std::vec::Vec<u64>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for AskSplitResponse {}

impl AskSplitResponse {
    pub fn new() -> AskSplitResponse {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static AskSplitResponse {
        static mut instance: ::protobuf::lazy::Lazy<AskSplitResponse> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const AskSplitResponse,
        };
        unsafe {
            instance.get(AskSplitResponse::new)
        }
    }

    // .pdpb.ResponseHeader header = 1;

    pub fn clear_header(&mut self) {
        self.header.clear();
    }

    pub fn has_header(&self) -> bool {
        self.header.is_some()
    }

    // Param is passed by value, moved
    pub fn set_header(&mut self, v: ResponseHeader) {
        self.header = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_header(&mut self) -> &mut ResponseHeader {
        if self.header.is_none() {
            self.header.set_default();
        }
        self.header.as_mut().unwrap()
    }

    // Take field
    pub fn take_header(&mut self) -> ResponseHeader {
        self.header.take().unwrap_or_else(|| ResponseHeader::new())
    }

    pub fn get_header(&self) -> &ResponseHeader {
        self.header.as_ref().unwrap_or_else(|| ResponseHeader::default_instance())
    }

    fn get_header_for_reflect(&self) -> &::protobuf::SingularPtrField<ResponseHeader> {
        &self.header
    }

    fn mut_header_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<ResponseHeader> {
        &mut self.header
    }

    // uint64 new_region_id = 2;

    pub fn clear_new_region_id(&mut self) {
        self.new_region_id = 0;
    }

    // Param is passed by value, moved
    pub fn set_new_region_id(&mut self, v: u64) {
        self.new_region_id = v;
    }

    pub fn get_new_region_id(&self) -> u64 {
        self.new_region_id
    }

    fn get_new_region_id_for_reflect(&self) -> &u64 {
        &self.new_region_id
    }

    fn mut_new_region_id_for_reflect(&mut self) -> &mut u64 {
        &mut self.new_region_id
    }

    // repeated uint64 new_peer_ids = 3;

    pub fn clear_new_peer_ids(&mut self) {
        self.new_peer_ids.clear();
    }

    // Param is passed by value, moved
    pub fn set_new_peer_ids(&mut self, v: ::std::vec::Vec<u64>) {
        self.new_peer_ids = v;
    }

    // Mutable pointer to the field.
    pub fn mut_new_peer_ids(&mut self) -> &mut ::std::vec::Vec<u64> {
        &mut self.new_peer_ids
    }

    // Take field
    pub fn take_new_peer_ids(&mut self) -> ::std::vec::Vec<u64> {
        ::std::mem::replace(&mut self.new_peer_ids, ::std::vec::Vec::new())
    }

    pub fn get_new_peer_ids(&self) -> &[u64] {
        &self.new_peer_ids
    }

    fn get_new_peer_ids_for_reflect(&self) -> &::std::vec::Vec<u64> {
        &self.new_peer_ids
    }

    fn mut_new_peer_ids_for_reflect(&mut self) -> &mut ::std::vec::Vec<u64> {
        &mut self.new_peer_ids
    }
}

impl ::protobuf::Message for AskSplitResponse {
    fn is_initialized(&self) -> bool {
        for v in &self.header {
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
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.header)?;
                },
                2 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_uint64()?;
                    self.new_region_id = tmp;
                },
                3 => {
                    ::protobuf::rt::read_repeated_uint64_into(wire_type, is, &mut self.new_peer_ids)?;
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
        if let Some(ref v) = self.header.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if self.new_region_id != 0 {
            my_size += ::protobuf::rt::value_size(2, self.new_region_id, ::protobuf::wire_format::WireTypeVarint);
        }
        for value in &self.new_peer_ids {
            my_size += ::protobuf::rt::value_size(3, *value, ::protobuf::wire_format::WireTypeVarint);
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(ref v) = self.header.as_ref() {
            os.write_tag(1, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if self.new_region_id != 0 {
            os.write_uint64(2, self.new_region_id)?;
        }
        for v in &self.new_peer_ids {
            os.write_uint64(3, *v)?;
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

impl ::protobuf::MessageStatic for AskSplitResponse {
    fn new() -> AskSplitResponse {
        AskSplitResponse::new()
    }

    fn descriptor_static(_: ::std::option::Option<AskSplitResponse>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<ResponseHeader>>(
                    "header",
                    AskSplitResponse::get_header_for_reflect,
                    AskSplitResponse::mut_header_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "new_region_id",
                    AskSplitResponse::get_new_region_id_for_reflect,
                    AskSplitResponse::mut_new_region_id_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_vec_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "new_peer_ids",
                    AskSplitResponse::get_new_peer_ids_for_reflect,
                    AskSplitResponse::mut_new_peer_ids_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<AskSplitResponse>(
                    "AskSplitResponse",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for AskSplitResponse {
    fn clear(&mut self) {
        self.clear_header();
        self.clear_new_region_id();
        self.clear_new_peer_ids();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for AskSplitResponse {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for AskSplitResponse {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct ReportSplitRequest {
    // message fields
    pub header: ::protobuf::SingularPtrField<RequestHeader>,
    pub left: ::protobuf::SingularPtrField<super::metapb::Region>,
    pub right: ::protobuf::SingularPtrField<super::metapb::Region>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for ReportSplitRequest {}

impl ReportSplitRequest {
    pub fn new() -> ReportSplitRequest {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static ReportSplitRequest {
        static mut instance: ::protobuf::lazy::Lazy<ReportSplitRequest> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ReportSplitRequest,
        };
        unsafe {
            instance.get(ReportSplitRequest::new)
        }
    }

    // .pdpb.RequestHeader header = 1;

    pub fn clear_header(&mut self) {
        self.header.clear();
    }

    pub fn has_header(&self) -> bool {
        self.header.is_some()
    }

    // Param is passed by value, moved
    pub fn set_header(&mut self, v: RequestHeader) {
        self.header = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_header(&mut self) -> &mut RequestHeader {
        if self.header.is_none() {
            self.header.set_default();
        }
        self.header.as_mut().unwrap()
    }

    // Take field
    pub fn take_header(&mut self) -> RequestHeader {
        self.header.take().unwrap_or_else(|| RequestHeader::new())
    }

    pub fn get_header(&self) -> &RequestHeader {
        self.header.as_ref().unwrap_or_else(|| RequestHeader::default_instance())
    }

    fn get_header_for_reflect(&self) -> &::protobuf::SingularPtrField<RequestHeader> {
        &self.header
    }

    fn mut_header_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<RequestHeader> {
        &mut self.header
    }

    // .metapb.Region left = 2;

    pub fn clear_left(&mut self) {
        self.left.clear();
    }

    pub fn has_left(&self) -> bool {
        self.left.is_some()
    }

    // Param is passed by value, moved
    pub fn set_left(&mut self, v: super::metapb::Region) {
        self.left = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_left(&mut self) -> &mut super::metapb::Region {
        if self.left.is_none() {
            self.left.set_default();
        }
        self.left.as_mut().unwrap()
    }

    // Take field
    pub fn take_left(&mut self) -> super::metapb::Region {
        self.left.take().unwrap_or_else(|| super::metapb::Region::new())
    }

    pub fn get_left(&self) -> &super::metapb::Region {
        self.left.as_ref().unwrap_or_else(|| super::metapb::Region::default_instance())
    }

    fn get_left_for_reflect(&self) -> &::protobuf::SingularPtrField<super::metapb::Region> {
        &self.left
    }

    fn mut_left_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<super::metapb::Region> {
        &mut self.left
    }

    // .metapb.Region right = 3;

    pub fn clear_right(&mut self) {
        self.right.clear();
    }

    pub fn has_right(&self) -> bool {
        self.right.is_some()
    }

    // Param is passed by value, moved
    pub fn set_right(&mut self, v: super::metapb::Region) {
        self.right = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_right(&mut self) -> &mut super::metapb::Region {
        if self.right.is_none() {
            self.right.set_default();
        }
        self.right.as_mut().unwrap()
    }

    // Take field
    pub fn take_right(&mut self) -> super::metapb::Region {
        self.right.take().unwrap_or_else(|| super::metapb::Region::new())
    }

    pub fn get_right(&self) -> &super::metapb::Region {
        self.right.as_ref().unwrap_or_else(|| super::metapb::Region::default_instance())
    }

    fn get_right_for_reflect(&self) -> &::protobuf::SingularPtrField<super::metapb::Region> {
        &self.right
    }

    fn mut_right_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<super::metapb::Region> {
        &mut self.right
    }
}

impl ::protobuf::Message for ReportSplitRequest {
    fn is_initialized(&self) -> bool {
        for v in &self.header {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.left {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.right {
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
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.header)?;
                },
                2 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.left)?;
                },
                3 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.right)?;
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
        if let Some(ref v) = self.header.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if let Some(ref v) = self.left.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if let Some(ref v) = self.right.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(ref v) = self.header.as_ref() {
            os.write_tag(1, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if let Some(ref v) = self.left.as_ref() {
            os.write_tag(2, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if let Some(ref v) = self.right.as_ref() {
            os.write_tag(3, ::protobuf::wire_format::WireTypeLengthDelimited)?;
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

impl ::protobuf::MessageStatic for ReportSplitRequest {
    fn new() -> ReportSplitRequest {
        ReportSplitRequest::new()
    }

    fn descriptor_static(_: ::std::option::Option<ReportSplitRequest>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<RequestHeader>>(
                    "header",
                    ReportSplitRequest::get_header_for_reflect,
                    ReportSplitRequest::mut_header_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<super::metapb::Region>>(
                    "left",
                    ReportSplitRequest::get_left_for_reflect,
                    ReportSplitRequest::mut_left_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<super::metapb::Region>>(
                    "right",
                    ReportSplitRequest::get_right_for_reflect,
                    ReportSplitRequest::mut_right_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<ReportSplitRequest>(
                    "ReportSplitRequest",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for ReportSplitRequest {
    fn clear(&mut self) {
        self.clear_header();
        self.clear_left();
        self.clear_right();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for ReportSplitRequest {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for ReportSplitRequest {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct ReportSplitResponse {
    // message fields
    pub header: ::protobuf::SingularPtrField<ResponseHeader>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for ReportSplitResponse {}

impl ReportSplitResponse {
    pub fn new() -> ReportSplitResponse {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static ReportSplitResponse {
        static mut instance: ::protobuf::lazy::Lazy<ReportSplitResponse> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ReportSplitResponse,
        };
        unsafe {
            instance.get(ReportSplitResponse::new)
        }
    }

    // .pdpb.ResponseHeader header = 1;

    pub fn clear_header(&mut self) {
        self.header.clear();
    }

    pub fn has_header(&self) -> bool {
        self.header.is_some()
    }

    // Param is passed by value, moved
    pub fn set_header(&mut self, v: ResponseHeader) {
        self.header = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_header(&mut self) -> &mut ResponseHeader {
        if self.header.is_none() {
            self.header.set_default();
        }
        self.header.as_mut().unwrap()
    }

    // Take field
    pub fn take_header(&mut self) -> ResponseHeader {
        self.header.take().unwrap_or_else(|| ResponseHeader::new())
    }

    pub fn get_header(&self) -> &ResponseHeader {
        self.header.as_ref().unwrap_or_else(|| ResponseHeader::default_instance())
    }

    fn get_header_for_reflect(&self) -> &::protobuf::SingularPtrField<ResponseHeader> {
        &self.header
    }

    fn mut_header_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<ResponseHeader> {
        &mut self.header
    }
}

impl ::protobuf::Message for ReportSplitResponse {
    fn is_initialized(&self) -> bool {
        for v in &self.header {
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
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.header)?;
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
        if let Some(ref v) = self.header.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(ref v) = self.header.as_ref() {
            os.write_tag(1, ::protobuf::wire_format::WireTypeLengthDelimited)?;
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

impl ::protobuf::MessageStatic for ReportSplitResponse {
    fn new() -> ReportSplitResponse {
        ReportSplitResponse::new()
    }

    fn descriptor_static(_: ::std::option::Option<ReportSplitResponse>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<ResponseHeader>>(
                    "header",
                    ReportSplitResponse::get_header_for_reflect,
                    ReportSplitResponse::mut_header_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<ReportSplitResponse>(
                    "ReportSplitResponse",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for ReportSplitResponse {
    fn clear(&mut self) {
        self.clear_header();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for ReportSplitResponse {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for ReportSplitResponse {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct StoreStats {
    // message fields
    pub store_id: u64,
    pub capacity: u64,
    pub available: u64,
    pub region_count: u32,
    pub sending_snap_count: u32,
    pub receiving_snap_count: u32,
    pub start_time: u32,
    pub applying_snap_count: u32,
    pub is_busy: bool,
    pub used_size: u64,
    pub bytes_written: u64,
    pub keys_written: u64,
    pub bytes_read: u64,
    pub keys_read: u64,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for StoreStats {}

impl StoreStats {
    pub fn new() -> StoreStats {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static StoreStats {
        static mut instance: ::protobuf::lazy::Lazy<StoreStats> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const StoreStats,
        };
        unsafe {
            instance.get(StoreStats::new)
        }
    }

    // uint64 store_id = 1;

    pub fn clear_store_id(&mut self) {
        self.store_id = 0;
    }

    // Param is passed by value, moved
    pub fn set_store_id(&mut self, v: u64) {
        self.store_id = v;
    }

    pub fn get_store_id(&self) -> u64 {
        self.store_id
    }

    fn get_store_id_for_reflect(&self) -> &u64 {
        &self.store_id
    }

    fn mut_store_id_for_reflect(&mut self) -> &mut u64 {
        &mut self.store_id
    }

    // uint64 capacity = 2;

    pub fn clear_capacity(&mut self) {
        self.capacity = 0;
    }

    // Param is passed by value, moved
    pub fn set_capacity(&mut self, v: u64) {
        self.capacity = v;
    }

    pub fn get_capacity(&self) -> u64 {
        self.capacity
    }

    fn get_capacity_for_reflect(&self) -> &u64 {
        &self.capacity
    }

    fn mut_capacity_for_reflect(&mut self) -> &mut u64 {
        &mut self.capacity
    }

    // uint64 available = 3;

    pub fn clear_available(&mut self) {
        self.available = 0;
    }

    // Param is passed by value, moved
    pub fn set_available(&mut self, v: u64) {
        self.available = v;
    }

    pub fn get_available(&self) -> u64 {
        self.available
    }

    fn get_available_for_reflect(&self) -> &u64 {
        &self.available
    }

    fn mut_available_for_reflect(&mut self) -> &mut u64 {
        &mut self.available
    }

    // uint32 region_count = 4;

    pub fn clear_region_count(&mut self) {
        self.region_count = 0;
    }

    // Param is passed by value, moved
    pub fn set_region_count(&mut self, v: u32) {
        self.region_count = v;
    }

    pub fn get_region_count(&self) -> u32 {
        self.region_count
    }

    fn get_region_count_for_reflect(&self) -> &u32 {
        &self.region_count
    }

    fn mut_region_count_for_reflect(&mut self) -> &mut u32 {
        &mut self.region_count
    }

    // uint32 sending_snap_count = 5;

    pub fn clear_sending_snap_count(&mut self) {
        self.sending_snap_count = 0;
    }

    // Param is passed by value, moved
    pub fn set_sending_snap_count(&mut self, v: u32) {
        self.sending_snap_count = v;
    }

    pub fn get_sending_snap_count(&self) -> u32 {
        self.sending_snap_count
    }

    fn get_sending_snap_count_for_reflect(&self) -> &u32 {
        &self.sending_snap_count
    }

    fn mut_sending_snap_count_for_reflect(&mut self) -> &mut u32 {
        &mut self.sending_snap_count
    }

    // uint32 receiving_snap_count = 6;

    pub fn clear_receiving_snap_count(&mut self) {
        self.receiving_snap_count = 0;
    }

    // Param is passed by value, moved
    pub fn set_receiving_snap_count(&mut self, v: u32) {
        self.receiving_snap_count = v;
    }

    pub fn get_receiving_snap_count(&self) -> u32 {
        self.receiving_snap_count
    }

    fn get_receiving_snap_count_for_reflect(&self) -> &u32 {
        &self.receiving_snap_count
    }

    fn mut_receiving_snap_count_for_reflect(&mut self) -> &mut u32 {
        &mut self.receiving_snap_count
    }

    // uint32 start_time = 7;

    pub fn clear_start_time(&mut self) {
        self.start_time = 0;
    }

    // Param is passed by value, moved
    pub fn set_start_time(&mut self, v: u32) {
        self.start_time = v;
    }

    pub fn get_start_time(&self) -> u32 {
        self.start_time
    }

    fn get_start_time_for_reflect(&self) -> &u32 {
        &self.start_time
    }

    fn mut_start_time_for_reflect(&mut self) -> &mut u32 {
        &mut self.start_time
    }

    // uint32 applying_snap_count = 8;

    pub fn clear_applying_snap_count(&mut self) {
        self.applying_snap_count = 0;
    }

    // Param is passed by value, moved
    pub fn set_applying_snap_count(&mut self, v: u32) {
        self.applying_snap_count = v;
    }

    pub fn get_applying_snap_count(&self) -> u32 {
        self.applying_snap_count
    }

    fn get_applying_snap_count_for_reflect(&self) -> &u32 {
        &self.applying_snap_count
    }

    fn mut_applying_snap_count_for_reflect(&mut self) -> &mut u32 {
        &mut self.applying_snap_count
    }

    // bool is_busy = 9;

    pub fn clear_is_busy(&mut self) {
        self.is_busy = false;
    }

    // Param is passed by value, moved
    pub fn set_is_busy(&mut self, v: bool) {
        self.is_busy = v;
    }

    pub fn get_is_busy(&self) -> bool {
        self.is_busy
    }

    fn get_is_busy_for_reflect(&self) -> &bool {
        &self.is_busy
    }

    fn mut_is_busy_for_reflect(&mut self) -> &mut bool {
        &mut self.is_busy
    }

    // uint64 used_size = 10;

    pub fn clear_used_size(&mut self) {
        self.used_size = 0;
    }

    // Param is passed by value, moved
    pub fn set_used_size(&mut self, v: u64) {
        self.used_size = v;
    }

    pub fn get_used_size(&self) -> u64 {
        self.used_size
    }

    fn get_used_size_for_reflect(&self) -> &u64 {
        &self.used_size
    }

    fn mut_used_size_for_reflect(&mut self) -> &mut u64 {
        &mut self.used_size
    }

    // uint64 bytes_written = 11;

    pub fn clear_bytes_written(&mut self) {
        self.bytes_written = 0;
    }

    // Param is passed by value, moved
    pub fn set_bytes_written(&mut self, v: u64) {
        self.bytes_written = v;
    }

    pub fn get_bytes_written(&self) -> u64 {
        self.bytes_written
    }

    fn get_bytes_written_for_reflect(&self) -> &u64 {
        &self.bytes_written
    }

    fn mut_bytes_written_for_reflect(&mut self) -> &mut u64 {
        &mut self.bytes_written
    }

    // uint64 keys_written = 12;

    pub fn clear_keys_written(&mut self) {
        self.keys_written = 0;
    }

    // Param is passed by value, moved
    pub fn set_keys_written(&mut self, v: u64) {
        self.keys_written = v;
    }

    pub fn get_keys_written(&self) -> u64 {
        self.keys_written
    }

    fn get_keys_written_for_reflect(&self) -> &u64 {
        &self.keys_written
    }

    fn mut_keys_written_for_reflect(&mut self) -> &mut u64 {
        &mut self.keys_written
    }

    // uint64 bytes_read = 13;

    pub fn clear_bytes_read(&mut self) {
        self.bytes_read = 0;
    }

    // Param is passed by value, moved
    pub fn set_bytes_read(&mut self, v: u64) {
        self.bytes_read = v;
    }

    pub fn get_bytes_read(&self) -> u64 {
        self.bytes_read
    }

    fn get_bytes_read_for_reflect(&self) -> &u64 {
        &self.bytes_read
    }

    fn mut_bytes_read_for_reflect(&mut self) -> &mut u64 {
        &mut self.bytes_read
    }

    // uint64 keys_read = 14;

    pub fn clear_keys_read(&mut self) {
        self.keys_read = 0;
    }

    // Param is passed by value, moved
    pub fn set_keys_read(&mut self, v: u64) {
        self.keys_read = v;
    }

    pub fn get_keys_read(&self) -> u64 {
        self.keys_read
    }

    fn get_keys_read_for_reflect(&self) -> &u64 {
        &self.keys_read
    }

    fn mut_keys_read_for_reflect(&mut self) -> &mut u64 {
        &mut self.keys_read
    }
}

impl ::protobuf::Message for StoreStats {
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
                    self.store_id = tmp;
                },
                2 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_uint64()?;
                    self.capacity = tmp;
                },
                3 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_uint64()?;
                    self.available = tmp;
                },
                4 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_uint32()?;
                    self.region_count = tmp;
                },
                5 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_uint32()?;
                    self.sending_snap_count = tmp;
                },
                6 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_uint32()?;
                    self.receiving_snap_count = tmp;
                },
                7 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_uint32()?;
                    self.start_time = tmp;
                },
                8 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_uint32()?;
                    self.applying_snap_count = tmp;
                },
                9 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_bool()?;
                    self.is_busy = tmp;
                },
                10 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_uint64()?;
                    self.used_size = tmp;
                },
                11 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_uint64()?;
                    self.bytes_written = tmp;
                },
                12 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_uint64()?;
                    self.keys_written = tmp;
                },
                13 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_uint64()?;
                    self.bytes_read = tmp;
                },
                14 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_uint64()?;
                    self.keys_read = tmp;
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
        if self.store_id != 0 {
            my_size += ::protobuf::rt::value_size(1, self.store_id, ::protobuf::wire_format::WireTypeVarint);
        }
        if self.capacity != 0 {
            my_size += ::protobuf::rt::value_size(2, self.capacity, ::protobuf::wire_format::WireTypeVarint);
        }
        if self.available != 0 {
            my_size += ::protobuf::rt::value_size(3, self.available, ::protobuf::wire_format::WireTypeVarint);
        }
        if self.region_count != 0 {
            my_size += ::protobuf::rt::value_size(4, self.region_count, ::protobuf::wire_format::WireTypeVarint);
        }
        if self.sending_snap_count != 0 {
            my_size += ::protobuf::rt::value_size(5, self.sending_snap_count, ::protobuf::wire_format::WireTypeVarint);
        }
        if self.receiving_snap_count != 0 {
            my_size += ::protobuf::rt::value_size(6, self.receiving_snap_count, ::protobuf::wire_format::WireTypeVarint);
        }
        if self.start_time != 0 {
            my_size += ::protobuf::rt::value_size(7, self.start_time, ::protobuf::wire_format::WireTypeVarint);
        }
        if self.applying_snap_count != 0 {
            my_size += ::protobuf::rt::value_size(8, self.applying_snap_count, ::protobuf::wire_format::WireTypeVarint);
        }
        if self.is_busy != false {
            my_size += 2;
        }
        if self.used_size != 0 {
            my_size += ::protobuf::rt::value_size(10, self.used_size, ::protobuf::wire_format::WireTypeVarint);
        }
        if self.bytes_written != 0 {
            my_size += ::protobuf::rt::value_size(11, self.bytes_written, ::protobuf::wire_format::WireTypeVarint);
        }
        if self.keys_written != 0 {
            my_size += ::protobuf::rt::value_size(12, self.keys_written, ::protobuf::wire_format::WireTypeVarint);
        }
        if self.bytes_read != 0 {
            my_size += ::protobuf::rt::value_size(13, self.bytes_read, ::protobuf::wire_format::WireTypeVarint);
        }
        if self.keys_read != 0 {
            my_size += ::protobuf::rt::value_size(14, self.keys_read, ::protobuf::wire_format::WireTypeVarint);
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if self.store_id != 0 {
            os.write_uint64(1, self.store_id)?;
        }
        if self.capacity != 0 {
            os.write_uint64(2, self.capacity)?;
        }
        if self.available != 0 {
            os.write_uint64(3, self.available)?;
        }
        if self.region_count != 0 {
            os.write_uint32(4, self.region_count)?;
        }
        if self.sending_snap_count != 0 {
            os.write_uint32(5, self.sending_snap_count)?;
        }
        if self.receiving_snap_count != 0 {
            os.write_uint32(6, self.receiving_snap_count)?;
        }
        if self.start_time != 0 {
            os.write_uint32(7, self.start_time)?;
        }
        if self.applying_snap_count != 0 {
            os.write_uint32(8, self.applying_snap_count)?;
        }
        if self.is_busy != false {
            os.write_bool(9, self.is_busy)?;
        }
        if self.used_size != 0 {
            os.write_uint64(10, self.used_size)?;
        }
        if self.bytes_written != 0 {
            os.write_uint64(11, self.bytes_written)?;
        }
        if self.keys_written != 0 {
            os.write_uint64(12, self.keys_written)?;
        }
        if self.bytes_read != 0 {
            os.write_uint64(13, self.bytes_read)?;
        }
        if self.keys_read != 0 {
            os.write_uint64(14, self.keys_read)?;
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

impl ::protobuf::MessageStatic for StoreStats {
    fn new() -> StoreStats {
        StoreStats::new()
    }

    fn descriptor_static(_: ::std::option::Option<StoreStats>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "store_id",
                    StoreStats::get_store_id_for_reflect,
                    StoreStats::mut_store_id_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "capacity",
                    StoreStats::get_capacity_for_reflect,
                    StoreStats::mut_capacity_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "available",
                    StoreStats::get_available_for_reflect,
                    StoreStats::mut_available_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeUint32>(
                    "region_count",
                    StoreStats::get_region_count_for_reflect,
                    StoreStats::mut_region_count_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeUint32>(
                    "sending_snap_count",
                    StoreStats::get_sending_snap_count_for_reflect,
                    StoreStats::mut_sending_snap_count_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeUint32>(
                    "receiving_snap_count",
                    StoreStats::get_receiving_snap_count_for_reflect,
                    StoreStats::mut_receiving_snap_count_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeUint32>(
                    "start_time",
                    StoreStats::get_start_time_for_reflect,
                    StoreStats::mut_start_time_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeUint32>(
                    "applying_snap_count",
                    StoreStats::get_applying_snap_count_for_reflect,
                    StoreStats::mut_applying_snap_count_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeBool>(
                    "is_busy",
                    StoreStats::get_is_busy_for_reflect,
                    StoreStats::mut_is_busy_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "used_size",
                    StoreStats::get_used_size_for_reflect,
                    StoreStats::mut_used_size_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "bytes_written",
                    StoreStats::get_bytes_written_for_reflect,
                    StoreStats::mut_bytes_written_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "keys_written",
                    StoreStats::get_keys_written_for_reflect,
                    StoreStats::mut_keys_written_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "bytes_read",
                    StoreStats::get_bytes_read_for_reflect,
                    StoreStats::mut_bytes_read_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "keys_read",
                    StoreStats::get_keys_read_for_reflect,
                    StoreStats::mut_keys_read_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<StoreStats>(
                    "StoreStats",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for StoreStats {
    fn clear(&mut self) {
        self.clear_store_id();
        self.clear_capacity();
        self.clear_available();
        self.clear_region_count();
        self.clear_sending_snap_count();
        self.clear_receiving_snap_count();
        self.clear_start_time();
        self.clear_applying_snap_count();
        self.clear_is_busy();
        self.clear_used_size();
        self.clear_bytes_written();
        self.clear_keys_written();
        self.clear_bytes_read();
        self.clear_keys_read();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for StoreStats {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for StoreStats {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct StoreHeartbeatRequest {
    // message fields
    pub header: ::protobuf::SingularPtrField<RequestHeader>,
    pub stats: ::protobuf::SingularPtrField<StoreStats>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for StoreHeartbeatRequest {}

impl StoreHeartbeatRequest {
    pub fn new() -> StoreHeartbeatRequest {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static StoreHeartbeatRequest {
        static mut instance: ::protobuf::lazy::Lazy<StoreHeartbeatRequest> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const StoreHeartbeatRequest,
        };
        unsafe {
            instance.get(StoreHeartbeatRequest::new)
        }
    }

    // .pdpb.RequestHeader header = 1;

    pub fn clear_header(&mut self) {
        self.header.clear();
    }

    pub fn has_header(&self) -> bool {
        self.header.is_some()
    }

    // Param is passed by value, moved
    pub fn set_header(&mut self, v: RequestHeader) {
        self.header = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_header(&mut self) -> &mut RequestHeader {
        if self.header.is_none() {
            self.header.set_default();
        }
        self.header.as_mut().unwrap()
    }

    // Take field
    pub fn take_header(&mut self) -> RequestHeader {
        self.header.take().unwrap_or_else(|| RequestHeader::new())
    }

    pub fn get_header(&self) -> &RequestHeader {
        self.header.as_ref().unwrap_or_else(|| RequestHeader::default_instance())
    }

    fn get_header_for_reflect(&self) -> &::protobuf::SingularPtrField<RequestHeader> {
        &self.header
    }

    fn mut_header_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<RequestHeader> {
        &mut self.header
    }

    // .pdpb.StoreStats stats = 2;

    pub fn clear_stats(&mut self) {
        self.stats.clear();
    }

    pub fn has_stats(&self) -> bool {
        self.stats.is_some()
    }

    // Param is passed by value, moved
    pub fn set_stats(&mut self, v: StoreStats) {
        self.stats = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_stats(&mut self) -> &mut StoreStats {
        if self.stats.is_none() {
            self.stats.set_default();
        }
        self.stats.as_mut().unwrap()
    }

    // Take field
    pub fn take_stats(&mut self) -> StoreStats {
        self.stats.take().unwrap_or_else(|| StoreStats::new())
    }

    pub fn get_stats(&self) -> &StoreStats {
        self.stats.as_ref().unwrap_or_else(|| StoreStats::default_instance())
    }

    fn get_stats_for_reflect(&self) -> &::protobuf::SingularPtrField<StoreStats> {
        &self.stats
    }

    fn mut_stats_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<StoreStats> {
        &mut self.stats
    }
}

impl ::protobuf::Message for StoreHeartbeatRequest {
    fn is_initialized(&self) -> bool {
        for v in &self.header {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.stats {
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
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.header)?;
                },
                2 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.stats)?;
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
        if let Some(ref v) = self.header.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if let Some(ref v) = self.stats.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(ref v) = self.header.as_ref() {
            os.write_tag(1, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if let Some(ref v) = self.stats.as_ref() {
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

impl ::protobuf::MessageStatic for StoreHeartbeatRequest {
    fn new() -> StoreHeartbeatRequest {
        StoreHeartbeatRequest::new()
    }

    fn descriptor_static(_: ::std::option::Option<StoreHeartbeatRequest>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<RequestHeader>>(
                    "header",
                    StoreHeartbeatRequest::get_header_for_reflect,
                    StoreHeartbeatRequest::mut_header_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<StoreStats>>(
                    "stats",
                    StoreHeartbeatRequest::get_stats_for_reflect,
                    StoreHeartbeatRequest::mut_stats_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<StoreHeartbeatRequest>(
                    "StoreHeartbeatRequest",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for StoreHeartbeatRequest {
    fn clear(&mut self) {
        self.clear_header();
        self.clear_stats();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for StoreHeartbeatRequest {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for StoreHeartbeatRequest {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct StoreHeartbeatResponse {
    // message fields
    pub header: ::protobuf::SingularPtrField<ResponseHeader>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for StoreHeartbeatResponse {}

impl StoreHeartbeatResponse {
    pub fn new() -> StoreHeartbeatResponse {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static StoreHeartbeatResponse {
        static mut instance: ::protobuf::lazy::Lazy<StoreHeartbeatResponse> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const StoreHeartbeatResponse,
        };
        unsafe {
            instance.get(StoreHeartbeatResponse::new)
        }
    }

    // .pdpb.ResponseHeader header = 1;

    pub fn clear_header(&mut self) {
        self.header.clear();
    }

    pub fn has_header(&self) -> bool {
        self.header.is_some()
    }

    // Param is passed by value, moved
    pub fn set_header(&mut self, v: ResponseHeader) {
        self.header = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_header(&mut self) -> &mut ResponseHeader {
        if self.header.is_none() {
            self.header.set_default();
        }
        self.header.as_mut().unwrap()
    }

    // Take field
    pub fn take_header(&mut self) -> ResponseHeader {
        self.header.take().unwrap_or_else(|| ResponseHeader::new())
    }

    pub fn get_header(&self) -> &ResponseHeader {
        self.header.as_ref().unwrap_or_else(|| ResponseHeader::default_instance())
    }

    fn get_header_for_reflect(&self) -> &::protobuf::SingularPtrField<ResponseHeader> {
        &self.header
    }

    fn mut_header_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<ResponseHeader> {
        &mut self.header
    }
}

impl ::protobuf::Message for StoreHeartbeatResponse {
    fn is_initialized(&self) -> bool {
        for v in &self.header {
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
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.header)?;
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
        if let Some(ref v) = self.header.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(ref v) = self.header.as_ref() {
            os.write_tag(1, ::protobuf::wire_format::WireTypeLengthDelimited)?;
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

impl ::protobuf::MessageStatic for StoreHeartbeatResponse {
    fn new() -> StoreHeartbeatResponse {
        StoreHeartbeatResponse::new()
    }

    fn descriptor_static(_: ::std::option::Option<StoreHeartbeatResponse>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<ResponseHeader>>(
                    "header",
                    StoreHeartbeatResponse::get_header_for_reflect,
                    StoreHeartbeatResponse::mut_header_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<StoreHeartbeatResponse>(
                    "StoreHeartbeatResponse",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for StoreHeartbeatResponse {
    fn clear(&mut self) {
        self.clear_header();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for StoreHeartbeatResponse {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for StoreHeartbeatResponse {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(Clone,PartialEq,Eq,Debug,Hash)]
pub enum ErrorType {
    OK = 0,
    UNKNOWN = 1,
    NOT_BOOTSTRAPPED = 2,
    STORE_TOMBSTONE = 3,
    ALREADY_BOOTSTRAPPED = 4,
}

impl ::protobuf::ProtobufEnum for ErrorType {
    fn value(&self) -> i32 {
        *self as i32
    }

    fn from_i32(value: i32) -> ::std::option::Option<ErrorType> {
        match value {
            0 => ::std::option::Option::Some(ErrorType::OK),
            1 => ::std::option::Option::Some(ErrorType::UNKNOWN),
            2 => ::std::option::Option::Some(ErrorType::NOT_BOOTSTRAPPED),
            3 => ::std::option::Option::Some(ErrorType::STORE_TOMBSTONE),
            4 => ::std::option::Option::Some(ErrorType::ALREADY_BOOTSTRAPPED),
            _ => ::std::option::Option::None
        }
    }

    fn values() -> &'static [Self] {
        static values: &'static [ErrorType] = &[
            ErrorType::OK,
            ErrorType::UNKNOWN,
            ErrorType::NOT_BOOTSTRAPPED,
            ErrorType::STORE_TOMBSTONE,
            ErrorType::ALREADY_BOOTSTRAPPED,
        ];
        values
    }

    fn enum_descriptor_static(_: ::std::option::Option<ErrorType>) -> &'static ::protobuf::reflect::EnumDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::EnumDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::EnumDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                ::protobuf::reflect::EnumDescriptor::new("ErrorType", file_descriptor_proto())
            })
        }
    }
}

impl ::std::marker::Copy for ErrorType {
}

impl ::std::default::Default for ErrorType {
    fn default() -> Self {
        ErrorType::OK
    }
}

impl ::protobuf::reflect::ProtobufValue for ErrorType {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Enum(self.descriptor())
    }
}

static file_descriptor_proto_data: &'static [u8] = b"\
    \n\npdpb.proto\x12\x04pdpb\x1a\x0cmetapb.proto\x1a\reraftpb.proto\x1a\
    \x14gogoproto/gogo.proto\".\n\rRequestHeader\x12\x1d\n\ncluster_id\x18\
    \x01\x20\x01(\x04R\tclusterId\"R\n\x0eResponseHeader\x12\x1d\n\ncluster_\
    id\x18\x01\x20\x01(\x04R\tclusterId\x12!\n\x05error\x18\x02\x20\x01(\x0b\
    2\x0b.pdpb.ErrorR\x05error\"F\n\x05Error\x12#\n\x04type\x18\x01\x20\x01(\
    \x0e2\x0f.pdpb.ErrorTypeR\x04type\x12\x18\n\x07message\x18\x02\x20\x01(\
    \tR\x07message\"O\n\nTsoRequest\x12+\n\x06header\x18\x01\x20\x01(\x0b2\
    \x13.pdpb.RequestHeaderR\x06header\x12\x14\n\x05count\x18\x02\x20\x01(\r\
    R\x05count\"A\n\tTimestamp\x12\x1a\n\x08physical\x18\x01\x20\x01(\x03R\
    \x08physical\x12\x18\n\x07logical\x18\x02\x20\x01(\x03R\x07logical\"\x80\
    \x01\n\x0bTsoResponse\x12,\n\x06header\x18\x01\x20\x01(\x0b2\x14.pdpb.Re\
    sponseHeaderR\x06header\x12\x14\n\x05count\x18\x02\x20\x01(\rR\x05count\
    \x12-\n\ttimestamp\x18\x03\x20\x01(\x0b2\x0f.pdpb.TimestampR\ttimestamp\
    \"\x8c\x01\n\x10BootstrapRequest\x12+\n\x06header\x18\x01\x20\x01(\x0b2\
    \x13.pdpb.RequestHeaderR\x06header\x12#\n\x05store\x18\x02\x20\x01(\x0b2\
    \r.metapb.StoreR\x05store\x12&\n\x06region\x18\x03\x20\x01(\x0b2\x0e.met\
    apb.RegionR\x06region\"A\n\x11BootstrapResponse\x12,\n\x06header\x18\x01\
    \x20\x01(\x0b2\x14.pdpb.ResponseHeaderR\x06header\"D\n\x15IsBootstrapped\
    Request\x12+\n\x06header\x18\x01\x20\x01(\x0b2\x13.pdpb.RequestHeaderR\
    \x06header\"j\n\x16IsBootstrappedResponse\x12,\n\x06header\x18\x01\x20\
    \x01(\x0b2\x14.pdpb.ResponseHeaderR\x06header\x12\"\n\x0cbootstrapped\
    \x18\x02\x20\x01(\x08R\x0cbootstrapped\"=\n\x0eAllocIDRequest\x12+\n\x06\
    header\x18\x01\x20\x01(\x0b2\x13.pdpb.RequestHeaderR\x06header\"O\n\x0fA\
    llocIDResponse\x12,\n\x06header\x18\x01\x20\x01(\x0b2\x14.pdpb.ResponseH\
    eaderR\x06header\x12\x0e\n\x02id\x18\x02\x20\x01(\x04R\x02id\"Y\n\x0fGet\
    StoreRequest\x12+\n\x06header\x18\x01\x20\x01(\x0b2\x13.pdpb.RequestHead\
    erR\x06header\x12\x19\n\x08store_id\x18\x02\x20\x01(\x04R\x07storeId\"e\
    \n\x10GetStoreResponse\x12,\n\x06header\x18\x01\x20\x01(\x0b2\x14.pdpb.R\
    esponseHeaderR\x06header\x12#\n\x05store\x18\x02\x20\x01(\x0b2\r.metapb.\
    StoreR\x05store\"c\n\x0fPutStoreRequest\x12+\n\x06header\x18\x01\x20\x01\
    (\x0b2\x13.pdpb.RequestHeaderR\x06header\x12#\n\x05store\x18\x02\x20\x01\
    (\x0b2\r.metapb.StoreR\x05store\"@\n\x10PutStoreResponse\x12,\n\x06heade\
    r\x18\x01\x20\x01(\x0b2\x14.pdpb.ResponseHeaderR\x06header\"^\n\x10GetRe\
    gionRequest\x12+\n\x06header\x18\x01\x20\x01(\x0b2\x13.pdpb.RequestHeade\
    rR\x06header\x12\x1d\n\nregion_key\x18\x02\x20\x01(\x0cR\tregionKey\"\
    \x8f\x01\n\x11GetRegionResponse\x12,\n\x06header\x18\x01\x20\x01(\x0b2\
    \x14.pdpb.ResponseHeaderR\x06header\x12&\n\x06region\x18\x02\x20\x01(\
    \x0b2\x0e.metapb.RegionR\x06region\x12$\n\x06leader\x18\x03\x20\x01(\x0b\
    2\x0c.metapb.PeerR\x06leader\"`\n\x14GetRegionByIDRequest\x12+\n\x06head\
    er\x18\x01\x20\x01(\x0b2\x13.pdpb.RequestHeaderR\x06header\x12\x1b\n\tre\
    gion_id\x18\x02\x20\x01(\x04R\x08regionId\"F\n\x17GetClusterConfigReques\
    t\x12+\n\x06header\x18\x01\x20\x01(\x0b2\x13.pdpb.RequestHeaderR\x06head\
    er\"s\n\x18GetClusterConfigResponse\x12,\n\x06header\x18\x01\x20\x01(\
    \x0b2\x14.pdpb.ResponseHeaderR\x06header\x12)\n\x07cluster\x18\x02\x20\
    \x01(\x0b2\x0f.metapb.ClusterR\x07cluster\"q\n\x17PutClusterConfigReques\
    t\x12+\n\x06header\x18\x01\x20\x01(\x0b2\x13.pdpb.RequestHeaderR\x06head\
    er\x12)\n\x07cluster\x18\x02\x20\x01(\x0b2\x0f.metapb.ClusterR\x07cluste\
    r\"H\n\x18PutClusterConfigResponse\x12,\n\x06header\x18\x01\x20\x01(\x0b\
    2\x14.pdpb.ResponseHeaderR\x06header\"w\n\x06Member\x12\x12\n\x04name\
    \x18\x01\x20\x01(\tR\x04name\x12\x1b\n\tmember_id\x18\x02\x20\x01(\x04R\
    \x08memberId\x12\x1b\n\tpeer_urls\x18\x03\x20\x03(\tR\x08peerUrls\x12\
    \x1f\n\x0bclient_urls\x18\x04\x20\x03(\tR\nclientUrls\"@\n\x11GetMembers\
    Request\x12+\n\x06header\x18\x01\x20\x01(\x0b2\x13.pdpb.RequestHeaderR\
    \x06header\"\x90\x01\n\x12GetMembersResponse\x12,\n\x06header\x18\x01\
    \x20\x01(\x0b2\x14.pdpb.ResponseHeaderR\x06header\x12&\n\x07members\x18\
    \x02\x20\x03(\x0b2\x0c.pdpb.MemberR\x07members\x12$\n\x06leader\x18\x03\
    \x20\x01(\x0b2\x0c.pdpb.MemberR\x06leader\"P\n\tPeerStats\x12\x20\n\x04p\
    eer\x18\x01\x20\x01(\x0b2\x0c.metapb.PeerR\x04peer\x12!\n\x0cdown_second\
    s\x18\x02\x20\x01(\x04R\x0bdownSeconds\"\xa5\x03\n\x16RegionHeartbeatReq\
    uest\x12+\n\x06header\x18\x01\x20\x01(\x0b2\x13.pdpb.RequestHeaderR\x06h\
    eader\x12&\n\x06region\x18\x02\x20\x01(\x0b2\x0e.metapb.RegionR\x06regio\
    n\x12$\n\x06leader\x18\x03\x20\x01(\x0b2\x0c.metapb.PeerR\x06leader\x12.\
    \n\ndown_peers\x18\x04\x20\x03(\x0b2\x0f.pdpb.PeerStatsR\tdownPeers\x121\
    \n\rpending_peers\x18\x05\x20\x03(\x0b2\x0c.metapb.PeerR\x0cpendingPeers\
    \x12#\n\rbytes_written\x18\x06\x20\x01(\x04R\x0cbytesWritten\x12\x1d\n\n\
    bytes_read\x18\x07\x20\x01(\x04R\tbytesRead\x12!\n\x0ckeys_written\x18\
    \x08\x20\x01(\x04R\x0bkeysWritten\x12\x1b\n\tkeys_read\x18\t\x20\x01(\
    \x04R\x08keysRead\x12)\n\x10approximate_size\x18\n\x20\x01(\x04R\x0fappr\
    oximateSize\"h\n\nChangePeer\x12\x20\n\x04peer\x18\x01\x20\x01(\x0b2\x0c\
    .metapb.PeerR\x04peer\x128\n\x0bchange_type\x18\x02\x20\x01(\x0e2\x17.er\
    aftpb.ConfChangeTypeR\nchangeType\"2\n\x0eTransferLeader\x12\x20\n\x04pe\
    er\x18\x01\x20\x01(\x0b2\x0c.metapb.PeerR\x04peer\"\xbd\x02\n\x17RegionH\
    eartbeatResponse\x12,\n\x06header\x18\x01\x20\x01(\x0b2\x14.pdpb.Respons\
    eHeaderR\x06header\x121\n\x0bchange_peer\x18\x02\x20\x01(\x0b2\x10.pdpb.\
    ChangePeerR\nchangePeer\x12=\n\x0ftransfer_leader\x18\x03\x20\x01(\x0b2\
    \x14.pdpb.TransferLeaderR\x0etransferLeader\x12\x1b\n\tregion_id\x18\x04\
    \x20\x01(\x04R\x08regionId\x126\n\x0cregion_epoch\x18\x05\x20\x01(\x0b2\
    \x13.metapb.RegionEpochR\x0bregionEpoch\x12-\n\x0btarget_peer\x18\x06\
    \x20\x01(\x0b2\x0c.metapb.PeerR\ntargetPeer\"f\n\x0fAskSplitRequest\x12+\
    \n\x06header\x18\x01\x20\x01(\x0b2\x13.pdpb.RequestHeaderR\x06header\x12\
    &\n\x06region\x18\x02\x20\x01(\x0b2\x0e.metapb.RegionR\x06region\"\x86\
    \x01\n\x10AskSplitResponse\x12,\n\x06header\x18\x01\x20\x01(\x0b2\x14.pd\
    pb.ResponseHeaderR\x06header\x12\"\n\rnew_region_id\x18\x02\x20\x01(\x04\
    R\x0bnewRegionId\x12\x20\n\x0cnew_peer_ids\x18\x03\x20\x03(\x04R\nnewPee\
    rIds\"\x8b\x01\n\x12ReportSplitRequest\x12+\n\x06header\x18\x01\x20\x01(\
    \x0b2\x13.pdpb.RequestHeaderR\x06header\x12\"\n\x04left\x18\x02\x20\x01(\
    \x0b2\x0e.metapb.RegionR\x04left\x12$\n\x05right\x18\x03\x20\x01(\x0b2\
    \x0e.metapb.RegionR\x05right\"C\n\x13ReportSplitResponse\x12,\n\x06heade\
    r\x18\x01\x20\x01(\x0b2\x14.pdpb.ResponseHeaderR\x06header\"\xed\x03\n\n\
    StoreStats\x12\x19\n\x08store_id\x18\x01\x20\x01(\x04R\x07storeId\x12\
    \x1a\n\x08capacity\x18\x02\x20\x01(\x04R\x08capacity\x12\x1c\n\tavailabl\
    e\x18\x03\x20\x01(\x04R\tavailable\x12!\n\x0cregion_count\x18\x04\x20\
    \x01(\rR\x0bregionCount\x12,\n\x12sending_snap_count\x18\x05\x20\x01(\rR\
    \x10sendingSnapCount\x120\n\x14receiving_snap_count\x18\x06\x20\x01(\rR\
    \x12receivingSnapCount\x12\x1d\n\nstart_time\x18\x07\x20\x01(\rR\tstartT\
    ime\x12.\n\x13applying_snap_count\x18\x08\x20\x01(\rR\x11applyingSnapCou\
    nt\x12\x17\n\x07is_busy\x18\t\x20\x01(\x08R\x06isBusy\x12\x1b\n\tused_si\
    ze\x18\n\x20\x01(\x04R\x08usedSize\x12#\n\rbytes_written\x18\x0b\x20\x01\
    (\x04R\x0cbytesWritten\x12!\n\x0ckeys_written\x18\x0c\x20\x01(\x04R\x0bk\
    eysWritten\x12\x1d\n\nbytes_read\x18\r\x20\x01(\x04R\tbytesRead\x12\x1b\
    \n\tkeys_read\x18\x0e\x20\x01(\x04R\x08keysRead\"l\n\x15StoreHeartbeatRe\
    quest\x12+\n\x06header\x18\x01\x20\x01(\x0b2\x13.pdpb.RequestHeaderR\x06\
    header\x12&\n\x05stats\x18\x02\x20\x01(\x0b2\x10.pdpb.StoreStatsR\x05sta\
    ts\"F\n\x16StoreHeartbeatResponse\x12,\n\x06header\x18\x01\x20\x01(\x0b2\
    \x14.pdpb.ResponseHeaderR\x06header*e\n\tErrorType\x12\x06\n\x02OK\x10\0\
    \x12\x0b\n\x07UNKNOWN\x10\x01\x12\x14\n\x10NOT_BOOTSTRAPPED\x10\x02\x12\
    \x13\n\x0fSTORE_TOMBSTONE\x10\x03\x12\x18\n\x14ALREADY_BOOTSTRAPPED\x10\
    \x042\x96\x08\n\x02PD\x12A\n\nGetMembers\x12\x17.pdpb.GetMembersRequest\
    \x1a\x18.pdpb.GetMembersResponse\"\0\x120\n\x03Tso\x12\x10.pdpb.TsoReque\
    st\x1a\x11.pdpb.TsoResponse\"\0(\x010\x01\x12>\n\tBootstrap\x12\x16.pdpb\
    .BootstrapRequest\x1a\x17.pdpb.BootstrapResponse\"\0\x12M\n\x0eIsBootstr\
    apped\x12\x1b.pdpb.IsBootstrappedRequest\x1a\x1c.pdpb.IsBootstrappedResp\
    onse\"\0\x128\n\x07AllocID\x12\x14.pdpb.AllocIDRequest\x1a\x15.pdpb.Allo\
    cIDResponse\"\0\x12;\n\x08GetStore\x12\x15.pdpb.GetStoreRequest\x1a\x16.\
    pdpb.GetStoreResponse\"\0\x12;\n\x08PutStore\x12\x15.pdpb.PutStoreReques\
    t\x1a\x16.pdpb.PutStoreResponse\"\0\x12M\n\x0eStoreHeartbeat\x12\x1b.pdp\
    b.StoreHeartbeatRequest\x1a\x1c.pdpb.StoreHeartbeatResponse\"\0\x12T\n\
    \x0fRegionHeartbeat\x12\x1c.pdpb.RegionHeartbeatRequest\x1a\x1d.pdpb.Reg\
    ionHeartbeatResponse\"\0(\x010\x01\x12>\n\tGetRegion\x12\x16.pdpb.GetReg\
    ionRequest\x1a\x17.pdpb.GetRegionResponse\"\0\x12F\n\rGetRegionByID\x12\
    \x1a.pdpb.GetRegionByIDRequest\x1a\x17.pdpb.GetRegionResponse\"\0\x12;\n\
    \x08AskSplit\x12\x15.pdpb.AskSplitRequest\x1a\x16.pdpb.AskSplitResponse\
    \"\0\x12D\n\x0bReportSplit\x12\x18.pdpb.ReportSplitRequest\x1a\x19.pdpb.\
    ReportSplitResponse\"\0\x12S\n\x10GetClusterConfig\x12\x1d.pdpb.GetClust\
    erConfigRequest\x1a\x1e.pdpb.GetClusterConfigResponse\"\0\x12S\n\x10PutC\
    lusterConfig\x12\x1d.pdpb.PutClusterConfigRequest\x1a\x1e.pdpb.PutCluste\
    rConfigResponse\"\0B&\n\x18com.pingcap.tikv.kvproto\xe0\xe2\x1e\x01\xc8\
    \xe2\x1e\x01\xd0\xe2\x1e\x01J\xa9^\n\x07\x12\x05\0\0\xcc\x02\x01\n\x08\n\
    \x01\x0c\x12\x03\0\0\x12\n\x08\n\x01\x02\x12\x03\x01\x08\x0c\n\t\n\x02\
    \x03\0\x12\x03\x03\x07\x15\n\t\n\x02\x03\x01\x12\x03\x04\x07\x16\n\t\n\
    \x02\x03\x02\x12\x03\x06\x07\x1d\n\x08\n\x01\x08\x12\x03\x08\0$\n\x0b\n\
    \x04\x08\xe7\x07\0\x12\x03\x08\0$\n\x0c\n\x05\x08\xe7\x07\0\x02\x12\x03\
    \x08\x07\x1c\n\r\n\x06\x08\xe7\x07\0\x02\0\x12\x03\x08\x07\x1c\n\x0e\n\
    \x07\x08\xe7\x07\0\x02\0\x01\x12\x03\x08\x08\x1b\n\x0c\n\x05\x08\xe7\x07\
    \0\x03\x12\x03\x08\x1f#\n\x08\n\x01\x08\x12\x03\t\0(\n\x0b\n\x04\x08\xe7\
    \x07\x01\x12\x03\t\0(\n\x0c\n\x05\x08\xe7\x07\x01\x02\x12\x03\t\x07\x20\
    \n\r\n\x06\x08\xe7\x07\x01\x02\0\x12\x03\t\x07\x20\n\x0e\n\x07\x08\xe7\
    \x07\x01\x02\0\x01\x12\x03\t\x08\x1f\n\x0c\n\x05\x08\xe7\x07\x01\x03\x12\
    \x03\t#'\n\x08\n\x01\x08\x12\x03\n\0*\n\x0b\n\x04\x08\xe7\x07\x02\x12\
    \x03\n\0*\n\x0c\n\x05\x08\xe7\x07\x02\x02\x12\x03\n\x07\"\n\r\n\x06\x08\
    \xe7\x07\x02\x02\0\x12\x03\n\x07\"\n\x0e\n\x07\x08\xe7\x07\x02\x02\0\x01\
    \x12\x03\n\x08!\n\x0c\n\x05\x08\xe7\x07\x02\x03\x12\x03\n%)\n\x08\n\x01\
    \x08\x12\x03\x0c\01\n\x0b\n\x04\x08\xe7\x07\x03\x12\x03\x0c\01\n\x0c\n\
    \x05\x08\xe7\x07\x03\x02\x12\x03\x0c\x07\x13\n\r\n\x06\x08\xe7\x07\x03\
    \x02\0\x12\x03\x0c\x07\x13\n\x0e\n\x07\x08\xe7\x07\x03\x02\0\x01\x12\x03\
    \x0c\x07\x13\n\x0c\n\x05\x08\xe7\x07\x03\x07\x12\x03\x0c\x160\n\n\n\x02\
    \x06\0\x12\x04\x0e\0.\x01\n\n\n\x03\x06\0\x01\x12\x03\x0e\x08\n\n\x8c\
    \x01\n\x04\x06\0\x02\0\x12\x03\x11\x04E\x1a\x7f\x20GetMembers\x20get\x20\
    the\x20member\x20list\x20of\x20this\x20cluster.\x20It\x20does\x20not\x20\
    require\n\x20the\x20cluster_id\x20in\x20request\x20matchs\x20the\x20id\
    \x20of\x20this\x20cluster.\n\n\x0c\n\x05\x06\0\x02\0\x01\x12\x03\x11\x08\
    \x12\n\x0c\n\x05\x06\0\x02\0\x02\x12\x03\x11\x13$\n\x0c\n\x05\x06\0\x02\
    \0\x03\x12\x03\x11/A\n\x0b\n\x04\x06\0\x02\x01\x12\x03\x13\x04>\n\x0c\n\
    \x05\x06\0\x02\x01\x01\x12\x03\x13\x08\x0b\n\x0c\n\x05\x06\0\x02\x01\x05\
    \x12\x03\x13\x0c\x12\n\x0c\n\x05\x06\0\x02\x01\x02\x12\x03\x13\x13\x1d\n\
    \x0c\n\x05\x06\0\x02\x01\x06\x12\x03\x13(.\n\x0c\n\x05\x06\0\x02\x01\x03\
    \x12\x03\x13/:\n\x0b\n\x04\x06\0\x02\x02\x12\x03\x15\x04B\n\x0c\n\x05\
    \x06\0\x02\x02\x01\x12\x03\x15\x08\x11\n\x0c\n\x05\x06\0\x02\x02\x02\x12\
    \x03\x15\x12\"\n\x0c\n\x05\x06\0\x02\x02\x03\x12\x03\x15->\n\x0b\n\x04\
    \x06\0\x02\x03\x12\x03\x17\x04Q\n\x0c\n\x05\x06\0\x02\x03\x01\x12\x03\
    \x17\x08\x16\n\x0c\n\x05\x06\0\x02\x03\x02\x12\x03\x17\x17,\n\x0c\n\x05\
    \x06\0\x02\x03\x03\x12\x03\x177M\n\x0b\n\x04\x06\0\x02\x04\x12\x03\x19\
    \x04<\n\x0c\n\x05\x06\0\x02\x04\x01\x12\x03\x19\x08\x0f\n\x0c\n\x05\x06\
    \0\x02\x04\x02\x12\x03\x19\x10\x1e\n\x0c\n\x05\x06\0\x02\x04\x03\x12\x03\
    \x19)8\n\x0b\n\x04\x06\0\x02\x05\x12\x03\x1b\x04?\n\x0c\n\x05\x06\0\x02\
    \x05\x01\x12\x03\x1b\x08\x10\n\x0c\n\x05\x06\0\x02\x05\x02\x12\x03\x1b\
    \x11\x20\n\x0c\n\x05\x06\0\x02\x05\x03\x12\x03\x1b+;\n\x0b\n\x04\x06\0\
    \x02\x06\x12\x03\x1d\x04?\n\x0c\n\x05\x06\0\x02\x06\x01\x12\x03\x1d\x08\
    \x10\n\x0c\n\x05\x06\0\x02\x06\x02\x12\x03\x1d\x11\x20\n\x0c\n\x05\x06\0\
    \x02\x06\x03\x12\x03\x1d+;\n\x0b\n\x04\x06\0\x02\x07\x12\x03\x1f\x04Q\n\
    \x0c\n\x05\x06\0\x02\x07\x01\x12\x03\x1f\x08\x16\n\x0c\n\x05\x06\0\x02\
    \x07\x02\x12\x03\x1f\x17,\n\x0c\n\x05\x06\0\x02\x07\x03\x12\x03\x1f7M\n\
    \x0b\n\x04\x06\0\x02\x08\x12\x03!\x04b\n\x0c\n\x05\x06\0\x02\x08\x01\x12\
    \x03!\x08\x17\n\x0c\n\x05\x06\0\x02\x08\x05\x12\x03!\x18\x1e\n\x0c\n\x05\
    \x06\0\x02\x08\x02\x12\x03!\x1f5\n\x0c\n\x05\x06\0\x02\x08\x06\x12\x03!@\
    F\n\x0c\n\x05\x06\0\x02\x08\x03\x12\x03!G^\n\x0b\n\x04\x06\0\x02\t\x12\
    \x03#\x04B\n\x0c\n\x05\x06\0\x02\t\x01\x12\x03#\x08\x11\n\x0c\n\x05\x06\
    \0\x02\t\x02\x12\x03#\x12\"\n\x0c\n\x05\x06\0\x02\t\x03\x12\x03#->\n\x0b\
    \n\x04\x06\0\x02\n\x12\x03%\x04J\n\x0c\n\x05\x06\0\x02\n\x01\x12\x03%\
    \x08\x15\n\x0c\n\x05\x06\0\x02\n\x02\x12\x03%\x16*\n\x0c\n\x05\x06\0\x02\
    \n\x03\x12\x03%5F\n\x0b\n\x04\x06\0\x02\x0b\x12\x03'\x04?\n\x0c\n\x05\
    \x06\0\x02\x0b\x01\x12\x03'\x08\x10\n\x0c\n\x05\x06\0\x02\x0b\x02\x12\
    \x03'\x11\x20\n\x0c\n\x05\x06\0\x02\x0b\x03\x12\x03'+;\n\x0b\n\x04\x06\0\
    \x02\x0c\x12\x03)\x04H\n\x0c\n\x05\x06\0\x02\x0c\x01\x12\x03)\x08\x13\n\
    \x0c\n\x05\x06\0\x02\x0c\x02\x12\x03)\x14&\n\x0c\n\x05\x06\0\x02\x0c\x03\
    \x12\x03)1D\n\x0b\n\x04\x06\0\x02\r\x12\x03+\x04W\n\x0c\n\x05\x06\0\x02\
    \r\x01\x12\x03+\x08\x18\n\x0c\n\x05\x06\0\x02\r\x02\x12\x03+\x190\n\x0c\
    \n\x05\x06\0\x02\r\x03\x12\x03+;S\n\x0b\n\x04\x06\0\x02\x0e\x12\x03-\x04\
    W\n\x0c\n\x05\x06\0\x02\x0e\x01\x12\x03-\x08\x18\n\x0c\n\x05\x06\0\x02\
    \x0e\x02\x12\x03-\x190\n\x0c\n\x05\x06\0\x02\x0e\x03\x12\x03-;S\n\n\n\
    \x02\x04\0\x12\x040\03\x01\n\n\n\x03\x04\0\x01\x12\x030\x08\x15\nD\n\x04\
    \x04\0\x02\0\x12\x032\x04\x1a\x1a7\x20cluster_id\x20is\x20the\x20ID\x20o\
    f\x20the\x20cluster\x20which\x20be\x20sent\x20to.\n\n\r\n\x05\x04\0\x02\
    \0\x04\x12\x042\x040\x17\n\x0c\n\x05\x04\0\x02\0\x05\x12\x032\x04\n\n\
    \x0c\n\x05\x04\0\x02\0\x01\x12\x032\x0b\x15\n\x0c\n\x05\x04\0\x02\0\x03\
    \x12\x032\x18\x19\n\n\n\x02\x04\x01\x12\x045\09\x01\n\n\n\x03\x04\x01\
    \x01\x12\x035\x08\x16\nK\n\x04\x04\x01\x02\0\x12\x037\x04\x1a\x1a>\x20cl\
    uster_id\x20is\x20the\x20ID\x20of\x20the\x20cluster\x20which\x20sent\x20\
    the\x20response.\n\n\r\n\x05\x04\x01\x02\0\x04\x12\x047\x045\x18\n\x0c\n\
    \x05\x04\x01\x02\0\x05\x12\x037\x04\n\n\x0c\n\x05\x04\x01\x02\0\x01\x12\
    \x037\x0b\x15\n\x0c\n\x05\x04\x01\x02\0\x03\x12\x037\x18\x19\n\x0b\n\x04\
    \x04\x01\x02\x01\x12\x038\x04\x14\n\r\n\x05\x04\x01\x02\x01\x04\x12\x048\
    \x047\x1a\n\x0c\n\x05\x04\x01\x02\x01\x06\x12\x038\x04\t\n\x0c\n\x05\x04\
    \x01\x02\x01\x01\x12\x038\n\x0f\n\x0c\n\x05\x04\x01\x02\x01\x03\x12\x038\
    \x12\x13\n\n\n\x02\x05\0\x12\x04;\0A\x01\n\n\n\x03\x05\0\x01\x12\x03;\
    \x05\x0e\n\x0b\n\x04\x05\0\x02\0\x12\x03<\x04\x0b\n\x0c\n\x05\x05\0\x02\
    \0\x01\x12\x03<\x04\x06\n\x0c\n\x05\x05\0\x02\0\x02\x12\x03<\t\n\n\x0b\n\
    \x04\x05\0\x02\x01\x12\x03=\x04\x10\n\x0c\n\x05\x05\0\x02\x01\x01\x12\
    \x03=\x04\x0b\n\x0c\n\x05\x05\0\x02\x01\x02\x12\x03=\x0e\x0f\n\x0b\n\x04\
    \x05\0\x02\x02\x12\x03>\x04\x19\n\x0c\n\x05\x05\0\x02\x02\x01\x12\x03>\
    \x04\x14\n\x0c\n\x05\x05\0\x02\x02\x02\x12\x03>\x17\x18\n\x0b\n\x04\x05\
    \0\x02\x03\x12\x03?\x04\x18\n\x0c\n\x05\x05\0\x02\x03\x01\x12\x03?\x04\
    \x13\n\x0c\n\x05\x05\0\x02\x03\x02\x12\x03?\x16\x17\n\x0b\n\x04\x05\0\
    \x02\x04\x12\x03@\x04\x1d\n\x0c\n\x05\x05\0\x02\x04\x01\x12\x03@\x04\x18\
    \n\x0c\n\x05\x05\0\x02\x04\x02\x12\x03@\x1b\x1c\n\n\n\x02\x04\x02\x12\
    \x04C\0F\x01\n\n\n\x03\x04\x02\x01\x12\x03C\x08\r\n\x0b\n\x04\x04\x02\
    \x02\0\x12\x03D\x04\x17\n\r\n\x05\x04\x02\x02\0\x04\x12\x04D\x04C\x0f\n\
    \x0c\n\x05\x04\x02\x02\0\x06\x12\x03D\x04\r\n\x0c\n\x05\x04\x02\x02\0\
    \x01\x12\x03D\x0e\x12\n\x0c\n\x05\x04\x02\x02\0\x03\x12\x03D\x15\x16\n\
    \x0b\n\x04\x04\x02\x02\x01\x12\x03E\x04\x17\n\r\n\x05\x04\x02\x02\x01\
    \x04\x12\x04E\x04D\x17\n\x0c\n\x05\x04\x02\x02\x01\x05\x12\x03E\x04\n\n\
    \x0c\n\x05\x04\x02\x02\x01\x01\x12\x03E\x0b\x12\n\x0c\n\x05\x04\x02\x02\
    \x01\x03\x12\x03E\x15\x16\n\n\n\x02\x04\x03\x12\x04H\0L\x01\n\n\n\x03\
    \x04\x03\x01\x12\x03H\x08\x12\n\x0b\n\x04\x04\x03\x02\0\x12\x03I\x04\x1d\
    \n\r\n\x05\x04\x03\x02\0\x04\x12\x04I\x04H\x14\n\x0c\n\x05\x04\x03\x02\0\
    \x06\x12\x03I\x04\x11\n\x0c\n\x05\x04\x03\x02\0\x01\x12\x03I\x12\x18\n\
    \x0c\n\x05\x04\x03\x02\0\x03\x12\x03I\x1b\x1c\n\x0b\n\x04\x04\x03\x02\
    \x01\x12\x03K\x04\x15\n\r\n\x05\x04\x03\x02\x01\x04\x12\x04K\x04I\x1d\n\
    \x0c\n\x05\x04\x03\x02\x01\x05\x12\x03K\x04\n\n\x0c\n\x05\x04\x03\x02\
    \x01\x01\x12\x03K\x0b\x10\n\x0c\n\x05\x04\x03\x02\x01\x03\x12\x03K\x13\
    \x14\n\n\n\x02\x04\x04\x12\x04N\0Q\x01\n\n\n\x03\x04\x04\x01\x12\x03N\
    \x08\x11\n\x0b\n\x04\x04\x04\x02\0\x12\x03O\x04\x17\n\r\n\x05\x04\x04\
    \x02\0\x04\x12\x04O\x04N\x13\n\x0c\n\x05\x04\x04\x02\0\x05\x12\x03O\x04\
    \t\n\x0c\n\x05\x04\x04\x02\0\x01\x12\x03O\n\x12\n\x0c\n\x05\x04\x04\x02\
    \0\x03\x12\x03O\x15\x16\n\x0b\n\x04\x04\x04\x02\x01\x12\x03P\x04\x16\n\r\
    \n\x05\x04\x04\x02\x01\x04\x12\x04P\x04O\x17\n\x0c\n\x05\x04\x04\x02\x01\
    \x05\x12\x03P\x04\t\n\x0c\n\x05\x04\x04\x02\x01\x01\x12\x03P\n\x11\n\x0c\
    \n\x05\x04\x04\x02\x01\x03\x12\x03P\x14\x15\n\n\n\x02\x04\x05\x12\x04S\0\
    X\x01\n\n\n\x03\x04\x05\x01\x12\x03S\x08\x13\n\x0b\n\x04\x04\x05\x02\0\
    \x12\x03T\x04\x1e\n\r\n\x05\x04\x05\x02\0\x04\x12\x04T\x04S\x15\n\x0c\n\
    \x05\x04\x05\x02\0\x06\x12\x03T\x04\x12\n\x0c\n\x05\x04\x05\x02\0\x01\
    \x12\x03T\x13\x19\n\x0c\n\x05\x04\x05\x02\0\x03\x12\x03T\x1c\x1d\n\x0b\n\
    \x04\x04\x05\x02\x01\x12\x03V\x04\x15\n\r\n\x05\x04\x05\x02\x01\x04\x12\
    \x04V\x04T\x1e\n\x0c\n\x05\x04\x05\x02\x01\x05\x12\x03V\x04\n\n\x0c\n\
    \x05\x04\x05\x02\x01\x01\x12\x03V\x0b\x10\n\x0c\n\x05\x04\x05\x02\x01\
    \x03\x12\x03V\x13\x14\n\x0b\n\x04\x04\x05\x02\x02\x12\x03W\x04\x1c\n\r\n\
    \x05\x04\x05\x02\x02\x04\x12\x04W\x04V\x15\n\x0c\n\x05\x04\x05\x02\x02\
    \x06\x12\x03W\x04\r\n\x0c\n\x05\x04\x05\x02\x02\x01\x12\x03W\x0e\x17\n\
    \x0c\n\x05\x04\x05\x02\x02\x03\x12\x03W\x1a\x1b\n\n\n\x02\x04\x06\x12\
    \x04Z\0_\x01\n\n\n\x03\x04\x06\x01\x12\x03Z\x08\x18\n\x0b\n\x04\x04\x06\
    \x02\0\x12\x03[\x04\x1d\n\r\n\x05\x04\x06\x02\0\x04\x12\x04[\x04Z\x1a\n\
    \x0c\n\x05\x04\x06\x02\0\x06\x12\x03[\x04\x11\n\x0c\n\x05\x04\x06\x02\0\
    \x01\x12\x03[\x12\x18\n\x0c\n\x05\x04\x06\x02\0\x03\x12\x03[\x1b\x1c\n\
    \x0b\n\x04\x04\x06\x02\x01\x12\x03]\x04\x1b\n\r\n\x05\x04\x06\x02\x01\
    \x04\x12\x04]\x04[\x1d\n\x0c\n\x05\x04\x06\x02\x01\x06\x12\x03]\x04\x10\
    \n\x0c\n\x05\x04\x06\x02\x01\x01\x12\x03]\x11\x16\n\x0c\n\x05\x04\x06\
    \x02\x01\x03\x12\x03]\x19\x1a\n\x0b\n\x04\x04\x06\x02\x02\x12\x03^\x04\
    \x1d\n\r\n\x05\x04\x06\x02\x02\x04\x12\x04^\x04]\x1b\n\x0c\n\x05\x04\x06\
    \x02\x02\x06\x12\x03^\x04\x11\n\x0c\n\x05\x04\x06\x02\x02\x01\x12\x03^\
    \x12\x18\n\x0c\n\x05\x04\x06\x02\x02\x03\x12\x03^\x1b\x1c\n\n\n\x02\x04\
    \x07\x12\x04a\0c\x01\n\n\n\x03\x04\x07\x01\x12\x03a\x08\x19\n\x0b\n\x04\
    \x04\x07\x02\0\x12\x03b\x04\x1e\n\r\n\x05\x04\x07\x02\0\x04\x12\x04b\x04\
    a\x1b\n\x0c\n\x05\x04\x07\x02\0\x06\x12\x03b\x04\x12\n\x0c\n\x05\x04\x07\
    \x02\0\x01\x12\x03b\x13\x19\n\x0c\n\x05\x04\x07\x02\0\x03\x12\x03b\x1c\
    \x1d\n\n\n\x02\x04\x08\x12\x04e\0g\x01\n\n\n\x03\x04\x08\x01\x12\x03e\
    \x08\x1d\n\x0b\n\x04\x04\x08\x02\0\x12\x03f\x04\x1d\n\r\n\x05\x04\x08\
    \x02\0\x04\x12\x04f\x04e\x1f\n\x0c\n\x05\x04\x08\x02\0\x06\x12\x03f\x04\
    \x11\n\x0c\n\x05\x04\x08\x02\0\x01\x12\x03f\x12\x18\n\x0c\n\x05\x04\x08\
    \x02\0\x03\x12\x03f\x1b\x1c\n\n\n\x02\x04\t\x12\x04i\0m\x01\n\n\n\x03\
    \x04\t\x01\x12\x03i\x08\x1e\n\x0b\n\x04\x04\t\x02\0\x12\x03j\x04\x1e\n\r\
    \n\x05\x04\t\x02\0\x04\x12\x04j\x04i\x20\n\x0c\n\x05\x04\t\x02\0\x06\x12\
    \x03j\x04\x12\n\x0c\n\x05\x04\t\x02\0\x01\x12\x03j\x13\x19\n\x0c\n\x05\
    \x04\t\x02\0\x03\x12\x03j\x1c\x1d\n\x0b\n\x04\x04\t\x02\x01\x12\x03l\x04\
    \x1a\n\r\n\x05\x04\t\x02\x01\x04\x12\x04l\x04j\x1e\n\x0c\n\x05\x04\t\x02\
    \x01\x05\x12\x03l\x04\x08\n\x0c\n\x05\x04\t\x02\x01\x01\x12\x03l\t\x15\n\
    \x0c\n\x05\x04\t\x02\x01\x03\x12\x03l\x18\x19\n\n\n\x02\x04\n\x12\x04o\0\
    q\x01\n\n\n\x03\x04\n\x01\x12\x03o\x08\x16\n\x0b\n\x04\x04\n\x02\0\x12\
    \x03p\x04\x1d\n\r\n\x05\x04\n\x02\0\x04\x12\x04p\x04o\x18\n\x0c\n\x05\
    \x04\n\x02\0\x06\x12\x03p\x04\x11\n\x0c\n\x05\x04\n\x02\0\x01\x12\x03p\
    \x12\x18\n\x0c\n\x05\x04\n\x02\0\x03\x12\x03p\x1b\x1c\n\n\n\x02\x04\x0b\
    \x12\x04s\0w\x01\n\n\n\x03\x04\x0b\x01\x12\x03s\x08\x17\n\x0b\n\x04\x04\
    \x0b\x02\0\x12\x03t\x04\x1e\n\r\n\x05\x04\x0b\x02\0\x04\x12\x04t\x04s\
    \x19\n\x0c\n\x05\x04\x0b\x02\0\x06\x12\x03t\x04\x12\n\x0c\n\x05\x04\x0b\
    \x02\0\x01\x12\x03t\x13\x19\n\x0c\n\x05\x04\x0b\x02\0\x03\x12\x03t\x1c\
    \x1d\n\x0b\n\x04\x04\x0b\x02\x01\x12\x03v\x04\x12\n\r\n\x05\x04\x0b\x02\
    \x01\x04\x12\x04v\x04t\x1e\n\x0c\n\x05\x04\x0b\x02\x01\x05\x12\x03v\x04\
    \n\n\x0c\n\x05\x04\x0b\x02\x01\x01\x12\x03v\x0b\r\n\x0c\n\x05\x04\x0b\
    \x02\x01\x03\x12\x03v\x10\x11\n\n\n\x02\x04\x0c\x12\x04y\0}\x01\n\n\n\
    \x03\x04\x0c\x01\x12\x03y\x08\x17\n\x0b\n\x04\x04\x0c\x02\0\x12\x03z\x04\
    \x1d\n\r\n\x05\x04\x0c\x02\0\x04\x12\x04z\x04y\x19\n\x0c\n\x05\x04\x0c\
    \x02\0\x06\x12\x03z\x04\x11\n\x0c\n\x05\x04\x0c\x02\0\x01\x12\x03z\x12\
    \x18\n\x0c\n\x05\x04\x0c\x02\0\x03\x12\x03z\x1b\x1c\n\x0b\n\x04\x04\x0c\
    \x02\x01\x12\x03|\x04\x18\n\r\n\x05\x04\x0c\x02\x01\x04\x12\x04|\x04z\
    \x1d\n\x0c\n\x05\x04\x0c\x02\x01\x05\x12\x03|\x04\n\n\x0c\n\x05\x04\x0c\
    \x02\x01\x01\x12\x03|\x0b\x13\n\x0c\n\x05\x04\x0c\x02\x01\x03\x12\x03|\
    \x16\x17\n\x0b\n\x02\x04\r\x12\x05\x7f\0\x83\x01\x01\n\n\n\x03\x04\r\x01\
    \x12\x03\x7f\x08\x18\n\x0c\n\x04\x04\r\x02\0\x12\x04\x80\x01\x04\x1e\n\
    \x0e\n\x05\x04\r\x02\0\x04\x12\x05\x80\x01\x04\x7f\x1a\n\r\n\x05\x04\r\
    \x02\0\x06\x12\x04\x80\x01\x04\x12\n\r\n\x05\x04\r\x02\0\x01\x12\x04\x80\
    \x01\x13\x19\n\r\n\x05\x04\r\x02\0\x03\x12\x04\x80\x01\x1c\x1d\n\x0c\n\
    \x04\x04\r\x02\x01\x12\x04\x82\x01\x04\x1b\n\x0f\n\x05\x04\r\x02\x01\x04\
    \x12\x06\x82\x01\x04\x80\x01\x1e\n\r\n\x05\x04\r\x02\x01\x06\x12\x04\x82\
    \x01\x04\x10\n\r\n\x05\x04\r\x02\x01\x01\x12\x04\x82\x01\x11\x16\n\r\n\
    \x05\x04\r\x02\x01\x03\x12\x04\x82\x01\x19\x1a\n\x0c\n\x02\x04\x0e\x12\
    \x06\x85\x01\0\x89\x01\x01\n\x0b\n\x03\x04\x0e\x01\x12\x04\x85\x01\x08\
    \x17\n\x0c\n\x04\x04\x0e\x02\0\x12\x04\x86\x01\x04\x1d\n\x0f\n\x05\x04\
    \x0e\x02\0\x04\x12\x06\x86\x01\x04\x85\x01\x19\n\r\n\x05\x04\x0e\x02\0\
    \x06\x12\x04\x86\x01\x04\x11\n\r\n\x05\x04\x0e\x02\0\x01\x12\x04\x86\x01\
    \x12\x18\n\r\n\x05\x04\x0e\x02\0\x03\x12\x04\x86\x01\x1b\x1c\n\x0c\n\x04\
    \x04\x0e\x02\x01\x12\x04\x88\x01\x04\x1b\n\x0f\n\x05\x04\x0e\x02\x01\x04\
    \x12\x06\x88\x01\x04\x86\x01\x1d\n\r\n\x05\x04\x0e\x02\x01\x06\x12\x04\
    \x88\x01\x04\x10\n\r\n\x05\x04\x0e\x02\x01\x01\x12\x04\x88\x01\x11\x16\n\
    \r\n\x05\x04\x0e\x02\x01\x03\x12\x04\x88\x01\x19\x1a\n\x0c\n\x02\x04\x0f\
    \x12\x06\x8b\x01\0\x8d\x01\x01\n\x0b\n\x03\x04\x0f\x01\x12\x04\x8b\x01\
    \x08\x18\n\x0c\n\x04\x04\x0f\x02\0\x12\x04\x8c\x01\x04\x1e\n\x0f\n\x05\
    \x04\x0f\x02\0\x04\x12\x06\x8c\x01\x04\x8b\x01\x1a\n\r\n\x05\x04\x0f\x02\
    \0\x06\x12\x04\x8c\x01\x04\x12\n\r\n\x05\x04\x0f\x02\0\x01\x12\x04\x8c\
    \x01\x13\x19\n\r\n\x05\x04\x0f\x02\0\x03\x12\x04\x8c\x01\x1c\x1d\n\x0c\n\
    \x02\x04\x10\x12\x06\x8f\x01\0\x93\x01\x01\n\x0b\n\x03\x04\x10\x01\x12\
    \x04\x8f\x01\x08\x18\n\x0c\n\x04\x04\x10\x02\0\x12\x04\x90\x01\x04\x1d\n\
    \x0f\n\x05\x04\x10\x02\0\x04\x12\x06\x90\x01\x04\x8f\x01\x1a\n\r\n\x05\
    \x04\x10\x02\0\x06\x12\x04\x90\x01\x04\x11\n\r\n\x05\x04\x10\x02\0\x01\
    \x12\x04\x90\x01\x12\x18\n\r\n\x05\x04\x10\x02\0\x03\x12\x04\x90\x01\x1b\
    \x1c\n\x0c\n\x04\x04\x10\x02\x01\x12\x04\x92\x01\x04\x19\n\x0f\n\x05\x04\
    \x10\x02\x01\x04\x12\x06\x92\x01\x04\x90\x01\x1d\n\r\n\x05\x04\x10\x02\
    \x01\x05\x12\x04\x92\x01\x04\t\n\r\n\x05\x04\x10\x02\x01\x01\x12\x04\x92\
    \x01\n\x14\n\r\n\x05\x04\x10\x02\x01\x03\x12\x04\x92\x01\x17\x18\n\x0c\n\
    \x02\x04\x11\x12\x06\x95\x01\0\x9a\x01\x01\n\x0b\n\x03\x04\x11\x01\x12\
    \x04\x95\x01\x08\x19\n\x0c\n\x04\x04\x11\x02\0\x12\x04\x96\x01\x04\x1e\n\
    \x0f\n\x05\x04\x11\x02\0\x04\x12\x06\x96\x01\x04\x95\x01\x1b\n\r\n\x05\
    \x04\x11\x02\0\x06\x12\x04\x96\x01\x04\x12\n\r\n\x05\x04\x11\x02\0\x01\
    \x12\x04\x96\x01\x13\x19\n\r\n\x05\x04\x11\x02\0\x03\x12\x04\x96\x01\x1c\
    \x1d\n\x0c\n\x04\x04\x11\x02\x01\x12\x04\x98\x01\x04\x1d\n\x0f\n\x05\x04\
    \x11\x02\x01\x04\x12\x06\x98\x01\x04\x96\x01\x1e\n\r\n\x05\x04\x11\x02\
    \x01\x06\x12\x04\x98\x01\x04\x11\n\r\n\x05\x04\x11\x02\x01\x01\x12\x04\
    \x98\x01\x12\x18\n\r\n\x05\x04\x11\x02\x01\x03\x12\x04\x98\x01\x1b\x1c\n\
    \x0c\n\x04\x04\x11\x02\x02\x12\x04\x99\x01\x04\x1b\n\x0f\n\x05\x04\x11\
    \x02\x02\x04\x12\x06\x99\x01\x04\x98\x01\x1d\n\r\n\x05\x04\x11\x02\x02\
    \x06\x12\x04\x99\x01\x04\x0f\n\r\n\x05\x04\x11\x02\x02\x01\x12\x04\x99\
    \x01\x10\x16\n\r\n\x05\x04\x11\x02\x02\x03\x12\x04\x99\x01\x19\x1a\n\x0c\
    \n\x02\x04\x12\x12\x06\x9c\x01\0\xa0\x01\x01\n\x0b\n\x03\x04\x12\x01\x12\
    \x04\x9c\x01\x08\x1c\n\x0c\n\x04\x04\x12\x02\0\x12\x04\x9d\x01\x04\x1d\n\
    \x0f\n\x05\x04\x12\x02\0\x04\x12\x06\x9d\x01\x04\x9c\x01\x1e\n\r\n\x05\
    \x04\x12\x02\0\x06\x12\x04\x9d\x01\x04\x11\n\r\n\x05\x04\x12\x02\0\x01\
    \x12\x04\x9d\x01\x12\x18\n\r\n\x05\x04\x12\x02\0\x03\x12\x04\x9d\x01\x1b\
    \x1c\n\x0c\n\x04\x04\x12\x02\x01\x12\x04\x9f\x01\x04\x19\n\x0f\n\x05\x04\
    \x12\x02\x01\x04\x12\x06\x9f\x01\x04\x9d\x01\x1d\n\r\n\x05\x04\x12\x02\
    \x01\x05\x12\x04\x9f\x01\x04\n\n\r\n\x05\x04\x12\x02\x01\x01\x12\x04\x9f\
    \x01\x0b\x14\n\r\n\x05\x04\x12\x02\x01\x03\x12\x04\x9f\x01\x17\x18\nN\n\
    \x02\x04\x13\x12\x06\xa4\x01\0\xa6\x01\x012@\x20Use\x20GetRegionResponse\
    \x20as\x20the\x20response\x20of\x20GetRegionByIDRequest.\n\n\x0b\n\x03\
    \x04\x13\x01\x12\x04\xa4\x01\x08\x1f\n\x0c\n\x04\x04\x13\x02\0\x12\x04\
    \xa5\x01\x04\x1d\n\x0f\n\x05\x04\x13\x02\0\x04\x12\x06\xa5\x01\x04\xa4\
    \x01!\n\r\n\x05\x04\x13\x02\0\x06\x12\x04\xa5\x01\x04\x11\n\r\n\x05\x04\
    \x13\x02\0\x01\x12\x04\xa5\x01\x12\x18\n\r\n\x05\x04\x13\x02\0\x03\x12\
    \x04\xa5\x01\x1b\x1c\n\x0c\n\x02\x04\x14\x12\x06\xa8\x01\0\xac\x01\x01\n\
    \x0b\n\x03\x04\x14\x01\x12\x04\xa8\x01\x08\x20\n\x0c\n\x04\x04\x14\x02\0\
    \x12\x04\xa9\x01\x04\x1e\n\x0f\n\x05\x04\x14\x02\0\x04\x12\x06\xa9\x01\
    \x04\xa8\x01\"\n\r\n\x05\x04\x14\x02\0\x06\x12\x04\xa9\x01\x04\x12\n\r\n\
    \x05\x04\x14\x02\0\x01\x12\x04\xa9\x01\x13\x19\n\r\n\x05\x04\x14\x02\0\
    \x03\x12\x04\xa9\x01\x1c\x1d\n\x0c\n\x04\x04\x14\x02\x01\x12\x04\xab\x01\
    \x04\x1f\n\x0f\n\x05\x04\x14\x02\x01\x04\x12\x06\xab\x01\x04\xa9\x01\x1e\
    \n\r\n\x05\x04\x14\x02\x01\x06\x12\x04\xab\x01\x04\x12\n\r\n\x05\x04\x14\
    \x02\x01\x01\x12\x04\xab\x01\x13\x1a\n\r\n\x05\x04\x14\x02\x01\x03\x12\
    \x04\xab\x01\x1d\x1e\n\x0c\n\x02\x04\x15\x12\x06\xae\x01\0\xb2\x01\x01\n\
    \x0b\n\x03\x04\x15\x01\x12\x04\xae\x01\x08\x1f\n\x0c\n\x04\x04\x15\x02\0\
    \x12\x04\xaf\x01\x04\x1d\n\x0f\n\x05\x04\x15\x02\0\x04\x12\x06\xaf\x01\
    \x04\xae\x01!\n\r\n\x05\x04\x15\x02\0\x06\x12\x04\xaf\x01\x04\x11\n\r\n\
    \x05\x04\x15\x02\0\x01\x12\x04\xaf\x01\x12\x18\n\r\n\x05\x04\x15\x02\0\
    \x03\x12\x04\xaf\x01\x1b\x1c\n\x0c\n\x04\x04\x15\x02\x01\x12\x04\xb1\x01\
    \x04\x1f\n\x0f\n\x05\x04\x15\x02\x01\x04\x12\x06\xb1\x01\x04\xaf\x01\x1d\
    \n\r\n\x05\x04\x15\x02\x01\x06\x12\x04\xb1\x01\x04\x12\n\r\n\x05\x04\x15\
    \x02\x01\x01\x12\x04\xb1\x01\x13\x1a\n\r\n\x05\x04\x15\x02\x01\x03\x12\
    \x04\xb1\x01\x1d\x1e\n\x0c\n\x02\x04\x16\x12\x06\xb4\x01\0\xb6\x01\x01\n\
    \x0b\n\x03\x04\x16\x01\x12\x04\xb4\x01\x08\x20\n\x0c\n\x04\x04\x16\x02\0\
    \x12\x04\xb5\x01\x04\x1e\n\x0f\n\x05\x04\x16\x02\0\x04\x12\x06\xb5\x01\
    \x04\xb4\x01\"\n\r\n\x05\x04\x16\x02\0\x06\x12\x04\xb5\x01\x04\x12\n\r\n\
    \x05\x04\x16\x02\0\x01\x12\x04\xb5\x01\x13\x19\n\r\n\x05\x04\x16\x02\0\
    \x03\x12\x04\xb5\x01\x1c\x1d\n\x0c\n\x02\x04\x17\x12\x06\xb8\x01\0\xbf\
    \x01\x01\n\x0b\n\x03\x04\x17\x01\x12\x04\xb8\x01\x08\x0e\n2\n\x04\x04\
    \x17\x02\0\x12\x04\xba\x01\x04\x14\x1a$\x20name\x20is\x20the\x20name\x20\
    of\x20the\x20PD\x20member.\n\n\x0f\n\x05\x04\x17\x02\0\x04\x12\x06\xba\
    \x01\x04\xb8\x01\x10\n\r\n\x05\x04\x17\x02\0\x05\x12\x04\xba\x01\x04\n\n\
    \r\n\x05\x04\x17\x02\0\x01\x12\x04\xba\x01\x0b\x0f\n\r\n\x05\x04\x17\x02\
    \0\x03\x12\x04\xba\x01\x12\x13\n<\n\x04\x04\x17\x02\x01\x12\x04\xbc\x01\
    \x04\x19\x1a.\x20member_id\x20is\x20the\x20unique\x20id\x20of\x20the\x20\
    PD\x20member.\n\n\x0f\n\x05\x04\x17\x02\x01\x04\x12\x06\xbc\x01\x04\xba\
    \x01\x14\n\r\n\x05\x04\x17\x02\x01\x05\x12\x04\xbc\x01\x04\n\n\r\n\x05\
    \x04\x17\x02\x01\x01\x12\x04\xbc\x01\x0b\x14\n\r\n\x05\x04\x17\x02\x01\
    \x03\x12\x04\xbc\x01\x17\x18\n\x0c\n\x04\x04\x17\x02\x02\x12\x04\xbd\x01\
    \x04\"\n\r\n\x05\x04\x17\x02\x02\x04\x12\x04\xbd\x01\x04\x0c\n\r\n\x05\
    \x04\x17\x02\x02\x05\x12\x04\xbd\x01\r\x13\n\r\n\x05\x04\x17\x02\x02\x01\
    \x12\x04\xbd\x01\x14\x1d\n\r\n\x05\x04\x17\x02\x02\x03\x12\x04\xbd\x01\
    \x20!\n\x0c\n\x04\x04\x17\x02\x03\x12\x04\xbe\x01\x04$\n\r\n\x05\x04\x17\
    \x02\x03\x04\x12\x04\xbe\x01\x04\x0c\n\r\n\x05\x04\x17\x02\x03\x05\x12\
    \x04\xbe\x01\r\x13\n\r\n\x05\x04\x17\x02\x03\x01\x12\x04\xbe\x01\x14\x1f\
    \n\r\n\x05\x04\x17\x02\x03\x03\x12\x04\xbe\x01\"#\n\x0c\n\x02\x04\x18\
    \x12\x06\xc1\x01\0\xc3\x01\x01\n\x0b\n\x03\x04\x18\x01\x12\x04\xc1\x01\
    \x08\x19\n\x0c\n\x04\x04\x18\x02\0\x12\x04\xc2\x01\x04\x1d\n\x0f\n\x05\
    \x04\x18\x02\0\x04\x12\x06\xc2\x01\x04\xc1\x01\x1b\n\r\n\x05\x04\x18\x02\
    \0\x06\x12\x04\xc2\x01\x04\x11\n\r\n\x05\x04\x18\x02\0\x01\x12\x04\xc2\
    \x01\x12\x18\n\r\n\x05\x04\x18\x02\0\x03\x12\x04\xc2\x01\x1b\x1c\n\x0c\n\
    \x02\x04\x19\x12\x06\xc5\x01\0\xca\x01\x01\n\x0b\n\x03\x04\x19\x01\x12\
    \x04\xc5\x01\x08\x1a\n\x0c\n\x04\x04\x19\x02\0\x12\x04\xc6\x01\x04\x1e\n\
    \x0f\n\x05\x04\x19\x02\0\x04\x12\x06\xc6\x01\x04\xc5\x01\x1c\n\r\n\x05\
    \x04\x19\x02\0\x06\x12\x04\xc6\x01\x04\x12\n\r\n\x05\x04\x19\x02\0\x01\
    \x12\x04\xc6\x01\x13\x19\n\r\n\x05\x04\x19\x02\0\x03\x12\x04\xc6\x01\x1c\
    \x1d\n\x0c\n\x04\x04\x19\x02\x01\x12\x04\xc8\x01\x04\x20\n\r\n\x05\x04\
    \x19\x02\x01\x04\x12\x04\xc8\x01\x04\x0c\n\r\n\x05\x04\x19\x02\x01\x06\
    \x12\x04\xc8\x01\r\x13\n\r\n\x05\x04\x19\x02\x01\x01\x12\x04\xc8\x01\x14\
    \x1b\n\r\n\x05\x04\x19\x02\x01\x03\x12\x04\xc8\x01\x1e\x1f\n\x0c\n\x04\
    \x04\x19\x02\x02\x12\x04\xc9\x01\x04\x16\n\x0f\n\x05\x04\x19\x02\x02\x04\
    \x12\x06\xc9\x01\x04\xc8\x01\x20\n\r\n\x05\x04\x19\x02\x02\x06\x12\x04\
    \xc9\x01\x04\n\n\r\n\x05\x04\x19\x02\x02\x01\x12\x04\xc9\x01\x0b\x11\n\r\
    \n\x05\x04\x19\x02\x02\x03\x12\x04\xc9\x01\x14\x15\n\x0c\n\x02\x04\x1a\
    \x12\x06\xcc\x01\0\xcf\x01\x01\n\x0b\n\x03\x04\x1a\x01\x12\x04\xcc\x01\
    \x08\x11\n\x0c\n\x04\x04\x1a\x02\0\x12\x04\xcd\x01\x04\x19\n\x0f\n\x05\
    \x04\x1a\x02\0\x04\x12\x06\xcd\x01\x04\xcc\x01\x13\n\r\n\x05\x04\x1a\x02\
    \0\x06\x12\x04\xcd\x01\x04\x0f\n\r\n\x05\x04\x1a\x02\0\x01\x12\x04\xcd\
    \x01\x10\x14\n\r\n\x05\x04\x1a\x02\0\x03\x12\x04\xcd\x01\x17\x18\n\x0c\n\
    \x04\x04\x1a\x02\x01\x12\x04\xce\x01\x04\x1c\n\x0f\n\x05\x04\x1a\x02\x01\
    \x04\x12\x06\xce\x01\x04\xcd\x01\x19\n\r\n\x05\x04\x1a\x02\x01\x05\x12\
    \x04\xce\x01\x04\n\n\r\n\x05\x04\x1a\x02\x01\x01\x12\x04\xce\x01\x0b\x17\
    \n\r\n\x05\x04\x1a\x02\x01\x03\x12\x04\xce\x01\x1a\x1b\n\x0c\n\x02\x04\
    \x1b\x12\x06\xd1\x01\0\xe4\x01\x01\n\x0b\n\x03\x04\x1b\x01\x12\x04\xd1\
    \x01\x08\x1e\n\x0c\n\x04\x04\x1b\x02\0\x12\x04\xd2\x01\x04\x1d\n\x0f\n\
    \x05\x04\x1b\x02\0\x04\x12\x06\xd2\x01\x04\xd1\x01\x20\n\r\n\x05\x04\x1b\
    \x02\0\x06\x12\x04\xd2\x01\x04\x11\n\r\n\x05\x04\x1b\x02\0\x01\x12\x04\
    \xd2\x01\x12\x18\n\r\n\x05\x04\x1b\x02\0\x03\x12\x04\xd2\x01\x1b\x1c\n\
    \x0c\n\x04\x04\x1b\x02\x01\x12\x04\xd4\x01\x04\x1d\n\x0f\n\x05\x04\x1b\
    \x02\x01\x04\x12\x06\xd4\x01\x04\xd2\x01\x1d\n\r\n\x05\x04\x1b\x02\x01\
    \x06\x12\x04\xd4\x01\x04\x11\n\r\n\x05\x04\x1b\x02\x01\x01\x12\x04\xd4\
    \x01\x12\x18\n\r\n\x05\x04\x1b\x02\x01\x03\x12\x04\xd4\x01\x1b\x1c\n2\n\
    \x04\x04\x1b\x02\x02\x12\x04\xd6\x01\x04\x1b\x1a$\x20Leader\x20Peer\x20s\
    ending\x20the\x20heartbeat.\n\n\x0f\n\x05\x04\x1b\x02\x02\x04\x12\x06\
    \xd6\x01\x04\xd4\x01\x1d\n\r\n\x05\x04\x1b\x02\x02\x06\x12\x04\xd6\x01\
    \x04\x0f\n\r\n\x05\x04\x1b\x02\x02\x01\x12\x04\xd6\x01\x10\x16\n\r\n\x05\
    \x04\x1b\x02\x02\x03\x12\x04\xd6\x01\x19\x1a\n;\n\x04\x04\x1b\x02\x03\
    \x12\x04\xd8\x01\x04&\x1a-\x20Leader\x20considers\x20that\x20these\x20pe\
    ers\x20are\x20down.\n\n\r\n\x05\x04\x1b\x02\x03\x04\x12\x04\xd8\x01\x04\
    \x0c\n\r\n\x05\x04\x1b\x02\x03\x06\x12\x04\xd8\x01\r\x16\n\r\n\x05\x04\
    \x1b\x02\x03\x01\x12\x04\xd8\x01\x17!\n\r\n\x05\x04\x1b\x02\x03\x03\x12\
    \x04\xd8\x01$%\na\n\x04\x04\x1b\x02\x04\x12\x04\xdb\x01\x04+\x1aS\x20Pen\
    ding\x20peers\x20are\x20the\x20peers\x20that\x20the\x20leader\x20can't\
    \x20consider\x20as\n\x20working\x20followers.\n\n\r\n\x05\x04\x1b\x02\
    \x04\x04\x12\x04\xdb\x01\x04\x0c\n\r\n\x05\x04\x1b\x02\x04\x06\x12\x04\
    \xdb\x01\r\x18\n\r\n\x05\x04\x1b\x02\x04\x01\x12\x04\xdb\x01\x19&\n\r\n\
    \x05\x04\x1b\x02\x04\x03\x12\x04\xdb\x01)*\n6\n\x04\x04\x1b\x02\x05\x12\
    \x04\xdd\x01\x04\x1d\x1a(\x20Bytes\x20read/written\x20during\x20this\x20\
    period.\n\n\x0f\n\x05\x04\x1b\x02\x05\x04\x12\x06\xdd\x01\x04\xdb\x01+\n\
    \r\n\x05\x04\x1b\x02\x05\x05\x12\x04\xdd\x01\x04\n\n\r\n\x05\x04\x1b\x02\
    \x05\x01\x12\x04\xdd\x01\x0b\x18\n\r\n\x05\x04\x1b\x02\x05\x03\x12\x04\
    \xdd\x01\x1b\x1c\n\x0c\n\x04\x04\x1b\x02\x06\x12\x04\xde\x01\x04\x1a\n\
    \x0f\n\x05\x04\x1b\x02\x06\x04\x12\x06\xde\x01\x04\xdd\x01\x1d\n\r\n\x05\
    \x04\x1b\x02\x06\x05\x12\x04\xde\x01\x04\n\n\r\n\x05\x04\x1b\x02\x06\x01\
    \x12\x04\xde\x01\x0b\x15\n\r\n\x05\x04\x1b\x02\x06\x03\x12\x04\xde\x01\
    \x18\x19\n5\n\x04\x04\x1b\x02\x07\x12\x04\xe0\x01\x04\x1c\x1a'\x20Keys\
    \x20read/written\x20during\x20this\x20period.\n\n\x0f\n\x05\x04\x1b\x02\
    \x07\x04\x12\x06\xe0\x01\x04\xde\x01\x1a\n\r\n\x05\x04\x1b\x02\x07\x05\
    \x12\x04\xe0\x01\x04\n\n\r\n\x05\x04\x1b\x02\x07\x01\x12\x04\xe0\x01\x0b\
    \x17\n\r\n\x05\x04\x1b\x02\x07\x03\x12\x04\xe0\x01\x1a\x1b\n\x0c\n\x04\
    \x04\x1b\x02\x08\x12\x04\xe1\x01\x04\x19\n\x0f\n\x05\x04\x1b\x02\x08\x04\
    \x12\x06\xe1\x01\x04\xe0\x01\x1c\n\r\n\x05\x04\x1b\x02\x08\x05\x12\x04\
    \xe1\x01\x04\n\n\r\n\x05\x04\x1b\x02\x08\x01\x12\x04\xe1\x01\x0b\x14\n\r\
    \n\x05\x04\x1b\x02\x08\x03\x12\x04\xe1\x01\x17\x18\n(\n\x04\x04\x1b\x02\
    \t\x12\x04\xe3\x01\x04!\x1a\x1a\x20Approximate\x20region\x20size.\n\n\
    \x0f\n\x05\x04\x1b\x02\t\x04\x12\x06\xe3\x01\x04\xe1\x01\x19\n\r\n\x05\
    \x04\x1b\x02\t\x05\x12\x04\xe3\x01\x04\n\n\r\n\x05\x04\x1b\x02\t\x01\x12\
    \x04\xe3\x01\x0b\x1b\n\r\n\x05\x04\x1b\x02\t\x03\x12\x04\xe3\x01\x1e\x20\
    \n\x0c\n\x02\x04\x1c\x12\x06\xe6\x01\0\xe9\x01\x01\n\x0b\n\x03\x04\x1c\
    \x01\x12\x04\xe6\x01\x08\x12\n\x0c\n\x04\x04\x1c\x02\0\x12\x04\xe7\x01\
    \x04\x19\n\x0f\n\x05\x04\x1c\x02\0\x04\x12\x06\xe7\x01\x04\xe6\x01\x14\n\
    \r\n\x05\x04\x1c\x02\0\x06\x12\x04\xe7\x01\x04\x0f\n\r\n\x05\x04\x1c\x02\
    \0\x01\x12\x04\xe7\x01\x10\x14\n\r\n\x05\x04\x1c\x02\0\x03\x12\x04\xe7\
    \x01\x17\x18\n\x0c\n\x04\x04\x1c\x02\x01\x12\x04\xe8\x01\x04+\n\x0f\n\
    \x05\x04\x1c\x02\x01\x04\x12\x06\xe8\x01\x04\xe7\x01\x19\n\r\n\x05\x04\
    \x1c\x02\x01\x06\x12\x04\xe8\x01\x04\x1a\n\r\n\x05\x04\x1c\x02\x01\x01\
    \x12\x04\xe8\x01\x1b&\n\r\n\x05\x04\x1c\x02\x01\x03\x12\x04\xe8\x01)*\n\
    \x0c\n\x02\x04\x1d\x12\x06\xeb\x01\0\xed\x01\x01\n\x0b\n\x03\x04\x1d\x01\
    \x12\x04\xeb\x01\x08\x16\n\x0c\n\x04\x04\x1d\x02\0\x12\x04\xec\x01\x04\
    \x19\n\x0f\n\x05\x04\x1d\x02\0\x04\x12\x06\xec\x01\x04\xeb\x01\x18\n\r\n\
    \x05\x04\x1d\x02\0\x06\x12\x04\xec\x01\x04\x0f\n\r\n\x05\x04\x1d\x02\0\
    \x01\x12\x04\xec\x01\x10\x14\n\r\n\x05\x04\x1d\x02\0\x03\x12\x04\xec\x01\
    \x17\x18\n\x0c\n\x02\x04\x1e\x12\x06\xef\x01\0\x88\x02\x01\n\x0b\n\x03\
    \x04\x1e\x01\x12\x04\xef\x01\x08\x1f\n\x0c\n\x04\x04\x1e\x02\0\x12\x04\
    \xf0\x01\x04\x1e\n\x0f\n\x05\x04\x1e\x02\0\x04\x12\x06\xf0\x01\x04\xef\
    \x01!\n\r\n\x05\x04\x1e\x02\0\x06\x12\x04\xf0\x01\x04\x12\n\r\n\x05\x04\
    \x1e\x02\0\x01\x12\x04\xf0\x01\x13\x19\n\r\n\x05\x04\x1e\x02\0\x03\x12\
    \x04\xf0\x01\x1c\x1d\n\xcf\x06\n\x04\x04\x1e\x02\x01\x12\x04\x80\x02\x04\
    \x1f\x1a\xc0\x06\x20Notice,\x20Pd\x20only\x20allows\x20handling\x20repor\
    ted\x20epoch\x20>=\x20current\x20pd's.\n\x20Leader\x20peer\x20reports\
    \x20region\x20status\x20with\x20RegionHeartbeatRequest\n\x20to\x20pd\x20\
    regularly,\x20pd\x20will\x20determine\x20whether\x20this\x20region\n\x20\
    should\x20do\x20ChangePeer\x20or\x20not.\n\x20E,g,\x20max\x20peer\x20num\
    ber\x20is\x203,\x20region\x20A,\x20first\x20only\x20peer\x201\x20in\x20A\
    .\n\x201.\x20Pd\x20region\x20state\x20->\x20Peers\x20(1),\x20ConfVer\x20\
    (1).\n\x202.\x20Leader\x20peer\x201\x20reports\x20region\x20state\x20to\
    \x20pd,\x20pd\x20finds\x20the\n\x20peer\x20number\x20is\x20<\x203,\x20so\
    \x20first\x20changes\x20its\x20current\x20region\n\x20state\x20->\x20Pee\
    rs\x20(1,\x202),\x20ConfVer\x20(1),\x20and\x20returns\x20ChangePeer\x20A\
    dding\x202.\n\x203.\x20Leader\x20does\x20ChangePeer,\x20then\x20reports\
    \x20Peers\x20(1,\x202),\x20ConfVer\x20(2),\n\x20pd\x20updates\x20its\x20\
    state\x20->\x20Peers\x20(1,\x202),\x20ConfVer\x20(2).\n\x204.\x20Leader\
    \x20may\x20report\x20old\x20Peers\x20(1),\x20ConfVer\x20(1)\x20to\x20pd\
    \x20before\x20ConfChange\n\x20finished,\x20pd\x20stills\x20responses\x20\
    ChangePeer\x20Adding\x202,\x20of\x20course,\x20we\x20must\n\x20guarantee\
    \x20the\x20second\x20ChangePeer\x20can't\x20be\x20applied\x20in\x20TiKV.\
    \n\n\x0f\n\x05\x04\x1e\x02\x01\x04\x12\x06\x80\x02\x04\xf0\x01\x1e\n\r\n\
    \x05\x04\x1e\x02\x01\x06\x12\x04\x80\x02\x04\x0e\n\r\n\x05\x04\x1e\x02\
    \x01\x01\x12\x04\x80\x02\x0f\x1a\n\r\n\x05\x04\x1e\x02\x01\x03\x12\x04\
    \x80\x02\x1d\x1e\nV\n\x04\x04\x1e\x02\x02\x12\x04\x82\x02\x04'\x1aH\x20P\
    d\x20can\x20return\x20transfer_leader\x20to\x20let\x20TiKV\x20does\x20le\
    ader\x20transfer\x20itself.\n\n\x0f\n\x05\x04\x1e\x02\x02\x04\x12\x06\
    \x82\x02\x04\x80\x02\x1f\n\r\n\x05\x04\x1e\x02\x02\x06\x12\x04\x82\x02\
    \x04\x12\n\r\n\x05\x04\x1e\x02\x02\x01\x12\x04\x82\x02\x13\"\n\r\n\x05\
    \x04\x1e\x02\x02\x03\x12\x04\x82\x02%&\n\x20\n\x04\x04\x1e\x02\x03\x12\
    \x04\x84\x02\x04\x19\x1a\x12\x20ID\x20of\x20the\x20region\n\n\x0f\n\x05\
    \x04\x1e\x02\x03\x04\x12\x06\x84\x02\x04\x82\x02'\n\r\n\x05\x04\x1e\x02\
    \x03\x05\x12\x04\x84\x02\x04\n\n\r\n\x05\x04\x1e\x02\x03\x01\x12\x04\x84\
    \x02\x0b\x14\n\r\n\x05\x04\x1e\x02\x03\x03\x12\x04\x84\x02\x17\x18\n\x0c\
    \n\x04\x04\x1e\x02\x04\x12\x04\x85\x02\x04(\n\x0f\n\x05\x04\x1e\x02\x04\
    \x04\x12\x06\x85\x02\x04\x84\x02\x19\n\r\n\x05\x04\x1e\x02\x04\x06\x12\
    \x04\x85\x02\x04\x16\n\r\n\x05\x04\x1e\x02\x04\x01\x12\x04\x85\x02\x17#\
    \n\r\n\x05\x04\x1e\x02\x04\x03\x12\x04\x85\x02&'\nY\n\x04\x04\x1e\x02\
    \x05\x12\x04\x87\x02\x04\x20\x1aK\x20Leader\x20of\x20the\x20region\x20at\
    \x20the\x20moment\x20of\x20the\x20corresponding\x20request\x20was\x20mad\
    e.\n\n\x0f\n\x05\x04\x1e\x02\x05\x04\x12\x06\x87\x02\x04\x85\x02(\n\r\n\
    \x05\x04\x1e\x02\x05\x06\x12\x04\x87\x02\x04\x0f\n\r\n\x05\x04\x1e\x02\
    \x05\x01\x12\x04\x87\x02\x10\x1b\n\r\n\x05\x04\x1e\x02\x05\x03\x12\x04\
    \x87\x02\x1e\x1f\n\x0c\n\x02\x04\x1f\x12\x06\x8a\x02\0\x8e\x02\x01\n\x0b\
    \n\x03\x04\x1f\x01\x12\x04\x8a\x02\x08\x17\n\x0c\n\x04\x04\x1f\x02\0\x12\
    \x04\x8b\x02\x04\x1d\n\x0f\n\x05\x04\x1f\x02\0\x04\x12\x06\x8b\x02\x04\
    \x8a\x02\x19\n\r\n\x05\x04\x1f\x02\0\x06\x12\x04\x8b\x02\x04\x11\n\r\n\
    \x05\x04\x1f\x02\0\x01\x12\x04\x8b\x02\x12\x18\n\r\n\x05\x04\x1f\x02\0\
    \x03\x12\x04\x8b\x02\x1b\x1c\n\x0c\n\x04\x04\x1f\x02\x01\x12\x04\x8d\x02\
    \x04\x1d\n\x0f\n\x05\x04\x1f\x02\x01\x04\x12\x06\x8d\x02\x04\x8b\x02\x1d\
    \n\r\n\x05\x04\x1f\x02\x01\x06\x12\x04\x8d\x02\x04\x11\n\r\n\x05\x04\x1f\
    \x02\x01\x01\x12\x04\x8d\x02\x12\x18\n\r\n\x05\x04\x1f\x02\x01\x03\x12\
    \x04\x8d\x02\x1b\x1c\n\x0c\n\x02\x04\x20\x12\x06\x90\x02\0\x99\x02\x01\n\
    \x0b\n\x03\x04\x20\x01\x12\x04\x90\x02\x08\x18\n\x0c\n\x04\x04\x20\x02\0\
    \x12\x04\x91\x02\x04\x1e\n\x0f\n\x05\x04\x20\x02\0\x04\x12\x06\x91\x02\
    \x04\x90\x02\x1a\n\r\n\x05\x04\x20\x02\0\x06\x12\x04\x91\x02\x04\x12\n\r\
    \n\x05\x04\x20\x02\0\x01\x12\x04\x91\x02\x13\x19\n\r\n\x05\x04\x20\x02\0\
    \x03\x12\x04\x91\x02\x1c\x1d\n\xba\x01\n\x04\x04\x20\x02\x01\x12\x04\x96\
    \x02\x04\x1d\x1a\xab\x01\x20We\x20split\x20the\x20region\x20into\x20two,\
    \x20first\x20uses\x20the\x20origin\n\x20parent\x20region\x20id,\x20and\
    \x20the\x20second\x20uses\x20the\x20new_region_id.\n\x20We\x20must\x20gu\
    arantee\x20that\x20the\x20new_region_id\x20is\x20global\x20unique.\n\n\
    \x0f\n\x05\x04\x20\x02\x01\x04\x12\x06\x96\x02\x04\x91\x02\x1e\n\r\n\x05\
    \x04\x20\x02\x01\x05\x12\x04\x96\x02\x04\n\n\r\n\x05\x04\x20\x02\x01\x01\
    \x12\x04\x96\x02\x0b\x18\n\r\n\x05\x04\x20\x02\x01\x03\x12\x04\x96\x02\
    \x1b\x1c\n6\n\x04\x04\x20\x02\x02\x12\x04\x98\x02\x04%\x1a(\x20The\x20pe\
    er\x20ids\x20for\x20the\x20new\x20split\x20region.\n\n\r\n\x05\x04\x20\
    \x02\x02\x04\x12\x04\x98\x02\x04\x0c\n\r\n\x05\x04\x20\x02\x02\x05\x12\
    \x04\x98\x02\r\x13\n\r\n\x05\x04\x20\x02\x02\x01\x12\x04\x98\x02\x14\x20\
    \n\r\n\x05\x04\x20\x02\x02\x03\x12\x04\x98\x02#$\n\x0c\n\x02\x04!\x12\
    \x06\x9b\x02\0\xa0\x02\x01\n\x0b\n\x03\x04!\x01\x12\x04\x9b\x02\x08\x1a\
    \n\x0c\n\x04\x04!\x02\0\x12\x04\x9c\x02\x04\x1d\n\x0f\n\x05\x04!\x02\0\
    \x04\x12\x06\x9c\x02\x04\x9b\x02\x1c\n\r\n\x05\x04!\x02\0\x06\x12\x04\
    \x9c\x02\x04\x11\n\r\n\x05\x04!\x02\0\x01\x12\x04\x9c\x02\x12\x18\n\r\n\
    \x05\x04!\x02\0\x03\x12\x04\x9c\x02\x1b\x1c\n\x0c\n\x04\x04!\x02\x01\x12\
    \x04\x9e\x02\x04\x1b\n\x0f\n\x05\x04!\x02\x01\x04\x12\x06\x9e\x02\x04\
    \x9c\x02\x1d\n\r\n\x05\x04!\x02\x01\x06\x12\x04\x9e\x02\x04\x11\n\r\n\
    \x05\x04!\x02\x01\x01\x12\x04\x9e\x02\x12\x16\n\r\n\x05\x04!\x02\x01\x03\
    \x12\x04\x9e\x02\x19\x1a\n\x0c\n\x04\x04!\x02\x02\x12\x04\x9f\x02\x04\
    \x1c\n\x0f\n\x05\x04!\x02\x02\x04\x12\x06\x9f\x02\x04\x9e\x02\x1b\n\r\n\
    \x05\x04!\x02\x02\x06\x12\x04\x9f\x02\x04\x11\n\r\n\x05\x04!\x02\x02\x01\
    \x12\x04\x9f\x02\x12\x17\n\r\n\x05\x04!\x02\x02\x03\x12\x04\x9f\x02\x1a\
    \x1b\n\x0c\n\x02\x04\"\x12\x06\xa2\x02\0\xa4\x02\x01\n\x0b\n\x03\x04\"\
    \x01\x12\x04\xa2\x02\x08\x1b\n\x0c\n\x04\x04\"\x02\0\x12\x04\xa3\x02\x04\
    \x1e\n\x0f\n\x05\x04\"\x02\0\x04\x12\x06\xa3\x02\x04\xa2\x02\x1d\n\r\n\
    \x05\x04\"\x02\0\x06\x12\x04\xa3\x02\x04\x12\n\r\n\x05\x04\"\x02\0\x01\
    \x12\x04\xa3\x02\x13\x19\n\r\n\x05\x04\"\x02\0\x03\x12\x04\xa3\x02\x1c\
    \x1d\n\x0c\n\x02\x04#\x12\x06\xa6\x02\0\xc2\x02\x01\n\x0b\n\x03\x04#\x01\
    \x12\x04\xa6\x02\x08\x12\n\x0c\n\x04\x04#\x02\0\x12\x04\xa7\x02\x04\x18\
    \n\x0f\n\x05\x04#\x02\0\x04\x12\x06\xa7\x02\x04\xa6\x02\x14\n\r\n\x05\
    \x04#\x02\0\x05\x12\x04\xa7\x02\x04\n\n\r\n\x05\x04#\x02\0\x01\x12\x04\
    \xa7\x02\x0b\x13\n\r\n\x05\x04#\x02\0\x03\x12\x04\xa7\x02\x16\x17\n'\n\
    \x04\x04#\x02\x01\x12\x04\xa9\x02\x04\x18\x1a\x19\x20Capacity\x20for\x20\
    the\x20store.\n\n\x0f\n\x05\x04#\x02\x01\x04\x12\x06\xa9\x02\x04\xa7\x02\
    \x18\n\r\n\x05\x04#\x02\x01\x05\x12\x04\xa9\x02\x04\n\n\r\n\x05\x04#\x02\
    \x01\x01\x12\x04\xa9\x02\x0b\x13\n\r\n\x05\x04#\x02\x01\x03\x12\x04\xa9\
    \x02\x16\x17\n-\n\x04\x04#\x02\x02\x12\x04\xab\x02\x04\x19\x1a\x1f\x20Av\
    ailable\x20size\x20for\x20the\x20store.\n\n\x0f\n\x05\x04#\x02\x02\x04\
    \x12\x06\xab\x02\x04\xa9\x02\x18\n\r\n\x05\x04#\x02\x02\x05\x12\x04\xab\
    \x02\x04\n\n\r\n\x05\x04#\x02\x02\x01\x12\x04\xab\x02\x0b\x14\n\r\n\x05\
    \x04#\x02\x02\x03\x12\x04\xab\x02\x17\x18\n1\n\x04\x04#\x02\x03\x12\x04\
    \xad\x02\x04\x1c\x1a#\x20Total\x20region\x20count\x20in\x20this\x20store\
    .\n\n\x0f\n\x05\x04#\x02\x03\x04\x12\x06\xad\x02\x04\xab\x02\x19\n\r\n\
    \x05\x04#\x02\x03\x05\x12\x04\xad\x02\x04\n\n\r\n\x05\x04#\x02\x03\x01\
    \x12\x04\xad\x02\x0b\x17\n\r\n\x05\x04#\x02\x03\x03\x12\x04\xad\x02\x1a\
    \x1b\n/\n\x04\x04#\x02\x04\x12\x04\xaf\x02\x04\"\x1a!\x20Current\x20send\
    ing\x20snapshot\x20count.\n\n\x0f\n\x05\x04#\x02\x04\x04\x12\x06\xaf\x02\
    \x04\xad\x02\x1c\n\r\n\x05\x04#\x02\x04\x05\x12\x04\xaf\x02\x04\n\n\r\n\
    \x05\x04#\x02\x04\x01\x12\x04\xaf\x02\x0b\x1d\n\r\n\x05\x04#\x02\x04\x03\
    \x12\x04\xaf\x02\x20!\n1\n\x04\x04#\x02\x05\x12\x04\xb1\x02\x04$\x1a#\
    \x20Current\x20receiving\x20snapshot\x20count.\n\n\x0f\n\x05\x04#\x02\
    \x05\x04\x12\x06\xb1\x02\x04\xaf\x02\"\n\r\n\x05\x04#\x02\x05\x05\x12\
    \x04\xb1\x02\x04\n\n\r\n\x05\x04#\x02\x05\x01\x12\x04\xb1\x02\x0b\x1f\n\
    \r\n\x05\x04#\x02\x05\x03\x12\x04\xb1\x02\"#\nF\n\x04\x04#\x02\x06\x12\
    \x04\xb3\x02\x04\x1a\x1a8\x20When\x20the\x20store\x20is\x20started\x20(u\
    nix\x20timestamp\x20in\x20seconds).\n\n\x0f\n\x05\x04#\x02\x06\x04\x12\
    \x06\xb3\x02\x04\xb1\x02$\n\r\n\x05\x04#\x02\x06\x05\x12\x04\xb3\x02\x04\
    \n\n\r\n\x05\x04#\x02\x06\x01\x12\x04\xb3\x02\x0b\x15\n\r\n\x05\x04#\x02\
    \x06\x03\x12\x04\xb3\x02\x18\x19\n5\n\x04\x04#\x02\x07\x12\x04\xb5\x02\
    \x04#\x1a'\x20How\x20many\x20region\x20is\x20applying\x20snapshot.\n\n\
    \x0f\n\x05\x04#\x02\x07\x04\x12\x06\xb5\x02\x04\xb3\x02\x1a\n\r\n\x05\
    \x04#\x02\x07\x05\x12\x04\xb5\x02\x04\n\n\r\n\x05\x04#\x02\x07\x01\x12\
    \x04\xb5\x02\x0b\x1e\n\r\n\x05\x04#\x02\x07\x03\x12\x04\xb5\x02!\"\n$\n\
    \x04\x04#\x02\x08\x12\x04\xb7\x02\x04\x15\x1a\x16\x20If\x20the\x20store\
    \x20is\x20busy\n\n\x0f\n\x05\x04#\x02\x08\x04\x12\x06\xb7\x02\x04\xb5\
    \x02#\n\r\n\x05\x04#\x02\x08\x05\x12\x04\xb7\x02\x04\x08\n\r\n\x05\x04#\
    \x02\x08\x01\x12\x04\xb7\x02\t\x10\n\r\n\x05\x04#\x02\x08\x03\x12\x04\
    \xb7\x02\x13\x14\n)\n\x04\x04#\x02\t\x12\x04\xb9\x02\x04\x1a\x1a\x1b\x20\
    Actually\x20used\x20space\x20by\x20db\n\n\x0f\n\x05\x04#\x02\t\x04\x12\
    \x06\xb9\x02\x04\xb7\x02\x15\n\r\n\x05\x04#\x02\t\x05\x12\x04\xb9\x02\
    \x04\n\n\r\n\x05\x04#\x02\t\x01\x12\x04\xb9\x02\x0b\x14\n\r\n\x05\x04#\
    \x02\t\x03\x12\x04\xb9\x02\x17\x19\n,\n\x04\x04#\x02\n\x12\x04\xbb\x02\
    \x04\x1e\x1a\x1e\x20Bytes\x20written\x20for\x20the\x20store.\n\n\x0f\n\
    \x05\x04#\x02\n\x04\x12\x06\xbb\x02\x04\xb9\x02\x1a\n\r\n\x05\x04#\x02\n\
    \x05\x12\x04\xbb\x02\x04\n\n\r\n\x05\x04#\x02\n\x01\x12\x04\xbb\x02\x0b\
    \x18\n\r\n\x05\x04#\x02\n\x03\x12\x04\xbb\x02\x1b\x1d\n+\n\x04\x04#\x02\
    \x0b\x12\x04\xbd\x02\x04\x1d\x1a\x1d\x20Keys\x20written\x20for\x20the\
    \x20store.\n\n\x0f\n\x05\x04#\x02\x0b\x04\x12\x06\xbd\x02\x04\xbb\x02\
    \x1e\n\r\n\x05\x04#\x02\x0b\x05\x12\x04\xbd\x02\x04\n\n\r\n\x05\x04#\x02\
    \x0b\x01\x12\x04\xbd\x02\x0b\x17\n\r\n\x05\x04#\x02\x0b\x03\x12\x04\xbd\
    \x02\x1a\x1c\n)\n\x04\x04#\x02\x0c\x12\x04\xbf\x02\x04\x1b\x1a\x1b\x20By\
    tes\x20read\x20for\x20the\x20store.\n\n\x0f\n\x05\x04#\x02\x0c\x04\x12\
    \x06\xbf\x02\x04\xbd\x02\x1d\n\r\n\x05\x04#\x02\x0c\x05\x12\x04\xbf\x02\
    \x04\n\n\r\n\x05\x04#\x02\x0c\x01\x12\x04\xbf\x02\x0b\x15\n\r\n\x05\x04#\
    \x02\x0c\x03\x12\x04\xbf\x02\x18\x1a\n(\n\x04\x04#\x02\r\x12\x04\xc1\x02\
    \x04\x1a\x1a\x1a\x20Keys\x20read\x20for\x20the\x20store.\n\n\x0f\n\x05\
    \x04#\x02\r\x04\x12\x06\xc1\x02\x04\xbf\x02\x1b\n\r\n\x05\x04#\x02\r\x05\
    \x12\x04\xc1\x02\x04\n\n\r\n\x05\x04#\x02\r\x01\x12\x04\xc1\x02\x0b\x14\
    \n\r\n\x05\x04#\x02\r\x03\x12\x04\xc1\x02\x17\x19\n\x0c\n\x02\x04$\x12\
    \x06\xc4\x02\0\xc8\x02\x01\n\x0b\n\x03\x04$\x01\x12\x04\xc4\x02\x08\x1d\
    \n\x0c\n\x04\x04$\x02\0\x12\x04\xc5\x02\x04\x1d\n\x0f\n\x05\x04$\x02\0\
    \x04\x12\x06\xc5\x02\x04\xc4\x02\x1f\n\r\n\x05\x04$\x02\0\x06\x12\x04\
    \xc5\x02\x04\x11\n\r\n\x05\x04$\x02\0\x01\x12\x04\xc5\x02\x12\x18\n\r\n\
    \x05\x04$\x02\0\x03\x12\x04\xc5\x02\x1b\x1c\n\x0c\n\x04\x04$\x02\x01\x12\
    \x04\xc7\x02\x04\x19\n\x0f\n\x05\x04$\x02\x01\x04\x12\x06\xc7\x02\x04\
    \xc5\x02\x1d\n\r\n\x05\x04$\x02\x01\x06\x12\x04\xc7\x02\x04\x0e\n\r\n\
    \x05\x04$\x02\x01\x01\x12\x04\xc7\x02\x0f\x14\n\r\n\x05\x04$\x02\x01\x03\
    \x12\x04\xc7\x02\x17\x18\n\x0c\n\x02\x04%\x12\x06\xca\x02\0\xcc\x02\x01\
    \n\x0b\n\x03\x04%\x01\x12\x04\xca\x02\x08\x1e\n\x0c\n\x04\x04%\x02\0\x12\
    \x04\xcb\x02\x04\x1e\n\x0f\n\x05\x04%\x02\0\x04\x12\x06\xcb\x02\x04\xca\
    \x02\x20\n\r\n\x05\x04%\x02\0\x06\x12\x04\xcb\x02\x04\x12\n\r\n\x05\x04%\
    \x02\0\x01\x12\x04\xcb\x02\x13\x19\n\r\n\x05\x04%\x02\0\x03\x12\x04\xcb\
    \x02\x1c\x1db\x06proto3\
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
