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
pub struct GetRequest {
    // message fields
    pub cf: ::std::string::String,
    pub key: ::std::vec::Vec<u8>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for GetRequest {}

impl GetRequest {
    pub fn new() -> GetRequest {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static GetRequest {
        static mut instance: ::protobuf::lazy::Lazy<GetRequest> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const GetRequest,
        };
        unsafe {
            instance.get(GetRequest::new)
        }
    }

    // string cf = 1;

    pub fn clear_cf(&mut self) {
        self.cf.clear();
    }

    // Param is passed by value, moved
    pub fn set_cf(&mut self, v: ::std::string::String) {
        self.cf = v;
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_cf(&mut self) -> &mut ::std::string::String {
        &mut self.cf
    }

    // Take field
    pub fn take_cf(&mut self) -> ::std::string::String {
        ::std::mem::replace(&mut self.cf, ::std::string::String::new())
    }

    pub fn get_cf(&self) -> &str {
        &self.cf
    }

    fn get_cf_for_reflect(&self) -> &::std::string::String {
        &self.cf
    }

    fn mut_cf_for_reflect(&mut self) -> &mut ::std::string::String {
        &mut self.cf
    }

    // bytes key = 2;

    pub fn clear_key(&mut self) {
        self.key.clear();
    }

    // Param is passed by value, moved
    pub fn set_key(&mut self, v: ::std::vec::Vec<u8>) {
        self.key = v;
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_key(&mut self) -> &mut ::std::vec::Vec<u8> {
        &mut self.key
    }

    // Take field
    pub fn take_key(&mut self) -> ::std::vec::Vec<u8> {
        ::std::mem::replace(&mut self.key, ::std::vec::Vec::new())
    }

    pub fn get_key(&self) -> &[u8] {
        &self.key
    }

    fn get_key_for_reflect(&self) -> &::std::vec::Vec<u8> {
        &self.key
    }

    fn mut_key_for_reflect(&mut self) -> &mut ::std::vec::Vec<u8> {
        &mut self.key
    }
}

impl ::protobuf::Message for GetRequest {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !is.eof()? {
            let (field_number, wire_type) = is.read_tag_unpack()?;
            match field_number {
                1 => {
                    ::protobuf::rt::read_singular_proto3_string_into(wire_type, is, &mut self.cf)?;
                },
                2 => {
                    ::protobuf::rt::read_singular_proto3_bytes_into(wire_type, is, &mut self.key)?;
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
        if !self.cf.is_empty() {
            my_size += ::protobuf::rt::string_size(1, &self.cf);
        }
        if !self.key.is_empty() {
            my_size += ::protobuf::rt::bytes_size(2, &self.key);
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if !self.cf.is_empty() {
            os.write_string(1, &self.cf)?;
        }
        if !self.key.is_empty() {
            os.write_bytes(2, &self.key)?;
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

impl ::protobuf::MessageStatic for GetRequest {
    fn new() -> GetRequest {
        GetRequest::new()
    }

    fn descriptor_static(_: ::std::option::Option<GetRequest>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeString>(
                    "cf",
                    GetRequest::get_cf_for_reflect,
                    GetRequest::mut_cf_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeBytes>(
                    "key",
                    GetRequest::get_key_for_reflect,
                    GetRequest::mut_key_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<GetRequest>(
                    "GetRequest",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for GetRequest {
    fn clear(&mut self) {
        self.clear_cf();
        self.clear_key();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for GetRequest {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for GetRequest {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct GetResponse {
    // message fields
    pub value: ::std::vec::Vec<u8>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for GetResponse {}

impl GetResponse {
    pub fn new() -> GetResponse {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static GetResponse {
        static mut instance: ::protobuf::lazy::Lazy<GetResponse> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const GetResponse,
        };
        unsafe {
            instance.get(GetResponse::new)
        }
    }

    // bytes value = 1;

    pub fn clear_value(&mut self) {
        self.value.clear();
    }

    // Param is passed by value, moved
    pub fn set_value(&mut self, v: ::std::vec::Vec<u8>) {
        self.value = v;
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_value(&mut self) -> &mut ::std::vec::Vec<u8> {
        &mut self.value
    }

    // Take field
    pub fn take_value(&mut self) -> ::std::vec::Vec<u8> {
        ::std::mem::replace(&mut self.value, ::std::vec::Vec::new())
    }

    pub fn get_value(&self) -> &[u8] {
        &self.value
    }

    fn get_value_for_reflect(&self) -> &::std::vec::Vec<u8> {
        &self.value
    }

    fn mut_value_for_reflect(&mut self) -> &mut ::std::vec::Vec<u8> {
        &mut self.value
    }
}

impl ::protobuf::Message for GetResponse {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !is.eof()? {
            let (field_number, wire_type) = is.read_tag_unpack()?;
            match field_number {
                1 => {
                    ::protobuf::rt::read_singular_proto3_bytes_into(wire_type, is, &mut self.value)?;
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
        if !self.value.is_empty() {
            my_size += ::protobuf::rt::bytes_size(1, &self.value);
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if !self.value.is_empty() {
            os.write_bytes(1, &self.value)?;
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

impl ::protobuf::MessageStatic for GetResponse {
    fn new() -> GetResponse {
        GetResponse::new()
    }

    fn descriptor_static(_: ::std::option::Option<GetResponse>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeBytes>(
                    "value",
                    GetResponse::get_value_for_reflect,
                    GetResponse::mut_value_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<GetResponse>(
                    "GetResponse",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for GetResponse {
    fn clear(&mut self) {
        self.clear_value();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for GetResponse {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for GetResponse {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct PutRequest {
    // message fields
    pub cf: ::std::string::String,
    pub key: ::std::vec::Vec<u8>,
    pub value: ::std::vec::Vec<u8>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for PutRequest {}

impl PutRequest {
    pub fn new() -> PutRequest {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static PutRequest {
        static mut instance: ::protobuf::lazy::Lazy<PutRequest> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const PutRequest,
        };
        unsafe {
            instance.get(PutRequest::new)
        }
    }

    // string cf = 1;

    pub fn clear_cf(&mut self) {
        self.cf.clear();
    }

    // Param is passed by value, moved
    pub fn set_cf(&mut self, v: ::std::string::String) {
        self.cf = v;
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_cf(&mut self) -> &mut ::std::string::String {
        &mut self.cf
    }

    // Take field
    pub fn take_cf(&mut self) -> ::std::string::String {
        ::std::mem::replace(&mut self.cf, ::std::string::String::new())
    }

    pub fn get_cf(&self) -> &str {
        &self.cf
    }

    fn get_cf_for_reflect(&self) -> &::std::string::String {
        &self.cf
    }

    fn mut_cf_for_reflect(&mut self) -> &mut ::std::string::String {
        &mut self.cf
    }

    // bytes key = 2;

    pub fn clear_key(&mut self) {
        self.key.clear();
    }

    // Param is passed by value, moved
    pub fn set_key(&mut self, v: ::std::vec::Vec<u8>) {
        self.key = v;
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_key(&mut self) -> &mut ::std::vec::Vec<u8> {
        &mut self.key
    }

    // Take field
    pub fn take_key(&mut self) -> ::std::vec::Vec<u8> {
        ::std::mem::replace(&mut self.key, ::std::vec::Vec::new())
    }

    pub fn get_key(&self) -> &[u8] {
        &self.key
    }

    fn get_key_for_reflect(&self) -> &::std::vec::Vec<u8> {
        &self.key
    }

    fn mut_key_for_reflect(&mut self) -> &mut ::std::vec::Vec<u8> {
        &mut self.key
    }

    // bytes value = 3;

    pub fn clear_value(&mut self) {
        self.value.clear();
    }

    // Param is passed by value, moved
    pub fn set_value(&mut self, v: ::std::vec::Vec<u8>) {
        self.value = v;
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_value(&mut self) -> &mut ::std::vec::Vec<u8> {
        &mut self.value
    }

    // Take field
    pub fn take_value(&mut self) -> ::std::vec::Vec<u8> {
        ::std::mem::replace(&mut self.value, ::std::vec::Vec::new())
    }

    pub fn get_value(&self) -> &[u8] {
        &self.value
    }

    fn get_value_for_reflect(&self) -> &::std::vec::Vec<u8> {
        &self.value
    }

    fn mut_value_for_reflect(&mut self) -> &mut ::std::vec::Vec<u8> {
        &mut self.value
    }
}

impl ::protobuf::Message for PutRequest {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !is.eof()? {
            let (field_number, wire_type) = is.read_tag_unpack()?;
            match field_number {
                1 => {
                    ::protobuf::rt::read_singular_proto3_string_into(wire_type, is, &mut self.cf)?;
                },
                2 => {
                    ::protobuf::rt::read_singular_proto3_bytes_into(wire_type, is, &mut self.key)?;
                },
                3 => {
                    ::protobuf::rt::read_singular_proto3_bytes_into(wire_type, is, &mut self.value)?;
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
        if !self.cf.is_empty() {
            my_size += ::protobuf::rt::string_size(1, &self.cf);
        }
        if !self.key.is_empty() {
            my_size += ::protobuf::rt::bytes_size(2, &self.key);
        }
        if !self.value.is_empty() {
            my_size += ::protobuf::rt::bytes_size(3, &self.value);
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if !self.cf.is_empty() {
            os.write_string(1, &self.cf)?;
        }
        if !self.key.is_empty() {
            os.write_bytes(2, &self.key)?;
        }
        if !self.value.is_empty() {
            os.write_bytes(3, &self.value)?;
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

impl ::protobuf::MessageStatic for PutRequest {
    fn new() -> PutRequest {
        PutRequest::new()
    }

    fn descriptor_static(_: ::std::option::Option<PutRequest>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeString>(
                    "cf",
                    PutRequest::get_cf_for_reflect,
                    PutRequest::mut_cf_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeBytes>(
                    "key",
                    PutRequest::get_key_for_reflect,
                    PutRequest::mut_key_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeBytes>(
                    "value",
                    PutRequest::get_value_for_reflect,
                    PutRequest::mut_value_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<PutRequest>(
                    "PutRequest",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for PutRequest {
    fn clear(&mut self) {
        self.clear_cf();
        self.clear_key();
        self.clear_value();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for PutRequest {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for PutRequest {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct PutResponse {
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for PutResponse {}

impl PutResponse {
    pub fn new() -> PutResponse {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static PutResponse {
        static mut instance: ::protobuf::lazy::Lazy<PutResponse> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const PutResponse,
        };
        unsafe {
            instance.get(PutResponse::new)
        }
    }
}

impl ::protobuf::Message for PutResponse {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !is.eof()? {
            let (field_number, wire_type) = is.read_tag_unpack()?;
            match field_number {
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
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
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

impl ::protobuf::MessageStatic for PutResponse {
    fn new() -> PutResponse {
        PutResponse::new()
    }

    fn descriptor_static(_: ::std::option::Option<PutResponse>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let fields = ::std::vec::Vec::new();
                ::protobuf::reflect::MessageDescriptor::new::<PutResponse>(
                    "PutResponse",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for PutResponse {
    fn clear(&mut self) {
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for PutResponse {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for PutResponse {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct DeleteRequest {
    // message fields
    pub cf: ::std::string::String,
    pub key: ::std::vec::Vec<u8>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for DeleteRequest {}

impl DeleteRequest {
    pub fn new() -> DeleteRequest {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static DeleteRequest {
        static mut instance: ::protobuf::lazy::Lazy<DeleteRequest> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const DeleteRequest,
        };
        unsafe {
            instance.get(DeleteRequest::new)
        }
    }

    // string cf = 1;

    pub fn clear_cf(&mut self) {
        self.cf.clear();
    }

    // Param is passed by value, moved
    pub fn set_cf(&mut self, v: ::std::string::String) {
        self.cf = v;
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_cf(&mut self) -> &mut ::std::string::String {
        &mut self.cf
    }

    // Take field
    pub fn take_cf(&mut self) -> ::std::string::String {
        ::std::mem::replace(&mut self.cf, ::std::string::String::new())
    }

    pub fn get_cf(&self) -> &str {
        &self.cf
    }

    fn get_cf_for_reflect(&self) -> &::std::string::String {
        &self.cf
    }

    fn mut_cf_for_reflect(&mut self) -> &mut ::std::string::String {
        &mut self.cf
    }

    // bytes key = 2;

    pub fn clear_key(&mut self) {
        self.key.clear();
    }

    // Param is passed by value, moved
    pub fn set_key(&mut self, v: ::std::vec::Vec<u8>) {
        self.key = v;
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_key(&mut self) -> &mut ::std::vec::Vec<u8> {
        &mut self.key
    }

    // Take field
    pub fn take_key(&mut self) -> ::std::vec::Vec<u8> {
        ::std::mem::replace(&mut self.key, ::std::vec::Vec::new())
    }

    pub fn get_key(&self) -> &[u8] {
        &self.key
    }

    fn get_key_for_reflect(&self) -> &::std::vec::Vec<u8> {
        &self.key
    }

    fn mut_key_for_reflect(&mut self) -> &mut ::std::vec::Vec<u8> {
        &mut self.key
    }
}

impl ::protobuf::Message for DeleteRequest {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !is.eof()? {
            let (field_number, wire_type) = is.read_tag_unpack()?;
            match field_number {
                1 => {
                    ::protobuf::rt::read_singular_proto3_string_into(wire_type, is, &mut self.cf)?;
                },
                2 => {
                    ::protobuf::rt::read_singular_proto3_bytes_into(wire_type, is, &mut self.key)?;
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
        if !self.cf.is_empty() {
            my_size += ::protobuf::rt::string_size(1, &self.cf);
        }
        if !self.key.is_empty() {
            my_size += ::protobuf::rt::bytes_size(2, &self.key);
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if !self.cf.is_empty() {
            os.write_string(1, &self.cf)?;
        }
        if !self.key.is_empty() {
            os.write_bytes(2, &self.key)?;
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

impl ::protobuf::MessageStatic for DeleteRequest {
    fn new() -> DeleteRequest {
        DeleteRequest::new()
    }

    fn descriptor_static(_: ::std::option::Option<DeleteRequest>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeString>(
                    "cf",
                    DeleteRequest::get_cf_for_reflect,
                    DeleteRequest::mut_cf_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeBytes>(
                    "key",
                    DeleteRequest::get_key_for_reflect,
                    DeleteRequest::mut_key_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<DeleteRequest>(
                    "DeleteRequest",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for DeleteRequest {
    fn clear(&mut self) {
        self.clear_cf();
        self.clear_key();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for DeleteRequest {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for DeleteRequest {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct DeleteResponse {
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for DeleteResponse {}

impl DeleteResponse {
    pub fn new() -> DeleteResponse {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static DeleteResponse {
        static mut instance: ::protobuf::lazy::Lazy<DeleteResponse> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const DeleteResponse,
        };
        unsafe {
            instance.get(DeleteResponse::new)
        }
    }
}

impl ::protobuf::Message for DeleteResponse {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !is.eof()? {
            let (field_number, wire_type) = is.read_tag_unpack()?;
            match field_number {
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
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
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

impl ::protobuf::MessageStatic for DeleteResponse {
    fn new() -> DeleteResponse {
        DeleteResponse::new()
    }

    fn descriptor_static(_: ::std::option::Option<DeleteResponse>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let fields = ::std::vec::Vec::new();
                ::protobuf::reflect::MessageDescriptor::new::<DeleteResponse>(
                    "DeleteResponse",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for DeleteResponse {
    fn clear(&mut self) {
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for DeleteResponse {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for DeleteResponse {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct DeleteRangeRequest {
    // message fields
    pub cf: ::std::string::String,
    pub start_key: ::std::vec::Vec<u8>,
    pub end_key: ::std::vec::Vec<u8>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for DeleteRangeRequest {}

impl DeleteRangeRequest {
    pub fn new() -> DeleteRangeRequest {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static DeleteRangeRequest {
        static mut instance: ::protobuf::lazy::Lazy<DeleteRangeRequest> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const DeleteRangeRequest,
        };
        unsafe {
            instance.get(DeleteRangeRequest::new)
        }
    }

    // string cf = 1;

    pub fn clear_cf(&mut self) {
        self.cf.clear();
    }

    // Param is passed by value, moved
    pub fn set_cf(&mut self, v: ::std::string::String) {
        self.cf = v;
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_cf(&mut self) -> &mut ::std::string::String {
        &mut self.cf
    }

    // Take field
    pub fn take_cf(&mut self) -> ::std::string::String {
        ::std::mem::replace(&mut self.cf, ::std::string::String::new())
    }

    pub fn get_cf(&self) -> &str {
        &self.cf
    }

    fn get_cf_for_reflect(&self) -> &::std::string::String {
        &self.cf
    }

    fn mut_cf_for_reflect(&mut self) -> &mut ::std::string::String {
        &mut self.cf
    }

    // bytes start_key = 2;

    pub fn clear_start_key(&mut self) {
        self.start_key.clear();
    }

    // Param is passed by value, moved
    pub fn set_start_key(&mut self, v: ::std::vec::Vec<u8>) {
        self.start_key = v;
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_start_key(&mut self) -> &mut ::std::vec::Vec<u8> {
        &mut self.start_key
    }

    // Take field
    pub fn take_start_key(&mut self) -> ::std::vec::Vec<u8> {
        ::std::mem::replace(&mut self.start_key, ::std::vec::Vec::new())
    }

    pub fn get_start_key(&self) -> &[u8] {
        &self.start_key
    }

    fn get_start_key_for_reflect(&self) -> &::std::vec::Vec<u8> {
        &self.start_key
    }

    fn mut_start_key_for_reflect(&mut self) -> &mut ::std::vec::Vec<u8> {
        &mut self.start_key
    }

    // bytes end_key = 3;

    pub fn clear_end_key(&mut self) {
        self.end_key.clear();
    }

    // Param is passed by value, moved
    pub fn set_end_key(&mut self, v: ::std::vec::Vec<u8>) {
        self.end_key = v;
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_end_key(&mut self) -> &mut ::std::vec::Vec<u8> {
        &mut self.end_key
    }

    // Take field
    pub fn take_end_key(&mut self) -> ::std::vec::Vec<u8> {
        ::std::mem::replace(&mut self.end_key, ::std::vec::Vec::new())
    }

    pub fn get_end_key(&self) -> &[u8] {
        &self.end_key
    }

    fn get_end_key_for_reflect(&self) -> &::std::vec::Vec<u8> {
        &self.end_key
    }

    fn mut_end_key_for_reflect(&mut self) -> &mut ::std::vec::Vec<u8> {
        &mut self.end_key
    }
}

impl ::protobuf::Message for DeleteRangeRequest {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !is.eof()? {
            let (field_number, wire_type) = is.read_tag_unpack()?;
            match field_number {
                1 => {
                    ::protobuf::rt::read_singular_proto3_string_into(wire_type, is, &mut self.cf)?;
                },
                2 => {
                    ::protobuf::rt::read_singular_proto3_bytes_into(wire_type, is, &mut self.start_key)?;
                },
                3 => {
                    ::protobuf::rt::read_singular_proto3_bytes_into(wire_type, is, &mut self.end_key)?;
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
        if !self.cf.is_empty() {
            my_size += ::protobuf::rt::string_size(1, &self.cf);
        }
        if !self.start_key.is_empty() {
            my_size += ::protobuf::rt::bytes_size(2, &self.start_key);
        }
        if !self.end_key.is_empty() {
            my_size += ::protobuf::rt::bytes_size(3, &self.end_key);
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if !self.cf.is_empty() {
            os.write_string(1, &self.cf)?;
        }
        if !self.start_key.is_empty() {
            os.write_bytes(2, &self.start_key)?;
        }
        if !self.end_key.is_empty() {
            os.write_bytes(3, &self.end_key)?;
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

impl ::protobuf::MessageStatic for DeleteRangeRequest {
    fn new() -> DeleteRangeRequest {
        DeleteRangeRequest::new()
    }

    fn descriptor_static(_: ::std::option::Option<DeleteRangeRequest>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeString>(
                    "cf",
                    DeleteRangeRequest::get_cf_for_reflect,
                    DeleteRangeRequest::mut_cf_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeBytes>(
                    "start_key",
                    DeleteRangeRequest::get_start_key_for_reflect,
                    DeleteRangeRequest::mut_start_key_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeBytes>(
                    "end_key",
                    DeleteRangeRequest::get_end_key_for_reflect,
                    DeleteRangeRequest::mut_end_key_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<DeleteRangeRequest>(
                    "DeleteRangeRequest",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for DeleteRangeRequest {
    fn clear(&mut self) {
        self.clear_cf();
        self.clear_start_key();
        self.clear_end_key();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for DeleteRangeRequest {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for DeleteRangeRequest {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct DeleteRangeResponse {
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for DeleteRangeResponse {}

impl DeleteRangeResponse {
    pub fn new() -> DeleteRangeResponse {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static DeleteRangeResponse {
        static mut instance: ::protobuf::lazy::Lazy<DeleteRangeResponse> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const DeleteRangeResponse,
        };
        unsafe {
            instance.get(DeleteRangeResponse::new)
        }
    }
}

impl ::protobuf::Message for DeleteRangeResponse {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !is.eof()? {
            let (field_number, wire_type) = is.read_tag_unpack()?;
            match field_number {
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
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
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

impl ::protobuf::MessageStatic for DeleteRangeResponse {
    fn new() -> DeleteRangeResponse {
        DeleteRangeResponse::new()
    }

    fn descriptor_static(_: ::std::option::Option<DeleteRangeResponse>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let fields = ::std::vec::Vec::new();
                ::protobuf::reflect::MessageDescriptor::new::<DeleteRangeResponse>(
                    "DeleteRangeResponse",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for DeleteRangeResponse {
    fn clear(&mut self) {
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for DeleteRangeResponse {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for DeleteRangeResponse {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct SnapRequest {
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for SnapRequest {}

impl SnapRequest {
    pub fn new() -> SnapRequest {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static SnapRequest {
        static mut instance: ::protobuf::lazy::Lazy<SnapRequest> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const SnapRequest,
        };
        unsafe {
            instance.get(SnapRequest::new)
        }
    }
}

impl ::protobuf::Message for SnapRequest {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !is.eof()? {
            let (field_number, wire_type) = is.read_tag_unpack()?;
            match field_number {
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
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
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

impl ::protobuf::MessageStatic for SnapRequest {
    fn new() -> SnapRequest {
        SnapRequest::new()
    }

    fn descriptor_static(_: ::std::option::Option<SnapRequest>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let fields = ::std::vec::Vec::new();
                ::protobuf::reflect::MessageDescriptor::new::<SnapRequest>(
                    "SnapRequest",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for SnapRequest {
    fn clear(&mut self) {
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for SnapRequest {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for SnapRequest {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct SnapResponse {
    // message fields
    pub region: ::protobuf::SingularPtrField<super::metapb::Region>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for SnapResponse {}

impl SnapResponse {
    pub fn new() -> SnapResponse {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static SnapResponse {
        static mut instance: ::protobuf::lazy::Lazy<SnapResponse> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const SnapResponse,
        };
        unsafe {
            instance.get(SnapResponse::new)
        }
    }

    // .metapb.Region region = 1;

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

impl ::protobuf::Message for SnapResponse {
    fn is_initialized(&self) -> bool {
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
        if let Some(ref v) = self.region.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(ref v) = self.region.as_ref() {
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

impl ::protobuf::MessageStatic for SnapResponse {
    fn new() -> SnapResponse {
        SnapResponse::new()
    }

    fn descriptor_static(_: ::std::option::Option<SnapResponse>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<super::metapb::Region>>(
                    "region",
                    SnapResponse::get_region_for_reflect,
                    SnapResponse::mut_region_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<SnapResponse>(
                    "SnapResponse",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for SnapResponse {
    fn clear(&mut self) {
        self.clear_region();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for SnapResponse {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for SnapResponse {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct PrewriteRequest {
    // message fields
    pub key: ::std::vec::Vec<u8>,
    pub value: ::std::vec::Vec<u8>,
    pub lock: ::std::vec::Vec<u8>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for PrewriteRequest {}

impl PrewriteRequest {
    pub fn new() -> PrewriteRequest {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static PrewriteRequest {
        static mut instance: ::protobuf::lazy::Lazy<PrewriteRequest> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const PrewriteRequest,
        };
        unsafe {
            instance.get(PrewriteRequest::new)
        }
    }

    // bytes key = 1;

    pub fn clear_key(&mut self) {
        self.key.clear();
    }

    // Param is passed by value, moved
    pub fn set_key(&mut self, v: ::std::vec::Vec<u8>) {
        self.key = v;
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_key(&mut self) -> &mut ::std::vec::Vec<u8> {
        &mut self.key
    }

    // Take field
    pub fn take_key(&mut self) -> ::std::vec::Vec<u8> {
        ::std::mem::replace(&mut self.key, ::std::vec::Vec::new())
    }

    pub fn get_key(&self) -> &[u8] {
        &self.key
    }

    fn get_key_for_reflect(&self) -> &::std::vec::Vec<u8> {
        &self.key
    }

    fn mut_key_for_reflect(&mut self) -> &mut ::std::vec::Vec<u8> {
        &mut self.key
    }

    // bytes value = 2;

    pub fn clear_value(&mut self) {
        self.value.clear();
    }

    // Param is passed by value, moved
    pub fn set_value(&mut self, v: ::std::vec::Vec<u8>) {
        self.value = v;
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_value(&mut self) -> &mut ::std::vec::Vec<u8> {
        &mut self.value
    }

    // Take field
    pub fn take_value(&mut self) -> ::std::vec::Vec<u8> {
        ::std::mem::replace(&mut self.value, ::std::vec::Vec::new())
    }

    pub fn get_value(&self) -> &[u8] {
        &self.value
    }

    fn get_value_for_reflect(&self) -> &::std::vec::Vec<u8> {
        &self.value
    }

    fn mut_value_for_reflect(&mut self) -> &mut ::std::vec::Vec<u8> {
        &mut self.value
    }

    // bytes lock = 3;

    pub fn clear_lock(&mut self) {
        self.lock.clear();
    }

    // Param is passed by value, moved
    pub fn set_lock(&mut self, v: ::std::vec::Vec<u8>) {
        self.lock = v;
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_lock(&mut self) -> &mut ::std::vec::Vec<u8> {
        &mut self.lock
    }

    // Take field
    pub fn take_lock(&mut self) -> ::std::vec::Vec<u8> {
        ::std::mem::replace(&mut self.lock, ::std::vec::Vec::new())
    }

    pub fn get_lock(&self) -> &[u8] {
        &self.lock
    }

    fn get_lock_for_reflect(&self) -> &::std::vec::Vec<u8> {
        &self.lock
    }

    fn mut_lock_for_reflect(&mut self) -> &mut ::std::vec::Vec<u8> {
        &mut self.lock
    }
}

impl ::protobuf::Message for PrewriteRequest {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !is.eof()? {
            let (field_number, wire_type) = is.read_tag_unpack()?;
            match field_number {
                1 => {
                    ::protobuf::rt::read_singular_proto3_bytes_into(wire_type, is, &mut self.key)?;
                },
                2 => {
                    ::protobuf::rt::read_singular_proto3_bytes_into(wire_type, is, &mut self.value)?;
                },
                3 => {
                    ::protobuf::rt::read_singular_proto3_bytes_into(wire_type, is, &mut self.lock)?;
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
        if !self.key.is_empty() {
            my_size += ::protobuf::rt::bytes_size(1, &self.key);
        }
        if !self.value.is_empty() {
            my_size += ::protobuf::rt::bytes_size(2, &self.value);
        }
        if !self.lock.is_empty() {
            my_size += ::protobuf::rt::bytes_size(3, &self.lock);
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if !self.key.is_empty() {
            os.write_bytes(1, &self.key)?;
        }
        if !self.value.is_empty() {
            os.write_bytes(2, &self.value)?;
        }
        if !self.lock.is_empty() {
            os.write_bytes(3, &self.lock)?;
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

impl ::protobuf::MessageStatic for PrewriteRequest {
    fn new() -> PrewriteRequest {
        PrewriteRequest::new()
    }

    fn descriptor_static(_: ::std::option::Option<PrewriteRequest>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeBytes>(
                    "key",
                    PrewriteRequest::get_key_for_reflect,
                    PrewriteRequest::mut_key_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeBytes>(
                    "value",
                    PrewriteRequest::get_value_for_reflect,
                    PrewriteRequest::mut_value_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeBytes>(
                    "lock",
                    PrewriteRequest::get_lock_for_reflect,
                    PrewriteRequest::mut_lock_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<PrewriteRequest>(
                    "PrewriteRequest",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for PrewriteRequest {
    fn clear(&mut self) {
        self.clear_key();
        self.clear_value();
        self.clear_lock();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for PrewriteRequest {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for PrewriteRequest {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct PrewriteResponse {
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for PrewriteResponse {}

impl PrewriteResponse {
    pub fn new() -> PrewriteResponse {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static PrewriteResponse {
        static mut instance: ::protobuf::lazy::Lazy<PrewriteResponse> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const PrewriteResponse,
        };
        unsafe {
            instance.get(PrewriteResponse::new)
        }
    }
}

impl ::protobuf::Message for PrewriteResponse {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !is.eof()? {
            let (field_number, wire_type) = is.read_tag_unpack()?;
            match field_number {
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
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
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

impl ::protobuf::MessageStatic for PrewriteResponse {
    fn new() -> PrewriteResponse {
        PrewriteResponse::new()
    }

    fn descriptor_static(_: ::std::option::Option<PrewriteResponse>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let fields = ::std::vec::Vec::new();
                ::protobuf::reflect::MessageDescriptor::new::<PrewriteResponse>(
                    "PrewriteResponse",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for PrewriteResponse {
    fn clear(&mut self) {
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for PrewriteResponse {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for PrewriteResponse {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct Request {
    // message fields
    pub cmd_type: CmdType,
    pub get: ::protobuf::SingularPtrField<GetRequest>,
    pub put: ::protobuf::SingularPtrField<PutRequest>,
    pub delete: ::protobuf::SingularPtrField<DeleteRequest>,
    pub snap: ::protobuf::SingularPtrField<SnapRequest>,
    pub prewrite: ::protobuf::SingularPtrField<PrewriteRequest>,
    pub delete_range: ::protobuf::SingularPtrField<DeleteRangeRequest>,
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

    // .raft_cmdpb.CmdType cmd_type = 1;

    pub fn clear_cmd_type(&mut self) {
        self.cmd_type = CmdType::Invalid;
    }

    // Param is passed by value, moved
    pub fn set_cmd_type(&mut self, v: CmdType) {
        self.cmd_type = v;
    }

    pub fn get_cmd_type(&self) -> CmdType {
        self.cmd_type
    }

    fn get_cmd_type_for_reflect(&self) -> &CmdType {
        &self.cmd_type
    }

    fn mut_cmd_type_for_reflect(&mut self) -> &mut CmdType {
        &mut self.cmd_type
    }

    // .raft_cmdpb.GetRequest get = 2;

    pub fn clear_get(&mut self) {
        self.get.clear();
    }

    pub fn has_get(&self) -> bool {
        self.get.is_some()
    }

    // Param is passed by value, moved
    pub fn set_get(&mut self, v: GetRequest) {
        self.get = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_get(&mut self) -> &mut GetRequest {
        if self.get.is_none() {
            self.get.set_default();
        }
        self.get.as_mut().unwrap()
    }

    // Take field
    pub fn take_get(&mut self) -> GetRequest {
        self.get.take().unwrap_or_else(|| GetRequest::new())
    }

    pub fn get_get(&self) -> &GetRequest {
        self.get.as_ref().unwrap_or_else(|| GetRequest::default_instance())
    }

    fn get_get_for_reflect(&self) -> &::protobuf::SingularPtrField<GetRequest> {
        &self.get
    }

    fn mut_get_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<GetRequest> {
        &mut self.get
    }

    // .raft_cmdpb.PutRequest put = 4;

    pub fn clear_put(&mut self) {
        self.put.clear();
    }

    pub fn has_put(&self) -> bool {
        self.put.is_some()
    }

    // Param is passed by value, moved
    pub fn set_put(&mut self, v: PutRequest) {
        self.put = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_put(&mut self) -> &mut PutRequest {
        if self.put.is_none() {
            self.put.set_default();
        }
        self.put.as_mut().unwrap()
    }

    // Take field
    pub fn take_put(&mut self) -> PutRequest {
        self.put.take().unwrap_or_else(|| PutRequest::new())
    }

    pub fn get_put(&self) -> &PutRequest {
        self.put.as_ref().unwrap_or_else(|| PutRequest::default_instance())
    }

    fn get_put_for_reflect(&self) -> &::protobuf::SingularPtrField<PutRequest> {
        &self.put
    }

    fn mut_put_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<PutRequest> {
        &mut self.put
    }

    // .raft_cmdpb.DeleteRequest delete = 5;

    pub fn clear_delete(&mut self) {
        self.delete.clear();
    }

    pub fn has_delete(&self) -> bool {
        self.delete.is_some()
    }

    // Param is passed by value, moved
    pub fn set_delete(&mut self, v: DeleteRequest) {
        self.delete = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_delete(&mut self) -> &mut DeleteRequest {
        if self.delete.is_none() {
            self.delete.set_default();
        }
        self.delete.as_mut().unwrap()
    }

    // Take field
    pub fn take_delete(&mut self) -> DeleteRequest {
        self.delete.take().unwrap_or_else(|| DeleteRequest::new())
    }

    pub fn get_delete(&self) -> &DeleteRequest {
        self.delete.as_ref().unwrap_or_else(|| DeleteRequest::default_instance())
    }

    fn get_delete_for_reflect(&self) -> &::protobuf::SingularPtrField<DeleteRequest> {
        &self.delete
    }

    fn mut_delete_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<DeleteRequest> {
        &mut self.delete
    }

    // .raft_cmdpb.SnapRequest snap = 6;

    pub fn clear_snap(&mut self) {
        self.snap.clear();
    }

    pub fn has_snap(&self) -> bool {
        self.snap.is_some()
    }

    // Param is passed by value, moved
    pub fn set_snap(&mut self, v: SnapRequest) {
        self.snap = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_snap(&mut self) -> &mut SnapRequest {
        if self.snap.is_none() {
            self.snap.set_default();
        }
        self.snap.as_mut().unwrap()
    }

    // Take field
    pub fn take_snap(&mut self) -> SnapRequest {
        self.snap.take().unwrap_or_else(|| SnapRequest::new())
    }

    pub fn get_snap(&self) -> &SnapRequest {
        self.snap.as_ref().unwrap_or_else(|| SnapRequest::default_instance())
    }

    fn get_snap_for_reflect(&self) -> &::protobuf::SingularPtrField<SnapRequest> {
        &self.snap
    }

    fn mut_snap_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<SnapRequest> {
        &mut self.snap
    }

    // .raft_cmdpb.PrewriteRequest prewrite = 7;

    pub fn clear_prewrite(&mut self) {
        self.prewrite.clear();
    }

    pub fn has_prewrite(&self) -> bool {
        self.prewrite.is_some()
    }

    // Param is passed by value, moved
    pub fn set_prewrite(&mut self, v: PrewriteRequest) {
        self.prewrite = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_prewrite(&mut self) -> &mut PrewriteRequest {
        if self.prewrite.is_none() {
            self.prewrite.set_default();
        }
        self.prewrite.as_mut().unwrap()
    }

    // Take field
    pub fn take_prewrite(&mut self) -> PrewriteRequest {
        self.prewrite.take().unwrap_or_else(|| PrewriteRequest::new())
    }

    pub fn get_prewrite(&self) -> &PrewriteRequest {
        self.prewrite.as_ref().unwrap_or_else(|| PrewriteRequest::default_instance())
    }

    fn get_prewrite_for_reflect(&self) -> &::protobuf::SingularPtrField<PrewriteRequest> {
        &self.prewrite
    }

    fn mut_prewrite_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<PrewriteRequest> {
        &mut self.prewrite
    }

    // .raft_cmdpb.DeleteRangeRequest delete_range = 8;

    pub fn clear_delete_range(&mut self) {
        self.delete_range.clear();
    }

    pub fn has_delete_range(&self) -> bool {
        self.delete_range.is_some()
    }

    // Param is passed by value, moved
    pub fn set_delete_range(&mut self, v: DeleteRangeRequest) {
        self.delete_range = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_delete_range(&mut self) -> &mut DeleteRangeRequest {
        if self.delete_range.is_none() {
            self.delete_range.set_default();
        }
        self.delete_range.as_mut().unwrap()
    }

    // Take field
    pub fn take_delete_range(&mut self) -> DeleteRangeRequest {
        self.delete_range.take().unwrap_or_else(|| DeleteRangeRequest::new())
    }

    pub fn get_delete_range(&self) -> &DeleteRangeRequest {
        self.delete_range.as_ref().unwrap_or_else(|| DeleteRangeRequest::default_instance())
    }

    fn get_delete_range_for_reflect(&self) -> &::protobuf::SingularPtrField<DeleteRangeRequest> {
        &self.delete_range
    }

    fn mut_delete_range_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<DeleteRangeRequest> {
        &mut self.delete_range
    }
}

impl ::protobuf::Message for Request {
    fn is_initialized(&self) -> bool {
        for v in &self.get {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.put {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.delete {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.snap {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.prewrite {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.delete_range {
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
                    self.cmd_type = tmp;
                },
                2 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.get)?;
                },
                4 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.put)?;
                },
                5 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.delete)?;
                },
                6 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.snap)?;
                },
                7 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.prewrite)?;
                },
                8 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.delete_range)?;
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
        if self.cmd_type != CmdType::Invalid {
            my_size += ::protobuf::rt::enum_size(1, self.cmd_type);
        }
        if let Some(ref v) = self.get.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if let Some(ref v) = self.put.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if let Some(ref v) = self.delete.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if let Some(ref v) = self.snap.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if let Some(ref v) = self.prewrite.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if let Some(ref v) = self.delete_range.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if self.cmd_type != CmdType::Invalid {
            os.write_enum(1, self.cmd_type.value())?;
        }
        if let Some(ref v) = self.get.as_ref() {
            os.write_tag(2, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if let Some(ref v) = self.put.as_ref() {
            os.write_tag(4, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if let Some(ref v) = self.delete.as_ref() {
            os.write_tag(5, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if let Some(ref v) = self.snap.as_ref() {
            os.write_tag(6, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if let Some(ref v) = self.prewrite.as_ref() {
            os.write_tag(7, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if let Some(ref v) = self.delete_range.as_ref() {
            os.write_tag(8, ::protobuf::wire_format::WireTypeLengthDelimited)?;
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
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeEnum<CmdType>>(
                    "cmd_type",
                    Request::get_cmd_type_for_reflect,
                    Request::mut_cmd_type_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<GetRequest>>(
                    "get",
                    Request::get_get_for_reflect,
                    Request::mut_get_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<PutRequest>>(
                    "put",
                    Request::get_put_for_reflect,
                    Request::mut_put_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<DeleteRequest>>(
                    "delete",
                    Request::get_delete_for_reflect,
                    Request::mut_delete_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<SnapRequest>>(
                    "snap",
                    Request::get_snap_for_reflect,
                    Request::mut_snap_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<PrewriteRequest>>(
                    "prewrite",
                    Request::get_prewrite_for_reflect,
                    Request::mut_prewrite_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<DeleteRangeRequest>>(
                    "delete_range",
                    Request::get_delete_range_for_reflect,
                    Request::mut_delete_range_for_reflect,
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
        self.clear_cmd_type();
        self.clear_get();
        self.clear_put();
        self.clear_delete();
        self.clear_snap();
        self.clear_prewrite();
        self.clear_delete_range();
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
    pub cmd_type: CmdType,
    pub get: ::protobuf::SingularPtrField<GetResponse>,
    pub put: ::protobuf::SingularPtrField<PutResponse>,
    pub delete: ::protobuf::SingularPtrField<DeleteResponse>,
    pub snap: ::protobuf::SingularPtrField<SnapResponse>,
    pub prewrite: ::protobuf::SingularPtrField<PrewriteResponse>,
    pub delte_range: ::protobuf::SingularPtrField<DeleteRangeResponse>,
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

    // .raft_cmdpb.CmdType cmd_type = 1;

    pub fn clear_cmd_type(&mut self) {
        self.cmd_type = CmdType::Invalid;
    }

    // Param is passed by value, moved
    pub fn set_cmd_type(&mut self, v: CmdType) {
        self.cmd_type = v;
    }

    pub fn get_cmd_type(&self) -> CmdType {
        self.cmd_type
    }

    fn get_cmd_type_for_reflect(&self) -> &CmdType {
        &self.cmd_type
    }

    fn mut_cmd_type_for_reflect(&mut self) -> &mut CmdType {
        &mut self.cmd_type
    }

    // .raft_cmdpb.GetResponse get = 2;

    pub fn clear_get(&mut self) {
        self.get.clear();
    }

    pub fn has_get(&self) -> bool {
        self.get.is_some()
    }

    // Param is passed by value, moved
    pub fn set_get(&mut self, v: GetResponse) {
        self.get = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_get(&mut self) -> &mut GetResponse {
        if self.get.is_none() {
            self.get.set_default();
        }
        self.get.as_mut().unwrap()
    }

    // Take field
    pub fn take_get(&mut self) -> GetResponse {
        self.get.take().unwrap_or_else(|| GetResponse::new())
    }

    pub fn get_get(&self) -> &GetResponse {
        self.get.as_ref().unwrap_or_else(|| GetResponse::default_instance())
    }

    fn get_get_for_reflect(&self) -> &::protobuf::SingularPtrField<GetResponse> {
        &self.get
    }

    fn mut_get_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<GetResponse> {
        &mut self.get
    }

    // .raft_cmdpb.PutResponse put = 4;

    pub fn clear_put(&mut self) {
        self.put.clear();
    }

    pub fn has_put(&self) -> bool {
        self.put.is_some()
    }

    // Param is passed by value, moved
    pub fn set_put(&mut self, v: PutResponse) {
        self.put = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_put(&mut self) -> &mut PutResponse {
        if self.put.is_none() {
            self.put.set_default();
        }
        self.put.as_mut().unwrap()
    }

    // Take field
    pub fn take_put(&mut self) -> PutResponse {
        self.put.take().unwrap_or_else(|| PutResponse::new())
    }

    pub fn get_put(&self) -> &PutResponse {
        self.put.as_ref().unwrap_or_else(|| PutResponse::default_instance())
    }

    fn get_put_for_reflect(&self) -> &::protobuf::SingularPtrField<PutResponse> {
        &self.put
    }

    fn mut_put_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<PutResponse> {
        &mut self.put
    }

    // .raft_cmdpb.DeleteResponse delete = 5;

    pub fn clear_delete(&mut self) {
        self.delete.clear();
    }

    pub fn has_delete(&self) -> bool {
        self.delete.is_some()
    }

    // Param is passed by value, moved
    pub fn set_delete(&mut self, v: DeleteResponse) {
        self.delete = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_delete(&mut self) -> &mut DeleteResponse {
        if self.delete.is_none() {
            self.delete.set_default();
        }
        self.delete.as_mut().unwrap()
    }

    // Take field
    pub fn take_delete(&mut self) -> DeleteResponse {
        self.delete.take().unwrap_or_else(|| DeleteResponse::new())
    }

    pub fn get_delete(&self) -> &DeleteResponse {
        self.delete.as_ref().unwrap_or_else(|| DeleteResponse::default_instance())
    }

    fn get_delete_for_reflect(&self) -> &::protobuf::SingularPtrField<DeleteResponse> {
        &self.delete
    }

    fn mut_delete_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<DeleteResponse> {
        &mut self.delete
    }

    // .raft_cmdpb.SnapResponse snap = 6;

    pub fn clear_snap(&mut self) {
        self.snap.clear();
    }

    pub fn has_snap(&self) -> bool {
        self.snap.is_some()
    }

    // Param is passed by value, moved
    pub fn set_snap(&mut self, v: SnapResponse) {
        self.snap = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_snap(&mut self) -> &mut SnapResponse {
        if self.snap.is_none() {
            self.snap.set_default();
        }
        self.snap.as_mut().unwrap()
    }

    // Take field
    pub fn take_snap(&mut self) -> SnapResponse {
        self.snap.take().unwrap_or_else(|| SnapResponse::new())
    }

    pub fn get_snap(&self) -> &SnapResponse {
        self.snap.as_ref().unwrap_or_else(|| SnapResponse::default_instance())
    }

    fn get_snap_for_reflect(&self) -> &::protobuf::SingularPtrField<SnapResponse> {
        &self.snap
    }

    fn mut_snap_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<SnapResponse> {
        &mut self.snap
    }

    // .raft_cmdpb.PrewriteResponse prewrite = 7;

    pub fn clear_prewrite(&mut self) {
        self.prewrite.clear();
    }

    pub fn has_prewrite(&self) -> bool {
        self.prewrite.is_some()
    }

    // Param is passed by value, moved
    pub fn set_prewrite(&mut self, v: PrewriteResponse) {
        self.prewrite = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_prewrite(&mut self) -> &mut PrewriteResponse {
        if self.prewrite.is_none() {
            self.prewrite.set_default();
        }
        self.prewrite.as_mut().unwrap()
    }

    // Take field
    pub fn take_prewrite(&mut self) -> PrewriteResponse {
        self.prewrite.take().unwrap_or_else(|| PrewriteResponse::new())
    }

    pub fn get_prewrite(&self) -> &PrewriteResponse {
        self.prewrite.as_ref().unwrap_or_else(|| PrewriteResponse::default_instance())
    }

    fn get_prewrite_for_reflect(&self) -> &::protobuf::SingularPtrField<PrewriteResponse> {
        &self.prewrite
    }

    fn mut_prewrite_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<PrewriteResponse> {
        &mut self.prewrite
    }

    // .raft_cmdpb.DeleteRangeResponse delte_range = 8;

    pub fn clear_delte_range(&mut self) {
        self.delte_range.clear();
    }

    pub fn has_delte_range(&self) -> bool {
        self.delte_range.is_some()
    }

    // Param is passed by value, moved
    pub fn set_delte_range(&mut self, v: DeleteRangeResponse) {
        self.delte_range = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_delte_range(&mut self) -> &mut DeleteRangeResponse {
        if self.delte_range.is_none() {
            self.delte_range.set_default();
        }
        self.delte_range.as_mut().unwrap()
    }

    // Take field
    pub fn take_delte_range(&mut self) -> DeleteRangeResponse {
        self.delte_range.take().unwrap_or_else(|| DeleteRangeResponse::new())
    }

    pub fn get_delte_range(&self) -> &DeleteRangeResponse {
        self.delte_range.as_ref().unwrap_or_else(|| DeleteRangeResponse::default_instance())
    }

    fn get_delte_range_for_reflect(&self) -> &::protobuf::SingularPtrField<DeleteRangeResponse> {
        &self.delte_range
    }

    fn mut_delte_range_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<DeleteRangeResponse> {
        &mut self.delte_range
    }
}

impl ::protobuf::Message for Response {
    fn is_initialized(&self) -> bool {
        for v in &self.get {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.put {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.delete {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.snap {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.prewrite {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.delte_range {
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
                    self.cmd_type = tmp;
                },
                2 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.get)?;
                },
                4 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.put)?;
                },
                5 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.delete)?;
                },
                6 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.snap)?;
                },
                7 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.prewrite)?;
                },
                8 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.delte_range)?;
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
        if self.cmd_type != CmdType::Invalid {
            my_size += ::protobuf::rt::enum_size(1, self.cmd_type);
        }
        if let Some(ref v) = self.get.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if let Some(ref v) = self.put.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if let Some(ref v) = self.delete.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if let Some(ref v) = self.snap.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if let Some(ref v) = self.prewrite.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if let Some(ref v) = self.delte_range.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if self.cmd_type != CmdType::Invalid {
            os.write_enum(1, self.cmd_type.value())?;
        }
        if let Some(ref v) = self.get.as_ref() {
            os.write_tag(2, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if let Some(ref v) = self.put.as_ref() {
            os.write_tag(4, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if let Some(ref v) = self.delete.as_ref() {
            os.write_tag(5, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if let Some(ref v) = self.snap.as_ref() {
            os.write_tag(6, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if let Some(ref v) = self.prewrite.as_ref() {
            os.write_tag(7, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if let Some(ref v) = self.delte_range.as_ref() {
            os.write_tag(8, ::protobuf::wire_format::WireTypeLengthDelimited)?;
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
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeEnum<CmdType>>(
                    "cmd_type",
                    Response::get_cmd_type_for_reflect,
                    Response::mut_cmd_type_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<GetResponse>>(
                    "get",
                    Response::get_get_for_reflect,
                    Response::mut_get_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<PutResponse>>(
                    "put",
                    Response::get_put_for_reflect,
                    Response::mut_put_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<DeleteResponse>>(
                    "delete",
                    Response::get_delete_for_reflect,
                    Response::mut_delete_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<SnapResponse>>(
                    "snap",
                    Response::get_snap_for_reflect,
                    Response::mut_snap_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<PrewriteResponse>>(
                    "prewrite",
                    Response::get_prewrite_for_reflect,
                    Response::mut_prewrite_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<DeleteRangeResponse>>(
                    "delte_range",
                    Response::get_delte_range_for_reflect,
                    Response::mut_delte_range_for_reflect,
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
        self.clear_cmd_type();
        self.clear_get();
        self.clear_put();
        self.clear_delete();
        self.clear_snap();
        self.clear_prewrite();
        self.clear_delte_range();
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

#[derive(PartialEq,Clone,Default)]
pub struct ChangePeerRequest {
    // message fields
    pub change_type: super::eraftpb::ConfChangeType,
    pub peer: ::protobuf::SingularPtrField<super::metapb::Peer>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for ChangePeerRequest {}

impl ChangePeerRequest {
    pub fn new() -> ChangePeerRequest {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static ChangePeerRequest {
        static mut instance: ::protobuf::lazy::Lazy<ChangePeerRequest> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ChangePeerRequest,
        };
        unsafe {
            instance.get(ChangePeerRequest::new)
        }
    }

    // .eraftpb.ConfChangeType change_type = 1;

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

    // .metapb.Peer peer = 2;

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

impl ::protobuf::Message for ChangePeerRequest {
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
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_enum()?;
                    self.change_type = tmp;
                },
                2 => {
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
        if self.change_type != super::eraftpb::ConfChangeType::AddNode {
            my_size += ::protobuf::rt::enum_size(1, self.change_type);
        }
        if let Some(ref v) = self.peer.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if self.change_type != super::eraftpb::ConfChangeType::AddNode {
            os.write_enum(1, self.change_type.value())?;
        }
        if let Some(ref v) = self.peer.as_ref() {
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

impl ::protobuf::MessageStatic for ChangePeerRequest {
    fn new() -> ChangePeerRequest {
        ChangePeerRequest::new()
    }

    fn descriptor_static(_: ::std::option::Option<ChangePeerRequest>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeEnum<super::eraftpb::ConfChangeType>>(
                    "change_type",
                    ChangePeerRequest::get_change_type_for_reflect,
                    ChangePeerRequest::mut_change_type_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<super::metapb::Peer>>(
                    "peer",
                    ChangePeerRequest::get_peer_for_reflect,
                    ChangePeerRequest::mut_peer_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<ChangePeerRequest>(
                    "ChangePeerRequest",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for ChangePeerRequest {
    fn clear(&mut self) {
        self.clear_change_type();
        self.clear_peer();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for ChangePeerRequest {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for ChangePeerRequest {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct ChangePeerResponse {
    // message fields
    pub region: ::protobuf::SingularPtrField<super::metapb::Region>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for ChangePeerResponse {}

impl ChangePeerResponse {
    pub fn new() -> ChangePeerResponse {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static ChangePeerResponse {
        static mut instance: ::protobuf::lazy::Lazy<ChangePeerResponse> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ChangePeerResponse,
        };
        unsafe {
            instance.get(ChangePeerResponse::new)
        }
    }

    // .metapb.Region region = 1;

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

impl ::protobuf::Message for ChangePeerResponse {
    fn is_initialized(&self) -> bool {
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
        if let Some(ref v) = self.region.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(ref v) = self.region.as_ref() {
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

impl ::protobuf::MessageStatic for ChangePeerResponse {
    fn new() -> ChangePeerResponse {
        ChangePeerResponse::new()
    }

    fn descriptor_static(_: ::std::option::Option<ChangePeerResponse>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<super::metapb::Region>>(
                    "region",
                    ChangePeerResponse::get_region_for_reflect,
                    ChangePeerResponse::mut_region_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<ChangePeerResponse>(
                    "ChangePeerResponse",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for ChangePeerResponse {
    fn clear(&mut self) {
        self.clear_region();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for ChangePeerResponse {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for ChangePeerResponse {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct SplitRequest {
    // message fields
    pub split_key: ::std::vec::Vec<u8>,
    pub new_region_id: u64,
    pub new_peer_ids: ::std::vec::Vec<u64>,
    pub right_derive: bool,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for SplitRequest {}

impl SplitRequest {
    pub fn new() -> SplitRequest {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static SplitRequest {
        static mut instance: ::protobuf::lazy::Lazy<SplitRequest> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const SplitRequest,
        };
        unsafe {
            instance.get(SplitRequest::new)
        }
    }

    // bytes split_key = 1;

    pub fn clear_split_key(&mut self) {
        self.split_key.clear();
    }

    // Param is passed by value, moved
    pub fn set_split_key(&mut self, v: ::std::vec::Vec<u8>) {
        self.split_key = v;
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_split_key(&mut self) -> &mut ::std::vec::Vec<u8> {
        &mut self.split_key
    }

    // Take field
    pub fn take_split_key(&mut self) -> ::std::vec::Vec<u8> {
        ::std::mem::replace(&mut self.split_key, ::std::vec::Vec::new())
    }

    pub fn get_split_key(&self) -> &[u8] {
        &self.split_key
    }

    fn get_split_key_for_reflect(&self) -> &::std::vec::Vec<u8> {
        &self.split_key
    }

    fn mut_split_key_for_reflect(&mut self) -> &mut ::std::vec::Vec<u8> {
        &mut self.split_key
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

    // bool right_derive = 4;

    pub fn clear_right_derive(&mut self) {
        self.right_derive = false;
    }

    // Param is passed by value, moved
    pub fn set_right_derive(&mut self, v: bool) {
        self.right_derive = v;
    }

    pub fn get_right_derive(&self) -> bool {
        self.right_derive
    }

    fn get_right_derive_for_reflect(&self) -> &bool {
        &self.right_derive
    }

    fn mut_right_derive_for_reflect(&mut self) -> &mut bool {
        &mut self.right_derive
    }
}

impl ::protobuf::Message for SplitRequest {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !is.eof()? {
            let (field_number, wire_type) = is.read_tag_unpack()?;
            match field_number {
                1 => {
                    ::protobuf::rt::read_singular_proto3_bytes_into(wire_type, is, &mut self.split_key)?;
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
                4 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_bool()?;
                    self.right_derive = tmp;
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
        if !self.split_key.is_empty() {
            my_size += ::protobuf::rt::bytes_size(1, &self.split_key);
        }
        if self.new_region_id != 0 {
            my_size += ::protobuf::rt::value_size(2, self.new_region_id, ::protobuf::wire_format::WireTypeVarint);
        }
        for value in &self.new_peer_ids {
            my_size += ::protobuf::rt::value_size(3, *value, ::protobuf::wire_format::WireTypeVarint);
        };
        if self.right_derive != false {
            my_size += 2;
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if !self.split_key.is_empty() {
            os.write_bytes(1, &self.split_key)?;
        }
        if self.new_region_id != 0 {
            os.write_uint64(2, self.new_region_id)?;
        }
        for v in &self.new_peer_ids {
            os.write_uint64(3, *v)?;
        };
        if self.right_derive != false {
            os.write_bool(4, self.right_derive)?;
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

impl ::protobuf::MessageStatic for SplitRequest {
    fn new() -> SplitRequest {
        SplitRequest::new()
    }

    fn descriptor_static(_: ::std::option::Option<SplitRequest>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeBytes>(
                    "split_key",
                    SplitRequest::get_split_key_for_reflect,
                    SplitRequest::mut_split_key_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "new_region_id",
                    SplitRequest::get_new_region_id_for_reflect,
                    SplitRequest::mut_new_region_id_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_vec_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "new_peer_ids",
                    SplitRequest::get_new_peer_ids_for_reflect,
                    SplitRequest::mut_new_peer_ids_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeBool>(
                    "right_derive",
                    SplitRequest::get_right_derive_for_reflect,
                    SplitRequest::mut_right_derive_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<SplitRequest>(
                    "SplitRequest",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for SplitRequest {
    fn clear(&mut self) {
        self.clear_split_key();
        self.clear_new_region_id();
        self.clear_new_peer_ids();
        self.clear_right_derive();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for SplitRequest {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for SplitRequest {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct SplitResponse {
    // message fields
    pub left: ::protobuf::SingularPtrField<super::metapb::Region>,
    pub right: ::protobuf::SingularPtrField<super::metapb::Region>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for SplitResponse {}

impl SplitResponse {
    pub fn new() -> SplitResponse {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static SplitResponse {
        static mut instance: ::protobuf::lazy::Lazy<SplitResponse> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const SplitResponse,
        };
        unsafe {
            instance.get(SplitResponse::new)
        }
    }

    // .metapb.Region left = 1;

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

    // .metapb.Region right = 2;

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

impl ::protobuf::Message for SplitResponse {
    fn is_initialized(&self) -> bool {
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
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.left)?;
                },
                2 => {
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
        if let Some(ref v) = self.left.as_ref() {
            os.write_tag(1, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if let Some(ref v) = self.right.as_ref() {
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

impl ::protobuf::MessageStatic for SplitResponse {
    fn new() -> SplitResponse {
        SplitResponse::new()
    }

    fn descriptor_static(_: ::std::option::Option<SplitResponse>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<super::metapb::Region>>(
                    "left",
                    SplitResponse::get_left_for_reflect,
                    SplitResponse::mut_left_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<super::metapb::Region>>(
                    "right",
                    SplitResponse::get_right_for_reflect,
                    SplitResponse::mut_right_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<SplitResponse>(
                    "SplitResponse",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for SplitResponse {
    fn clear(&mut self) {
        self.clear_left();
        self.clear_right();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for SplitResponse {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for SplitResponse {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct CompactLogRequest {
    // message fields
    pub compact_index: u64,
    pub compact_term: u64,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for CompactLogRequest {}

impl CompactLogRequest {
    pub fn new() -> CompactLogRequest {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static CompactLogRequest {
        static mut instance: ::protobuf::lazy::Lazy<CompactLogRequest> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const CompactLogRequest,
        };
        unsafe {
            instance.get(CompactLogRequest::new)
        }
    }

    // uint64 compact_index = 1;

    pub fn clear_compact_index(&mut self) {
        self.compact_index = 0;
    }

    // Param is passed by value, moved
    pub fn set_compact_index(&mut self, v: u64) {
        self.compact_index = v;
    }

    pub fn get_compact_index(&self) -> u64 {
        self.compact_index
    }

    fn get_compact_index_for_reflect(&self) -> &u64 {
        &self.compact_index
    }

    fn mut_compact_index_for_reflect(&mut self) -> &mut u64 {
        &mut self.compact_index
    }

    // uint64 compact_term = 2;

    pub fn clear_compact_term(&mut self) {
        self.compact_term = 0;
    }

    // Param is passed by value, moved
    pub fn set_compact_term(&mut self, v: u64) {
        self.compact_term = v;
    }

    pub fn get_compact_term(&self) -> u64 {
        self.compact_term
    }

    fn get_compact_term_for_reflect(&self) -> &u64 {
        &self.compact_term
    }

    fn mut_compact_term_for_reflect(&mut self) -> &mut u64 {
        &mut self.compact_term
    }
}

impl ::protobuf::Message for CompactLogRequest {
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
                    self.compact_index = tmp;
                },
                2 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_uint64()?;
                    self.compact_term = tmp;
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
        if self.compact_index != 0 {
            my_size += ::protobuf::rt::value_size(1, self.compact_index, ::protobuf::wire_format::WireTypeVarint);
        }
        if self.compact_term != 0 {
            my_size += ::protobuf::rt::value_size(2, self.compact_term, ::protobuf::wire_format::WireTypeVarint);
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if self.compact_index != 0 {
            os.write_uint64(1, self.compact_index)?;
        }
        if self.compact_term != 0 {
            os.write_uint64(2, self.compact_term)?;
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

impl ::protobuf::MessageStatic for CompactLogRequest {
    fn new() -> CompactLogRequest {
        CompactLogRequest::new()
    }

    fn descriptor_static(_: ::std::option::Option<CompactLogRequest>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "compact_index",
                    CompactLogRequest::get_compact_index_for_reflect,
                    CompactLogRequest::mut_compact_index_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "compact_term",
                    CompactLogRequest::get_compact_term_for_reflect,
                    CompactLogRequest::mut_compact_term_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<CompactLogRequest>(
                    "CompactLogRequest",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for CompactLogRequest {
    fn clear(&mut self) {
        self.clear_compact_index();
        self.clear_compact_term();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for CompactLogRequest {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for CompactLogRequest {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct CompactLogResponse {
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for CompactLogResponse {}

impl CompactLogResponse {
    pub fn new() -> CompactLogResponse {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static CompactLogResponse {
        static mut instance: ::protobuf::lazy::Lazy<CompactLogResponse> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const CompactLogResponse,
        };
        unsafe {
            instance.get(CompactLogResponse::new)
        }
    }
}

impl ::protobuf::Message for CompactLogResponse {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !is.eof()? {
            let (field_number, wire_type) = is.read_tag_unpack()?;
            match field_number {
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
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
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

impl ::protobuf::MessageStatic for CompactLogResponse {
    fn new() -> CompactLogResponse {
        CompactLogResponse::new()
    }

    fn descriptor_static(_: ::std::option::Option<CompactLogResponse>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let fields = ::std::vec::Vec::new();
                ::protobuf::reflect::MessageDescriptor::new::<CompactLogResponse>(
                    "CompactLogResponse",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for CompactLogResponse {
    fn clear(&mut self) {
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for CompactLogResponse {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for CompactLogResponse {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct TransferLeaderRequest {
    // message fields
    pub peer: ::protobuf::SingularPtrField<super::metapb::Peer>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for TransferLeaderRequest {}

impl TransferLeaderRequest {
    pub fn new() -> TransferLeaderRequest {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static TransferLeaderRequest {
        static mut instance: ::protobuf::lazy::Lazy<TransferLeaderRequest> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const TransferLeaderRequest,
        };
        unsafe {
            instance.get(TransferLeaderRequest::new)
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

impl ::protobuf::Message for TransferLeaderRequest {
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

impl ::protobuf::MessageStatic for TransferLeaderRequest {
    fn new() -> TransferLeaderRequest {
        TransferLeaderRequest::new()
    }

    fn descriptor_static(_: ::std::option::Option<TransferLeaderRequest>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<super::metapb::Peer>>(
                    "peer",
                    TransferLeaderRequest::get_peer_for_reflect,
                    TransferLeaderRequest::mut_peer_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<TransferLeaderRequest>(
                    "TransferLeaderRequest",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for TransferLeaderRequest {
    fn clear(&mut self) {
        self.clear_peer();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for TransferLeaderRequest {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for TransferLeaderRequest {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct TransferLeaderResponse {
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for TransferLeaderResponse {}

impl TransferLeaderResponse {
    pub fn new() -> TransferLeaderResponse {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static TransferLeaderResponse {
        static mut instance: ::protobuf::lazy::Lazy<TransferLeaderResponse> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const TransferLeaderResponse,
        };
        unsafe {
            instance.get(TransferLeaderResponse::new)
        }
    }
}

impl ::protobuf::Message for TransferLeaderResponse {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !is.eof()? {
            let (field_number, wire_type) = is.read_tag_unpack()?;
            match field_number {
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
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
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

impl ::protobuf::MessageStatic for TransferLeaderResponse {
    fn new() -> TransferLeaderResponse {
        TransferLeaderResponse::new()
    }

    fn descriptor_static(_: ::std::option::Option<TransferLeaderResponse>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let fields = ::std::vec::Vec::new();
                ::protobuf::reflect::MessageDescriptor::new::<TransferLeaderResponse>(
                    "TransferLeaderResponse",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for TransferLeaderResponse {
    fn clear(&mut self) {
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for TransferLeaderResponse {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for TransferLeaderResponse {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct VerifyHashRequest {
    // message fields
    pub index: u64,
    pub hash: ::std::vec::Vec<u8>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for VerifyHashRequest {}

impl VerifyHashRequest {
    pub fn new() -> VerifyHashRequest {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static VerifyHashRequest {
        static mut instance: ::protobuf::lazy::Lazy<VerifyHashRequest> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const VerifyHashRequest,
        };
        unsafe {
            instance.get(VerifyHashRequest::new)
        }
    }

    // uint64 index = 1;

    pub fn clear_index(&mut self) {
        self.index = 0;
    }

    // Param is passed by value, moved
    pub fn set_index(&mut self, v: u64) {
        self.index = v;
    }

    pub fn get_index(&self) -> u64 {
        self.index
    }

    fn get_index_for_reflect(&self) -> &u64 {
        &self.index
    }

    fn mut_index_for_reflect(&mut self) -> &mut u64 {
        &mut self.index
    }

    // bytes hash = 2;

    pub fn clear_hash(&mut self) {
        self.hash.clear();
    }

    // Param is passed by value, moved
    pub fn set_hash(&mut self, v: ::std::vec::Vec<u8>) {
        self.hash = v;
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_hash(&mut self) -> &mut ::std::vec::Vec<u8> {
        &mut self.hash
    }

    // Take field
    pub fn take_hash(&mut self) -> ::std::vec::Vec<u8> {
        ::std::mem::replace(&mut self.hash, ::std::vec::Vec::new())
    }

    pub fn get_hash(&self) -> &[u8] {
        &self.hash
    }

    fn get_hash_for_reflect(&self) -> &::std::vec::Vec<u8> {
        &self.hash
    }

    fn mut_hash_for_reflect(&mut self) -> &mut ::std::vec::Vec<u8> {
        &mut self.hash
    }
}

impl ::protobuf::Message for VerifyHashRequest {
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
                    self.index = tmp;
                },
                2 => {
                    ::protobuf::rt::read_singular_proto3_bytes_into(wire_type, is, &mut self.hash)?;
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
        if self.index != 0 {
            my_size += ::protobuf::rt::value_size(1, self.index, ::protobuf::wire_format::WireTypeVarint);
        }
        if !self.hash.is_empty() {
            my_size += ::protobuf::rt::bytes_size(2, &self.hash);
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if self.index != 0 {
            os.write_uint64(1, self.index)?;
        }
        if !self.hash.is_empty() {
            os.write_bytes(2, &self.hash)?;
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

impl ::protobuf::MessageStatic for VerifyHashRequest {
    fn new() -> VerifyHashRequest {
        VerifyHashRequest::new()
    }

    fn descriptor_static(_: ::std::option::Option<VerifyHashRequest>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "index",
                    VerifyHashRequest::get_index_for_reflect,
                    VerifyHashRequest::mut_index_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeBytes>(
                    "hash",
                    VerifyHashRequest::get_hash_for_reflect,
                    VerifyHashRequest::mut_hash_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<VerifyHashRequest>(
                    "VerifyHashRequest",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for VerifyHashRequest {
    fn clear(&mut self) {
        self.clear_index();
        self.clear_hash();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for VerifyHashRequest {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for VerifyHashRequest {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct VerifyHashResponse {
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for VerifyHashResponse {}

impl VerifyHashResponse {
    pub fn new() -> VerifyHashResponse {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static VerifyHashResponse {
        static mut instance: ::protobuf::lazy::Lazy<VerifyHashResponse> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const VerifyHashResponse,
        };
        unsafe {
            instance.get(VerifyHashResponse::new)
        }
    }
}

impl ::protobuf::Message for VerifyHashResponse {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !is.eof()? {
            let (field_number, wire_type) = is.read_tag_unpack()?;
            match field_number {
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
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
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

impl ::protobuf::MessageStatic for VerifyHashResponse {
    fn new() -> VerifyHashResponse {
        VerifyHashResponse::new()
    }

    fn descriptor_static(_: ::std::option::Option<VerifyHashResponse>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let fields = ::std::vec::Vec::new();
                ::protobuf::reflect::MessageDescriptor::new::<VerifyHashResponse>(
                    "VerifyHashResponse",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for VerifyHashResponse {
    fn clear(&mut self) {
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for VerifyHashResponse {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for VerifyHashResponse {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct AdminRequest {
    // message fields
    pub cmd_type: AdminCmdType,
    pub change_peer: ::protobuf::SingularPtrField<ChangePeerRequest>,
    pub split: ::protobuf::SingularPtrField<SplitRequest>,
    pub compact_log: ::protobuf::SingularPtrField<CompactLogRequest>,
    pub transfer_leader: ::protobuf::SingularPtrField<TransferLeaderRequest>,
    pub verify_hash: ::protobuf::SingularPtrField<VerifyHashRequest>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for AdminRequest {}

impl AdminRequest {
    pub fn new() -> AdminRequest {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static AdminRequest {
        static mut instance: ::protobuf::lazy::Lazy<AdminRequest> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const AdminRequest,
        };
        unsafe {
            instance.get(AdminRequest::new)
        }
    }

    // .raft_cmdpb.AdminCmdType cmd_type = 1;

    pub fn clear_cmd_type(&mut self) {
        self.cmd_type = AdminCmdType::InvalidAdmin;
    }

    // Param is passed by value, moved
    pub fn set_cmd_type(&mut self, v: AdminCmdType) {
        self.cmd_type = v;
    }

    pub fn get_cmd_type(&self) -> AdminCmdType {
        self.cmd_type
    }

    fn get_cmd_type_for_reflect(&self) -> &AdminCmdType {
        &self.cmd_type
    }

    fn mut_cmd_type_for_reflect(&mut self) -> &mut AdminCmdType {
        &mut self.cmd_type
    }

    // .raft_cmdpb.ChangePeerRequest change_peer = 2;

    pub fn clear_change_peer(&mut self) {
        self.change_peer.clear();
    }

    pub fn has_change_peer(&self) -> bool {
        self.change_peer.is_some()
    }

    // Param is passed by value, moved
    pub fn set_change_peer(&mut self, v: ChangePeerRequest) {
        self.change_peer = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_change_peer(&mut self) -> &mut ChangePeerRequest {
        if self.change_peer.is_none() {
            self.change_peer.set_default();
        }
        self.change_peer.as_mut().unwrap()
    }

    // Take field
    pub fn take_change_peer(&mut self) -> ChangePeerRequest {
        self.change_peer.take().unwrap_or_else(|| ChangePeerRequest::new())
    }

    pub fn get_change_peer(&self) -> &ChangePeerRequest {
        self.change_peer.as_ref().unwrap_or_else(|| ChangePeerRequest::default_instance())
    }

    fn get_change_peer_for_reflect(&self) -> &::protobuf::SingularPtrField<ChangePeerRequest> {
        &self.change_peer
    }

    fn mut_change_peer_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<ChangePeerRequest> {
        &mut self.change_peer
    }

    // .raft_cmdpb.SplitRequest split = 3;

    pub fn clear_split(&mut self) {
        self.split.clear();
    }

    pub fn has_split(&self) -> bool {
        self.split.is_some()
    }

    // Param is passed by value, moved
    pub fn set_split(&mut self, v: SplitRequest) {
        self.split = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_split(&mut self) -> &mut SplitRequest {
        if self.split.is_none() {
            self.split.set_default();
        }
        self.split.as_mut().unwrap()
    }

    // Take field
    pub fn take_split(&mut self) -> SplitRequest {
        self.split.take().unwrap_or_else(|| SplitRequest::new())
    }

    pub fn get_split(&self) -> &SplitRequest {
        self.split.as_ref().unwrap_or_else(|| SplitRequest::default_instance())
    }

    fn get_split_for_reflect(&self) -> &::protobuf::SingularPtrField<SplitRequest> {
        &self.split
    }

    fn mut_split_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<SplitRequest> {
        &mut self.split
    }

    // .raft_cmdpb.CompactLogRequest compact_log = 4;

    pub fn clear_compact_log(&mut self) {
        self.compact_log.clear();
    }

    pub fn has_compact_log(&self) -> bool {
        self.compact_log.is_some()
    }

    // Param is passed by value, moved
    pub fn set_compact_log(&mut self, v: CompactLogRequest) {
        self.compact_log = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_compact_log(&mut self) -> &mut CompactLogRequest {
        if self.compact_log.is_none() {
            self.compact_log.set_default();
        }
        self.compact_log.as_mut().unwrap()
    }

    // Take field
    pub fn take_compact_log(&mut self) -> CompactLogRequest {
        self.compact_log.take().unwrap_or_else(|| CompactLogRequest::new())
    }

    pub fn get_compact_log(&self) -> &CompactLogRequest {
        self.compact_log.as_ref().unwrap_or_else(|| CompactLogRequest::default_instance())
    }

    fn get_compact_log_for_reflect(&self) -> &::protobuf::SingularPtrField<CompactLogRequest> {
        &self.compact_log
    }

    fn mut_compact_log_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<CompactLogRequest> {
        &mut self.compact_log
    }

    // .raft_cmdpb.TransferLeaderRequest transfer_leader = 5;

    pub fn clear_transfer_leader(&mut self) {
        self.transfer_leader.clear();
    }

    pub fn has_transfer_leader(&self) -> bool {
        self.transfer_leader.is_some()
    }

    // Param is passed by value, moved
    pub fn set_transfer_leader(&mut self, v: TransferLeaderRequest) {
        self.transfer_leader = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_transfer_leader(&mut self) -> &mut TransferLeaderRequest {
        if self.transfer_leader.is_none() {
            self.transfer_leader.set_default();
        }
        self.transfer_leader.as_mut().unwrap()
    }

    // Take field
    pub fn take_transfer_leader(&mut self) -> TransferLeaderRequest {
        self.transfer_leader.take().unwrap_or_else(|| TransferLeaderRequest::new())
    }

    pub fn get_transfer_leader(&self) -> &TransferLeaderRequest {
        self.transfer_leader.as_ref().unwrap_or_else(|| TransferLeaderRequest::default_instance())
    }

    fn get_transfer_leader_for_reflect(&self) -> &::protobuf::SingularPtrField<TransferLeaderRequest> {
        &self.transfer_leader
    }

    fn mut_transfer_leader_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<TransferLeaderRequest> {
        &mut self.transfer_leader
    }

    // .raft_cmdpb.VerifyHashRequest verify_hash = 6;

    pub fn clear_verify_hash(&mut self) {
        self.verify_hash.clear();
    }

    pub fn has_verify_hash(&self) -> bool {
        self.verify_hash.is_some()
    }

    // Param is passed by value, moved
    pub fn set_verify_hash(&mut self, v: VerifyHashRequest) {
        self.verify_hash = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_verify_hash(&mut self) -> &mut VerifyHashRequest {
        if self.verify_hash.is_none() {
            self.verify_hash.set_default();
        }
        self.verify_hash.as_mut().unwrap()
    }

    // Take field
    pub fn take_verify_hash(&mut self) -> VerifyHashRequest {
        self.verify_hash.take().unwrap_or_else(|| VerifyHashRequest::new())
    }

    pub fn get_verify_hash(&self) -> &VerifyHashRequest {
        self.verify_hash.as_ref().unwrap_or_else(|| VerifyHashRequest::default_instance())
    }

    fn get_verify_hash_for_reflect(&self) -> &::protobuf::SingularPtrField<VerifyHashRequest> {
        &self.verify_hash
    }

    fn mut_verify_hash_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<VerifyHashRequest> {
        &mut self.verify_hash
    }
}

impl ::protobuf::Message for AdminRequest {
    fn is_initialized(&self) -> bool {
        for v in &self.change_peer {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.split {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.compact_log {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.transfer_leader {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.verify_hash {
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
                    self.cmd_type = tmp;
                },
                2 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.change_peer)?;
                },
                3 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.split)?;
                },
                4 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.compact_log)?;
                },
                5 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.transfer_leader)?;
                },
                6 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.verify_hash)?;
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
        if self.cmd_type != AdminCmdType::InvalidAdmin {
            my_size += ::protobuf::rt::enum_size(1, self.cmd_type);
        }
        if let Some(ref v) = self.change_peer.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if let Some(ref v) = self.split.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if let Some(ref v) = self.compact_log.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if let Some(ref v) = self.transfer_leader.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if let Some(ref v) = self.verify_hash.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if self.cmd_type != AdminCmdType::InvalidAdmin {
            os.write_enum(1, self.cmd_type.value())?;
        }
        if let Some(ref v) = self.change_peer.as_ref() {
            os.write_tag(2, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if let Some(ref v) = self.split.as_ref() {
            os.write_tag(3, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if let Some(ref v) = self.compact_log.as_ref() {
            os.write_tag(4, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if let Some(ref v) = self.transfer_leader.as_ref() {
            os.write_tag(5, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if let Some(ref v) = self.verify_hash.as_ref() {
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

impl ::protobuf::MessageStatic for AdminRequest {
    fn new() -> AdminRequest {
        AdminRequest::new()
    }

    fn descriptor_static(_: ::std::option::Option<AdminRequest>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeEnum<AdminCmdType>>(
                    "cmd_type",
                    AdminRequest::get_cmd_type_for_reflect,
                    AdminRequest::mut_cmd_type_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<ChangePeerRequest>>(
                    "change_peer",
                    AdminRequest::get_change_peer_for_reflect,
                    AdminRequest::mut_change_peer_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<SplitRequest>>(
                    "split",
                    AdminRequest::get_split_for_reflect,
                    AdminRequest::mut_split_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<CompactLogRequest>>(
                    "compact_log",
                    AdminRequest::get_compact_log_for_reflect,
                    AdminRequest::mut_compact_log_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<TransferLeaderRequest>>(
                    "transfer_leader",
                    AdminRequest::get_transfer_leader_for_reflect,
                    AdminRequest::mut_transfer_leader_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<VerifyHashRequest>>(
                    "verify_hash",
                    AdminRequest::get_verify_hash_for_reflect,
                    AdminRequest::mut_verify_hash_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<AdminRequest>(
                    "AdminRequest",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for AdminRequest {
    fn clear(&mut self) {
        self.clear_cmd_type();
        self.clear_change_peer();
        self.clear_split();
        self.clear_compact_log();
        self.clear_transfer_leader();
        self.clear_verify_hash();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for AdminRequest {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for AdminRequest {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct AdminResponse {
    // message fields
    pub cmd_type: AdminCmdType,
    pub change_peer: ::protobuf::SingularPtrField<ChangePeerResponse>,
    pub split: ::protobuf::SingularPtrField<SplitResponse>,
    pub compact_log: ::protobuf::SingularPtrField<CompactLogResponse>,
    pub transfer_leader: ::protobuf::SingularPtrField<TransferLeaderResponse>,
    pub verify_hash: ::protobuf::SingularPtrField<VerifyHashResponse>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for AdminResponse {}

impl AdminResponse {
    pub fn new() -> AdminResponse {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static AdminResponse {
        static mut instance: ::protobuf::lazy::Lazy<AdminResponse> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const AdminResponse,
        };
        unsafe {
            instance.get(AdminResponse::new)
        }
    }

    // .raft_cmdpb.AdminCmdType cmd_type = 1;

    pub fn clear_cmd_type(&mut self) {
        self.cmd_type = AdminCmdType::InvalidAdmin;
    }

    // Param is passed by value, moved
    pub fn set_cmd_type(&mut self, v: AdminCmdType) {
        self.cmd_type = v;
    }

    pub fn get_cmd_type(&self) -> AdminCmdType {
        self.cmd_type
    }

    fn get_cmd_type_for_reflect(&self) -> &AdminCmdType {
        &self.cmd_type
    }

    fn mut_cmd_type_for_reflect(&mut self) -> &mut AdminCmdType {
        &mut self.cmd_type
    }

    // .raft_cmdpb.ChangePeerResponse change_peer = 2;

    pub fn clear_change_peer(&mut self) {
        self.change_peer.clear();
    }

    pub fn has_change_peer(&self) -> bool {
        self.change_peer.is_some()
    }

    // Param is passed by value, moved
    pub fn set_change_peer(&mut self, v: ChangePeerResponse) {
        self.change_peer = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_change_peer(&mut self) -> &mut ChangePeerResponse {
        if self.change_peer.is_none() {
            self.change_peer.set_default();
        }
        self.change_peer.as_mut().unwrap()
    }

    // Take field
    pub fn take_change_peer(&mut self) -> ChangePeerResponse {
        self.change_peer.take().unwrap_or_else(|| ChangePeerResponse::new())
    }

    pub fn get_change_peer(&self) -> &ChangePeerResponse {
        self.change_peer.as_ref().unwrap_or_else(|| ChangePeerResponse::default_instance())
    }

    fn get_change_peer_for_reflect(&self) -> &::protobuf::SingularPtrField<ChangePeerResponse> {
        &self.change_peer
    }

    fn mut_change_peer_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<ChangePeerResponse> {
        &mut self.change_peer
    }

    // .raft_cmdpb.SplitResponse split = 3;

    pub fn clear_split(&mut self) {
        self.split.clear();
    }

    pub fn has_split(&self) -> bool {
        self.split.is_some()
    }

    // Param is passed by value, moved
    pub fn set_split(&mut self, v: SplitResponse) {
        self.split = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_split(&mut self) -> &mut SplitResponse {
        if self.split.is_none() {
            self.split.set_default();
        }
        self.split.as_mut().unwrap()
    }

    // Take field
    pub fn take_split(&mut self) -> SplitResponse {
        self.split.take().unwrap_or_else(|| SplitResponse::new())
    }

    pub fn get_split(&self) -> &SplitResponse {
        self.split.as_ref().unwrap_or_else(|| SplitResponse::default_instance())
    }

    fn get_split_for_reflect(&self) -> &::protobuf::SingularPtrField<SplitResponse> {
        &self.split
    }

    fn mut_split_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<SplitResponse> {
        &mut self.split
    }

    // .raft_cmdpb.CompactLogResponse compact_log = 4;

    pub fn clear_compact_log(&mut self) {
        self.compact_log.clear();
    }

    pub fn has_compact_log(&self) -> bool {
        self.compact_log.is_some()
    }

    // Param is passed by value, moved
    pub fn set_compact_log(&mut self, v: CompactLogResponse) {
        self.compact_log = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_compact_log(&mut self) -> &mut CompactLogResponse {
        if self.compact_log.is_none() {
            self.compact_log.set_default();
        }
        self.compact_log.as_mut().unwrap()
    }

    // Take field
    pub fn take_compact_log(&mut self) -> CompactLogResponse {
        self.compact_log.take().unwrap_or_else(|| CompactLogResponse::new())
    }

    pub fn get_compact_log(&self) -> &CompactLogResponse {
        self.compact_log.as_ref().unwrap_or_else(|| CompactLogResponse::default_instance())
    }

    fn get_compact_log_for_reflect(&self) -> &::protobuf::SingularPtrField<CompactLogResponse> {
        &self.compact_log
    }

    fn mut_compact_log_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<CompactLogResponse> {
        &mut self.compact_log
    }

    // .raft_cmdpb.TransferLeaderResponse transfer_leader = 5;

    pub fn clear_transfer_leader(&mut self) {
        self.transfer_leader.clear();
    }

    pub fn has_transfer_leader(&self) -> bool {
        self.transfer_leader.is_some()
    }

    // Param is passed by value, moved
    pub fn set_transfer_leader(&mut self, v: TransferLeaderResponse) {
        self.transfer_leader = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_transfer_leader(&mut self) -> &mut TransferLeaderResponse {
        if self.transfer_leader.is_none() {
            self.transfer_leader.set_default();
        }
        self.transfer_leader.as_mut().unwrap()
    }

    // Take field
    pub fn take_transfer_leader(&mut self) -> TransferLeaderResponse {
        self.transfer_leader.take().unwrap_or_else(|| TransferLeaderResponse::new())
    }

    pub fn get_transfer_leader(&self) -> &TransferLeaderResponse {
        self.transfer_leader.as_ref().unwrap_or_else(|| TransferLeaderResponse::default_instance())
    }

    fn get_transfer_leader_for_reflect(&self) -> &::protobuf::SingularPtrField<TransferLeaderResponse> {
        &self.transfer_leader
    }

    fn mut_transfer_leader_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<TransferLeaderResponse> {
        &mut self.transfer_leader
    }

    // .raft_cmdpb.VerifyHashResponse verify_hash = 6;

    pub fn clear_verify_hash(&mut self) {
        self.verify_hash.clear();
    }

    pub fn has_verify_hash(&self) -> bool {
        self.verify_hash.is_some()
    }

    // Param is passed by value, moved
    pub fn set_verify_hash(&mut self, v: VerifyHashResponse) {
        self.verify_hash = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_verify_hash(&mut self) -> &mut VerifyHashResponse {
        if self.verify_hash.is_none() {
            self.verify_hash.set_default();
        }
        self.verify_hash.as_mut().unwrap()
    }

    // Take field
    pub fn take_verify_hash(&mut self) -> VerifyHashResponse {
        self.verify_hash.take().unwrap_or_else(|| VerifyHashResponse::new())
    }

    pub fn get_verify_hash(&self) -> &VerifyHashResponse {
        self.verify_hash.as_ref().unwrap_or_else(|| VerifyHashResponse::default_instance())
    }

    fn get_verify_hash_for_reflect(&self) -> &::protobuf::SingularPtrField<VerifyHashResponse> {
        &self.verify_hash
    }

    fn mut_verify_hash_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<VerifyHashResponse> {
        &mut self.verify_hash
    }
}

impl ::protobuf::Message for AdminResponse {
    fn is_initialized(&self) -> bool {
        for v in &self.change_peer {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.split {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.compact_log {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.transfer_leader {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.verify_hash {
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
                    self.cmd_type = tmp;
                },
                2 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.change_peer)?;
                },
                3 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.split)?;
                },
                4 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.compact_log)?;
                },
                5 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.transfer_leader)?;
                },
                6 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.verify_hash)?;
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
        if self.cmd_type != AdminCmdType::InvalidAdmin {
            my_size += ::protobuf::rt::enum_size(1, self.cmd_type);
        }
        if let Some(ref v) = self.change_peer.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if let Some(ref v) = self.split.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if let Some(ref v) = self.compact_log.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if let Some(ref v) = self.transfer_leader.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if let Some(ref v) = self.verify_hash.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if self.cmd_type != AdminCmdType::InvalidAdmin {
            os.write_enum(1, self.cmd_type.value())?;
        }
        if let Some(ref v) = self.change_peer.as_ref() {
            os.write_tag(2, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if let Some(ref v) = self.split.as_ref() {
            os.write_tag(3, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if let Some(ref v) = self.compact_log.as_ref() {
            os.write_tag(4, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if let Some(ref v) = self.transfer_leader.as_ref() {
            os.write_tag(5, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if let Some(ref v) = self.verify_hash.as_ref() {
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

impl ::protobuf::MessageStatic for AdminResponse {
    fn new() -> AdminResponse {
        AdminResponse::new()
    }

    fn descriptor_static(_: ::std::option::Option<AdminResponse>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeEnum<AdminCmdType>>(
                    "cmd_type",
                    AdminResponse::get_cmd_type_for_reflect,
                    AdminResponse::mut_cmd_type_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<ChangePeerResponse>>(
                    "change_peer",
                    AdminResponse::get_change_peer_for_reflect,
                    AdminResponse::mut_change_peer_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<SplitResponse>>(
                    "split",
                    AdminResponse::get_split_for_reflect,
                    AdminResponse::mut_split_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<CompactLogResponse>>(
                    "compact_log",
                    AdminResponse::get_compact_log_for_reflect,
                    AdminResponse::mut_compact_log_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<TransferLeaderResponse>>(
                    "transfer_leader",
                    AdminResponse::get_transfer_leader_for_reflect,
                    AdminResponse::mut_transfer_leader_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<VerifyHashResponse>>(
                    "verify_hash",
                    AdminResponse::get_verify_hash_for_reflect,
                    AdminResponse::mut_verify_hash_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<AdminResponse>(
                    "AdminResponse",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for AdminResponse {
    fn clear(&mut self) {
        self.clear_cmd_type();
        self.clear_change_peer();
        self.clear_split();
        self.clear_compact_log();
        self.clear_transfer_leader();
        self.clear_verify_hash();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for AdminResponse {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for AdminResponse {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct RegionLeaderRequest {
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for RegionLeaderRequest {}

impl RegionLeaderRequest {
    pub fn new() -> RegionLeaderRequest {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static RegionLeaderRequest {
        static mut instance: ::protobuf::lazy::Lazy<RegionLeaderRequest> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const RegionLeaderRequest,
        };
        unsafe {
            instance.get(RegionLeaderRequest::new)
        }
    }
}

impl ::protobuf::Message for RegionLeaderRequest {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !is.eof()? {
            let (field_number, wire_type) = is.read_tag_unpack()?;
            match field_number {
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
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
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

impl ::protobuf::MessageStatic for RegionLeaderRequest {
    fn new() -> RegionLeaderRequest {
        RegionLeaderRequest::new()
    }

    fn descriptor_static(_: ::std::option::Option<RegionLeaderRequest>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let fields = ::std::vec::Vec::new();
                ::protobuf::reflect::MessageDescriptor::new::<RegionLeaderRequest>(
                    "RegionLeaderRequest",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for RegionLeaderRequest {
    fn clear(&mut self) {
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for RegionLeaderRequest {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for RegionLeaderRequest {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct RegionLeaderResponse {
    // message fields
    pub leader: ::protobuf::SingularPtrField<super::metapb::Peer>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for RegionLeaderResponse {}

impl RegionLeaderResponse {
    pub fn new() -> RegionLeaderResponse {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static RegionLeaderResponse {
        static mut instance: ::protobuf::lazy::Lazy<RegionLeaderResponse> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const RegionLeaderResponse,
        };
        unsafe {
            instance.get(RegionLeaderResponse::new)
        }
    }

    // .metapb.Peer leader = 1;

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

impl ::protobuf::Message for RegionLeaderResponse {
    fn is_initialized(&self) -> bool {
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
        if let Some(ref v) = self.leader.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(ref v) = self.leader.as_ref() {
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

impl ::protobuf::MessageStatic for RegionLeaderResponse {
    fn new() -> RegionLeaderResponse {
        RegionLeaderResponse::new()
    }

    fn descriptor_static(_: ::std::option::Option<RegionLeaderResponse>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<super::metapb::Peer>>(
                    "leader",
                    RegionLeaderResponse::get_leader_for_reflect,
                    RegionLeaderResponse::mut_leader_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<RegionLeaderResponse>(
                    "RegionLeaderResponse",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for RegionLeaderResponse {
    fn clear(&mut self) {
        self.clear_leader();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for RegionLeaderResponse {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for RegionLeaderResponse {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct RegionDetailRequest {
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for RegionDetailRequest {}

impl RegionDetailRequest {
    pub fn new() -> RegionDetailRequest {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static RegionDetailRequest {
        static mut instance: ::protobuf::lazy::Lazy<RegionDetailRequest> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const RegionDetailRequest,
        };
        unsafe {
            instance.get(RegionDetailRequest::new)
        }
    }
}

impl ::protobuf::Message for RegionDetailRequest {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !is.eof()? {
            let (field_number, wire_type) = is.read_tag_unpack()?;
            match field_number {
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
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
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

impl ::protobuf::MessageStatic for RegionDetailRequest {
    fn new() -> RegionDetailRequest {
        RegionDetailRequest::new()
    }

    fn descriptor_static(_: ::std::option::Option<RegionDetailRequest>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let fields = ::std::vec::Vec::new();
                ::protobuf::reflect::MessageDescriptor::new::<RegionDetailRequest>(
                    "RegionDetailRequest",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for RegionDetailRequest {
    fn clear(&mut self) {
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for RegionDetailRequest {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for RegionDetailRequest {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct RegionDetailResponse {
    // message fields
    pub region: ::protobuf::SingularPtrField<super::metapb::Region>,
    pub leader: ::protobuf::SingularPtrField<super::metapb::Peer>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for RegionDetailResponse {}

impl RegionDetailResponse {
    pub fn new() -> RegionDetailResponse {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static RegionDetailResponse {
        static mut instance: ::protobuf::lazy::Lazy<RegionDetailResponse> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const RegionDetailResponse,
        };
        unsafe {
            instance.get(RegionDetailResponse::new)
        }
    }

    // .metapb.Region region = 1;

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

    // .metapb.Peer leader = 2;

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

impl ::protobuf::Message for RegionDetailResponse {
    fn is_initialized(&self) -> bool {
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
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.region)?;
                },
                2 => {
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
        if let Some(ref v) = self.region.as_ref() {
            os.write_tag(1, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if let Some(ref v) = self.leader.as_ref() {
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

impl ::protobuf::MessageStatic for RegionDetailResponse {
    fn new() -> RegionDetailResponse {
        RegionDetailResponse::new()
    }

    fn descriptor_static(_: ::std::option::Option<RegionDetailResponse>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<super::metapb::Region>>(
                    "region",
                    RegionDetailResponse::get_region_for_reflect,
                    RegionDetailResponse::mut_region_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<super::metapb::Peer>>(
                    "leader",
                    RegionDetailResponse::get_leader_for_reflect,
                    RegionDetailResponse::mut_leader_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<RegionDetailResponse>(
                    "RegionDetailResponse",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for RegionDetailResponse {
    fn clear(&mut self) {
        self.clear_region();
        self.clear_leader();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for RegionDetailResponse {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for RegionDetailResponse {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct StatusRequest {
    // message fields
    pub cmd_type: StatusCmdType,
    pub region_leader: ::protobuf::SingularPtrField<RegionLeaderRequest>,
    pub region_detail: ::protobuf::SingularPtrField<RegionDetailRequest>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for StatusRequest {}

impl StatusRequest {
    pub fn new() -> StatusRequest {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static StatusRequest {
        static mut instance: ::protobuf::lazy::Lazy<StatusRequest> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const StatusRequest,
        };
        unsafe {
            instance.get(StatusRequest::new)
        }
    }

    // .raft_cmdpb.StatusCmdType cmd_type = 1;

    pub fn clear_cmd_type(&mut self) {
        self.cmd_type = StatusCmdType::InvalidStatus;
    }

    // Param is passed by value, moved
    pub fn set_cmd_type(&mut self, v: StatusCmdType) {
        self.cmd_type = v;
    }

    pub fn get_cmd_type(&self) -> StatusCmdType {
        self.cmd_type
    }

    fn get_cmd_type_for_reflect(&self) -> &StatusCmdType {
        &self.cmd_type
    }

    fn mut_cmd_type_for_reflect(&mut self) -> &mut StatusCmdType {
        &mut self.cmd_type
    }

    // .raft_cmdpb.RegionLeaderRequest region_leader = 2;

    pub fn clear_region_leader(&mut self) {
        self.region_leader.clear();
    }

    pub fn has_region_leader(&self) -> bool {
        self.region_leader.is_some()
    }

    // Param is passed by value, moved
    pub fn set_region_leader(&mut self, v: RegionLeaderRequest) {
        self.region_leader = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_region_leader(&mut self) -> &mut RegionLeaderRequest {
        if self.region_leader.is_none() {
            self.region_leader.set_default();
        }
        self.region_leader.as_mut().unwrap()
    }

    // Take field
    pub fn take_region_leader(&mut self) -> RegionLeaderRequest {
        self.region_leader.take().unwrap_or_else(|| RegionLeaderRequest::new())
    }

    pub fn get_region_leader(&self) -> &RegionLeaderRequest {
        self.region_leader.as_ref().unwrap_or_else(|| RegionLeaderRequest::default_instance())
    }

    fn get_region_leader_for_reflect(&self) -> &::protobuf::SingularPtrField<RegionLeaderRequest> {
        &self.region_leader
    }

    fn mut_region_leader_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<RegionLeaderRequest> {
        &mut self.region_leader
    }

    // .raft_cmdpb.RegionDetailRequest region_detail = 3;

    pub fn clear_region_detail(&mut self) {
        self.region_detail.clear();
    }

    pub fn has_region_detail(&self) -> bool {
        self.region_detail.is_some()
    }

    // Param is passed by value, moved
    pub fn set_region_detail(&mut self, v: RegionDetailRequest) {
        self.region_detail = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_region_detail(&mut self) -> &mut RegionDetailRequest {
        if self.region_detail.is_none() {
            self.region_detail.set_default();
        }
        self.region_detail.as_mut().unwrap()
    }

    // Take field
    pub fn take_region_detail(&mut self) -> RegionDetailRequest {
        self.region_detail.take().unwrap_or_else(|| RegionDetailRequest::new())
    }

    pub fn get_region_detail(&self) -> &RegionDetailRequest {
        self.region_detail.as_ref().unwrap_or_else(|| RegionDetailRequest::default_instance())
    }

    fn get_region_detail_for_reflect(&self) -> &::protobuf::SingularPtrField<RegionDetailRequest> {
        &self.region_detail
    }

    fn mut_region_detail_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<RegionDetailRequest> {
        &mut self.region_detail
    }
}

impl ::protobuf::Message for StatusRequest {
    fn is_initialized(&self) -> bool {
        for v in &self.region_leader {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.region_detail {
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
                    self.cmd_type = tmp;
                },
                2 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.region_leader)?;
                },
                3 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.region_detail)?;
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
        if self.cmd_type != StatusCmdType::InvalidStatus {
            my_size += ::protobuf::rt::enum_size(1, self.cmd_type);
        }
        if let Some(ref v) = self.region_leader.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if let Some(ref v) = self.region_detail.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if self.cmd_type != StatusCmdType::InvalidStatus {
            os.write_enum(1, self.cmd_type.value())?;
        }
        if let Some(ref v) = self.region_leader.as_ref() {
            os.write_tag(2, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if let Some(ref v) = self.region_detail.as_ref() {
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

impl ::protobuf::MessageStatic for StatusRequest {
    fn new() -> StatusRequest {
        StatusRequest::new()
    }

    fn descriptor_static(_: ::std::option::Option<StatusRequest>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeEnum<StatusCmdType>>(
                    "cmd_type",
                    StatusRequest::get_cmd_type_for_reflect,
                    StatusRequest::mut_cmd_type_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<RegionLeaderRequest>>(
                    "region_leader",
                    StatusRequest::get_region_leader_for_reflect,
                    StatusRequest::mut_region_leader_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<RegionDetailRequest>>(
                    "region_detail",
                    StatusRequest::get_region_detail_for_reflect,
                    StatusRequest::mut_region_detail_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<StatusRequest>(
                    "StatusRequest",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for StatusRequest {
    fn clear(&mut self) {
        self.clear_cmd_type();
        self.clear_region_leader();
        self.clear_region_detail();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for StatusRequest {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for StatusRequest {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct StatusResponse {
    // message fields
    pub cmd_type: StatusCmdType,
    pub region_leader: ::protobuf::SingularPtrField<RegionLeaderResponse>,
    pub region_detail: ::protobuf::SingularPtrField<RegionDetailResponse>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for StatusResponse {}

impl StatusResponse {
    pub fn new() -> StatusResponse {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static StatusResponse {
        static mut instance: ::protobuf::lazy::Lazy<StatusResponse> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const StatusResponse,
        };
        unsafe {
            instance.get(StatusResponse::new)
        }
    }

    // .raft_cmdpb.StatusCmdType cmd_type = 1;

    pub fn clear_cmd_type(&mut self) {
        self.cmd_type = StatusCmdType::InvalidStatus;
    }

    // Param is passed by value, moved
    pub fn set_cmd_type(&mut self, v: StatusCmdType) {
        self.cmd_type = v;
    }

    pub fn get_cmd_type(&self) -> StatusCmdType {
        self.cmd_type
    }

    fn get_cmd_type_for_reflect(&self) -> &StatusCmdType {
        &self.cmd_type
    }

    fn mut_cmd_type_for_reflect(&mut self) -> &mut StatusCmdType {
        &mut self.cmd_type
    }

    // .raft_cmdpb.RegionLeaderResponse region_leader = 2;

    pub fn clear_region_leader(&mut self) {
        self.region_leader.clear();
    }

    pub fn has_region_leader(&self) -> bool {
        self.region_leader.is_some()
    }

    // Param is passed by value, moved
    pub fn set_region_leader(&mut self, v: RegionLeaderResponse) {
        self.region_leader = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_region_leader(&mut self) -> &mut RegionLeaderResponse {
        if self.region_leader.is_none() {
            self.region_leader.set_default();
        }
        self.region_leader.as_mut().unwrap()
    }

    // Take field
    pub fn take_region_leader(&mut self) -> RegionLeaderResponse {
        self.region_leader.take().unwrap_or_else(|| RegionLeaderResponse::new())
    }

    pub fn get_region_leader(&self) -> &RegionLeaderResponse {
        self.region_leader.as_ref().unwrap_or_else(|| RegionLeaderResponse::default_instance())
    }

    fn get_region_leader_for_reflect(&self) -> &::protobuf::SingularPtrField<RegionLeaderResponse> {
        &self.region_leader
    }

    fn mut_region_leader_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<RegionLeaderResponse> {
        &mut self.region_leader
    }

    // .raft_cmdpb.RegionDetailResponse region_detail = 3;

    pub fn clear_region_detail(&mut self) {
        self.region_detail.clear();
    }

    pub fn has_region_detail(&self) -> bool {
        self.region_detail.is_some()
    }

    // Param is passed by value, moved
    pub fn set_region_detail(&mut self, v: RegionDetailResponse) {
        self.region_detail = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_region_detail(&mut self) -> &mut RegionDetailResponse {
        if self.region_detail.is_none() {
            self.region_detail.set_default();
        }
        self.region_detail.as_mut().unwrap()
    }

    // Take field
    pub fn take_region_detail(&mut self) -> RegionDetailResponse {
        self.region_detail.take().unwrap_or_else(|| RegionDetailResponse::new())
    }

    pub fn get_region_detail(&self) -> &RegionDetailResponse {
        self.region_detail.as_ref().unwrap_or_else(|| RegionDetailResponse::default_instance())
    }

    fn get_region_detail_for_reflect(&self) -> &::protobuf::SingularPtrField<RegionDetailResponse> {
        &self.region_detail
    }

    fn mut_region_detail_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<RegionDetailResponse> {
        &mut self.region_detail
    }
}

impl ::protobuf::Message for StatusResponse {
    fn is_initialized(&self) -> bool {
        for v in &self.region_leader {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.region_detail {
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
                    self.cmd_type = tmp;
                },
                2 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.region_leader)?;
                },
                3 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.region_detail)?;
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
        if self.cmd_type != StatusCmdType::InvalidStatus {
            my_size += ::protobuf::rt::enum_size(1, self.cmd_type);
        }
        if let Some(ref v) = self.region_leader.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if let Some(ref v) = self.region_detail.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if self.cmd_type != StatusCmdType::InvalidStatus {
            os.write_enum(1, self.cmd_type.value())?;
        }
        if let Some(ref v) = self.region_leader.as_ref() {
            os.write_tag(2, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if let Some(ref v) = self.region_detail.as_ref() {
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

impl ::protobuf::MessageStatic for StatusResponse {
    fn new() -> StatusResponse {
        StatusResponse::new()
    }

    fn descriptor_static(_: ::std::option::Option<StatusResponse>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeEnum<StatusCmdType>>(
                    "cmd_type",
                    StatusResponse::get_cmd_type_for_reflect,
                    StatusResponse::mut_cmd_type_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<RegionLeaderResponse>>(
                    "region_leader",
                    StatusResponse::get_region_leader_for_reflect,
                    StatusResponse::mut_region_leader_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<RegionDetailResponse>>(
                    "region_detail",
                    StatusResponse::get_region_detail_for_reflect,
                    StatusResponse::mut_region_detail_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<StatusResponse>(
                    "StatusResponse",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for StatusResponse {
    fn clear(&mut self) {
        self.clear_cmd_type();
        self.clear_region_leader();
        self.clear_region_detail();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for StatusResponse {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for StatusResponse {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct RaftRequestHeader {
    // message fields
    pub region_id: u64,
    pub peer: ::protobuf::SingularPtrField<super::metapb::Peer>,
    pub read_quorum: bool,
    pub uuid: ::std::vec::Vec<u8>,
    pub region_epoch: ::protobuf::SingularPtrField<super::metapb::RegionEpoch>,
    pub term: u64,
    pub sync_log: bool,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for RaftRequestHeader {}

impl RaftRequestHeader {
    pub fn new() -> RaftRequestHeader {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static RaftRequestHeader {
        static mut instance: ::protobuf::lazy::Lazy<RaftRequestHeader> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const RaftRequestHeader,
        };
        unsafe {
            instance.get(RaftRequestHeader::new)
        }
    }

    // uint64 region_id = 1;

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

    // .metapb.Peer peer = 2;

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

    // bool read_quorum = 3;

    pub fn clear_read_quorum(&mut self) {
        self.read_quorum = false;
    }

    // Param is passed by value, moved
    pub fn set_read_quorum(&mut self, v: bool) {
        self.read_quorum = v;
    }

    pub fn get_read_quorum(&self) -> bool {
        self.read_quorum
    }

    fn get_read_quorum_for_reflect(&self) -> &bool {
        &self.read_quorum
    }

    fn mut_read_quorum_for_reflect(&mut self) -> &mut bool {
        &mut self.read_quorum
    }

    // bytes uuid = 4;

    pub fn clear_uuid(&mut self) {
        self.uuid.clear();
    }

    // Param is passed by value, moved
    pub fn set_uuid(&mut self, v: ::std::vec::Vec<u8>) {
        self.uuid = v;
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_uuid(&mut self) -> &mut ::std::vec::Vec<u8> {
        &mut self.uuid
    }

    // Take field
    pub fn take_uuid(&mut self) -> ::std::vec::Vec<u8> {
        ::std::mem::replace(&mut self.uuid, ::std::vec::Vec::new())
    }

    pub fn get_uuid(&self) -> &[u8] {
        &self.uuid
    }

    fn get_uuid_for_reflect(&self) -> &::std::vec::Vec<u8> {
        &self.uuid
    }

    fn mut_uuid_for_reflect(&mut self) -> &mut ::std::vec::Vec<u8> {
        &mut self.uuid
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

    // uint64 term = 6;

    pub fn clear_term(&mut self) {
        self.term = 0;
    }

    // Param is passed by value, moved
    pub fn set_term(&mut self, v: u64) {
        self.term = v;
    }

    pub fn get_term(&self) -> u64 {
        self.term
    }

    fn get_term_for_reflect(&self) -> &u64 {
        &self.term
    }

    fn mut_term_for_reflect(&mut self) -> &mut u64 {
        &mut self.term
    }

    // bool sync_log = 7;

    pub fn clear_sync_log(&mut self) {
        self.sync_log = false;
    }

    // Param is passed by value, moved
    pub fn set_sync_log(&mut self, v: bool) {
        self.sync_log = v;
    }

    pub fn get_sync_log(&self) -> bool {
        self.sync_log
    }

    fn get_sync_log_for_reflect(&self) -> &bool {
        &self.sync_log
    }

    fn mut_sync_log_for_reflect(&mut self) -> &mut bool {
        &mut self.sync_log
    }
}

impl ::protobuf::Message for RaftRequestHeader {
    fn is_initialized(&self) -> bool {
        for v in &self.peer {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.region_epoch {
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
                    self.region_id = tmp;
                },
                2 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.peer)?;
                },
                3 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_bool()?;
                    self.read_quorum = tmp;
                },
                4 => {
                    ::protobuf::rt::read_singular_proto3_bytes_into(wire_type, is, &mut self.uuid)?;
                },
                5 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.region_epoch)?;
                },
                6 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_uint64()?;
                    self.term = tmp;
                },
                7 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_bool()?;
                    self.sync_log = tmp;
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
        if self.region_id != 0 {
            my_size += ::protobuf::rt::value_size(1, self.region_id, ::protobuf::wire_format::WireTypeVarint);
        }
        if let Some(ref v) = self.peer.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if self.read_quorum != false {
            my_size += 2;
        }
        if !self.uuid.is_empty() {
            my_size += ::protobuf::rt::bytes_size(4, &self.uuid);
        }
        if let Some(ref v) = self.region_epoch.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if self.term != 0 {
            my_size += ::protobuf::rt::value_size(6, self.term, ::protobuf::wire_format::WireTypeVarint);
        }
        if self.sync_log != false {
            my_size += 2;
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if self.region_id != 0 {
            os.write_uint64(1, self.region_id)?;
        }
        if let Some(ref v) = self.peer.as_ref() {
            os.write_tag(2, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if self.read_quorum != false {
            os.write_bool(3, self.read_quorum)?;
        }
        if !self.uuid.is_empty() {
            os.write_bytes(4, &self.uuid)?;
        }
        if let Some(ref v) = self.region_epoch.as_ref() {
            os.write_tag(5, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if self.term != 0 {
            os.write_uint64(6, self.term)?;
        }
        if self.sync_log != false {
            os.write_bool(7, self.sync_log)?;
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

impl ::protobuf::MessageStatic for RaftRequestHeader {
    fn new() -> RaftRequestHeader {
        RaftRequestHeader::new()
    }

    fn descriptor_static(_: ::std::option::Option<RaftRequestHeader>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "region_id",
                    RaftRequestHeader::get_region_id_for_reflect,
                    RaftRequestHeader::mut_region_id_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<super::metapb::Peer>>(
                    "peer",
                    RaftRequestHeader::get_peer_for_reflect,
                    RaftRequestHeader::mut_peer_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeBool>(
                    "read_quorum",
                    RaftRequestHeader::get_read_quorum_for_reflect,
                    RaftRequestHeader::mut_read_quorum_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeBytes>(
                    "uuid",
                    RaftRequestHeader::get_uuid_for_reflect,
                    RaftRequestHeader::mut_uuid_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<super::metapb::RegionEpoch>>(
                    "region_epoch",
                    RaftRequestHeader::get_region_epoch_for_reflect,
                    RaftRequestHeader::mut_region_epoch_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "term",
                    RaftRequestHeader::get_term_for_reflect,
                    RaftRequestHeader::mut_term_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeBool>(
                    "sync_log",
                    RaftRequestHeader::get_sync_log_for_reflect,
                    RaftRequestHeader::mut_sync_log_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<RaftRequestHeader>(
                    "RaftRequestHeader",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for RaftRequestHeader {
    fn clear(&mut self) {
        self.clear_region_id();
        self.clear_peer();
        self.clear_read_quorum();
        self.clear_uuid();
        self.clear_region_epoch();
        self.clear_term();
        self.clear_sync_log();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for RaftRequestHeader {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for RaftRequestHeader {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct RaftResponseHeader {
    // message fields
    pub error: ::protobuf::SingularPtrField<super::errorpb::Error>,
    pub uuid: ::std::vec::Vec<u8>,
    pub current_term: u64,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for RaftResponseHeader {}

impl RaftResponseHeader {
    pub fn new() -> RaftResponseHeader {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static RaftResponseHeader {
        static mut instance: ::protobuf::lazy::Lazy<RaftResponseHeader> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const RaftResponseHeader,
        };
        unsafe {
            instance.get(RaftResponseHeader::new)
        }
    }

    // .errorpb.Error error = 1;

    pub fn clear_error(&mut self) {
        self.error.clear();
    }

    pub fn has_error(&self) -> bool {
        self.error.is_some()
    }

    // Param is passed by value, moved
    pub fn set_error(&mut self, v: super::errorpb::Error) {
        self.error = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_error(&mut self) -> &mut super::errorpb::Error {
        if self.error.is_none() {
            self.error.set_default();
        }
        self.error.as_mut().unwrap()
    }

    // Take field
    pub fn take_error(&mut self) -> super::errorpb::Error {
        self.error.take().unwrap_or_else(|| super::errorpb::Error::new())
    }

    pub fn get_error(&self) -> &super::errorpb::Error {
        self.error.as_ref().unwrap_or_else(|| super::errorpb::Error::default_instance())
    }

    fn get_error_for_reflect(&self) -> &::protobuf::SingularPtrField<super::errorpb::Error> {
        &self.error
    }

    fn mut_error_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<super::errorpb::Error> {
        &mut self.error
    }

    // bytes uuid = 2;

    pub fn clear_uuid(&mut self) {
        self.uuid.clear();
    }

    // Param is passed by value, moved
    pub fn set_uuid(&mut self, v: ::std::vec::Vec<u8>) {
        self.uuid = v;
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_uuid(&mut self) -> &mut ::std::vec::Vec<u8> {
        &mut self.uuid
    }

    // Take field
    pub fn take_uuid(&mut self) -> ::std::vec::Vec<u8> {
        ::std::mem::replace(&mut self.uuid, ::std::vec::Vec::new())
    }

    pub fn get_uuid(&self) -> &[u8] {
        &self.uuid
    }

    fn get_uuid_for_reflect(&self) -> &::std::vec::Vec<u8> {
        &self.uuid
    }

    fn mut_uuid_for_reflect(&mut self) -> &mut ::std::vec::Vec<u8> {
        &mut self.uuid
    }

    // uint64 current_term = 3;

    pub fn clear_current_term(&mut self) {
        self.current_term = 0;
    }

    // Param is passed by value, moved
    pub fn set_current_term(&mut self, v: u64) {
        self.current_term = v;
    }

    pub fn get_current_term(&self) -> u64 {
        self.current_term
    }

    fn get_current_term_for_reflect(&self) -> &u64 {
        &self.current_term
    }

    fn mut_current_term_for_reflect(&mut self) -> &mut u64 {
        &mut self.current_term
    }
}

impl ::protobuf::Message for RaftResponseHeader {
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
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.error)?;
                },
                2 => {
                    ::protobuf::rt::read_singular_proto3_bytes_into(wire_type, is, &mut self.uuid)?;
                },
                3 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_uint64()?;
                    self.current_term = tmp;
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
        if !self.uuid.is_empty() {
            my_size += ::protobuf::rt::bytes_size(2, &self.uuid);
        }
        if self.current_term != 0 {
            my_size += ::protobuf::rt::value_size(3, self.current_term, ::protobuf::wire_format::WireTypeVarint);
        }
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
        if !self.uuid.is_empty() {
            os.write_bytes(2, &self.uuid)?;
        }
        if self.current_term != 0 {
            os.write_uint64(3, self.current_term)?;
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

impl ::protobuf::MessageStatic for RaftResponseHeader {
    fn new() -> RaftResponseHeader {
        RaftResponseHeader::new()
    }

    fn descriptor_static(_: ::std::option::Option<RaftResponseHeader>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<super::errorpb::Error>>(
                    "error",
                    RaftResponseHeader::get_error_for_reflect,
                    RaftResponseHeader::mut_error_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeBytes>(
                    "uuid",
                    RaftResponseHeader::get_uuid_for_reflect,
                    RaftResponseHeader::mut_uuid_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "current_term",
                    RaftResponseHeader::get_current_term_for_reflect,
                    RaftResponseHeader::mut_current_term_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<RaftResponseHeader>(
                    "RaftResponseHeader",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for RaftResponseHeader {
    fn clear(&mut self) {
        self.clear_error();
        self.clear_uuid();
        self.clear_current_term();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for RaftResponseHeader {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for RaftResponseHeader {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct RaftCmdRequest {
    // message fields
    pub header: ::protobuf::SingularPtrField<RaftRequestHeader>,
    pub requests: ::protobuf::RepeatedField<Request>,
    pub admin_request: ::protobuf::SingularPtrField<AdminRequest>,
    pub status_request: ::protobuf::SingularPtrField<StatusRequest>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for RaftCmdRequest {}

impl RaftCmdRequest {
    pub fn new() -> RaftCmdRequest {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static RaftCmdRequest {
        static mut instance: ::protobuf::lazy::Lazy<RaftCmdRequest> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const RaftCmdRequest,
        };
        unsafe {
            instance.get(RaftCmdRequest::new)
        }
    }

    // .raft_cmdpb.RaftRequestHeader header = 1;

    pub fn clear_header(&mut self) {
        self.header.clear();
    }

    pub fn has_header(&self) -> bool {
        self.header.is_some()
    }

    // Param is passed by value, moved
    pub fn set_header(&mut self, v: RaftRequestHeader) {
        self.header = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_header(&mut self) -> &mut RaftRequestHeader {
        if self.header.is_none() {
            self.header.set_default();
        }
        self.header.as_mut().unwrap()
    }

    // Take field
    pub fn take_header(&mut self) -> RaftRequestHeader {
        self.header.take().unwrap_or_else(|| RaftRequestHeader::new())
    }

    pub fn get_header(&self) -> &RaftRequestHeader {
        self.header.as_ref().unwrap_or_else(|| RaftRequestHeader::default_instance())
    }

    fn get_header_for_reflect(&self) -> &::protobuf::SingularPtrField<RaftRequestHeader> {
        &self.header
    }

    fn mut_header_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<RaftRequestHeader> {
        &mut self.header
    }

    // repeated .raft_cmdpb.Request requests = 2;

    pub fn clear_requests(&mut self) {
        self.requests.clear();
    }

    // Param is passed by value, moved
    pub fn set_requests(&mut self, v: ::protobuf::RepeatedField<Request>) {
        self.requests = v;
    }

    // Mutable pointer to the field.
    pub fn mut_requests(&mut self) -> &mut ::protobuf::RepeatedField<Request> {
        &mut self.requests
    }

    // Take field
    pub fn take_requests(&mut self) -> ::protobuf::RepeatedField<Request> {
        ::std::mem::replace(&mut self.requests, ::protobuf::RepeatedField::new())
    }

    pub fn get_requests(&self) -> &[Request] {
        &self.requests
    }

    fn get_requests_for_reflect(&self) -> &::protobuf::RepeatedField<Request> {
        &self.requests
    }

    fn mut_requests_for_reflect(&mut self) -> &mut ::protobuf::RepeatedField<Request> {
        &mut self.requests
    }

    // .raft_cmdpb.AdminRequest admin_request = 3;

    pub fn clear_admin_request(&mut self) {
        self.admin_request.clear();
    }

    pub fn has_admin_request(&self) -> bool {
        self.admin_request.is_some()
    }

    // Param is passed by value, moved
    pub fn set_admin_request(&mut self, v: AdminRequest) {
        self.admin_request = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_admin_request(&mut self) -> &mut AdminRequest {
        if self.admin_request.is_none() {
            self.admin_request.set_default();
        }
        self.admin_request.as_mut().unwrap()
    }

    // Take field
    pub fn take_admin_request(&mut self) -> AdminRequest {
        self.admin_request.take().unwrap_or_else(|| AdminRequest::new())
    }

    pub fn get_admin_request(&self) -> &AdminRequest {
        self.admin_request.as_ref().unwrap_or_else(|| AdminRequest::default_instance())
    }

    fn get_admin_request_for_reflect(&self) -> &::protobuf::SingularPtrField<AdminRequest> {
        &self.admin_request
    }

    fn mut_admin_request_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<AdminRequest> {
        &mut self.admin_request
    }

    // .raft_cmdpb.StatusRequest status_request = 4;

    pub fn clear_status_request(&mut self) {
        self.status_request.clear();
    }

    pub fn has_status_request(&self) -> bool {
        self.status_request.is_some()
    }

    // Param is passed by value, moved
    pub fn set_status_request(&mut self, v: StatusRequest) {
        self.status_request = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_status_request(&mut self) -> &mut StatusRequest {
        if self.status_request.is_none() {
            self.status_request.set_default();
        }
        self.status_request.as_mut().unwrap()
    }

    // Take field
    pub fn take_status_request(&mut self) -> StatusRequest {
        self.status_request.take().unwrap_or_else(|| StatusRequest::new())
    }

    pub fn get_status_request(&self) -> &StatusRequest {
        self.status_request.as_ref().unwrap_or_else(|| StatusRequest::default_instance())
    }

    fn get_status_request_for_reflect(&self) -> &::protobuf::SingularPtrField<StatusRequest> {
        &self.status_request
    }

    fn mut_status_request_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<StatusRequest> {
        &mut self.status_request
    }
}

impl ::protobuf::Message for RaftCmdRequest {
    fn is_initialized(&self) -> bool {
        for v in &self.header {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.requests {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.admin_request {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.status_request {
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
                    ::protobuf::rt::read_repeated_message_into(wire_type, is, &mut self.requests)?;
                },
                3 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.admin_request)?;
                },
                4 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.status_request)?;
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
        for value in &self.requests {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        if let Some(ref v) = self.admin_request.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if let Some(ref v) = self.status_request.as_ref() {
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
        for v in &self.requests {
            os.write_tag(2, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        };
        if let Some(ref v) = self.admin_request.as_ref() {
            os.write_tag(3, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if let Some(ref v) = self.status_request.as_ref() {
            os.write_tag(4, ::protobuf::wire_format::WireTypeLengthDelimited)?;
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

impl ::protobuf::MessageStatic for RaftCmdRequest {
    fn new() -> RaftCmdRequest {
        RaftCmdRequest::new()
    }

    fn descriptor_static(_: ::std::option::Option<RaftCmdRequest>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<RaftRequestHeader>>(
                    "header",
                    RaftCmdRequest::get_header_for_reflect,
                    RaftCmdRequest::mut_header_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_repeated_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<Request>>(
                    "requests",
                    RaftCmdRequest::get_requests_for_reflect,
                    RaftCmdRequest::mut_requests_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<AdminRequest>>(
                    "admin_request",
                    RaftCmdRequest::get_admin_request_for_reflect,
                    RaftCmdRequest::mut_admin_request_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<StatusRequest>>(
                    "status_request",
                    RaftCmdRequest::get_status_request_for_reflect,
                    RaftCmdRequest::mut_status_request_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<RaftCmdRequest>(
                    "RaftCmdRequest",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for RaftCmdRequest {
    fn clear(&mut self) {
        self.clear_header();
        self.clear_requests();
        self.clear_admin_request();
        self.clear_status_request();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for RaftCmdRequest {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for RaftCmdRequest {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct RaftCmdResponse {
    // message fields
    pub header: ::protobuf::SingularPtrField<RaftResponseHeader>,
    pub responses: ::protobuf::RepeatedField<Response>,
    pub admin_response: ::protobuf::SingularPtrField<AdminResponse>,
    pub status_response: ::protobuf::SingularPtrField<StatusResponse>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for RaftCmdResponse {}

impl RaftCmdResponse {
    pub fn new() -> RaftCmdResponse {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static RaftCmdResponse {
        static mut instance: ::protobuf::lazy::Lazy<RaftCmdResponse> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const RaftCmdResponse,
        };
        unsafe {
            instance.get(RaftCmdResponse::new)
        }
    }

    // .raft_cmdpb.RaftResponseHeader header = 1;

    pub fn clear_header(&mut self) {
        self.header.clear();
    }

    pub fn has_header(&self) -> bool {
        self.header.is_some()
    }

    // Param is passed by value, moved
    pub fn set_header(&mut self, v: RaftResponseHeader) {
        self.header = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_header(&mut self) -> &mut RaftResponseHeader {
        if self.header.is_none() {
            self.header.set_default();
        }
        self.header.as_mut().unwrap()
    }

    // Take field
    pub fn take_header(&mut self) -> RaftResponseHeader {
        self.header.take().unwrap_or_else(|| RaftResponseHeader::new())
    }

    pub fn get_header(&self) -> &RaftResponseHeader {
        self.header.as_ref().unwrap_or_else(|| RaftResponseHeader::default_instance())
    }

    fn get_header_for_reflect(&self) -> &::protobuf::SingularPtrField<RaftResponseHeader> {
        &self.header
    }

    fn mut_header_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<RaftResponseHeader> {
        &mut self.header
    }

    // repeated .raft_cmdpb.Response responses = 2;

    pub fn clear_responses(&mut self) {
        self.responses.clear();
    }

    // Param is passed by value, moved
    pub fn set_responses(&mut self, v: ::protobuf::RepeatedField<Response>) {
        self.responses = v;
    }

    // Mutable pointer to the field.
    pub fn mut_responses(&mut self) -> &mut ::protobuf::RepeatedField<Response> {
        &mut self.responses
    }

    // Take field
    pub fn take_responses(&mut self) -> ::protobuf::RepeatedField<Response> {
        ::std::mem::replace(&mut self.responses, ::protobuf::RepeatedField::new())
    }

    pub fn get_responses(&self) -> &[Response] {
        &self.responses
    }

    fn get_responses_for_reflect(&self) -> &::protobuf::RepeatedField<Response> {
        &self.responses
    }

    fn mut_responses_for_reflect(&mut self) -> &mut ::protobuf::RepeatedField<Response> {
        &mut self.responses
    }

    // .raft_cmdpb.AdminResponse admin_response = 3;

    pub fn clear_admin_response(&mut self) {
        self.admin_response.clear();
    }

    pub fn has_admin_response(&self) -> bool {
        self.admin_response.is_some()
    }

    // Param is passed by value, moved
    pub fn set_admin_response(&mut self, v: AdminResponse) {
        self.admin_response = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_admin_response(&mut self) -> &mut AdminResponse {
        if self.admin_response.is_none() {
            self.admin_response.set_default();
        }
        self.admin_response.as_mut().unwrap()
    }

    // Take field
    pub fn take_admin_response(&mut self) -> AdminResponse {
        self.admin_response.take().unwrap_or_else(|| AdminResponse::new())
    }

    pub fn get_admin_response(&self) -> &AdminResponse {
        self.admin_response.as_ref().unwrap_or_else(|| AdminResponse::default_instance())
    }

    fn get_admin_response_for_reflect(&self) -> &::protobuf::SingularPtrField<AdminResponse> {
        &self.admin_response
    }

    fn mut_admin_response_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<AdminResponse> {
        &mut self.admin_response
    }

    // .raft_cmdpb.StatusResponse status_response = 4;

    pub fn clear_status_response(&mut self) {
        self.status_response.clear();
    }

    pub fn has_status_response(&self) -> bool {
        self.status_response.is_some()
    }

    // Param is passed by value, moved
    pub fn set_status_response(&mut self, v: StatusResponse) {
        self.status_response = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_status_response(&mut self) -> &mut StatusResponse {
        if self.status_response.is_none() {
            self.status_response.set_default();
        }
        self.status_response.as_mut().unwrap()
    }

    // Take field
    pub fn take_status_response(&mut self) -> StatusResponse {
        self.status_response.take().unwrap_or_else(|| StatusResponse::new())
    }

    pub fn get_status_response(&self) -> &StatusResponse {
        self.status_response.as_ref().unwrap_or_else(|| StatusResponse::default_instance())
    }

    fn get_status_response_for_reflect(&self) -> &::protobuf::SingularPtrField<StatusResponse> {
        &self.status_response
    }

    fn mut_status_response_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<StatusResponse> {
        &mut self.status_response
    }
}

impl ::protobuf::Message for RaftCmdResponse {
    fn is_initialized(&self) -> bool {
        for v in &self.header {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.responses {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.admin_response {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.status_response {
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
                    ::protobuf::rt::read_repeated_message_into(wire_type, is, &mut self.responses)?;
                },
                3 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.admin_response)?;
                },
                4 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.status_response)?;
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
        for value in &self.responses {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        if let Some(ref v) = self.admin_response.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if let Some(ref v) = self.status_response.as_ref() {
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
        for v in &self.responses {
            os.write_tag(2, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        };
        if let Some(ref v) = self.admin_response.as_ref() {
            os.write_tag(3, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if let Some(ref v) = self.status_response.as_ref() {
            os.write_tag(4, ::protobuf::wire_format::WireTypeLengthDelimited)?;
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

impl ::protobuf::MessageStatic for RaftCmdResponse {
    fn new() -> RaftCmdResponse {
        RaftCmdResponse::new()
    }

    fn descriptor_static(_: ::std::option::Option<RaftCmdResponse>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<RaftResponseHeader>>(
                    "header",
                    RaftCmdResponse::get_header_for_reflect,
                    RaftCmdResponse::mut_header_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_repeated_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<Response>>(
                    "responses",
                    RaftCmdResponse::get_responses_for_reflect,
                    RaftCmdResponse::mut_responses_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<AdminResponse>>(
                    "admin_response",
                    RaftCmdResponse::get_admin_response_for_reflect,
                    RaftCmdResponse::mut_admin_response_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<StatusResponse>>(
                    "status_response",
                    RaftCmdResponse::get_status_response_for_reflect,
                    RaftCmdResponse::mut_status_response_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<RaftCmdResponse>(
                    "RaftCmdResponse",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for RaftCmdResponse {
    fn clear(&mut self) {
        self.clear_header();
        self.clear_responses();
        self.clear_admin_response();
        self.clear_status_response();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for RaftCmdResponse {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for RaftCmdResponse {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(Clone,PartialEq,Eq,Debug,Hash)]
pub enum CmdType {
    Invalid = 0,
    Get = 1,
    Put = 3,
    Delete = 4,
    Snap = 5,
    Prewrite = 6,
    DeleteRange = 7,
}

impl ::protobuf::ProtobufEnum for CmdType {
    fn value(&self) -> i32 {
        *self as i32
    }

    fn from_i32(value: i32) -> ::std::option::Option<CmdType> {
        match value {
            0 => ::std::option::Option::Some(CmdType::Invalid),
            1 => ::std::option::Option::Some(CmdType::Get),
            3 => ::std::option::Option::Some(CmdType::Put),
            4 => ::std::option::Option::Some(CmdType::Delete),
            5 => ::std::option::Option::Some(CmdType::Snap),
            6 => ::std::option::Option::Some(CmdType::Prewrite),
            7 => ::std::option::Option::Some(CmdType::DeleteRange),
            _ => ::std::option::Option::None
        }
    }

    fn values() -> &'static [Self] {
        static values: &'static [CmdType] = &[
            CmdType::Invalid,
            CmdType::Get,
            CmdType::Put,
            CmdType::Delete,
            CmdType::Snap,
            CmdType::Prewrite,
            CmdType::DeleteRange,
        ];
        values
    }

    fn enum_descriptor_static(_: ::std::option::Option<CmdType>) -> &'static ::protobuf::reflect::EnumDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::EnumDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::EnumDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                ::protobuf::reflect::EnumDescriptor::new("CmdType", file_descriptor_proto())
            })
        }
    }
}

impl ::std::marker::Copy for CmdType {
}

impl ::std::default::Default for CmdType {
    fn default() -> Self {
        CmdType::Invalid
    }
}

impl ::protobuf::reflect::ProtobufValue for CmdType {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Enum(self.descriptor())
    }
}

#[derive(Clone,PartialEq,Eq,Debug,Hash)]
pub enum AdminCmdType {
    InvalidAdmin = 0,
    ChangePeer = 1,
    Split = 2,
    CompactLog = 3,
    TransferLeader = 4,
    ComputeHash = 5,
    VerifyHash = 6,
}

impl ::protobuf::ProtobufEnum for AdminCmdType {
    fn value(&self) -> i32 {
        *self as i32
    }

    fn from_i32(value: i32) -> ::std::option::Option<AdminCmdType> {
        match value {
            0 => ::std::option::Option::Some(AdminCmdType::InvalidAdmin),
            1 => ::std::option::Option::Some(AdminCmdType::ChangePeer),
            2 => ::std::option::Option::Some(AdminCmdType::Split),
            3 => ::std::option::Option::Some(AdminCmdType::CompactLog),
            4 => ::std::option::Option::Some(AdminCmdType::TransferLeader),
            5 => ::std::option::Option::Some(AdminCmdType::ComputeHash),
            6 => ::std::option::Option::Some(AdminCmdType::VerifyHash),
            _ => ::std::option::Option::None
        }
    }

    fn values() -> &'static [Self] {
        static values: &'static [AdminCmdType] = &[
            AdminCmdType::InvalidAdmin,
            AdminCmdType::ChangePeer,
            AdminCmdType::Split,
            AdminCmdType::CompactLog,
            AdminCmdType::TransferLeader,
            AdminCmdType::ComputeHash,
            AdminCmdType::VerifyHash,
        ];
        values
    }

    fn enum_descriptor_static(_: ::std::option::Option<AdminCmdType>) -> &'static ::protobuf::reflect::EnumDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::EnumDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::EnumDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                ::protobuf::reflect::EnumDescriptor::new("AdminCmdType", file_descriptor_proto())
            })
        }
    }
}

impl ::std::marker::Copy for AdminCmdType {
}

impl ::std::default::Default for AdminCmdType {
    fn default() -> Self {
        AdminCmdType::InvalidAdmin
    }
}

impl ::protobuf::reflect::ProtobufValue for AdminCmdType {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Enum(self.descriptor())
    }
}

#[derive(Clone,PartialEq,Eq,Debug,Hash)]
pub enum StatusCmdType {
    InvalidStatus = 0,
    RegionLeader = 1,
    RegionDetail = 2,
}

impl ::protobuf::ProtobufEnum for StatusCmdType {
    fn value(&self) -> i32 {
        *self as i32
    }

    fn from_i32(value: i32) -> ::std::option::Option<StatusCmdType> {
        match value {
            0 => ::std::option::Option::Some(StatusCmdType::InvalidStatus),
            1 => ::std::option::Option::Some(StatusCmdType::RegionLeader),
            2 => ::std::option::Option::Some(StatusCmdType::RegionDetail),
            _ => ::std::option::Option::None
        }
    }

    fn values() -> &'static [Self] {
        static values: &'static [StatusCmdType] = &[
            StatusCmdType::InvalidStatus,
            StatusCmdType::RegionLeader,
            StatusCmdType::RegionDetail,
        ];
        values
    }

    fn enum_descriptor_static(_: ::std::option::Option<StatusCmdType>) -> &'static ::protobuf::reflect::EnumDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::EnumDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::EnumDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                ::protobuf::reflect::EnumDescriptor::new("StatusCmdType", file_descriptor_proto())
            })
        }
    }
}

impl ::std::marker::Copy for StatusCmdType {
}

impl ::std::default::Default for StatusCmdType {
    fn default() -> Self {
        StatusCmdType::InvalidStatus
    }
}

impl ::protobuf::reflect::ProtobufValue for StatusCmdType {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Enum(self.descriptor())
    }
}

static file_descriptor_proto_data: &'static [u8] = b"\
    \n\x10raft_cmdpb.proto\x12\nraft_cmdpb\x1a\x0cmetapb.proto\x1a\rerrorpb.\
    proto\x1a\reraftpb.proto\".\n\nGetRequest\x12\x0e\n\x02cf\x18\x01\x20\
    \x01(\tR\x02cf\x12\x10\n\x03key\x18\x02\x20\x01(\x0cR\x03key\"#\n\x0bGet\
    Response\x12\x14\n\x05value\x18\x01\x20\x01(\x0cR\x05value\"D\n\nPutRequ\
    est\x12\x0e\n\x02cf\x18\x01\x20\x01(\tR\x02cf\x12\x10\n\x03key\x18\x02\
    \x20\x01(\x0cR\x03key\x12\x14\n\x05value\x18\x03\x20\x01(\x0cR\x05value\
    \"\r\n\x0bPutResponse\"1\n\rDeleteRequest\x12\x0e\n\x02cf\x18\x01\x20\
    \x01(\tR\x02cf\x12\x10\n\x03key\x18\x02\x20\x01(\x0cR\x03key\"\x10\n\x0e\
    DeleteResponse\"Z\n\x12DeleteRangeRequest\x12\x0e\n\x02cf\x18\x01\x20\
    \x01(\tR\x02cf\x12\x1b\n\tstart_key\x18\x02\x20\x01(\x0cR\x08startKey\
    \x12\x17\n\x07end_key\x18\x03\x20\x01(\x0cR\x06endKey\"\x15\n\x13DeleteR\
    angeResponse\"\r\n\x0bSnapRequest\"6\n\x0cSnapResponse\x12&\n\x06region\
    \x18\x01\x20\x01(\x0b2\x0e.metapb.RegionR\x06region\"M\n\x0fPrewriteRequ\
    est\x12\x10\n\x03key\x18\x01\x20\x01(\x0cR\x03key\x12\x14\n\x05value\x18\
    \x02\x20\x01(\x0cR\x05value\x12\x12\n\x04lock\x18\x03\x20\x01(\x0cR\x04l\
    ock\"\x12\n\x10PrewriteResponse\"\xe9\x02\n\x07Request\x12.\n\x08cmd_typ\
    e\x18\x01\x20\x01(\x0e2\x13.raft_cmdpb.CmdTypeR\x07cmdType\x12(\n\x03get\
    \x18\x02\x20\x01(\x0b2\x16.raft_cmdpb.GetRequestR\x03get\x12(\n\x03put\
    \x18\x04\x20\x01(\x0b2\x16.raft_cmdpb.PutRequestR\x03put\x121\n\x06delet\
    e\x18\x05\x20\x01(\x0b2\x19.raft_cmdpb.DeleteRequestR\x06delete\x12+\n\
    \x04snap\x18\x06\x20\x01(\x0b2\x17.raft_cmdpb.SnapRequestR\x04snap\x127\
    \n\x08prewrite\x18\x07\x20\x01(\x0b2\x1b.raft_cmdpb.PrewriteRequestR\x08\
    prewrite\x12A\n\x0cdelete_range\x18\x08\x20\x01(\x0b2\x1e.raft_cmdpb.Del\
    eteRangeRequestR\x0bdeleteRange\"\xee\x02\n\x08Response\x12.\n\x08cmd_ty\
    pe\x18\x01\x20\x01(\x0e2\x13.raft_cmdpb.CmdTypeR\x07cmdType\x12)\n\x03ge\
    t\x18\x02\x20\x01(\x0b2\x17.raft_cmdpb.GetResponseR\x03get\x12)\n\x03put\
    \x18\x04\x20\x01(\x0b2\x17.raft_cmdpb.PutResponseR\x03put\x122\n\x06dele\
    te\x18\x05\x20\x01(\x0b2\x1a.raft_cmdpb.DeleteResponseR\x06delete\x12,\n\
    \x04snap\x18\x06\x20\x01(\x0b2\x18.raft_cmdpb.SnapResponseR\x04snap\x128\
    \n\x08prewrite\x18\x07\x20\x01(\x0b2\x1c.raft_cmdpb.PrewriteResponseR\
    \x08prewrite\x12@\n\x0bdelte_range\x18\x08\x20\x01(\x0b2\x1f.raft_cmdpb.\
    DeleteRangeResponseR\ndelteRange\"o\n\x11ChangePeerRequest\x128\n\x0bcha\
    nge_type\x18\x01\x20\x01(\x0e2\x17.eraftpb.ConfChangeTypeR\nchangeType\
    \x12\x20\n\x04peer\x18\x02\x20\x01(\x0b2\x0c.metapb.PeerR\x04peer\"<\n\
    \x12ChangePeerResponse\x12&\n\x06region\x18\x01\x20\x01(\x0b2\x0e.metapb\
    .RegionR\x06region\"\x94\x01\n\x0cSplitRequest\x12\x1b\n\tsplit_key\x18\
    \x01\x20\x01(\x0cR\x08splitKey\x12\"\n\rnew_region_id\x18\x02\x20\x01(\
    \x04R\x0bnewRegionId\x12\x20\n\x0cnew_peer_ids\x18\x03\x20\x03(\x04R\nne\
    wPeerIds\x12!\n\x0cright_derive\x18\x04\x20\x01(\x08R\x0brightDerive\"Y\
    \n\rSplitResponse\x12\"\n\x04left\x18\x01\x20\x01(\x0b2\x0e.metapb.Regio\
    nR\x04left\x12$\n\x05right\x18\x02\x20\x01(\x0b2\x0e.metapb.RegionR\x05r\
    ight\"[\n\x11CompactLogRequest\x12#\n\rcompact_index\x18\x01\x20\x01(\
    \x04R\x0ccompactIndex\x12!\n\x0ccompact_term\x18\x02\x20\x01(\x04R\x0bco\
    mpactTerm\"\x14\n\x12CompactLogResponse\"9\n\x15TransferLeaderRequest\
    \x12\x20\n\x04peer\x18\x01\x20\x01(\x0b2\x0c.metapb.PeerR\x04peer\"\x18\
    \n\x16TransferLeaderResponse\"=\n\x11VerifyHashRequest\x12\x14\n\x05inde\
    x\x18\x01\x20\x01(\x04R\x05index\x12\x12\n\x04hash\x18\x02\x20\x01(\x0cR\
    \x04hash\"\x14\n\x12VerifyHashResponse\"\xff\x02\n\x0cAdminRequest\x123\
    \n\x08cmd_type\x18\x01\x20\x01(\x0e2\x18.raft_cmdpb.AdminCmdTypeR\x07cmd\
    Type\x12>\n\x0bchange_peer\x18\x02\x20\x01(\x0b2\x1d.raft_cmdpb.ChangePe\
    erRequestR\nchangePeer\x12.\n\x05split\x18\x03\x20\x01(\x0b2\x18.raft_cm\
    dpb.SplitRequestR\x05split\x12>\n\x0bcompact_log\x18\x04\x20\x01(\x0b2\
    \x1d.raft_cmdpb.CompactLogRequestR\ncompactLog\x12J\n\x0ftransfer_leader\
    \x18\x05\x20\x01(\x0b2!.raft_cmdpb.TransferLeaderRequestR\x0etransferLea\
    der\x12>\n\x0bverify_hash\x18\x06\x20\x01(\x0b2\x1d.raft_cmdpb.VerifyHas\
    hRequestR\nverifyHash\"\x85\x03\n\rAdminResponse\x123\n\x08cmd_type\x18\
    \x01\x20\x01(\x0e2\x18.raft_cmdpb.AdminCmdTypeR\x07cmdType\x12?\n\x0bcha\
    nge_peer\x18\x02\x20\x01(\x0b2\x1e.raft_cmdpb.ChangePeerResponseR\nchang\
    ePeer\x12/\n\x05split\x18\x03\x20\x01(\x0b2\x19.raft_cmdpb.SplitResponse\
    R\x05split\x12?\n\x0bcompact_log\x18\x04\x20\x01(\x0b2\x1e.raft_cmdpb.Co\
    mpactLogResponseR\ncompactLog\x12K\n\x0ftransfer_leader\x18\x05\x20\x01(\
    \x0b2\".raft_cmdpb.TransferLeaderResponseR\x0etransferLeader\x12?\n\x0bv\
    erify_hash\x18\x06\x20\x01(\x0b2\x1e.raft_cmdpb.VerifyHashResponseR\nver\
    ifyHash\"\x15\n\x13RegionLeaderRequest\"<\n\x14RegionLeaderResponse\x12$\
    \n\x06leader\x18\x01\x20\x01(\x0b2\x0c.metapb.PeerR\x06leader\"\x15\n\
    \x13RegionDetailRequest\"d\n\x14RegionDetailResponse\x12&\n\x06region\
    \x18\x01\x20\x01(\x0b2\x0e.metapb.RegionR\x06region\x12$\n\x06leader\x18\
    \x02\x20\x01(\x0b2\x0c.metapb.PeerR\x06leader\"\xd1\x01\n\rStatusRequest\
    \x124\n\x08cmd_type\x18\x01\x20\x01(\x0e2\x19.raft_cmdpb.StatusCmdTypeR\
    \x07cmdType\x12D\n\rregion_leader\x18\x02\x20\x01(\x0b2\x1f.raft_cmdpb.R\
    egionLeaderRequestR\x0cregionLeader\x12D\n\rregion_detail\x18\x03\x20\
    \x01(\x0b2\x1f.raft_cmdpb.RegionDetailRequestR\x0cregionDetail\"\xd4\x01\
    \n\x0eStatusResponse\x124\n\x08cmd_type\x18\x01\x20\x01(\x0e2\x19.raft_c\
    mdpb.StatusCmdTypeR\x07cmdType\x12E\n\rregion_leader\x18\x02\x20\x01(\
    \x0b2\x20.raft_cmdpb.RegionLeaderResponseR\x0cregionLeader\x12E\n\rregio\
    n_detail\x18\x03\x20\x01(\x0b2\x20.raft_cmdpb.RegionDetailResponseR\x0cr\
    egionDetail\"\xee\x01\n\x11RaftRequestHeader\x12\x1b\n\tregion_id\x18\
    \x01\x20\x01(\x04R\x08regionId\x12\x20\n\x04peer\x18\x02\x20\x01(\x0b2\
    \x0c.metapb.PeerR\x04peer\x12\x1f\n\x0bread_quorum\x18\x03\x20\x01(\x08R\
    \nreadQuorum\x12\x12\n\x04uuid\x18\x04\x20\x01(\x0cR\x04uuid\x126\n\x0cr\
    egion_epoch\x18\x05\x20\x01(\x0b2\x13.metapb.RegionEpochR\x0bregionEpoch\
    \x12\x12\n\x04term\x18\x06\x20\x01(\x04R\x04term\x12\x19\n\x08sync_log\
    \x18\x07\x20\x01(\x08R\x07syncLog\"q\n\x12RaftResponseHeader\x12$\n\x05e\
    rror\x18\x01\x20\x01(\x0b2\x0e.errorpb.ErrorR\x05error\x12\x12\n\x04uuid\
    \x18\x02\x20\x01(\x0cR\x04uuid\x12!\n\x0ccurrent_term\x18\x03\x20\x01(\
    \x04R\x0bcurrentTerm\"\xf9\x01\n\x0eRaftCmdRequest\x125\n\x06header\x18\
    \x01\x20\x01(\x0b2\x1d.raft_cmdpb.RaftRequestHeaderR\x06header\x12/\n\
    \x08requests\x18\x02\x20\x03(\x0b2\x13.raft_cmdpb.RequestR\x08requests\
    \x12=\n\radmin_request\x18\x03\x20\x01(\x0b2\x18.raft_cmdpb.AdminRequest\
    R\x0cadminRequest\x12@\n\x0estatus_request\x18\x04\x20\x01(\x0b2\x19.raf\
    t_cmdpb.StatusRequestR\rstatusRequest\"\x84\x02\n\x0fRaftCmdResponse\x12\
    6\n\x06header\x18\x01\x20\x01(\x0b2\x1e.raft_cmdpb.RaftResponseHeaderR\
    \x06header\x122\n\tresponses\x18\x02\x20\x03(\x0b2\x14.raft_cmdpb.Respon\
    seR\tresponses\x12@\n\x0eadmin_response\x18\x03\x20\x01(\x0b2\x19.raft_c\
    mdpb.AdminResponseR\radminResponse\x12C\n\x0fstatus_response\x18\x04\x20\
    \x01(\x0b2\x1a.raft_cmdpb.StatusResponseR\x0estatusResponse*]\n\x07CmdTy\
    pe\x12\x0b\n\x07Invalid\x10\0\x12\x07\n\x03Get\x10\x01\x12\x07\n\x03Put\
    \x10\x03\x12\n\n\x06Delete\x10\x04\x12\x08\n\x04Snap\x10\x05\x12\x0c\n\
    \x08Prewrite\x10\x06\x12\x0f\n\x0bDeleteRange\x10\x07*\x80\x01\n\x0cAdmi\
    nCmdType\x12\x10\n\x0cInvalidAdmin\x10\0\x12\x0e\n\nChangePeer\x10\x01\
    \x12\t\n\x05Split\x10\x02\x12\x0e\n\nCompactLog\x10\x03\x12\x12\n\x0eTra\
    nsferLeader\x10\x04\x12\x0f\n\x0bComputeHash\x10\x05\x12\x0e\n\nVerifyHa\
    sh\x10\x06*F\n\rStatusCmdType\x12\x11\n\rInvalidStatus\x10\0\x12\x10\n\
    \x0cRegionLeader\x10\x01\x12\x10\n\x0cRegionDetail\x10\x02B\x1a\n\x18com\
    .pingcap.tikv.kvprotoJ\xf3F\n\x07\x12\x05\0\0\xef\x01\x01\n\x08\n\x01\
    \x0c\x12\x03\0\0\x12\n\x08\n\x01\x02\x12\x03\x01\x08\x12\n\t\n\x02\x03\0\
    \x12\x03\x03\x07\x15\n\t\n\x02\x03\x01\x12\x03\x04\x07\x16\n\t\n\x02\x03\
    \x02\x12\x03\x05\x07\x16\n\x08\n\x01\x08\x12\x03\x07\01\n\x0b\n\x04\x08\
    \xe7\x07\0\x12\x03\x07\01\n\x0c\n\x05\x08\xe7\x07\0\x02\x12\x03\x07\x07\
    \x13\n\r\n\x06\x08\xe7\x07\0\x02\0\x12\x03\x07\x07\x13\n\x0e\n\x07\x08\
    \xe7\x07\0\x02\0\x01\x12\x03\x07\x07\x13\n\x0c\n\x05\x08\xe7\x07\0\x07\
    \x12\x03\x07\x160\n\n\n\x02\x04\0\x12\x04\t\0\x0c\x01\n\n\n\x03\x04\0\
    \x01\x12\x03\t\x08\x12\n\x0b\n\x04\x04\0\x02\0\x12\x03\n\x04\x12\n\r\n\
    \x05\x04\0\x02\0\x04\x12\x04\n\x04\t\x14\n\x0c\n\x05\x04\0\x02\0\x05\x12\
    \x03\n\x04\n\n\x0c\n\x05\x04\0\x02\0\x01\x12\x03\n\x0b\r\n\x0c\n\x05\x04\
    \0\x02\0\x03\x12\x03\n\x10\x11\n\x0b\n\x04\x04\0\x02\x01\x12\x03\x0b\x04\
    \x12\n\r\n\x05\x04\0\x02\x01\x04\x12\x04\x0b\x04\n\x12\n\x0c\n\x05\x04\0\
    \x02\x01\x05\x12\x03\x0b\x04\t\n\x0c\n\x05\x04\0\x02\x01\x01\x12\x03\x0b\
    \n\r\n\x0c\n\x05\x04\0\x02\x01\x03\x12\x03\x0b\x10\x11\n\n\n\x02\x04\x01\
    \x12\x04\x0e\0\x10\x01\n\n\n\x03\x04\x01\x01\x12\x03\x0e\x08\x13\n\x0b\n\
    \x04\x04\x01\x02\0\x12\x03\x0f\x04\x14\n\r\n\x05\x04\x01\x02\0\x04\x12\
    \x04\x0f\x04\x0e\x15\n\x0c\n\x05\x04\x01\x02\0\x05\x12\x03\x0f\x04\t\n\
    \x0c\n\x05\x04\x01\x02\0\x01\x12\x03\x0f\n\x0f\n\x0c\n\x05\x04\x01\x02\0\
    \x03\x12\x03\x0f\x12\x13\n\n\n\x02\x04\x02\x12\x04\x12\0\x16\x01\n\n\n\
    \x03\x04\x02\x01\x12\x03\x12\x08\x12\n\x0b\n\x04\x04\x02\x02\0\x12\x03\
    \x13\x04\x12\n\r\n\x05\x04\x02\x02\0\x04\x12\x04\x13\x04\x12\x14\n\x0c\n\
    \x05\x04\x02\x02\0\x05\x12\x03\x13\x04\n\n\x0c\n\x05\x04\x02\x02\0\x01\
    \x12\x03\x13\x0b\r\n\x0c\n\x05\x04\x02\x02\0\x03\x12\x03\x13\x10\x11\n\
    \x0b\n\x04\x04\x02\x02\x01\x12\x03\x14\x04\x12\n\r\n\x05\x04\x02\x02\x01\
    \x04\x12\x04\x14\x04\x13\x12\n\x0c\n\x05\x04\x02\x02\x01\x05\x12\x03\x14\
    \x04\t\n\x0c\n\x05\x04\x02\x02\x01\x01\x12\x03\x14\n\r\n\x0c\n\x05\x04\
    \x02\x02\x01\x03\x12\x03\x14\x10\x11\n\x0b\n\x04\x04\x02\x02\x02\x12\x03\
    \x15\x04\x14\n\r\n\x05\x04\x02\x02\x02\x04\x12\x04\x15\x04\x14\x12\n\x0c\
    \n\x05\x04\x02\x02\x02\x05\x12\x03\x15\x04\t\n\x0c\n\x05\x04\x02\x02\x02\
    \x01\x12\x03\x15\n\x0f\n\x0c\n\x05\x04\x02\x02\x02\x03\x12\x03\x15\x12\
    \x13\n\t\n\x02\x04\x03\x12\x03\x18\0\x16\n\n\n\x03\x04\x03\x01\x12\x03\
    \x18\x08\x13\n\n\n\x02\x04\x04\x12\x04\x1a\0\x1d\x01\n\n\n\x03\x04\x04\
    \x01\x12\x03\x1a\x08\x15\n\x0b\n\x04\x04\x04\x02\0\x12\x03\x1b\x04\x12\n\
    \r\n\x05\x04\x04\x02\0\x04\x12\x04\x1b\x04\x1a\x17\n\x0c\n\x05\x04\x04\
    \x02\0\x05\x12\x03\x1b\x04\n\n\x0c\n\x05\x04\x04\x02\0\x01\x12\x03\x1b\
    \x0b\r\n\x0c\n\x05\x04\x04\x02\0\x03\x12\x03\x1b\x10\x11\n\x0b\n\x04\x04\
    \x04\x02\x01\x12\x03\x1c\x04\x12\n\r\n\x05\x04\x04\x02\x01\x04\x12\x04\
    \x1c\x04\x1b\x12\n\x0c\n\x05\x04\x04\x02\x01\x05\x12\x03\x1c\x04\t\n\x0c\
    \n\x05\x04\x04\x02\x01\x01\x12\x03\x1c\n\r\n\x0c\n\x05\x04\x04\x02\x01\
    \x03\x12\x03\x1c\x10\x11\n\t\n\x02\x04\x05\x12\x03\x1f\0\x19\n\n\n\x03\
    \x04\x05\x01\x12\x03\x1f\x08\x16\n\n\n\x02\x04\x06\x12\x04!\0%\x01\n\n\n\
    \x03\x04\x06\x01\x12\x03!\x08\x1a\n\x0b\n\x04\x04\x06\x02\0\x12\x03\"\
    \x04\x12\n\r\n\x05\x04\x06\x02\0\x04\x12\x04\"\x04!\x1c\n\x0c\n\x05\x04\
    \x06\x02\0\x05\x12\x03\"\x04\n\n\x0c\n\x05\x04\x06\x02\0\x01\x12\x03\"\
    \x0b\r\n\x0c\n\x05\x04\x06\x02\0\x03\x12\x03\"\x10\x11\n\x0b\n\x04\x04\
    \x06\x02\x01\x12\x03#\x04\x18\n\r\n\x05\x04\x06\x02\x01\x04\x12\x04#\x04\
    \"\x12\n\x0c\n\x05\x04\x06\x02\x01\x05\x12\x03#\x04\t\n\x0c\n\x05\x04\
    \x06\x02\x01\x01\x12\x03#\n\x13\n\x0c\n\x05\x04\x06\x02\x01\x03\x12\x03#\
    \x16\x17\n\x0b\n\x04\x04\x06\x02\x02\x12\x03$\x04\x16\n\r\n\x05\x04\x06\
    \x02\x02\x04\x12\x04$\x04#\x18\n\x0c\n\x05\x04\x06\x02\x02\x05\x12\x03$\
    \x04\t\n\x0c\n\x05\x04\x06\x02\x02\x01\x12\x03$\n\x11\n\x0c\n\x05\x04\
    \x06\x02\x02\x03\x12\x03$\x14\x15\n\t\n\x02\x04\x07\x12\x03'\0\x1e\n\n\n\
    \x03\x04\x07\x01\x12\x03'\x08\x1b\n\t\n\x02\x04\x08\x12\x03)\0\x16\n\n\n\
    \x03\x04\x08\x01\x12\x03)\x08\x13\n\n\n\x02\x04\t\x12\x04+\0-\x01\n\n\n\
    \x03\x04\t\x01\x12\x03+\x08\x14\n\x0b\n\x04\x04\t\x02\0\x12\x03,\x04\x1d\
    \n\r\n\x05\x04\t\x02\0\x04\x12\x04,\x04+\x16\n\x0c\n\x05\x04\t\x02\0\x06\
    \x12\x03,\x04\x11\n\x0c\n\x05\x04\t\x02\0\x01\x12\x03,\x12\x18\n\x0c\n\
    \x05\x04\t\x02\0\x03\x12\x03,\x1b\x1c\n\n\n\x02\x04\n\x12\x04/\03\x01\n\
    \n\n\x03\x04\n\x01\x12\x03/\x08\x17\n\x0b\n\x04\x04\n\x02\0\x12\x030\x04\
    \x12\n\r\n\x05\x04\n\x02\0\x04\x12\x040\x04/\x19\n\x0c\n\x05\x04\n\x02\0\
    \x05\x12\x030\x04\t\n\x0c\n\x05\x04\n\x02\0\x01\x12\x030\n\r\n\x0c\n\x05\
    \x04\n\x02\0\x03\x12\x030\x10\x11\n\x0b\n\x04\x04\n\x02\x01\x12\x031\x04\
    \x14\n\r\n\x05\x04\n\x02\x01\x04\x12\x041\x040\x12\n\x0c\n\x05\x04\n\x02\
    \x01\x05\x12\x031\x04\t\n\x0c\n\x05\x04\n\x02\x01\x01\x12\x031\n\x0f\n\
    \x0c\n\x05\x04\n\x02\x01\x03\x12\x031\x12\x13\n\x0b\n\x04\x04\n\x02\x02\
    \x12\x032\x04\x13\n\r\n\x05\x04\n\x02\x02\x04\x12\x042\x041\x14\n\x0c\n\
    \x05\x04\n\x02\x02\x05\x12\x032\x04\t\n\x0c\n\x05\x04\n\x02\x02\x01\x12\
    \x032\n\x0e\n\x0c\n\x05\x04\n\x02\x02\x03\x12\x032\x11\x12\n\t\n\x02\x04\
    \x0b\x12\x035\0\x1b\n\n\n\x03\x04\x0b\x01\x12\x035\x08\x18\n\n\n\x02\x05\
    \0\x12\x047\0?\x01\n\n\n\x03\x05\0\x01\x12\x037\x05\x0c\n\x0b\n\x04\x05\
    \0\x02\0\x12\x038\x04\x10\n\x0c\n\x05\x05\0\x02\0\x01\x12\x038\x04\x0b\n\
    \x0c\n\x05\x05\0\x02\0\x02\x12\x038\x0e\x0f\n\x0b\n\x04\x05\0\x02\x01\
    \x12\x039\x04\x0c\n\x0c\n\x05\x05\0\x02\x01\x01\x12\x039\x04\x07\n\x0c\n\
    \x05\x05\0\x02\x01\x02\x12\x039\n\x0b\n\x0b\n\x04\x05\0\x02\x02\x12\x03:\
    \x04\x0c\n\x0c\n\x05\x05\0\x02\x02\x01\x12\x03:\x04\x07\n\x0c\n\x05\x05\
    \0\x02\x02\x02\x12\x03:\n\x0b\n\x0b\n\x04\x05\0\x02\x03\x12\x03;\x04\x0f\
    \n\x0c\n\x05\x05\0\x02\x03\x01\x12\x03;\x04\n\n\x0c\n\x05\x05\0\x02\x03\
    \x02\x12\x03;\r\x0e\n\x0b\n\x04\x05\0\x02\x04\x12\x03<\x04\r\n\x0c\n\x05\
    \x05\0\x02\x04\x01\x12\x03<\x04\x08\n\x0c\n\x05\x05\0\x02\x04\x02\x12\
    \x03<\x0b\x0c\n\x0b\n\x04\x05\0\x02\x05\x12\x03=\x04\x11\n\x0c\n\x05\x05\
    \0\x02\x05\x01\x12\x03=\x04\x0c\n\x0c\n\x05\x05\0\x02\x05\x02\x12\x03=\
    \x0f\x10\n\x0b\n\x04\x05\0\x02\x06\x12\x03>\x04\x14\n\x0c\n\x05\x05\0\
    \x02\x06\x01\x12\x03>\x04\x0f\n\x0c\n\x05\x05\0\x02\x06\x02\x12\x03>\x12\
    \x13\n\n\n\x02\x04\x0c\x12\x04A\0I\x01\n\n\n\x03\x04\x0c\x01\x12\x03A\
    \x08\x0f\n\x0b\n\x04\x04\x0c\x02\0\x12\x03B\x04\x19\n\r\n\x05\x04\x0c\
    \x02\0\x04\x12\x04B\x04A\x11\n\x0c\n\x05\x04\x0c\x02\0\x06\x12\x03B\x04\
    \x0b\n\x0c\n\x05\x04\x0c\x02\0\x01\x12\x03B\x0c\x14\n\x0c\n\x05\x04\x0c\
    \x02\0\x03\x12\x03B\x17\x18\n\x0b\n\x04\x04\x0c\x02\x01\x12\x03C\x04\x17\
    \n\r\n\x05\x04\x0c\x02\x01\x04\x12\x04C\x04B\x19\n\x0c\n\x05\x04\x0c\x02\
    \x01\x06\x12\x03C\x04\x0e\n\x0c\n\x05\x04\x0c\x02\x01\x01\x12\x03C\x0f\
    \x12\n\x0c\n\x05\x04\x0c\x02\x01\x03\x12\x03C\x15\x16\n\x0b\n\x04\x04\
    \x0c\x02\x02\x12\x03D\x04\x17\n\r\n\x05\x04\x0c\x02\x02\x04\x12\x04D\x04\
    C\x17\n\x0c\n\x05\x04\x0c\x02\x02\x06\x12\x03D\x04\x0e\n\x0c\n\x05\x04\
    \x0c\x02\x02\x01\x12\x03D\x0f\x12\n\x0c\n\x05\x04\x0c\x02\x02\x03\x12\
    \x03D\x15\x16\n\x0b\n\x04\x04\x0c\x02\x03\x12\x03E\x04\x1d\n\r\n\x05\x04\
    \x0c\x02\x03\x04\x12\x04E\x04D\x17\n\x0c\n\x05\x04\x0c\x02\x03\x06\x12\
    \x03E\x04\x11\n\x0c\n\x05\x04\x0c\x02\x03\x01\x12\x03E\x12\x18\n\x0c\n\
    \x05\x04\x0c\x02\x03\x03\x12\x03E\x1b\x1c\n\x0b\n\x04\x04\x0c\x02\x04\
    \x12\x03F\x04\x19\n\r\n\x05\x04\x0c\x02\x04\x04\x12\x04F\x04E\x1d\n\x0c\
    \n\x05\x04\x0c\x02\x04\x06\x12\x03F\x04\x0f\n\x0c\n\x05\x04\x0c\x02\x04\
    \x01\x12\x03F\x10\x14\n\x0c\n\x05\x04\x0c\x02\x04\x03\x12\x03F\x17\x18\n\
    \x0b\n\x04\x04\x0c\x02\x05\x12\x03G\x04!\n\r\n\x05\x04\x0c\x02\x05\x04\
    \x12\x04G\x04F\x19\n\x0c\n\x05\x04\x0c\x02\x05\x06\x12\x03G\x04\x13\n\
    \x0c\n\x05\x04\x0c\x02\x05\x01\x12\x03G\x14\x1c\n\x0c\n\x05\x04\x0c\x02\
    \x05\x03\x12\x03G\x1f\x20\n\x0b\n\x04\x04\x0c\x02\x06\x12\x03H\x04(\n\r\
    \n\x05\x04\x0c\x02\x06\x04\x12\x04H\x04G!\n\x0c\n\x05\x04\x0c\x02\x06\
    \x06\x12\x03H\x04\x16\n\x0c\n\x05\x04\x0c\x02\x06\x01\x12\x03H\x17#\n\
    \x0c\n\x05\x04\x0c\x02\x06\x03\x12\x03H&'\n\n\n\x02\x04\r\x12\x04K\0S\
    \x01\n\n\n\x03\x04\r\x01\x12\x03K\x08\x10\n\x0b\n\x04\x04\r\x02\0\x12\
    \x03L\x04\x19\n\r\n\x05\x04\r\x02\0\x04\x12\x04L\x04K\x12\n\x0c\n\x05\
    \x04\r\x02\0\x06\x12\x03L\x04\x0b\n\x0c\n\x05\x04\r\x02\0\x01\x12\x03L\
    \x0c\x14\n\x0c\n\x05\x04\r\x02\0\x03\x12\x03L\x17\x18\n\x0b\n\x04\x04\r\
    \x02\x01\x12\x03M\x04\x18\n\r\n\x05\x04\r\x02\x01\x04\x12\x04M\x04L\x19\
    \n\x0c\n\x05\x04\r\x02\x01\x06\x12\x03M\x04\x0f\n\x0c\n\x05\x04\r\x02\
    \x01\x01\x12\x03M\x10\x13\n\x0c\n\x05\x04\r\x02\x01\x03\x12\x03M\x16\x17\
    \n\x0b\n\x04\x04\r\x02\x02\x12\x03N\x04\x18\n\r\n\x05\x04\r\x02\x02\x04\
    \x12\x04N\x04M\x18\n\x0c\n\x05\x04\r\x02\x02\x06\x12\x03N\x04\x0f\n\x0c\
    \n\x05\x04\r\x02\x02\x01\x12\x03N\x10\x13\n\x0c\n\x05\x04\r\x02\x02\x03\
    \x12\x03N\x16\x17\n\x0b\n\x04\x04\r\x02\x03\x12\x03O\x04\x1e\n\r\n\x05\
    \x04\r\x02\x03\x04\x12\x04O\x04N\x18\n\x0c\n\x05\x04\r\x02\x03\x06\x12\
    \x03O\x04\x12\n\x0c\n\x05\x04\r\x02\x03\x01\x12\x03O\x13\x19\n\x0c\n\x05\
    \x04\r\x02\x03\x03\x12\x03O\x1c\x1d\n\x0b\n\x04\x04\r\x02\x04\x12\x03P\
    \x04\x1a\n\r\n\x05\x04\r\x02\x04\x04\x12\x04P\x04O\x1e\n\x0c\n\x05\x04\r\
    \x02\x04\x06\x12\x03P\x04\x10\n\x0c\n\x05\x04\r\x02\x04\x01\x12\x03P\x11\
    \x15\n\x0c\n\x05\x04\r\x02\x04\x03\x12\x03P\x18\x19\n\x0b\n\x04\x04\r\
    \x02\x05\x12\x03Q\x04\"\n\r\n\x05\x04\r\x02\x05\x04\x12\x04Q\x04P\x1a\n\
    \x0c\n\x05\x04\r\x02\x05\x06\x12\x03Q\x04\x14\n\x0c\n\x05\x04\r\x02\x05\
    \x01\x12\x03Q\x15\x1d\n\x0c\n\x05\x04\r\x02\x05\x03\x12\x03Q\x20!\n\x0b\
    \n\x04\x04\r\x02\x06\x12\x03R\x04(\n\r\n\x05\x04\r\x02\x06\x04\x12\x04R\
    \x04Q\"\n\x0c\n\x05\x04\r\x02\x06\x06\x12\x03R\x04\x17\n\x0c\n\x05\x04\r\
    \x02\x06\x01\x12\x03R\x18#\n\x0c\n\x05\x04\r\x02\x06\x03\x12\x03R&'\n\n\
    \n\x02\x04\x0e\x12\x04U\0Y\x01\n\n\n\x03\x04\x0e\x01\x12\x03U\x08\x19\nA\
    \n\x04\x04\x0e\x02\0\x12\x03W\x04+\x1a4\x20This\x20can\x20be\x20only\x20\
    called\x20in\x20internal\x20RaftStore\x20now.\n\n\r\n\x05\x04\x0e\x02\0\
    \x04\x12\x04W\x04U\x1b\n\x0c\n\x05\x04\x0e\x02\0\x06\x12\x03W\x04\x1a\n\
    \x0c\n\x05\x04\x0e\x02\0\x01\x12\x03W\x1b&\n\x0c\n\x05\x04\x0e\x02\0\x03\
    \x12\x03W)*\n\x0b\n\x04\x04\x0e\x02\x01\x12\x03X\x04\x19\n\r\n\x05\x04\
    \x0e\x02\x01\x04\x12\x04X\x04W+\n\x0c\n\x05\x04\x0e\x02\x01\x06\x12\x03X\
    \x04\x0f\n\x0c\n\x05\x04\x0e\x02\x01\x01\x12\x03X\x10\x14\n\x0c\n\x05\
    \x04\x0e\x02\x01\x03\x12\x03X\x17\x18\n\n\n\x02\x04\x0f\x12\x04[\0]\x01\
    \n\n\n\x03\x04\x0f\x01\x12\x03[\x08\x1a\n\x0b\n\x04\x04\x0f\x02\0\x12\
    \x03\\\x04\x1d\n\r\n\x05\x04\x0f\x02\0\x04\x12\x04\\\x04[\x1c\n\x0c\n\
    \x05\x04\x0f\x02\0\x06\x12\x03\\\x04\x11\n\x0c\n\x05\x04\x0f\x02\0\x01\
    \x12\x03\\\x12\x18\n\x0c\n\x05\x04\x0f\x02\0\x03\x12\x03\\\x1b\x1c\n\n\n\
    \x02\x04\x10\x12\x04_\0l\x01\n\n\n\x03\x04\x10\x01\x12\x03_\x08\x14\nv\n\
    \x04\x04\x10\x02\0\x12\x03b\x04\x18\x1ai\x20This\x20can\x20be\x20only\
    \x20called\x20in\x20internal\x20RaftStore\x20now.\n\x20The\x20split_key\
    \x20must\x20be\x20in\x20the\x20been\x20splitting\x20region.\n\n\r\n\x05\
    \x04\x10\x02\0\x04\x12\x04b\x04_\x16\n\x0c\n\x05\x04\x10\x02\0\x05\x12\
    \x03b\x04\t\n\x0c\n\x05\x04\x10\x02\0\x01\x12\x03b\n\x13\n\x0c\n\x05\x04\
    \x10\x02\0\x03\x12\x03b\x16\x17\n\xba\x01\n\x04\x04\x10\x02\x01\x12\x03f\
    \x04\x1d\x1a\xac\x01\x20We\x20split\x20the\x20region\x20into\x20two,\x20\
    first\x20uses\x20the\x20origin\x20\n\x20parent\x20region\x20id,\x20and\
    \x20the\x20second\x20uses\x20the\x20new_region_id.\n\x20We\x20must\x20gu\
    arantee\x20that\x20the\x20new_region_id\x20is\x20global\x20unique.\n\n\r\
    \n\x05\x04\x10\x02\x01\x04\x12\x04f\x04b\x18\n\x0c\n\x05\x04\x10\x02\x01\
    \x05\x12\x03f\x04\n\n\x0c\n\x05\x04\x10\x02\x01\x01\x12\x03f\x0b\x18\n\
    \x0c\n\x05\x04\x10\x02\x01\x03\x12\x03f\x1b\x1c\n5\n\x04\x04\x10\x02\x02\
    \x12\x03h\x04%\x1a(\x20The\x20peer\x20ids\x20for\x20the\x20new\x20split\
    \x20region.\n\n\x0c\n\x05\x04\x10\x02\x02\x04\x12\x03h\x04\x0c\n\x0c\n\
    \x05\x04\x10\x02\x02\x05\x12\x03h\r\x13\n\x0c\n\x05\x04\x10\x02\x02\x01\
    \x12\x03h\x14\x20\n\x0c\n\x05\x04\x10\x02\x02\x03\x12\x03h#$\nb\n\x04\
    \x04\x10\x02\x03\x12\x03k\x04\x1a\x1aU\x20If\x20true,\x20right\x20region\
    \x20derive\x20the\x20origin\x20region_id,\x20\n\x20left\x20region\x20use\
    \x20new_region_id.\n\n\r\n\x05\x04\x10\x02\x03\x04\x12\x04k\x04h%\n\x0c\
    \n\x05\x04\x10\x02\x03\x05\x12\x03k\x04\x08\n\x0c\n\x05\x04\x10\x02\x03\
    \x01\x12\x03k\t\x15\n\x0c\n\x05\x04\x10\x02\x03\x03\x12\x03k\x18\x19\n\n\
    \n\x02\x04\x11\x12\x04n\0q\x01\n\n\n\x03\x04\x11\x01\x12\x03n\x08\x15\n\
    \x0b\n\x04\x04\x11\x02\0\x12\x03o\x04\x1b\n\r\n\x05\x04\x11\x02\0\x04\
    \x12\x04o\x04n\x17\n\x0c\n\x05\x04\x11\x02\0\x06\x12\x03o\x04\x11\n\x0c\
    \n\x05\x04\x11\x02\0\x01\x12\x03o\x12\x16\n\x0c\n\x05\x04\x11\x02\0\x03\
    \x12\x03o\x19\x1a\n\x0b\n\x04\x04\x11\x02\x01\x12\x03p\x04\x1c\n\r\n\x05\
    \x04\x11\x02\x01\x04\x12\x04p\x04o\x1b\n\x0c\n\x05\x04\x11\x02\x01\x06\
    \x12\x03p\x04\x11\n\x0c\n\x05\x04\x11\x02\x01\x01\x12\x03p\x12\x17\n\x0c\
    \n\x05\x04\x11\x02\x01\x03\x12\x03p\x1a\x1b\n\n\n\x02\x04\x12\x12\x04s\0\
    v\x01\n\n\n\x03\x04\x12\x01\x12\x03s\x08\x19\n\x0b\n\x04\x04\x12\x02\0\
    \x12\x03t\x04\x1d\n\r\n\x05\x04\x12\x02\0\x04\x12\x04t\x04s\x1b\n\x0c\n\
    \x05\x04\x12\x02\0\x05\x12\x03t\x04\n\n\x0c\n\x05\x04\x12\x02\0\x01\x12\
    \x03t\x0b\x18\n\x0c\n\x05\x04\x12\x02\0\x03\x12\x03t\x1b\x1c\n\x0b\n\x04\
    \x04\x12\x02\x01\x12\x03u\x04\x1c\n\r\n\x05\x04\x12\x02\x01\x04\x12\x04u\
    \x04t\x1d\n\x0c\n\x05\x04\x12\x02\x01\x05\x12\x03u\x04\n\n\x0c\n\x05\x04\
    \x12\x02\x01\x01\x12\x03u\x0b\x17\n\x0c\n\x05\x04\x12\x02\x01\x03\x12\
    \x03u\x1a\x1b\n\t\n\x02\x04\x13\x12\x03x\0\x1d\n\n\n\x03\x04\x13\x01\x12\
    \x03x\x08\x1a\n\n\n\x02\x04\x14\x12\x04z\0|\x01\n\n\n\x03\x04\x14\x01\
    \x12\x03z\x08\x1d\n\x0b\n\x04\x04\x14\x02\0\x12\x03{\x04\x19\n\r\n\x05\
    \x04\x14\x02\0\x04\x12\x04{\x04z\x1f\n\x0c\n\x05\x04\x14\x02\0\x06\x12\
    \x03{\x04\x0f\n\x0c\n\x05\x04\x14\x02\0\x01\x12\x03{\x10\x14\n\x0c\n\x05\
    \x04\x14\x02\0\x03\x12\x03{\x17\x18\n\t\n\x02\x04\x15\x12\x03~\0!\n\n\n\
    \x03\x04\x15\x01\x12\x03~\x08\x1e\n\x0c\n\x02\x04\x16\x12\x06\x80\x01\0\
    \x83\x01\x01\n\x0b\n\x03\x04\x16\x01\x12\x04\x80\x01\x08\x19\n\x0c\n\x04\
    \x04\x16\x02\0\x12\x04\x81\x01\x04\x15\n\x0f\n\x05\x04\x16\x02\0\x04\x12\
    \x06\x81\x01\x04\x80\x01\x1b\n\r\n\x05\x04\x16\x02\0\x05\x12\x04\x81\x01\
    \x04\n\n\r\n\x05\x04\x16\x02\0\x01\x12\x04\x81\x01\x0b\x10\n\r\n\x05\x04\
    \x16\x02\0\x03\x12\x04\x81\x01\x13\x14\n\x0c\n\x04\x04\x16\x02\x01\x12\
    \x04\x82\x01\x04\x13\n\x0f\n\x05\x04\x16\x02\x01\x04\x12\x06\x82\x01\x04\
    \x81\x01\x15\n\r\n\x05\x04\x16\x02\x01\x05\x12\x04\x82\x01\x04\t\n\r\n\
    \x05\x04\x16\x02\x01\x01\x12\x04\x82\x01\n\x0e\n\r\n\x05\x04\x16\x02\x01\
    \x03\x12\x04\x82\x01\x11\x12\n\n\n\x02\x04\x17\x12\x04\x85\x01\0\x1d\n\
    \x0b\n\x03\x04\x17\x01\x12\x04\x85\x01\x08\x1a\n\x0c\n\x02\x05\x01\x12\
    \x06\x87\x01\0\x8f\x01\x01\n\x0b\n\x03\x05\x01\x01\x12\x04\x87\x01\x05\
    \x11\n\x0c\n\x04\x05\x01\x02\0\x12\x04\x88\x01\x04\x15\n\r\n\x05\x05\x01\
    \x02\0\x01\x12\x04\x88\x01\x04\x10\n\r\n\x05\x05\x01\x02\0\x02\x12\x04\
    \x88\x01\x13\x14\n\x0c\n\x04\x05\x01\x02\x01\x12\x04\x89\x01\x04\x13\n\r\
    \n\x05\x05\x01\x02\x01\x01\x12\x04\x89\x01\x04\x0e\n\r\n\x05\x05\x01\x02\
    \x01\x02\x12\x04\x89\x01\x11\x12\n\x0c\n\x04\x05\x01\x02\x02\x12\x04\x8a\
    \x01\x04\x0e\n\r\n\x05\x05\x01\x02\x02\x01\x12\x04\x8a\x01\x04\t\n\r\n\
    \x05\x05\x01\x02\x02\x02\x12\x04\x8a\x01\x0c\r\n\x0c\n\x04\x05\x01\x02\
    \x03\x12\x04\x8b\x01\x04\x13\n\r\n\x05\x05\x01\x02\x03\x01\x12\x04\x8b\
    \x01\x04\x0e\n\r\n\x05\x05\x01\x02\x03\x02\x12\x04\x8b\x01\x11\x12\n\x0c\
    \n\x04\x05\x01\x02\x04\x12\x04\x8c\x01\x04\x17\n\r\n\x05\x05\x01\x02\x04\
    \x01\x12\x04\x8c\x01\x04\x12\n\r\n\x05\x05\x01\x02\x04\x02\x12\x04\x8c\
    \x01\x15\x16\n\x0c\n\x04\x05\x01\x02\x05\x12\x04\x8d\x01\x04\x14\n\r\n\
    \x05\x05\x01\x02\x05\x01\x12\x04\x8d\x01\x04\x0f\n\r\n\x05\x05\x01\x02\
    \x05\x02\x12\x04\x8d\x01\x12\x13\n\x0c\n\x04\x05\x01\x02\x06\x12\x04\x8e\
    \x01\x04\x13\n\r\n\x05\x05\x01\x02\x06\x01\x12\x04\x8e\x01\x04\x0e\n\r\n\
    \x05\x05\x01\x02\x06\x02\x12\x04\x8e\x01\x11\x12\n\x0c\n\x02\x04\x18\x12\
    \x06\x91\x01\0\x98\x01\x01\n\x0b\n\x03\x04\x18\x01\x12\x04\x91\x01\x08\
    \x14\n\x0c\n\x04\x04\x18\x02\0\x12\x04\x92\x01\x04\x1e\n\x0f\n\x05\x04\
    \x18\x02\0\x04\x12\x06\x92\x01\x04\x91\x01\x16\n\r\n\x05\x04\x18\x02\0\
    \x06\x12\x04\x92\x01\x04\x10\n\r\n\x05\x04\x18\x02\0\x01\x12\x04\x92\x01\
    \x11\x19\n\r\n\x05\x04\x18\x02\0\x03\x12\x04\x92\x01\x1c\x1d\n\x0c\n\x04\
    \x04\x18\x02\x01\x12\x04\x93\x01\x04&\n\x0f\n\x05\x04\x18\x02\x01\x04\
    \x12\x06\x93\x01\x04\x92\x01\x1e\n\r\n\x05\x04\x18\x02\x01\x06\x12\x04\
    \x93\x01\x04\x15\n\r\n\x05\x04\x18\x02\x01\x01\x12\x04\x93\x01\x16!\n\r\
    \n\x05\x04\x18\x02\x01\x03\x12\x04\x93\x01$%\n\x0c\n\x04\x04\x18\x02\x02\
    \x12\x04\x94\x01\x04\x1b\n\x0f\n\x05\x04\x18\x02\x02\x04\x12\x06\x94\x01\
    \x04\x93\x01&\n\r\n\x05\x04\x18\x02\x02\x06\x12\x04\x94\x01\x04\x10\n\r\
    \n\x05\x04\x18\x02\x02\x01\x12\x04\x94\x01\x11\x16\n\r\n\x05\x04\x18\x02\
    \x02\x03\x12\x04\x94\x01\x19\x1a\n\x0c\n\x04\x04\x18\x02\x03\x12\x04\x95\
    \x01\x04&\n\x0f\n\x05\x04\x18\x02\x03\x04\x12\x06\x95\x01\x04\x94\x01\
    \x1b\n\r\n\x05\x04\x18\x02\x03\x06\x12\x04\x95\x01\x04\x15\n\r\n\x05\x04\
    \x18\x02\x03\x01\x12\x04\x95\x01\x16!\n\r\n\x05\x04\x18\x02\x03\x03\x12\
    \x04\x95\x01$%\n\x0c\n\x04\x04\x18\x02\x04\x12\x04\x96\x01\x04.\n\x0f\n\
    \x05\x04\x18\x02\x04\x04\x12\x06\x96\x01\x04\x95\x01&\n\r\n\x05\x04\x18\
    \x02\x04\x06\x12\x04\x96\x01\x04\x19\n\r\n\x05\x04\x18\x02\x04\x01\x12\
    \x04\x96\x01\x1a)\n\r\n\x05\x04\x18\x02\x04\x03\x12\x04\x96\x01,-\n\x0c\
    \n\x04\x04\x18\x02\x05\x12\x04\x97\x01\x04&\n\x0f\n\x05\x04\x18\x02\x05\
    \x04\x12\x06\x97\x01\x04\x96\x01.\n\r\n\x05\x04\x18\x02\x05\x06\x12\x04\
    \x97\x01\x04\x15\n\r\n\x05\x04\x18\x02\x05\x01\x12\x04\x97\x01\x16!\n\r\
    \n\x05\x04\x18\x02\x05\x03\x12\x04\x97\x01$%\n\x0c\n\x02\x04\x19\x12\x06\
    \x9a\x01\0\xa1\x01\x01\n\x0b\n\x03\x04\x19\x01\x12\x04\x9a\x01\x08\x15\n\
    \x0c\n\x04\x04\x19\x02\0\x12\x04\x9b\x01\x04\x1e\n\x0f\n\x05\x04\x19\x02\
    \0\x04\x12\x06\x9b\x01\x04\x9a\x01\x17\n\r\n\x05\x04\x19\x02\0\x06\x12\
    \x04\x9b\x01\x04\x10\n\r\n\x05\x04\x19\x02\0\x01\x12\x04\x9b\x01\x11\x19\
    \n\r\n\x05\x04\x19\x02\0\x03\x12\x04\x9b\x01\x1c\x1d\n\x0c\n\x04\x04\x19\
    \x02\x01\x12\x04\x9c\x01\x04'\n\x0f\n\x05\x04\x19\x02\x01\x04\x12\x06\
    \x9c\x01\x04\x9b\x01\x1e\n\r\n\x05\x04\x19\x02\x01\x06\x12\x04\x9c\x01\
    \x04\x16\n\r\n\x05\x04\x19\x02\x01\x01\x12\x04\x9c\x01\x17\"\n\r\n\x05\
    \x04\x19\x02\x01\x03\x12\x04\x9c\x01%&\n\x0c\n\x04\x04\x19\x02\x02\x12\
    \x04\x9d\x01\x04\x1c\n\x0f\n\x05\x04\x19\x02\x02\x04\x12\x06\x9d\x01\x04\
    \x9c\x01'\n\r\n\x05\x04\x19\x02\x02\x06\x12\x04\x9d\x01\x04\x11\n\r\n\
    \x05\x04\x19\x02\x02\x01\x12\x04\x9d\x01\x12\x17\n\r\n\x05\x04\x19\x02\
    \x02\x03\x12\x04\x9d\x01\x1a\x1b\n\x0c\n\x04\x04\x19\x02\x03\x12\x04\x9e\
    \x01\x04'\n\x0f\n\x05\x04\x19\x02\x03\x04\x12\x06\x9e\x01\x04\x9d\x01\
    \x1c\n\r\n\x05\x04\x19\x02\x03\x06\x12\x04\x9e\x01\x04\x16\n\r\n\x05\x04\
    \x19\x02\x03\x01\x12\x04\x9e\x01\x17\"\n\r\n\x05\x04\x19\x02\x03\x03\x12\
    \x04\x9e\x01%&\n\x0c\n\x04\x04\x19\x02\x04\x12\x04\x9f\x01\x04/\n\x0f\n\
    \x05\x04\x19\x02\x04\x04\x12\x06\x9f\x01\x04\x9e\x01'\n\r\n\x05\x04\x19\
    \x02\x04\x06\x12\x04\x9f\x01\x04\x1a\n\r\n\x05\x04\x19\x02\x04\x01\x12\
    \x04\x9f\x01\x1b*\n\r\n\x05\x04\x19\x02\x04\x03\x12\x04\x9f\x01-.\n\x0c\
    \n\x04\x04\x19\x02\x05\x12\x04\xa0\x01\x04'\n\x0f\n\x05\x04\x19\x02\x05\
    \x04\x12\x06\xa0\x01\x04\x9f\x01/\n\r\n\x05\x04\x19\x02\x05\x06\x12\x04\
    \xa0\x01\x04\x16\n\r\n\x05\x04\x19\x02\x05\x01\x12\x04\xa0\x01\x17\"\n\r\
    \n\x05\x04\x19\x02\x05\x03\x12\x04\xa0\x01%&\n/\n\x02\x04\x1a\x12\x04\
    \xa4\x01\0\x1e\x1a#\x20For\x20get\x20the\x20leader\x20of\x20the\x20regio\
    n.\n\n\x0b\n\x03\x04\x1a\x01\x12\x04\xa4\x01\x08\x1b\n\x0c\n\x02\x04\x1b\
    \x12\x06\xa6\x01\0\xa8\x01\x01\n\x0b\n\x03\x04\x1b\x01\x12\x04\xa6\x01\
    \x08\x1c\n\x0c\n\x04\x04\x1b\x02\0\x12\x04\xa7\x01\x04\x1b\n\x0f\n\x05\
    \x04\x1b\x02\0\x04\x12\x06\xa7\x01\x04\xa6\x01\x1e\n\r\n\x05\x04\x1b\x02\
    \0\x06\x12\x04\xa7\x01\x04\x0f\n\r\n\x05\x04\x1b\x02\0\x01\x12\x04\xa7\
    \x01\x10\x16\n\r\n\x05\x04\x1b\x02\0\x03\x12\x04\xa7\x01\x19\x1a\n\xe2\
    \x04\n\x02\x04\x1c\x12\x04\xb3\x01\0\x1e\x1a\xd5\x04\x20For\x20getting\
    \x20more\x20information\x20of\x20the\x20region.\n\x20We\x20add\x20some\
    \x20admin\x20operations\x20(ChangePeer,\x20Split...)\x20into\x20the\x20p\
    b\x20job\x20list,\n\x20then\x20pd\x20server\x20will\x20peek\x20the\x20fi\
    rst\x20one,\x20handle\x20it\x20and\x20then\x20pop\x20it\x20from\x20the\
    \x20job\x20lib.\x20\n\x20But\x20sometimes,\x20the\x20pd\x20server\x20may\
    \x20crash\x20before\x20popping.\x20When\x20another\x20pd\x20server\n\x20\
    starts\x20and\x20finds\x20the\x20job\x20is\x20running\x20but\x20not\x20f\
    inished,\x20it\x20will\x20first\x20check\x20whether\n\x20the\x20raft\x20\
    server\x20already\x20has\x20handled\x20this\x20job.\n\x20E,g,\x20for\x20\
    ChangePeer,\x20if\x20we\x20add\x20Peer10\x20into\x20region1\x20and\x20fi\
    nd\x20region1\x20has\x20already\x20had\n\x20Peer10,\x20we\x20can\x20thin\
    k\x20this\x20ChangePeer\x20is\x20finished,\x20and\x20can\x20pop\x20this\
    \x20job\x20from\x20job\x20list\n\x20directly.\n\n\x0b\n\x03\x04\x1c\x01\
    \x12\x04\xb3\x01\x08\x1b\n\x0c\n\x02\x04\x1d\x12\x06\xb5\x01\0\xb8\x01\
    \x01\n\x0b\n\x03\x04\x1d\x01\x12\x04\xb5\x01\x08\x1c\n\x0c\n\x04\x04\x1d\
    \x02\0\x12\x04\xb6\x01\x04\x1d\n\x0f\n\x05\x04\x1d\x02\0\x04\x12\x06\xb6\
    \x01\x04\xb5\x01\x1e\n\r\n\x05\x04\x1d\x02\0\x06\x12\x04\xb6\x01\x04\x11\
    \n\r\n\x05\x04\x1d\x02\0\x01\x12\x04\xb6\x01\x12\x18\n\r\n\x05\x04\x1d\
    \x02\0\x03\x12\x04\xb6\x01\x1b\x1c\n\x0c\n\x04\x04\x1d\x02\x01\x12\x04\
    \xb7\x01\x04\x1b\n\x0f\n\x05\x04\x1d\x02\x01\x04\x12\x06\xb7\x01\x04\xb6\
    \x01\x1d\n\r\n\x05\x04\x1d\x02\x01\x06\x12\x04\xb7\x01\x04\x0f\n\r\n\x05\
    \x04\x1d\x02\x01\x01\x12\x04\xb7\x01\x10\x16\n\r\n\x05\x04\x1d\x02\x01\
    \x03\x12\x04\xb7\x01\x19\x1a\n\x0c\n\x02\x05\x02\x12\x06\xbb\x01\0\xbf\
    \x01\x01\n\x0b\n\x03\x05\x02\x01\x12\x04\xbb\x01\x05\x12\n\x0c\n\x04\x05\
    \x02\x02\0\x12\x04\xbc\x01\x04\x16\n\r\n\x05\x05\x02\x02\0\x01\x12\x04\
    \xbc\x01\x04\x11\n\r\n\x05\x05\x02\x02\0\x02\x12\x04\xbc\x01\x14\x15\n\
    \x0c\n\x04\x05\x02\x02\x01\x12\x04\xbd\x01\x04\x15\n\r\n\x05\x05\x02\x02\
    \x01\x01\x12\x04\xbd\x01\x04\x10\n\r\n\x05\x05\x02\x02\x01\x02\x12\x04\
    \xbd\x01\x13\x14\n\x0c\n\x04\x05\x02\x02\x02\x12\x04\xbe\x01\x04\x15\n\r\
    \n\x05\x05\x02\x02\x02\x01\x12\x04\xbe\x01\x04\x10\n\r\n\x05\x05\x02\x02\
    \x02\x02\x12\x04\xbe\x01\x13\x14\n\x0c\n\x02\x04\x1e\x12\x06\xc1\x01\0\
    \xc5\x01\x01\n\x0b\n\x03\x04\x1e\x01\x12\x04\xc1\x01\x08\x15\n\x0c\n\x04\
    \x04\x1e\x02\0\x12\x04\xc2\x01\x04\x1f\n\x0f\n\x05\x04\x1e\x02\0\x04\x12\
    \x06\xc2\x01\x04\xc1\x01\x17\n\r\n\x05\x04\x1e\x02\0\x06\x12\x04\xc2\x01\
    \x04\x11\n\r\n\x05\x04\x1e\x02\0\x01\x12\x04\xc2\x01\x12\x1a\n\r\n\x05\
    \x04\x1e\x02\0\x03\x12\x04\xc2\x01\x1d\x1e\n\x0c\n\x04\x04\x1e\x02\x01\
    \x12\x04\xc3\x01\x04*\n\x0f\n\x05\x04\x1e\x02\x01\x04\x12\x06\xc3\x01\
    \x04\xc2\x01\x1f\n\r\n\x05\x04\x1e\x02\x01\x06\x12\x04\xc3\x01\x04\x17\n\
    \r\n\x05\x04\x1e\x02\x01\x01\x12\x04\xc3\x01\x18%\n\r\n\x05\x04\x1e\x02\
    \x01\x03\x12\x04\xc3\x01()\n\x0c\n\x04\x04\x1e\x02\x02\x12\x04\xc4\x01\
    \x04*\n\x0f\n\x05\x04\x1e\x02\x02\x04\x12\x06\xc4\x01\x04\xc3\x01*\n\r\n\
    \x05\x04\x1e\x02\x02\x06\x12\x04\xc4\x01\x04\x17\n\r\n\x05\x04\x1e\x02\
    \x02\x01\x12\x04\xc4\x01\x18%\n\r\n\x05\x04\x1e\x02\x02\x03\x12\x04\xc4\
    \x01()\n\x0c\n\x02\x04\x1f\x12\x06\xc7\x01\0\xcb\x01\x01\n\x0b\n\x03\x04\
    \x1f\x01\x12\x04\xc7\x01\x08\x16\n\x0c\n\x04\x04\x1f\x02\0\x12\x04\xc8\
    \x01\x04\x1f\n\x0f\n\x05\x04\x1f\x02\0\x04\x12\x06\xc8\x01\x04\xc7\x01\
    \x18\n\r\n\x05\x04\x1f\x02\0\x06\x12\x04\xc8\x01\x04\x11\n\r\n\x05\x04\
    \x1f\x02\0\x01\x12\x04\xc8\x01\x12\x1a\n\r\n\x05\x04\x1f\x02\0\x03\x12\
    \x04\xc8\x01\x1d\x1e\n\x0c\n\x04\x04\x1f\x02\x01\x12\x04\xc9\x01\x04+\n\
    \x0f\n\x05\x04\x1f\x02\x01\x04\x12\x06\xc9\x01\x04\xc8\x01\x1f\n\r\n\x05\
    \x04\x1f\x02\x01\x06\x12\x04\xc9\x01\x04\x18\n\r\n\x05\x04\x1f\x02\x01\
    \x01\x12\x04\xc9\x01\x19&\n\r\n\x05\x04\x1f\x02\x01\x03\x12\x04\xc9\x01)\
    *\n\x0c\n\x04\x04\x1f\x02\x02\x12\x04\xca\x01\x04+\n\x0f\n\x05\x04\x1f\
    \x02\x02\x04\x12\x06\xca\x01\x04\xc9\x01+\n\r\n\x05\x04\x1f\x02\x02\x06\
    \x12\x04\xca\x01\x04\x18\n\r\n\x05\x04\x1f\x02\x02\x01\x12\x04\xca\x01\
    \x19&\n\r\n\x05\x04\x1f\x02\x02\x03\x12\x04\xca\x01)*\n\x0c\n\x02\x04\
    \x20\x12\x06\xcd\x01\0\xd9\x01\x01\n\x0b\n\x03\x04\x20\x01\x12\x04\xcd\
    \x01\x08\x19\n\x0c\n\x04\x04\x20\x02\0\x12\x04\xce\x01\x04\x19\n\x0f\n\
    \x05\x04\x20\x02\0\x04\x12\x06\xce\x01\x04\xcd\x01\x1b\n\r\n\x05\x04\x20\
    \x02\0\x05\x12\x04\xce\x01\x04\n\n\r\n\x05\x04\x20\x02\0\x01\x12\x04\xce\
    \x01\x0b\x14\n\r\n\x05\x04\x20\x02\0\x03\x12\x04\xce\x01\x17\x18\n\x0c\n\
    \x04\x04\x20\x02\x01\x12\x04\xcf\x01\x04\x19\n\x0f\n\x05\x04\x20\x02\x01\
    \x04\x12\x06\xcf\x01\x04\xce\x01\x19\n\r\n\x05\x04\x20\x02\x01\x06\x12\
    \x04\xcf\x01\x04\x0f\n\r\n\x05\x04\x20\x02\x01\x01\x12\x04\xcf\x01\x10\
    \x14\n\r\n\x05\x04\x20\x02\x01\x03\x12\x04\xcf\x01\x17\x18\n+\n\x04\x04\
    \x20\x02\x02\x12\x04\xd1\x01\x04\x19\x1a\x1d\x20true\x20for\x20read\x20l\
    inearization\n\n\x0f\n\x05\x04\x20\x02\x02\x04\x12\x06\xd1\x01\x04\xcf\
    \x01\x19\n\r\n\x05\x04\x20\x02\x02\x05\x12\x04\xd1\x01\x04\x08\n\r\n\x05\
    \x04\x20\x02\x02\x01\x12\x04\xd1\x01\t\x14\n\r\n\x05\x04\x20\x02\x02\x03\
    \x12\x04\xd1\x01\x17\x18\n3\n\x04\x04\x20\x02\x03\x12\x04\xd3\x01\x04\
    \x13\x1a%\x2016\x20bytes,\x20to\x20distinguish\x20request.\x20\x20\n\n\
    \x0f\n\x05\x04\x20\x02\x03\x04\x12\x06\xd3\x01\x04\xd1\x01\x19\n\r\n\x05\
    \x04\x20\x02\x03\x05\x12\x04\xd3\x01\x04\t\n\r\n\x05\x04\x20\x02\x03\x01\
    \x12\x04\xd3\x01\n\x0e\n\r\n\x05\x04\x20\x02\x03\x03\x12\x04\xd3\x01\x11\
    \x12\n\x0c\n\x04\x04\x20\x02\x04\x12\x04\xd5\x01\x04(\n\x0f\n\x05\x04\
    \x20\x02\x04\x04\x12\x06\xd5\x01\x04\xd3\x01\x13\n\r\n\x05\x04\x20\x02\
    \x04\x06\x12\x04\xd5\x01\x04\x16\n\r\n\x05\x04\x20\x02\x04\x01\x12\x04\
    \xd5\x01\x17#\n\r\n\x05\x04\x20\x02\x04\x03\x12\x04\xd5\x01&'\n\x0c\n\
    \x04\x04\x20\x02\x05\x12\x04\xd6\x01\x04\x14\n\x0f\n\x05\x04\x20\x02\x05\
    \x04\x12\x06\xd6\x01\x04\xd5\x01(\n\r\n\x05\x04\x20\x02\x05\x05\x12\x04\
    \xd6\x01\x04\n\n\r\n\x05\x04\x20\x02\x05\x01\x12\x04\xd6\x01\x0b\x0f\n\r\
    \n\x05\x04\x20\x02\x05\x03\x12\x04\xd6\x01\x12\x13\n\x0c\n\x04\x04\x20\
    \x02\x06\x12\x04\xd8\x01\x04\x16\n\x0f\n\x05\x04\x20\x02\x06\x04\x12\x06\
    \xd8\x01\x04\xd6\x01\x14\n\r\n\x05\x04\x20\x02\x06\x05\x12\x04\xd8\x01\
    \x04\x08\n\r\n\x05\x04\x20\x02\x06\x01\x12\x04\xd8\x01\t\x11\n\r\n\x05\
    \x04\x20\x02\x06\x03\x12\x04\xd8\x01\x14\x15\n\x0c\n\x02\x04!\x12\x06\
    \xdb\x01\0\xdf\x01\x01\n\x0b\n\x03\x04!\x01\x12\x04\xdb\x01\x08\x1a\n\
    \x0c\n\x04\x04!\x02\0\x12\x04\xdc\x01\x04\x1c\n\x0f\n\x05\x04!\x02\0\x04\
    \x12\x06\xdc\x01\x04\xdb\x01\x1c\n\r\n\x05\x04!\x02\0\x06\x12\x04\xdc\
    \x01\x04\x11\n\r\n\x05\x04!\x02\0\x01\x12\x04\xdc\x01\x12\x17\n\r\n\x05\
    \x04!\x02\0\x03\x12\x04\xdc\x01\x1a\x1b\n\x0c\n\x04\x04!\x02\x01\x12\x04\
    \xdd\x01\x04\x13\n\x0f\n\x05\x04!\x02\x01\x04\x12\x06\xdd\x01\x04\xdc\
    \x01\x1c\n\r\n\x05\x04!\x02\x01\x05\x12\x04\xdd\x01\x04\t\n\r\n\x05\x04!\
    \x02\x01\x01\x12\x04\xdd\x01\n\x0e\n\r\n\x05\x04!\x02\x01\x03\x12\x04\
    \xdd\x01\x11\x12\n\x0c\n\x04\x04!\x02\x02\x12\x04\xde\x01\x04\x1c\n\x0f\
    \n\x05\x04!\x02\x02\x04\x12\x06\xde\x01\x04\xdd\x01\x13\n\r\n\x05\x04!\
    \x02\x02\x05\x12\x04\xde\x01\x04\n\n\r\n\x05\x04!\x02\x02\x01\x12\x04\
    \xde\x01\x0b\x17\n\r\n\x05\x04!\x02\x02\x03\x12\x04\xde\x01\x1a\x1b\n\
    \x0c\n\x02\x04\"\x12\x06\xe1\x01\0\xe8\x01\x01\n\x0b\n\x03\x04\"\x01\x12\
    \x04\xe1\x01\x08\x16\n\x0c\n\x04\x04\"\x02\0\x12\x04\xe2\x01\x04!\n\x0f\
    \n\x05\x04\"\x02\0\x04\x12\x06\xe2\x01\x04\xe1\x01\x18\n\r\n\x05\x04\"\
    \x02\0\x06\x12\x04\xe2\x01\x04\x15\n\r\n\x05\x04\"\x02\0\x01\x12\x04\xe2\
    \x01\x16\x1c\n\r\n\x05\x04\"\x02\0\x03\x12\x04\xe2\x01\x1f\x20\nZ\n\x04\
    \x04\"\x02\x01\x12\x04\xe5\x01\x04\"\x1aL\x20We\x20can't\x20enclose\x20n\
    ormal\x20requests\x20and\x20administrator\x20request\n\x20at\x20same\x20\
    time.\x20\n\n\r\n\x05\x04\"\x02\x01\x04\x12\x04\xe5\x01\x04\x0c\n\r\n\
    \x05\x04\"\x02\x01\x06\x12\x04\xe5\x01\r\x14\n\r\n\x05\x04\"\x02\x01\x01\
    \x12\x04\xe5\x01\x15\x1d\n\r\n\x05\x04\"\x02\x01\x03\x12\x04\xe5\x01\x20\
    !\n\x0c\n\x04\x04\"\x02\x02\x12\x04\xe6\x01\x04#\n\x0f\n\x05\x04\"\x02\
    \x02\x04\x12\x06\xe6\x01\x04\xe5\x01\"\n\r\n\x05\x04\"\x02\x02\x06\x12\
    \x04\xe6\x01\x04\x10\n\r\n\x05\x04\"\x02\x02\x01\x12\x04\xe6\x01\x11\x1e\
    \n\r\n\x05\x04\"\x02\x02\x03\x12\x04\xe6\x01!\"\n\x0c\n\x04\x04\"\x02\
    \x03\x12\x04\xe7\x01\x04%\n\x0f\n\x05\x04\"\x02\x03\x04\x12\x06\xe7\x01\
    \x04\xe6\x01#\n\r\n\x05\x04\"\x02\x03\x06\x12\x04\xe7\x01\x04\x11\n\r\n\
    \x05\x04\"\x02\x03\x01\x12\x04\xe7\x01\x12\x20\n\r\n\x05\x04\"\x02\x03\
    \x03\x12\x04\xe7\x01#$\n\x0c\n\x02\x04#\x12\x06\xea\x01\0\xef\x01\x01\n\
    \x0b\n\x03\x04#\x01\x12\x04\xea\x01\x08\x17\n\x0c\n\x04\x04#\x02\0\x12\
    \x04\xeb\x01\x04\"\n\x0f\n\x05\x04#\x02\0\x04\x12\x06\xeb\x01\x04\xea\
    \x01\x19\n\r\n\x05\x04#\x02\0\x06\x12\x04\xeb\x01\x04\x16\n\r\n\x05\x04#\
    \x02\0\x01\x12\x04\xeb\x01\x17\x1d\n\r\n\x05\x04#\x02\0\x03\x12\x04\xeb\
    \x01\x20!\n\x0c\n\x04\x04#\x02\x01\x12\x04\xec\x01\x04$\n\r\n\x05\x04#\
    \x02\x01\x04\x12\x04\xec\x01\x04\x0c\n\r\n\x05\x04#\x02\x01\x06\x12\x04\
    \xec\x01\r\x15\n\r\n\x05\x04#\x02\x01\x01\x12\x04\xec\x01\x16\x1f\n\r\n\
    \x05\x04#\x02\x01\x03\x12\x04\xec\x01\"#\n\x0c\n\x04\x04#\x02\x02\x12\
    \x04\xed\x01\x04%\n\x0f\n\x05\x04#\x02\x02\x04\x12\x06\xed\x01\x04\xec\
    \x01$\n\r\n\x05\x04#\x02\x02\x06\x12\x04\xed\x01\x04\x11\n\r\n\x05\x04#\
    \x02\x02\x01\x12\x04\xed\x01\x12\x20\n\r\n\x05\x04#\x02\x02\x03\x12\x04\
    \xed\x01#$\n\x0c\n\x04\x04#\x02\x03\x12\x04\xee\x01\x04'\n\x0f\n\x05\x04\
    #\x02\x03\x04\x12\x06\xee\x01\x04\xed\x01%\n\r\n\x05\x04#\x02\x03\x06\
    \x12\x04\xee\x01\x04\x12\n\r\n\x05\x04#\x02\x03\x01\x12\x04\xee\x01\x13\
    \"\n\r\n\x05\x04#\x02\x03\x03\x12\x04\xee\x01%&b\x06proto3\
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
