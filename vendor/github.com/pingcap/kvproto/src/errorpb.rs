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
pub struct NotLeader {
    // message fields
    pub region_id: u64,
    pub leader: ::protobuf::SingularPtrField<super::metapb::Peer>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for NotLeader {}

impl NotLeader {
    pub fn new() -> NotLeader {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static NotLeader {
        static mut instance: ::protobuf::lazy::Lazy<NotLeader> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const NotLeader,
        };
        unsafe {
            instance.get(NotLeader::new)
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

impl ::protobuf::Message for NotLeader {
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
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_uint64()?;
                    self.region_id = tmp;
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
        if self.region_id != 0 {
            my_size += ::protobuf::rt::value_size(1, self.region_id, ::protobuf::wire_format::WireTypeVarint);
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
        if self.region_id != 0 {
            os.write_uint64(1, self.region_id)?;
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

impl ::protobuf::MessageStatic for NotLeader {
    fn new() -> NotLeader {
        NotLeader::new()
    }

    fn descriptor_static(_: ::std::option::Option<NotLeader>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "region_id",
                    NotLeader::get_region_id_for_reflect,
                    NotLeader::mut_region_id_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<super::metapb::Peer>>(
                    "leader",
                    NotLeader::get_leader_for_reflect,
                    NotLeader::mut_leader_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<NotLeader>(
                    "NotLeader",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for NotLeader {
    fn clear(&mut self) {
        self.clear_region_id();
        self.clear_leader();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for NotLeader {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for NotLeader {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct StoreNotMatch {
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for StoreNotMatch {}

impl StoreNotMatch {
    pub fn new() -> StoreNotMatch {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static StoreNotMatch {
        static mut instance: ::protobuf::lazy::Lazy<StoreNotMatch> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const StoreNotMatch,
        };
        unsafe {
            instance.get(StoreNotMatch::new)
        }
    }
}

impl ::protobuf::Message for StoreNotMatch {
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

impl ::protobuf::MessageStatic for StoreNotMatch {
    fn new() -> StoreNotMatch {
        StoreNotMatch::new()
    }

    fn descriptor_static(_: ::std::option::Option<StoreNotMatch>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let fields = ::std::vec::Vec::new();
                ::protobuf::reflect::MessageDescriptor::new::<StoreNotMatch>(
                    "StoreNotMatch",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for StoreNotMatch {
    fn clear(&mut self) {
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for StoreNotMatch {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for StoreNotMatch {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct RegionNotFound {
    // message fields
    pub region_id: u64,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for RegionNotFound {}

impl RegionNotFound {
    pub fn new() -> RegionNotFound {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static RegionNotFound {
        static mut instance: ::protobuf::lazy::Lazy<RegionNotFound> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const RegionNotFound,
        };
        unsafe {
            instance.get(RegionNotFound::new)
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
}

impl ::protobuf::Message for RegionNotFound {
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
        if self.region_id != 0 {
            my_size += ::protobuf::rt::value_size(1, self.region_id, ::protobuf::wire_format::WireTypeVarint);
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if self.region_id != 0 {
            os.write_uint64(1, self.region_id)?;
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

impl ::protobuf::MessageStatic for RegionNotFound {
    fn new() -> RegionNotFound {
        RegionNotFound::new()
    }

    fn descriptor_static(_: ::std::option::Option<RegionNotFound>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "region_id",
                    RegionNotFound::get_region_id_for_reflect,
                    RegionNotFound::mut_region_id_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<RegionNotFound>(
                    "RegionNotFound",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for RegionNotFound {
    fn clear(&mut self) {
        self.clear_region_id();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for RegionNotFound {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for RegionNotFound {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct KeyNotInRegion {
    // message fields
    pub key: ::std::vec::Vec<u8>,
    pub region_id: u64,
    pub start_key: ::std::vec::Vec<u8>,
    pub end_key: ::std::vec::Vec<u8>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for KeyNotInRegion {}

impl KeyNotInRegion {
    pub fn new() -> KeyNotInRegion {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static KeyNotInRegion {
        static mut instance: ::protobuf::lazy::Lazy<KeyNotInRegion> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const KeyNotInRegion,
        };
        unsafe {
            instance.get(KeyNotInRegion::new)
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

    // bytes start_key = 3;

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

    // bytes end_key = 4;

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

impl ::protobuf::Message for KeyNotInRegion {
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
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_uint64()?;
                    self.region_id = tmp;
                },
                3 => {
                    ::protobuf::rt::read_singular_proto3_bytes_into(wire_type, is, &mut self.start_key)?;
                },
                4 => {
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
        if !self.key.is_empty() {
            my_size += ::protobuf::rt::bytes_size(1, &self.key);
        }
        if self.region_id != 0 {
            my_size += ::protobuf::rt::value_size(2, self.region_id, ::protobuf::wire_format::WireTypeVarint);
        }
        if !self.start_key.is_empty() {
            my_size += ::protobuf::rt::bytes_size(3, &self.start_key);
        }
        if !self.end_key.is_empty() {
            my_size += ::protobuf::rt::bytes_size(4, &self.end_key);
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if !self.key.is_empty() {
            os.write_bytes(1, &self.key)?;
        }
        if self.region_id != 0 {
            os.write_uint64(2, self.region_id)?;
        }
        if !self.start_key.is_empty() {
            os.write_bytes(3, &self.start_key)?;
        }
        if !self.end_key.is_empty() {
            os.write_bytes(4, &self.end_key)?;
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

impl ::protobuf::MessageStatic for KeyNotInRegion {
    fn new() -> KeyNotInRegion {
        KeyNotInRegion::new()
    }

    fn descriptor_static(_: ::std::option::Option<KeyNotInRegion>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeBytes>(
                    "key",
                    KeyNotInRegion::get_key_for_reflect,
                    KeyNotInRegion::mut_key_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "region_id",
                    KeyNotInRegion::get_region_id_for_reflect,
                    KeyNotInRegion::mut_region_id_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeBytes>(
                    "start_key",
                    KeyNotInRegion::get_start_key_for_reflect,
                    KeyNotInRegion::mut_start_key_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeBytes>(
                    "end_key",
                    KeyNotInRegion::get_end_key_for_reflect,
                    KeyNotInRegion::mut_end_key_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<KeyNotInRegion>(
                    "KeyNotInRegion",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for KeyNotInRegion {
    fn clear(&mut self) {
        self.clear_key();
        self.clear_region_id();
        self.clear_start_key();
        self.clear_end_key();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for KeyNotInRegion {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for KeyNotInRegion {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct StaleEpoch {
    // message fields
    pub new_regions: ::protobuf::RepeatedField<super::metapb::Region>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for StaleEpoch {}

impl StaleEpoch {
    pub fn new() -> StaleEpoch {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static StaleEpoch {
        static mut instance: ::protobuf::lazy::Lazy<StaleEpoch> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const StaleEpoch,
        };
        unsafe {
            instance.get(StaleEpoch::new)
        }
    }

    // repeated .metapb.Region new_regions = 1;

    pub fn clear_new_regions(&mut self) {
        self.new_regions.clear();
    }

    // Param is passed by value, moved
    pub fn set_new_regions(&mut self, v: ::protobuf::RepeatedField<super::metapb::Region>) {
        self.new_regions = v;
    }

    // Mutable pointer to the field.
    pub fn mut_new_regions(&mut self) -> &mut ::protobuf::RepeatedField<super::metapb::Region> {
        &mut self.new_regions
    }

    // Take field
    pub fn take_new_regions(&mut self) -> ::protobuf::RepeatedField<super::metapb::Region> {
        ::std::mem::replace(&mut self.new_regions, ::protobuf::RepeatedField::new())
    }

    pub fn get_new_regions(&self) -> &[super::metapb::Region] {
        &self.new_regions
    }

    fn get_new_regions_for_reflect(&self) -> &::protobuf::RepeatedField<super::metapb::Region> {
        &self.new_regions
    }

    fn mut_new_regions_for_reflect(&mut self) -> &mut ::protobuf::RepeatedField<super::metapb::Region> {
        &mut self.new_regions
    }
}

impl ::protobuf::Message for StaleEpoch {
    fn is_initialized(&self) -> bool {
        for v in &self.new_regions {
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
                    ::protobuf::rt::read_repeated_message_into(wire_type, is, &mut self.new_regions)?;
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
        for value in &self.new_regions {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        for v in &self.new_regions {
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

impl ::protobuf::MessageStatic for StaleEpoch {
    fn new() -> StaleEpoch {
        StaleEpoch::new()
    }

    fn descriptor_static(_: ::std::option::Option<StaleEpoch>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_repeated_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<super::metapb::Region>>(
                    "new_regions",
                    StaleEpoch::get_new_regions_for_reflect,
                    StaleEpoch::mut_new_regions_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<StaleEpoch>(
                    "StaleEpoch",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for StaleEpoch {
    fn clear(&mut self) {
        self.clear_new_regions();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for StaleEpoch {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for StaleEpoch {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct ServerIsBusy {
    // message fields
    pub reason: ::std::string::String,
    pub backoff_ms: u64,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for ServerIsBusy {}

impl ServerIsBusy {
    pub fn new() -> ServerIsBusy {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static ServerIsBusy {
        static mut instance: ::protobuf::lazy::Lazy<ServerIsBusy> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ServerIsBusy,
        };
        unsafe {
            instance.get(ServerIsBusy::new)
        }
    }

    // string reason = 1;

    pub fn clear_reason(&mut self) {
        self.reason.clear();
    }

    // Param is passed by value, moved
    pub fn set_reason(&mut self, v: ::std::string::String) {
        self.reason = v;
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_reason(&mut self) -> &mut ::std::string::String {
        &mut self.reason
    }

    // Take field
    pub fn take_reason(&mut self) -> ::std::string::String {
        ::std::mem::replace(&mut self.reason, ::std::string::String::new())
    }

    pub fn get_reason(&self) -> &str {
        &self.reason
    }

    fn get_reason_for_reflect(&self) -> &::std::string::String {
        &self.reason
    }

    fn mut_reason_for_reflect(&mut self) -> &mut ::std::string::String {
        &mut self.reason
    }

    // uint64 backoff_ms = 2;

    pub fn clear_backoff_ms(&mut self) {
        self.backoff_ms = 0;
    }

    // Param is passed by value, moved
    pub fn set_backoff_ms(&mut self, v: u64) {
        self.backoff_ms = v;
    }

    pub fn get_backoff_ms(&self) -> u64 {
        self.backoff_ms
    }

    fn get_backoff_ms_for_reflect(&self) -> &u64 {
        &self.backoff_ms
    }

    fn mut_backoff_ms_for_reflect(&mut self) -> &mut u64 {
        &mut self.backoff_ms
    }
}

impl ::protobuf::Message for ServerIsBusy {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !is.eof()? {
            let (field_number, wire_type) = is.read_tag_unpack()?;
            match field_number {
                1 => {
                    ::protobuf::rt::read_singular_proto3_string_into(wire_type, is, &mut self.reason)?;
                },
                2 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_uint64()?;
                    self.backoff_ms = tmp;
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
        if !self.reason.is_empty() {
            my_size += ::protobuf::rt::string_size(1, &self.reason);
        }
        if self.backoff_ms != 0 {
            my_size += ::protobuf::rt::value_size(2, self.backoff_ms, ::protobuf::wire_format::WireTypeVarint);
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if !self.reason.is_empty() {
            os.write_string(1, &self.reason)?;
        }
        if self.backoff_ms != 0 {
            os.write_uint64(2, self.backoff_ms)?;
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

impl ::protobuf::MessageStatic for ServerIsBusy {
    fn new() -> ServerIsBusy {
        ServerIsBusy::new()
    }

    fn descriptor_static(_: ::std::option::Option<ServerIsBusy>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeString>(
                    "reason",
                    ServerIsBusy::get_reason_for_reflect,
                    ServerIsBusy::mut_reason_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "backoff_ms",
                    ServerIsBusy::get_backoff_ms_for_reflect,
                    ServerIsBusy::mut_backoff_ms_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<ServerIsBusy>(
                    "ServerIsBusy",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for ServerIsBusy {
    fn clear(&mut self) {
        self.clear_reason();
        self.clear_backoff_ms();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for ServerIsBusy {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for ServerIsBusy {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct StaleCommand {
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for StaleCommand {}

impl StaleCommand {
    pub fn new() -> StaleCommand {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static StaleCommand {
        static mut instance: ::protobuf::lazy::Lazy<StaleCommand> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const StaleCommand,
        };
        unsafe {
            instance.get(StaleCommand::new)
        }
    }
}

impl ::protobuf::Message for StaleCommand {
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

impl ::protobuf::MessageStatic for StaleCommand {
    fn new() -> StaleCommand {
        StaleCommand::new()
    }

    fn descriptor_static(_: ::std::option::Option<StaleCommand>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let fields = ::std::vec::Vec::new();
                ::protobuf::reflect::MessageDescriptor::new::<StaleCommand>(
                    "StaleCommand",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for StaleCommand {
    fn clear(&mut self) {
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for StaleCommand {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for StaleCommand {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct RaftEntryTooLarge {
    // message fields
    pub region_id: u64,
    pub entry_size: u64,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for RaftEntryTooLarge {}

impl RaftEntryTooLarge {
    pub fn new() -> RaftEntryTooLarge {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static RaftEntryTooLarge {
        static mut instance: ::protobuf::lazy::Lazy<RaftEntryTooLarge> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const RaftEntryTooLarge,
        };
        unsafe {
            instance.get(RaftEntryTooLarge::new)
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

    // uint64 entry_size = 2;

    pub fn clear_entry_size(&mut self) {
        self.entry_size = 0;
    }

    // Param is passed by value, moved
    pub fn set_entry_size(&mut self, v: u64) {
        self.entry_size = v;
    }

    pub fn get_entry_size(&self) -> u64 {
        self.entry_size
    }

    fn get_entry_size_for_reflect(&self) -> &u64 {
        &self.entry_size
    }

    fn mut_entry_size_for_reflect(&mut self) -> &mut u64 {
        &mut self.entry_size
    }
}

impl ::protobuf::Message for RaftEntryTooLarge {
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
                    self.region_id = tmp;
                },
                2 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_uint64()?;
                    self.entry_size = tmp;
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
        if self.entry_size != 0 {
            my_size += ::protobuf::rt::value_size(2, self.entry_size, ::protobuf::wire_format::WireTypeVarint);
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if self.region_id != 0 {
            os.write_uint64(1, self.region_id)?;
        }
        if self.entry_size != 0 {
            os.write_uint64(2, self.entry_size)?;
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

impl ::protobuf::MessageStatic for RaftEntryTooLarge {
    fn new() -> RaftEntryTooLarge {
        RaftEntryTooLarge::new()
    }

    fn descriptor_static(_: ::std::option::Option<RaftEntryTooLarge>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "region_id",
                    RaftEntryTooLarge::get_region_id_for_reflect,
                    RaftEntryTooLarge::mut_region_id_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "entry_size",
                    RaftEntryTooLarge::get_entry_size_for_reflect,
                    RaftEntryTooLarge::mut_entry_size_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<RaftEntryTooLarge>(
                    "RaftEntryTooLarge",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for RaftEntryTooLarge {
    fn clear(&mut self) {
        self.clear_region_id();
        self.clear_entry_size();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for RaftEntryTooLarge {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for RaftEntryTooLarge {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct Error {
    // message fields
    pub message: ::std::string::String,
    pub not_leader: ::protobuf::SingularPtrField<NotLeader>,
    pub region_not_found: ::protobuf::SingularPtrField<RegionNotFound>,
    pub key_not_in_region: ::protobuf::SingularPtrField<KeyNotInRegion>,
    pub stale_epoch: ::protobuf::SingularPtrField<StaleEpoch>,
    pub server_is_busy: ::protobuf::SingularPtrField<ServerIsBusy>,
    pub stale_command: ::protobuf::SingularPtrField<StaleCommand>,
    pub store_not_match: ::protobuf::SingularPtrField<StoreNotMatch>,
    pub raft_entry_too_large: ::protobuf::SingularPtrField<RaftEntryTooLarge>,
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

    // string message = 1;

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

    // .errorpb.NotLeader not_leader = 2;

    pub fn clear_not_leader(&mut self) {
        self.not_leader.clear();
    }

    pub fn has_not_leader(&self) -> bool {
        self.not_leader.is_some()
    }

    // Param is passed by value, moved
    pub fn set_not_leader(&mut self, v: NotLeader) {
        self.not_leader = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_not_leader(&mut self) -> &mut NotLeader {
        if self.not_leader.is_none() {
            self.not_leader.set_default();
        }
        self.not_leader.as_mut().unwrap()
    }

    // Take field
    pub fn take_not_leader(&mut self) -> NotLeader {
        self.not_leader.take().unwrap_or_else(|| NotLeader::new())
    }

    pub fn get_not_leader(&self) -> &NotLeader {
        self.not_leader.as_ref().unwrap_or_else(|| NotLeader::default_instance())
    }

    fn get_not_leader_for_reflect(&self) -> &::protobuf::SingularPtrField<NotLeader> {
        &self.not_leader
    }

    fn mut_not_leader_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<NotLeader> {
        &mut self.not_leader
    }

    // .errorpb.RegionNotFound region_not_found = 3;

    pub fn clear_region_not_found(&mut self) {
        self.region_not_found.clear();
    }

    pub fn has_region_not_found(&self) -> bool {
        self.region_not_found.is_some()
    }

    // Param is passed by value, moved
    pub fn set_region_not_found(&mut self, v: RegionNotFound) {
        self.region_not_found = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_region_not_found(&mut self) -> &mut RegionNotFound {
        if self.region_not_found.is_none() {
            self.region_not_found.set_default();
        }
        self.region_not_found.as_mut().unwrap()
    }

    // Take field
    pub fn take_region_not_found(&mut self) -> RegionNotFound {
        self.region_not_found.take().unwrap_or_else(|| RegionNotFound::new())
    }

    pub fn get_region_not_found(&self) -> &RegionNotFound {
        self.region_not_found.as_ref().unwrap_or_else(|| RegionNotFound::default_instance())
    }

    fn get_region_not_found_for_reflect(&self) -> &::protobuf::SingularPtrField<RegionNotFound> {
        &self.region_not_found
    }

    fn mut_region_not_found_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<RegionNotFound> {
        &mut self.region_not_found
    }

    // .errorpb.KeyNotInRegion key_not_in_region = 4;

    pub fn clear_key_not_in_region(&mut self) {
        self.key_not_in_region.clear();
    }

    pub fn has_key_not_in_region(&self) -> bool {
        self.key_not_in_region.is_some()
    }

    // Param is passed by value, moved
    pub fn set_key_not_in_region(&mut self, v: KeyNotInRegion) {
        self.key_not_in_region = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_key_not_in_region(&mut self) -> &mut KeyNotInRegion {
        if self.key_not_in_region.is_none() {
            self.key_not_in_region.set_default();
        }
        self.key_not_in_region.as_mut().unwrap()
    }

    // Take field
    pub fn take_key_not_in_region(&mut self) -> KeyNotInRegion {
        self.key_not_in_region.take().unwrap_or_else(|| KeyNotInRegion::new())
    }

    pub fn get_key_not_in_region(&self) -> &KeyNotInRegion {
        self.key_not_in_region.as_ref().unwrap_or_else(|| KeyNotInRegion::default_instance())
    }

    fn get_key_not_in_region_for_reflect(&self) -> &::protobuf::SingularPtrField<KeyNotInRegion> {
        &self.key_not_in_region
    }

    fn mut_key_not_in_region_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<KeyNotInRegion> {
        &mut self.key_not_in_region
    }

    // .errorpb.StaleEpoch stale_epoch = 5;

    pub fn clear_stale_epoch(&mut self) {
        self.stale_epoch.clear();
    }

    pub fn has_stale_epoch(&self) -> bool {
        self.stale_epoch.is_some()
    }

    // Param is passed by value, moved
    pub fn set_stale_epoch(&mut self, v: StaleEpoch) {
        self.stale_epoch = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_stale_epoch(&mut self) -> &mut StaleEpoch {
        if self.stale_epoch.is_none() {
            self.stale_epoch.set_default();
        }
        self.stale_epoch.as_mut().unwrap()
    }

    // Take field
    pub fn take_stale_epoch(&mut self) -> StaleEpoch {
        self.stale_epoch.take().unwrap_or_else(|| StaleEpoch::new())
    }

    pub fn get_stale_epoch(&self) -> &StaleEpoch {
        self.stale_epoch.as_ref().unwrap_or_else(|| StaleEpoch::default_instance())
    }

    fn get_stale_epoch_for_reflect(&self) -> &::protobuf::SingularPtrField<StaleEpoch> {
        &self.stale_epoch
    }

    fn mut_stale_epoch_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<StaleEpoch> {
        &mut self.stale_epoch
    }

    // .errorpb.ServerIsBusy server_is_busy = 6;

    pub fn clear_server_is_busy(&mut self) {
        self.server_is_busy.clear();
    }

    pub fn has_server_is_busy(&self) -> bool {
        self.server_is_busy.is_some()
    }

    // Param is passed by value, moved
    pub fn set_server_is_busy(&mut self, v: ServerIsBusy) {
        self.server_is_busy = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_server_is_busy(&mut self) -> &mut ServerIsBusy {
        if self.server_is_busy.is_none() {
            self.server_is_busy.set_default();
        }
        self.server_is_busy.as_mut().unwrap()
    }

    // Take field
    pub fn take_server_is_busy(&mut self) -> ServerIsBusy {
        self.server_is_busy.take().unwrap_or_else(|| ServerIsBusy::new())
    }

    pub fn get_server_is_busy(&self) -> &ServerIsBusy {
        self.server_is_busy.as_ref().unwrap_or_else(|| ServerIsBusy::default_instance())
    }

    fn get_server_is_busy_for_reflect(&self) -> &::protobuf::SingularPtrField<ServerIsBusy> {
        &self.server_is_busy
    }

    fn mut_server_is_busy_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<ServerIsBusy> {
        &mut self.server_is_busy
    }

    // .errorpb.StaleCommand stale_command = 7;

    pub fn clear_stale_command(&mut self) {
        self.stale_command.clear();
    }

    pub fn has_stale_command(&self) -> bool {
        self.stale_command.is_some()
    }

    // Param is passed by value, moved
    pub fn set_stale_command(&mut self, v: StaleCommand) {
        self.stale_command = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_stale_command(&mut self) -> &mut StaleCommand {
        if self.stale_command.is_none() {
            self.stale_command.set_default();
        }
        self.stale_command.as_mut().unwrap()
    }

    // Take field
    pub fn take_stale_command(&mut self) -> StaleCommand {
        self.stale_command.take().unwrap_or_else(|| StaleCommand::new())
    }

    pub fn get_stale_command(&self) -> &StaleCommand {
        self.stale_command.as_ref().unwrap_or_else(|| StaleCommand::default_instance())
    }

    fn get_stale_command_for_reflect(&self) -> &::protobuf::SingularPtrField<StaleCommand> {
        &self.stale_command
    }

    fn mut_stale_command_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<StaleCommand> {
        &mut self.stale_command
    }

    // .errorpb.StoreNotMatch store_not_match = 8;

    pub fn clear_store_not_match(&mut self) {
        self.store_not_match.clear();
    }

    pub fn has_store_not_match(&self) -> bool {
        self.store_not_match.is_some()
    }

    // Param is passed by value, moved
    pub fn set_store_not_match(&mut self, v: StoreNotMatch) {
        self.store_not_match = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_store_not_match(&mut self) -> &mut StoreNotMatch {
        if self.store_not_match.is_none() {
            self.store_not_match.set_default();
        }
        self.store_not_match.as_mut().unwrap()
    }

    // Take field
    pub fn take_store_not_match(&mut self) -> StoreNotMatch {
        self.store_not_match.take().unwrap_or_else(|| StoreNotMatch::new())
    }

    pub fn get_store_not_match(&self) -> &StoreNotMatch {
        self.store_not_match.as_ref().unwrap_or_else(|| StoreNotMatch::default_instance())
    }

    fn get_store_not_match_for_reflect(&self) -> &::protobuf::SingularPtrField<StoreNotMatch> {
        &self.store_not_match
    }

    fn mut_store_not_match_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<StoreNotMatch> {
        &mut self.store_not_match
    }

    // .errorpb.RaftEntryTooLarge raft_entry_too_large = 9;

    pub fn clear_raft_entry_too_large(&mut self) {
        self.raft_entry_too_large.clear();
    }

    pub fn has_raft_entry_too_large(&self) -> bool {
        self.raft_entry_too_large.is_some()
    }

    // Param is passed by value, moved
    pub fn set_raft_entry_too_large(&mut self, v: RaftEntryTooLarge) {
        self.raft_entry_too_large = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_raft_entry_too_large(&mut self) -> &mut RaftEntryTooLarge {
        if self.raft_entry_too_large.is_none() {
            self.raft_entry_too_large.set_default();
        }
        self.raft_entry_too_large.as_mut().unwrap()
    }

    // Take field
    pub fn take_raft_entry_too_large(&mut self) -> RaftEntryTooLarge {
        self.raft_entry_too_large.take().unwrap_or_else(|| RaftEntryTooLarge::new())
    }

    pub fn get_raft_entry_too_large(&self) -> &RaftEntryTooLarge {
        self.raft_entry_too_large.as_ref().unwrap_or_else(|| RaftEntryTooLarge::default_instance())
    }

    fn get_raft_entry_too_large_for_reflect(&self) -> &::protobuf::SingularPtrField<RaftEntryTooLarge> {
        &self.raft_entry_too_large
    }

    fn mut_raft_entry_too_large_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<RaftEntryTooLarge> {
        &mut self.raft_entry_too_large
    }
}

impl ::protobuf::Message for Error {
    fn is_initialized(&self) -> bool {
        for v in &self.not_leader {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.region_not_found {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.key_not_in_region {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.stale_epoch {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.server_is_busy {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.stale_command {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.store_not_match {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.raft_entry_too_large {
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
                    ::protobuf::rt::read_singular_proto3_string_into(wire_type, is, &mut self.message)?;
                },
                2 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.not_leader)?;
                },
                3 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.region_not_found)?;
                },
                4 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.key_not_in_region)?;
                },
                5 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.stale_epoch)?;
                },
                6 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.server_is_busy)?;
                },
                7 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.stale_command)?;
                },
                8 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.store_not_match)?;
                },
                9 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.raft_entry_too_large)?;
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
        if !self.message.is_empty() {
            my_size += ::protobuf::rt::string_size(1, &self.message);
        }
        if let Some(ref v) = self.not_leader.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if let Some(ref v) = self.region_not_found.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if let Some(ref v) = self.key_not_in_region.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if let Some(ref v) = self.stale_epoch.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if let Some(ref v) = self.server_is_busy.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if let Some(ref v) = self.stale_command.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if let Some(ref v) = self.store_not_match.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if let Some(ref v) = self.raft_entry_too_large.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if !self.message.is_empty() {
            os.write_string(1, &self.message)?;
        }
        if let Some(ref v) = self.not_leader.as_ref() {
            os.write_tag(2, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if let Some(ref v) = self.region_not_found.as_ref() {
            os.write_tag(3, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if let Some(ref v) = self.key_not_in_region.as_ref() {
            os.write_tag(4, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if let Some(ref v) = self.stale_epoch.as_ref() {
            os.write_tag(5, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if let Some(ref v) = self.server_is_busy.as_ref() {
            os.write_tag(6, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if let Some(ref v) = self.stale_command.as_ref() {
            os.write_tag(7, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if let Some(ref v) = self.store_not_match.as_ref() {
            os.write_tag(8, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if let Some(ref v) = self.raft_entry_too_large.as_ref() {
            os.write_tag(9, ::protobuf::wire_format::WireTypeLengthDelimited)?;
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
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeString>(
                    "message",
                    Error::get_message_for_reflect,
                    Error::mut_message_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<NotLeader>>(
                    "not_leader",
                    Error::get_not_leader_for_reflect,
                    Error::mut_not_leader_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<RegionNotFound>>(
                    "region_not_found",
                    Error::get_region_not_found_for_reflect,
                    Error::mut_region_not_found_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<KeyNotInRegion>>(
                    "key_not_in_region",
                    Error::get_key_not_in_region_for_reflect,
                    Error::mut_key_not_in_region_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<StaleEpoch>>(
                    "stale_epoch",
                    Error::get_stale_epoch_for_reflect,
                    Error::mut_stale_epoch_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<ServerIsBusy>>(
                    "server_is_busy",
                    Error::get_server_is_busy_for_reflect,
                    Error::mut_server_is_busy_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<StaleCommand>>(
                    "stale_command",
                    Error::get_stale_command_for_reflect,
                    Error::mut_stale_command_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<StoreNotMatch>>(
                    "store_not_match",
                    Error::get_store_not_match_for_reflect,
                    Error::mut_store_not_match_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<RaftEntryTooLarge>>(
                    "raft_entry_too_large",
                    Error::get_raft_entry_too_large_for_reflect,
                    Error::mut_raft_entry_too_large_for_reflect,
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
        self.clear_message();
        self.clear_not_leader();
        self.clear_region_not_found();
        self.clear_key_not_in_region();
        self.clear_stale_epoch();
        self.clear_server_is_busy();
        self.clear_stale_command();
        self.clear_store_not_match();
        self.clear_raft_entry_too_large();
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

static file_descriptor_proto_data: &'static [u8] = b"\
    \n\rerrorpb.proto\x12\x07errorpb\x1a\x0cmetapb.proto\x1a\x14gogoproto/go\
    go.proto\"N\n\tNotLeader\x12\x1b\n\tregion_id\x18\x01\x20\x01(\x04R\x08r\
    egionId\x12$\n\x06leader\x18\x02\x20\x01(\x0b2\x0c.metapb.PeerR\x06leade\
    r\"\x0f\n\rStoreNotMatch\"-\n\x0eRegionNotFound\x12\x1b\n\tregion_id\x18\
    \x01\x20\x01(\x04R\x08regionId\"u\n\x0eKeyNotInRegion\x12\x10\n\x03key\
    \x18\x01\x20\x01(\x0cR\x03key\x12\x1b\n\tregion_id\x18\x02\x20\x01(\x04R\
    \x08regionId\x12\x1b\n\tstart_key\x18\x03\x20\x01(\x0cR\x08startKey\x12\
    \x17\n\x07end_key\x18\x04\x20\x01(\x0cR\x06endKey\"=\n\nStaleEpoch\x12/\
    \n\x0bnew_regions\x18\x01\x20\x03(\x0b2\x0e.metapb.RegionR\nnewRegions\"\
    E\n\x0cServerIsBusy\x12\x16\n\x06reason\x18\x01\x20\x01(\tR\x06reason\
    \x12\x1d\n\nbackoff_ms\x18\x02\x20\x01(\x04R\tbackoffMs\"\x0e\n\x0cStale\
    Command\"O\n\x11RaftEntryTooLarge\x12\x1b\n\tregion_id\x18\x01\x20\x01(\
    \x04R\x08regionId\x12\x1d\n\nentry_size\x18\x02\x20\x01(\x04R\tentrySize\
    \"\x97\x04\n\x05Error\x12\x18\n\x07message\x18\x01\x20\x01(\tR\x07messag\
    e\x121\n\nnot_leader\x18\x02\x20\x01(\x0b2\x12.errorpb.NotLeaderR\tnotLe\
    ader\x12A\n\x10region_not_found\x18\x03\x20\x01(\x0b2\x17.errorpb.Region\
    NotFoundR\x0eregionNotFound\x12B\n\x11key_not_in_region\x18\x04\x20\x01(\
    \x0b2\x17.errorpb.KeyNotInRegionR\x0ekeyNotInRegion\x124\n\x0bstale_epoc\
    h\x18\x05\x20\x01(\x0b2\x13.errorpb.StaleEpochR\nstaleEpoch\x12;\n\x0ese\
    rver_is_busy\x18\x06\x20\x01(\x0b2\x15.errorpb.ServerIsBusyR\x0cserverIs\
    Busy\x12:\n\rstale_command\x18\x07\x20\x01(\x0b2\x15.errorpb.StaleComman\
    dR\x0cstaleCommand\x12>\n\x0fstore_not_match\x18\x08\x20\x01(\x0b2\x16.e\
    rrorpb.StoreNotMatchR\rstoreNotMatch\x12K\n\x14raft_entry_too_large\x18\
    \t\x20\x01(\x0b2\x1a.errorpb.RaftEntryTooLargeR\x11raftEntryTooLargeB&\n\
    \x18com.pingcap.tikv.kvproto\xd0\xe2\x1e\x01\xe0\xe2\x1e\x01\xc8\xe2\x1e\
    \x01J\x8f\x10\n\x06\x12\x04\0\0:\x01\n\x08\n\x01\x0c\x12\x03\0\0\x12\n\
    \x08\n\x01\x02\x12\x03\x01\x08\x0f\n\t\n\x02\x03\0\x12\x03\x03\x07\x15\n\
    \t\n\x02\x03\x01\x12\x03\x04\x07\x1d\n\x08\n\x01\x08\x12\x03\x06\0(\n\
    \x0b\n\x04\x08\xe7\x07\0\x12\x03\x06\0(\n\x0c\n\x05\x08\xe7\x07\0\x02\
    \x12\x03\x06\x07\x20\n\r\n\x06\x08\xe7\x07\0\x02\0\x12\x03\x06\x07\x20\n\
    \x0e\n\x07\x08\xe7\x07\0\x02\0\x01\x12\x03\x06\x08\x1f\n\x0c\n\x05\x08\
    \xe7\x07\0\x03\x12\x03\x06#'\n\x08\n\x01\x08\x12\x03\x07\0$\n\x0b\n\x04\
    \x08\xe7\x07\x01\x12\x03\x07\0$\n\x0c\n\x05\x08\xe7\x07\x01\x02\x12\x03\
    \x07\x07\x1c\n\r\n\x06\x08\xe7\x07\x01\x02\0\x12\x03\x07\x07\x1c\n\x0e\n\
    \x07\x08\xe7\x07\x01\x02\0\x01\x12\x03\x07\x08\x1b\n\x0c\n\x05\x08\xe7\
    \x07\x01\x03\x12\x03\x07\x1f#\n\x08\n\x01\x08\x12\x03\x08\0*\n\x0b\n\x04\
    \x08\xe7\x07\x02\x12\x03\x08\0*\n\x0c\n\x05\x08\xe7\x07\x02\x02\x12\x03\
    \x08\x07\"\n\r\n\x06\x08\xe7\x07\x02\x02\0\x12\x03\x08\x07\"\n\x0e\n\x07\
    \x08\xe7\x07\x02\x02\0\x01\x12\x03\x08\x08!\n\x0c\n\x05\x08\xe7\x07\x02\
    \x03\x12\x03\x08%)\n\x08\n\x01\x08\x12\x03\n\01\n\x0b\n\x04\x08\xe7\x07\
    \x03\x12\x03\n\01\n\x0c\n\x05\x08\xe7\x07\x03\x02\x12\x03\n\x07\x13\n\r\
    \n\x06\x08\xe7\x07\x03\x02\0\x12\x03\n\x07\x13\n\x0e\n\x07\x08\xe7\x07\
    \x03\x02\0\x01\x12\x03\n\x07\x13\n\x0c\n\x05\x08\xe7\x07\x03\x07\x12\x03\
    \n\x160\n\n\n\x02\x04\0\x12\x04\x0c\0\x0f\x01\n\n\n\x03\x04\0\x01\x12\
    \x03\x0c\x08\x11\n\x0b\n\x04\x04\0\x02\0\x12\x03\r\x04\x19\n\r\n\x05\x04\
    \0\x02\0\x04\x12\x04\r\x04\x0c\x13\n\x0c\n\x05\x04\0\x02\0\x05\x12\x03\r\
    \x04\n\n\x0c\n\x05\x04\0\x02\0\x01\x12\x03\r\x0b\x14\n\x0c\n\x05\x04\0\
    \x02\0\x03\x12\x03\r\x17\x18\n\x0b\n\x04\x04\0\x02\x01\x12\x03\x0e\x04\
    \x1b\n\r\n\x05\x04\0\x02\x01\x04\x12\x04\x0e\x04\r\x19\n\x0c\n\x05\x04\0\
    \x02\x01\x06\x12\x03\x0e\x04\x0f\n\x0c\n\x05\x04\0\x02\x01\x01\x12\x03\
    \x0e\x10\x16\n\x0c\n\x05\x04\0\x02\x01\x03\x12\x03\x0e\x19\x1a\n\n\n\x02\
    \x04\x01\x12\x04\x11\0\x12\x01\n\n\n\x03\x04\x01\x01\x12\x03\x11\x08\x15\
    \n\n\n\x02\x04\x02\x12\x04\x14\0\x16\x01\n\n\n\x03\x04\x02\x01\x12\x03\
    \x14\x08\x16\n\x0b\n\x04\x04\x02\x02\0\x12\x03\x15\x04\x19\n\r\n\x05\x04\
    \x02\x02\0\x04\x12\x04\x15\x04\x14\x18\n\x0c\n\x05\x04\x02\x02\0\x05\x12\
    \x03\x15\x04\n\n\x0c\n\x05\x04\x02\x02\0\x01\x12\x03\x15\x0b\x14\n\x0c\n\
    \x05\x04\x02\x02\0\x03\x12\x03\x15\x17\x18\n\n\n\x02\x04\x03\x12\x04\x18\
    \0\x1d\x01\n\n\n\x03\x04\x03\x01\x12\x03\x18\x08\x16\n\x0b\n\x04\x04\x03\
    \x02\0\x12\x03\x19\x04\x12\n\r\n\x05\x04\x03\x02\0\x04\x12\x04\x19\x04\
    \x18\x18\n\x0c\n\x05\x04\x03\x02\0\x05\x12\x03\x19\x04\t\n\x0c\n\x05\x04\
    \x03\x02\0\x01\x12\x03\x19\n\r\n\x0c\n\x05\x04\x03\x02\0\x03\x12\x03\x19\
    \x10\x11\n\x0b\n\x04\x04\x03\x02\x01\x12\x03\x1a\x04\x19\n\r\n\x05\x04\
    \x03\x02\x01\x04\x12\x04\x1a\x04\x19\x12\n\x0c\n\x05\x04\x03\x02\x01\x05\
    \x12\x03\x1a\x04\n\n\x0c\n\x05\x04\x03\x02\x01\x01\x12\x03\x1a\x0b\x14\n\
    \x0c\n\x05\x04\x03\x02\x01\x03\x12\x03\x1a\x17\x18\n\x0b\n\x04\x04\x03\
    \x02\x02\x12\x03\x1b\x04\x18\n\r\n\x05\x04\x03\x02\x02\x04\x12\x04\x1b\
    \x04\x1a\x19\n\x0c\n\x05\x04\x03\x02\x02\x05\x12\x03\x1b\x04\t\n\x0c\n\
    \x05\x04\x03\x02\x02\x01\x12\x03\x1b\n\x13\n\x0c\n\x05\x04\x03\x02\x02\
    \x03\x12\x03\x1b\x16\x17\n\x0b\n\x04\x04\x03\x02\x03\x12\x03\x1c\x04\x16\
    \n\r\n\x05\x04\x03\x02\x03\x04\x12\x04\x1c\x04\x1b\x18\n\x0c\n\x05\x04\
    \x03\x02\x03\x05\x12\x03\x1c\x04\t\n\x0c\n\x05\x04\x03\x02\x03\x01\x12\
    \x03\x1c\n\x11\n\x0c\n\x05\x04\x03\x02\x03\x03\x12\x03\x1c\x14\x15\n\n\n\
    \x02\x04\x04\x12\x04\x1f\0!\x01\n\n\n\x03\x04\x04\x01\x12\x03\x1f\x08\
    \x12\n\x0b\n\x04\x04\x04\x02\0\x12\x03\x20\x04+\n\x0c\n\x05\x04\x04\x02\
    \0\x04\x12\x03\x20\x04\x0c\n\x0c\n\x05\x04\x04\x02\0\x06\x12\x03\x20\r\
    \x1a\n\x0c\n\x05\x04\x04\x02\0\x01\x12\x03\x20\x1b&\n\x0c\n\x05\x04\x04\
    \x02\0\x03\x12\x03\x20)*\n\n\n\x02\x04\x05\x12\x04#\0&\x01\n\n\n\x03\x04\
    \x05\x01\x12\x03#\x08\x14\n\x0b\n\x04\x04\x05\x02\0\x12\x03$\x04\x16\n\r\
    \n\x05\x04\x05\x02\0\x04\x12\x04$\x04#\x16\n\x0c\n\x05\x04\x05\x02\0\x05\
    \x12\x03$\x04\n\n\x0c\n\x05\x04\x05\x02\0\x01\x12\x03$\x0b\x11\n\x0c\n\
    \x05\x04\x05\x02\0\x03\x12\x03$\x14\x15\n\x0b\n\x04\x04\x05\x02\x01\x12\
    \x03%\x04\x1a\n\r\n\x05\x04\x05\x02\x01\x04\x12\x04%\x04$\x16\n\x0c\n\
    \x05\x04\x05\x02\x01\x05\x12\x03%\x04\n\n\x0c\n\x05\x04\x05\x02\x01\x01\
    \x12\x03%\x0b\x15\n\x0c\n\x05\x04\x05\x02\x01\x03\x12\x03%\x18\x19\n\n\n\
    \x02\x04\x06\x12\x04(\0)\x01\n\n\n\x03\x04\x06\x01\x12\x03(\x08\x14\n\n\
    \n\x02\x04\x07\x12\x04+\0.\x01\n\n\n\x03\x04\x07\x01\x12\x03+\x08\x19\n\
    \x0b\n\x04\x04\x07\x02\0\x12\x03,\x04\x19\n\r\n\x05\x04\x07\x02\0\x04\
    \x12\x04,\x04+\x1b\n\x0c\n\x05\x04\x07\x02\0\x05\x12\x03,\x04\n\n\x0c\n\
    \x05\x04\x07\x02\0\x01\x12\x03,\x0b\x14\n\x0c\n\x05\x04\x07\x02\0\x03\
    \x12\x03,\x17\x18\n\x0b\n\x04\x04\x07\x02\x01\x12\x03-\x04\x1a\n\r\n\x05\
    \x04\x07\x02\x01\x04\x12\x04-\x04,\x19\n\x0c\n\x05\x04\x07\x02\x01\x05\
    \x12\x03-\x04\n\n\x0c\n\x05\x04\x07\x02\x01\x01\x12\x03-\x0b\x15\n\x0c\n\
    \x05\x04\x07\x02\x01\x03\x12\x03-\x18\x19\n\n\n\x02\x04\x08\x12\x040\0:\
    \x01\n\n\n\x03\x04\x08\x01\x12\x030\x08\r\n\x0b\n\x04\x04\x08\x02\0\x12\
    \x031\x04\x17\n\r\n\x05\x04\x08\x02\0\x04\x12\x041\x040\x0f\n\x0c\n\x05\
    \x04\x08\x02\0\x05\x12\x031\x04\n\n\x0c\n\x05\x04\x08\x02\0\x01\x12\x031\
    \x0b\x12\n\x0c\n\x05\x04\x08\x02\0\x03\x12\x031\x15\x16\n\x0b\n\x04\x04\
    \x08\x02\x01\x12\x032\x04\x1d\n\r\n\x05\x04\x08\x02\x01\x04\x12\x042\x04\
    1\x17\n\x0c\n\x05\x04\x08\x02\x01\x06\x12\x032\x04\r\n\x0c\n\x05\x04\x08\
    \x02\x01\x01\x12\x032\x0e\x18\n\x0c\n\x05\x04\x08\x02\x01\x03\x12\x032\
    \x1b\x1c\n\x0b\n\x04\x04\x08\x02\x02\x12\x033\x04(\n\r\n\x05\x04\x08\x02\
    \x02\x04\x12\x043\x042\x1d\n\x0c\n\x05\x04\x08\x02\x02\x06\x12\x033\x04\
    \x12\n\x0c\n\x05\x04\x08\x02\x02\x01\x12\x033\x13#\n\x0c\n\x05\x04\x08\
    \x02\x02\x03\x12\x033&'\n\x0b\n\x04\x04\x08\x02\x03\x12\x034\x04)\n\r\n\
    \x05\x04\x08\x02\x03\x04\x12\x044\x043(\n\x0c\n\x05\x04\x08\x02\x03\x06\
    \x12\x034\x04\x12\n\x0c\n\x05\x04\x08\x02\x03\x01\x12\x034\x13$\n\x0c\n\
    \x05\x04\x08\x02\x03\x03\x12\x034'(\n\x0b\n\x04\x04\x08\x02\x04\x12\x035\
    \x04\x1f\n\r\n\x05\x04\x08\x02\x04\x04\x12\x045\x044)\n\x0c\n\x05\x04\
    \x08\x02\x04\x06\x12\x035\x04\x0e\n\x0c\n\x05\x04\x08\x02\x04\x01\x12\
    \x035\x0f\x1a\n\x0c\n\x05\x04\x08\x02\x04\x03\x12\x035\x1d\x1e\n\x0b\n\
    \x04\x04\x08\x02\x05\x12\x036\x04$\n\r\n\x05\x04\x08\x02\x05\x04\x12\x04\
    6\x045\x1f\n\x0c\n\x05\x04\x08\x02\x05\x06\x12\x036\x04\x10\n\x0c\n\x05\
    \x04\x08\x02\x05\x01\x12\x036\x11\x1f\n\x0c\n\x05\x04\x08\x02\x05\x03\
    \x12\x036\"#\n\x0b\n\x04\x04\x08\x02\x06\x12\x037\x04#\n\r\n\x05\x04\x08\
    \x02\x06\x04\x12\x047\x046$\n\x0c\n\x05\x04\x08\x02\x06\x06\x12\x037\x04\
    \x10\n\x0c\n\x05\x04\x08\x02\x06\x01\x12\x037\x11\x1e\n\x0c\n\x05\x04\
    \x08\x02\x06\x03\x12\x037!\"\n\x0b\n\x04\x04\x08\x02\x07\x12\x038\x04&\n\
    \r\n\x05\x04\x08\x02\x07\x04\x12\x048\x047#\n\x0c\n\x05\x04\x08\x02\x07\
    \x06\x12\x038\x04\x11\n\x0c\n\x05\x04\x08\x02\x07\x01\x12\x038\x12!\n\
    \x0c\n\x05\x04\x08\x02\x07\x03\x12\x038$%\n\x0b\n\x04\x04\x08\x02\x08\
    \x12\x039\x04/\n\r\n\x05\x04\x08\x02\x08\x04\x12\x049\x048&\n\x0c\n\x05\
    \x04\x08\x02\x08\x06\x12\x039\x04\x15\n\x0c\n\x05\x04\x08\x02\x08\x01\
    \x12\x039\x16*\n\x0c\n\x05\x04\x08\x02\x08\x03\x12\x039-.b\x06proto3\
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
