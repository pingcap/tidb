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
pub struct Entry {
    // message fields
    pub entry_type: EntryType,
    pub term: u64,
    pub index: u64,
    pub data: ::std::vec::Vec<u8>,
    pub sync_log: bool,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for Entry {}

impl Entry {
    pub fn new() -> Entry {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static Entry {
        static mut instance: ::protobuf::lazy::Lazy<Entry> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const Entry,
        };
        unsafe {
            instance.get(Entry::new)
        }
    }

    // .eraftpb.EntryType entry_type = 1;

    pub fn clear_entry_type(&mut self) {
        self.entry_type = EntryType::EntryNormal;
    }

    // Param is passed by value, moved
    pub fn set_entry_type(&mut self, v: EntryType) {
        self.entry_type = v;
    }

    pub fn get_entry_type(&self) -> EntryType {
        self.entry_type
    }

    fn get_entry_type_for_reflect(&self) -> &EntryType {
        &self.entry_type
    }

    fn mut_entry_type_for_reflect(&mut self) -> &mut EntryType {
        &mut self.entry_type
    }

    // uint64 term = 2;

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

    // uint64 index = 3;

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

    // bytes data = 4;

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

    // bool sync_log = 5;

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

impl ::protobuf::Message for Entry {
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
                    self.entry_type = tmp;
                },
                2 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_uint64()?;
                    self.term = tmp;
                },
                3 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_uint64()?;
                    self.index = tmp;
                },
                4 => {
                    ::protobuf::rt::read_singular_proto3_bytes_into(wire_type, is, &mut self.data)?;
                },
                5 => {
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
        if self.entry_type != EntryType::EntryNormal {
            my_size += ::protobuf::rt::enum_size(1, self.entry_type);
        }
        if self.term != 0 {
            my_size += ::protobuf::rt::value_size(2, self.term, ::protobuf::wire_format::WireTypeVarint);
        }
        if self.index != 0 {
            my_size += ::protobuf::rt::value_size(3, self.index, ::protobuf::wire_format::WireTypeVarint);
        }
        if !self.data.is_empty() {
            my_size += ::protobuf::rt::bytes_size(4, &self.data);
        }
        if self.sync_log != false {
            my_size += 2;
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if self.entry_type != EntryType::EntryNormal {
            os.write_enum(1, self.entry_type.value())?;
        }
        if self.term != 0 {
            os.write_uint64(2, self.term)?;
        }
        if self.index != 0 {
            os.write_uint64(3, self.index)?;
        }
        if !self.data.is_empty() {
            os.write_bytes(4, &self.data)?;
        }
        if self.sync_log != false {
            os.write_bool(5, self.sync_log)?;
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

impl ::protobuf::MessageStatic for Entry {
    fn new() -> Entry {
        Entry::new()
    }

    fn descriptor_static(_: ::std::option::Option<Entry>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeEnum<EntryType>>(
                    "entry_type",
                    Entry::get_entry_type_for_reflect,
                    Entry::mut_entry_type_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "term",
                    Entry::get_term_for_reflect,
                    Entry::mut_term_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "index",
                    Entry::get_index_for_reflect,
                    Entry::mut_index_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeBytes>(
                    "data",
                    Entry::get_data_for_reflect,
                    Entry::mut_data_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeBool>(
                    "sync_log",
                    Entry::get_sync_log_for_reflect,
                    Entry::mut_sync_log_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<Entry>(
                    "Entry",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for Entry {
    fn clear(&mut self) {
        self.clear_entry_type();
        self.clear_term();
        self.clear_index();
        self.clear_data();
        self.clear_sync_log();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for Entry {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for Entry {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct SnapshotMetadata {
    // message fields
    pub conf_state: ::protobuf::SingularPtrField<ConfState>,
    pub index: u64,
    pub term: u64,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for SnapshotMetadata {}

impl SnapshotMetadata {
    pub fn new() -> SnapshotMetadata {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static SnapshotMetadata {
        static mut instance: ::protobuf::lazy::Lazy<SnapshotMetadata> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const SnapshotMetadata,
        };
        unsafe {
            instance.get(SnapshotMetadata::new)
        }
    }

    // .eraftpb.ConfState conf_state = 1;

    pub fn clear_conf_state(&mut self) {
        self.conf_state.clear();
    }

    pub fn has_conf_state(&self) -> bool {
        self.conf_state.is_some()
    }

    // Param is passed by value, moved
    pub fn set_conf_state(&mut self, v: ConfState) {
        self.conf_state = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_conf_state(&mut self) -> &mut ConfState {
        if self.conf_state.is_none() {
            self.conf_state.set_default();
        }
        self.conf_state.as_mut().unwrap()
    }

    // Take field
    pub fn take_conf_state(&mut self) -> ConfState {
        self.conf_state.take().unwrap_or_else(|| ConfState::new())
    }

    pub fn get_conf_state(&self) -> &ConfState {
        self.conf_state.as_ref().unwrap_or_else(|| ConfState::default_instance())
    }

    fn get_conf_state_for_reflect(&self) -> &::protobuf::SingularPtrField<ConfState> {
        &self.conf_state
    }

    fn mut_conf_state_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<ConfState> {
        &mut self.conf_state
    }

    // uint64 index = 2;

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

    // uint64 term = 3;

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
}

impl ::protobuf::Message for SnapshotMetadata {
    fn is_initialized(&self) -> bool {
        for v in &self.conf_state {
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
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.conf_state)?;
                },
                2 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_uint64()?;
                    self.index = tmp;
                },
                3 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_uint64()?;
                    self.term = tmp;
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
        if let Some(ref v) = self.conf_state.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if self.index != 0 {
            my_size += ::protobuf::rt::value_size(2, self.index, ::protobuf::wire_format::WireTypeVarint);
        }
        if self.term != 0 {
            my_size += ::protobuf::rt::value_size(3, self.term, ::protobuf::wire_format::WireTypeVarint);
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(ref v) = self.conf_state.as_ref() {
            os.write_tag(1, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if self.index != 0 {
            os.write_uint64(2, self.index)?;
        }
        if self.term != 0 {
            os.write_uint64(3, self.term)?;
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

impl ::protobuf::MessageStatic for SnapshotMetadata {
    fn new() -> SnapshotMetadata {
        SnapshotMetadata::new()
    }

    fn descriptor_static(_: ::std::option::Option<SnapshotMetadata>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<ConfState>>(
                    "conf_state",
                    SnapshotMetadata::get_conf_state_for_reflect,
                    SnapshotMetadata::mut_conf_state_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "index",
                    SnapshotMetadata::get_index_for_reflect,
                    SnapshotMetadata::mut_index_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "term",
                    SnapshotMetadata::get_term_for_reflect,
                    SnapshotMetadata::mut_term_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<SnapshotMetadata>(
                    "SnapshotMetadata",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for SnapshotMetadata {
    fn clear(&mut self) {
        self.clear_conf_state();
        self.clear_index();
        self.clear_term();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for SnapshotMetadata {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for SnapshotMetadata {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct Snapshot {
    // message fields
    pub data: ::std::vec::Vec<u8>,
    pub metadata: ::protobuf::SingularPtrField<SnapshotMetadata>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for Snapshot {}

impl Snapshot {
    pub fn new() -> Snapshot {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static Snapshot {
        static mut instance: ::protobuf::lazy::Lazy<Snapshot> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const Snapshot,
        };
        unsafe {
            instance.get(Snapshot::new)
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

    // .eraftpb.SnapshotMetadata metadata = 2;

    pub fn clear_metadata(&mut self) {
        self.metadata.clear();
    }

    pub fn has_metadata(&self) -> bool {
        self.metadata.is_some()
    }

    // Param is passed by value, moved
    pub fn set_metadata(&mut self, v: SnapshotMetadata) {
        self.metadata = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_metadata(&mut self) -> &mut SnapshotMetadata {
        if self.metadata.is_none() {
            self.metadata.set_default();
        }
        self.metadata.as_mut().unwrap()
    }

    // Take field
    pub fn take_metadata(&mut self) -> SnapshotMetadata {
        self.metadata.take().unwrap_or_else(|| SnapshotMetadata::new())
    }

    pub fn get_metadata(&self) -> &SnapshotMetadata {
        self.metadata.as_ref().unwrap_or_else(|| SnapshotMetadata::default_instance())
    }

    fn get_metadata_for_reflect(&self) -> &::protobuf::SingularPtrField<SnapshotMetadata> {
        &self.metadata
    }

    fn mut_metadata_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<SnapshotMetadata> {
        &mut self.metadata
    }
}

impl ::protobuf::Message for Snapshot {
    fn is_initialized(&self) -> bool {
        for v in &self.metadata {
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
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.metadata)?;
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
        if let Some(ref v) = self.metadata.as_ref() {
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
        if let Some(ref v) = self.metadata.as_ref() {
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

impl ::protobuf::MessageStatic for Snapshot {
    fn new() -> Snapshot {
        Snapshot::new()
    }

    fn descriptor_static(_: ::std::option::Option<Snapshot>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeBytes>(
                    "data",
                    Snapshot::get_data_for_reflect,
                    Snapshot::mut_data_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<SnapshotMetadata>>(
                    "metadata",
                    Snapshot::get_metadata_for_reflect,
                    Snapshot::mut_metadata_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<Snapshot>(
                    "Snapshot",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for Snapshot {
    fn clear(&mut self) {
        self.clear_data();
        self.clear_metadata();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for Snapshot {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for Snapshot {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct Message {
    // message fields
    pub msg_type: MessageType,
    pub to: u64,
    pub from: u64,
    pub term: u64,
    pub log_term: u64,
    pub index: u64,
    pub entries: ::protobuf::RepeatedField<Entry>,
    pub commit: u64,
    pub snapshot: ::protobuf::SingularPtrField<Snapshot>,
    pub reject: bool,
    pub reject_hint: u64,
    pub context: ::std::vec::Vec<u8>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for Message {}

impl Message {
    pub fn new() -> Message {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static Message {
        static mut instance: ::protobuf::lazy::Lazy<Message> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const Message,
        };
        unsafe {
            instance.get(Message::new)
        }
    }

    // .eraftpb.MessageType msg_type = 1;

    pub fn clear_msg_type(&mut self) {
        self.msg_type = MessageType::MsgHup;
    }

    // Param is passed by value, moved
    pub fn set_msg_type(&mut self, v: MessageType) {
        self.msg_type = v;
    }

    pub fn get_msg_type(&self) -> MessageType {
        self.msg_type
    }

    fn get_msg_type_for_reflect(&self) -> &MessageType {
        &self.msg_type
    }

    fn mut_msg_type_for_reflect(&mut self) -> &mut MessageType {
        &mut self.msg_type
    }

    // uint64 to = 2;

    pub fn clear_to(&mut self) {
        self.to = 0;
    }

    // Param is passed by value, moved
    pub fn set_to(&mut self, v: u64) {
        self.to = v;
    }

    pub fn get_to(&self) -> u64 {
        self.to
    }

    fn get_to_for_reflect(&self) -> &u64 {
        &self.to
    }

    fn mut_to_for_reflect(&mut self) -> &mut u64 {
        &mut self.to
    }

    // uint64 from = 3;

    pub fn clear_from(&mut self) {
        self.from = 0;
    }

    // Param is passed by value, moved
    pub fn set_from(&mut self, v: u64) {
        self.from = v;
    }

    pub fn get_from(&self) -> u64 {
        self.from
    }

    fn get_from_for_reflect(&self) -> &u64 {
        &self.from
    }

    fn mut_from_for_reflect(&mut self) -> &mut u64 {
        &mut self.from
    }

    // uint64 term = 4;

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

    // uint64 log_term = 5;

    pub fn clear_log_term(&mut self) {
        self.log_term = 0;
    }

    // Param is passed by value, moved
    pub fn set_log_term(&mut self, v: u64) {
        self.log_term = v;
    }

    pub fn get_log_term(&self) -> u64 {
        self.log_term
    }

    fn get_log_term_for_reflect(&self) -> &u64 {
        &self.log_term
    }

    fn mut_log_term_for_reflect(&mut self) -> &mut u64 {
        &mut self.log_term
    }

    // uint64 index = 6;

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

    // repeated .eraftpb.Entry entries = 7;

    pub fn clear_entries(&mut self) {
        self.entries.clear();
    }

    // Param is passed by value, moved
    pub fn set_entries(&mut self, v: ::protobuf::RepeatedField<Entry>) {
        self.entries = v;
    }

    // Mutable pointer to the field.
    pub fn mut_entries(&mut self) -> &mut ::protobuf::RepeatedField<Entry> {
        &mut self.entries
    }

    // Take field
    pub fn take_entries(&mut self) -> ::protobuf::RepeatedField<Entry> {
        ::std::mem::replace(&mut self.entries, ::protobuf::RepeatedField::new())
    }

    pub fn get_entries(&self) -> &[Entry] {
        &self.entries
    }

    fn get_entries_for_reflect(&self) -> &::protobuf::RepeatedField<Entry> {
        &self.entries
    }

    fn mut_entries_for_reflect(&mut self) -> &mut ::protobuf::RepeatedField<Entry> {
        &mut self.entries
    }

    // uint64 commit = 8;

    pub fn clear_commit(&mut self) {
        self.commit = 0;
    }

    // Param is passed by value, moved
    pub fn set_commit(&mut self, v: u64) {
        self.commit = v;
    }

    pub fn get_commit(&self) -> u64 {
        self.commit
    }

    fn get_commit_for_reflect(&self) -> &u64 {
        &self.commit
    }

    fn mut_commit_for_reflect(&mut self) -> &mut u64 {
        &mut self.commit
    }

    // .eraftpb.Snapshot snapshot = 9;

    pub fn clear_snapshot(&mut self) {
        self.snapshot.clear();
    }

    pub fn has_snapshot(&self) -> bool {
        self.snapshot.is_some()
    }

    // Param is passed by value, moved
    pub fn set_snapshot(&mut self, v: Snapshot) {
        self.snapshot = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_snapshot(&mut self) -> &mut Snapshot {
        if self.snapshot.is_none() {
            self.snapshot.set_default();
        }
        self.snapshot.as_mut().unwrap()
    }

    // Take field
    pub fn take_snapshot(&mut self) -> Snapshot {
        self.snapshot.take().unwrap_or_else(|| Snapshot::new())
    }

    pub fn get_snapshot(&self) -> &Snapshot {
        self.snapshot.as_ref().unwrap_or_else(|| Snapshot::default_instance())
    }

    fn get_snapshot_for_reflect(&self) -> &::protobuf::SingularPtrField<Snapshot> {
        &self.snapshot
    }

    fn mut_snapshot_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<Snapshot> {
        &mut self.snapshot
    }

    // bool reject = 10;

    pub fn clear_reject(&mut self) {
        self.reject = false;
    }

    // Param is passed by value, moved
    pub fn set_reject(&mut self, v: bool) {
        self.reject = v;
    }

    pub fn get_reject(&self) -> bool {
        self.reject
    }

    fn get_reject_for_reflect(&self) -> &bool {
        &self.reject
    }

    fn mut_reject_for_reflect(&mut self) -> &mut bool {
        &mut self.reject
    }

    // uint64 reject_hint = 11;

    pub fn clear_reject_hint(&mut self) {
        self.reject_hint = 0;
    }

    // Param is passed by value, moved
    pub fn set_reject_hint(&mut self, v: u64) {
        self.reject_hint = v;
    }

    pub fn get_reject_hint(&self) -> u64 {
        self.reject_hint
    }

    fn get_reject_hint_for_reflect(&self) -> &u64 {
        &self.reject_hint
    }

    fn mut_reject_hint_for_reflect(&mut self) -> &mut u64 {
        &mut self.reject_hint
    }

    // bytes context = 12;

    pub fn clear_context(&mut self) {
        self.context.clear();
    }

    // Param is passed by value, moved
    pub fn set_context(&mut self, v: ::std::vec::Vec<u8>) {
        self.context = v;
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_context(&mut self) -> &mut ::std::vec::Vec<u8> {
        &mut self.context
    }

    // Take field
    pub fn take_context(&mut self) -> ::std::vec::Vec<u8> {
        ::std::mem::replace(&mut self.context, ::std::vec::Vec::new())
    }

    pub fn get_context(&self) -> &[u8] {
        &self.context
    }

    fn get_context_for_reflect(&self) -> &::std::vec::Vec<u8> {
        &self.context
    }

    fn mut_context_for_reflect(&mut self) -> &mut ::std::vec::Vec<u8> {
        &mut self.context
    }
}

impl ::protobuf::Message for Message {
    fn is_initialized(&self) -> bool {
        for v in &self.entries {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.snapshot {
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
                    self.msg_type = tmp;
                },
                2 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_uint64()?;
                    self.to = tmp;
                },
                3 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_uint64()?;
                    self.from = tmp;
                },
                4 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_uint64()?;
                    self.term = tmp;
                },
                5 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_uint64()?;
                    self.log_term = tmp;
                },
                6 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_uint64()?;
                    self.index = tmp;
                },
                7 => {
                    ::protobuf::rt::read_repeated_message_into(wire_type, is, &mut self.entries)?;
                },
                8 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_uint64()?;
                    self.commit = tmp;
                },
                9 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.snapshot)?;
                },
                10 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_bool()?;
                    self.reject = tmp;
                },
                11 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_uint64()?;
                    self.reject_hint = tmp;
                },
                12 => {
                    ::protobuf::rt::read_singular_proto3_bytes_into(wire_type, is, &mut self.context)?;
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
        if self.msg_type != MessageType::MsgHup {
            my_size += ::protobuf::rt::enum_size(1, self.msg_type);
        }
        if self.to != 0 {
            my_size += ::protobuf::rt::value_size(2, self.to, ::protobuf::wire_format::WireTypeVarint);
        }
        if self.from != 0 {
            my_size += ::protobuf::rt::value_size(3, self.from, ::protobuf::wire_format::WireTypeVarint);
        }
        if self.term != 0 {
            my_size += ::protobuf::rt::value_size(4, self.term, ::protobuf::wire_format::WireTypeVarint);
        }
        if self.log_term != 0 {
            my_size += ::protobuf::rt::value_size(5, self.log_term, ::protobuf::wire_format::WireTypeVarint);
        }
        if self.index != 0 {
            my_size += ::protobuf::rt::value_size(6, self.index, ::protobuf::wire_format::WireTypeVarint);
        }
        for value in &self.entries {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        if self.commit != 0 {
            my_size += ::protobuf::rt::value_size(8, self.commit, ::protobuf::wire_format::WireTypeVarint);
        }
        if let Some(ref v) = self.snapshot.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if self.reject != false {
            my_size += 2;
        }
        if self.reject_hint != 0 {
            my_size += ::protobuf::rt::value_size(11, self.reject_hint, ::protobuf::wire_format::WireTypeVarint);
        }
        if !self.context.is_empty() {
            my_size += ::protobuf::rt::bytes_size(12, &self.context);
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if self.msg_type != MessageType::MsgHup {
            os.write_enum(1, self.msg_type.value())?;
        }
        if self.to != 0 {
            os.write_uint64(2, self.to)?;
        }
        if self.from != 0 {
            os.write_uint64(3, self.from)?;
        }
        if self.term != 0 {
            os.write_uint64(4, self.term)?;
        }
        if self.log_term != 0 {
            os.write_uint64(5, self.log_term)?;
        }
        if self.index != 0 {
            os.write_uint64(6, self.index)?;
        }
        for v in &self.entries {
            os.write_tag(7, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        };
        if self.commit != 0 {
            os.write_uint64(8, self.commit)?;
        }
        if let Some(ref v) = self.snapshot.as_ref() {
            os.write_tag(9, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if self.reject != false {
            os.write_bool(10, self.reject)?;
        }
        if self.reject_hint != 0 {
            os.write_uint64(11, self.reject_hint)?;
        }
        if !self.context.is_empty() {
            os.write_bytes(12, &self.context)?;
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

impl ::protobuf::MessageStatic for Message {
    fn new() -> Message {
        Message::new()
    }

    fn descriptor_static(_: ::std::option::Option<Message>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeEnum<MessageType>>(
                    "msg_type",
                    Message::get_msg_type_for_reflect,
                    Message::mut_msg_type_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "to",
                    Message::get_to_for_reflect,
                    Message::mut_to_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "from",
                    Message::get_from_for_reflect,
                    Message::mut_from_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "term",
                    Message::get_term_for_reflect,
                    Message::mut_term_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "log_term",
                    Message::get_log_term_for_reflect,
                    Message::mut_log_term_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "index",
                    Message::get_index_for_reflect,
                    Message::mut_index_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_repeated_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<Entry>>(
                    "entries",
                    Message::get_entries_for_reflect,
                    Message::mut_entries_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "commit",
                    Message::get_commit_for_reflect,
                    Message::mut_commit_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<Snapshot>>(
                    "snapshot",
                    Message::get_snapshot_for_reflect,
                    Message::mut_snapshot_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeBool>(
                    "reject",
                    Message::get_reject_for_reflect,
                    Message::mut_reject_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "reject_hint",
                    Message::get_reject_hint_for_reflect,
                    Message::mut_reject_hint_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeBytes>(
                    "context",
                    Message::get_context_for_reflect,
                    Message::mut_context_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<Message>(
                    "Message",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for Message {
    fn clear(&mut self) {
        self.clear_msg_type();
        self.clear_to();
        self.clear_from();
        self.clear_term();
        self.clear_log_term();
        self.clear_index();
        self.clear_entries();
        self.clear_commit();
        self.clear_snapshot();
        self.clear_reject();
        self.clear_reject_hint();
        self.clear_context();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for Message {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for Message {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct HardState {
    // message fields
    pub term: u64,
    pub vote: u64,
    pub commit: u64,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for HardState {}

impl HardState {
    pub fn new() -> HardState {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static HardState {
        static mut instance: ::protobuf::lazy::Lazy<HardState> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const HardState,
        };
        unsafe {
            instance.get(HardState::new)
        }
    }

    // uint64 term = 1;

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

    // uint64 vote = 2;

    pub fn clear_vote(&mut self) {
        self.vote = 0;
    }

    // Param is passed by value, moved
    pub fn set_vote(&mut self, v: u64) {
        self.vote = v;
    }

    pub fn get_vote(&self) -> u64 {
        self.vote
    }

    fn get_vote_for_reflect(&self) -> &u64 {
        &self.vote
    }

    fn mut_vote_for_reflect(&mut self) -> &mut u64 {
        &mut self.vote
    }

    // uint64 commit = 3;

    pub fn clear_commit(&mut self) {
        self.commit = 0;
    }

    // Param is passed by value, moved
    pub fn set_commit(&mut self, v: u64) {
        self.commit = v;
    }

    pub fn get_commit(&self) -> u64 {
        self.commit
    }

    fn get_commit_for_reflect(&self) -> &u64 {
        &self.commit
    }

    fn mut_commit_for_reflect(&mut self) -> &mut u64 {
        &mut self.commit
    }
}

impl ::protobuf::Message for HardState {
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
                    self.term = tmp;
                },
                2 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_uint64()?;
                    self.vote = tmp;
                },
                3 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_uint64()?;
                    self.commit = tmp;
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
        if self.term != 0 {
            my_size += ::protobuf::rt::value_size(1, self.term, ::protobuf::wire_format::WireTypeVarint);
        }
        if self.vote != 0 {
            my_size += ::protobuf::rt::value_size(2, self.vote, ::protobuf::wire_format::WireTypeVarint);
        }
        if self.commit != 0 {
            my_size += ::protobuf::rt::value_size(3, self.commit, ::protobuf::wire_format::WireTypeVarint);
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if self.term != 0 {
            os.write_uint64(1, self.term)?;
        }
        if self.vote != 0 {
            os.write_uint64(2, self.vote)?;
        }
        if self.commit != 0 {
            os.write_uint64(3, self.commit)?;
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

impl ::protobuf::MessageStatic for HardState {
    fn new() -> HardState {
        HardState::new()
    }

    fn descriptor_static(_: ::std::option::Option<HardState>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "term",
                    HardState::get_term_for_reflect,
                    HardState::mut_term_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "vote",
                    HardState::get_vote_for_reflect,
                    HardState::mut_vote_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "commit",
                    HardState::get_commit_for_reflect,
                    HardState::mut_commit_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<HardState>(
                    "HardState",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for HardState {
    fn clear(&mut self) {
        self.clear_term();
        self.clear_vote();
        self.clear_commit();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for HardState {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for HardState {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct ConfState {
    // message fields
    pub nodes: ::std::vec::Vec<u64>,
    pub learners: ::std::vec::Vec<u64>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for ConfState {}

impl ConfState {
    pub fn new() -> ConfState {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static ConfState {
        static mut instance: ::protobuf::lazy::Lazy<ConfState> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ConfState,
        };
        unsafe {
            instance.get(ConfState::new)
        }
    }

    // repeated uint64 nodes = 1;

    pub fn clear_nodes(&mut self) {
        self.nodes.clear();
    }

    // Param is passed by value, moved
    pub fn set_nodes(&mut self, v: ::std::vec::Vec<u64>) {
        self.nodes = v;
    }

    // Mutable pointer to the field.
    pub fn mut_nodes(&mut self) -> &mut ::std::vec::Vec<u64> {
        &mut self.nodes
    }

    // Take field
    pub fn take_nodes(&mut self) -> ::std::vec::Vec<u64> {
        ::std::mem::replace(&mut self.nodes, ::std::vec::Vec::new())
    }

    pub fn get_nodes(&self) -> &[u64] {
        &self.nodes
    }

    fn get_nodes_for_reflect(&self) -> &::std::vec::Vec<u64> {
        &self.nodes
    }

    fn mut_nodes_for_reflect(&mut self) -> &mut ::std::vec::Vec<u64> {
        &mut self.nodes
    }

    // repeated uint64 learners = 2;

    pub fn clear_learners(&mut self) {
        self.learners.clear();
    }

    // Param is passed by value, moved
    pub fn set_learners(&mut self, v: ::std::vec::Vec<u64>) {
        self.learners = v;
    }

    // Mutable pointer to the field.
    pub fn mut_learners(&mut self) -> &mut ::std::vec::Vec<u64> {
        &mut self.learners
    }

    // Take field
    pub fn take_learners(&mut self) -> ::std::vec::Vec<u64> {
        ::std::mem::replace(&mut self.learners, ::std::vec::Vec::new())
    }

    pub fn get_learners(&self) -> &[u64] {
        &self.learners
    }

    fn get_learners_for_reflect(&self) -> &::std::vec::Vec<u64> {
        &self.learners
    }

    fn mut_learners_for_reflect(&mut self) -> &mut ::std::vec::Vec<u64> {
        &mut self.learners
    }
}

impl ::protobuf::Message for ConfState {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !is.eof()? {
            let (field_number, wire_type) = is.read_tag_unpack()?;
            match field_number {
                1 => {
                    ::protobuf::rt::read_repeated_uint64_into(wire_type, is, &mut self.nodes)?;
                },
                2 => {
                    ::protobuf::rt::read_repeated_uint64_into(wire_type, is, &mut self.learners)?;
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
        for value in &self.nodes {
            my_size += ::protobuf::rt::value_size(1, *value, ::protobuf::wire_format::WireTypeVarint);
        };
        for value in &self.learners {
            my_size += ::protobuf::rt::value_size(2, *value, ::protobuf::wire_format::WireTypeVarint);
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        for v in &self.nodes {
            os.write_uint64(1, *v)?;
        };
        for v in &self.learners {
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

impl ::protobuf::MessageStatic for ConfState {
    fn new() -> ConfState {
        ConfState::new()
    }

    fn descriptor_static(_: ::std::option::Option<ConfState>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_vec_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "nodes",
                    ConfState::get_nodes_for_reflect,
                    ConfState::mut_nodes_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_vec_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "learners",
                    ConfState::get_learners_for_reflect,
                    ConfState::mut_learners_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<ConfState>(
                    "ConfState",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for ConfState {
    fn clear(&mut self) {
        self.clear_nodes();
        self.clear_learners();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for ConfState {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for ConfState {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct ConfChange {
    // message fields
    pub id: u64,
    pub change_type: ConfChangeType,
    pub node_id: u64,
    pub context: ::std::vec::Vec<u8>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for ConfChange {}

impl ConfChange {
    pub fn new() -> ConfChange {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static ConfChange {
        static mut instance: ::protobuf::lazy::Lazy<ConfChange> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ConfChange,
        };
        unsafe {
            instance.get(ConfChange::new)
        }
    }

    // uint64 id = 1;

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

    // .eraftpb.ConfChangeType change_type = 2;

    pub fn clear_change_type(&mut self) {
        self.change_type = ConfChangeType::AddNode;
    }

    // Param is passed by value, moved
    pub fn set_change_type(&mut self, v: ConfChangeType) {
        self.change_type = v;
    }

    pub fn get_change_type(&self) -> ConfChangeType {
        self.change_type
    }

    fn get_change_type_for_reflect(&self) -> &ConfChangeType {
        &self.change_type
    }

    fn mut_change_type_for_reflect(&mut self) -> &mut ConfChangeType {
        &mut self.change_type
    }

    // uint64 node_id = 3;

    pub fn clear_node_id(&mut self) {
        self.node_id = 0;
    }

    // Param is passed by value, moved
    pub fn set_node_id(&mut self, v: u64) {
        self.node_id = v;
    }

    pub fn get_node_id(&self) -> u64 {
        self.node_id
    }

    fn get_node_id_for_reflect(&self) -> &u64 {
        &self.node_id
    }

    fn mut_node_id_for_reflect(&mut self) -> &mut u64 {
        &mut self.node_id
    }

    // bytes context = 4;

    pub fn clear_context(&mut self) {
        self.context.clear();
    }

    // Param is passed by value, moved
    pub fn set_context(&mut self, v: ::std::vec::Vec<u8>) {
        self.context = v;
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_context(&mut self) -> &mut ::std::vec::Vec<u8> {
        &mut self.context
    }

    // Take field
    pub fn take_context(&mut self) -> ::std::vec::Vec<u8> {
        ::std::mem::replace(&mut self.context, ::std::vec::Vec::new())
    }

    pub fn get_context(&self) -> &[u8] {
        &self.context
    }

    fn get_context_for_reflect(&self) -> &::std::vec::Vec<u8> {
        &self.context
    }

    fn mut_context_for_reflect(&mut self) -> &mut ::std::vec::Vec<u8> {
        &mut self.context
    }
}

impl ::protobuf::Message for ConfChange {
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
                    self.id = tmp;
                },
                2 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_enum()?;
                    self.change_type = tmp;
                },
                3 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_uint64()?;
                    self.node_id = tmp;
                },
                4 => {
                    ::protobuf::rt::read_singular_proto3_bytes_into(wire_type, is, &mut self.context)?;
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
        if self.id != 0 {
            my_size += ::protobuf::rt::value_size(1, self.id, ::protobuf::wire_format::WireTypeVarint);
        }
        if self.change_type != ConfChangeType::AddNode {
            my_size += ::protobuf::rt::enum_size(2, self.change_type);
        }
        if self.node_id != 0 {
            my_size += ::protobuf::rt::value_size(3, self.node_id, ::protobuf::wire_format::WireTypeVarint);
        }
        if !self.context.is_empty() {
            my_size += ::protobuf::rt::bytes_size(4, &self.context);
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if self.id != 0 {
            os.write_uint64(1, self.id)?;
        }
        if self.change_type != ConfChangeType::AddNode {
            os.write_enum(2, self.change_type.value())?;
        }
        if self.node_id != 0 {
            os.write_uint64(3, self.node_id)?;
        }
        if !self.context.is_empty() {
            os.write_bytes(4, &self.context)?;
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

impl ::protobuf::MessageStatic for ConfChange {
    fn new() -> ConfChange {
        ConfChange::new()
    }

    fn descriptor_static(_: ::std::option::Option<ConfChange>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "id",
                    ConfChange::get_id_for_reflect,
                    ConfChange::mut_id_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeEnum<ConfChangeType>>(
                    "change_type",
                    ConfChange::get_change_type_for_reflect,
                    ConfChange::mut_change_type_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "node_id",
                    ConfChange::get_node_id_for_reflect,
                    ConfChange::mut_node_id_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeBytes>(
                    "context",
                    ConfChange::get_context_for_reflect,
                    ConfChange::mut_context_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<ConfChange>(
                    "ConfChange",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for ConfChange {
    fn clear(&mut self) {
        self.clear_id();
        self.clear_change_type();
        self.clear_node_id();
        self.clear_context();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for ConfChange {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for ConfChange {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(Clone,PartialEq,Eq,Debug,Hash)]
pub enum EntryType {
    EntryNormal = 0,
    EntryConfChange = 1,
}

impl ::protobuf::ProtobufEnum for EntryType {
    fn value(&self) -> i32 {
        *self as i32
    }

    fn from_i32(value: i32) -> ::std::option::Option<EntryType> {
        match value {
            0 => ::std::option::Option::Some(EntryType::EntryNormal),
            1 => ::std::option::Option::Some(EntryType::EntryConfChange),
            _ => ::std::option::Option::None
        }
    }

    fn values() -> &'static [Self] {
        static values: &'static [EntryType] = &[
            EntryType::EntryNormal,
            EntryType::EntryConfChange,
        ];
        values
    }

    fn enum_descriptor_static(_: ::std::option::Option<EntryType>) -> &'static ::protobuf::reflect::EnumDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::EnumDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::EnumDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                ::protobuf::reflect::EnumDescriptor::new("EntryType", file_descriptor_proto())
            })
        }
    }
}

impl ::std::marker::Copy for EntryType {
}

impl ::std::default::Default for EntryType {
    fn default() -> Self {
        EntryType::EntryNormal
    }
}

impl ::protobuf::reflect::ProtobufValue for EntryType {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Enum(self.descriptor())
    }
}

#[derive(Clone,PartialEq,Eq,Debug,Hash)]
pub enum MessageType {
    MsgHup = 0,
    MsgBeat = 1,
    MsgPropose = 2,
    MsgAppend = 3,
    MsgAppendResponse = 4,
    MsgRequestVote = 5,
    MsgRequestVoteResponse = 6,
    MsgSnapshot = 7,
    MsgHeartbeat = 8,
    MsgHeartbeatResponse = 9,
    MsgUnreachable = 10,
    MsgSnapStatus = 11,
    MsgCheckQuorum = 12,
    MsgTransferLeader = 13,
    MsgTimeoutNow = 14,
    MsgReadIndex = 15,
    MsgReadIndexResp = 16,
    MsgRequestPreVote = 17,
    MsgRequestPreVoteResponse = 18,
}

impl ::protobuf::ProtobufEnum for MessageType {
    fn value(&self) -> i32 {
        *self as i32
    }

    fn from_i32(value: i32) -> ::std::option::Option<MessageType> {
        match value {
            0 => ::std::option::Option::Some(MessageType::MsgHup),
            1 => ::std::option::Option::Some(MessageType::MsgBeat),
            2 => ::std::option::Option::Some(MessageType::MsgPropose),
            3 => ::std::option::Option::Some(MessageType::MsgAppend),
            4 => ::std::option::Option::Some(MessageType::MsgAppendResponse),
            5 => ::std::option::Option::Some(MessageType::MsgRequestVote),
            6 => ::std::option::Option::Some(MessageType::MsgRequestVoteResponse),
            7 => ::std::option::Option::Some(MessageType::MsgSnapshot),
            8 => ::std::option::Option::Some(MessageType::MsgHeartbeat),
            9 => ::std::option::Option::Some(MessageType::MsgHeartbeatResponse),
            10 => ::std::option::Option::Some(MessageType::MsgUnreachable),
            11 => ::std::option::Option::Some(MessageType::MsgSnapStatus),
            12 => ::std::option::Option::Some(MessageType::MsgCheckQuorum),
            13 => ::std::option::Option::Some(MessageType::MsgTransferLeader),
            14 => ::std::option::Option::Some(MessageType::MsgTimeoutNow),
            15 => ::std::option::Option::Some(MessageType::MsgReadIndex),
            16 => ::std::option::Option::Some(MessageType::MsgReadIndexResp),
            17 => ::std::option::Option::Some(MessageType::MsgRequestPreVote),
            18 => ::std::option::Option::Some(MessageType::MsgRequestPreVoteResponse),
            _ => ::std::option::Option::None
        }
    }

    fn values() -> &'static [Self] {
        static values: &'static [MessageType] = &[
            MessageType::MsgHup,
            MessageType::MsgBeat,
            MessageType::MsgPropose,
            MessageType::MsgAppend,
            MessageType::MsgAppendResponse,
            MessageType::MsgRequestVote,
            MessageType::MsgRequestVoteResponse,
            MessageType::MsgSnapshot,
            MessageType::MsgHeartbeat,
            MessageType::MsgHeartbeatResponse,
            MessageType::MsgUnreachable,
            MessageType::MsgSnapStatus,
            MessageType::MsgCheckQuorum,
            MessageType::MsgTransferLeader,
            MessageType::MsgTimeoutNow,
            MessageType::MsgReadIndex,
            MessageType::MsgReadIndexResp,
            MessageType::MsgRequestPreVote,
            MessageType::MsgRequestPreVoteResponse,
        ];
        values
    }

    fn enum_descriptor_static(_: ::std::option::Option<MessageType>) -> &'static ::protobuf::reflect::EnumDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::EnumDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::EnumDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                ::protobuf::reflect::EnumDescriptor::new("MessageType", file_descriptor_proto())
            })
        }
    }
}

impl ::std::marker::Copy for MessageType {
}

impl ::std::default::Default for MessageType {
    fn default() -> Self {
        MessageType::MsgHup
    }
}

impl ::protobuf::reflect::ProtobufValue for MessageType {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Enum(self.descriptor())
    }
}

#[derive(Clone,PartialEq,Eq,Debug,Hash)]
pub enum ConfChangeType {
    AddNode = 0,
    RemoveNode = 1,
    AddLearnerNode = 2,
}

impl ::protobuf::ProtobufEnum for ConfChangeType {
    fn value(&self) -> i32 {
        *self as i32
    }

    fn from_i32(value: i32) -> ::std::option::Option<ConfChangeType> {
        match value {
            0 => ::std::option::Option::Some(ConfChangeType::AddNode),
            1 => ::std::option::Option::Some(ConfChangeType::RemoveNode),
            2 => ::std::option::Option::Some(ConfChangeType::AddLearnerNode),
            _ => ::std::option::Option::None
        }
    }

    fn values() -> &'static [Self] {
        static values: &'static [ConfChangeType] = &[
            ConfChangeType::AddNode,
            ConfChangeType::RemoveNode,
            ConfChangeType::AddLearnerNode,
        ];
        values
    }

    fn enum_descriptor_static(_: ::std::option::Option<ConfChangeType>) -> &'static ::protobuf::reflect::EnumDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::EnumDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::EnumDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                ::protobuf::reflect::EnumDescriptor::new("ConfChangeType", file_descriptor_proto())
            })
        }
    }
}

impl ::std::marker::Copy for ConfChangeType {
}

impl ::std::default::Default for ConfChangeType {
    fn default() -> Self {
        ConfChangeType::AddNode
    }
}

impl ::protobuf::reflect::ProtobufValue for ConfChangeType {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Enum(self.descriptor())
    }
}

static file_descriptor_proto_data: &'static [u8] = b"\
    \n\reraftpb.proto\x12\x07eraftpb\"\x93\x01\n\x05Entry\x121\n\nentry_type\
    \x18\x01\x20\x01(\x0e2\x12.eraftpb.EntryTypeR\tentryType\x12\x12\n\x04te\
    rm\x18\x02\x20\x01(\x04R\x04term\x12\x14\n\x05index\x18\x03\x20\x01(\x04\
    R\x05index\x12\x12\n\x04data\x18\x04\x20\x01(\x0cR\x04data\x12\x19\n\x08\
    sync_log\x18\x05\x20\x01(\x08R\x07syncLog\"o\n\x10SnapshotMetadata\x121\
    \n\nconf_state\x18\x01\x20\x01(\x0b2\x12.eraftpb.ConfStateR\tconfState\
    \x12\x14\n\x05index\x18\x02\x20\x01(\x04R\x05index\x12\x12\n\x04term\x18\
    \x03\x20\x01(\x04R\x04term\"U\n\x08Snapshot\x12\x12\n\x04data\x18\x01\
    \x20\x01(\x0cR\x04data\x125\n\x08metadata\x18\x02\x20\x01(\x0b2\x19.eraf\
    tpb.SnapshotMetadataR\x08metadata\"\xe7\x02\n\x07Message\x12/\n\x08msg_t\
    ype\x18\x01\x20\x01(\x0e2\x14.eraftpb.MessageTypeR\x07msgType\x12\x0e\n\
    \x02to\x18\x02\x20\x01(\x04R\x02to\x12\x12\n\x04from\x18\x03\x20\x01(\
    \x04R\x04from\x12\x12\n\x04term\x18\x04\x20\x01(\x04R\x04term\x12\x19\n\
    \x08log_term\x18\x05\x20\x01(\x04R\x07logTerm\x12\x14\n\x05index\x18\x06\
    \x20\x01(\x04R\x05index\x12(\n\x07entries\x18\x07\x20\x03(\x0b2\x0e.eraf\
    tpb.EntryR\x07entries\x12\x16\n\x06commit\x18\x08\x20\x01(\x04R\x06commi\
    t\x12-\n\x08snapshot\x18\t\x20\x01(\x0b2\x11.eraftpb.SnapshotR\x08snapsh\
    ot\x12\x16\n\x06reject\x18\n\x20\x01(\x08R\x06reject\x12\x1f\n\x0breject\
    _hint\x18\x0b\x20\x01(\x04R\nrejectHint\x12\x18\n\x07context\x18\x0c\x20\
    \x01(\x0cR\x07context\"K\n\tHardState\x12\x12\n\x04term\x18\x01\x20\x01(\
    \x04R\x04term\x12\x12\n\x04vote\x18\x02\x20\x01(\x04R\x04vote\x12\x16\n\
    \x06commit\x18\x03\x20\x01(\x04R\x06commit\"=\n\tConfState\x12\x14\n\x05\
    nodes\x18\x01\x20\x03(\x04R\x05nodes\x12\x1a\n\x08learners\x18\x02\x20\
    \x03(\x04R\x08learners\"\x89\x01\n\nConfChange\x12\x0e\n\x02id\x18\x01\
    \x20\x01(\x04R\x02id\x128\n\x0bchange_type\x18\x02\x20\x01(\x0e2\x17.era\
    ftpb.ConfChangeTypeR\nchangeType\x12\x17\n\x07node_id\x18\x03\x20\x01(\
    \x04R\x06nodeId\x12\x18\n\x07context\x18\x04\x20\x01(\x0cR\x07context*1\
    \n\tEntryType\x12\x0f\n\x0bEntryNormal\x10\0\x12\x13\n\x0fEntryConfChang\
    e\x10\x01*\x8c\x03\n\x0bMessageType\x12\n\n\x06MsgHup\x10\0\x12\x0b\n\
    \x07MsgBeat\x10\x01\x12\x0e\n\nMsgPropose\x10\x02\x12\r\n\tMsgAppend\x10\
    \x03\x12\x15\n\x11MsgAppendResponse\x10\x04\x12\x12\n\x0eMsgRequestVote\
    \x10\x05\x12\x1a\n\x16MsgRequestVoteResponse\x10\x06\x12\x0f\n\x0bMsgSna\
    pshot\x10\x07\x12\x10\n\x0cMsgHeartbeat\x10\x08\x12\x18\n\x14MsgHeartbea\
    tResponse\x10\t\x12\x12\n\x0eMsgUnreachable\x10\n\x12\x11\n\rMsgSnapStat\
    us\x10\x0b\x12\x12\n\x0eMsgCheckQuorum\x10\x0c\x12\x15\n\x11MsgTransferL\
    eader\x10\r\x12\x11\n\rMsgTimeoutNow\x10\x0e\x12\x10\n\x0cMsgReadIndex\
    \x10\x0f\x12\x14\n\x10MsgReadIndexResp\x10\x10\x12\x15\n\x11MsgRequestPr\
    eVote\x10\x11\x12\x1d\n\x19MsgRequestPreVoteResponse\x10\x12*A\n\x0eConf\
    ChangeType\x12\x0b\n\x07AddNode\x10\0\x12\x0e\n\nRemoveNode\x10\x01\x12\
    \x12\n\x0eAddLearnerNode\x10\x02B\x1a\n\x18com.pingcap.tikv.kvprotoJ\xad\
    \x1b\n\x06\x12\x04\0\0X\x01\n\x08\n\x01\x0c\x12\x03\0\0\x12\n\x08\n\x01\
    \x02\x12\x03\x01\x08\x0f\n\x08\n\x01\x08\x12\x03\x03\01\n\x0b\n\x04\x08\
    \xe7\x07\0\x12\x03\x03\01\n\x0c\n\x05\x08\xe7\x07\0\x02\x12\x03\x03\x07\
    \x13\n\r\n\x06\x08\xe7\x07\0\x02\0\x12\x03\x03\x07\x13\n\x0e\n\x07\x08\
    \xe7\x07\0\x02\0\x01\x12\x03\x03\x07\x13\n\x0c\n\x05\x08\xe7\x07\0\x07\
    \x12\x03\x03\x160\n\n\n\x02\x05\0\x12\x04\x05\0\x08\x01\n\n\n\x03\x05\0\
    \x01\x12\x03\x05\x05\x0e\n\x0b\n\x04\x05\0\x02\0\x12\x03\x06\x04\x14\n\
    \x0c\n\x05\x05\0\x02\0\x01\x12\x03\x06\x04\x0f\n\x0c\n\x05\x05\0\x02\0\
    \x02\x12\x03\x06\x12\x13\n\x0b\n\x04\x05\0\x02\x01\x12\x03\x07\x04\x18\n\
    \x0c\n\x05\x05\0\x02\x01\x01\x12\x03\x07\x04\x13\n\x0c\n\x05\x05\0\x02\
    \x01\x02\x12\x03\x07\x16\x17\n\n\n\x02\x04\0\x12\x04\n\0\x10\x01\n\n\n\
    \x03\x04\0\x01\x12\x03\n\x08\r\n\x0b\n\x04\x04\0\x02\0\x12\x03\x0b\x04\
    \x1d\n\r\n\x05\x04\0\x02\0\x04\x12\x04\x0b\x04\n\x0f\n\x0c\n\x05\x04\0\
    \x02\0\x06\x12\x03\x0b\x04\r\n\x0c\n\x05\x04\0\x02\0\x01\x12\x03\x0b\x0e\
    \x18\n\x0c\n\x05\x04\0\x02\0\x03\x12\x03\x0b\x1b\x1c\n\x0b\n\x04\x04\0\
    \x02\x01\x12\x03\x0c\x04\x14\n\r\n\x05\x04\0\x02\x01\x04\x12\x04\x0c\x04\
    \x0b\x1d\n\x0c\n\x05\x04\0\x02\x01\x05\x12\x03\x0c\x04\n\n\x0c\n\x05\x04\
    \0\x02\x01\x01\x12\x03\x0c\x0b\x0f\n\x0c\n\x05\x04\0\x02\x01\x03\x12\x03\
    \x0c\x12\x13\n\x0b\n\x04\x04\0\x02\x02\x12\x03\r\x04\x15\n\r\n\x05\x04\0\
    \x02\x02\x04\x12\x04\r\x04\x0c\x14\n\x0c\n\x05\x04\0\x02\x02\x05\x12\x03\
    \r\x04\n\n\x0c\n\x05\x04\0\x02\x02\x01\x12\x03\r\x0b\x10\n\x0c\n\x05\x04\
    \0\x02\x02\x03\x12\x03\r\x13\x14\n\x0b\n\x04\x04\0\x02\x03\x12\x03\x0e\
    \x04\x13\n\r\n\x05\x04\0\x02\x03\x04\x12\x04\x0e\x04\r\x15\n\x0c\n\x05\
    \x04\0\x02\x03\x05\x12\x03\x0e\x04\t\n\x0c\n\x05\x04\0\x02\x03\x01\x12\
    \x03\x0e\n\x0e\n\x0c\n\x05\x04\0\x02\x03\x03\x12\x03\x0e\x11\x12\n\x0b\n\
    \x04\x04\0\x02\x04\x12\x03\x0f\x04\x16\n\r\n\x05\x04\0\x02\x04\x04\x12\
    \x04\x0f\x04\x0e\x13\n\x0c\n\x05\x04\0\x02\x04\x05\x12\x03\x0f\x04\x08\n\
    \x0c\n\x05\x04\0\x02\x04\x01\x12\x03\x0f\t\x11\n\x0c\n\x05\x04\0\x02\x04\
    \x03\x12\x03\x0f\x14\x15\n\n\n\x02\x04\x01\x12\x04\x12\0\x16\x01\n\n\n\
    \x03\x04\x01\x01\x12\x03\x12\x08\x18\n\x0b\n\x04\x04\x01\x02\0\x12\x03\
    \x13\x04\x1d\n\r\n\x05\x04\x01\x02\0\x04\x12\x04\x13\x04\x12\x1a\n\x0c\n\
    \x05\x04\x01\x02\0\x06\x12\x03\x13\x04\r\n\x0c\n\x05\x04\x01\x02\0\x01\
    \x12\x03\x13\x0e\x18\n\x0c\n\x05\x04\x01\x02\0\x03\x12\x03\x13\x1b\x1c\n\
    \x0b\n\x04\x04\x01\x02\x01\x12\x03\x14\x04\x15\n\r\n\x05\x04\x01\x02\x01\
    \x04\x12\x04\x14\x04\x13\x1d\n\x0c\n\x05\x04\x01\x02\x01\x05\x12\x03\x14\
    \x04\n\n\x0c\n\x05\x04\x01\x02\x01\x01\x12\x03\x14\x0b\x10\n\x0c\n\x05\
    \x04\x01\x02\x01\x03\x12\x03\x14\x13\x14\n\x0b\n\x04\x04\x01\x02\x02\x12\
    \x03\x15\x04\x14\n\r\n\x05\x04\x01\x02\x02\x04\x12\x04\x15\x04\x14\x15\n\
    \x0c\n\x05\x04\x01\x02\x02\x05\x12\x03\x15\x04\n\n\x0c\n\x05\x04\x01\x02\
    \x02\x01\x12\x03\x15\x0b\x0f\n\x0c\n\x05\x04\x01\x02\x02\x03\x12\x03\x15\
    \x12\x13\n\n\n\x02\x04\x02\x12\x04\x18\0\x1b\x01\n\n\n\x03\x04\x02\x01\
    \x12\x03\x18\x08\x10\n\x0b\n\x04\x04\x02\x02\0\x12\x03\x19\x04\x13\n\r\n\
    \x05\x04\x02\x02\0\x04\x12\x04\x19\x04\x18\x12\n\x0c\n\x05\x04\x02\x02\0\
    \x05\x12\x03\x19\x04\t\n\x0c\n\x05\x04\x02\x02\0\x01\x12\x03\x19\n\x0e\n\
    \x0c\n\x05\x04\x02\x02\0\x03\x12\x03\x19\x11\x12\n\x0b\n\x04\x04\x02\x02\
    \x01\x12\x03\x1a\x04\"\n\r\n\x05\x04\x02\x02\x01\x04\x12\x04\x1a\x04\x19\
    \x13\n\x0c\n\x05\x04\x02\x02\x01\x06\x12\x03\x1a\x04\x14\n\x0c\n\x05\x04\
    \x02\x02\x01\x01\x12\x03\x1a\x15\x1d\n\x0c\n\x05\x04\x02\x02\x01\x03\x12\
    \x03\x1a\x20!\n\n\n\x02\x05\x01\x12\x04\x1d\01\x01\n\n\n\x03\x05\x01\x01\
    \x12\x03\x1d\x05\x10\n\x0b\n\x04\x05\x01\x02\0\x12\x03\x1e\x04\x0f\n\x0c\
    \n\x05\x05\x01\x02\0\x01\x12\x03\x1e\x04\n\n\x0c\n\x05\x05\x01\x02\0\x02\
    \x12\x03\x1e\r\x0e\n\x0b\n\x04\x05\x01\x02\x01\x12\x03\x1f\x04\x10\n\x0c\
    \n\x05\x05\x01\x02\x01\x01\x12\x03\x1f\x04\x0b\n\x0c\n\x05\x05\x01\x02\
    \x01\x02\x12\x03\x1f\x0e\x0f\n\x0b\n\x04\x05\x01\x02\x02\x12\x03\x20\x04\
    \x13\n\x0c\n\x05\x05\x01\x02\x02\x01\x12\x03\x20\x04\x0e\n\x0c\n\x05\x05\
    \x01\x02\x02\x02\x12\x03\x20\x11\x12\n\x0b\n\x04\x05\x01\x02\x03\x12\x03\
    !\x04\x12\n\x0c\n\x05\x05\x01\x02\x03\x01\x12\x03!\x04\r\n\x0c\n\x05\x05\
    \x01\x02\x03\x02\x12\x03!\x10\x11\n\x0b\n\x04\x05\x01\x02\x04\x12\x03\"\
    \x04\x1a\n\x0c\n\x05\x05\x01\x02\x04\x01\x12\x03\"\x04\x15\n\x0c\n\x05\
    \x05\x01\x02\x04\x02\x12\x03\"\x18\x19\n\x0b\n\x04\x05\x01\x02\x05\x12\
    \x03#\x04\x17\n\x0c\n\x05\x05\x01\x02\x05\x01\x12\x03#\x04\x12\n\x0c\n\
    \x05\x05\x01\x02\x05\x02\x12\x03#\x15\x16\n\x0b\n\x04\x05\x01\x02\x06\
    \x12\x03$\x04\x1f\n\x0c\n\x05\x05\x01\x02\x06\x01\x12\x03$\x04\x1a\n\x0c\
    \n\x05\x05\x01\x02\x06\x02\x12\x03$\x1d\x1e\n\x0b\n\x04\x05\x01\x02\x07\
    \x12\x03%\x04\x14\n\x0c\n\x05\x05\x01\x02\x07\x01\x12\x03%\x04\x0f\n\x0c\
    \n\x05\x05\x01\x02\x07\x02\x12\x03%\x12\x13\n\x0b\n\x04\x05\x01\x02\x08\
    \x12\x03&\x04\x15\n\x0c\n\x05\x05\x01\x02\x08\x01\x12\x03&\x04\x10\n\x0c\
    \n\x05\x05\x01\x02\x08\x02\x12\x03&\x13\x14\n\x0b\n\x04\x05\x01\x02\t\
    \x12\x03'\x04\x1d\n\x0c\n\x05\x05\x01\x02\t\x01\x12\x03'\x04\x18\n\x0c\n\
    \x05\x05\x01\x02\t\x02\x12\x03'\x1b\x1c\n\x0b\n\x04\x05\x01\x02\n\x12\
    \x03(\x04\x18\n\x0c\n\x05\x05\x01\x02\n\x01\x12\x03(\x04\x12\n\x0c\n\x05\
    \x05\x01\x02\n\x02\x12\x03(\x15\x17\n\x0b\n\x04\x05\x01\x02\x0b\x12\x03)\
    \x04\x17\n\x0c\n\x05\x05\x01\x02\x0b\x01\x12\x03)\x04\x11\n\x0c\n\x05\
    \x05\x01\x02\x0b\x02\x12\x03)\x14\x16\n\x0b\n\x04\x05\x01\x02\x0c\x12\
    \x03*\x04\x18\n\x0c\n\x05\x05\x01\x02\x0c\x01\x12\x03*\x04\x12\n\x0c\n\
    \x05\x05\x01\x02\x0c\x02\x12\x03*\x15\x17\n\x0b\n\x04\x05\x01\x02\r\x12\
    \x03+\x04\x1b\n\x0c\n\x05\x05\x01\x02\r\x01\x12\x03+\x04\x15\n\x0c\n\x05\
    \x05\x01\x02\r\x02\x12\x03+\x18\x1a\n\x0b\n\x04\x05\x01\x02\x0e\x12\x03,\
    \x04\x17\n\x0c\n\x05\x05\x01\x02\x0e\x01\x12\x03,\x04\x11\n\x0c\n\x05\
    \x05\x01\x02\x0e\x02\x12\x03,\x14\x16\n\x0b\n\x04\x05\x01\x02\x0f\x12\
    \x03-\x04\x16\n\x0c\n\x05\x05\x01\x02\x0f\x01\x12\x03-\x04\x10\n\x0c\n\
    \x05\x05\x01\x02\x0f\x02\x12\x03-\x13\x15\n\x0b\n\x04\x05\x01\x02\x10\
    \x12\x03.\x04\x1a\n\x0c\n\x05\x05\x01\x02\x10\x01\x12\x03.\x04\x14\n\x0c\
    \n\x05\x05\x01\x02\x10\x02\x12\x03.\x17\x19\n\x0b\n\x04\x05\x01\x02\x11\
    \x12\x03/\x04\x1b\n\x0c\n\x05\x05\x01\x02\x11\x01\x12\x03/\x04\x15\n\x0c\
    \n\x05\x05\x01\x02\x11\x02\x12\x03/\x18\x1a\n\x0b\n\x04\x05\x01\x02\x12\
    \x12\x030\x04#\n\x0c\n\x05\x05\x01\x02\x12\x01\x12\x030\x04\x1d\n\x0c\n\
    \x05\x05\x01\x02\x12\x02\x12\x030\x20\"\n\n\n\x02\x04\x03\x12\x043\0@\
    \x01\n\n\n\x03\x04\x03\x01\x12\x033\x08\x0f\n\x0b\n\x04\x04\x03\x02\0\
    \x12\x034\x04\x1d\n\r\n\x05\x04\x03\x02\0\x04\x12\x044\x043\x11\n\x0c\n\
    \x05\x04\x03\x02\0\x06\x12\x034\x04\x0f\n\x0c\n\x05\x04\x03\x02\0\x01\
    \x12\x034\x10\x18\n\x0c\n\x05\x04\x03\x02\0\x03\x12\x034\x1b\x1c\n\x0b\n\
    \x04\x04\x03\x02\x01\x12\x035\x04\x12\n\r\n\x05\x04\x03\x02\x01\x04\x12\
    \x045\x044\x1d\n\x0c\n\x05\x04\x03\x02\x01\x05\x12\x035\x04\n\n\x0c\n\
    \x05\x04\x03\x02\x01\x01\x12\x035\x0b\r\n\x0c\n\x05\x04\x03\x02\x01\x03\
    \x12\x035\x10\x11\n\x0b\n\x04\x04\x03\x02\x02\x12\x036\x04\x14\n\r\n\x05\
    \x04\x03\x02\x02\x04\x12\x046\x045\x12\n\x0c\n\x05\x04\x03\x02\x02\x05\
    \x12\x036\x04\n\n\x0c\n\x05\x04\x03\x02\x02\x01\x12\x036\x0b\x0f\n\x0c\n\
    \x05\x04\x03\x02\x02\x03\x12\x036\x12\x13\n\x0b\n\x04\x04\x03\x02\x03\
    \x12\x037\x04\x14\n\r\n\x05\x04\x03\x02\x03\x04\x12\x047\x046\x14\n\x0c\
    \n\x05\x04\x03\x02\x03\x05\x12\x037\x04\n\n\x0c\n\x05\x04\x03\x02\x03\
    \x01\x12\x037\x0b\x0f\n\x0c\n\x05\x04\x03\x02\x03\x03\x12\x037\x12\x13\n\
    \x0b\n\x04\x04\x03\x02\x04\x12\x038\x04\x18\n\r\n\x05\x04\x03\x02\x04\
    \x04\x12\x048\x047\x14\n\x0c\n\x05\x04\x03\x02\x04\x05\x12\x038\x04\n\n\
    \x0c\n\x05\x04\x03\x02\x04\x01\x12\x038\x0b\x13\n\x0c\n\x05\x04\x03\x02\
    \x04\x03\x12\x038\x16\x17\n\x0b\n\x04\x04\x03\x02\x05\x12\x039\x04\x15\n\
    \r\n\x05\x04\x03\x02\x05\x04\x12\x049\x048\x18\n\x0c\n\x05\x04\x03\x02\
    \x05\x05\x12\x039\x04\n\n\x0c\n\x05\x04\x03\x02\x05\x01\x12\x039\x0b\x10\
    \n\x0c\n\x05\x04\x03\x02\x05\x03\x12\x039\x13\x14\n\x0b\n\x04\x04\x03\
    \x02\x06\x12\x03:\x04\x1f\n\x0c\n\x05\x04\x03\x02\x06\x04\x12\x03:\x04\
    \x0c\n\x0c\n\x05\x04\x03\x02\x06\x06\x12\x03:\r\x12\n\x0c\n\x05\x04\x03\
    \x02\x06\x01\x12\x03:\x13\x1a\n\x0c\n\x05\x04\x03\x02\x06\x03\x12\x03:\
    \x1d\x1e\n\x0b\n\x04\x04\x03\x02\x07\x12\x03;\x04\x16\n\r\n\x05\x04\x03\
    \x02\x07\x04\x12\x04;\x04:\x1f\n\x0c\n\x05\x04\x03\x02\x07\x05\x12\x03;\
    \x04\n\n\x0c\n\x05\x04\x03\x02\x07\x01\x12\x03;\x0b\x11\n\x0c\n\x05\x04\
    \x03\x02\x07\x03\x12\x03;\x14\x15\n\x0b\n\x04\x04\x03\x02\x08\x12\x03<\
    \x04\x1a\n\r\n\x05\x04\x03\x02\x08\x04\x12\x04<\x04;\x16\n\x0c\n\x05\x04\
    \x03\x02\x08\x06\x12\x03<\x04\x0c\n\x0c\n\x05\x04\x03\x02\x08\x01\x12\
    \x03<\r\x15\n\x0c\n\x05\x04\x03\x02\x08\x03\x12\x03<\x18\x19\n\x0b\n\x04\
    \x04\x03\x02\t\x12\x03=\x04\x15\n\r\n\x05\x04\x03\x02\t\x04\x12\x04=\x04\
    <\x1a\n\x0c\n\x05\x04\x03\x02\t\x05\x12\x03=\x04\x08\n\x0c\n\x05\x04\x03\
    \x02\t\x01\x12\x03=\t\x0f\n\x0c\n\x05\x04\x03\x02\t\x03\x12\x03=\x12\x14\
    \n\x0b\n\x04\x04\x03\x02\n\x12\x03>\x04\x1c\n\r\n\x05\x04\x03\x02\n\x04\
    \x12\x04>\x04=\x15\n\x0c\n\x05\x04\x03\x02\n\x05\x12\x03>\x04\n\n\x0c\n\
    \x05\x04\x03\x02\n\x01\x12\x03>\x0b\x16\n\x0c\n\x05\x04\x03\x02\n\x03\
    \x12\x03>\x19\x1b\n\x0b\n\x04\x04\x03\x02\x0b\x12\x03?\x04\x17\n\r\n\x05\
    \x04\x03\x02\x0b\x04\x12\x04?\x04>\x1c\n\x0c\n\x05\x04\x03\x02\x0b\x05\
    \x12\x03?\x04\t\n\x0c\n\x05\x04\x03\x02\x0b\x01\x12\x03?\n\x11\n\x0c\n\
    \x05\x04\x03\x02\x0b\x03\x12\x03?\x14\x16\n\n\n\x02\x04\x04\x12\x04B\0F\
    \x01\n\n\n\x03\x04\x04\x01\x12\x03B\x08\x11\n\x0b\n\x04\x04\x04\x02\0\
    \x12\x03C\x04\x14\n\r\n\x05\x04\x04\x02\0\x04\x12\x04C\x04B\x13\n\x0c\n\
    \x05\x04\x04\x02\0\x05\x12\x03C\x04\n\n\x0c\n\x05\x04\x04\x02\0\x01\x12\
    \x03C\x0b\x0f\n\x0c\n\x05\x04\x04\x02\0\x03\x12\x03C\x12\x13\n\x0b\n\x04\
    \x04\x04\x02\x01\x12\x03D\x04\x14\n\r\n\x05\x04\x04\x02\x01\x04\x12\x04D\
    \x04C\x14\n\x0c\n\x05\x04\x04\x02\x01\x05\x12\x03D\x04\n\n\x0c\n\x05\x04\
    \x04\x02\x01\x01\x12\x03D\x0b\x0f\n\x0c\n\x05\x04\x04\x02\x01\x03\x12\
    \x03D\x12\x13\n\x0b\n\x04\x04\x04\x02\x02\x12\x03E\x04\x16\n\r\n\x05\x04\
    \x04\x02\x02\x04\x12\x04E\x04D\x14\n\x0c\n\x05\x04\x04\x02\x02\x05\x12\
    \x03E\x04\n\n\x0c\n\x05\x04\x04\x02\x02\x01\x12\x03E\x0b\x11\n\x0c\n\x05\
    \x04\x04\x02\x02\x03\x12\x03E\x14\x15\n\n\n\x02\x04\x05\x12\x04H\0K\x01\
    \n\n\n\x03\x04\x05\x01\x12\x03H\x08\x11\n\x0b\n\x04\x04\x05\x02\0\x12\
    \x03I\x04\x1e\n\x0c\n\x05\x04\x05\x02\0\x04\x12\x03I\x04\x0c\n\x0c\n\x05\
    \x04\x05\x02\0\x05\x12\x03I\r\x13\n\x0c\n\x05\x04\x05\x02\0\x01\x12\x03I\
    \x14\x19\n\x0c\n\x05\x04\x05\x02\0\x03\x12\x03I\x1c\x1d\n\x0b\n\x04\x04\
    \x05\x02\x01\x12\x03J\x04!\n\x0c\n\x05\x04\x05\x02\x01\x04\x12\x03J\x04\
    \x0c\n\x0c\n\x05\x04\x05\x02\x01\x05\x12\x03J\r\x13\n\x0c\n\x05\x04\x05\
    \x02\x01\x01\x12\x03J\x14\x1c\n\x0c\n\x05\x04\x05\x02\x01\x03\x12\x03J\
    \x1f\x20\n\n\n\x02\x05\x02\x12\x04M\0Q\x01\n\n\n\x03\x05\x02\x01\x12\x03\
    M\x05\x13\n\x0b\n\x04\x05\x02\x02\0\x12\x03N\x04\x13\n\x0c\n\x05\x05\x02\
    \x02\0\x01\x12\x03N\x04\x0b\n\x0c\n\x05\x05\x02\x02\0\x02\x12\x03N\x11\
    \x12\n\x0b\n\x04\x05\x02\x02\x01\x12\x03O\x04\x13\n\x0c\n\x05\x05\x02\
    \x02\x01\x01\x12\x03O\x04\x0e\n\x0c\n\x05\x05\x02\x02\x01\x02\x12\x03O\
    \x11\x12\n\x0b\n\x04\x05\x02\x02\x02\x12\x03P\x04\x17\n\x0c\n\x05\x05\
    \x02\x02\x02\x01\x12\x03P\x04\x12\n\x0c\n\x05\x05\x02\x02\x02\x02\x12\
    \x03P\x15\x16\n\n\n\x02\x04\x06\x12\x04S\0X\x01\n\n\n\x03\x04\x06\x01\
    \x12\x03S\x08\x12\n\x0b\n\x04\x04\x06\x02\0\x12\x03T\x04\x12\n\r\n\x05\
    \x04\x06\x02\0\x04\x12\x04T\x04S\x14\n\x0c\n\x05\x04\x06\x02\0\x05\x12\
    \x03T\x04\n\n\x0c\n\x05\x04\x06\x02\0\x01\x12\x03T\x0b\r\n\x0c\n\x05\x04\
    \x06\x02\0\x03\x12\x03T\x10\x11\n\x0b\n\x04\x04\x06\x02\x01\x12\x03U\x04\
    #\n\r\n\x05\x04\x06\x02\x01\x04\x12\x04U\x04T\x12\n\x0c\n\x05\x04\x06\
    \x02\x01\x06\x12\x03U\x04\x12\n\x0c\n\x05\x04\x06\x02\x01\x01\x12\x03U\
    \x13\x1e\n\x0c\n\x05\x04\x06\x02\x01\x03\x12\x03U!\"\n\x0b\n\x04\x04\x06\
    \x02\x02\x12\x03V\x04\x17\n\r\n\x05\x04\x06\x02\x02\x04\x12\x04V\x04U#\n\
    \x0c\n\x05\x04\x06\x02\x02\x05\x12\x03V\x04\n\n\x0c\n\x05\x04\x06\x02\
    \x02\x01\x12\x03V\x0b\x12\n\x0c\n\x05\x04\x06\x02\x02\x03\x12\x03V\x15\
    \x16\n\x0b\n\x04\x04\x06\x02\x03\x12\x03W\x04\x16\n\r\n\x05\x04\x06\x02\
    \x03\x04\x12\x04W\x04V\x17\n\x0c\n\x05\x04\x06\x02\x03\x05\x12\x03W\x04\
    \t\n\x0c\n\x05\x04\x06\x02\x03\x01\x12\x03W\n\x11\n\x0c\n\x05\x04\x06\
    \x02\x03\x03\x12\x03W\x14\x15b\x06proto3\
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
