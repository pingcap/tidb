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
pub struct FailPointRequest {
    // message fields
    pub fail_cfgs: ::protobuf::RepeatedField<FailPointCfg>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for FailPointRequest {}

impl FailPointRequest {
    pub fn new() -> FailPointRequest {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static FailPointRequest {
        static mut instance: ::protobuf::lazy::Lazy<FailPointRequest> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const FailPointRequest,
        };
        unsafe {
            instance.get(FailPointRequest::new)
        }
    }

    // repeated .tikvpb.FailPointCfg fail_cfgs = 1;

    pub fn clear_fail_cfgs(&mut self) {
        self.fail_cfgs.clear();
    }

    // Param is passed by value, moved
    pub fn set_fail_cfgs(&mut self, v: ::protobuf::RepeatedField<FailPointCfg>) {
        self.fail_cfgs = v;
    }

    // Mutable pointer to the field.
    pub fn mut_fail_cfgs(&mut self) -> &mut ::protobuf::RepeatedField<FailPointCfg> {
        &mut self.fail_cfgs
    }

    // Take field
    pub fn take_fail_cfgs(&mut self) -> ::protobuf::RepeatedField<FailPointCfg> {
        ::std::mem::replace(&mut self.fail_cfgs, ::protobuf::RepeatedField::new())
    }

    pub fn get_fail_cfgs(&self) -> &[FailPointCfg] {
        &self.fail_cfgs
    }

    fn get_fail_cfgs_for_reflect(&self) -> &::protobuf::RepeatedField<FailPointCfg> {
        &self.fail_cfgs
    }

    fn mut_fail_cfgs_for_reflect(&mut self) -> &mut ::protobuf::RepeatedField<FailPointCfg> {
        &mut self.fail_cfgs
    }
}

impl ::protobuf::Message for FailPointRequest {
    fn is_initialized(&self) -> bool {
        for v in &self.fail_cfgs {
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
                    ::protobuf::rt::read_repeated_message_into(wire_type, is, &mut self.fail_cfgs)?;
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
        for value in &self.fail_cfgs {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        for v in &self.fail_cfgs {
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

impl ::protobuf::MessageStatic for FailPointRequest {
    fn new() -> FailPointRequest {
        FailPointRequest::new()
    }

    fn descriptor_static(_: ::std::option::Option<FailPointRequest>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_repeated_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<FailPointCfg>>(
                    "fail_cfgs",
                    FailPointRequest::get_fail_cfgs_for_reflect,
                    FailPointRequest::mut_fail_cfgs_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<FailPointRequest>(
                    "FailPointRequest",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for FailPointRequest {
    fn clear(&mut self) {
        self.clear_fail_cfgs();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for FailPointRequest {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for FailPointRequest {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct FailPointCfg {
    // message fields
    pub name: ::std::string::String,
    pub actions: ::std::string::String,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for FailPointCfg {}

impl FailPointCfg {
    pub fn new() -> FailPointCfg {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static FailPointCfg {
        static mut instance: ::protobuf::lazy::Lazy<FailPointCfg> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const FailPointCfg,
        };
        unsafe {
            instance.get(FailPointCfg::new)
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

    // string actions = 2;

    pub fn clear_actions(&mut self) {
        self.actions.clear();
    }

    // Param is passed by value, moved
    pub fn set_actions(&mut self, v: ::std::string::String) {
        self.actions = v;
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_actions(&mut self) -> &mut ::std::string::String {
        &mut self.actions
    }

    // Take field
    pub fn take_actions(&mut self) -> ::std::string::String {
        ::std::mem::replace(&mut self.actions, ::std::string::String::new())
    }

    pub fn get_actions(&self) -> &str {
        &self.actions
    }

    fn get_actions_for_reflect(&self) -> &::std::string::String {
        &self.actions
    }

    fn mut_actions_for_reflect(&mut self) -> &mut ::std::string::String {
        &mut self.actions
    }
}

impl ::protobuf::Message for FailPointCfg {
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
                    ::protobuf::rt::read_singular_proto3_string_into(wire_type, is, &mut self.actions)?;
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
        if !self.actions.is_empty() {
            my_size += ::protobuf::rt::string_size(2, &self.actions);
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if !self.name.is_empty() {
            os.write_string(1, &self.name)?;
        }
        if !self.actions.is_empty() {
            os.write_string(2, &self.actions)?;
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

impl ::protobuf::MessageStatic for FailPointCfg {
    fn new() -> FailPointCfg {
        FailPointCfg::new()
    }

    fn descriptor_static(_: ::std::option::Option<FailPointCfg>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeString>(
                    "name",
                    FailPointCfg::get_name_for_reflect,
                    FailPointCfg::mut_name_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeString>(
                    "actions",
                    FailPointCfg::get_actions_for_reflect,
                    FailPointCfg::mut_actions_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<FailPointCfg>(
                    "FailPointCfg",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for FailPointCfg {
    fn clear(&mut self) {
        self.clear_name();
        self.clear_actions();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for FailPointCfg {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for FailPointCfg {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct FailPointResponse {
    // message fields
    pub success: bool,
    pub error: ::std::string::String,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for FailPointResponse {}

impl FailPointResponse {
    pub fn new() -> FailPointResponse {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static FailPointResponse {
        static mut instance: ::protobuf::lazy::Lazy<FailPointResponse> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const FailPointResponse,
        };
        unsafe {
            instance.get(FailPointResponse::new)
        }
    }

    // bool success = 1;

    pub fn clear_success(&mut self) {
        self.success = false;
    }

    // Param is passed by value, moved
    pub fn set_success(&mut self, v: bool) {
        self.success = v;
    }

    pub fn get_success(&self) -> bool {
        self.success
    }

    fn get_success_for_reflect(&self) -> &bool {
        &self.success
    }

    fn mut_success_for_reflect(&mut self) -> &mut bool {
        &mut self.success
    }

    // string error = 2;

    pub fn clear_error(&mut self) {
        self.error.clear();
    }

    // Param is passed by value, moved
    pub fn set_error(&mut self, v: ::std::string::String) {
        self.error = v;
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_error(&mut self) -> &mut ::std::string::String {
        &mut self.error
    }

    // Take field
    pub fn take_error(&mut self) -> ::std::string::String {
        ::std::mem::replace(&mut self.error, ::std::string::String::new())
    }

    pub fn get_error(&self) -> &str {
        &self.error
    }

    fn get_error_for_reflect(&self) -> &::std::string::String {
        &self.error
    }

    fn mut_error_for_reflect(&mut self) -> &mut ::std::string::String {
        &mut self.error
    }
}

impl ::protobuf::Message for FailPointResponse {
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
                    let tmp = is.read_bool()?;
                    self.success = tmp;
                },
                2 => {
                    ::protobuf::rt::read_singular_proto3_string_into(wire_type, is, &mut self.error)?;
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
        if self.success != false {
            my_size += 2;
        }
        if !self.error.is_empty() {
            my_size += ::protobuf::rt::string_size(2, &self.error);
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if self.success != false {
            os.write_bool(1, self.success)?;
        }
        if !self.error.is_empty() {
            os.write_string(2, &self.error)?;
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

impl ::protobuf::MessageStatic for FailPointResponse {
    fn new() -> FailPointResponse {
        FailPointResponse::new()
    }

    fn descriptor_static(_: ::std::option::Option<FailPointResponse>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeBool>(
                    "success",
                    FailPointResponse::get_success_for_reflect,
                    FailPointResponse::mut_success_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_simple_field_accessor::<_, ::protobuf::types::ProtobufTypeString>(
                    "error",
                    FailPointResponse::get_error_for_reflect,
                    FailPointResponse::mut_error_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<FailPointResponse>(
                    "FailPointResponse",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for FailPointResponse {
    fn clear(&mut self) {
        self.clear_success();
        self.clear_error();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for FailPointResponse {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for FailPointResponse {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

static file_descriptor_proto_data: &'static [u8] = b"\
    \n\x0ctikvpb.proto\x12\x06tikvpb\x1a\x11coprocessor.proto\x1a\rkvrpcpb.p\
    roto\x1a\x13raft_serverpb.proto\x1a\x14gogoproto/gogo.proto\"E\n\x10Fail\
    PointRequest\x121\n\tfail_cfgs\x18\x01\x20\x03(\x0b2\x14.tikvpb.FailPoin\
    tCfgR\x08failCfgs\"<\n\x0cFailPointCfg\x12\x12\n\x04name\x18\x01\x20\x01\
    (\tR\x04name\x12\x18\n\x07actions\x18\x02\x20\x01(\tR\x07actions\"C\n\
    \x11FailPointResponse\x12\x18\n\x07success\x18\x01\x20\x01(\x08R\x07succ\
    ess\x12\x14\n\x05error\x18\x02\x20\x01(\tR\x05error2\x9f\x0c\n\x04Tikv\
    \x124\n\x05KvGet\x12\x13.kvrpcpb.GetRequest\x1a\x14.kvrpcpb.GetResponse\
    \"\0\x127\n\x06KvScan\x12\x14.kvrpcpb.ScanRequest\x1a\x15.kvrpcpb.ScanRe\
    sponse\"\0\x12C\n\nKvPrewrite\x12\x18.kvrpcpb.PrewriteRequest\x1a\x19.kv\
    rpcpb.PrewriteResponse\"\0\x12=\n\x08KvCommit\x12\x16.kvrpcpb.CommitRequ\
    est\x1a\x17.kvrpcpb.CommitResponse\"\0\x12=\n\x08KvImport\x12\x16.kvrpcp\
    b.ImportRequest\x1a\x17.kvrpcpb.ImportResponse\"\0\x12@\n\tKvCleanup\x12\
    \x17.kvrpcpb.CleanupRequest\x1a\x18.kvrpcpb.CleanupResponse\"\0\x12C\n\n\
    KvBatchGet\x12\x18.kvrpcpb.BatchGetRequest\x1a\x19.kvrpcpb.BatchGetRespo\
    nse\"\0\x12R\n\x0fKvBatchRollback\x12\x1d.kvrpcpb.BatchRollbackRequest\
    \x1a\x1e.kvrpcpb.BatchRollbackResponse\"\0\x12C\n\nKvScanLock\x12\x18.kv\
    rpcpb.ScanLockRequest\x1a\x19.kvrpcpb.ScanLockResponse\"\0\x12L\n\rKvRes\
    olveLock\x12\x1b.kvrpcpb.ResolveLockRequest\x1a\x1c.kvrpcpb.ResolveLockR\
    esponse\"\0\x121\n\x04KvGC\x12\x12.kvrpcpb.GCRequest\x1a\x13.kvrpcpb.GCR\
    esponse\"\0\x12L\n\rKvDeleteRange\x12\x1b.kvrpcpb.DeleteRangeRequest\x1a\
    \x1c.kvrpcpb.DeleteRangeResponse\"\0\x12;\n\x06RawGet\x12\x16.kvrpcpb.Ra\
    wGetRequest\x1a\x17.kvrpcpb.RawGetResponse\"\0\x12;\n\x06RawPut\x12\x16.\
    kvrpcpb.RawPutRequest\x1a\x17.kvrpcpb.RawPutResponse\"\0\x12D\n\tRawDele\
    te\x12\x19.kvrpcpb.RawDeleteRequest\x1a\x1a.kvrpcpb.RawDeleteResponse\"\
    \0\x12>\n\x07RawScan\x12\x17.kvrpcpb.RawScanRequest\x1a\x18.kvrpcpb.RawS\
    canResponse\"\0\x12<\n\x0bCoprocessor\x12\x14.coprocessor.Request\x1a\
    \x15.coprocessor.Response\"\0\x12;\n\x04Raft\x12\x1a.raft_serverpb.RaftM\
    essage\x1a\x13.raft_serverpb.Done\"\0(\x01\x12A\n\x08Snapshot\x12\x1c.ra\
    ft_serverpb.SnapshotChunk\x1a\x13.raft_serverpb.Done\"\0(\x01\x12J\n\x0b\
    SplitRegion\x12\x1b.kvrpcpb.SplitRegionRequest\x1a\x1c.kvrpcpb.SplitRegi\
    onResponse\"\0\x12M\n\x0cMvccGetByKey\x12\x1c.kvrpcpb.MvccGetByKeyReques\
    t\x1a\x1d.kvrpcpb.MvccGetByKeyResponse\"\0\x12Y\n\x10MvccGetByStartTs\
    \x12\x20.kvrpcpb.MvccGetByStartTsRequest\x1a!.kvrpcpb.MvccGetByStartTsRe\
    sponse\"\0\x12B\n\tFailPoint\x12\x18.tikvpb.FailPointRequest\x1a\x19.tik\
    vpb.FailPointResponse\"\0B&\n\x18com.pingcap.tikv.kvproto\xe0\xe2\x1e\
    \x01\xc8\xe2\x1e\x01\xd0\xe2\x1e\x01J\xaf\x12\n\x06\x12\x04\0\0B\x01\n\
    \x08\n\x01\x0c\x12\x03\0\0\x12\n\x08\n\x01\x02\x12\x03\x01\x08\x0e\n\t\n\
    \x02\x03\0\x12\x03\x03\x07\x1a\n\t\n\x02\x03\x01\x12\x03\x04\x07\x16\n\t\
    \n\x02\x03\x02\x12\x03\x05\x07\x1c\n\t\n\x02\x03\x03\x12\x03\x07\x07\x1d\
    \n\x08\n\x01\x08\x12\x03\t\0$\n\x0b\n\x04\x08\xe7\x07\0\x12\x03\t\0$\n\
    \x0c\n\x05\x08\xe7\x07\0\x02\x12\x03\t\x07\x1c\n\r\n\x06\x08\xe7\x07\0\
    \x02\0\x12\x03\t\x07\x1c\n\x0e\n\x07\x08\xe7\x07\0\x02\0\x01\x12\x03\t\
    \x08\x1b\n\x0c\n\x05\x08\xe7\x07\0\x03\x12\x03\t\x1f#\n\x08\n\x01\x08\
    \x12\x03\n\0(\n\x0b\n\x04\x08\xe7\x07\x01\x12\x03\n\0(\n\x0c\n\x05\x08\
    \xe7\x07\x01\x02\x12\x03\n\x07\x20\n\r\n\x06\x08\xe7\x07\x01\x02\0\x12\
    \x03\n\x07\x20\n\x0e\n\x07\x08\xe7\x07\x01\x02\0\x01\x12\x03\n\x08\x1f\n\
    \x0c\n\x05\x08\xe7\x07\x01\x03\x12\x03\n#'\n\x08\n\x01\x08\x12\x03\x0b\0\
    *\n\x0b\n\x04\x08\xe7\x07\x02\x12\x03\x0b\0*\n\x0c\n\x05\x08\xe7\x07\x02\
    \x02\x12\x03\x0b\x07\"\n\r\n\x06\x08\xe7\x07\x02\x02\0\x12\x03\x0b\x07\"\
    \n\x0e\n\x07\x08\xe7\x07\x02\x02\0\x01\x12\x03\x0b\x08!\n\x0c\n\x05\x08\
    \xe7\x07\x02\x03\x12\x03\x0b%)\n\x08\n\x01\x08\x12\x03\r\01\n\x0b\n\x04\
    \x08\xe7\x07\x03\x12\x03\r\01\n\x0c\n\x05\x08\xe7\x07\x03\x02\x12\x03\r\
    \x07\x13\n\r\n\x06\x08\xe7\x07\x03\x02\0\x12\x03\r\x07\x13\n\x0e\n\x07\
    \x08\xe7\x07\x03\x02\0\x01\x12\x03\r\x07\x13\n\x0c\n\x05\x08\xe7\x07\x03\
    \x07\x12\x03\r\x160\n1\n\x02\x06\0\x12\x04\x10\04\x01\x1a%\x20Serve\x20a\
    s\x20a\x20distributed\x20kv\x20database.\n\n\n\n\x03\x06\0\x01\x12\x03\
    \x10\x08\x0c\n3\n\x04\x06\0\x02\0\x12\x03\x12\x04B\x1a&\x20KV\x20command\
    s\x20with\x20mvcc/txn\x20supported.\n\n\x0c\n\x05\x06\0\x02\0\x01\x12\
    \x03\x12\x08\r\n\x0c\n\x05\x06\0\x02\0\x02\x12\x03\x12\x0e\x20\n\x0c\n\
    \x05\x06\0\x02\0\x03\x12\x03\x12+>\n\x0b\n\x04\x06\0\x02\x01\x12\x03\x13\
    \x04E\n\x0c\n\x05\x06\0\x02\x01\x01\x12\x03\x13\x08\x0e\n\x0c\n\x05\x06\
    \0\x02\x01\x02\x12\x03\x13\x0f\"\n\x0c\n\x05\x06\0\x02\x01\x03\x12\x03\
    \x13-A\n\x0b\n\x04\x06\0\x02\x02\x12\x03\x14\x04Q\n\x0c\n\x05\x06\0\x02\
    \x02\x01\x12\x03\x14\x08\x12\n\x0c\n\x05\x06\0\x02\x02\x02\x12\x03\x14\
    \x13*\n\x0c\n\x05\x06\0\x02\x02\x03\x12\x03\x145M\n\x0b\n\x04\x06\0\x02\
    \x03\x12\x03\x15\x04K\n\x0c\n\x05\x06\0\x02\x03\x01\x12\x03\x15\x08\x10\
    \n\x0c\n\x05\x06\0\x02\x03\x02\x12\x03\x15\x11&\n\x0c\n\x05\x06\0\x02\
    \x03\x03\x12\x03\x151G\n\x0b\n\x04\x06\0\x02\x04\x12\x03\x16\x04K\n\x0c\
    \n\x05\x06\0\x02\x04\x01\x12\x03\x16\x08\x10\n\x0c\n\x05\x06\0\x02\x04\
    \x02\x12\x03\x16\x11&\n\x0c\n\x05\x06\0\x02\x04\x03\x12\x03\x161G\n\x0b\
    \n\x04\x06\0\x02\x05\x12\x03\x17\x04N\n\x0c\n\x05\x06\0\x02\x05\x01\x12\
    \x03\x17\x08\x11\n\x0c\n\x05\x06\0\x02\x05\x02\x12\x03\x17\x12(\n\x0c\n\
    \x05\x06\0\x02\x05\x03\x12\x03\x173J\n\x0b\n\x04\x06\0\x02\x06\x12\x03\
    \x18\x04Q\n\x0c\n\x05\x06\0\x02\x06\x01\x12\x03\x18\x08\x12\n\x0c\n\x05\
    \x06\0\x02\x06\x02\x12\x03\x18\x13*\n\x0c\n\x05\x06\0\x02\x06\x03\x12\
    \x03\x185M\n\x0b\n\x04\x06\0\x02\x07\x12\x03\x19\x04`\n\x0c\n\x05\x06\0\
    \x02\x07\x01\x12\x03\x19\x08\x17\n\x0c\n\x05\x06\0\x02\x07\x02\x12\x03\
    \x19\x184\n\x0c\n\x05\x06\0\x02\x07\x03\x12\x03\x19?\\\n\x0b\n\x04\x06\0\
    \x02\x08\x12\x03\x1a\x04Q\n\x0c\n\x05\x06\0\x02\x08\x01\x12\x03\x1a\x08\
    \x12\n\x0c\n\x05\x06\0\x02\x08\x02\x12\x03\x1a\x13*\n\x0c\n\x05\x06\0\
    \x02\x08\x03\x12\x03\x1a5M\n\x0b\n\x04\x06\0\x02\t\x12\x03\x1b\x04Z\n\
    \x0c\n\x05\x06\0\x02\t\x01\x12\x03\x1b\x08\x15\n\x0c\n\x05\x06\0\x02\t\
    \x02\x12\x03\x1b\x160\n\x0c\n\x05\x06\0\x02\t\x03\x12\x03\x1b;V\n\x0b\n\
    \x04\x06\0\x02\n\x12\x03\x1c\x04?\n\x0c\n\x05\x06\0\x02\n\x01\x12\x03\
    \x1c\x08\x0c\n\x0c\n\x05\x06\0\x02\n\x02\x12\x03\x1c\r\x1e\n\x0c\n\x05\
    \x06\0\x02\n\x03\x12\x03\x1c);\n\x0b\n\x04\x06\0\x02\x0b\x12\x03\x1d\x04\
    Z\n\x0c\n\x05\x06\0\x02\x0b\x01\x12\x03\x1d\x08\x15\n\x0c\n\x05\x06\0\
    \x02\x0b\x02\x12\x03\x1d\x160\n\x0c\n\x05\x06\0\x02\x0b\x03\x12\x03\x1d;\
    V\n\x1e\n\x04\x06\0\x02\x0c\x12\x03\x20\x04I\x1a\x11\x20RawKV\x20command\
    s.\n\n\x0c\n\x05\x06\0\x02\x0c\x01\x12\x03\x20\x08\x0e\n\x0c\n\x05\x06\0\
    \x02\x0c\x02\x12\x03\x20\x0f$\n\x0c\n\x05\x06\0\x02\x0c\x03\x12\x03\x20/\
    E\n\x0b\n\x04\x06\0\x02\r\x12\x03!\x04I\n\x0c\n\x05\x06\0\x02\r\x01\x12\
    \x03!\x08\x0e\n\x0c\n\x05\x06\0\x02\r\x02\x12\x03!\x0f$\n\x0c\n\x05\x06\
    \0\x02\r\x03\x12\x03!/E\n\x0b\n\x04\x06\0\x02\x0e\x12\x03\"\x04R\n\x0c\n\
    \x05\x06\0\x02\x0e\x01\x12\x03\"\x08\x11\n\x0c\n\x05\x06\0\x02\x0e\x02\
    \x12\x03\"\x12*\n\x0c\n\x05\x06\0\x02\x0e\x03\x12\x03\"5N\n\x0b\n\x04\
    \x06\0\x02\x0f\x12\x03#\x04L\n\x0c\n\x05\x06\0\x02\x0f\x01\x12\x03#\x08\
    \x0f\n\x0c\n\x05\x06\0\x02\x0f\x02\x12\x03#\x10&\n\x0c\n\x05\x06\0\x02\
    \x0f\x03\x12\x03#1H\n&\n\x04\x06\0\x02\x10\x12\x03&\x04J\x1a\x19\x20SQL\
    \x20push\x20down\x20commands.\n\n\x0c\n\x05\x06\0\x02\x10\x01\x12\x03&\
    \x08\x13\n\x0c\n\x05\x06\0\x02\x10\x02\x12\x03&\x14'\n\x0c\n\x05\x06\0\
    \x02\x10\x03\x12\x03&2F\n-\n\x04\x06\0\x02\x11\x12\x03)\x04N\x1a\x20\x20\
    Raft\x20commands\x20(tikv\x20<->\x20tikv).\n\n\x0c\n\x05\x06\0\x02\x11\
    \x01\x12\x03)\x08\x0c\n\x0c\n\x05\x06\0\x02\x11\x05\x12\x03)\r\x13\n\x0c\
    \n\x05\x06\0\x02\x11\x02\x12\x03)\x14-\n\x0c\n\x05\x06\0\x02\x11\x03\x12\
    \x03)8J\n\x0b\n\x04\x06\0\x02\x12\x12\x03*\x04T\n\x0c\n\x05\x06\0\x02\
    \x12\x01\x12\x03*\x08\x10\n\x0c\n\x05\x06\0\x02\x12\x05\x12\x03*\x11\x17\
    \n\x0c\n\x05\x06\0\x02\x12\x02\x12\x03*\x183\n\x0c\n\x05\x06\0\x02\x12\
    \x03\x12\x03*>P\n\x1f\n\x04\x06\0\x02\x13\x12\x03-\x04Y\x1a\x12\x20Regio\
    n\x20commands.\n\n\x0c\n\x05\x06\0\x02\x13\x01\x12\x03-\x08\x13\n\x0c\n\
    \x05\x06\0\x02\x13\x02\x12\x03-\x15/\n\x0c\n\x05\x06\0\x02\x13\x03\x12\
    \x03-:U\n-\n\x04\x06\0\x02\x14\x12\x030\x04[\x1a\x20\x20transaction\x20d\
    ebugger\x20commands.\n\n\x0c\n\x05\x06\0\x02\x14\x01\x12\x030\x08\x14\n\
    \x0c\n\x05\x06\0\x02\x14\x02\x12\x030\x150\n\x0c\n\x05\x06\0\x02\x14\x03\
    \x12\x030;W\n\x0b\n\x04\x06\0\x02\x15\x12\x031\x04g\n\x0c\n\x05\x06\0\
    \x02\x15\x01\x12\x031\x08\x18\n\x0c\n\x05\x06\0\x02\x15\x02\x12\x031\x19\
    8\n\x0c\n\x05\x06\0\x02\x15\x03\x12\x031Cc\n\x0b\n\x04\x06\0\x02\x16\x12\
    \x033\x04B\n\x0c\n\x05\x06\0\x02\x16\x01\x12\x033\x08\x11\n\x0c\n\x05\
    \x06\0\x02\x16\x02\x12\x033\x12\"\n\x0c\n\x05\x06\0\x02\x16\x03\x12\x033\
    ->\n\n\n\x02\x04\0\x12\x046\08\x01\n\n\n\x03\x04\0\x01\x12\x036\x08\x18\
    \n\x0b\n\x04\x04\0\x02\0\x12\x037\x04(\n\x0c\n\x05\x04\0\x02\0\x04\x12\
    \x037\x04\x0c\n\x0c\n\x05\x04\0\x02\0\x06\x12\x037\r\x19\n\x0c\n\x05\x04\
    \0\x02\0\x01\x12\x037\x1a#\n\x0c\n\x05\x04\0\x02\0\x03\x12\x037&'\n\n\n\
    \x02\x04\x01\x12\x04:\0=\x01\n\n\n\x03\x04\x01\x01\x12\x03:\x08\x14\n\
    \x0b\n\x04\x04\x01\x02\0\x12\x03;\x04\x17\n\r\n\x05\x04\x01\x02\0\x04\
    \x12\x04;\x04:\x16\n\x0c\n\x05\x04\x01\x02\0\x05\x12\x03;\x04\n\n\x0c\n\
    \x05\x04\x01\x02\0\x01\x12\x03;\x0b\x0f\n\x0c\n\x05\x04\x01\x02\0\x03\
    \x12\x03;\x15\x16\n\x0b\n\x04\x04\x01\x02\x01\x12\x03<\x04\x17\n\r\n\x05\
    \x04\x01\x02\x01\x04\x12\x04<\x04;\x17\n\x0c\n\x05\x04\x01\x02\x01\x05\
    \x12\x03<\x04\n\n\x0c\n\x05\x04\x01\x02\x01\x01\x12\x03<\x0b\x12\n\x0c\n\
    \x05\x04\x01\x02\x01\x03\x12\x03<\x15\x16\n\n\n\x02\x04\x02\x12\x04?\0B\
    \x01\n\n\n\x03\x04\x02\x01\x12\x03?\x08\x19\n\x0b\n\x04\x04\x02\x02\0\
    \x12\x03@\x04\x15\n\r\n\x05\x04\x02\x02\0\x04\x12\x04@\x04?\x1b\n\x0c\n\
    \x05\x04\x02\x02\0\x05\x12\x03@\x04\x08\n\x0c\n\x05\x04\x02\x02\0\x01\
    \x12\x03@\t\x10\n\x0c\n\x05\x04\x02\x02\0\x03\x12\x03@\x13\x14\n\x0b\n\
    \x04\x04\x02\x02\x01\x12\x03A\x04\x15\n\r\n\x05\x04\x02\x02\x01\x04\x12\
    \x04A\x04@\x15\n\x0c\n\x05\x04\x02\x02\x01\x05\x12\x03A\x04\n\n\x0c\n\
    \x05\x04\x02\x02\x01\x01\x12\x03A\x0b\x10\n\x0c\n\x05\x04\x02\x02\x01\
    \x03\x12\x03A\x13\x14b\x06proto3\
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
