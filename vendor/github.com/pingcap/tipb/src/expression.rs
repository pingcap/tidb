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
pub struct FieldType {
    // message fields
    tp: ::std::option::Option<i32>,
    flag: ::std::option::Option<u32>,
    flen: ::std::option::Option<i32>,
    decimal: ::std::option::Option<i32>,
    collate: ::std::option::Option<i32>,
    charset: ::protobuf::SingularField<::std::string::String>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for FieldType {}

impl FieldType {
    pub fn new() -> FieldType {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static FieldType {
        static mut instance: ::protobuf::lazy::Lazy<FieldType> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const FieldType,
        };
        unsafe {
            instance.get(FieldType::new)
        }
    }

    // optional int32 tp = 1;

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

    // optional uint32 flag = 2;

    pub fn clear_flag(&mut self) {
        self.flag = ::std::option::Option::None;
    }

    pub fn has_flag(&self) -> bool {
        self.flag.is_some()
    }

    // Param is passed by value, moved
    pub fn set_flag(&mut self, v: u32) {
        self.flag = ::std::option::Option::Some(v);
    }

    pub fn get_flag(&self) -> u32 {
        self.flag.unwrap_or(0)
    }

    fn get_flag_for_reflect(&self) -> &::std::option::Option<u32> {
        &self.flag
    }

    fn mut_flag_for_reflect(&mut self) -> &mut ::std::option::Option<u32> {
        &mut self.flag
    }

    // optional int32 flen = 3;

    pub fn clear_flen(&mut self) {
        self.flen = ::std::option::Option::None;
    }

    pub fn has_flen(&self) -> bool {
        self.flen.is_some()
    }

    // Param is passed by value, moved
    pub fn set_flen(&mut self, v: i32) {
        self.flen = ::std::option::Option::Some(v);
    }

    pub fn get_flen(&self) -> i32 {
        self.flen.unwrap_or(0)
    }

    fn get_flen_for_reflect(&self) -> &::std::option::Option<i32> {
        &self.flen
    }

    fn mut_flen_for_reflect(&mut self) -> &mut ::std::option::Option<i32> {
        &mut self.flen
    }

    // optional int32 decimal = 4;

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

    // optional int32 collate = 5;

    pub fn clear_collate(&mut self) {
        self.collate = ::std::option::Option::None;
    }

    pub fn has_collate(&self) -> bool {
        self.collate.is_some()
    }

    // Param is passed by value, moved
    pub fn set_collate(&mut self, v: i32) {
        self.collate = ::std::option::Option::Some(v);
    }

    pub fn get_collate(&self) -> i32 {
        self.collate.unwrap_or(0)
    }

    fn get_collate_for_reflect(&self) -> &::std::option::Option<i32> {
        &self.collate
    }

    fn mut_collate_for_reflect(&mut self) -> &mut ::std::option::Option<i32> {
        &mut self.collate
    }

    // optional string charset = 6;

    pub fn clear_charset(&mut self) {
        self.charset.clear();
    }

    pub fn has_charset(&self) -> bool {
        self.charset.is_some()
    }

    // Param is passed by value, moved
    pub fn set_charset(&mut self, v: ::std::string::String) {
        self.charset = ::protobuf::SingularField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_charset(&mut self) -> &mut ::std::string::String {
        if self.charset.is_none() {
            self.charset.set_default();
        }
        self.charset.as_mut().unwrap()
    }

    // Take field
    pub fn take_charset(&mut self) -> ::std::string::String {
        self.charset.take().unwrap_or_else(|| ::std::string::String::new())
    }

    pub fn get_charset(&self) -> &str {
        match self.charset.as_ref() {
            Some(v) => &v,
            None => "",
        }
    }

    fn get_charset_for_reflect(&self) -> &::protobuf::SingularField<::std::string::String> {
        &self.charset
    }

    fn mut_charset_for_reflect(&mut self) -> &mut ::protobuf::SingularField<::std::string::String> {
        &mut self.charset
    }
}

impl ::protobuf::Message for FieldType {
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
                    self.tp = ::std::option::Option::Some(tmp);
                },
                2 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_uint32()?;
                    self.flag = ::std::option::Option::Some(tmp);
                },
                3 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_int32()?;
                    self.flen = ::std::option::Option::Some(tmp);
                },
                4 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_int32()?;
                    self.decimal = ::std::option::Option::Some(tmp);
                },
                5 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_int32()?;
                    self.collate = ::std::option::Option::Some(tmp);
                },
                6 => {
                    ::protobuf::rt::read_singular_string_into(wire_type, is, &mut self.charset)?;
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
            my_size += ::protobuf::rt::value_size(1, v, ::protobuf::wire_format::WireTypeVarint);
        }
        if let Some(v) = self.flag {
            my_size += ::protobuf::rt::value_size(2, v, ::protobuf::wire_format::WireTypeVarint);
        }
        if let Some(v) = self.flen {
            my_size += ::protobuf::rt::value_size(3, v, ::protobuf::wire_format::WireTypeVarint);
        }
        if let Some(v) = self.decimal {
            my_size += ::protobuf::rt::value_size(4, v, ::protobuf::wire_format::WireTypeVarint);
        }
        if let Some(v) = self.collate {
            my_size += ::protobuf::rt::value_size(5, v, ::protobuf::wire_format::WireTypeVarint);
        }
        if let Some(ref v) = self.charset.as_ref() {
            my_size += ::protobuf::rt::string_size(6, &v);
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.tp {
            os.write_int32(1, v)?;
        }
        if let Some(v) = self.flag {
            os.write_uint32(2, v)?;
        }
        if let Some(v) = self.flen {
            os.write_int32(3, v)?;
        }
        if let Some(v) = self.decimal {
            os.write_int32(4, v)?;
        }
        if let Some(v) = self.collate {
            os.write_int32(5, v)?;
        }
        if let Some(ref v) = self.charset.as_ref() {
            os.write_string(6, &v)?;
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

impl ::protobuf::MessageStatic for FieldType {
    fn new() -> FieldType {
        FieldType::new()
    }

    fn descriptor_static(_: ::std::option::Option<FieldType>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeInt32>(
                    "tp",
                    FieldType::get_tp_for_reflect,
                    FieldType::mut_tp_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeUint32>(
                    "flag",
                    FieldType::get_flag_for_reflect,
                    FieldType::mut_flag_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeInt32>(
                    "flen",
                    FieldType::get_flen_for_reflect,
                    FieldType::mut_flen_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeInt32>(
                    "decimal",
                    FieldType::get_decimal_for_reflect,
                    FieldType::mut_decimal_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeInt32>(
                    "collate",
                    FieldType::get_collate_for_reflect,
                    FieldType::mut_collate_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_field_accessor::<_, ::protobuf::types::ProtobufTypeString>(
                    "charset",
                    FieldType::get_charset_for_reflect,
                    FieldType::mut_charset_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<FieldType>(
                    "FieldType",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for FieldType {
    fn clear(&mut self) {
        self.clear_tp();
        self.clear_flag();
        self.clear_flen();
        self.clear_decimal();
        self.clear_collate();
        self.clear_charset();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for FieldType {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for FieldType {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct Expr {
    // message fields
    tp: ::std::option::Option<ExprType>,
    val: ::protobuf::SingularField<::std::vec::Vec<u8>>,
    children: ::protobuf::RepeatedField<Expr>,
    sig: ::std::option::Option<ScalarFuncSig>,
    field_type: ::protobuf::SingularPtrField<FieldType>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for Expr {}

impl Expr {
    pub fn new() -> Expr {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static Expr {
        static mut instance: ::protobuf::lazy::Lazy<Expr> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const Expr,
        };
        unsafe {
            instance.get(Expr::new)
        }
    }

    // optional .tipb.ExprType tp = 1;

    pub fn clear_tp(&mut self) {
        self.tp = ::std::option::Option::None;
    }

    pub fn has_tp(&self) -> bool {
        self.tp.is_some()
    }

    // Param is passed by value, moved
    pub fn set_tp(&mut self, v: ExprType) {
        self.tp = ::std::option::Option::Some(v);
    }

    pub fn get_tp(&self) -> ExprType {
        self.tp.unwrap_or(ExprType::Null)
    }

    fn get_tp_for_reflect(&self) -> &::std::option::Option<ExprType> {
        &self.tp
    }

    fn mut_tp_for_reflect(&mut self) -> &mut ::std::option::Option<ExprType> {
        &mut self.tp
    }

    // optional bytes val = 2;

    pub fn clear_val(&mut self) {
        self.val.clear();
    }

    pub fn has_val(&self) -> bool {
        self.val.is_some()
    }

    // Param is passed by value, moved
    pub fn set_val(&mut self, v: ::std::vec::Vec<u8>) {
        self.val = ::protobuf::SingularField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_val(&mut self) -> &mut ::std::vec::Vec<u8> {
        if self.val.is_none() {
            self.val.set_default();
        }
        self.val.as_mut().unwrap()
    }

    // Take field
    pub fn take_val(&mut self) -> ::std::vec::Vec<u8> {
        self.val.take().unwrap_or_else(|| ::std::vec::Vec::new())
    }

    pub fn get_val(&self) -> &[u8] {
        match self.val.as_ref() {
            Some(v) => &v,
            None => &[],
        }
    }

    fn get_val_for_reflect(&self) -> &::protobuf::SingularField<::std::vec::Vec<u8>> {
        &self.val
    }

    fn mut_val_for_reflect(&mut self) -> &mut ::protobuf::SingularField<::std::vec::Vec<u8>> {
        &mut self.val
    }

    // repeated .tipb.Expr children = 3;

    pub fn clear_children(&mut self) {
        self.children.clear();
    }

    // Param is passed by value, moved
    pub fn set_children(&mut self, v: ::protobuf::RepeatedField<Expr>) {
        self.children = v;
    }

    // Mutable pointer to the field.
    pub fn mut_children(&mut self) -> &mut ::protobuf::RepeatedField<Expr> {
        &mut self.children
    }

    // Take field
    pub fn take_children(&mut self) -> ::protobuf::RepeatedField<Expr> {
        ::std::mem::replace(&mut self.children, ::protobuf::RepeatedField::new())
    }

    pub fn get_children(&self) -> &[Expr] {
        &self.children
    }

    fn get_children_for_reflect(&self) -> &::protobuf::RepeatedField<Expr> {
        &self.children
    }

    fn mut_children_for_reflect(&mut self) -> &mut ::protobuf::RepeatedField<Expr> {
        &mut self.children
    }

    // optional .tipb.ScalarFuncSig sig = 4;

    pub fn clear_sig(&mut self) {
        self.sig = ::std::option::Option::None;
    }

    pub fn has_sig(&self) -> bool {
        self.sig.is_some()
    }

    // Param is passed by value, moved
    pub fn set_sig(&mut self, v: ScalarFuncSig) {
        self.sig = ::std::option::Option::Some(v);
    }

    pub fn get_sig(&self) -> ScalarFuncSig {
        self.sig.unwrap_or(ScalarFuncSig::CastIntAsInt)
    }

    fn get_sig_for_reflect(&self) -> &::std::option::Option<ScalarFuncSig> {
        &self.sig
    }

    fn mut_sig_for_reflect(&mut self) -> &mut ::std::option::Option<ScalarFuncSig> {
        &mut self.sig
    }

    // optional .tipb.FieldType field_type = 5;

    pub fn clear_field_type(&mut self) {
        self.field_type.clear();
    }

    pub fn has_field_type(&self) -> bool {
        self.field_type.is_some()
    }

    // Param is passed by value, moved
    pub fn set_field_type(&mut self, v: FieldType) {
        self.field_type = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_field_type(&mut self) -> &mut FieldType {
        if self.field_type.is_none() {
            self.field_type.set_default();
        }
        self.field_type.as_mut().unwrap()
    }

    // Take field
    pub fn take_field_type(&mut self) -> FieldType {
        self.field_type.take().unwrap_or_else(|| FieldType::new())
    }

    pub fn get_field_type(&self) -> &FieldType {
        self.field_type.as_ref().unwrap_or_else(|| FieldType::default_instance())
    }

    fn get_field_type_for_reflect(&self) -> &::protobuf::SingularPtrField<FieldType> {
        &self.field_type
    }

    fn mut_field_type_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<FieldType> {
        &mut self.field_type
    }
}

impl ::protobuf::Message for Expr {
    fn is_initialized(&self) -> bool {
        for v in &self.children {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.field_type {
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
                    ::protobuf::rt::read_singular_bytes_into(wire_type, is, &mut self.val)?;
                },
                3 => {
                    ::protobuf::rt::read_repeated_message_into(wire_type, is, &mut self.children)?;
                },
                4 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_enum()?;
                    self.sig = ::std::option::Option::Some(tmp);
                },
                5 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.field_type)?;
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
        if let Some(ref v) = self.val.as_ref() {
            my_size += ::protobuf::rt::bytes_size(2, &v);
        }
        for value in &self.children {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        if let Some(v) = self.sig {
            my_size += ::protobuf::rt::enum_size(4, v);
        }
        if let Some(ref v) = self.field_type.as_ref() {
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
        if let Some(ref v) = self.val.as_ref() {
            os.write_bytes(2, &v)?;
        }
        for v in &self.children {
            os.write_tag(3, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        };
        if let Some(v) = self.sig {
            os.write_enum(4, v.value())?;
        }
        if let Some(ref v) = self.field_type.as_ref() {
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

impl ::protobuf::MessageStatic for Expr {
    fn new() -> Expr {
        Expr::new()
    }

    fn descriptor_static(_: ::std::option::Option<Expr>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeEnum<ExprType>>(
                    "tp",
                    Expr::get_tp_for_reflect,
                    Expr::mut_tp_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_field_accessor::<_, ::protobuf::types::ProtobufTypeBytes>(
                    "val",
                    Expr::get_val_for_reflect,
                    Expr::mut_val_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_repeated_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<Expr>>(
                    "children",
                    Expr::get_children_for_reflect,
                    Expr::mut_children_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeEnum<ScalarFuncSig>>(
                    "sig",
                    Expr::get_sig_for_reflect,
                    Expr::mut_sig_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<FieldType>>(
                    "field_type",
                    Expr::get_field_type_for_reflect,
                    Expr::mut_field_type_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<Expr>(
                    "Expr",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for Expr {
    fn clear(&mut self) {
        self.clear_tp();
        self.clear_val();
        self.clear_children();
        self.clear_sig();
        self.clear_field_type();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for Expr {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for Expr {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct ByItem {
    // message fields
    expr: ::protobuf::SingularPtrField<Expr>,
    desc: ::std::option::Option<bool>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for ByItem {}

impl ByItem {
    pub fn new() -> ByItem {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static ByItem {
        static mut instance: ::protobuf::lazy::Lazy<ByItem> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ByItem,
        };
        unsafe {
            instance.get(ByItem::new)
        }
    }

    // optional .tipb.Expr expr = 1;

    pub fn clear_expr(&mut self) {
        self.expr.clear();
    }

    pub fn has_expr(&self) -> bool {
        self.expr.is_some()
    }

    // Param is passed by value, moved
    pub fn set_expr(&mut self, v: Expr) {
        self.expr = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_expr(&mut self) -> &mut Expr {
        if self.expr.is_none() {
            self.expr.set_default();
        }
        self.expr.as_mut().unwrap()
    }

    // Take field
    pub fn take_expr(&mut self) -> Expr {
        self.expr.take().unwrap_or_else(|| Expr::new())
    }

    pub fn get_expr(&self) -> &Expr {
        self.expr.as_ref().unwrap_or_else(|| Expr::default_instance())
    }

    fn get_expr_for_reflect(&self) -> &::protobuf::SingularPtrField<Expr> {
        &self.expr
    }

    fn mut_expr_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<Expr> {
        &mut self.expr
    }

    // optional bool desc = 2;

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

impl ::protobuf::Message for ByItem {
    fn is_initialized(&self) -> bool {
        for v in &self.expr {
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
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.expr)?;
                },
                2 => {
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
        if let Some(ref v) = self.expr.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if let Some(v) = self.desc {
            my_size += 2;
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(ref v) = self.expr.as_ref() {
            os.write_tag(1, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if let Some(v) = self.desc {
            os.write_bool(2, v)?;
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

impl ::protobuf::MessageStatic for ByItem {
    fn new() -> ByItem {
        ByItem::new()
    }

    fn descriptor_static(_: ::std::option::Option<ByItem>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<Expr>>(
                    "expr",
                    ByItem::get_expr_for_reflect,
                    ByItem::mut_expr_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeBool>(
                    "desc",
                    ByItem::get_desc_for_reflect,
                    ByItem::mut_desc_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<ByItem>(
                    "ByItem",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for ByItem {
    fn clear(&mut self) {
        self.clear_expr();
        self.clear_desc();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for ByItem {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for ByItem {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(Clone,PartialEq,Eq,Debug,Hash)]
pub enum ExprType {
    Null = 0,
    Int64 = 1,
    Uint64 = 2,
    Float32 = 3,
    Float64 = 4,
    String = 5,
    Bytes = 6,
    MysqlBit = 101,
    MysqlDecimal = 102,
    MysqlDuration = 103,
    MysqlEnum = 104,
    MysqlHex = 105,
    MysqlSet = 106,
    MysqlTime = 107,
    MysqlJson = 108,
    ValueList = 151,
    ColumnRef = 201,
    Not = 1001,
    Neg = 1002,
    BitNeg = 1003,
    LT = 2001,
    LE = 2002,
    EQ = 2003,
    NE = 2004,
    GE = 2005,
    GT = 2006,
    NullEQ = 2007,
    BitAnd = 2101,
    BitOr = 2102,
    BitXor = 2103,
    LeftShift = 2104,
    RighShift = 2105,
    Plus = 2201,
    Minus = 2202,
    Mul = 2203,
    Div = 2204,
    IntDiv = 2205,
    Mod = 2206,
    And = 2301,
    Or = 2302,
    Xor = 2303,
    Count = 3001,
    Sum = 3002,
    Avg = 3003,
    Min = 3004,
    Max = 3005,
    First = 3006,
    GroupConcat = 3007,
    Agg_BitAnd = 3008,
    Agg_BitOr = 3009,
    Agg_BitXor = 3010,
    Std = 3011,
    Stddev = 3012,
    StddevPop = 3013,
    StddevSamp = 3014,
    VarPop = 3015,
    VarSamp = 3016,
    Variance = 3017,
    Abs = 3101,
    Pow = 3102,
    Round = 3103,
    Concat = 3201,
    ConcatWS = 3202,
    Left = 3203,
    Length = 3204,
    Lower = 3205,
    Repeat = 3206,
    Replace = 3207,
    Upper = 3208,
    Strcmp = 3209,
    Convert = 3210,
    Cast = 3211,
    Substring = 3212,
    SubstringIndex = 3213,
    Locate = 3214,
    Trim = 3215,
    If = 3301,
    NullIf = 3302,
    IfNull = 3303,
    Date = 3401,
    DateAdd = 3402,
    DateSub = 3403,
    Year = 3411,
    YearWeek = 3412,
    Month = 3421,
    Week = 3431,
    Weekday = 3432,
    WeekOfYear = 3433,
    Day = 3441,
    DayName = 3442,
    DayOfYear = 3443,
    DayOfMonth = 3444,
    DayOfWeek = 3445,
    Hour = 3451,
    Minute = 3452,
    Second = 3453,
    Microsecond = 3454,
    Extract = 3461,
    Coalesce = 3501,
    Greatest = 3502,
    Least = 3503,
    JsonExtract = 3601,
    JsonType = 3602,
    JsonArray = 3603,
    JsonObject = 3604,
    JsonMerge = 3605,
    JsonValid = 3606,
    JsonSet = 3607,
    JsonInsert = 3608,
    JsonReplace = 3609,
    JsonRemove = 3610,
    JsonContains = 3611,
    JsonUnquote = 3612,
    JsonContainsPath = 3613,
    In = 4001,
    IsTruth = 4002,
    IsNull = 4003,
    ExprRow = 4004,
    Like = 4005,
    RLike = 4006,
    Case = 4007,
    ScalarFunc = 10000,
}

impl ::protobuf::ProtobufEnum for ExprType {
    fn value(&self) -> i32 {
        *self as i32
    }

    fn from_i32(value: i32) -> ::std::option::Option<ExprType> {
        match value {
            0 => ::std::option::Option::Some(ExprType::Null),
            1 => ::std::option::Option::Some(ExprType::Int64),
            2 => ::std::option::Option::Some(ExprType::Uint64),
            3 => ::std::option::Option::Some(ExprType::Float32),
            4 => ::std::option::Option::Some(ExprType::Float64),
            5 => ::std::option::Option::Some(ExprType::String),
            6 => ::std::option::Option::Some(ExprType::Bytes),
            101 => ::std::option::Option::Some(ExprType::MysqlBit),
            102 => ::std::option::Option::Some(ExprType::MysqlDecimal),
            103 => ::std::option::Option::Some(ExprType::MysqlDuration),
            104 => ::std::option::Option::Some(ExprType::MysqlEnum),
            105 => ::std::option::Option::Some(ExprType::MysqlHex),
            106 => ::std::option::Option::Some(ExprType::MysqlSet),
            107 => ::std::option::Option::Some(ExprType::MysqlTime),
            108 => ::std::option::Option::Some(ExprType::MysqlJson),
            151 => ::std::option::Option::Some(ExprType::ValueList),
            201 => ::std::option::Option::Some(ExprType::ColumnRef),
            1001 => ::std::option::Option::Some(ExprType::Not),
            1002 => ::std::option::Option::Some(ExprType::Neg),
            1003 => ::std::option::Option::Some(ExprType::BitNeg),
            2001 => ::std::option::Option::Some(ExprType::LT),
            2002 => ::std::option::Option::Some(ExprType::LE),
            2003 => ::std::option::Option::Some(ExprType::EQ),
            2004 => ::std::option::Option::Some(ExprType::NE),
            2005 => ::std::option::Option::Some(ExprType::GE),
            2006 => ::std::option::Option::Some(ExprType::GT),
            2007 => ::std::option::Option::Some(ExprType::NullEQ),
            2101 => ::std::option::Option::Some(ExprType::BitAnd),
            2102 => ::std::option::Option::Some(ExprType::BitOr),
            2103 => ::std::option::Option::Some(ExprType::BitXor),
            2104 => ::std::option::Option::Some(ExprType::LeftShift),
            2105 => ::std::option::Option::Some(ExprType::RighShift),
            2201 => ::std::option::Option::Some(ExprType::Plus),
            2202 => ::std::option::Option::Some(ExprType::Minus),
            2203 => ::std::option::Option::Some(ExprType::Mul),
            2204 => ::std::option::Option::Some(ExprType::Div),
            2205 => ::std::option::Option::Some(ExprType::IntDiv),
            2206 => ::std::option::Option::Some(ExprType::Mod),
            2301 => ::std::option::Option::Some(ExprType::And),
            2302 => ::std::option::Option::Some(ExprType::Or),
            2303 => ::std::option::Option::Some(ExprType::Xor),
            3001 => ::std::option::Option::Some(ExprType::Count),
            3002 => ::std::option::Option::Some(ExprType::Sum),
            3003 => ::std::option::Option::Some(ExprType::Avg),
            3004 => ::std::option::Option::Some(ExprType::Min),
            3005 => ::std::option::Option::Some(ExprType::Max),
            3006 => ::std::option::Option::Some(ExprType::First),
            3007 => ::std::option::Option::Some(ExprType::GroupConcat),
            3008 => ::std::option::Option::Some(ExprType::Agg_BitAnd),
            3009 => ::std::option::Option::Some(ExprType::Agg_BitOr),
            3010 => ::std::option::Option::Some(ExprType::Agg_BitXor),
            3011 => ::std::option::Option::Some(ExprType::Std),
            3012 => ::std::option::Option::Some(ExprType::Stddev),
            3013 => ::std::option::Option::Some(ExprType::StddevPop),
            3014 => ::std::option::Option::Some(ExprType::StddevSamp),
            3015 => ::std::option::Option::Some(ExprType::VarPop),
            3016 => ::std::option::Option::Some(ExprType::VarSamp),
            3017 => ::std::option::Option::Some(ExprType::Variance),
            3101 => ::std::option::Option::Some(ExprType::Abs),
            3102 => ::std::option::Option::Some(ExprType::Pow),
            3103 => ::std::option::Option::Some(ExprType::Round),
            3201 => ::std::option::Option::Some(ExprType::Concat),
            3202 => ::std::option::Option::Some(ExprType::ConcatWS),
            3203 => ::std::option::Option::Some(ExprType::Left),
            3204 => ::std::option::Option::Some(ExprType::Length),
            3205 => ::std::option::Option::Some(ExprType::Lower),
            3206 => ::std::option::Option::Some(ExprType::Repeat),
            3207 => ::std::option::Option::Some(ExprType::Replace),
            3208 => ::std::option::Option::Some(ExprType::Upper),
            3209 => ::std::option::Option::Some(ExprType::Strcmp),
            3210 => ::std::option::Option::Some(ExprType::Convert),
            3211 => ::std::option::Option::Some(ExprType::Cast),
            3212 => ::std::option::Option::Some(ExprType::Substring),
            3213 => ::std::option::Option::Some(ExprType::SubstringIndex),
            3214 => ::std::option::Option::Some(ExprType::Locate),
            3215 => ::std::option::Option::Some(ExprType::Trim),
            3301 => ::std::option::Option::Some(ExprType::If),
            3302 => ::std::option::Option::Some(ExprType::NullIf),
            3303 => ::std::option::Option::Some(ExprType::IfNull),
            3401 => ::std::option::Option::Some(ExprType::Date),
            3402 => ::std::option::Option::Some(ExprType::DateAdd),
            3403 => ::std::option::Option::Some(ExprType::DateSub),
            3411 => ::std::option::Option::Some(ExprType::Year),
            3412 => ::std::option::Option::Some(ExprType::YearWeek),
            3421 => ::std::option::Option::Some(ExprType::Month),
            3431 => ::std::option::Option::Some(ExprType::Week),
            3432 => ::std::option::Option::Some(ExprType::Weekday),
            3433 => ::std::option::Option::Some(ExprType::WeekOfYear),
            3441 => ::std::option::Option::Some(ExprType::Day),
            3442 => ::std::option::Option::Some(ExprType::DayName),
            3443 => ::std::option::Option::Some(ExprType::DayOfYear),
            3444 => ::std::option::Option::Some(ExprType::DayOfMonth),
            3445 => ::std::option::Option::Some(ExprType::DayOfWeek),
            3451 => ::std::option::Option::Some(ExprType::Hour),
            3452 => ::std::option::Option::Some(ExprType::Minute),
            3453 => ::std::option::Option::Some(ExprType::Second),
            3454 => ::std::option::Option::Some(ExprType::Microsecond),
            3461 => ::std::option::Option::Some(ExprType::Extract),
            3501 => ::std::option::Option::Some(ExprType::Coalesce),
            3502 => ::std::option::Option::Some(ExprType::Greatest),
            3503 => ::std::option::Option::Some(ExprType::Least),
            3601 => ::std::option::Option::Some(ExprType::JsonExtract),
            3602 => ::std::option::Option::Some(ExprType::JsonType),
            3603 => ::std::option::Option::Some(ExprType::JsonArray),
            3604 => ::std::option::Option::Some(ExprType::JsonObject),
            3605 => ::std::option::Option::Some(ExprType::JsonMerge),
            3606 => ::std::option::Option::Some(ExprType::JsonValid),
            3607 => ::std::option::Option::Some(ExprType::JsonSet),
            3608 => ::std::option::Option::Some(ExprType::JsonInsert),
            3609 => ::std::option::Option::Some(ExprType::JsonReplace),
            3610 => ::std::option::Option::Some(ExprType::JsonRemove),
            3611 => ::std::option::Option::Some(ExprType::JsonContains),
            3612 => ::std::option::Option::Some(ExprType::JsonUnquote),
            3613 => ::std::option::Option::Some(ExprType::JsonContainsPath),
            4001 => ::std::option::Option::Some(ExprType::In),
            4002 => ::std::option::Option::Some(ExprType::IsTruth),
            4003 => ::std::option::Option::Some(ExprType::IsNull),
            4004 => ::std::option::Option::Some(ExprType::ExprRow),
            4005 => ::std::option::Option::Some(ExprType::Like),
            4006 => ::std::option::Option::Some(ExprType::RLike),
            4007 => ::std::option::Option::Some(ExprType::Case),
            10000 => ::std::option::Option::Some(ExprType::ScalarFunc),
            _ => ::std::option::Option::None
        }
    }

    fn values() -> &'static [Self] {
        static values: &'static [ExprType] = &[
            ExprType::Null,
            ExprType::Int64,
            ExprType::Uint64,
            ExprType::Float32,
            ExprType::Float64,
            ExprType::String,
            ExprType::Bytes,
            ExprType::MysqlBit,
            ExprType::MysqlDecimal,
            ExprType::MysqlDuration,
            ExprType::MysqlEnum,
            ExprType::MysqlHex,
            ExprType::MysqlSet,
            ExprType::MysqlTime,
            ExprType::MysqlJson,
            ExprType::ValueList,
            ExprType::ColumnRef,
            ExprType::Not,
            ExprType::Neg,
            ExprType::BitNeg,
            ExprType::LT,
            ExprType::LE,
            ExprType::EQ,
            ExprType::NE,
            ExprType::GE,
            ExprType::GT,
            ExprType::NullEQ,
            ExprType::BitAnd,
            ExprType::BitOr,
            ExprType::BitXor,
            ExprType::LeftShift,
            ExprType::RighShift,
            ExprType::Plus,
            ExprType::Minus,
            ExprType::Mul,
            ExprType::Div,
            ExprType::IntDiv,
            ExprType::Mod,
            ExprType::And,
            ExprType::Or,
            ExprType::Xor,
            ExprType::Count,
            ExprType::Sum,
            ExprType::Avg,
            ExprType::Min,
            ExprType::Max,
            ExprType::First,
            ExprType::GroupConcat,
            ExprType::Agg_BitAnd,
            ExprType::Agg_BitOr,
            ExprType::Agg_BitXor,
            ExprType::Std,
            ExprType::Stddev,
            ExprType::StddevPop,
            ExprType::StddevSamp,
            ExprType::VarPop,
            ExprType::VarSamp,
            ExprType::Variance,
            ExprType::Abs,
            ExprType::Pow,
            ExprType::Round,
            ExprType::Concat,
            ExprType::ConcatWS,
            ExprType::Left,
            ExprType::Length,
            ExprType::Lower,
            ExprType::Repeat,
            ExprType::Replace,
            ExprType::Upper,
            ExprType::Strcmp,
            ExprType::Convert,
            ExprType::Cast,
            ExprType::Substring,
            ExprType::SubstringIndex,
            ExprType::Locate,
            ExprType::Trim,
            ExprType::If,
            ExprType::NullIf,
            ExprType::IfNull,
            ExprType::Date,
            ExprType::DateAdd,
            ExprType::DateSub,
            ExprType::Year,
            ExprType::YearWeek,
            ExprType::Month,
            ExprType::Week,
            ExprType::Weekday,
            ExprType::WeekOfYear,
            ExprType::Day,
            ExprType::DayName,
            ExprType::DayOfYear,
            ExprType::DayOfMonth,
            ExprType::DayOfWeek,
            ExprType::Hour,
            ExprType::Minute,
            ExprType::Second,
            ExprType::Microsecond,
            ExprType::Extract,
            ExprType::Coalesce,
            ExprType::Greatest,
            ExprType::Least,
            ExprType::JsonExtract,
            ExprType::JsonType,
            ExprType::JsonArray,
            ExprType::JsonObject,
            ExprType::JsonMerge,
            ExprType::JsonValid,
            ExprType::JsonSet,
            ExprType::JsonInsert,
            ExprType::JsonReplace,
            ExprType::JsonRemove,
            ExprType::JsonContains,
            ExprType::JsonUnquote,
            ExprType::JsonContainsPath,
            ExprType::In,
            ExprType::IsTruth,
            ExprType::IsNull,
            ExprType::ExprRow,
            ExprType::Like,
            ExprType::RLike,
            ExprType::Case,
            ExprType::ScalarFunc,
        ];
        values
    }

    fn enum_descriptor_static(_: ::std::option::Option<ExprType>) -> &'static ::protobuf::reflect::EnumDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::EnumDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::EnumDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                ::protobuf::reflect::EnumDescriptor::new("ExprType", file_descriptor_proto())
            })
        }
    }
}

impl ::std::marker::Copy for ExprType {
}

impl ::protobuf::reflect::ProtobufValue for ExprType {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Enum(self.descriptor())
    }
}

#[derive(Clone,PartialEq,Eq,Debug,Hash)]
pub enum ScalarFuncSig {
    CastIntAsInt = 0,
    CastIntAsReal = 1,
    CastIntAsString = 2,
    CastIntAsDecimal = 3,
    CastIntAsTime = 4,
    CastIntAsDuration = 5,
    CastIntAsJson = 6,
    CastRealAsInt = 10,
    CastRealAsReal = 11,
    CastRealAsString = 12,
    CastRealAsDecimal = 13,
    CastRealAsTime = 14,
    CastRealAsDuration = 15,
    CastRealAsJson = 16,
    CastDecimalAsInt = 20,
    CastDecimalAsReal = 21,
    CastDecimalAsString = 22,
    CastDecimalAsDecimal = 23,
    CastDecimalAsTime = 24,
    CastDecimalAsDuration = 25,
    CastDecimalAsJson = 26,
    CastStringAsInt = 30,
    CastStringAsReal = 31,
    CastStringAsString = 32,
    CastStringAsDecimal = 33,
    CastStringAsTime = 34,
    CastStringAsDuration = 35,
    CastStringAsJson = 36,
    CastTimeAsInt = 40,
    CastTimeAsReal = 41,
    CastTimeAsString = 42,
    CastTimeAsDecimal = 43,
    CastTimeAsTime = 44,
    CastTimeAsDuration = 45,
    CastTimeAsJson = 46,
    CastDurationAsInt = 50,
    CastDurationAsReal = 51,
    CastDurationAsString = 52,
    CastDurationAsDecimal = 53,
    CastDurationAsTime = 54,
    CastDurationAsDuration = 55,
    CastDurationAsJson = 56,
    CastJsonAsInt = 60,
    CastJsonAsReal = 61,
    CastJsonAsString = 62,
    CastJsonAsDecimal = 63,
    CastJsonAsTime = 64,
    CastJsonAsDuration = 65,
    CastJsonAsJson = 66,
    LTInt = 100,
    LTReal = 101,
    LTDecimal = 102,
    LTString = 103,
    LTTime = 104,
    LTDuration = 105,
    LTJson = 106,
    LEInt = 110,
    LEReal = 111,
    LEDecimal = 112,
    LEString = 113,
    LETime = 114,
    LEDuration = 115,
    LEJson = 116,
    GTInt = 120,
    GTReal = 121,
    GTDecimal = 122,
    GTString = 123,
    GTTime = 124,
    GTDuration = 125,
    GTJson = 126,
    GEInt = 130,
    GEReal = 131,
    GEDecimal = 132,
    GEString = 133,
    GETime = 134,
    GEDuration = 135,
    GEJson = 136,
    EQInt = 140,
    EQReal = 141,
    EQDecimal = 142,
    EQString = 143,
    EQTime = 144,
    EQDuration = 145,
    EQJson = 146,
    NEInt = 150,
    NEReal = 151,
    NEDecimal = 152,
    NEString = 153,
    NETime = 154,
    NEDuration = 155,
    NEJson = 156,
    NullEQInt = 160,
    NullEQReal = 161,
    NullEQDecimal = 162,
    NullEQString = 163,
    NullEQTime = 164,
    NullEQDuration = 165,
    NullEQJson = 166,
    PlusReal = 200,
    PlusDecimal = 201,
    PlusInt = 203,
    MinusReal = 204,
    MinusDecimal = 205,
    MinusInt = 207,
    MultiplyReal = 208,
    MultiplyDecimal = 209,
    MultiplyInt = 210,
    DivideReal = 211,
    DivideDecimal = 212,
    AbsInt = 2101,
    AbsUInt = 2102,
    AbsReal = 2103,
    AbsDecimal = 2104,
    CeilIntToDec = 2105,
    CeilIntToInt = 2106,
    CeilDecToInt = 2107,
    CeilDecToDec = 2108,
    CeilReal = 2109,
    FloorIntToDec = 2110,
    FloorIntToInt = 2111,
    FloorDecToInt = 2112,
    FloorDecToDec = 2113,
    FloorReal = 2114,
    LogicalAnd = 3101,
    LogicalOr = 3102,
    LogicalXor = 3103,
    UnaryNot = 3104,
    UnaryMinusInt = 3108,
    UnaryMinusReal = 3109,
    UnaryMinusDecimal = 3110,
    DecimalIsNull = 3111,
    DurationIsNull = 3112,
    RealIsNull = 3113,
    StringIsNull = 3114,
    TimeIsNull = 3115,
    IntIsNull = 3116,
    JsonIsNull = 3117,
    BitAndSig = 3118,
    BitOrSig = 3119,
    BitXorSig = 3120,
    BitNegSig = 3121,
    IntIsTrue = 3122,
    RealIsTrue = 3123,
    DecimalIsTrue = 3124,
    IntIsFalse = 3125,
    RealIsFalse = 3126,
    DecimalIsFalse = 3127,
    InInt = 4001,
    InReal = 4002,
    InDecimal = 4003,
    InString = 4004,
    InTime = 4005,
    InDuration = 4006,
    InJson = 4007,
    IfNullInt = 4101,
    IfNullReal = 4102,
    IfNullDecimal = 4103,
    IfNullString = 4104,
    IfNullTime = 4105,
    IfNullDuration = 4106,
    IfInt = 4107,
    IfReal = 4108,
    IfDecimal = 4109,
    IfString = 4110,
    IfTime = 4111,
    IfDuration = 4112,
    IfNullJson = 4113,
    IfJson = 4114,
    CoalesceInt = 4201,
    CoalesceReal = 4202,
    CoalesceDecimal = 4203,
    CoalesceString = 4204,
    CoalesceTime = 4205,
    CoalesceDuration = 4206,
    CoalesceJson = 4207,
    CaseWhenInt = 4208,
    CaseWhenReal = 4209,
    CaseWhenDecimal = 4210,
    CaseWhenString = 4211,
    CaseWhenTime = 4212,
    CaseWhenDuration = 4213,
    CaseWhenJson = 4214,
    LikeSig = 4310,
    JsonExtractSig = 5001,
    JsonUnquoteSig = 5002,
    JsonTypeSig = 5003,
    JsonSetSig = 5004,
    JsonInsertSig = 5005,
    JsonReplaceSig = 5006,
    JsonRemoveSig = 5007,
    JsonMergeSig = 5008,
    JsonObjectSig = 5009,
    JsonArraySig = 5010,
}

impl ::protobuf::ProtobufEnum for ScalarFuncSig {
    fn value(&self) -> i32 {
        *self as i32
    }

    fn from_i32(value: i32) -> ::std::option::Option<ScalarFuncSig> {
        match value {
            0 => ::std::option::Option::Some(ScalarFuncSig::CastIntAsInt),
            1 => ::std::option::Option::Some(ScalarFuncSig::CastIntAsReal),
            2 => ::std::option::Option::Some(ScalarFuncSig::CastIntAsString),
            3 => ::std::option::Option::Some(ScalarFuncSig::CastIntAsDecimal),
            4 => ::std::option::Option::Some(ScalarFuncSig::CastIntAsTime),
            5 => ::std::option::Option::Some(ScalarFuncSig::CastIntAsDuration),
            6 => ::std::option::Option::Some(ScalarFuncSig::CastIntAsJson),
            10 => ::std::option::Option::Some(ScalarFuncSig::CastRealAsInt),
            11 => ::std::option::Option::Some(ScalarFuncSig::CastRealAsReal),
            12 => ::std::option::Option::Some(ScalarFuncSig::CastRealAsString),
            13 => ::std::option::Option::Some(ScalarFuncSig::CastRealAsDecimal),
            14 => ::std::option::Option::Some(ScalarFuncSig::CastRealAsTime),
            15 => ::std::option::Option::Some(ScalarFuncSig::CastRealAsDuration),
            16 => ::std::option::Option::Some(ScalarFuncSig::CastRealAsJson),
            20 => ::std::option::Option::Some(ScalarFuncSig::CastDecimalAsInt),
            21 => ::std::option::Option::Some(ScalarFuncSig::CastDecimalAsReal),
            22 => ::std::option::Option::Some(ScalarFuncSig::CastDecimalAsString),
            23 => ::std::option::Option::Some(ScalarFuncSig::CastDecimalAsDecimal),
            24 => ::std::option::Option::Some(ScalarFuncSig::CastDecimalAsTime),
            25 => ::std::option::Option::Some(ScalarFuncSig::CastDecimalAsDuration),
            26 => ::std::option::Option::Some(ScalarFuncSig::CastDecimalAsJson),
            30 => ::std::option::Option::Some(ScalarFuncSig::CastStringAsInt),
            31 => ::std::option::Option::Some(ScalarFuncSig::CastStringAsReal),
            32 => ::std::option::Option::Some(ScalarFuncSig::CastStringAsString),
            33 => ::std::option::Option::Some(ScalarFuncSig::CastStringAsDecimal),
            34 => ::std::option::Option::Some(ScalarFuncSig::CastStringAsTime),
            35 => ::std::option::Option::Some(ScalarFuncSig::CastStringAsDuration),
            36 => ::std::option::Option::Some(ScalarFuncSig::CastStringAsJson),
            40 => ::std::option::Option::Some(ScalarFuncSig::CastTimeAsInt),
            41 => ::std::option::Option::Some(ScalarFuncSig::CastTimeAsReal),
            42 => ::std::option::Option::Some(ScalarFuncSig::CastTimeAsString),
            43 => ::std::option::Option::Some(ScalarFuncSig::CastTimeAsDecimal),
            44 => ::std::option::Option::Some(ScalarFuncSig::CastTimeAsTime),
            45 => ::std::option::Option::Some(ScalarFuncSig::CastTimeAsDuration),
            46 => ::std::option::Option::Some(ScalarFuncSig::CastTimeAsJson),
            50 => ::std::option::Option::Some(ScalarFuncSig::CastDurationAsInt),
            51 => ::std::option::Option::Some(ScalarFuncSig::CastDurationAsReal),
            52 => ::std::option::Option::Some(ScalarFuncSig::CastDurationAsString),
            53 => ::std::option::Option::Some(ScalarFuncSig::CastDurationAsDecimal),
            54 => ::std::option::Option::Some(ScalarFuncSig::CastDurationAsTime),
            55 => ::std::option::Option::Some(ScalarFuncSig::CastDurationAsDuration),
            56 => ::std::option::Option::Some(ScalarFuncSig::CastDurationAsJson),
            60 => ::std::option::Option::Some(ScalarFuncSig::CastJsonAsInt),
            61 => ::std::option::Option::Some(ScalarFuncSig::CastJsonAsReal),
            62 => ::std::option::Option::Some(ScalarFuncSig::CastJsonAsString),
            63 => ::std::option::Option::Some(ScalarFuncSig::CastJsonAsDecimal),
            64 => ::std::option::Option::Some(ScalarFuncSig::CastJsonAsTime),
            65 => ::std::option::Option::Some(ScalarFuncSig::CastJsonAsDuration),
            66 => ::std::option::Option::Some(ScalarFuncSig::CastJsonAsJson),
            100 => ::std::option::Option::Some(ScalarFuncSig::LTInt),
            101 => ::std::option::Option::Some(ScalarFuncSig::LTReal),
            102 => ::std::option::Option::Some(ScalarFuncSig::LTDecimal),
            103 => ::std::option::Option::Some(ScalarFuncSig::LTString),
            104 => ::std::option::Option::Some(ScalarFuncSig::LTTime),
            105 => ::std::option::Option::Some(ScalarFuncSig::LTDuration),
            106 => ::std::option::Option::Some(ScalarFuncSig::LTJson),
            110 => ::std::option::Option::Some(ScalarFuncSig::LEInt),
            111 => ::std::option::Option::Some(ScalarFuncSig::LEReal),
            112 => ::std::option::Option::Some(ScalarFuncSig::LEDecimal),
            113 => ::std::option::Option::Some(ScalarFuncSig::LEString),
            114 => ::std::option::Option::Some(ScalarFuncSig::LETime),
            115 => ::std::option::Option::Some(ScalarFuncSig::LEDuration),
            116 => ::std::option::Option::Some(ScalarFuncSig::LEJson),
            120 => ::std::option::Option::Some(ScalarFuncSig::GTInt),
            121 => ::std::option::Option::Some(ScalarFuncSig::GTReal),
            122 => ::std::option::Option::Some(ScalarFuncSig::GTDecimal),
            123 => ::std::option::Option::Some(ScalarFuncSig::GTString),
            124 => ::std::option::Option::Some(ScalarFuncSig::GTTime),
            125 => ::std::option::Option::Some(ScalarFuncSig::GTDuration),
            126 => ::std::option::Option::Some(ScalarFuncSig::GTJson),
            130 => ::std::option::Option::Some(ScalarFuncSig::GEInt),
            131 => ::std::option::Option::Some(ScalarFuncSig::GEReal),
            132 => ::std::option::Option::Some(ScalarFuncSig::GEDecimal),
            133 => ::std::option::Option::Some(ScalarFuncSig::GEString),
            134 => ::std::option::Option::Some(ScalarFuncSig::GETime),
            135 => ::std::option::Option::Some(ScalarFuncSig::GEDuration),
            136 => ::std::option::Option::Some(ScalarFuncSig::GEJson),
            140 => ::std::option::Option::Some(ScalarFuncSig::EQInt),
            141 => ::std::option::Option::Some(ScalarFuncSig::EQReal),
            142 => ::std::option::Option::Some(ScalarFuncSig::EQDecimal),
            143 => ::std::option::Option::Some(ScalarFuncSig::EQString),
            144 => ::std::option::Option::Some(ScalarFuncSig::EQTime),
            145 => ::std::option::Option::Some(ScalarFuncSig::EQDuration),
            146 => ::std::option::Option::Some(ScalarFuncSig::EQJson),
            150 => ::std::option::Option::Some(ScalarFuncSig::NEInt),
            151 => ::std::option::Option::Some(ScalarFuncSig::NEReal),
            152 => ::std::option::Option::Some(ScalarFuncSig::NEDecimal),
            153 => ::std::option::Option::Some(ScalarFuncSig::NEString),
            154 => ::std::option::Option::Some(ScalarFuncSig::NETime),
            155 => ::std::option::Option::Some(ScalarFuncSig::NEDuration),
            156 => ::std::option::Option::Some(ScalarFuncSig::NEJson),
            160 => ::std::option::Option::Some(ScalarFuncSig::NullEQInt),
            161 => ::std::option::Option::Some(ScalarFuncSig::NullEQReal),
            162 => ::std::option::Option::Some(ScalarFuncSig::NullEQDecimal),
            163 => ::std::option::Option::Some(ScalarFuncSig::NullEQString),
            164 => ::std::option::Option::Some(ScalarFuncSig::NullEQTime),
            165 => ::std::option::Option::Some(ScalarFuncSig::NullEQDuration),
            166 => ::std::option::Option::Some(ScalarFuncSig::NullEQJson),
            200 => ::std::option::Option::Some(ScalarFuncSig::PlusReal),
            201 => ::std::option::Option::Some(ScalarFuncSig::PlusDecimal),
            203 => ::std::option::Option::Some(ScalarFuncSig::PlusInt),
            204 => ::std::option::Option::Some(ScalarFuncSig::MinusReal),
            205 => ::std::option::Option::Some(ScalarFuncSig::MinusDecimal),
            207 => ::std::option::Option::Some(ScalarFuncSig::MinusInt),
            208 => ::std::option::Option::Some(ScalarFuncSig::MultiplyReal),
            209 => ::std::option::Option::Some(ScalarFuncSig::MultiplyDecimal),
            210 => ::std::option::Option::Some(ScalarFuncSig::MultiplyInt),
            211 => ::std::option::Option::Some(ScalarFuncSig::DivideReal),
            212 => ::std::option::Option::Some(ScalarFuncSig::DivideDecimal),
            2101 => ::std::option::Option::Some(ScalarFuncSig::AbsInt),
            2102 => ::std::option::Option::Some(ScalarFuncSig::AbsUInt),
            2103 => ::std::option::Option::Some(ScalarFuncSig::AbsReal),
            2104 => ::std::option::Option::Some(ScalarFuncSig::AbsDecimal),
            2105 => ::std::option::Option::Some(ScalarFuncSig::CeilIntToDec),
            2106 => ::std::option::Option::Some(ScalarFuncSig::CeilIntToInt),
            2107 => ::std::option::Option::Some(ScalarFuncSig::CeilDecToInt),
            2108 => ::std::option::Option::Some(ScalarFuncSig::CeilDecToDec),
            2109 => ::std::option::Option::Some(ScalarFuncSig::CeilReal),
            2110 => ::std::option::Option::Some(ScalarFuncSig::FloorIntToDec),
            2111 => ::std::option::Option::Some(ScalarFuncSig::FloorIntToInt),
            2112 => ::std::option::Option::Some(ScalarFuncSig::FloorDecToInt),
            2113 => ::std::option::Option::Some(ScalarFuncSig::FloorDecToDec),
            2114 => ::std::option::Option::Some(ScalarFuncSig::FloorReal),
            3101 => ::std::option::Option::Some(ScalarFuncSig::LogicalAnd),
            3102 => ::std::option::Option::Some(ScalarFuncSig::LogicalOr),
            3103 => ::std::option::Option::Some(ScalarFuncSig::LogicalXor),
            3104 => ::std::option::Option::Some(ScalarFuncSig::UnaryNot),
            3108 => ::std::option::Option::Some(ScalarFuncSig::UnaryMinusInt),
            3109 => ::std::option::Option::Some(ScalarFuncSig::UnaryMinusReal),
            3110 => ::std::option::Option::Some(ScalarFuncSig::UnaryMinusDecimal),
            3111 => ::std::option::Option::Some(ScalarFuncSig::DecimalIsNull),
            3112 => ::std::option::Option::Some(ScalarFuncSig::DurationIsNull),
            3113 => ::std::option::Option::Some(ScalarFuncSig::RealIsNull),
            3114 => ::std::option::Option::Some(ScalarFuncSig::StringIsNull),
            3115 => ::std::option::Option::Some(ScalarFuncSig::TimeIsNull),
            3116 => ::std::option::Option::Some(ScalarFuncSig::IntIsNull),
            3117 => ::std::option::Option::Some(ScalarFuncSig::JsonIsNull),
            3118 => ::std::option::Option::Some(ScalarFuncSig::BitAndSig),
            3119 => ::std::option::Option::Some(ScalarFuncSig::BitOrSig),
            3120 => ::std::option::Option::Some(ScalarFuncSig::BitXorSig),
            3121 => ::std::option::Option::Some(ScalarFuncSig::BitNegSig),
            3122 => ::std::option::Option::Some(ScalarFuncSig::IntIsTrue),
            3123 => ::std::option::Option::Some(ScalarFuncSig::RealIsTrue),
            3124 => ::std::option::Option::Some(ScalarFuncSig::DecimalIsTrue),
            3125 => ::std::option::Option::Some(ScalarFuncSig::IntIsFalse),
            3126 => ::std::option::Option::Some(ScalarFuncSig::RealIsFalse),
            3127 => ::std::option::Option::Some(ScalarFuncSig::DecimalIsFalse),
            4001 => ::std::option::Option::Some(ScalarFuncSig::InInt),
            4002 => ::std::option::Option::Some(ScalarFuncSig::InReal),
            4003 => ::std::option::Option::Some(ScalarFuncSig::InDecimal),
            4004 => ::std::option::Option::Some(ScalarFuncSig::InString),
            4005 => ::std::option::Option::Some(ScalarFuncSig::InTime),
            4006 => ::std::option::Option::Some(ScalarFuncSig::InDuration),
            4007 => ::std::option::Option::Some(ScalarFuncSig::InJson),
            4101 => ::std::option::Option::Some(ScalarFuncSig::IfNullInt),
            4102 => ::std::option::Option::Some(ScalarFuncSig::IfNullReal),
            4103 => ::std::option::Option::Some(ScalarFuncSig::IfNullDecimal),
            4104 => ::std::option::Option::Some(ScalarFuncSig::IfNullString),
            4105 => ::std::option::Option::Some(ScalarFuncSig::IfNullTime),
            4106 => ::std::option::Option::Some(ScalarFuncSig::IfNullDuration),
            4107 => ::std::option::Option::Some(ScalarFuncSig::IfInt),
            4108 => ::std::option::Option::Some(ScalarFuncSig::IfReal),
            4109 => ::std::option::Option::Some(ScalarFuncSig::IfDecimal),
            4110 => ::std::option::Option::Some(ScalarFuncSig::IfString),
            4111 => ::std::option::Option::Some(ScalarFuncSig::IfTime),
            4112 => ::std::option::Option::Some(ScalarFuncSig::IfDuration),
            4113 => ::std::option::Option::Some(ScalarFuncSig::IfNullJson),
            4114 => ::std::option::Option::Some(ScalarFuncSig::IfJson),
            4201 => ::std::option::Option::Some(ScalarFuncSig::CoalesceInt),
            4202 => ::std::option::Option::Some(ScalarFuncSig::CoalesceReal),
            4203 => ::std::option::Option::Some(ScalarFuncSig::CoalesceDecimal),
            4204 => ::std::option::Option::Some(ScalarFuncSig::CoalesceString),
            4205 => ::std::option::Option::Some(ScalarFuncSig::CoalesceTime),
            4206 => ::std::option::Option::Some(ScalarFuncSig::CoalesceDuration),
            4207 => ::std::option::Option::Some(ScalarFuncSig::CoalesceJson),
            4208 => ::std::option::Option::Some(ScalarFuncSig::CaseWhenInt),
            4209 => ::std::option::Option::Some(ScalarFuncSig::CaseWhenReal),
            4210 => ::std::option::Option::Some(ScalarFuncSig::CaseWhenDecimal),
            4211 => ::std::option::Option::Some(ScalarFuncSig::CaseWhenString),
            4212 => ::std::option::Option::Some(ScalarFuncSig::CaseWhenTime),
            4213 => ::std::option::Option::Some(ScalarFuncSig::CaseWhenDuration),
            4214 => ::std::option::Option::Some(ScalarFuncSig::CaseWhenJson),
            4310 => ::std::option::Option::Some(ScalarFuncSig::LikeSig),
            5001 => ::std::option::Option::Some(ScalarFuncSig::JsonExtractSig),
            5002 => ::std::option::Option::Some(ScalarFuncSig::JsonUnquoteSig),
            5003 => ::std::option::Option::Some(ScalarFuncSig::JsonTypeSig),
            5004 => ::std::option::Option::Some(ScalarFuncSig::JsonSetSig),
            5005 => ::std::option::Option::Some(ScalarFuncSig::JsonInsertSig),
            5006 => ::std::option::Option::Some(ScalarFuncSig::JsonReplaceSig),
            5007 => ::std::option::Option::Some(ScalarFuncSig::JsonRemoveSig),
            5008 => ::std::option::Option::Some(ScalarFuncSig::JsonMergeSig),
            5009 => ::std::option::Option::Some(ScalarFuncSig::JsonObjectSig),
            5010 => ::std::option::Option::Some(ScalarFuncSig::JsonArraySig),
            _ => ::std::option::Option::None
        }
    }

    fn values() -> &'static [Self] {
        static values: &'static [ScalarFuncSig] = &[
            ScalarFuncSig::CastIntAsInt,
            ScalarFuncSig::CastIntAsReal,
            ScalarFuncSig::CastIntAsString,
            ScalarFuncSig::CastIntAsDecimal,
            ScalarFuncSig::CastIntAsTime,
            ScalarFuncSig::CastIntAsDuration,
            ScalarFuncSig::CastIntAsJson,
            ScalarFuncSig::CastRealAsInt,
            ScalarFuncSig::CastRealAsReal,
            ScalarFuncSig::CastRealAsString,
            ScalarFuncSig::CastRealAsDecimal,
            ScalarFuncSig::CastRealAsTime,
            ScalarFuncSig::CastRealAsDuration,
            ScalarFuncSig::CastRealAsJson,
            ScalarFuncSig::CastDecimalAsInt,
            ScalarFuncSig::CastDecimalAsReal,
            ScalarFuncSig::CastDecimalAsString,
            ScalarFuncSig::CastDecimalAsDecimal,
            ScalarFuncSig::CastDecimalAsTime,
            ScalarFuncSig::CastDecimalAsDuration,
            ScalarFuncSig::CastDecimalAsJson,
            ScalarFuncSig::CastStringAsInt,
            ScalarFuncSig::CastStringAsReal,
            ScalarFuncSig::CastStringAsString,
            ScalarFuncSig::CastStringAsDecimal,
            ScalarFuncSig::CastStringAsTime,
            ScalarFuncSig::CastStringAsDuration,
            ScalarFuncSig::CastStringAsJson,
            ScalarFuncSig::CastTimeAsInt,
            ScalarFuncSig::CastTimeAsReal,
            ScalarFuncSig::CastTimeAsString,
            ScalarFuncSig::CastTimeAsDecimal,
            ScalarFuncSig::CastTimeAsTime,
            ScalarFuncSig::CastTimeAsDuration,
            ScalarFuncSig::CastTimeAsJson,
            ScalarFuncSig::CastDurationAsInt,
            ScalarFuncSig::CastDurationAsReal,
            ScalarFuncSig::CastDurationAsString,
            ScalarFuncSig::CastDurationAsDecimal,
            ScalarFuncSig::CastDurationAsTime,
            ScalarFuncSig::CastDurationAsDuration,
            ScalarFuncSig::CastDurationAsJson,
            ScalarFuncSig::CastJsonAsInt,
            ScalarFuncSig::CastJsonAsReal,
            ScalarFuncSig::CastJsonAsString,
            ScalarFuncSig::CastJsonAsDecimal,
            ScalarFuncSig::CastJsonAsTime,
            ScalarFuncSig::CastJsonAsDuration,
            ScalarFuncSig::CastJsonAsJson,
            ScalarFuncSig::LTInt,
            ScalarFuncSig::LTReal,
            ScalarFuncSig::LTDecimal,
            ScalarFuncSig::LTString,
            ScalarFuncSig::LTTime,
            ScalarFuncSig::LTDuration,
            ScalarFuncSig::LTJson,
            ScalarFuncSig::LEInt,
            ScalarFuncSig::LEReal,
            ScalarFuncSig::LEDecimal,
            ScalarFuncSig::LEString,
            ScalarFuncSig::LETime,
            ScalarFuncSig::LEDuration,
            ScalarFuncSig::LEJson,
            ScalarFuncSig::GTInt,
            ScalarFuncSig::GTReal,
            ScalarFuncSig::GTDecimal,
            ScalarFuncSig::GTString,
            ScalarFuncSig::GTTime,
            ScalarFuncSig::GTDuration,
            ScalarFuncSig::GTJson,
            ScalarFuncSig::GEInt,
            ScalarFuncSig::GEReal,
            ScalarFuncSig::GEDecimal,
            ScalarFuncSig::GEString,
            ScalarFuncSig::GETime,
            ScalarFuncSig::GEDuration,
            ScalarFuncSig::GEJson,
            ScalarFuncSig::EQInt,
            ScalarFuncSig::EQReal,
            ScalarFuncSig::EQDecimal,
            ScalarFuncSig::EQString,
            ScalarFuncSig::EQTime,
            ScalarFuncSig::EQDuration,
            ScalarFuncSig::EQJson,
            ScalarFuncSig::NEInt,
            ScalarFuncSig::NEReal,
            ScalarFuncSig::NEDecimal,
            ScalarFuncSig::NEString,
            ScalarFuncSig::NETime,
            ScalarFuncSig::NEDuration,
            ScalarFuncSig::NEJson,
            ScalarFuncSig::NullEQInt,
            ScalarFuncSig::NullEQReal,
            ScalarFuncSig::NullEQDecimal,
            ScalarFuncSig::NullEQString,
            ScalarFuncSig::NullEQTime,
            ScalarFuncSig::NullEQDuration,
            ScalarFuncSig::NullEQJson,
            ScalarFuncSig::PlusReal,
            ScalarFuncSig::PlusDecimal,
            ScalarFuncSig::PlusInt,
            ScalarFuncSig::MinusReal,
            ScalarFuncSig::MinusDecimal,
            ScalarFuncSig::MinusInt,
            ScalarFuncSig::MultiplyReal,
            ScalarFuncSig::MultiplyDecimal,
            ScalarFuncSig::MultiplyInt,
            ScalarFuncSig::DivideReal,
            ScalarFuncSig::DivideDecimal,
            ScalarFuncSig::AbsInt,
            ScalarFuncSig::AbsUInt,
            ScalarFuncSig::AbsReal,
            ScalarFuncSig::AbsDecimal,
            ScalarFuncSig::CeilIntToDec,
            ScalarFuncSig::CeilIntToInt,
            ScalarFuncSig::CeilDecToInt,
            ScalarFuncSig::CeilDecToDec,
            ScalarFuncSig::CeilReal,
            ScalarFuncSig::FloorIntToDec,
            ScalarFuncSig::FloorIntToInt,
            ScalarFuncSig::FloorDecToInt,
            ScalarFuncSig::FloorDecToDec,
            ScalarFuncSig::FloorReal,
            ScalarFuncSig::LogicalAnd,
            ScalarFuncSig::LogicalOr,
            ScalarFuncSig::LogicalXor,
            ScalarFuncSig::UnaryNot,
            ScalarFuncSig::UnaryMinusInt,
            ScalarFuncSig::UnaryMinusReal,
            ScalarFuncSig::UnaryMinusDecimal,
            ScalarFuncSig::DecimalIsNull,
            ScalarFuncSig::DurationIsNull,
            ScalarFuncSig::RealIsNull,
            ScalarFuncSig::StringIsNull,
            ScalarFuncSig::TimeIsNull,
            ScalarFuncSig::IntIsNull,
            ScalarFuncSig::JsonIsNull,
            ScalarFuncSig::BitAndSig,
            ScalarFuncSig::BitOrSig,
            ScalarFuncSig::BitXorSig,
            ScalarFuncSig::BitNegSig,
            ScalarFuncSig::IntIsTrue,
            ScalarFuncSig::RealIsTrue,
            ScalarFuncSig::DecimalIsTrue,
            ScalarFuncSig::IntIsFalse,
            ScalarFuncSig::RealIsFalse,
            ScalarFuncSig::DecimalIsFalse,
            ScalarFuncSig::InInt,
            ScalarFuncSig::InReal,
            ScalarFuncSig::InDecimal,
            ScalarFuncSig::InString,
            ScalarFuncSig::InTime,
            ScalarFuncSig::InDuration,
            ScalarFuncSig::InJson,
            ScalarFuncSig::IfNullInt,
            ScalarFuncSig::IfNullReal,
            ScalarFuncSig::IfNullDecimal,
            ScalarFuncSig::IfNullString,
            ScalarFuncSig::IfNullTime,
            ScalarFuncSig::IfNullDuration,
            ScalarFuncSig::IfInt,
            ScalarFuncSig::IfReal,
            ScalarFuncSig::IfDecimal,
            ScalarFuncSig::IfString,
            ScalarFuncSig::IfTime,
            ScalarFuncSig::IfDuration,
            ScalarFuncSig::IfNullJson,
            ScalarFuncSig::IfJson,
            ScalarFuncSig::CoalesceInt,
            ScalarFuncSig::CoalesceReal,
            ScalarFuncSig::CoalesceDecimal,
            ScalarFuncSig::CoalesceString,
            ScalarFuncSig::CoalesceTime,
            ScalarFuncSig::CoalesceDuration,
            ScalarFuncSig::CoalesceJson,
            ScalarFuncSig::CaseWhenInt,
            ScalarFuncSig::CaseWhenReal,
            ScalarFuncSig::CaseWhenDecimal,
            ScalarFuncSig::CaseWhenString,
            ScalarFuncSig::CaseWhenTime,
            ScalarFuncSig::CaseWhenDuration,
            ScalarFuncSig::CaseWhenJson,
            ScalarFuncSig::LikeSig,
            ScalarFuncSig::JsonExtractSig,
            ScalarFuncSig::JsonUnquoteSig,
            ScalarFuncSig::JsonTypeSig,
            ScalarFuncSig::JsonSetSig,
            ScalarFuncSig::JsonInsertSig,
            ScalarFuncSig::JsonReplaceSig,
            ScalarFuncSig::JsonRemoveSig,
            ScalarFuncSig::JsonMergeSig,
            ScalarFuncSig::JsonObjectSig,
            ScalarFuncSig::JsonArraySig,
        ];
        values
    }

    fn enum_descriptor_static(_: ::std::option::Option<ScalarFuncSig>) -> &'static ::protobuf::reflect::EnumDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::EnumDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::EnumDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                ::protobuf::reflect::EnumDescriptor::new("ScalarFuncSig", file_descriptor_proto())
            })
        }
    }
}

impl ::std::marker::Copy for ScalarFuncSig {
}

impl ::protobuf::reflect::ProtobufValue for ScalarFuncSig {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Enum(self.descriptor())
    }
}

static file_descriptor_proto_data: &'static [u8] = b"\
    \n\x10expression.proto\x12\x04tipb\x1a\x14gogoproto/gogo.proto\"\xb5\x01\
    \n\tFieldType\x12\x14\n\x02tp\x18\x01\x20\x01(\x05R\x02tpB\x04\xc8\xde\
    \x1f\0\x12\x18\n\x04flag\x18\x02\x20\x01(\rR\x04flagB\x04\xc8\xde\x1f\0\
    \x12\x18\n\x04flen\x18\x03\x20\x01(\x05R\x04flenB\x04\xc8\xde\x1f\0\x12\
    \x1e\n\x07decimal\x18\x04\x20\x01(\x05R\x07decimalB\x04\xc8\xde\x1f\0\
    \x12\x1e\n\x07collate\x18\x05\x20\x01(\x05R\x07collateB\x04\xc8\xde\x1f\
    \0\x12\x1e\n\x07charset\x18\x06\x20\x01(\tR\x07charsetB\x04\xc8\xde\x1f\
    \0\"\xc3\x01\n\x04Expr\x12$\n\x02tp\x18\x01\x20\x01(\x0e2\x0e.tipb.ExprT\
    ypeR\x02tpB\x04\xc8\xde\x1f\0\x12\x10\n\x03val\x18\x02\x20\x01(\x0cR\x03\
    val\x12&\n\x08children\x18\x03\x20\x03(\x0b2\n.tipb.ExprR\x08children\
    \x12+\n\x03sig\x18\x04\x20\x01(\x0e2\x13.tipb.ScalarFuncSigR\x03sigB\x04\
    \xc8\xde\x1f\0\x12.\n\nfield_type\x18\x05\x20\x01(\x0b2\x0f.tipb.FieldTy\
    peR\tfieldType\"B\n\x06ByItem\x12\x1e\n\x04expr\x18\x01\x20\x01(\x0b2\n.\
    tipb.ExprR\x04expr\x12\x18\n\x04desc\x18\x02\x20\x01(\x08R\x04descB\x04\
    \xc8\xde\x1f\0*\xe8\x0c\n\x08ExprType\x12\x08\n\x04Null\x10\0\x12\t\n\
    \x05Int64\x10\x01\x12\n\n\x06Uint64\x10\x02\x12\x0b\n\x07Float32\x10\x03\
    \x12\x0b\n\x07Float64\x10\x04\x12\n\n\x06String\x10\x05\x12\t\n\x05Bytes\
    \x10\x06\x12\x0c\n\x08MysqlBit\x10e\x12\x10\n\x0cMysqlDecimal\x10f\x12\
    \x11\n\rMysqlDuration\x10g\x12\r\n\tMysqlEnum\x10h\x12\x0c\n\x08MysqlHex\
    \x10i\x12\x0c\n\x08MysqlSet\x10j\x12\r\n\tMysqlTime\x10k\x12\r\n\tMysqlJ\
    son\x10l\x12\x0e\n\tValueList\x10\x97\x01\x12\x0e\n\tColumnRef\x10\xc9\
    \x01\x12\x08\n\x03Not\x10\xe9\x07\x12\x08\n\x03Neg\x10\xea\x07\x12\x0b\n\
    \x06BitNeg\x10\xeb\x07\x12\x07\n\x02LT\x10\xd1\x0f\x12\x07\n\x02LE\x10\
    \xd2\x0f\x12\x07\n\x02EQ\x10\xd3\x0f\x12\x07\n\x02NE\x10\xd4\x0f\x12\x07\
    \n\x02GE\x10\xd5\x0f\x12\x07\n\x02GT\x10\xd6\x0f\x12\x0b\n\x06NullEQ\x10\
    \xd7\x0f\x12\x0b\n\x06BitAnd\x10\xb5\x10\x12\n\n\x05BitOr\x10\xb6\x10\
    \x12\x0b\n\x06BitXor\x10\xb7\x10\x12\x0e\n\tLeftShift\x10\xb8\x10\x12\
    \x0e\n\tRighShift\x10\xb9\x10\x12\t\n\x04Plus\x10\x99\x11\x12\n\n\x05Min\
    us\x10\x9a\x11\x12\x08\n\x03Mul\x10\x9b\x11\x12\x08\n\x03Div\x10\x9c\x11\
    \x12\x0b\n\x06IntDiv\x10\x9d\x11\x12\x08\n\x03Mod\x10\x9e\x11\x12\x08\n\
    \x03And\x10\xfd\x11\x12\x07\n\x02Or\x10\xfe\x11\x12\x08\n\x03Xor\x10\xff\
    \x11\x12\n\n\x05Count\x10\xb9\x17\x12\x08\n\x03Sum\x10\xba\x17\x12\x08\n\
    \x03Avg\x10\xbb\x17\x12\x08\n\x03Min\x10\xbc\x17\x12\x08\n\x03Max\x10\
    \xbd\x17\x12\n\n\x05First\x10\xbe\x17\x12\x10\n\x0bGroupConcat\x10\xbf\
    \x17\x12\x0f\n\nAgg_BitAnd\x10\xc0\x17\x12\x0e\n\tAgg_BitOr\x10\xc1\x17\
    \x12\x0f\n\nAgg_BitXor\x10\xc2\x17\x12\x08\n\x03Std\x10\xc3\x17\x12\x0b\
    \n\x06Stddev\x10\xc4\x17\x12\x0e\n\tStddevPop\x10\xc5\x17\x12\x0f\n\nStd\
    devSamp\x10\xc6\x17\x12\x0b\n\x06VarPop\x10\xc7\x17\x12\x0c\n\x07VarSamp\
    \x10\xc8\x17\x12\r\n\x08Variance\x10\xc9\x17\x12\x08\n\x03Abs\x10\x9d\
    \x18\x12\x08\n\x03Pow\x10\x9e\x18\x12\n\n\x05Round\x10\x9f\x18\x12\x0b\n\
    \x06Concat\x10\x81\x19\x12\r\n\x08ConcatWS\x10\x82\x19\x12\t\n\x04Left\
    \x10\x83\x19\x12\x0b\n\x06Length\x10\x84\x19\x12\n\n\x05Lower\x10\x85\
    \x19\x12\x0b\n\x06Repeat\x10\x86\x19\x12\x0c\n\x07Replace\x10\x87\x19\
    \x12\n\n\x05Upper\x10\x88\x19\x12\x0b\n\x06Strcmp\x10\x89\x19\x12\x0c\n\
    \x07Convert\x10\x8a\x19\x12\t\n\x04Cast\x10\x8b\x19\x12\x0e\n\tSubstring\
    \x10\x8c\x19\x12\x13\n\x0eSubstringIndex\x10\x8d\x19\x12\x0b\n\x06Locate\
    \x10\x8e\x19\x12\t\n\x04Trim\x10\x8f\x19\x12\x07\n\x02If\x10\xe5\x19\x12\
    \x0b\n\x06NullIf\x10\xe6\x19\x12\x0b\n\x06IfNull\x10\xe7\x19\x12\t\n\x04\
    Date\x10\xc9\x1a\x12\x0c\n\x07DateAdd\x10\xca\x1a\x12\x0c\n\x07DateSub\
    \x10\xcb\x1a\x12\t\n\x04Year\x10\xd3\x1a\x12\r\n\x08YearWeek\x10\xd4\x1a\
    \x12\n\n\x05Month\x10\xdd\x1a\x12\t\n\x04Week\x10\xe7\x1a\x12\x0c\n\x07W\
    eekday\x10\xe8\x1a\x12\x0f\n\nWeekOfYear\x10\xe9\x1a\x12\x08\n\x03Day\
    \x10\xf1\x1a\x12\x0c\n\x07DayName\x10\xf2\x1a\x12\x0e\n\tDayOfYear\x10\
    \xf3\x1a\x12\x0f\n\nDayOfMonth\x10\xf4\x1a\x12\x0e\n\tDayOfWeek\x10\xf5\
    \x1a\x12\t\n\x04Hour\x10\xfb\x1a\x12\x0b\n\x06Minute\x10\xfc\x1a\x12\x0b\
    \n\x06Second\x10\xfd\x1a\x12\x10\n\x0bMicrosecond\x10\xfe\x1a\x12\x0c\n\
    \x07Extract\x10\x85\x1b\x12\r\n\x08Coalesce\x10\xad\x1b\x12\r\n\x08Great\
    est\x10\xae\x1b\x12\n\n\x05Least\x10\xaf\x1b\x12\x10\n\x0bJsonExtract\
    \x10\x91\x1c\x12\r\n\x08JsonType\x10\x92\x1c\x12\x0e\n\tJsonArray\x10\
    \x93\x1c\x12\x0f\n\nJsonObject\x10\x94\x1c\x12\x0e\n\tJsonMerge\x10\x95\
    \x1c\x12\x0e\n\tJsonValid\x10\x96\x1c\x12\x0c\n\x07JsonSet\x10\x97\x1c\
    \x12\x0f\n\nJsonInsert\x10\x98\x1c\x12\x10\n\x0bJsonReplace\x10\x99\x1c\
    \x12\x0f\n\nJsonRemove\x10\x9a\x1c\x12\x11\n\x0cJsonContains\x10\x9b\x1c\
    \x12\x10\n\x0bJsonUnquote\x10\x9c\x1c\x12\x15\n\x10JsonContainsPath\x10\
    \x9d\x1c\x12\x07\n\x02In\x10\xa1\x1f\x12\x0c\n\x07IsTruth\x10\xa2\x1f\
    \x12\x0b\n\x06IsNull\x10\xa3\x1f\x12\x0c\n\x07ExprRow\x10\xa4\x1f\x12\t\
    \n\x04Like\x10\xa5\x1f\x12\n\n\x05RLike\x10\xa6\x1f\x12\t\n\x04Case\x10\
    \xa7\x1f\x12\x0f\n\nScalarFunc\x10\x90N*\xa3\x1b\n\rScalarFuncSig\x12\
    \x10\n\x0cCastIntAsInt\x10\0\x12\x11\n\rCastIntAsReal\x10\x01\x12\x13\n\
    \x0fCastIntAsString\x10\x02\x12\x14\n\x10CastIntAsDecimal\x10\x03\x12\
    \x11\n\rCastIntAsTime\x10\x04\x12\x15\n\x11CastIntAsDuration\x10\x05\x12\
    \x11\n\rCastIntAsJson\x10\x06\x12\x11\n\rCastRealAsInt\x10\n\x12\x12\n\
    \x0eCastRealAsReal\x10\x0b\x12\x14\n\x10CastRealAsString\x10\x0c\x12\x15\
    \n\x11CastRealAsDecimal\x10\r\x12\x12\n\x0eCastRealAsTime\x10\x0e\x12\
    \x16\n\x12CastRealAsDuration\x10\x0f\x12\x12\n\x0eCastRealAsJson\x10\x10\
    \x12\x14\n\x10CastDecimalAsInt\x10\x14\x12\x15\n\x11CastDecimalAsReal\
    \x10\x15\x12\x17\n\x13CastDecimalAsString\x10\x16\x12\x18\n\x14CastDecim\
    alAsDecimal\x10\x17\x12\x15\n\x11CastDecimalAsTime\x10\x18\x12\x19\n\x15\
    CastDecimalAsDuration\x10\x19\x12\x15\n\x11CastDecimalAsJson\x10\x1a\x12\
    \x13\n\x0fCastStringAsInt\x10\x1e\x12\x14\n\x10CastStringAsReal\x10\x1f\
    \x12\x16\n\x12CastStringAsString\x10\x20\x12\x17\n\x13CastStringAsDecima\
    l\x10!\x12\x14\n\x10CastStringAsTime\x10\"\x12\x18\n\x14CastStringAsDura\
    tion\x10#\x12\x14\n\x10CastStringAsJson\x10$\x12\x11\n\rCastTimeAsInt\
    \x10(\x12\x12\n\x0eCastTimeAsReal\x10)\x12\x14\n\x10CastTimeAsString\x10\
    *\x12\x15\n\x11CastTimeAsDecimal\x10+\x12\x12\n\x0eCastTimeAsTime\x10,\
    \x12\x16\n\x12CastTimeAsDuration\x10-\x12\x12\n\x0eCastTimeAsJson\x10.\
    \x12\x15\n\x11CastDurationAsInt\x102\x12\x16\n\x12CastDurationAsReal\x10\
    3\x12\x18\n\x14CastDurationAsString\x104\x12\x19\n\x15CastDurationAsDeci\
    mal\x105\x12\x16\n\x12CastDurationAsTime\x106\x12\x1a\n\x16CastDurationA\
    sDuration\x107\x12\x16\n\x12CastDurationAsJson\x108\x12\x11\n\rCastJsonA\
    sInt\x10<\x12\x12\n\x0eCastJsonAsReal\x10=\x12\x14\n\x10CastJsonAsString\
    \x10>\x12\x15\n\x11CastJsonAsDecimal\x10?\x12\x12\n\x0eCastJsonAsTime\
    \x10@\x12\x16\n\x12CastJsonAsDuration\x10A\x12\x12\n\x0eCastJsonAsJson\
    \x10B\x12\t\n\x05LTInt\x10d\x12\n\n\x06LTReal\x10e\x12\r\n\tLTDecimal\
    \x10f\x12\x0c\n\x08LTString\x10g\x12\n\n\x06LTTime\x10h\x12\x0e\n\nLTDur\
    ation\x10i\x12\n\n\x06LTJson\x10j\x12\t\n\x05LEInt\x10n\x12\n\n\x06LERea\
    l\x10o\x12\r\n\tLEDecimal\x10p\x12\x0c\n\x08LEString\x10q\x12\n\n\x06LET\
    ime\x10r\x12\x0e\n\nLEDuration\x10s\x12\n\n\x06LEJson\x10t\x12\t\n\x05GT\
    Int\x10x\x12\n\n\x06GTReal\x10y\x12\r\n\tGTDecimal\x10z\x12\x0c\n\x08GTS\
    tring\x10{\x12\n\n\x06GTTime\x10|\x12\x0e\n\nGTDuration\x10}\x12\n\n\x06\
    GTJson\x10~\x12\n\n\x05GEInt\x10\x82\x01\x12\x0b\n\x06GEReal\x10\x83\x01\
    \x12\x0e\n\tGEDecimal\x10\x84\x01\x12\r\n\x08GEString\x10\x85\x01\x12\
    \x0b\n\x06GETime\x10\x86\x01\x12\x0f\n\nGEDuration\x10\x87\x01\x12\x0b\n\
    \x06GEJson\x10\x88\x01\x12\n\n\x05EQInt\x10\x8c\x01\x12\x0b\n\x06EQReal\
    \x10\x8d\x01\x12\x0e\n\tEQDecimal\x10\x8e\x01\x12\r\n\x08EQString\x10\
    \x8f\x01\x12\x0b\n\x06EQTime\x10\x90\x01\x12\x0f\n\nEQDuration\x10\x91\
    \x01\x12\x0b\n\x06EQJson\x10\x92\x01\x12\n\n\x05NEInt\x10\x96\x01\x12\
    \x0b\n\x06NEReal\x10\x97\x01\x12\x0e\n\tNEDecimal\x10\x98\x01\x12\r\n\
    \x08NEString\x10\x99\x01\x12\x0b\n\x06NETime\x10\x9a\x01\x12\x0f\n\nNEDu\
    ration\x10\x9b\x01\x12\x0b\n\x06NEJson\x10\x9c\x01\x12\x0e\n\tNullEQInt\
    \x10\xa0\x01\x12\x0f\n\nNullEQReal\x10\xa1\x01\x12\x12\n\rNullEQDecimal\
    \x10\xa2\x01\x12\x11\n\x0cNullEQString\x10\xa3\x01\x12\x0f\n\nNullEQTime\
    \x10\xa4\x01\x12\x13\n\x0eNullEQDuration\x10\xa5\x01\x12\x0f\n\nNullEQJs\
    on\x10\xa6\x01\x12\r\n\x08PlusReal\x10\xc8\x01\x12\x10\n\x0bPlusDecimal\
    \x10\xc9\x01\x12\x0c\n\x07PlusInt\x10\xcb\x01\x12\x0e\n\tMinusReal\x10\
    \xcc\x01\x12\x11\n\x0cMinusDecimal\x10\xcd\x01\x12\r\n\x08MinusInt\x10\
    \xcf\x01\x12\x11\n\x0cMultiplyReal\x10\xd0\x01\x12\x14\n\x0fMultiplyDeci\
    mal\x10\xd1\x01\x12\x10\n\x0bMultiplyInt\x10\xd2\x01\x12\x0f\n\nDivideRe\
    al\x10\xd3\x01\x12\x12\n\rDivideDecimal\x10\xd4\x01\x12\x0b\n\x06AbsInt\
    \x10\xb5\x10\x12\x0c\n\x07AbsUInt\x10\xb6\x10\x12\x0c\n\x07AbsReal\x10\
    \xb7\x10\x12\x0f\n\nAbsDecimal\x10\xb8\x10\x12\x11\n\x0cCeilIntToDec\x10\
    \xb9\x10\x12\x11\n\x0cCeilIntToInt\x10\xba\x10\x12\x11\n\x0cCeilDecToInt\
    \x10\xbb\x10\x12\x11\n\x0cCeilDecToDec\x10\xbc\x10\x12\r\n\x08CeilReal\
    \x10\xbd\x10\x12\x12\n\rFloorIntToDec\x10\xbe\x10\x12\x12\n\rFloorIntToI\
    nt\x10\xbf\x10\x12\x12\n\rFloorDecToInt\x10\xc0\x10\x12\x12\n\rFloorDecT\
    oDec\x10\xc1\x10\x12\x0e\n\tFloorReal\x10\xc2\x10\x12\x0f\n\nLogicalAnd\
    \x10\x9d\x18\x12\x0e\n\tLogicalOr\x10\x9e\x18\x12\x0f\n\nLogicalXor\x10\
    \x9f\x18\x12\r\n\x08UnaryNot\x10\xa0\x18\x12\x12\n\rUnaryMinusInt\x10\
    \xa4\x18\x12\x13\n\x0eUnaryMinusReal\x10\xa5\x18\x12\x16\n\x11UnaryMinus\
    Decimal\x10\xa6\x18\x12\x12\n\rDecimalIsNull\x10\xa7\x18\x12\x13\n\x0eDu\
    rationIsNull\x10\xa8\x18\x12\x0f\n\nRealIsNull\x10\xa9\x18\x12\x11\n\x0c\
    StringIsNull\x10\xaa\x18\x12\x0f\n\nTimeIsNull\x10\xab\x18\x12\x0e\n\tIn\
    tIsNull\x10\xac\x18\x12\x0f\n\nJsonIsNull\x10\xad\x18\x12\x0e\n\tBitAndS\
    ig\x10\xae\x18\x12\r\n\x08BitOrSig\x10\xaf\x18\x12\x0e\n\tBitXorSig\x10\
    \xb0\x18\x12\x0e\n\tBitNegSig\x10\xb1\x18\x12\x0e\n\tIntIsTrue\x10\xb2\
    \x18\x12\x0f\n\nRealIsTrue\x10\xb3\x18\x12\x12\n\rDecimalIsTrue\x10\xb4\
    \x18\x12\x0f\n\nIntIsFalse\x10\xb5\x18\x12\x10\n\x0bRealIsFalse\x10\xb6\
    \x18\x12\x13\n\x0eDecimalIsFalse\x10\xb7\x18\x12\n\n\x05InInt\x10\xa1\
    \x1f\x12\x0b\n\x06InReal\x10\xa2\x1f\x12\x0e\n\tInDecimal\x10\xa3\x1f\
    \x12\r\n\x08InString\x10\xa4\x1f\x12\x0b\n\x06InTime\x10\xa5\x1f\x12\x0f\
    \n\nInDuration\x10\xa6\x1f\x12\x0b\n\x06InJson\x10\xa7\x1f\x12\x0e\n\tIf\
    NullInt\x10\x85\x20\x12\x0f\n\nIfNullReal\x10\x86\x20\x12\x12\n\rIfNullD\
    ecimal\x10\x87\x20\x12\x11\n\x0cIfNullString\x10\x88\x20\x12\x0f\n\nIfNu\
    llTime\x10\x89\x20\x12\x13\n\x0eIfNullDuration\x10\x8a\x20\x12\n\n\x05If\
    Int\x10\x8b\x20\x12\x0b\n\x06IfReal\x10\x8c\x20\x12\x0e\n\tIfDecimal\x10\
    \x8d\x20\x12\r\n\x08IfString\x10\x8e\x20\x12\x0b\n\x06IfTime\x10\x8f\x20\
    \x12\x0f\n\nIfDuration\x10\x90\x20\x12\x0f\n\nIfNullJson\x10\x91\x20\x12\
    \x0b\n\x06IfJson\x10\x92\x20\x12\x10\n\x0bCoalesceInt\x10\xe9\x20\x12\
    \x11\n\x0cCoalesceReal\x10\xea\x20\x12\x14\n\x0fCoalesceDecimal\x10\xeb\
    \x20\x12\x13\n\x0eCoalesceString\x10\xec\x20\x12\x11\n\x0cCoalesceTime\
    \x10\xed\x20\x12\x15\n\x10CoalesceDuration\x10\xee\x20\x12\x11\n\x0cCoal\
    esceJson\x10\xef\x20\x12\x10\n\x0bCaseWhenInt\x10\xf0\x20\x12\x11\n\x0cC\
    aseWhenReal\x10\xf1\x20\x12\x14\n\x0fCaseWhenDecimal\x10\xf2\x20\x12\x13\
    \n\x0eCaseWhenString\x10\xf3\x20\x12\x11\n\x0cCaseWhenTime\x10\xf4\x20\
    \x12\x15\n\x10CaseWhenDuration\x10\xf5\x20\x12\x11\n\x0cCaseWhenJson\x10\
    \xf6\x20\x12\x0c\n\x07LikeSig\x10\xd6!\x12\x13\n\x0eJsonExtractSig\x10\
    \x89'\x12\x13\n\x0eJsonUnquoteSig\x10\x8a'\x12\x10\n\x0bJsonTypeSig\x10\
    \x8b'\x12\x0f\n\nJsonSetSig\x10\x8c'\x12\x12\n\rJsonInsertSig\x10\x8d'\
    \x12\x13\n\x0eJsonReplaceSig\x10\x8e'\x12\x12\n\rJsonRemoveSig\x10\x8f'\
    \x12\x11\n\x0cJsonMergeSig\x10\x90'\x12\x12\n\rJsonObjectSig\x10\x91'\
    \x12\x11\n\x0cJsonArraySig\x10\x92'B%\n\x15com.pingcap.tidb.tipbP\x01\
    \xc8\xe2\x1e\x01\xd0\xe2\x1e\x01\xe0\xe2\x1e\x01J\xde\x85\x01\n\x07\x12\
    \x05\0\0\xab\x03\x01\n\x08\n\x01\x0c\x12\x03\0\0\x12\n\x08\n\x01\x02\x12\
    \x03\x02\x08\x0c\n\x08\n\x01\x08\x12\x03\x04\0\"\n\x0b\n\x04\x08\xe7\x07\
    \0\x12\x03\x04\0\"\n\x0c\n\x05\x08\xe7\x07\0\x02\x12\x03\x04\x07\x1a\n\r\
    \n\x06\x08\xe7\x07\0\x02\0\x12\x03\x04\x07\x1a\n\x0e\n\x07\x08\xe7\x07\0\
    \x02\0\x01\x12\x03\x04\x07\x1a\n\x0c\n\x05\x08\xe7\x07\0\x03\x12\x03\x04\
    \x1d!\n\x08\n\x01\x08\x12\x03\x05\0.\n\x0b\n\x04\x08\xe7\x07\x01\x12\x03\
    \x05\0.\n\x0c\n\x05\x08\xe7\x07\x01\x02\x12\x03\x05\x07\x13\n\r\n\x06\
    \x08\xe7\x07\x01\x02\0\x12\x03\x05\x07\x13\n\x0e\n\x07\x08\xe7\x07\x01\
    \x02\0\x01\x12\x03\x05\x07\x13\n\x0c\n\x05\x08\xe7\x07\x01\x07\x12\x03\
    \x05\x16-\n\t\n\x02\x03\0\x12\x03\x07\x07\x1d\n\x08\n\x01\x08\x12\x03\t\
    \0(\n\x0b\n\x04\x08\xe7\x07\x02\x12\x03\t\0(\n\x0c\n\x05\x08\xe7\x07\x02\
    \x02\x12\x03\t\x07\x20\n\r\n\x06\x08\xe7\x07\x02\x02\0\x12\x03\t\x07\x20\
    \n\x0e\n\x07\x08\xe7\x07\x02\x02\0\x01\x12\x03\t\x08\x1f\n\x0c\n\x05\x08\
    \xe7\x07\x02\x03\x12\x03\t#'\n\x08\n\x01\x08\x12\x03\n\0$\n\x0b\n\x04\
    \x08\xe7\x07\x03\x12\x03\n\0$\n\x0c\n\x05\x08\xe7\x07\x03\x02\x12\x03\n\
    \x07\x1c\n\r\n\x06\x08\xe7\x07\x03\x02\0\x12\x03\n\x07\x1c\n\x0e\n\x07\
    \x08\xe7\x07\x03\x02\0\x01\x12\x03\n\x08\x1b\n\x0c\n\x05\x08\xe7\x07\x03\
    \x03\x12\x03\n\x1f#\n\x08\n\x01\x08\x12\x03\x0b\0*\n\x0b\n\x04\x08\xe7\
    \x07\x04\x12\x03\x0b\0*\n\x0c\n\x05\x08\xe7\x07\x04\x02\x12\x03\x0b\x07\
    \"\n\r\n\x06\x08\xe7\x07\x04\x02\0\x12\x03\x0b\x07\"\n\x0e\n\x07\x08\xe7\
    \x07\x04\x02\0\x01\x12\x03\x0b\x08!\n\x0c\n\x05\x08\xe7\x07\x04\x03\x12\
    \x03\x0b%)\n\n\n\x02\x04\0\x12\x04\r\0\x14\x01\n\n\n\x03\x04\0\x01\x12\
    \x03\r\x08\x11\n\x0b\n\x04\x04\0\x02\0\x12\x03\x0e\x08B\n\x0c\n\x05\x04\
    \0\x02\0\x04\x12\x03\x0e\x08\x10\n\x0c\n\x05\x04\0\x02\0\x05\x12\x03\x0e\
    \x11\x16\n\x0c\n\x05\x04\0\x02\0\x01\x12\x03\x0e\x17\x19\n\x0c\n\x05\x04\
    \0\x02\0\x03\x12\x03\x0e\x1c\x1d\n\x0c\n\x05\x04\0\x02\0\x08\x12\x03\x0e\
    #A\n\x0f\n\x08\x04\0\x02\0\x08\xe7\x07\0\x12\x03\x0e$@\n\x10\n\t\x04\0\
    \x02\0\x08\xe7\x07\0\x02\x12\x03\x0e$8\n\x11\n\n\x04\0\x02\0\x08\xe7\x07\
    \0\x02\0\x12\x03\x0e$8\n\x12\n\x0b\x04\0\x02\0\x08\xe7\x07\0\x02\0\x01\
    \x12\x03\x0e%7\n\x10\n\t\x04\0\x02\0\x08\xe7\x07\0\x03\x12\x03\x0e;@\n\
    \x0b\n\x04\x04\0\x02\x01\x12\x03\x0f\x08B\n\x0c\n\x05\x04\0\x02\x01\x04\
    \x12\x03\x0f\x08\x10\n\x0c\n\x05\x04\0\x02\x01\x05\x12\x03\x0f\x11\x17\n\
    \x0c\n\x05\x04\0\x02\x01\x01\x12\x03\x0f\x18\x1c\n\x0c\n\x05\x04\0\x02\
    \x01\x03\x12\x03\x0f\x1f\x20\n\x0c\n\x05\x04\0\x02\x01\x08\x12\x03\x0f#A\
    \n\x0f\n\x08\x04\0\x02\x01\x08\xe7\x07\0\x12\x03\x0f$@\n\x10\n\t\x04\0\
    \x02\x01\x08\xe7\x07\0\x02\x12\x03\x0f$8\n\x11\n\n\x04\0\x02\x01\x08\xe7\
    \x07\0\x02\0\x12\x03\x0f$8\n\x12\n\x0b\x04\0\x02\x01\x08\xe7\x07\0\x02\0\
    \x01\x12\x03\x0f%7\n\x10\n\t\x04\0\x02\x01\x08\xe7\x07\0\x03\x12\x03\x0f\
    ;@\n\x0b\n\x04\x04\0\x02\x02\x12\x03\x10\x08B\n\x0c\n\x05\x04\0\x02\x02\
    \x04\x12\x03\x10\x08\x10\n\x0c\n\x05\x04\0\x02\x02\x05\x12\x03\x10\x11\
    \x16\n\x0c\n\x05\x04\0\x02\x02\x01\x12\x03\x10\x17\x1b\n\x0c\n\x05\x04\0\
    \x02\x02\x03\x12\x03\x10\x1e\x1f\n\x0c\n\x05\x04\0\x02\x02\x08\x12\x03\
    \x10#A\n\x0f\n\x08\x04\0\x02\x02\x08\xe7\x07\0\x12\x03\x10$@\n\x10\n\t\
    \x04\0\x02\x02\x08\xe7\x07\0\x02\x12\x03\x10$8\n\x11\n\n\x04\0\x02\x02\
    \x08\xe7\x07\0\x02\0\x12\x03\x10$8\n\x12\n\x0b\x04\0\x02\x02\x08\xe7\x07\
    \0\x02\0\x01\x12\x03\x10%7\n\x10\n\t\x04\0\x02\x02\x08\xe7\x07\0\x03\x12\
    \x03\x10;@\n\x0b\n\x04\x04\0\x02\x03\x12\x03\x11\x08B\n\x0c\n\x05\x04\0\
    \x02\x03\x04\x12\x03\x11\x08\x10\n\x0c\n\x05\x04\0\x02\x03\x05\x12\x03\
    \x11\x11\x16\n\x0c\n\x05\x04\0\x02\x03\x01\x12\x03\x11\x17\x1e\n\x0c\n\
    \x05\x04\0\x02\x03\x03\x12\x03\x11!\"\n\x0c\n\x05\x04\0\x02\x03\x08\x12\
    \x03\x11#A\n\x0f\n\x08\x04\0\x02\x03\x08\xe7\x07\0\x12\x03\x11$@\n\x10\n\
    \t\x04\0\x02\x03\x08\xe7\x07\0\x02\x12\x03\x11$8\n\x11\n\n\x04\0\x02\x03\
    \x08\xe7\x07\0\x02\0\x12\x03\x11$8\n\x12\n\x0b\x04\0\x02\x03\x08\xe7\x07\
    \0\x02\0\x01\x12\x03\x11%7\n\x10\n\t\x04\0\x02\x03\x08\xe7\x07\0\x03\x12\
    \x03\x11;@\n\x0b\n\x04\x04\0\x02\x04\x12\x03\x12\x08B\n\x0c\n\x05\x04\0\
    \x02\x04\x04\x12\x03\x12\x08\x10\n\x0c\n\x05\x04\0\x02\x04\x05\x12\x03\
    \x12\x11\x16\n\x0c\n\x05\x04\0\x02\x04\x01\x12\x03\x12\x17\x1e\n\x0c\n\
    \x05\x04\0\x02\x04\x03\x12\x03\x12!\"\n\x0c\n\x05\x04\0\x02\x04\x08\x12\
    \x03\x12#A\n\x0f\n\x08\x04\0\x02\x04\x08\xe7\x07\0\x12\x03\x12$@\n\x10\n\
    \t\x04\0\x02\x04\x08\xe7\x07\0\x02\x12\x03\x12$8\n\x11\n\n\x04\0\x02\x04\
    \x08\xe7\x07\0\x02\0\x12\x03\x12$8\n\x12\n\x0b\x04\0\x02\x04\x08\xe7\x07\
    \0\x02\0\x01\x12\x03\x12%7\n\x10\n\t\x04\0\x02\x04\x08\xe7\x07\0\x03\x12\
    \x03\x12;@\n\x0b\n\x04\x04\0\x02\x05\x12\x03\x13\x08C\n\x0c\n\x05\x04\0\
    \x02\x05\x04\x12\x03\x13\x08\x10\n\x0c\n\x05\x04\0\x02\x05\x05\x12\x03\
    \x13\x11\x17\n\x0c\n\x05\x04\0\x02\x05\x01\x12\x03\x13\x18\x1f\n\x0c\n\
    \x05\x04\0\x02\x05\x03\x12\x03\x13\"#\n\x0c\n\x05\x04\0\x02\x05\x08\x12\
    \x03\x13$B\n\x0f\n\x08\x04\0\x02\x05\x08\xe7\x07\0\x12\x03\x13%A\n\x10\n\
    \t\x04\0\x02\x05\x08\xe7\x07\0\x02\x12\x03\x13%9\n\x11\n\n\x04\0\x02\x05\
    \x08\xe7\x07\0\x02\0\x12\x03\x13%9\n\x12\n\x0b\x04\0\x02\x05\x08\xe7\x07\
    \0\x02\0\x01\x12\x03\x13&8\n\x10\n\t\x04\0\x02\x05\x08\xe7\x07\0\x03\x12\
    \x03\x13<A\n\x20\n\x02\x05\0\x12\x05\x16\0\xbd\x01\x01\"\x13\x20Children\
    \x20count\x200.\x20\n\n\n\x03\x05\0\x01\x12\x03\x16\x05\r\n(\n\x04\x05\0\
    \x02\0\x12\x03\x19\x08\x11\x1a\x1b\x20Values\x20are\x20encoded\x20bytes.\
    \n\n\x0c\n\x05\x05\0\x02\0\x01\x12\x03\x19\x08\x0c\n\x0c\n\x05\x05\0\x02\
    \0\x02\x12\x03\x19\x0f\x10\n\x0b\n\x04\x05\0\x02\x01\x12\x03\x1a\x08\x12\
    \n\x0c\n\x05\x05\0\x02\x01\x01\x12\x03\x1a\x08\r\n\x0c\n\x05\x05\0\x02\
    \x01\x02\x12\x03\x1a\x10\x11\n\x0b\n\x04\x05\0\x02\x02\x12\x03\x1b\x08\
    \x13\n\x0c\n\x05\x05\0\x02\x02\x01\x12\x03\x1b\x08\x0e\n\x0c\n\x05\x05\0\
    \x02\x02\x02\x12\x03\x1b\x11\x12\n\x0b\n\x04\x05\0\x02\x03\x12\x03\x1c\
    \x08\x14\n\x0c\n\x05\x05\0\x02\x03\x01\x12\x03\x1c\x08\x0f\n\x0c\n\x05\
    \x05\0\x02\x03\x02\x12\x03\x1c\x12\x13\n\x0b\n\x04\x05\0\x02\x04\x12\x03\
    \x1d\x08\x14\n\x0c\n\x05\x05\0\x02\x04\x01\x12\x03\x1d\x08\x0f\n\x0c\n\
    \x05\x05\0\x02\x04\x02\x12\x03\x1d\x12\x13\n\x0b\n\x04\x05\0\x02\x05\x12\
    \x03\x1e\x08\x13\n\x0c\n\x05\x05\0\x02\x05\x01\x12\x03\x1e\x08\x0e\n\x0c\
    \n\x05\x05\0\x02\x05\x02\x12\x03\x1e\x11\x12\n\x0b\n\x04\x05\0\x02\x06\
    \x12\x03\x1f\x08\x12\n\x0c\n\x05\x05\0\x02\x06\x01\x12\x03\x1f\x08\r\n\
    \x0c\n\x05\x05\0\x02\x06\x02\x12\x03\x1f\x10\x11\n$\n\x04\x05\0\x02\x07\
    \x12\x03\"\x08\x17\x1a\x17\x20Mysql\x20specific\x20types.\n\n\x0c\n\x05\
    \x05\0\x02\x07\x01\x12\x03\"\x08\x10\n\x0c\n\x05\x05\0\x02\x07\x02\x12\
    \x03\"\x13\x16\n\x0b\n\x04\x05\0\x02\x08\x12\x03#\x08\x1b\n\x0c\n\x05\
    \x05\0\x02\x08\x01\x12\x03#\x08\x14\n\x0c\n\x05\x05\0\x02\x08\x02\x12\
    \x03#\x17\x1a\n\x0b\n\x04\x05\0\x02\t\x12\x03$\x08\x1c\n\x0c\n\x05\x05\0\
    \x02\t\x01\x12\x03$\x08\x15\n\x0c\n\x05\x05\0\x02\t\x02\x12\x03$\x18\x1b\
    \n\x0b\n\x04\x05\0\x02\n\x12\x03%\x08\x18\n\x0c\n\x05\x05\0\x02\n\x01\
    \x12\x03%\x08\x11\n\x0c\n\x05\x05\0\x02\n\x02\x12\x03%\x14\x17\n\x0b\n\
    \x04\x05\0\x02\x0b\x12\x03&\x08\x17\n\x0c\n\x05\x05\0\x02\x0b\x01\x12\
    \x03&\x08\x10\n\x0c\n\x05\x05\0\x02\x0b\x02\x12\x03&\x13\x16\n\x0b\n\x04\
    \x05\0\x02\x0c\x12\x03'\x08\x17\n\x0c\n\x05\x05\0\x02\x0c\x01\x12\x03'\
    \x08\x10\n\x0c\n\x05\x05\0\x02\x0c\x02\x12\x03'\x13\x16\n\x0b\n\x04\x05\
    \0\x02\r\x12\x03(\x08\x18\n\x0c\n\x05\x05\0\x02\r\x01\x12\x03(\x08\x11\n\
    \x0c\n\x05\x05\0\x02\r\x02\x12\x03(\x14\x17\n\x0b\n\x04\x05\0\x02\x0e\
    \x12\x03)\x08\x18\n\x0c\n\x05\x05\0\x02\x0e\x01\x12\x03)\x08\x11\n\x0c\n\
    \x05\x05\0\x02\x0e\x02\x12\x03)\x14\x17\n\"\n\x04\x05\0\x02\x0f\x12\x03,\
    \x08\x18\x1a\x15\x20Encoded\x20value\x20list.\n\n\x0c\n\x05\x05\0\x02\
    \x0f\x01\x12\x03,\x08\x11\n\x0c\n\x05\x05\0\x02\x0f\x02\x12\x03,\x14\x17\
    \n:\n\x04\x05\0\x02\x10\x12\x03/\x08\x18\x1a-\x20Column\x20reference.\
    \x20value\x20is\x20int64\x20column\x20ID.\n\n\x0c\n\x05\x05\0\x02\x10\
    \x01\x12\x03/\x08\x11\n\x0c\n\x05\x05\0\x02\x10\x02\x12\x03/\x14\x17\n2\
    \n\x04\x05\0\x02\x11\x12\x032\x08\x13\x1a%\x20Unary\x20operations,\x20ch\
    ildren\x20count\x201.\x20\n\x0c\n\x05\x05\0\x02\x11\x01\x12\x032\x08\x0b\
    \n\x0c\n\x05\x05\0\x02\x11\x02\x12\x032\x0e\x12\n\x0b\n\x04\x05\0\x02\
    \x12\x12\x033\x08\x13\n\x0c\n\x05\x05\0\x02\x12\x01\x12\x033\x08\x0b\n\
    \x0c\n\x05\x05\0\x02\x12\x02\x12\x033\x0e\x12\n\x0b\n\x04\x05\0\x02\x13\
    \x12\x034\x08\x16\n\x0c\n\x05\x05\0\x02\x13\x01\x12\x034\x08\x0e\n\x0c\n\
    \x05\x05\0\x02\x13\x02\x12\x034\x11\x15\nM\n\x04\x05\0\x02\x14\x12\x038\
    \x08\x12\x1a\x18\x20Comparison\x20operations.\n2&\x20Binary\x20operation\
    s,\x20children\x20count\x202.\x20\n\x0c\n\x05\x05\0\x02\x14\x01\x12\x038\
    \x08\n\n\x0c\n\x05\x05\0\x02\x14\x02\x12\x038\r\x11\n\x0b\n\x04\x05\0\
    \x02\x15\x12\x039\x08\x12\n\x0c\n\x05\x05\0\x02\x15\x01\x12\x039\x08\n\n\
    \x0c\n\x05\x05\0\x02\x15\x02\x12\x039\r\x11\n\x0b\n\x04\x05\0\x02\x16\
    \x12\x03:\x08\x12\n\x0c\n\x05\x05\0\x02\x16\x01\x12\x03:\x08\n\n\x0c\n\
    \x05\x05\0\x02\x16\x02\x12\x03:\r\x11\n\x0b\n\x04\x05\0\x02\x17\x12\x03;\
    \x08\x12\n\x0c\n\x05\x05\0\x02\x17\x01\x12\x03;\x08\n\n\x0c\n\x05\x05\0\
    \x02\x17\x02\x12\x03;\r\x11\n\x0b\n\x04\x05\0\x02\x18\x12\x03<\x08\x12\n\
    \x0c\n\x05\x05\0\x02\x18\x01\x12\x03<\x08\n\n\x0c\n\x05\x05\0\x02\x18\
    \x02\x12\x03<\r\x11\n\x0b\n\x04\x05\0\x02\x19\x12\x03=\x08\x12\n\x0c\n\
    \x05\x05\0\x02\x19\x01\x12\x03=\x08\n\n\x0c\n\x05\x05\0\x02\x19\x02\x12\
    \x03=\r\x11\n\x0b\n\x04\x05\0\x02\x1a\x12\x03>\x08\x16\n\x0c\n\x05\x05\0\
    \x02\x1a\x01\x12\x03>\x08\x0e\n\x0c\n\x05\x05\0\x02\x1a\x02\x12\x03>\x11\
    \x15\n\x1e\n\x04\x05\0\x02\x1b\x12\x03A\x08\x16\x1a\x11\x20Bit\x20operat\
    ions.\n\n\x0c\n\x05\x05\0\x02\x1b\x01\x12\x03A\x08\x0e\n\x0c\n\x05\x05\0\
    \x02\x1b\x02\x12\x03A\x11\x15\n\x0b\n\x04\x05\0\x02\x1c\x12\x03B\x08\x15\
    \n\x0c\n\x05\x05\0\x02\x1c\x01\x12\x03B\x08\r\n\x0c\n\x05\x05\0\x02\x1c\
    \x02\x12\x03B\x10\x14\n\x0b\n\x04\x05\0\x02\x1d\x12\x03C\x08\x16\n\x0c\n\
    \x05\x05\0\x02\x1d\x01\x12\x03C\x08\x0e\n\x0c\n\x05\x05\0\x02\x1d\x02\
    \x12\x03C\x11\x15\n\x0b\n\x04\x05\0\x02\x1e\x12\x03D\x08\x19\n\x0c\n\x05\
    \x05\0\x02\x1e\x01\x12\x03D\x08\x11\n\x0c\n\x05\x05\0\x02\x1e\x02\x12\
    \x03D\x14\x18\n\x0b\n\x04\x05\0\x02\x1f\x12\x03E\x08\x19\n\x0c\n\x05\x05\
    \0\x02\x1f\x01\x12\x03E\x08\x11\n\x0c\n\x05\x05\0\x02\x1f\x02\x12\x03E\
    \x14\x18\n\x1a\n\x04\x05\0\x02\x20\x12\x03H\x08\x14\x1a\r\x20Arithmatic.\
    \n\n\x0c\n\x05\x05\0\x02\x20\x01\x12\x03H\x08\x0c\n\x0c\n\x05\x05\0\x02\
    \x20\x02\x12\x03H\x0f\x13\n\x0b\n\x04\x05\0\x02!\x12\x03I\x08\x15\n\x0c\
    \n\x05\x05\0\x02!\x01\x12\x03I\x08\r\n\x0c\n\x05\x05\0\x02!\x02\x12\x03I\
    \x10\x14\n\x0b\n\x04\x05\0\x02\"\x12\x03J\x08\x13\n\x0c\n\x05\x05\0\x02\
    \"\x01\x12\x03J\x08\x0b\n\x0c\n\x05\x05\0\x02\"\x02\x12\x03J\x0e\x12\n\
    \x0b\n\x04\x05\0\x02#\x12\x03K\x08\x13\n\x0c\n\x05\x05\0\x02#\x01\x12\
    \x03K\x08\x0b\n\x0c\n\x05\x05\0\x02#\x02\x12\x03K\x0e\x12\n\x0b\n\x04\
    \x05\0\x02$\x12\x03L\x08\x16\n\x0c\n\x05\x05\0\x02$\x01\x12\x03L\x08\x0e\
    \n\x0c\n\x05\x05\0\x02$\x02\x12\x03L\x11\x15\n\x0b\n\x04\x05\0\x02%\x12\
    \x03M\x08\x13\n\x0c\n\x05\x05\0\x02%\x01\x12\x03M\x08\x0b\n\x0c\n\x05\
    \x05\0\x02%\x02\x12\x03M\x0e\x12\n\x20\n\x04\x05\0\x02&\x12\x03P\x08\x13\
    \x1a\x13\x20Logic\x20operations.\n\n\x0c\n\x05\x05\0\x02&\x01\x12\x03P\
    \x08\x0b\n\x0c\n\x05\x05\0\x02&\x02\x12\x03P\x0e\x12\n\x0b\n\x04\x05\0\
    \x02'\x12\x03Q\x08\x12\n\x0c\n\x05\x05\0\x02'\x01\x12\x03Q\x08\n\n\x0c\n\
    \x05\x05\0\x02'\x02\x12\x03Q\r\x11\n\x0b\n\x04\x05\0\x02(\x12\x03R\x08\
    \x13\n\x0c\n\x05\x05\0\x02(\x01\x12\x03R\x08\x0b\n\x0c\n\x05\x05\0\x02(\
    \x02\x12\x03R\x0e\x12\n\\\n\x04\x05\0\x02)\x12\x03V\x08\x15\x1a\x16\x20A\
    ggregate\x20functions.\n27\x20Mysql\x20functions,\x20children\x20count\
    \x20is\x20function\x20specific.\x20\n\x0c\n\x05\x05\0\x02)\x01\x12\x03V\
    \x08\r\n\x0c\n\x05\x05\0\x02)\x02\x12\x03V\x10\x14\n\x0b\n\x04\x05\0\x02\
    *\x12\x03W\x08\x13\n\x0c\n\x05\x05\0\x02*\x01\x12\x03W\x08\x0b\n\x0c\n\
    \x05\x05\0\x02*\x02\x12\x03W\x0e\x12\n\x0b\n\x04\x05\0\x02+\x12\x03X\x08\
    \x13\n\x0c\n\x05\x05\0\x02+\x01\x12\x03X\x08\x0b\n\x0c\n\x05\x05\0\x02+\
    \x02\x12\x03X\x0e\x12\n\x0b\n\x04\x05\0\x02,\x12\x03Y\x08\x13\n\x0c\n\
    \x05\x05\0\x02,\x01\x12\x03Y\x08\x0b\n\x0c\n\x05\x05\0\x02,\x02\x12\x03Y\
    \x0e\x12\n\x0b\n\x04\x05\0\x02-\x12\x03Z\x08\x13\n\x0c\n\x05\x05\0\x02-\
    \x01\x12\x03Z\x08\x0b\n\x0c\n\x05\x05\0\x02-\x02\x12\x03Z\x0e\x12\n\x0b\
    \n\x04\x05\0\x02.\x12\x03[\x08\x15\n\x0c\n\x05\x05\0\x02.\x01\x12\x03[\
    \x08\r\n\x0c\n\x05\x05\0\x02.\x02\x12\x03[\x10\x14\n\x0b\n\x04\x05\0\x02\
    /\x12\x03\\\x08\x1b\n\x0c\n\x05\x05\0\x02/\x01\x12\x03\\\x08\x13\n\x0c\n\
    \x05\x05\0\x02/\x02\x12\x03\\\x16\x1a\n\x0b\n\x04\x05\0\x020\x12\x03]\
    \x08\x1a\n\x0c\n\x05\x05\0\x020\x01\x12\x03]\x08\x12\n\x0c\n\x05\x05\0\
    \x020\x02\x12\x03]\x15\x19\n\x0b\n\x04\x05\0\x021\x12\x03^\x08\x19\n\x0c\
    \n\x05\x05\0\x021\x01\x12\x03^\x08\x11\n\x0c\n\x05\x05\0\x021\x02\x12\
    \x03^\x14\x18\n\x0b\n\x04\x05\0\x022\x12\x03_\x08\x1a\n\x0c\n\x05\x05\0\
    \x022\x01\x12\x03_\x08\x12\n\x0c\n\x05\x05\0\x022\x02\x12\x03_\x15\x19\n\
    \x0b\n\x04\x05\0\x023\x12\x03`\x08\x13\n\x0c\n\x05\x05\0\x023\x01\x12\
    \x03`\x08\x0b\n\x0c\n\x05\x05\0\x023\x02\x12\x03`\x0e\x12\n\x0b\n\x04\
    \x05\0\x024\x12\x03a\x08\x16\n\x0c\n\x05\x05\0\x024\x01\x12\x03a\x08\x0e\
    \n\x0c\n\x05\x05\0\x024\x02\x12\x03a\x11\x15\n\x0b\n\x04\x05\0\x025\x12\
    \x03b\x08\x19\n\x0c\n\x05\x05\0\x025\x01\x12\x03b\x08\x11\n\x0c\n\x05\
    \x05\0\x025\x02\x12\x03b\x14\x18\n\x0b\n\x04\x05\0\x026\x12\x03c\x08\x1a\
    \n\x0c\n\x05\x05\0\x026\x01\x12\x03c\x08\x12\n\x0c\n\x05\x05\0\x026\x02\
    \x12\x03c\x15\x19\n\x0b\n\x04\x05\0\x027\x12\x03d\x08\x16\n\x0c\n\x05\
    \x05\0\x027\x01\x12\x03d\x08\x0e\n\x0c\n\x05\x05\0\x027\x02\x12\x03d\x11\
    \x15\n\x0b\n\x04\x05\0\x028\x12\x03e\x08\x17\n\x0c\n\x05\x05\0\x028\x01\
    \x12\x03e\x08\x0f\n\x0c\n\x05\x05\0\x028\x02\x12\x03e\x12\x16\n\x0b\n\
    \x04\x05\0\x029\x12\x03f\x08\x18\n\x0c\n\x05\x05\0\x029\x01\x12\x03f\x08\
    \x10\n\x0c\n\x05\x05\0\x029\x02\x12\x03f\x13\x17\n\x1e\n\x04\x05\0\x02:\
    \x12\x03i\x08\x13\x1a\x11\x20Math\x20functions.\n\n\x0c\n\x05\x05\0\x02:\
    \x01\x12\x03i\x08\x0b\n\x0c\n\x05\x05\0\x02:\x02\x12\x03i\x0e\x12\n\x0b\
    \n\x04\x05\0\x02;\x12\x03j\x08\x13\n\x0c\n\x05\x05\0\x02;\x01\x12\x03j\
    \x08\x0b\n\x0c\n\x05\x05\0\x02;\x02\x12\x03j\x0e\x12\n\x0b\n\x04\x05\0\
    \x02<\x12\x03k\x08\x15\n\x0c\n\x05\x05\0\x02<\x01\x12\x03k\x08\r\n\x0c\n\
    \x05\x05\0\x02<\x02\x12\x03k\x10\x14\n\x20\n\x04\x05\0\x02=\x12\x03n\x08\
    \x16\x1a\x13\x20String\x20functions.\n\n\x0c\n\x05\x05\0\x02=\x01\x12\
    \x03n\x08\x0e\n\x0c\n\x05\x05\0\x02=\x02\x12\x03n\x11\x15\n\x0b\n\x04\
    \x05\0\x02>\x12\x03o\x08\x18\n\x0c\n\x05\x05\0\x02>\x01\x12\x03o\x08\x10\
    \n\x0c\n\x05\x05\0\x02>\x02\x12\x03o\x13\x17\n\x0b\n\x04\x05\0\x02?\x12\
    \x03p\x08\x14\n\x0c\n\x05\x05\0\x02?\x01\x12\x03p\x08\x0c\n\x0c\n\x05\
    \x05\0\x02?\x02\x12\x03p\x0f\x13\n\x0b\n\x04\x05\0\x02@\x12\x03q\x08\x16\
    \n\x0c\n\x05\x05\0\x02@\x01\x12\x03q\x08\x0e\n\x0c\n\x05\x05\0\x02@\x02\
    \x12\x03q\x11\x15\n\x0b\n\x04\x05\0\x02A\x12\x03r\x08\x15\n\x0c\n\x05\
    \x05\0\x02A\x01\x12\x03r\x08\r\n\x0c\n\x05\x05\0\x02A\x02\x12\x03r\x10\
    \x14\n\x0b\n\x04\x05\0\x02B\x12\x03s\x08\x16\n\x0c\n\x05\x05\0\x02B\x01\
    \x12\x03s\x08\x0e\n\x0c\n\x05\x05\0\x02B\x02\x12\x03s\x11\x15\n\x0b\n\
    \x04\x05\0\x02C\x12\x03t\x08\x17\n\x0c\n\x05\x05\0\x02C\x01\x12\x03t\x08\
    \x0f\n\x0c\n\x05\x05\0\x02C\x02\x12\x03t\x12\x16\n\x0b\n\x04\x05\0\x02D\
    \x12\x03u\x08\x15\n\x0c\n\x05\x05\0\x02D\x01\x12\x03u\x08\r\n\x0c\n\x05\
    \x05\0\x02D\x02\x12\x03u\x10\x14\n\x0b\n\x04\x05\0\x02E\x12\x03v\x08\x16\
    \n\x0c\n\x05\x05\0\x02E\x01\x12\x03v\x08\x0e\n\x0c\n\x05\x05\0\x02E\x02\
    \x12\x03v\x11\x15\n\x0b\n\x04\x05\0\x02F\x12\x03w\x08\x17\n\x0c\n\x05\
    \x05\0\x02F\x01\x12\x03w\x08\x0f\n\x0c\n\x05\x05\0\x02F\x02\x12\x03w\x12\
    \x16\n\x0b\n\x04\x05\0\x02G\x12\x03x\x08\x14\n\x0c\n\x05\x05\0\x02G\x01\
    \x12\x03x\x08\x0c\n\x0c\n\x05\x05\0\x02G\x02\x12\x03x\x0f\x13\n\x0b\n\
    \x04\x05\0\x02H\x12\x03y\x08\x19\n\x0c\n\x05\x05\0\x02H\x01\x12\x03y\x08\
    \x11\n\x0c\n\x05\x05\0\x02H\x02\x12\x03y\x14\x18\n\x0b\n\x04\x05\0\x02I\
    \x12\x03z\x08\x1e\n\x0c\n\x05\x05\0\x02I\x01\x12\x03z\x08\x16\n\x0c\n\
    \x05\x05\0\x02I\x02\x12\x03z\x19\x1d\n\x0b\n\x04\x05\0\x02J\x12\x03{\x08\
    \x16\n\x0c\n\x05\x05\0\x02J\x01\x12\x03{\x08\x0e\n\x0c\n\x05\x05\0\x02J\
    \x02\x12\x03{\x11\x15\n\x0b\n\x04\x05\0\x02K\x12\x03|\x08\x14\n\x0c\n\
    \x05\x05\0\x02K\x01\x12\x03|\x08\x0c\n\x0c\n\x05\x05\0\x02K\x02\x12\x03|\
    \x0f\x13\n&\n\x04\x05\0\x02L\x12\x03\x7f\x08\x12\x1a\x19\x20Control\x20f\
    low\x20functions.\n\n\x0c\n\x05\x05\0\x02L\x01\x12\x03\x7f\x08\n\n\x0c\n\
    \x05\x05\0\x02L\x02\x12\x03\x7f\r\x11\n\x0c\n\x04\x05\0\x02M\x12\x04\x80\
    \x01\x08\x16\n\r\n\x05\x05\0\x02M\x01\x12\x04\x80\x01\x08\x0e\n\r\n\x05\
    \x05\0\x02M\x02\x12\x04\x80\x01\x11\x15\n\x0c\n\x04\x05\0\x02N\x12\x04\
    \x81\x01\x08\x16\n\r\n\x05\x05\0\x02N\x01\x12\x04\x81\x01\x08\x0e\n\r\n\
    \x05\x05\0\x02N\x02\x12\x04\x81\x01\x11\x15\n\x1f\n\x04\x05\0\x02O\x12\
    \x04\x84\x01\x08\x14\x1a\x11\x20Time\x20functions.\n\n\r\n\x05\x05\0\x02\
    O\x01\x12\x04\x84\x01\x08\x0c\n\r\n\x05\x05\0\x02O\x02\x12\x04\x84\x01\
    \x0f\x13\n\x0c\n\x04\x05\0\x02P\x12\x04\x85\x01\x08\x17\n\r\n\x05\x05\0\
    \x02P\x01\x12\x04\x85\x01\x08\x0f\n\r\n\x05\x05\0\x02P\x02\x12\x04\x85\
    \x01\x12\x16\n\x0c\n\x04\x05\0\x02Q\x12\x04\x86\x01\x08\x17\n\r\n\x05\
    \x05\0\x02Q\x01\x12\x04\x86\x01\x08\x0f\n\r\n\x05\x05\0\x02Q\x02\x12\x04\
    \x86\x01\x12\x16\n\x0c\n\x04\x05\0\x02R\x12\x04\x88\x01\x08\x14\n\r\n\
    \x05\x05\0\x02R\x01\x12\x04\x88\x01\x08\x0c\n\r\n\x05\x05\0\x02R\x02\x12\
    \x04\x88\x01\x0f\x13\n\x0c\n\x04\x05\0\x02S\x12\x04\x89\x01\x08\x18\n\r\
    \n\x05\x05\0\x02S\x01\x12\x04\x89\x01\x08\x10\n\r\n\x05\x05\0\x02S\x02\
    \x12\x04\x89\x01\x13\x17\n\x0c\n\x04\x05\0\x02T\x12\x04\x8b\x01\x08\x15\
    \n\r\n\x05\x05\0\x02T\x01\x12\x04\x8b\x01\x08\r\n\r\n\x05\x05\0\x02T\x02\
    \x12\x04\x8b\x01\x10\x14\n\x0c\n\x04\x05\0\x02U\x12\x04\x8d\x01\x08\x14\
    \n\r\n\x05\x05\0\x02U\x01\x12\x04\x8d\x01\x08\x0c\n\r\n\x05\x05\0\x02U\
    \x02\x12\x04\x8d\x01\x0f\x13\n\x0c\n\x04\x05\0\x02V\x12\x04\x8e\x01\x08\
    \x17\n\r\n\x05\x05\0\x02V\x01\x12\x04\x8e\x01\x08\x0f\n\r\n\x05\x05\0\
    \x02V\x02\x12\x04\x8e\x01\x12\x16\n\x0c\n\x04\x05\0\x02W\x12\x04\x8f\x01\
    \x08\x1a\n\r\n\x05\x05\0\x02W\x01\x12\x04\x8f\x01\x08\x12\n\r\n\x05\x05\
    \0\x02W\x02\x12\x04\x8f\x01\x15\x19\n\x0c\n\x04\x05\0\x02X\x12\x04\x91\
    \x01\x08\x13\n\r\n\x05\x05\0\x02X\x01\x12\x04\x91\x01\x08\x0b\n\r\n\x05\
    \x05\0\x02X\x02\x12\x04\x91\x01\x0e\x12\n\x0c\n\x04\x05\0\x02Y\x12\x04\
    \x92\x01\x08\x17\n\r\n\x05\x05\0\x02Y\x01\x12\x04\x92\x01\x08\x0f\n\r\n\
    \x05\x05\0\x02Y\x02\x12\x04\x92\x01\x12\x16\n\x0c\n\x04\x05\0\x02Z\x12\
    \x04\x93\x01\x08\x19\n\r\n\x05\x05\0\x02Z\x01\x12\x04\x93\x01\x08\x11\n\
    \r\n\x05\x05\0\x02Z\x02\x12\x04\x93\x01\x14\x18\n\x0c\n\x04\x05\0\x02[\
    \x12\x04\x94\x01\x08\x1a\n\r\n\x05\x05\0\x02[\x01\x12\x04\x94\x01\x08\
    \x12\n\r\n\x05\x05\0\x02[\x02\x12\x04\x94\x01\x15\x19\n\x0c\n\x04\x05\0\
    \x02\\\x12\x04\x95\x01\x08\x19\n\r\n\x05\x05\0\x02\\\x01\x12\x04\x95\x01\
    \x08\x11\n\r\n\x05\x05\0\x02\\\x02\x12\x04\x95\x01\x14\x18\n\x0c\n\x04\
    \x05\0\x02]\x12\x04\x97\x01\x08\x14\n\r\n\x05\x05\0\x02]\x01\x12\x04\x97\
    \x01\x08\x0c\n\r\n\x05\x05\0\x02]\x02\x12\x04\x97\x01\x0f\x13\n\x0c\n\
    \x04\x05\0\x02^\x12\x04\x98\x01\x08\x16\n\r\n\x05\x05\0\x02^\x01\x12\x04\
    \x98\x01\x08\x0e\n\r\n\x05\x05\0\x02^\x02\x12\x04\x98\x01\x11\x15\n\x0c\
    \n\x04\x05\0\x02_\x12\x04\x99\x01\x08\x16\n\r\n\x05\x05\0\x02_\x01\x12\
    \x04\x99\x01\x08\x0e\n\r\n\x05\x05\0\x02_\x02\x12\x04\x99\x01\x11\x15\n\
    \x0c\n\x04\x05\0\x02`\x12\x04\x9a\x01\x08\x1b\n\r\n\x05\x05\0\x02`\x01\
    \x12\x04\x9a\x01\x08\x13\n\r\n\x05\x05\0\x02`\x02\x12\x04\x9a\x01\x16\
    \x1a\n\x0c\n\x04\x05\0\x02a\x12\x04\x9c\x01\x08\x17\n\r\n\x05\x05\0\x02a\
    \x01\x12\x04\x9c\x01\x08\x0f\n\r\n\x05\x05\0\x02a\x02\x12\x04\x9c\x01\
    \x12\x16\n\x20\n\x04\x05\0\x02b\x12\x04\x9f\x01\x08\x18\x1a\x12\x20Other\
    \x20functions;\n\n\r\n\x05\x05\0\x02b\x01\x12\x04\x9f\x01\x08\x10\n\r\n\
    \x05\x05\0\x02b\x02\x12\x04\x9f\x01\x13\x17\n\x0c\n\x04\x05\0\x02c\x12\
    \x04\xa0\x01\x08\x18\n\r\n\x05\x05\0\x02c\x01\x12\x04\xa0\x01\x08\x10\n\
    \r\n\x05\x05\0\x02c\x02\x12\x04\xa0\x01\x13\x17\n\x0c\n\x04\x05\0\x02d\
    \x12\x04\xa1\x01\x08\x15\n\r\n\x05\x05\0\x02d\x01\x12\x04\xa1\x01\x08\r\
    \n\r\n\x05\x05\0\x02d\x02\x12\x04\xa1\x01\x10\x14\n\x1f\n\x04\x05\0\x02e\
    \x12\x04\xa4\x01\x08\x1b\x1a\x11\x20Json\x20functions;\x20\n\r\n\x05\x05\
    \0\x02e\x01\x12\x04\xa4\x01\x08\x13\n\r\n\x05\x05\0\x02e\x02\x12\x04\xa4\
    \x01\x16\x1a\n\x0c\n\x04\x05\0\x02f\x12\x04\xa5\x01\x08\x18\n\r\n\x05\
    \x05\0\x02f\x01\x12\x04\xa5\x01\x08\x10\n\r\n\x05\x05\0\x02f\x02\x12\x04\
    \xa5\x01\x13\x17\n\x0c\n\x04\x05\0\x02g\x12\x04\xa6\x01\x08\x19\n\r\n\
    \x05\x05\0\x02g\x01\x12\x04\xa6\x01\x08\x11\n\r\n\x05\x05\0\x02g\x02\x12\
    \x04\xa6\x01\x14\x18\n\x0c\n\x04\x05\0\x02h\x12\x04\xa7\x01\x08\x1a\n\r\
    \n\x05\x05\0\x02h\x01\x12\x04\xa7\x01\x08\x12\n\r\n\x05\x05\0\x02h\x02\
    \x12\x04\xa7\x01\x15\x19\n\x0c\n\x04\x05\0\x02i\x12\x04\xa8\x01\x08\x19\
    \n\r\n\x05\x05\0\x02i\x01\x12\x04\xa8\x01\x08\x11\n\r\n\x05\x05\0\x02i\
    \x02\x12\x04\xa8\x01\x14\x18\n\x0c\n\x04\x05\0\x02j\x12\x04\xa9\x01\x08\
    \x19\n\r\n\x05\x05\0\x02j\x01\x12\x04\xa9\x01\x08\x11\n\r\n\x05\x05\0\
    \x02j\x02\x12\x04\xa9\x01\x14\x18\n\x0c\n\x04\x05\0\x02k\x12\x04\xaa\x01\
    \x08\x17\n\r\n\x05\x05\0\x02k\x01\x12\x04\xaa\x01\x08\x0f\n\r\n\x05\x05\
    \0\x02k\x02\x12\x04\xaa\x01\x12\x16\n\x0c\n\x04\x05\0\x02l\x12\x04\xab\
    \x01\x08\x1a\n\r\n\x05\x05\0\x02l\x01\x12\x04\xab\x01\x08\x12\n\r\n\x05\
    \x05\0\x02l\x02\x12\x04\xab\x01\x15\x19\n\x0c\n\x04\x05\0\x02m\x12\x04\
    \xac\x01\x08\x1b\n\r\n\x05\x05\0\x02m\x01\x12\x04\xac\x01\x08\x13\n\r\n\
    \x05\x05\0\x02m\x02\x12\x04\xac\x01\x16\x1a\n\x0c\n\x04\x05\0\x02n\x12\
    \x04\xad\x01\x08\x1a\n\r\n\x05\x05\0\x02n\x01\x12\x04\xad\x01\x08\x12\n\
    \r\n\x05\x05\0\x02n\x02\x12\x04\xad\x01\x15\x19\n\x0c\n\x04\x05\0\x02o\
    \x12\x04\xae\x01\x08\x1c\n\r\n\x05\x05\0\x02o\x01\x12\x04\xae\x01\x08\
    \x14\n\r\n\x05\x05\0\x02o\x02\x12\x04\xae\x01\x17\x1b\n\x0c\n\x04\x05\0\
    \x02p\x12\x04\xaf\x01\x08\x1b\n\r\n\x05\x05\0\x02p\x01\x12\x04\xaf\x01\
    \x08\x13\n\r\n\x05\x05\0\x02p\x02\x12\x04\xaf\x01\x16\x1a\n\x0c\n\x04\
    \x05\0\x02q\x12\x04\xb0\x01\x08\x20\n\r\n\x05\x05\0\x02q\x01\x12\x04\xb0\
    \x01\x08\x18\n\r\n\x05\x05\0\x02q\x02\x12\x04\xb0\x01\x1b\x1f\n\"\n\x04\
    \x05\0\x02r\x12\x04\xb3\x01\x08\x12\x1a\x14\x20Other\x20expressions.\x20\
    \n\r\n\x05\x05\0\x02r\x01\x12\x04\xb3\x01\x08\n\n\r\n\x05\x05\0\x02r\x02\
    \x12\x04\xb3\x01\r\x11\n\x0c\n\x04\x05\0\x02s\x12\x04\xb4\x01\x08\x17\n\
    \r\n\x05\x05\0\x02s\x01\x12\x04\xb4\x01\x08\x0f\n\r\n\x05\x05\0\x02s\x02\
    \x12\x04\xb4\x01\x12\x16\n\x0c\n\x04\x05\0\x02t\x12\x04\xb5\x01\x08\x16\
    \n\r\n\x05\x05\0\x02t\x01\x12\x04\xb5\x01\x08\x0e\n\r\n\x05\x05\0\x02t\
    \x02\x12\x04\xb5\x01\x11\x15\n\x0c\n\x04\x05\0\x02u\x12\x04\xb6\x01\x08\
    \x17\n\r\n\x05\x05\0\x02u\x01\x12\x04\xb6\x01\x08\x0f\n\r\n\x05\x05\0\
    \x02u\x02\x12\x04\xb6\x01\x12\x16\n\x0c\n\x04\x05\0\x02v\x12\x04\xb7\x01\
    \x08\x14\n\r\n\x05\x05\0\x02v\x01\x12\x04\xb7\x01\x08\x0c\n\r\n\x05\x05\
    \0\x02v\x02\x12\x04\xb7\x01\x0f\x13\n\x0c\n\x04\x05\0\x02w\x12\x04\xb8\
    \x01\x08\x15\n\r\n\x05\x05\0\x02w\x01\x12\x04\xb8\x01\x08\r\n\r\n\x05\
    \x05\0\x02w\x02\x12\x04\xb8\x01\x10\x14\n\x0c\n\x04\x05\0\x02x\x12\x04\
    \xb9\x01\x08\x14\n\r\n\x05\x05\0\x02x\x01\x12\x04\xb9\x01\x08\x0c\n\r\n\
    \x05\x05\0\x02x\x02\x12\x04\xb9\x01\x0f\x13\n\x1f\n\x04\x05\0\x02y\x12\
    \x04\xbc\x01\x08\x1b\x1a\x11\x20Scalar\x20Function\x20\n\r\n\x05\x05\0\
    \x02y\x01\x12\x04\xbc\x01\x08\x12\n\r\n\x05\x05\0\x02y\x02\x12\x04\xbc\
    \x01\x15\x1a\n\x0c\n\x02\x05\x01\x12\x06\xbf\x01\0\x9c\x03\x01\n\x0b\n\
    \x03\x05\x01\x01\x12\x04\xbf\x01\x05\x12\n\x17\n\x04\x05\x01\x02\0\x12\
    \x04\xc1\x01\x08\x19\x1a\t\x20Casting\x20\n\r\n\x05\x05\x01\x02\0\x01\
    \x12\x04\xc1\x01\x08\x14\n\r\n\x05\x05\x01\x02\0\x02\x12\x04\xc1\x01\x17\
    \x18\n\x0c\n\x04\x05\x01\x02\x01\x12\x04\xc2\x01\x08\x1a\n\r\n\x05\x05\
    \x01\x02\x01\x01\x12\x04\xc2\x01\x08\x15\n\r\n\x05\x05\x01\x02\x01\x02\
    \x12\x04\xc2\x01\x18\x19\n\x0c\n\x04\x05\x01\x02\x02\x12\x04\xc3\x01\x08\
    \x1c\n\r\n\x05\x05\x01\x02\x02\x01\x12\x04\xc3\x01\x08\x17\n\r\n\x05\x05\
    \x01\x02\x02\x02\x12\x04\xc3\x01\x1a\x1b\n\x0c\n\x04\x05\x01\x02\x03\x12\
    \x04\xc4\x01\x08\x1d\n\r\n\x05\x05\x01\x02\x03\x01\x12\x04\xc4\x01\x08\
    \x18\n\r\n\x05\x05\x01\x02\x03\x02\x12\x04\xc4\x01\x1b\x1c\n\x0c\n\x04\
    \x05\x01\x02\x04\x12\x04\xc5\x01\x08\x1a\n\r\n\x05\x05\x01\x02\x04\x01\
    \x12\x04\xc5\x01\x08\x15\n\r\n\x05\x05\x01\x02\x04\x02\x12\x04\xc5\x01\
    \x18\x19\n\x0c\n\x04\x05\x01\x02\x05\x12\x04\xc6\x01\x08\x1e\n\r\n\x05\
    \x05\x01\x02\x05\x01\x12\x04\xc6\x01\x08\x19\n\r\n\x05\x05\x01\x02\x05\
    \x02\x12\x04\xc6\x01\x1c\x1d\n\x0c\n\x04\x05\x01\x02\x06\x12\x04\xc7\x01\
    \x08\x1a\n\r\n\x05\x05\x01\x02\x06\x01\x12\x04\xc7\x01\x08\x15\n\r\n\x05\
    \x05\x01\x02\x06\x02\x12\x04\xc7\x01\x18\x19\n\x0c\n\x04\x05\x01\x02\x07\
    \x12\x04\xc9\x01\x08\x1b\n\r\n\x05\x05\x01\x02\x07\x01\x12\x04\xc9\x01\
    \x08\x15\n\r\n\x05\x05\x01\x02\x07\x02\x12\x04\xc9\x01\x18\x1a\n\x0c\n\
    \x04\x05\x01\x02\x08\x12\x04\xca\x01\x08\x1c\n\r\n\x05\x05\x01\x02\x08\
    \x01\x12\x04\xca\x01\x08\x16\n\r\n\x05\x05\x01\x02\x08\x02\x12\x04\xca\
    \x01\x19\x1b\n\x0c\n\x04\x05\x01\x02\t\x12\x04\xcb\x01\x08\x1e\n\r\n\x05\
    \x05\x01\x02\t\x01\x12\x04\xcb\x01\x08\x18\n\r\n\x05\x05\x01\x02\t\x02\
    \x12\x04\xcb\x01\x1b\x1d\n\x0c\n\x04\x05\x01\x02\n\x12\x04\xcc\x01\x08\
    \x1f\n\r\n\x05\x05\x01\x02\n\x01\x12\x04\xcc\x01\x08\x19\n\r\n\x05\x05\
    \x01\x02\n\x02\x12\x04\xcc\x01\x1c\x1e\n\x0c\n\x04\x05\x01\x02\x0b\x12\
    \x04\xcd\x01\x08\x1c\n\r\n\x05\x05\x01\x02\x0b\x01\x12\x04\xcd\x01\x08\
    \x16\n\r\n\x05\x05\x01\x02\x0b\x02\x12\x04\xcd\x01\x19\x1b\n\x0c\n\x04\
    \x05\x01\x02\x0c\x12\x04\xce\x01\x08\x20\n\r\n\x05\x05\x01\x02\x0c\x01\
    \x12\x04\xce\x01\x08\x1a\n\r\n\x05\x05\x01\x02\x0c\x02\x12\x04\xce\x01\
    \x1d\x1f\n\x0c\n\x04\x05\x01\x02\r\x12\x04\xcf\x01\x08\x1c\n\r\n\x05\x05\
    \x01\x02\r\x01\x12\x04\xcf\x01\x08\x16\n\r\n\x05\x05\x01\x02\r\x02\x12\
    \x04\xcf\x01\x19\x1b\n\x0c\n\x04\x05\x01\x02\x0e\x12\x04\xd1\x01\x08\x1e\
    \n\r\n\x05\x05\x01\x02\x0e\x01\x12\x04\xd1\x01\x08\x18\n\r\n\x05\x05\x01\
    \x02\x0e\x02\x12\x04\xd1\x01\x1b\x1d\n\x0c\n\x04\x05\x01\x02\x0f\x12\x04\
    \xd2\x01\x08\x1f\n\r\n\x05\x05\x01\x02\x0f\x01\x12\x04\xd2\x01\x08\x19\n\
    \r\n\x05\x05\x01\x02\x0f\x02\x12\x04\xd2\x01\x1c\x1e\n\x0c\n\x04\x05\x01\
    \x02\x10\x12\x04\xd3\x01\x08!\n\r\n\x05\x05\x01\x02\x10\x01\x12\x04\xd3\
    \x01\x08\x1b\n\r\n\x05\x05\x01\x02\x10\x02\x12\x04\xd3\x01\x1e\x20\n\x0c\
    \n\x04\x05\x01\x02\x11\x12\x04\xd4\x01\x08\"\n\r\n\x05\x05\x01\x02\x11\
    \x01\x12\x04\xd4\x01\x08\x1c\n\r\n\x05\x05\x01\x02\x11\x02\x12\x04\xd4\
    \x01\x1f!\n\x0c\n\x04\x05\x01\x02\x12\x12\x04\xd5\x01\x08\x1f\n\r\n\x05\
    \x05\x01\x02\x12\x01\x12\x04\xd5\x01\x08\x19\n\r\n\x05\x05\x01\x02\x12\
    \x02\x12\x04\xd5\x01\x1c\x1e\n\x0c\n\x04\x05\x01\x02\x13\x12\x04\xd6\x01\
    \x08#\n\r\n\x05\x05\x01\x02\x13\x01\x12\x04\xd6\x01\x08\x1d\n\r\n\x05\
    \x05\x01\x02\x13\x02\x12\x04\xd6\x01\x20\"\n\x0c\n\x04\x05\x01\x02\x14\
    \x12\x04\xd7\x01\x08\x1f\n\r\n\x05\x05\x01\x02\x14\x01\x12\x04\xd7\x01\
    \x08\x19\n\r\n\x05\x05\x01\x02\x14\x02\x12\x04\xd7\x01\x1c\x1e\n\x0c\n\
    \x04\x05\x01\x02\x15\x12\x04\xd9\x01\x08\x1d\n\r\n\x05\x05\x01\x02\x15\
    \x01\x12\x04\xd9\x01\x08\x17\n\r\n\x05\x05\x01\x02\x15\x02\x12\x04\xd9\
    \x01\x1a\x1c\n\x0c\n\x04\x05\x01\x02\x16\x12\x04\xda\x01\x08\x1e\n\r\n\
    \x05\x05\x01\x02\x16\x01\x12\x04\xda\x01\x08\x18\n\r\n\x05\x05\x01\x02\
    \x16\x02\x12\x04\xda\x01\x1b\x1d\n\x0c\n\x04\x05\x01\x02\x17\x12\x04\xdb\
    \x01\x08\x20\n\r\n\x05\x05\x01\x02\x17\x01\x12\x04\xdb\x01\x08\x1a\n\r\n\
    \x05\x05\x01\x02\x17\x02\x12\x04\xdb\x01\x1d\x1f\n\x0c\n\x04\x05\x01\x02\
    \x18\x12\x04\xdc\x01\x08!\n\r\n\x05\x05\x01\x02\x18\x01\x12\x04\xdc\x01\
    \x08\x1b\n\r\n\x05\x05\x01\x02\x18\x02\x12\x04\xdc\x01\x1e\x20\n\x0c\n\
    \x04\x05\x01\x02\x19\x12\x04\xdd\x01\x08\x1e\n\r\n\x05\x05\x01\x02\x19\
    \x01\x12\x04\xdd\x01\x08\x18\n\r\n\x05\x05\x01\x02\x19\x02\x12\x04\xdd\
    \x01\x1b\x1d\n\x0c\n\x04\x05\x01\x02\x1a\x12\x04\xde\x01\x08\"\n\r\n\x05\
    \x05\x01\x02\x1a\x01\x12\x04\xde\x01\x08\x1c\n\r\n\x05\x05\x01\x02\x1a\
    \x02\x12\x04\xde\x01\x1f!\n\x0c\n\x04\x05\x01\x02\x1b\x12\x04\xdf\x01\
    \x08\x1e\n\r\n\x05\x05\x01\x02\x1b\x01\x12\x04\xdf\x01\x08\x18\n\r\n\x05\
    \x05\x01\x02\x1b\x02\x12\x04\xdf\x01\x1b\x1d\n\x0c\n\x04\x05\x01\x02\x1c\
    \x12\x04\xe1\x01\x08\x1b\n\r\n\x05\x05\x01\x02\x1c\x01\x12\x04\xe1\x01\
    \x08\x15\n\r\n\x05\x05\x01\x02\x1c\x02\x12\x04\xe1\x01\x18\x1a\n\x0c\n\
    \x04\x05\x01\x02\x1d\x12\x04\xe2\x01\x08\x1c\n\r\n\x05\x05\x01\x02\x1d\
    \x01\x12\x04\xe2\x01\x08\x16\n\r\n\x05\x05\x01\x02\x1d\x02\x12\x04\xe2\
    \x01\x19\x1b\n\x0c\n\x04\x05\x01\x02\x1e\x12\x04\xe3\x01\x08\x1e\n\r\n\
    \x05\x05\x01\x02\x1e\x01\x12\x04\xe3\x01\x08\x18\n\r\n\x05\x05\x01\x02\
    \x1e\x02\x12\x04\xe3\x01\x1b\x1d\n\x0c\n\x04\x05\x01\x02\x1f\x12\x04\xe4\
    \x01\x08\x1f\n\r\n\x05\x05\x01\x02\x1f\x01\x12\x04\xe4\x01\x08\x19\n\r\n\
    \x05\x05\x01\x02\x1f\x02\x12\x04\xe4\x01\x1c\x1e\n\x0c\n\x04\x05\x01\x02\
    \x20\x12\x04\xe5\x01\x08\x1c\n\r\n\x05\x05\x01\x02\x20\x01\x12\x04\xe5\
    \x01\x08\x16\n\r\n\x05\x05\x01\x02\x20\x02\x12\x04\xe5\x01\x19\x1b\n\x0c\
    \n\x04\x05\x01\x02!\x12\x04\xe6\x01\x08\x20\n\r\n\x05\x05\x01\x02!\x01\
    \x12\x04\xe6\x01\x08\x1a\n\r\n\x05\x05\x01\x02!\x02\x12\x04\xe6\x01\x1d\
    \x1f\n\x0c\n\x04\x05\x01\x02\"\x12\x04\xe7\x01\x08\x1c\n\r\n\x05\x05\x01\
    \x02\"\x01\x12\x04\xe7\x01\x08\x16\n\r\n\x05\x05\x01\x02\"\x02\x12\x04\
    \xe7\x01\x19\x1b\n\x0c\n\x04\x05\x01\x02#\x12\x04\xe9\x01\x08\x1f\n\r\n\
    \x05\x05\x01\x02#\x01\x12\x04\xe9\x01\x08\x19\n\r\n\x05\x05\x01\x02#\x02\
    \x12\x04\xe9\x01\x1c\x1e\n\x0c\n\x04\x05\x01\x02$\x12\x04\xea\x01\x08\
    \x20\n\r\n\x05\x05\x01\x02$\x01\x12\x04\xea\x01\x08\x1a\n\r\n\x05\x05\
    \x01\x02$\x02\x12\x04\xea\x01\x1d\x1f\n\x0c\n\x04\x05\x01\x02%\x12\x04\
    \xeb\x01\x08\"\n\r\n\x05\x05\x01\x02%\x01\x12\x04\xeb\x01\x08\x1c\n\r\n\
    \x05\x05\x01\x02%\x02\x12\x04\xeb\x01\x1f!\n\x0c\n\x04\x05\x01\x02&\x12\
    \x04\xec\x01\x08#\n\r\n\x05\x05\x01\x02&\x01\x12\x04\xec\x01\x08\x1d\n\r\
    \n\x05\x05\x01\x02&\x02\x12\x04\xec\x01\x20\"\n\x0c\n\x04\x05\x01\x02'\
    \x12\x04\xed\x01\x08\x20\n\r\n\x05\x05\x01\x02'\x01\x12\x04\xed\x01\x08\
    \x1a\n\r\n\x05\x05\x01\x02'\x02\x12\x04\xed\x01\x1d\x1f\n\x0c\n\x04\x05\
    \x01\x02(\x12\x04\xee\x01\x08$\n\r\n\x05\x05\x01\x02(\x01\x12\x04\xee\
    \x01\x08\x1e\n\r\n\x05\x05\x01\x02(\x02\x12\x04\xee\x01!#\n\x0c\n\x04\
    \x05\x01\x02)\x12\x04\xef\x01\x08\x20\n\r\n\x05\x05\x01\x02)\x01\x12\x04\
    \xef\x01\x08\x1a\n\r\n\x05\x05\x01\x02)\x02\x12\x04\xef\x01\x1d\x1f\n\
    \x0c\n\x04\x05\x01\x02*\x12\x04\xf1\x01\x08\x1b\n\r\n\x05\x05\x01\x02*\
    \x01\x12\x04\xf1\x01\x08\x15\n\r\n\x05\x05\x01\x02*\x02\x12\x04\xf1\x01\
    \x18\x1a\n\x0c\n\x04\x05\x01\x02+\x12\x04\xf2\x01\x08\x1c\n\r\n\x05\x05\
    \x01\x02+\x01\x12\x04\xf2\x01\x08\x16\n\r\n\x05\x05\x01\x02+\x02\x12\x04\
    \xf2\x01\x19\x1b\n\x0c\n\x04\x05\x01\x02,\x12\x04\xf3\x01\x08\x1e\n\r\n\
    \x05\x05\x01\x02,\x01\x12\x04\xf3\x01\x08\x18\n\r\n\x05\x05\x01\x02,\x02\
    \x12\x04\xf3\x01\x1b\x1d\n\x0c\n\x04\x05\x01\x02-\x12\x04\xf4\x01\x08\
    \x1f\n\r\n\x05\x05\x01\x02-\x01\x12\x04\xf4\x01\x08\x19\n\r\n\x05\x05\
    \x01\x02-\x02\x12\x04\xf4\x01\x1c\x1e\n\x0c\n\x04\x05\x01\x02.\x12\x04\
    \xf5\x01\x08\x1c\n\r\n\x05\x05\x01\x02.\x01\x12\x04\xf5\x01\x08\x16\n\r\
    \n\x05\x05\x01\x02.\x02\x12\x04\xf5\x01\x19\x1b\n\x0c\n\x04\x05\x01\x02/\
    \x12\x04\xf6\x01\x08\x20\n\r\n\x05\x05\x01\x02/\x01\x12\x04\xf6\x01\x08\
    \x1a\n\r\n\x05\x05\x01\x02/\x02\x12\x04\xf6\x01\x1d\x1f\n\x0c\n\x04\x05\
    \x01\x020\x12\x04\xf7\x01\x08\x1c\n\r\n\x05\x05\x01\x020\x01\x12\x04\xf7\
    \x01\x08\x16\n\r\n\x05\x05\x01\x020\x02\x12\x04\xf7\x01\x19\x1b\n\x0c\n\
    \x04\x05\x01\x021\x12\x04\xf9\x01\x08\x14\n\r\n\x05\x05\x01\x021\x01\x12\
    \x04\xf9\x01\x08\r\n\r\n\x05\x05\x01\x021\x02\x12\x04\xf9\x01\x10\x13\n\
    \x0c\n\x04\x05\x01\x022\x12\x04\xfa\x01\x08\x15\n\r\n\x05\x05\x01\x022\
    \x01\x12\x04\xfa\x01\x08\x0e\n\r\n\x05\x05\x01\x022\x02\x12\x04\xfa\x01\
    \x11\x14\n\x0c\n\x04\x05\x01\x023\x12\x04\xfb\x01\x08\x18\n\r\n\x05\x05\
    \x01\x023\x01\x12\x04\xfb\x01\x08\x11\n\r\n\x05\x05\x01\x023\x02\x12\x04\
    \xfb\x01\x14\x17\n\x0c\n\x04\x05\x01\x024\x12\x04\xfc\x01\x08\x17\n\r\n\
    \x05\x05\x01\x024\x01\x12\x04\xfc\x01\x08\x10\n\r\n\x05\x05\x01\x024\x02\
    \x12\x04\xfc\x01\x13\x16\n\x0c\n\x04\x05\x01\x025\x12\x04\xfd\x01\x08\
    \x15\n\r\n\x05\x05\x01\x025\x01\x12\x04\xfd\x01\x08\x0e\n\r\n\x05\x05\
    \x01\x025\x02\x12\x04\xfd\x01\x11\x14\n\x0c\n\x04\x05\x01\x026\x12\x04\
    \xfe\x01\x08\x19\n\r\n\x05\x05\x01\x026\x01\x12\x04\xfe\x01\x08\x12\n\r\
    \n\x05\x05\x01\x026\x02\x12\x04\xfe\x01\x15\x18\n\x0c\n\x04\x05\x01\x027\
    \x12\x04\xff\x01\x08\x15\n\r\n\x05\x05\x01\x027\x01\x12\x04\xff\x01\x08\
    \x0e\n\r\n\x05\x05\x01\x027\x02\x12\x04\xff\x01\x11\x14\n\x0c\n\x04\x05\
    \x01\x028\x12\x04\x81\x02\x08\x14\n\r\n\x05\x05\x01\x028\x01\x12\x04\x81\
    \x02\x08\r\n\r\n\x05\x05\x01\x028\x02\x12\x04\x81\x02\x10\x13\n\x0c\n\
    \x04\x05\x01\x029\x12\x04\x82\x02\x08\x15\n\r\n\x05\x05\x01\x029\x01\x12\
    \x04\x82\x02\x08\x0e\n\r\n\x05\x05\x01\x029\x02\x12\x04\x82\x02\x11\x14\
    \n\x0c\n\x04\x05\x01\x02:\x12\x04\x83\x02\x08\x18\n\r\n\x05\x05\x01\x02:\
    \x01\x12\x04\x83\x02\x08\x11\n\r\n\x05\x05\x01\x02:\x02\x12\x04\x83\x02\
    \x14\x17\n\x0c\n\x04\x05\x01\x02;\x12\x04\x84\x02\x08\x17\n\r\n\x05\x05\
    \x01\x02;\x01\x12\x04\x84\x02\x08\x10\n\r\n\x05\x05\x01\x02;\x02\x12\x04\
    \x84\x02\x13\x16\n\x0c\n\x04\x05\x01\x02<\x12\x04\x85\x02\x08\x15\n\r\n\
    \x05\x05\x01\x02<\x01\x12\x04\x85\x02\x08\x0e\n\r\n\x05\x05\x01\x02<\x02\
    \x12\x04\x85\x02\x11\x14\n\x0c\n\x04\x05\x01\x02=\x12\x04\x86\x02\x08\
    \x19\n\r\n\x05\x05\x01\x02=\x01\x12\x04\x86\x02\x08\x12\n\r\n\x05\x05\
    \x01\x02=\x02\x12\x04\x86\x02\x15\x18\n\x0c\n\x04\x05\x01\x02>\x12\x04\
    \x87\x02\x08\x15\n\r\n\x05\x05\x01\x02>\x01\x12\x04\x87\x02\x08\x0e\n\r\
    \n\x05\x05\x01\x02>\x02\x12\x04\x87\x02\x11\x14\n\x0c\n\x04\x05\x01\x02?\
    \x12\x04\x89\x02\x08\x14\n\r\n\x05\x05\x01\x02?\x01\x12\x04\x89\x02\x08\
    \r\n\r\n\x05\x05\x01\x02?\x02\x12\x04\x89\x02\x10\x13\n\x0c\n\x04\x05\
    \x01\x02@\x12\x04\x8a\x02\x08\x15\n\r\n\x05\x05\x01\x02@\x01\x12\x04\x8a\
    \x02\x08\x0e\n\r\n\x05\x05\x01\x02@\x02\x12\x04\x8a\x02\x11\x14\n\x0c\n\
    \x04\x05\x01\x02A\x12\x04\x8b\x02\x08\x18\n\r\n\x05\x05\x01\x02A\x01\x12\
    \x04\x8b\x02\x08\x11\n\r\n\x05\x05\x01\x02A\x02\x12\x04\x8b\x02\x14\x17\
    \n\x0c\n\x04\x05\x01\x02B\x12\x04\x8c\x02\x08\x17\n\r\n\x05\x05\x01\x02B\
    \x01\x12\x04\x8c\x02\x08\x10\n\r\n\x05\x05\x01\x02B\x02\x12\x04\x8c\x02\
    \x13\x16\n\x0c\n\x04\x05\x01\x02C\x12\x04\x8d\x02\x08\x15\n\r\n\x05\x05\
    \x01\x02C\x01\x12\x04\x8d\x02\x08\x0e\n\r\n\x05\x05\x01\x02C\x02\x12\x04\
    \x8d\x02\x11\x14\n\x0c\n\x04\x05\x01\x02D\x12\x04\x8e\x02\x08\x19\n\r\n\
    \x05\x05\x01\x02D\x01\x12\x04\x8e\x02\x08\x12\n\r\n\x05\x05\x01\x02D\x02\
    \x12\x04\x8e\x02\x15\x18\n\x0c\n\x04\x05\x01\x02E\x12\x04\x8f\x02\x08\
    \x15\n\r\n\x05\x05\x01\x02E\x01\x12\x04\x8f\x02\x08\x0e\n\r\n\x05\x05\
    \x01\x02E\x02\x12\x04\x8f\x02\x11\x14\n\x0c\n\x04\x05\x01\x02F\x12\x04\
    \x91\x02\x08\x14\n\r\n\x05\x05\x01\x02F\x01\x12\x04\x91\x02\x08\r\n\r\n\
    \x05\x05\x01\x02F\x02\x12\x04\x91\x02\x10\x13\n\x0c\n\x04\x05\x01\x02G\
    \x12\x04\x92\x02\x08\x15\n\r\n\x05\x05\x01\x02G\x01\x12\x04\x92\x02\x08\
    \x0e\n\r\n\x05\x05\x01\x02G\x02\x12\x04\x92\x02\x11\x14\n\x0c\n\x04\x05\
    \x01\x02H\x12\x04\x93\x02\x08\x18\n\r\n\x05\x05\x01\x02H\x01\x12\x04\x93\
    \x02\x08\x11\n\r\n\x05\x05\x01\x02H\x02\x12\x04\x93\x02\x14\x17\n\x0c\n\
    \x04\x05\x01\x02I\x12\x04\x94\x02\x08\x17\n\r\n\x05\x05\x01\x02I\x01\x12\
    \x04\x94\x02\x08\x10\n\r\n\x05\x05\x01\x02I\x02\x12\x04\x94\x02\x13\x16\
    \n\x0c\n\x04\x05\x01\x02J\x12\x04\x95\x02\x08\x15\n\r\n\x05\x05\x01\x02J\
    \x01\x12\x04\x95\x02\x08\x0e\n\r\n\x05\x05\x01\x02J\x02\x12\x04\x95\x02\
    \x11\x14\n\x0c\n\x04\x05\x01\x02K\x12\x04\x96\x02\x08\x19\n\r\n\x05\x05\
    \x01\x02K\x01\x12\x04\x96\x02\x08\x12\n\r\n\x05\x05\x01\x02K\x02\x12\x04\
    \x96\x02\x15\x18\n\x0c\n\x04\x05\x01\x02L\x12\x04\x97\x02\x08\x15\n\r\n\
    \x05\x05\x01\x02L\x01\x12\x04\x97\x02\x08\x0e\n\r\n\x05\x05\x01\x02L\x02\
    \x12\x04\x97\x02\x11\x14\n\x0c\n\x04\x05\x01\x02M\x12\x04\x99\x02\x08\
    \x14\n\r\n\x05\x05\x01\x02M\x01\x12\x04\x99\x02\x08\r\n\r\n\x05\x05\x01\
    \x02M\x02\x12\x04\x99\x02\x10\x13\n\x0c\n\x04\x05\x01\x02N\x12\x04\x9a\
    \x02\x08\x15\n\r\n\x05\x05\x01\x02N\x01\x12\x04\x9a\x02\x08\x0e\n\r\n\
    \x05\x05\x01\x02N\x02\x12\x04\x9a\x02\x11\x14\n\x0c\n\x04\x05\x01\x02O\
    \x12\x04\x9b\x02\x08\x18\n\r\n\x05\x05\x01\x02O\x01\x12\x04\x9b\x02\x08\
    \x11\n\r\n\x05\x05\x01\x02O\x02\x12\x04\x9b\x02\x14\x17\n\x0c\n\x04\x05\
    \x01\x02P\x12\x04\x9c\x02\x08\x17\n\r\n\x05\x05\x01\x02P\x01\x12\x04\x9c\
    \x02\x08\x10\n\r\n\x05\x05\x01\x02P\x02\x12\x04\x9c\x02\x13\x16\n\x0c\n\
    \x04\x05\x01\x02Q\x12\x04\x9d\x02\x08\x15\n\r\n\x05\x05\x01\x02Q\x01\x12\
    \x04\x9d\x02\x08\x0e\n\r\n\x05\x05\x01\x02Q\x02\x12\x04\x9d\x02\x11\x14\
    \n\x0c\n\x04\x05\x01\x02R\x12\x04\x9e\x02\x08\x19\n\r\n\x05\x05\x01\x02R\
    \x01\x12\x04\x9e\x02\x08\x12\n\r\n\x05\x05\x01\x02R\x02\x12\x04\x9e\x02\
    \x15\x18\n\x0c\n\x04\x05\x01\x02S\x12\x04\x9f\x02\x08\x15\n\r\n\x05\x05\
    \x01\x02S\x01\x12\x04\x9f\x02\x08\x0e\n\r\n\x05\x05\x01\x02S\x02\x12\x04\
    \x9f\x02\x11\x14\n\x0c\n\x04\x05\x01\x02T\x12\x04\xa1\x02\x08\x14\n\r\n\
    \x05\x05\x01\x02T\x01\x12\x04\xa1\x02\x08\r\n\r\n\x05\x05\x01\x02T\x02\
    \x12\x04\xa1\x02\x10\x13\n\x0c\n\x04\x05\x01\x02U\x12\x04\xa2\x02\x08\
    \x15\n\r\n\x05\x05\x01\x02U\x01\x12\x04\xa2\x02\x08\x0e\n\r\n\x05\x05\
    \x01\x02U\x02\x12\x04\xa2\x02\x11\x14\n\x0c\n\x04\x05\x01\x02V\x12\x04\
    \xa3\x02\x08\x18\n\r\n\x05\x05\x01\x02V\x01\x12\x04\xa3\x02\x08\x11\n\r\
    \n\x05\x05\x01\x02V\x02\x12\x04\xa3\x02\x14\x17\n\x0c\n\x04\x05\x01\x02W\
    \x12\x04\xa4\x02\x08\x17\n\r\n\x05\x05\x01\x02W\x01\x12\x04\xa4\x02\x08\
    \x10\n\r\n\x05\x05\x01\x02W\x02\x12\x04\xa4\x02\x13\x16\n\x0c\n\x04\x05\
    \x01\x02X\x12\x04\xa5\x02\x08\x15\n\r\n\x05\x05\x01\x02X\x01\x12\x04\xa5\
    \x02\x08\x0e\n\r\n\x05\x05\x01\x02X\x02\x12\x04\xa5\x02\x11\x14\n\x0c\n\
    \x04\x05\x01\x02Y\x12\x04\xa6\x02\x08\x19\n\r\n\x05\x05\x01\x02Y\x01\x12\
    \x04\xa6\x02\x08\x12\n\r\n\x05\x05\x01\x02Y\x02\x12\x04\xa6\x02\x15\x18\
    \n\x0c\n\x04\x05\x01\x02Z\x12\x04\xa7\x02\x08\x15\n\r\n\x05\x05\x01\x02Z\
    \x01\x12\x04\xa7\x02\x08\x0e\n\r\n\x05\x05\x01\x02Z\x02\x12\x04\xa7\x02\
    \x11\x14\n\x0c\n\x04\x05\x01\x02[\x12\x04\xa9\x02\x08\x18\n\r\n\x05\x05\
    \x01\x02[\x01\x12\x04\xa9\x02\x08\x11\n\r\n\x05\x05\x01\x02[\x02\x12\x04\
    \xa9\x02\x14\x17\n\x0c\n\x04\x05\x01\x02\\\x12\x04\xaa\x02\x08\x19\n\r\n\
    \x05\x05\x01\x02\\\x01\x12\x04\xaa\x02\x08\x12\n\r\n\x05\x05\x01\x02\\\
    \x02\x12\x04\xaa\x02\x15\x18\n\x0c\n\x04\x05\x01\x02]\x12\x04\xab\x02\
    \x08\x1c\n\r\n\x05\x05\x01\x02]\x01\x12\x04\xab\x02\x08\x15\n\r\n\x05\
    \x05\x01\x02]\x02\x12\x04\xab\x02\x18\x1b\n\x0c\n\x04\x05\x01\x02^\x12\
    \x04\xac\x02\x08\x1b\n\r\n\x05\x05\x01\x02^\x01\x12\x04\xac\x02\x08\x14\
    \n\r\n\x05\x05\x01\x02^\x02\x12\x04\xac\x02\x17\x1a\n\x0c\n\x04\x05\x01\
    \x02_\x12\x04\xad\x02\x08\x19\n\r\n\x05\x05\x01\x02_\x01\x12\x04\xad\x02\
    \x08\x12\n\r\n\x05\x05\x01\x02_\x02\x12\x04\xad\x02\x15\x18\n\x0c\n\x04\
    \x05\x01\x02`\x12\x04\xae\x02\x08\x1d\n\r\n\x05\x05\x01\x02`\x01\x12\x04\
    \xae\x02\x08\x16\n\r\n\x05\x05\x01\x02`\x02\x12\x04\xae\x02\x19\x1c\n\
    \x0c\n\x04\x05\x01\x02a\x12\x04\xaf\x02\x08\x19\n\r\n\x05\x05\x01\x02a\
    \x01\x12\x04\xaf\x02\x08\x12\n\r\n\x05\x05\x01\x02a\x02\x12\x04\xaf\x02\
    \x15\x18\n\x0c\n\x04\x05\x01\x02b\x12\x04\xb1\x02\x08\x17\n\r\n\x05\x05\
    \x01\x02b\x01\x12\x04\xb1\x02\x08\x10\n\r\n\x05\x05\x01\x02b\x02\x12\x04\
    \xb1\x02\x13\x16\n\x0c\n\x04\x05\x01\x02c\x12\x04\xb2\x02\x08\x1a\n\r\n\
    \x05\x05\x01\x02c\x01\x12\x04\xb2\x02\x08\x13\n\r\n\x05\x05\x01\x02c\x02\
    \x12\x04\xb2\x02\x16\x19\n\x0c\n\x04\x05\x01\x02d\x12\x04\xb3\x02\x08\
    \x16\n\r\n\x05\x05\x01\x02d\x01\x12\x04\xb3\x02\x08\x0f\n\r\n\x05\x05\
    \x01\x02d\x02\x12\x04\xb3\x02\x12\x15\n\x0c\n\x04\x05\x01\x02e\x12\x04\
    \xb4\x02\x08\x18\n\r\n\x05\x05\x01\x02e\x01\x12\x04\xb4\x02\x08\x11\n\r\
    \n\x05\x05\x01\x02e\x02\x12\x04\xb4\x02\x14\x17\n\x0c\n\x04\x05\x01\x02f\
    \x12\x04\xb5\x02\x08\x1b\n\r\n\x05\x05\x01\x02f\x01\x12\x04\xb5\x02\x08\
    \x14\n\r\n\x05\x05\x01\x02f\x02\x12\x04\xb5\x02\x17\x1a\n\x0c\n\x04\x05\
    \x01\x02g\x12\x04\xb6\x02\x08\x17\n\r\n\x05\x05\x01\x02g\x01\x12\x04\xb6\
    \x02\x08\x10\n\r\n\x05\x05\x01\x02g\x02\x12\x04\xb6\x02\x13\x16\n\x0c\n\
    \x04\x05\x01\x02h\x12\x04\xb7\x02\x08\x1b\n\r\n\x05\x05\x01\x02h\x01\x12\
    \x04\xb7\x02\x08\x14\n\r\n\x05\x05\x01\x02h\x02\x12\x04\xb7\x02\x17\x1a\
    \n\x0c\n\x04\x05\x01\x02i\x12\x04\xb8\x02\x08\x1e\n\r\n\x05\x05\x01\x02i\
    \x01\x12\x04\xb8\x02\x08\x17\n\r\n\x05\x05\x01\x02i\x02\x12\x04\xb8\x02\
    \x1a\x1d\n\x0c\n\x04\x05\x01\x02j\x12\x04\xb9\x02\x08\x1a\n\r\n\x05\x05\
    \x01\x02j\x01\x12\x04\xb9\x02\x08\x13\n\r\n\x05\x05\x01\x02j\x02\x12\x04\
    \xb9\x02\x16\x19\n\x0c\n\x04\x05\x01\x02k\x12\x04\xba\x02\x08\x19\n\r\n\
    \x05\x05\x01\x02k\x01\x12\x04\xba\x02\x08\x12\n\r\n\x05\x05\x01\x02k\x02\
    \x12\x04\xba\x02\x15\x18\n\x0c\n\x04\x05\x01\x02l\x12\x04\xbb\x02\x08\
    \x1c\n\r\n\x05\x05\x01\x02l\x01\x12\x04\xbb\x02\x08\x15\n\r\n\x05\x05\
    \x01\x02l\x02\x12\x04\xbb\x02\x18\x1b\n\x0c\n\x04\x05\x01\x02m\x12\x04\
    \xbd\x02\x08\x16\n\r\n\x05\x05\x01\x02m\x01\x12\x04\xbd\x02\x08\x0e\n\r\
    \n\x05\x05\x01\x02m\x02\x12\x04\xbd\x02\x11\x15\n\x0c\n\x04\x05\x01\x02n\
    \x12\x04\xbe\x02\x08\x17\n\r\n\x05\x05\x01\x02n\x01\x12\x04\xbe\x02\x08\
    \x0f\n\r\n\x05\x05\x01\x02n\x02\x12\x04\xbe\x02\x12\x16\n\x0c\n\x04\x05\
    \x01\x02o\x12\x04\xbf\x02\x08\x17\n\r\n\x05\x05\x01\x02o\x01\x12\x04\xbf\
    \x02\x08\x0f\n\r\n\x05\x05\x01\x02o\x02\x12\x04\xbf\x02\x12\x16\n\x0c\n\
    \x04\x05\x01\x02p\x12\x04\xc0\x02\x08\x1a\n\r\n\x05\x05\x01\x02p\x01\x12\
    \x04\xc0\x02\x08\x12\n\r\n\x05\x05\x01\x02p\x02\x12\x04\xc0\x02\x15\x19\
    \n\x0c\n\x04\x05\x01\x02q\x12\x04\xc1\x02\x08\x1c\n\r\n\x05\x05\x01\x02q\
    \x01\x12\x04\xc1\x02\x08\x14\n\r\n\x05\x05\x01\x02q\x02\x12\x04\xc1\x02\
    \x17\x1b\n\x0c\n\x04\x05\x01\x02r\x12\x04\xc2\x02\x08\x1c\n\r\n\x05\x05\
    \x01\x02r\x01\x12\x04\xc2\x02\x08\x14\n\r\n\x05\x05\x01\x02r\x02\x12\x04\
    \xc2\x02\x17\x1b\n\x0c\n\x04\x05\x01\x02s\x12\x04\xc3\x02\x08\x1c\n\r\n\
    \x05\x05\x01\x02s\x01\x12\x04\xc3\x02\x08\x14\n\r\n\x05\x05\x01\x02s\x02\
    \x12\x04\xc3\x02\x17\x1b\n\x0c\n\x04\x05\x01\x02t\x12\x04\xc4\x02\x08\
    \x1c\n\r\n\x05\x05\x01\x02t\x01\x12\x04\xc4\x02\x08\x14\n\r\n\x05\x05\
    \x01\x02t\x02\x12\x04\xc4\x02\x17\x1b\n\x0c\n\x04\x05\x01\x02u\x12\x04\
    \xc5\x02\x08\x18\n\r\n\x05\x05\x01\x02u\x01\x12\x04\xc5\x02\x08\x10\n\r\
    \n\x05\x05\x01\x02u\x02\x12\x04\xc5\x02\x13\x17\n\x0c\n\x04\x05\x01\x02v\
    \x12\x04\xc6\x02\x08\x1d\n\r\n\x05\x05\x01\x02v\x01\x12\x04\xc6\x02\x08\
    \x15\n\r\n\x05\x05\x01\x02v\x02\x12\x04\xc6\x02\x18\x1c\n\x0c\n\x04\x05\
    \x01\x02w\x12\x04\xc7\x02\x08\x1d\n\r\n\x05\x05\x01\x02w\x01\x12\x04\xc7\
    \x02\x08\x15\n\r\n\x05\x05\x01\x02w\x02\x12\x04\xc7\x02\x18\x1c\n\x0c\n\
    \x04\x05\x01\x02x\x12\x04\xc8\x02\x08\x1d\n\r\n\x05\x05\x01\x02x\x01\x12\
    \x04\xc8\x02\x08\x15\n\r\n\x05\x05\x01\x02x\x02\x12\x04\xc8\x02\x18\x1c\
    \n\x0c\n\x04\x05\x01\x02y\x12\x04\xc9\x02\x08\x1d\n\r\n\x05\x05\x01\x02y\
    \x01\x12\x04\xc9\x02\x08\x15\n\r\n\x05\x05\x01\x02y\x02\x12\x04\xc9\x02\
    \x18\x1c\n\x0c\n\x04\x05\x01\x02z\x12\x04\xca\x02\x08\x19\n\r\n\x05\x05\
    \x01\x02z\x01\x12\x04\xca\x02\x08\x11\n\r\n\x05\x05\x01\x02z\x02\x12\x04\
    \xca\x02\x14\x18\n\x0c\n\x04\x05\x01\x02{\x12\x04\xcc\x02\x08\x1a\n\r\n\
    \x05\x05\x01\x02{\x01\x12\x04\xcc\x02\x08\x12\n\r\n\x05\x05\x01\x02{\x02\
    \x12\x04\xcc\x02\x15\x19\n\x0c\n\x04\x05\x01\x02|\x12\x04\xcd\x02\x08\
    \x19\n\r\n\x05\x05\x01\x02|\x01\x12\x04\xcd\x02\x08\x11\n\r\n\x05\x05\
    \x01\x02|\x02\x12\x04\xcd\x02\x14\x18\n\x0c\n\x04\x05\x01\x02}\x12\x04\
    \xce\x02\x08\x1a\n\r\n\x05\x05\x01\x02}\x01\x12\x04\xce\x02\x08\x12\n\r\
    \n\x05\x05\x01\x02}\x02\x12\x04\xce\x02\x15\x19\n\x0c\n\x04\x05\x01\x02~\
    \x12\x04\xcf\x02\x08\x18\n\r\n\x05\x05\x01\x02~\x01\x12\x04\xcf\x02\x08\
    \x10\n\r\n\x05\x05\x01\x02~\x02\x12\x04\xcf\x02\x13\x17\n\x0c\n\x04\x05\
    \x01\x02\x7f\x12\x04\xd0\x02\x08\x1d\n\r\n\x05\x05\x01\x02\x7f\x01\x12\
    \x04\xd0\x02\x08\x15\n\r\n\x05\x05\x01\x02\x7f\x02\x12\x04\xd0\x02\x18\
    \x1c\n\r\n\x05\x05\x01\x02\x80\x01\x12\x04\xd1\x02\x08\x1e\n\x0e\n\x06\
    \x05\x01\x02\x80\x01\x01\x12\x04\xd1\x02\x08\x16\n\x0e\n\x06\x05\x01\x02\
    \x80\x01\x02\x12\x04\xd1\x02\x19\x1d\n\r\n\x05\x05\x01\x02\x81\x01\x12\
    \x04\xd2\x02\x08!\n\x0e\n\x06\x05\x01\x02\x81\x01\x01\x12\x04\xd2\x02\
    \x08\x19\n\x0e\n\x06\x05\x01\x02\x81\x01\x02\x12\x04\xd2\x02\x1c\x20\n\r\
    \n\x05\x05\x01\x02\x82\x01\x12\x04\xd3\x02\x08\x1d\n\x0e\n\x06\x05\x01\
    \x02\x82\x01\x01\x12\x04\xd3\x02\x08\x15\n\x0e\n\x06\x05\x01\x02\x82\x01\
    \x02\x12\x04\xd3\x02\x18\x1c\n\r\n\x05\x05\x01\x02\x83\x01\x12\x04\xd4\
    \x02\x08\x1e\n\x0e\n\x06\x05\x01\x02\x83\x01\x01\x12\x04\xd4\x02\x08\x16\
    \n\x0e\n\x06\x05\x01\x02\x83\x01\x02\x12\x04\xd4\x02\x19\x1d\n\r\n\x05\
    \x05\x01\x02\x84\x01\x12\x04\xd5\x02\x08\x1a\n\x0e\n\x06\x05\x01\x02\x84\
    \x01\x01\x12\x04\xd5\x02\x08\x12\n\x0e\n\x06\x05\x01\x02\x84\x01\x02\x12\
    \x04\xd5\x02\x15\x19\n\r\n\x05\x05\x01\x02\x85\x01\x12\x04\xd6\x02\x08\
    \x1c\n\x0e\n\x06\x05\x01\x02\x85\x01\x01\x12\x04\xd6\x02\x08\x14\n\x0e\n\
    \x06\x05\x01\x02\x85\x01\x02\x12\x04\xd6\x02\x17\x1b\n\r\n\x05\x05\x01\
    \x02\x86\x01\x12\x04\xd7\x02\x08\x1a\n\x0e\n\x06\x05\x01\x02\x86\x01\x01\
    \x12\x04\xd7\x02\x08\x12\n\x0e\n\x06\x05\x01\x02\x86\x01\x02\x12\x04\xd7\
    \x02\x15\x19\n\r\n\x05\x05\x01\x02\x87\x01\x12\x04\xd8\x02\x08\x19\n\x0e\
    \n\x06\x05\x01\x02\x87\x01\x01\x12\x04\xd8\x02\x08\x11\n\x0e\n\x06\x05\
    \x01\x02\x87\x01\x02\x12\x04\xd8\x02\x14\x18\n\r\n\x05\x05\x01\x02\x88\
    \x01\x12\x04\xd9\x02\x08\x1a\n\x0e\n\x06\x05\x01\x02\x88\x01\x01\x12\x04\
    \xd9\x02\x08\x12\n\x0e\n\x06\x05\x01\x02\x88\x01\x02\x12\x04\xd9\x02\x15\
    \x19\n\r\n\x05\x05\x01\x02\x89\x01\x12\x04\xda\x02\x08\x19\n\x0e\n\x06\
    \x05\x01\x02\x89\x01\x01\x12\x04\xda\x02\x08\x11\n\x0e\n\x06\x05\x01\x02\
    \x89\x01\x02\x12\x04\xda\x02\x14\x18\n\r\n\x05\x05\x01\x02\x8a\x01\x12\
    \x04\xdb\x02\x08\x18\n\x0e\n\x06\x05\x01\x02\x8a\x01\x01\x12\x04\xdb\x02\
    \x08\x10\n\x0e\n\x06\x05\x01\x02\x8a\x01\x02\x12\x04\xdb\x02\x13\x17\n\r\
    \n\x05\x05\x01\x02\x8b\x01\x12\x04\xdc\x02\x08\x19\n\x0e\n\x06\x05\x01\
    \x02\x8b\x01\x01\x12\x04\xdc\x02\x08\x11\n\x0e\n\x06\x05\x01\x02\x8b\x01\
    \x02\x12\x04\xdc\x02\x14\x18\n\r\n\x05\x05\x01\x02\x8c\x01\x12\x04\xdd\
    \x02\x08\x19\n\x0e\n\x06\x05\x01\x02\x8c\x01\x01\x12\x04\xdd\x02\x08\x11\
    \n\x0e\n\x06\x05\x01\x02\x8c\x01\x02\x12\x04\xdd\x02\x14\x18\n\r\n\x05\
    \x05\x01\x02\x8d\x01\x12\x04\xde\x02\x08\x19\n\x0e\n\x06\x05\x01\x02\x8d\
    \x01\x01\x12\x04\xde\x02\x08\x11\n\x0e\n\x06\x05\x01\x02\x8d\x01\x02\x12\
    \x04\xde\x02\x14\x18\n\r\n\x05\x05\x01\x02\x8e\x01\x12\x04\xdf\x02\x08\
    \x1a\n\x0e\n\x06\x05\x01\x02\x8e\x01\x01\x12\x04\xdf\x02\x08\x12\n\x0e\n\
    \x06\x05\x01\x02\x8e\x01\x02\x12\x04\xdf\x02\x15\x19\n\r\n\x05\x05\x01\
    \x02\x8f\x01\x12\x04\xe0\x02\x08\x1d\n\x0e\n\x06\x05\x01\x02\x8f\x01\x01\
    \x12\x04\xe0\x02\x08\x15\n\x0e\n\x06\x05\x01\x02\x8f\x01\x02\x12\x04\xe0\
    \x02\x18\x1c\n\r\n\x05\x05\x01\x02\x90\x01\x12\x04\xe1\x02\x08\x1a\n\x0e\
    \n\x06\x05\x01\x02\x90\x01\x01\x12\x04\xe1\x02\x08\x12\n\x0e\n\x06\x05\
    \x01\x02\x90\x01\x02\x12\x04\xe1\x02\x15\x19\n\r\n\x05\x05\x01\x02\x91\
    \x01\x12\x04\xe2\x02\x08\x1b\n\x0e\n\x06\x05\x01\x02\x91\x01\x01\x12\x04\
    \xe2\x02\x08\x13\n\x0e\n\x06\x05\x01\x02\x91\x01\x02\x12\x04\xe2\x02\x16\
    \x1a\n\r\n\x05\x05\x01\x02\x92\x01\x12\x04\xe3\x02\x08\x1e\n\x0e\n\x06\
    \x05\x01\x02\x92\x01\x01\x12\x04\xe3\x02\x08\x16\n\x0e\n\x06\x05\x01\x02\
    \x92\x01\x02\x12\x04\xe3\x02\x19\x1d\n\r\n\x05\x05\x01\x02\x93\x01\x12\
    \x04\xe5\x02\x08\x15\n\x0e\n\x06\x05\x01\x02\x93\x01\x01\x12\x04\xe5\x02\
    \x08\r\n\x0e\n\x06\x05\x01\x02\x93\x01\x02\x12\x04\xe5\x02\x10\x14\n\r\n\
    \x05\x05\x01\x02\x94\x01\x12\x04\xe6\x02\x08\x16\n\x0e\n\x06\x05\x01\x02\
    \x94\x01\x01\x12\x04\xe6\x02\x08\x0e\n\x0e\n\x06\x05\x01\x02\x94\x01\x02\
    \x12\x04\xe6\x02\x11\x15\n\r\n\x05\x05\x01\x02\x95\x01\x12\x04\xe7\x02\
    \x08\x19\n\x0e\n\x06\x05\x01\x02\x95\x01\x01\x12\x04\xe7\x02\x08\x11\n\
    \x0e\n\x06\x05\x01\x02\x95\x01\x02\x12\x04\xe7\x02\x14\x18\n\r\n\x05\x05\
    \x01\x02\x96\x01\x12\x04\xe8\x02\x08\x18\n\x0e\n\x06\x05\x01\x02\x96\x01\
    \x01\x12\x04\xe8\x02\x08\x10\n\x0e\n\x06\x05\x01\x02\x96\x01\x02\x12\x04\
    \xe8\x02\x13\x17\n\r\n\x05\x05\x01\x02\x97\x01\x12\x04\xe9\x02\x08\x16\n\
    \x0e\n\x06\x05\x01\x02\x97\x01\x01\x12\x04\xe9\x02\x08\x0e\n\x0e\n\x06\
    \x05\x01\x02\x97\x01\x02\x12\x04\xe9\x02\x11\x15\n\r\n\x05\x05\x01\x02\
    \x98\x01\x12\x04\xea\x02\x08\x1a\n\x0e\n\x06\x05\x01\x02\x98\x01\x01\x12\
    \x04\xea\x02\x08\x12\n\x0e\n\x06\x05\x01\x02\x98\x01\x02\x12\x04\xea\x02\
    \x15\x19\n\r\n\x05\x05\x01\x02\x99\x01\x12\x04\xeb\x02\x08\x16\n\x0e\n\
    \x06\x05\x01\x02\x99\x01\x01\x12\x04\xeb\x02\x08\x0e\n\x0e\n\x06\x05\x01\
    \x02\x99\x01\x02\x12\x04\xeb\x02\x11\x15\n\r\n\x05\x05\x01\x02\x9a\x01\
    \x12\x04\xed\x02\x08\x19\n\x0e\n\x06\x05\x01\x02\x9a\x01\x01\x12\x04\xed\
    \x02\x08\x11\n\x0e\n\x06\x05\x01\x02\x9a\x01\x02\x12\x04\xed\x02\x14\x18\
    \n\r\n\x05\x05\x01\x02\x9b\x01\x12\x04\xee\x02\x08\x1a\n\x0e\n\x06\x05\
    \x01\x02\x9b\x01\x01\x12\x04\xee\x02\x08\x12\n\x0e\n\x06\x05\x01\x02\x9b\
    \x01\x02\x12\x04\xee\x02\x15\x19\n\r\n\x05\x05\x01\x02\x9c\x01\x12\x04\
    \xef\x02\x08\x1d\n\x0e\n\x06\x05\x01\x02\x9c\x01\x01\x12\x04\xef\x02\x08\
    \x15\n\x0e\n\x06\x05\x01\x02\x9c\x01\x02\x12\x04\xef\x02\x18\x1c\n\r\n\
    \x05\x05\x01\x02\x9d\x01\x12\x04\xf0\x02\x08\x1c\n\x0e\n\x06\x05\x01\x02\
    \x9d\x01\x01\x12\x04\xf0\x02\x08\x14\n\x0e\n\x06\x05\x01\x02\x9d\x01\x02\
    \x12\x04\xf0\x02\x17\x1b\n\r\n\x05\x05\x01\x02\x9e\x01\x12\x04\xf1\x02\
    \x08\x1a\n\x0e\n\x06\x05\x01\x02\x9e\x01\x01\x12\x04\xf1\x02\x08\x12\n\
    \x0e\n\x06\x05\x01\x02\x9e\x01\x02\x12\x04\xf1\x02\x15\x19\n\r\n\x05\x05\
    \x01\x02\x9f\x01\x12\x04\xf2\x02\x08\x1e\n\x0e\n\x06\x05\x01\x02\x9f\x01\
    \x01\x12\x04\xf2\x02\x08\x16\n\x0e\n\x06\x05\x01\x02\x9f\x01\x02\x12\x04\
    \xf2\x02\x19\x1d\n\r\n\x05\x05\x01\x02\xa0\x01\x12\x04\xf3\x02\x08\x15\n\
    \x0e\n\x06\x05\x01\x02\xa0\x01\x01\x12\x04\xf3\x02\x08\r\n\x0e\n\x06\x05\
    \x01\x02\xa0\x01\x02\x12\x04\xf3\x02\x10\x14\n\r\n\x05\x05\x01\x02\xa1\
    \x01\x12\x04\xf4\x02\x08\x16\n\x0e\n\x06\x05\x01\x02\xa1\x01\x01\x12\x04\
    \xf4\x02\x08\x0e\n\x0e\n\x06\x05\x01\x02\xa1\x01\x02\x12\x04\xf4\x02\x11\
    \x15\n\r\n\x05\x05\x01\x02\xa2\x01\x12\x04\xf5\x02\x08\x19\n\x0e\n\x06\
    \x05\x01\x02\xa2\x01\x01\x12\x04\xf5\x02\x08\x11\n\x0e\n\x06\x05\x01\x02\
    \xa2\x01\x02\x12\x04\xf5\x02\x14\x18\n\r\n\x05\x05\x01\x02\xa3\x01\x12\
    \x04\xf6\x02\x08\x18\n\x0e\n\x06\x05\x01\x02\xa3\x01\x01\x12\x04\xf6\x02\
    \x08\x10\n\x0e\n\x06\x05\x01\x02\xa3\x01\x02\x12\x04\xf6\x02\x13\x17\n\r\
    \n\x05\x05\x01\x02\xa4\x01\x12\x04\xf7\x02\x08\x16\n\x0e\n\x06\x05\x01\
    \x02\xa4\x01\x01\x12\x04\xf7\x02\x08\x0e\n\x0e\n\x06\x05\x01\x02\xa4\x01\
    \x02\x12\x04\xf7\x02\x11\x15\n\r\n\x05\x05\x01\x02\xa5\x01\x12\x04\xf8\
    \x02\x08\x1a\n\x0e\n\x06\x05\x01\x02\xa5\x01\x01\x12\x04\xf8\x02\x08\x12\
    \n\x0e\n\x06\x05\x01\x02\xa5\x01\x02\x12\x04\xf8\x02\x15\x19\n\r\n\x05\
    \x05\x01\x02\xa6\x01\x12\x04\xf9\x02\x08\x1a\n\x0e\n\x06\x05\x01\x02\xa6\
    \x01\x01\x12\x04\xf9\x02\x08\x12\n\x0e\n\x06\x05\x01\x02\xa6\x01\x02\x12\
    \x04\xf9\x02\x15\x19\n\r\n\x05\x05\x01\x02\xa7\x01\x12\x04\xfa\x02\x08\
    \x16\n\x0e\n\x06\x05\x01\x02\xa7\x01\x01\x12\x04\xfa\x02\x08\x0e\n\x0e\n\
    \x06\x05\x01\x02\xa7\x01\x02\x12\x04\xfa\x02\x11\x15\n\r\n\x05\x05\x01\
    \x02\xa8\x01\x12\x04\xfc\x02\x08\x1b\n\x0e\n\x06\x05\x01\x02\xa8\x01\x01\
    \x12\x04\xfc\x02\x08\x13\n\x0e\n\x06\x05\x01\x02\xa8\x01\x02\x12\x04\xfc\
    \x02\x16\x1a\n\r\n\x05\x05\x01\x02\xa9\x01\x12\x04\xfd\x02\x08\x1c\n\x0e\
    \n\x06\x05\x01\x02\xa9\x01\x01\x12\x04\xfd\x02\x08\x14\n\x0e\n\x06\x05\
    \x01\x02\xa9\x01\x02\x12\x04\xfd\x02\x17\x1b\n\r\n\x05\x05\x01\x02\xaa\
    \x01\x12\x04\xfe\x02\x08\x1f\n\x0e\n\x06\x05\x01\x02\xaa\x01\x01\x12\x04\
    \xfe\x02\x08\x17\n\x0e\n\x06\x05\x01\x02\xaa\x01\x02\x12\x04\xfe\x02\x1a\
    \x1e\n\r\n\x05\x05\x01\x02\xab\x01\x12\x04\xff\x02\x08\x1e\n\x0e\n\x06\
    \x05\x01\x02\xab\x01\x01\x12\x04\xff\x02\x08\x16\n\x0e\n\x06\x05\x01\x02\
    \xab\x01\x02\x12\x04\xff\x02\x19\x1d\n\r\n\x05\x05\x01\x02\xac\x01\x12\
    \x04\x80\x03\x08\x1c\n\x0e\n\x06\x05\x01\x02\xac\x01\x01\x12\x04\x80\x03\
    \x08\x14\n\x0e\n\x06\x05\x01\x02\xac\x01\x02\x12\x04\x80\x03\x17\x1b\n\r\
    \n\x05\x05\x01\x02\xad\x01\x12\x04\x81\x03\x08\x20\n\x0e\n\x06\x05\x01\
    \x02\xad\x01\x01\x12\x04\x81\x03\x08\x18\n\x0e\n\x06\x05\x01\x02\xad\x01\
    \x02\x12\x04\x81\x03\x1b\x1f\n\r\n\x05\x05\x01\x02\xae\x01\x12\x04\x82\
    \x03\x08\x1c\n\x0e\n\x06\x05\x01\x02\xae\x01\x01\x12\x04\x82\x03\x08\x14\
    \n\x0e\n\x06\x05\x01\x02\xae\x01\x02\x12\x04\x82\x03\x17\x1b\n\r\n\x05\
    \x05\x01\x02\xaf\x01\x12\x04\x84\x03\x08\x1b\n\x0e\n\x06\x05\x01\x02\xaf\
    \x01\x01\x12\x04\x84\x03\x08\x13\n\x0e\n\x06\x05\x01\x02\xaf\x01\x02\x12\
    \x04\x84\x03\x16\x1a\n\r\n\x05\x05\x01\x02\xb0\x01\x12\x04\x85\x03\x08\
    \x1c\n\x0e\n\x06\x05\x01\x02\xb0\x01\x01\x12\x04\x85\x03\x08\x14\n\x0e\n\
    \x06\x05\x01\x02\xb0\x01\x02\x12\x04\x85\x03\x17\x1b\n\r\n\x05\x05\x01\
    \x02\xb1\x01\x12\x04\x86\x03\x08\x1f\n\x0e\n\x06\x05\x01\x02\xb1\x01\x01\
    \x12\x04\x86\x03\x08\x17\n\x0e\n\x06\x05\x01\x02\xb1\x01\x02\x12\x04\x86\
    \x03\x1a\x1e\n\r\n\x05\x05\x01\x02\xb2\x01\x12\x04\x87\x03\x08\x1e\n\x0e\
    \n\x06\x05\x01\x02\xb2\x01\x01\x12\x04\x87\x03\x08\x16\n\x0e\n\x06\x05\
    \x01\x02\xb2\x01\x02\x12\x04\x87\x03\x19\x1d\n\r\n\x05\x05\x01\x02\xb3\
    \x01\x12\x04\x88\x03\x08\x1c\n\x0e\n\x06\x05\x01\x02\xb3\x01\x01\x12\x04\
    \x88\x03\x08\x14\n\x0e\n\x06\x05\x01\x02\xb3\x01\x02\x12\x04\x88\x03\x17\
    \x1b\n\r\n\x05\x05\x01\x02\xb4\x01\x12\x04\x89\x03\x08\x20\n\x0e\n\x06\
    \x05\x01\x02\xb4\x01\x01\x12\x04\x89\x03\x08\x18\n\x0e\n\x06\x05\x01\x02\
    \xb4\x01\x02\x12\x04\x89\x03\x1b\x1f\n\r\n\x05\x05\x01\x02\xb5\x01\x12\
    \x04\x8a\x03\x08\x1c\n\x0e\n\x06\x05\x01\x02\xb5\x01\x01\x12\x04\x8a\x03\
    \x08\x14\n\x0e\n\x06\x05\x01\x02\xb5\x01\x02\x12\x04\x8a\x03\x17\x1b\n\
    \x8a\x01\n\x05\x05\x01\x02\xb6\x01\x12\x04\x90\x03\x08\x17\x1a{\n\x20Her\
    e\x20we\x20use\x20suffix\x20*Sig*\x20to\x20avoid\x20name\x20conflict.\
    \x20After\x20we\x20removes\n\x20all\x20same\x20things\x20in\x20ExprType,\
    \x20we\x20can\x20rename\x20them\x20back.\n\n\x0e\n\x06\x05\x01\x02\xb6\
    \x01\x01\x12\x04\x90\x03\x08\x0f\n\x0e\n\x06\x05\x01\x02\xb6\x01\x02\x12\
    \x04\x90\x03\x12\x16\n\r\n\x05\x05\x01\x02\xb7\x01\x12\x04\x92\x03\x08\
    \x1e\n\x0e\n\x06\x05\x01\x02\xb7\x01\x01\x12\x04\x92\x03\x08\x16\n\x0e\n\
    \x06\x05\x01\x02\xb7\x01\x02\x12\x04\x92\x03\x19\x1d\n\r\n\x05\x05\x01\
    \x02\xb8\x01\x12\x04\x93\x03\x08\x1e\n\x0e\n\x06\x05\x01\x02\xb8\x01\x01\
    \x12\x04\x93\x03\x08\x16\n\x0e\n\x06\x05\x01\x02\xb8\x01\x02\x12\x04\x93\
    \x03\x19\x1d\n\r\n\x05\x05\x01\x02\xb9\x01\x12\x04\x94\x03\x08\x1b\n\x0e\
    \n\x06\x05\x01\x02\xb9\x01\x01\x12\x04\x94\x03\x08\x13\n\x0e\n\x06\x05\
    \x01\x02\xb9\x01\x02\x12\x04\x94\x03\x16\x1a\n\r\n\x05\x05\x01\x02\xba\
    \x01\x12\x04\x95\x03\x08\x1a\n\x0e\n\x06\x05\x01\x02\xba\x01\x01\x12\x04\
    \x95\x03\x08\x12\n\x0e\n\x06\x05\x01\x02\xba\x01\x02\x12\x04\x95\x03\x15\
    \x19\n\r\n\x05\x05\x01\x02\xbb\x01\x12\x04\x96\x03\x08\x1d\n\x0e\n\x06\
    \x05\x01\x02\xbb\x01\x01\x12\x04\x96\x03\x08\x15\n\x0e\n\x06\x05\x01\x02\
    \xbb\x01\x02\x12\x04\x96\x03\x18\x1c\n\r\n\x05\x05\x01\x02\xbc\x01\x12\
    \x04\x97\x03\x08\x1e\n\x0e\n\x06\x05\x01\x02\xbc\x01\x01\x12\x04\x97\x03\
    \x08\x16\n\x0e\n\x06\x05\x01\x02\xbc\x01\x02\x12\x04\x97\x03\x19\x1d\n\r\
    \n\x05\x05\x01\x02\xbd\x01\x12\x04\x98\x03\x08\x1d\n\x0e\n\x06\x05\x01\
    \x02\xbd\x01\x01\x12\x04\x98\x03\x08\x15\n\x0e\n\x06\x05\x01\x02\xbd\x01\
    \x02\x12\x04\x98\x03\x18\x1c\n\r\n\x05\x05\x01\x02\xbe\x01\x12\x04\x99\
    \x03\x08\x1c\n\x0e\n\x06\x05\x01\x02\xbe\x01\x01\x12\x04\x99\x03\x08\x14\
    \n\x0e\n\x06\x05\x01\x02\xbe\x01\x02\x12\x04\x99\x03\x17\x1b\n\r\n\x05\
    \x05\x01\x02\xbf\x01\x12\x04\x9a\x03\x08\x1d\n\x0e\n\x06\x05\x01\x02\xbf\
    \x01\x01\x12\x04\x9a\x03\x08\x15\n\x0e\n\x06\x05\x01\x02\xbf\x01\x02\x12\
    \x04\x9a\x03\x18\x1c\n\r\n\x05\x05\x01\x02\xc0\x01\x12\x04\x9b\x03\x08\
    \x1c\n\x0e\n\x06\x05\x01\x02\xc0\x01\x01\x12\x04\x9b\x03\x08\x14\n\x0e\n\
    \x06\x05\x01\x02\xc0\x01\x02\x12\x04\x9b\x03\x17\x1b\n[\n\x02\x04\x01\
    \x12\x06\x9f\x03\0\xa5\x03\x01\x1aM\x20Evaluators\x20should\x20implement\
    \x20evaluation\x20functions\x20for\x20every\x20expression\x20type.\n\n\
    \x0b\n\x03\x04\x01\x01\x12\x04\x9f\x03\x08\x0c\n\x0c\n\x04\x04\x01\x02\0\
    \x12\x04\xa0\x03\x08@\n\r\n\x05\x04\x01\x02\0\x04\x12\x04\xa0\x03\x08\
    \x10\n\r\n\x05\x04\x01\x02\0\x06\x12\x04\xa0\x03\x11\x19\n\r\n\x05\x04\
    \x01\x02\0\x01\x12\x04\xa0\x03\x1a\x1c\n\r\n\x05\x04\x01\x02\0\x03\x12\
    \x04\xa0\x03\x1f\x20\n\r\n\x05\x04\x01\x02\0\x08\x12\x04\xa0\x03!?\n\x10\
    \n\x08\x04\x01\x02\0\x08\xe7\x07\0\x12\x04\xa0\x03\">\n\x11\n\t\x04\x01\
    \x02\0\x08\xe7\x07\0\x02\x12\x04\xa0\x03\"6\n\x12\n\n\x04\x01\x02\0\x08\
    \xe7\x07\0\x02\0\x12\x04\xa0\x03\"6\n\x13\n\x0b\x04\x01\x02\0\x08\xe7\
    \x07\0\x02\0\x01\x12\x04\xa0\x03#5\n\x11\n\t\x04\x01\x02\0\x08\xe7\x07\0\
    \x03\x12\x04\xa0\x039>\n\x0c\n\x04\x04\x01\x02\x01\x12\x04\xa1\x03\x08\
    \x1f\n\r\n\x05\x04\x01\x02\x01\x04\x12\x04\xa1\x03\x08\x10\n\r\n\x05\x04\
    \x01\x02\x01\x05\x12\x04\xa1\x03\x11\x16\n\r\n\x05\x04\x01\x02\x01\x01\
    \x12\x04\xa1\x03\x17\x1a\n\r\n\x05\x04\x01\x02\x01\x03\x12\x04\xa1\x03\
    \x1d\x1e\n\x0c\n\x04\x04\x01\x02\x02\x12\x04\xa2\x03\x08#\n\r\n\x05\x04\
    \x01\x02\x02\x04\x12\x04\xa2\x03\x08\x10\n\r\n\x05\x04\x01\x02\x02\x06\
    \x12\x04\xa2\x03\x11\x15\n\r\n\x05\x04\x01\x02\x02\x01\x12\x04\xa2\x03\
    \x16\x1e\n\r\n\x05\x04\x01\x02\x02\x03\x12\x04\xa2\x03!\"\n\x0c\n\x04\
    \x04\x01\x02\x03\x12\x04\xa3\x03\x08F\n\r\n\x05\x04\x01\x02\x03\x04\x12\
    \x04\xa3\x03\x08\x10\n\r\n\x05\x04\x01\x02\x03\x06\x12\x04\xa3\x03\x11\
    \x1e\n\r\n\x05\x04\x01\x02\x03\x01\x12\x04\xa3\x03\x1f\"\n\r\n\x05\x04\
    \x01\x02\x03\x03\x12\x04\xa3\x03%&\n\r\n\x05\x04\x01\x02\x03\x08\x12\x04\
    \xa3\x03'E\n\x10\n\x08\x04\x01\x02\x03\x08\xe7\x07\0\x12\x04\xa3\x03(D\n\
    \x11\n\t\x04\x01\x02\x03\x08\xe7\x07\0\x02\x12\x04\xa3\x03(<\n\x12\n\n\
    \x04\x01\x02\x03\x08\xe7\x07\0\x02\0\x12\x04\xa3\x03(<\n\x13\n\x0b\x04\
    \x01\x02\x03\x08\xe7\x07\0\x02\0\x01\x12\x04\xa3\x03);\n\x11\n\t\x04\x01\
    \x02\x03\x08\xe7\x07\0\x03\x12\x04\xa3\x03?D\n\x0c\n\x04\x04\x01\x02\x04\
    \x12\x04\xa4\x03\x08*\n\r\n\x05\x04\x01\x02\x04\x04\x12\x04\xa4\x03\x08\
    \x10\n\r\n\x05\x04\x01\x02\x04\x06\x12\x04\xa4\x03\x11\x1a\n\r\n\x05\x04\
    \x01\x02\x04\x01\x12\x04\xa4\x03\x1b%\n\r\n\x05\x04\x01\x02\x04\x03\x12\
    \x04\xa4\x03()\n6\n\x02\x04\x02\x12\x06\xa8\x03\0\xab\x03\x01\x1a(\x20By\
    Item\x20type\x20for\x20group\x20by\x20and\x20order\x20by.\n\n\x0b\n\x03\
    \x04\x02\x01\x12\x04\xa8\x03\x08\x0e\n\x0c\n\x04\x04\x02\x02\0\x12\x04\
    \xa9\x03\x08\x1f\n\r\n\x05\x04\x02\x02\0\x04\x12\x04\xa9\x03\x08\x10\n\r\
    \n\x05\x04\x02\x02\0\x06\x12\x04\xa9\x03\x11\x15\n\r\n\x05\x04\x02\x02\0\
    \x01\x12\x04\xa9\x03\x16\x1a\n\r\n\x05\x04\x02\x02\0\x03\x12\x04\xa9\x03\
    \x1d\x1e\n\x0c\n\x04\x04\x02\x02\x01\x12\x04\xaa\x03\x08>\n\r\n\x05\x04\
    \x02\x02\x01\x04\x12\x04\xaa\x03\x08\x10\n\r\n\x05\x04\x02\x02\x01\x05\
    \x12\x04\xaa\x03\x11\x15\n\r\n\x05\x04\x02\x02\x01\x01\x12\x04\xaa\x03\
    \x16\x1a\n\r\n\x05\x04\x02\x02\x01\x03\x12\x04\xaa\x03\x1d\x1e\n\r\n\x05\
    \x04\x02\x02\x01\x08\x12\x04\xaa\x03\x1f=\n\x10\n\x08\x04\x02\x02\x01\
    \x08\xe7\x07\0\x12\x04\xaa\x03\x20<\n\x11\n\t\x04\x02\x02\x01\x08\xe7\
    \x07\0\x02\x12\x04\xaa\x03\x204\n\x12\n\n\x04\x02\x02\x01\x08\xe7\x07\0\
    \x02\0\x12\x04\xaa\x03\x204\n\x13\n\x0b\x04\x02\x02\x01\x08\xe7\x07\0\
    \x02\0\x01\x12\x04\xaa\x03!3\n\x11\n\t\x04\x02\x02\x01\x08\xe7\x07\0\x03\
    \x12\x04\xaa\x037<\
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
