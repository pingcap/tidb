// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Source-shaped DDL job arguments from `pkg/meta/model/job_args.go`.
//!
//! Go version 1 stores an untyped argument array and caches pointers to the
//! decoded destinations. Version 2 stores one typed `JobArgs` pointer. The
//! shared cells and explicit dynamic pointer values below preserve both rules
//! without Rust `Any`, unsafe downcasts, or JSON-only DTO approximations.

use std::fmt;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use tidb_ast::CiString;
use tidb_datatype::GoString;

use crate::table::ConstraintInfo;
use crate::{
    ActionType, ColumnDefaultValue, ColumnInfo, DBInfo, GoAny, GoAnyBytes, GoAnyJsonError,
    GoAnyValue, GoEqualityProjection, GoJsonProjection, GoJsonReference, GoJsonReferenceIdentity,
    GoJsonValue, GoShared, GoSharedPointerSlice, GoSharedSlice, GoTypeIdentity, GoTypeKind, Job,
    JobState, JobVersion, PartitionInfo, PolicyRefInfo, ResourceGroupInfo, TableInfo, TableMode,
};

const MODEL_PACKAGE_PATH: &str = "github.com/pingcap/tidb/pkg/meta/model";
const AST_PACKAGE_PATH: &str = "github.com/pingcap/tidb/pkg/parser/ast";
const PDHTTP_PACKAGE_PATH: &str = "github.com/tikv/pd/client/http";

fn model_type(name: &str, kind: GoTypeKind) -> GoTypeIdentity {
    GoTypeIdentity::defined(MODEL_PACKAGE_PATH, name, format!("model.{name}"), kind)
}

fn builtin_type(name: &str, kind: GoTypeKind) -> GoTypeIdentity {
    GoTypeIdentity::unnamed(name, kind)
}

fn ast_type(name: &str, kind: GoTypeKind) -> GoTypeIdentity {
    GoTypeIdentity::defined(AST_PACKAGE_PATH, name, format!("ast.{name}"), kind)
}

fn pdhttp_type(name: &str, kind: GoTypeKind) -> GoTypeIdentity {
    GoTypeIdentity::defined(PDHTTP_PACKAGE_PATH, name, format!("http.{name}"), kind)
}

/// One addressable embedded Go struct field.
///
/// Cloning the containing Rust struct allocates a new field address and copies
/// the field value, matching a Go struct value copy. Explicit field pointers
/// clone the inner [`GoShared`] handle instead.
pub struct GoField<T>(GoShared<T>);

impl<T> GoField<T> {
    /// Allocates one field cell.
    #[must_use]
    pub fn new(value: T) -> Self {
        Self(GoShared::new(value))
    }

    /// Reads the field.
    pub fn read(&self) -> std::sync::RwLockReadGuard<'_, T> {
        self.0.read()
    }

    /// Mutates the field in place.
    pub fn write(&self) -> std::sync::RwLockWriteGuard<'_, T> {
        self.0.write()
    }

    /// Replaces the field value.
    pub fn set(&self, value: T) {
        *self.0.write() = value;
    }

    fn pointer_handle(&self) -> GoShared<T> {
        self.0.clone()
    }
}

impl<T: Clone> GoField<T> {
    /// Copies the field value.
    #[must_use]
    pub fn get(&self) -> T {
        self.0.read().clone()
    }
}

impl<T: Default> Default for GoField<T> {
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<T: Clone> Clone for GoField<T> {
    fn clone(&self) -> Self {
        Self::new(self.get())
    }
}

impl<T: Clone + fmt::Debug> fmt::Debug for GoField<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_tuple("GoField").field(&self.get()).finish()
    }
}

impl<T: Clone + Serialize> Serialize for GoField<T> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.get().serialize(serializer)
    }
}

impl<'de, T> Deserialize<'de> for GoField<T>
where
    T: Default + Deserialize<'de>,
{
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Option::<T>::deserialize(deserializer).map(|value| Self::new(value.unwrap_or_default()))
    }
}

/// Go `[]byte` with a copied slice header, shared backing, and base64 JSON.
#[derive(Clone, Debug, Default)]
pub struct GoByteSlice(pub GoSharedSlice<u8>);

impl Serialize for GoByteSlice {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        crate::serde_helpers::go_shared_bytes::serialize(&self.0, serializer)
    }
}

impl<'de> Deserialize<'de> for GoByteSlice {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        crate::serde_helpers::go_shared_bytes::deserialize(deserializer).map(Self)
    }
}

fn field_is_default<T>(field: &GoField<T>) -> bool
where
    T: Clone + Default + PartialEq,
{
    field.get() == T::default()
}

fn field_shared_slice_is_empty<T: Clone>(field: &GoField<GoSharedSlice<T>>) -> bool {
    field.read().is_empty()
}

fn field_shared_pointer_is_none<T>(field: &GoField<Option<GoShared<T>>>) -> bool {
    field.read().is_none()
}

fn field_shared_pointer_slice_is_empty<T>(field: &GoField<GoSharedPointerSlice<T>>) -> bool {
    field.read().is_empty()
}

#[derive(Clone, Debug)]
struct GoRawDynamicValue {
    go_type: GoTypeIdentity,
    raw: String,
}

impl GoAnyValue for GoRawDynamicValue {
    fn go_type(&self) -> GoTypeIdentity {
        self.go_type.clone()
    }

    fn copy_for_interface(&self) -> Box<dyn GoAnyValue> {
        Box::new(self.clone())
    }

    fn go_json_projection(&self) -> Result<GoJsonProjection, GoAnyJsonError> {
        Ok(GoJsonProjection::Value(GoJsonValue::Raw(self.raw.clone())))
    }

    fn append_go_format(&self, output: &mut Vec<u8>) {
        output.extend_from_slice(self.raw.as_bytes());
    }

    fn equality_projection(&self) -> Option<GoEqualityProjection<'_>> {
        None
    }
}

#[derive(Clone, Debug)]
struct GoTypedPointer<T: Clone + fmt::Debug> {
    pointee_type: GoTypeIdentity,
    value: Option<GoShared<T>>,
}

impl<T: Clone + fmt::Debug> GoTypedPointer<T> {
    fn new(pointee_type: GoTypeIdentity, value: Option<GoShared<T>>) -> Self {
        Self {
            pointee_type,
            value,
        }
    }
}

impl<T> GoAnyValue for GoTypedPointer<T>
where
    T: Clone + fmt::Debug + Serialize + Send + Sync + 'static,
{
    fn go_type(&self) -> GoTypeIdentity {
        self.pointee_type.pointer_to()
    }

    fn copy_for_interface(&self) -> Box<dyn GoAnyValue> {
        Box::new(self.clone())
    }

    fn go_json_projection(&self) -> Result<GoJsonProjection, GoAnyJsonError> {
        let Some(value) = &self.value else {
            return Ok(GoJsonProjection::Value(GoJsonValue::Null));
        };
        let raw = crate::serde_helpers::to_go_json(&*value.read())
            .map_err(|error| GoAnyJsonError::new(error.to_string()))?;
        let raw = String::from_utf8(raw).expect("JSON output is valid UTF-8");
        let child = GoAny::new(GoRawDynamicValue {
            go_type: self.pointee_type.clone(),
            raw,
        });
        Ok(GoJsonProjection::ReferencedPointer(
            GoJsonReference::new(
                GoJsonReferenceIdentity::Pointer(value.identity_address()),
                self.go_type(),
            ),
            child,
        ))
    }

    fn append_go_format(&self, output: &mut Vec<u8>) {
        let Some(value) = &self.value else {
            output.extend_from_slice(b"<nil>");
            return;
        };
        output.push(b'&');
        if let Ok(raw) = crate::serde_helpers::to_go_json(&*value.read()) {
            output.extend_from_slice(&raw);
        }
    }

    fn equality_projection(&self) -> Option<GoEqualityProjection<'_>> {
        Some(GoEqualityProjection::PointerAddress(
            self.value.as_ref().map(GoShared::identity_address),
        ))
    }
}

#[derive(Clone, Debug)]
struct GoTypedValue<T> {
    go_type: GoTypeIdentity,
    value: T,
}

impl<T> GoAnyValue for GoTypedValue<T>
where
    T: Clone + fmt::Debug + Serialize + Send + Sync + 'static,
{
    fn go_type(&self) -> GoTypeIdentity {
        self.go_type.clone()
    }

    fn copy_for_interface(&self) -> Box<dyn GoAnyValue> {
        Box::new(self.clone())
    }

    fn go_json_projection(&self) -> Result<GoJsonProjection, GoAnyJsonError> {
        let raw = crate::serde_helpers::to_go_json(&self.value)
            .map_err(|error| GoAnyJsonError::new(error.to_string()))?;
        Ok(GoJsonProjection::Value(GoJsonValue::Raw(
            String::from_utf8(raw).expect("JSON output is valid UTF-8"),
        )))
    }

    fn append_go_format(&self, output: &mut Vec<u8>) {
        if let Ok(raw) = crate::serde_helpers::to_go_json(&self.value) {
            output.extend_from_slice(&raw);
        }
    }

    fn equality_projection(&self) -> Option<GoEqualityProjection<'_>> {
        None
    }
}

fn typed_pointer_any<T>(pointee_type: GoTypeIdentity, value: Option<GoShared<T>>) -> GoAny
where
    T: Clone + fmt::Debug + Serialize + Send + Sync + 'static,
{
    GoAny::new(GoTypedPointer::new(pointee_type, value))
}

fn typed_value_any<T>(go_type: GoTypeIdentity, value: T) -> GoAny
where
    T: Clone + fmt::Debug + Serialize + Send + Sync + 'static,
{
    GoAny::new(GoTypedValue { go_type, value })
}

struct V1Decoder {
    raw: Vec<Box<RawValue>>,
    next: usize,
    decoded: Vec<GoAny>,
}

impl V1Decoder {
    fn new(job: &Job) -> Result<Self, serde_json::Error> {
        assert_eq!(
            job.version,
            JobVersion::V1,
            "Job.decodeArgs is only used for JobVersion1"
        );
        let bytes = job
            .raw_args
            .as_ref()
            .map_or_else(Vec::new, crate::PersistedRawJson::bytes);
        Ok(Self {
            raw: serde_json::from_slice::<Option<Vec<Box<RawValue>>>>(&bytes)?.unwrap_or_default(),
            next: 0,
            decoded: Vec::new(),
        })
    }

    fn decode<T>(
        &mut self,
        destination: &GoField<T>,
        field_type: GoTypeIdentity,
    ) -> Result<(), serde_json::Error>
    where
        T: Clone + Default + fmt::Debug + Serialize + DeserializeOwned + Send + Sync + 'static,
    {
        let index = self.next;
        self.next += 1;
        let Some(raw) = self.raw.get(index) else {
            return Ok(());
        };
        let decoded = serde_json::from_str::<Option<T>>(raw.get())?.unwrap_or_default();
        destination.set(decoded);
        self.decoded.push(typed_pointer_any(
            field_type,
            Some(destination.pointer_handle()),
        ));
        Ok(())
    }

    fn decode_pointee<T>(
        &mut self,
        destination: &GoShared<T>,
        pointee_type: GoTypeIdentity,
    ) -> Result<(), serde_json::Error>
    where
        T: Clone + fmt::Debug + Serialize + DeserializeOwned + Send + Sync + 'static,
    {
        let index = self.next;
        self.next += 1;
        let Some(raw) = self.raw.get(index) else {
            return Ok(());
        };
        if raw.get() != "null" {
            *destination.write() = serde_json::from_str(raw.get())?;
        }
        self.decoded
            .push(typed_pointer_any(pointee_type, Some(destination.clone())));
        Ok(())
    }

    fn finish(self, job: &mut Job) {
        job.set_v1_decoded_args(if self.decoded.is_empty() {
            GoSharedSlice::default()
        } else {
            GoSharedSlice::from_vec(self.decoded)
        });
    }
}

/// Source-typed values admitted by Go's private `Job.args []any` V2 slot.
#[derive(Clone, Debug)]
pub enum JobArgsValue {
    /// `*model.EmptyArgs`, including a typed nil pointer.
    Empty(Option<GoShared<EmptyArgs>>),
    /// `*model.CreateSchemaArgs`, including a typed nil pointer.
    CreateSchema(Option<GoShared<CreateSchemaArgs>>),
    /// `*model.RenameTableArgs`, including a typed nil pointer.
    RenameTable(Option<GoShared<RenameTableArgs>>),
    /// `*model.RenameTablesArgs`, including a typed nil pointer.
    RenameTables(Option<GoShared<RenameTablesArgs>>),
    /// `*model.ResourceGroupArgs`, including a typed nil pointer.
    ResourceGroup(Option<GoShared<ResourceGroupArgs>>),
    /// `*model.DropSchemaArgs`, including a typed nil pointer.
    DropSchema(Option<GoShared<DropSchemaArgs>>),
    /// `*model.ModifySchemaArgs`, including a typed nil pointer.
    ModifySchema(Option<GoShared<ModifySchemaArgs>>),
    /// `*model.CreateTableArgs`, including a typed nil pointer.
    CreateTable(Option<GoShared<CreateTableArgs>>),
    /// `*model.CreateMaterializedViewLogArgs`, including a typed nil pointer.
    CreateMaterializedViewLog(Option<GoShared<CreateMaterializedViewLogArgs>>),
    /// `*model.CreateMaterializedViewArgs`, including a typed nil pointer.
    CreateMaterializedView(Option<GoShared<CreateMaterializedViewArgs>>),
    /// `*model.BatchCreateTableArgs`, including a typed nil pointer.
    BatchCreateTable(Option<GoShared<BatchCreateTableArgs>>),
    /// `*model.TruncateTableArgs`, including a typed nil pointer.
    TruncateTable(Option<GoShared<TruncateTableArgs>>),
    /// `*model.TablePartitionArgs`, including a typed nil pointer.
    TablePartition(Option<GoShared<TablePartitionArgs>>),
    /// `*model.ExchangeTablePartitionArgs`, including a typed nil pointer.
    ExchangeTablePartition(Option<GoShared<ExchangeTablePartitionArgs>>),
    /// `*model.AlterTablePartitionArgs`, including a typed nil pointer.
    AlterTablePartition(Option<GoShared<AlterTablePartitionArgs>>),
    /// `*model.RebaseAutoIDArgs`, including a typed nil pointer.
    RebaseAutoId(Option<GoShared<RebaseAutoIDArgs>>),
    /// `*model.ModifyTableCommentArgs`, including a typed nil pointer.
    ModifyTableComment(Option<GoShared<ModifyTableCommentArgs>>),
    /// `*model.ModifyTableCharsetAndCollateArgs`, including a typed nil pointer.
    ModifyTableCharsetAndCollate(Option<GoShared<ModifyTableCharsetAndCollateArgs>>),
    /// `*model.ModifyIndexArgs`, including a typed nil pointer.
    ModifyIndex(Option<GoShared<ModifyIndexArgs>>),
    /// `*model.AlterIndexVisibilityArgs`, including a typed nil pointer.
    AlterIndexVisibility(Option<GoShared<AlterIndexVisibilityArgs>>),
    /// `*model.DropForeignKeyArgs`, including a typed nil pointer.
    DropForeignKey(Option<GoShared<DropForeignKeyArgs>>),
    /// `*model.ModifyTableAutoIDCacheArgs`, including a typed nil pointer.
    ModifyTableAutoIdCache(Option<GoShared<ModifyTableAutoIDCacheArgs>>),
    /// `*model.ShardRowIDArgs`, including a typed nil pointer.
    ShardRowId(Option<GoShared<ShardRowIDArgs>>),
    /// `*model.SetDefaultValueArgs`, including a typed nil pointer.
    SetDefaultValue(Option<GoShared<SetDefaultValueArgs>>),
    /// `*model.RefreshMetaArgs`, including a typed nil pointer.
    RefreshMeta(Option<GoShared<RefreshMetaArgs>>),
    /// `*model.ModifyTableEngineAttributeArgs`, including a typed nil pointer.
    ModifyTableEngineAttribute(Option<GoShared<ModifyTableEngineAttributeArgs>>),
    /// `*model.AlterTableModeArgs`, including a typed nil pointer.
    AlterTableMode(Option<GoShared<AlterTableModeArgs>>),
    /// `*model.CheckConstraintArgs`, including a typed nil pointer.
    CheckConstraint(Option<GoShared<CheckConstraintArgs>>),
    /// `*model.AddCheckConstraintArgs`, including a typed nil pointer.
    AddCheckConstraint(Option<GoShared<AddCheckConstraintArgs>>),
}

impl JobArgsValue {
    /// Applies this source-typed private argument value through Go `Job.FillArgs`.
    pub fn fill_job(&self, job: &mut Job) {
        match self {
            Self::Empty(value) => job.fill_args(value.clone()),
            Self::CreateSchema(value) => job.fill_args(value.clone()),
            Self::RenameTable(value) => job.fill_args(value.clone()),
            Self::RenameTables(value) => job.fill_args(value.clone()),
            Self::ResourceGroup(value) => job.fill_args(value.clone()),
            Self::DropSchema(value) => job.fill_args(value.clone()),
            Self::ModifySchema(value) => job.fill_args(value.clone()),
            Self::CreateTable(value) => job.fill_args(value.clone()),
            Self::CreateMaterializedViewLog(value) => job.fill_args(value.clone()),
            Self::CreateMaterializedView(value) => job.fill_args(value.clone()),
            Self::BatchCreateTable(value) => job.fill_args(value.clone()),
            Self::TruncateTable(value) => job.fill_args(value.clone()),
            Self::TablePartition(value) => job.fill_args(value.clone()),
            Self::ExchangeTablePartition(value) => job.fill_args(value.clone()),
            Self::AlterTablePartition(value) => job.fill_args(value.clone()),
            Self::RebaseAutoId(value) => job.fill_args(value.clone()),
            Self::ModifyTableComment(value) => job.fill_args(value.clone()),
            Self::ModifyTableCharsetAndCollate(value) => job.fill_args(value.clone()),
            Self::ModifyIndex(value) => job.fill_args(value.clone()),
            Self::AlterIndexVisibility(value) => job.fill_args(value.clone()),
            Self::DropForeignKey(value) => job.fill_args(value.clone()),
            Self::ModifyTableAutoIdCache(value) => job.fill_args(value.clone()),
            Self::ShardRowId(value) => job.fill_args(value.clone()),
            Self::SetDefaultValue(value) => job.fill_args(value.clone()),
            Self::RefreshMeta(value) => job.fill_args(value.clone()),
            Self::ModifyTableEngineAttribute(value) => job.fill_args(value.clone()),
            Self::AlterTableMode(value) => job.fill_args(value.clone()),
            Self::CheckConstraint(value) => job.fill_args(value.clone()),
            Self::AddCheckConstraint(value) => job.fill_args(value.clone()),
        }
    }

    fn go_type_identity(&self) -> GoTypeIdentity {
        let name = match self {
            Self::Empty(_) => "EmptyArgs",
            Self::CreateSchema(_) => "CreateSchemaArgs",
            Self::RenameTable(_) => "RenameTableArgs",
            Self::RenameTables(_) => "RenameTablesArgs",
            Self::ResourceGroup(_) => "ResourceGroupArgs",
            Self::DropSchema(_) => "DropSchemaArgs",
            Self::ModifySchema(_) => "ModifySchemaArgs",
            Self::CreateTable(_) => "CreateTableArgs",
            Self::CreateMaterializedViewLog(_) => "CreateMaterializedViewLogArgs",
            Self::CreateMaterializedView(_) => "CreateMaterializedViewArgs",
            Self::BatchCreateTable(_) => "BatchCreateTableArgs",
            Self::TruncateTable(_) => "TruncateTableArgs",
            Self::TablePartition(_) => "TablePartitionArgs",
            Self::ExchangeTablePartition(_) => "ExchangeTablePartitionArgs",
            Self::AlterTablePartition(_) => "AlterTablePartitionArgs",
            Self::RebaseAutoId(_) => "RebaseAutoIDArgs",
            Self::ModifyTableComment(_) => "ModifyTableCommentArgs",
            Self::ModifyTableCharsetAndCollate(_) => "ModifyTableCharsetAndCollateArgs",
            Self::ModifyIndex(_) => "ModifyIndexArgs",
            Self::AlterIndexVisibility(_) => "AlterIndexVisibilityArgs",
            Self::DropForeignKey(_) => "DropForeignKeyArgs",
            Self::ModifyTableAutoIdCache(_) => "ModifyTableAutoIDCacheArgs",
            Self::ShardRowId(_) => "ShardRowIDArgs",
            Self::SetDefaultValue(_) => "SetDefaultValueArgs",
            Self::RefreshMeta(_) => "RefreshMetaArgs",
            Self::ModifyTableEngineAttribute(_) => "ModifyTableEngineAttributeArgs",
            Self::AlterTableMode(_) => "AlterTableModeArgs",
            Self::CheckConstraint(_) => "CheckConstraintArgs",
            Self::AddCheckConstraint(_) => "AddCheckConstraintArgs",
        };
        model_type(name, GoTypeKind::Struct).pointer_to()
    }

    fn pointer_address(&self) -> Option<usize> {
        match self {
            Self::Empty(value) => value.as_ref().map(GoShared::identity_address),
            Self::CreateSchema(value) => value.as_ref().map(GoShared::identity_address),
            Self::RenameTable(value) => value.as_ref().map(GoShared::identity_address),
            Self::RenameTables(value) => value.as_ref().map(GoShared::identity_address),
            Self::ResourceGroup(value) => value.as_ref().map(GoShared::identity_address),
            Self::DropSchema(value) => value.as_ref().map(GoShared::identity_address),
            Self::ModifySchema(value) => value.as_ref().map(GoShared::identity_address),
            Self::CreateTable(value) => value.as_ref().map(GoShared::identity_address),
            Self::CreateMaterializedViewLog(value) => {
                value.as_ref().map(GoShared::identity_address)
            }
            Self::CreateMaterializedView(value) => value.as_ref().map(GoShared::identity_address),
            Self::BatchCreateTable(value) => value.as_ref().map(GoShared::identity_address),
            Self::TruncateTable(value) => value.as_ref().map(GoShared::identity_address),
            Self::TablePartition(value) => value.as_ref().map(GoShared::identity_address),
            Self::ExchangeTablePartition(value) => value.as_ref().map(GoShared::identity_address),
            Self::AlterTablePartition(value) => value.as_ref().map(GoShared::identity_address),
            Self::RebaseAutoId(value) => value.as_ref().map(GoShared::identity_address),
            Self::ModifyTableComment(value) => value.as_ref().map(GoShared::identity_address),
            Self::ModifyTableCharsetAndCollate(value) => {
                value.as_ref().map(GoShared::identity_address)
            }
            Self::ModifyIndex(value) => value.as_ref().map(GoShared::identity_address),
            Self::AlterIndexVisibility(value) => value.as_ref().map(GoShared::identity_address),
            Self::DropForeignKey(value) => value.as_ref().map(GoShared::identity_address),
            Self::ModifyTableAutoIdCache(value) => value.as_ref().map(GoShared::identity_address),
            Self::ShardRowId(value) => value.as_ref().map(GoShared::identity_address),
            Self::SetDefaultValue(value) => value.as_ref().map(GoShared::identity_address),
            Self::RefreshMeta(value) => value.as_ref().map(GoShared::identity_address),
            Self::ModifyTableEngineAttribute(value) => {
                value.as_ref().map(GoShared::identity_address)
            }
            Self::AlterTableMode(value) => value.as_ref().map(GoShared::identity_address),
            Self::CheckConstraint(value) => value.as_ref().map(GoShared::identity_address),
            Self::AddCheckConstraint(value) => value.as_ref().map(GoShared::identity_address),
        }
    }

    fn projection(&self) -> Result<GoJsonProjection, GoAnyJsonError> {
        match self {
            Self::Empty(value) => {
                GoTypedPointer::new(model_type("EmptyArgs", GoTypeKind::Struct), value.clone())
                    .go_json_projection()
            }
            Self::CreateSchema(value) => GoTypedPointer::new(
                model_type("CreateSchemaArgs", GoTypeKind::Struct),
                value.clone(),
            )
            .go_json_projection(),
            Self::RenameTable(value) => GoTypedPointer::new(
                model_type("RenameTableArgs", GoTypeKind::Struct),
                value.clone(),
            )
            .go_json_projection(),
            Self::RenameTables(value) => GoTypedPointer::new(
                model_type("RenameTablesArgs", GoTypeKind::Struct),
                value.clone(),
            )
            .go_json_projection(),
            Self::ResourceGroup(value) => GoTypedPointer::new(
                model_type("ResourceGroupArgs", GoTypeKind::Struct),
                value.clone(),
            )
            .go_json_projection(),
            Self::DropSchema(value) => GoTypedPointer::new(
                model_type("DropSchemaArgs", GoTypeKind::Struct),
                value.clone(),
            )
            .go_json_projection(),
            Self::ModifySchema(value) => GoTypedPointer::new(
                model_type("ModifySchemaArgs", GoTypeKind::Struct),
                value.clone(),
            )
            .go_json_projection(),
            Self::CreateTable(value) => GoTypedPointer::new(
                model_type("CreateTableArgs", GoTypeKind::Struct),
                value.clone(),
            )
            .go_json_projection(),
            Self::CreateMaterializedViewLog(value) => GoTypedPointer::new(
                model_type("CreateMaterializedViewLogArgs", GoTypeKind::Struct),
                value.clone(),
            )
            .go_json_projection(),
            Self::CreateMaterializedView(value) => GoTypedPointer::new(
                model_type("CreateMaterializedViewArgs", GoTypeKind::Struct),
                value.clone(),
            )
            .go_json_projection(),
            Self::BatchCreateTable(value) => GoTypedPointer::new(
                model_type("BatchCreateTableArgs", GoTypeKind::Struct),
                value.clone(),
            )
            .go_json_projection(),
            Self::TruncateTable(value) => GoTypedPointer::new(
                model_type("TruncateTableArgs", GoTypeKind::Struct),
                value.clone(),
            )
            .go_json_projection(),
            Self::TablePartition(value) => GoTypedPointer::new(
                model_type("TablePartitionArgs", GoTypeKind::Struct),
                value.clone(),
            )
            .go_json_projection(),
            Self::ExchangeTablePartition(value) => GoTypedPointer::new(
                model_type("ExchangeTablePartitionArgs", GoTypeKind::Struct),
                value.clone(),
            )
            .go_json_projection(),
            Self::AlterTablePartition(value) => GoTypedPointer::new(
                model_type("AlterTablePartitionArgs", GoTypeKind::Struct),
                value.clone(),
            )
            .go_json_projection(),
            Self::RebaseAutoId(value) => GoTypedPointer::new(
                model_type("RebaseAutoIDArgs", GoTypeKind::Struct),
                value.clone(),
            )
            .go_json_projection(),
            Self::ModifyTableComment(value) => GoTypedPointer::new(
                model_type("ModifyTableCommentArgs", GoTypeKind::Struct),
                value.clone(),
            )
            .go_json_projection(),
            Self::ModifyTableCharsetAndCollate(value) => GoTypedPointer::new(
                model_type("ModifyTableCharsetAndCollateArgs", GoTypeKind::Struct),
                value.clone(),
            )
            .go_json_projection(),
            Self::ModifyIndex(value) => GoTypedPointer::new(
                model_type("ModifyIndexArgs", GoTypeKind::Struct),
                value.clone(),
            )
            .go_json_projection(),
            Self::AlterIndexVisibility(value) => GoTypedPointer::new(
                model_type("AlterIndexVisibilityArgs", GoTypeKind::Struct),
                value.clone(),
            )
            .go_json_projection(),
            Self::DropForeignKey(value) => GoTypedPointer::new(
                model_type("DropForeignKeyArgs", GoTypeKind::Struct),
                value.clone(),
            )
            .go_json_projection(),
            Self::ModifyTableAutoIdCache(value) => GoTypedPointer::new(
                model_type("ModifyTableAutoIDCacheArgs", GoTypeKind::Struct),
                value.clone(),
            )
            .go_json_projection(),
            Self::ShardRowId(value) => GoTypedPointer::new(
                model_type("ShardRowIDArgs", GoTypeKind::Struct),
                value.clone(),
            )
            .go_json_projection(),
            Self::SetDefaultValue(value) => GoTypedPointer::new(
                model_type("SetDefaultValueArgs", GoTypeKind::Struct),
                value.clone(),
            )
            .go_json_projection(),
            Self::RefreshMeta(value) => GoTypedPointer::new(
                model_type("RefreshMetaArgs", GoTypeKind::Struct),
                value.clone(),
            )
            .go_json_projection(),
            Self::ModifyTableEngineAttribute(value) => GoTypedPointer::new(
                model_type("ModifyTableEngineAttributeArgs", GoTypeKind::Struct),
                value.clone(),
            )
            .go_json_projection(),
            Self::AlterTableMode(value) => GoTypedPointer::new(
                model_type("AlterTableModeArgs", GoTypeKind::Struct),
                value.clone(),
            )
            .go_json_projection(),
            Self::CheckConstraint(value) => GoTypedPointer::new(
                model_type("CheckConstraintArgs", GoTypeKind::Struct),
                value.clone(),
            )
            .go_json_projection(),
            Self::AddCheckConstraint(value) => GoTypedPointer::new(
                model_type("AddCheckConstraintArgs", GoTypeKind::Struct),
                value.clone(),
            )
            .go_json_projection(),
        }
    }
}

impl GoAnyValue for JobArgsValue {
    fn go_type(&self) -> GoTypeIdentity {
        self.go_type_identity()
    }

    fn copy_for_interface(&self) -> Box<dyn GoAnyValue> {
        Box::new(self.clone())
    }

    fn go_json_projection(&self) -> Result<GoJsonProjection, GoAnyJsonError> {
        self.projection()
    }

    fn append_go_format(&self, output: &mut Vec<u8>) {
        if self.pointer_address().is_none() {
            output.extend_from_slice(b"<nil>");
        } else if let Ok(value) = self.projection() {
            output.extend_from_slice(format!("{:?}", value).as_bytes());
        }
    }

    fn equality_projection(&self) -> Option<GoEqualityProjection<'_>> {
        Some(GoEqualityProjection::PointerAddress(self.pointer_address()))
    }

    fn job_args_value(&self) -> Option<&JobArgsValue> {
        Some(self)
    }
}

/// Go's private `JobArgs` interface contract.
pub trait JobArgs:
    Clone + Default + fmt::Debug + Serialize + DeserializeOwned + Send + Sync
{
    /// Converts the typed pointer to its exact dynamic interface value.
    fn into_job_args_value(value: Option<GoShared<Self>>) -> JobArgsValue;

    /// Exact dynamic-type assertion used by V2 getters.
    fn from_job_args_value(value: &JobArgsValue) -> Option<Option<GoShared<Self>>>;

    /// Go `getArgsV1`.
    fn get_args_v1(value: Option<&GoShared<Self>>, job: &Job) -> GoSharedSlice<GoAny>;

    /// Go `decodeV1` plus the `job.args` pointer cache.
    fn decode_v1(job: &mut Job) -> Result<Option<GoShared<Self>>, serde_json::Error>;
}

/// Go's private `FinishedJobArgs` extension.
pub trait FinishedJobArgs: JobArgs {
    /// Go `getFinishedArgsV1`.
    fn get_finished_args_v1(value: Option<&GoShared<Self>>, job: &Job) -> GoSharedSlice<GoAny>;
}

macro_rules! job_args_identity_methods {
    ($variant:ident) => {
        fn into_job_args_value(value: Option<GoShared<Self>>) -> JobArgsValue {
            JobArgsValue::$variant(value)
        }

        fn from_job_args_value(value: &JobArgsValue) -> Option<Option<GoShared<Self>>> {
            match value {
                JobArgsValue::$variant(value) => Some(value.clone()),
                _ => None,
            }
        }
    };
}

impl Job {
    /// Go `(*Job).FillArgs`.
    pub fn fill_args<T: JobArgs>(&mut self, value: Option<GoShared<T>>) {
        assert!(
            self.version == JobVersion::V1 || self.version == JobVersion::V2,
            "job version is invalid"
        );
        if self.version == JobVersion::V1 {
            self.set_v1_decoded_args(T::get_args_v1(value.as_ref(), self));
        } else {
            self.fill_v2_arg(GoAny::new(T::into_job_args_value(value)));
        }
    }

    /// Go `(*Job).FillFinishedArgs`.
    pub fn fill_finished_args<T: FinishedJobArgs>(&mut self, value: Option<GoShared<T>>) {
        assert!(
            self.version == JobVersion::V1 || self.version == JobVersion::V2,
            "job version is invalid"
        );
        if self.version == JobVersion::V1 {
            self.set_v1_decoded_args(T::get_finished_args_v1(value.as_ref(), self));
        } else {
            self.fill_v2_arg(GoAny::new(T::into_job_args_value(value)));
        }
    }
}

pub(crate) fn get_or_decode_args_v2<T: JobArgs>(
    job: &mut Job,
) -> Result<Option<GoShared<T>>, serde_json::Error> {
    assert_eq!(job.version, JobVersion::V2, "job version is not v2");
    let decoded = job.decoded_args();
    if !decoded.is_empty() {
        assert_eq!(decoded.len(), 1, "job args length is not 1");
        let value = decoded.get(0);
        let value = T::from_job_args_value(
            value
                .job_args_value()
                .unwrap_or_else(|| panic!("interface conversion: dynamic value is not JobArgs")),
        )
        .unwrap_or_else(|| panic!("interface conversion: wrong JobArgs dynamic type"));
        return Ok(value);
    }
    let bytes = job
        .raw_args
        .as_ref()
        .map_or_else(Vec::new, crate::PersistedRawJson::bytes);
    let value = serde_json::from_slice::<Option<GoShared<T>>>(&bytes)?;
    job.fill_v2_arg(GoAny::new(T::into_job_args_value(value.clone())));
    Ok(value)
}

pub(crate) fn get_or_decode_args<T: JobArgs>(
    job: &mut Job,
) -> Result<Option<GoShared<T>>, serde_json::Error> {
    if job.version == JobVersion::V1 {
        T::decode_v1(job)
    } else {
        get_or_decode_args_v2(job)
    }
}

#[path = "job_args_schema_table.rs"]
mod schema_table;
pub use schema_table::*;

#[path = "job_args_alter.rs"]
mod alter;
pub use alter::*;

#[path = "job_args_compat.rs"]
mod compat;
pub use compat::{
    index_arg_columnar_index_type, rename_tables_args_from_v1, IndexOp, RenameTableArgs,
};

#[cfg(test)]
#[path = "job_args_tests.rs"]
pub(crate) mod tests;
