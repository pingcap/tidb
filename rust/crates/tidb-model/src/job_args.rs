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

use crate::{
    ActionType, ColumnDefaultValue, ColumnarIndexType, DBInfo, GoAny, GoAnyBytes, GoAnyJsonError,
    GoAnyValue, GoEqualityProjection, GoJsonProjection, GoJsonReference, GoJsonReferenceIdentity,
    GoJsonValue, GoShared, GoSharedPointerSlice, GoSharedSlice, GoTypeIdentity, GoTypeKind, Job,
    JobState, JobVersion, PartitionInfo, PolicyRefInfo, TableInfo,
};

const MODEL_PACKAGE_PATH: &str = "github.com/pingcap/tidb/pkg/meta/model";

fn model_type(name: &str, kind: GoTypeKind) -> GoTypeIdentity {
    GoTypeIdentity::defined(MODEL_PACKAGE_PATH, name, format!("model.{name}"), kind)
}

fn builtin_type(name: &str, kind: GoTypeKind) -> GoTypeIdentity {
    GoTypeIdentity::unnamed(name, kind)
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
    /// `*model.DropSchemaArgs`, including a typed nil pointer.
    DropSchema(Option<GoShared<DropSchemaArgs>>),
    /// `*model.ModifySchemaArgs`, including a typed nil pointer.
    ModifySchema(Option<GoShared<ModifySchemaArgs>>),
    /// `*model.CreateTableArgs`, including a typed nil pointer.
    CreateTable(Option<GoShared<CreateTableArgs>>),
    /// `*model.BatchCreateTableArgs`, including a typed nil pointer.
    BatchCreateTable(Option<GoShared<BatchCreateTableArgs>>),
    /// `*model.TruncateTableArgs`, including a typed nil pointer.
    TruncateTable(Option<GoShared<TruncateTableArgs>>),
    /// `*model.TablePartitionArgs`, including a typed nil pointer.
    TablePartition(Option<GoShared<TablePartitionArgs>>),
    /// `*model.ExchangeTablePartitionArgs`, including a typed nil pointer.
    ExchangeTablePartition(Option<GoShared<ExchangeTablePartitionArgs>>),
    /// `*model.RebaseAutoIDArgs`, including a typed nil pointer.
    RebaseAutoId(Option<GoShared<RebaseAutoIDArgs>>),
    /// `*model.ModifyTableCommentArgs`, including a typed nil pointer.
    ModifyTableComment(Option<GoShared<ModifyTableCommentArgs>>),
    /// `*model.ModifyTableCharsetAndCollateArgs`, including a typed nil pointer.
    ModifyTableCharsetAndCollate(Option<GoShared<ModifyTableCharsetAndCollateArgs>>),
}

impl JobArgsValue {
    fn go_type_identity(&self) -> GoTypeIdentity {
        let name = match self {
            Self::Empty(_) => "EmptyArgs",
            Self::CreateSchema(_) => "CreateSchemaArgs",
            Self::DropSchema(_) => "DropSchemaArgs",
            Self::ModifySchema(_) => "ModifySchemaArgs",
            Self::CreateTable(_) => "CreateTableArgs",
            Self::BatchCreateTable(_) => "BatchCreateTableArgs",
            Self::TruncateTable(_) => "TruncateTableArgs",
            Self::TablePartition(_) => "TablePartitionArgs",
            Self::ExchangeTablePartition(_) => "ExchangeTablePartitionArgs",
            Self::RebaseAutoId(_) => "RebaseAutoIDArgs",
            Self::ModifyTableComment(_) => "ModifyTableCommentArgs",
            Self::ModifyTableCharsetAndCollate(_) => "ModifyTableCharsetAndCollateArgs",
        };
        model_type(name, GoTypeKind::Struct).pointer_to()
    }

    fn pointer_address(&self) -> Option<usize> {
        match self {
            Self::Empty(value) => value.as_ref().map(GoShared::identity_address),
            Self::CreateSchema(value) => value.as_ref().map(GoShared::identity_address),
            Self::DropSchema(value) => value.as_ref().map(GoShared::identity_address),
            Self::ModifySchema(value) => value.as_ref().map(GoShared::identity_address),
            Self::CreateTable(value) => value.as_ref().map(GoShared::identity_address),
            Self::BatchCreateTable(value) => value.as_ref().map(GoShared::identity_address),
            Self::TruncateTable(value) => value.as_ref().map(GoShared::identity_address),
            Self::TablePartition(value) => value.as_ref().map(GoShared::identity_address),
            Self::ExchangeTablePartition(value) => value.as_ref().map(GoShared::identity_address),
            Self::RebaseAutoId(value) => value.as_ref().map(GoShared::identity_address),
            Self::ModifyTableComment(value) => value.as_ref().map(GoShared::identity_address),
            Self::ModifyTableCharsetAndCollate(value) => {
                value.as_ref().map(GoShared::identity_address)
            }
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

/// Go `EmptyArgs`.
#[derive(Clone, Debug, Default, Serialize)]
pub struct EmptyArgs {}

impl JobArgs for EmptyArgs {
    fn into_job_args_value(value: Option<GoShared<Self>>) -> JobArgsValue {
        JobArgsValue::Empty(value)
    }

    fn from_job_args_value(value: &JobArgsValue) -> Option<Option<GoShared<Self>>> {
        match value {
            JobArgsValue::Empty(value) => Some(value.clone()),
            _ => None,
        }
    }

    fn get_args_v1(_value: Option<&GoShared<Self>>, _job: &Job) -> GoSharedSlice<GoAny> {
        GoSharedSlice::default()
    }

    fn decode_v1(job: &mut Job) -> Result<Option<GoShared<Self>>, serde_json::Error> {
        assert_eq!(job.version, JobVersion::V1, "job version is not v1");
        Ok(Some(GoShared::new(Self {})))
    }
}

/// Go `CreateSchemaArgs`.
#[derive(Clone, Debug, Default, Serialize)]
pub struct CreateSchemaArgs {
    /// Database metadata pointer.
    #[serde(
        rename = "db_info",
        default,
        skip_serializing_if = "field_shared_pointer_is_none"
    )]
    pub db_info: GoField<Option<GoShared<DBInfo>>>,
}

impl JobArgs for CreateSchemaArgs {
    fn into_job_args_value(value: Option<GoShared<Self>>) -> JobArgsValue {
        JobArgsValue::CreateSchema(value)
    }

    fn from_job_args_value(value: &JobArgsValue) -> Option<Option<GoShared<Self>>> {
        match value {
            JobArgsValue::CreateSchema(value) => Some(value.clone()),
            _ => None,
        }
    }

    fn get_args_v1(value: Option<&GoShared<Self>>, _job: &Job) -> GoSharedSlice<GoAny> {
        let value = value.expect("nil *CreateSchemaArgs receiver").read();
        GoSharedSlice::from_vec(vec![typed_pointer_any(
            model_type("DBInfo", GoTypeKind::Struct),
            value.db_info.get(),
        )])
    }

    fn decode_v1(job: &mut Job) -> Result<Option<GoShared<Self>>, serde_json::Error> {
        let database = GoShared::new(DBInfo::default());
        let value = GoShared::new(Self {
            db_info: GoField::new(Some(database.clone())),
        });
        let mut decoder = V1Decoder::new(job)?;
        decoder.decode_pointee(&database, model_type("DBInfo", GoTypeKind::Struct))?;
        decoder.finish(job);
        Ok(Some(value))
    }
}

/// Go `GetCreateSchemaArgs`.
pub fn get_create_schema_args(
    job: &mut Job,
) -> Result<Option<GoShared<CreateSchemaArgs>>, serde_json::Error> {
    get_or_decode_args(job)
}

/// Go `DropSchemaArgs`.
#[derive(Clone, Debug, Default, Serialize)]
pub struct DropSchemaArgs {
    /// Submission-time foreign-key check flag.
    #[serde(rename = "fk_check", default, skip_serializing_if = "field_is_default")]
    pub fk_check: GoField<bool>,
    /// Finished-job physical table identifiers.
    #[serde(
        rename = "all_dropped_table_ids",
        default,
        skip_serializing_if = "field_shared_slice_is_empty"
    )]
    pub all_dropped_table_ids: GoField<GoSharedSlice<i64>>,
}

impl JobArgs for DropSchemaArgs {
    fn into_job_args_value(value: Option<GoShared<Self>>) -> JobArgsValue {
        JobArgsValue::DropSchema(value)
    }

    fn from_job_args_value(value: &JobArgsValue) -> Option<Option<GoShared<Self>>> {
        match value {
            JobArgsValue::DropSchema(value) => Some(value.clone()),
            _ => None,
        }
    }

    fn get_args_v1(value: Option<&GoShared<Self>>, _job: &Job) -> GoSharedSlice<GoAny> {
        let value = value.expect("nil *DropSchemaArgs receiver").read();
        GoSharedSlice::from_vec(vec![ColumnDefaultValue::Bool(value.fk_check.get()).into()])
    }

    fn decode_v1(job: &mut Job) -> Result<Option<GoShared<Self>>, serde_json::Error> {
        let value = GoShared::new(Self::default());
        let mut decoder = V1Decoder::new(job)?;
        decoder.decode(
            &value.read().fk_check,
            builtin_type("bool", GoTypeKind::Bool),
        )?;
        decoder.finish(job);
        Ok(Some(value))
    }
}

impl FinishedJobArgs for DropSchemaArgs {
    fn get_finished_args_v1(value: Option<&GoShared<Self>>, _job: &Job) -> GoSharedSlice<GoAny> {
        let value = value.expect("nil *DropSchemaArgs receiver").read();
        GoSharedSlice::from_vec(vec![typed_value_any(
            builtin_type("[]int64", GoTypeKind::Slice),
            value.all_dropped_table_ids.get(),
        )])
    }
}

/// Go `GetDropSchemaArgs`.
pub fn get_drop_schema_args(
    job: &mut Job,
) -> Result<Option<GoShared<DropSchemaArgs>>, serde_json::Error> {
    get_or_decode_args(job)
}

/// Go `GetFinishedDropSchemaArgs`.
pub fn get_finished_drop_schema_args(
    job: &mut Job,
) -> Result<Option<GoShared<DropSchemaArgs>>, serde_json::Error> {
    if job.version == JobVersion::V1 {
        let value = GoShared::new(DropSchemaArgs::default());
        let mut decoder = V1Decoder::new(job)?;
        decoder.decode(
            &value.read().all_dropped_table_ids,
            builtin_type("[]int64", GoTypeKind::Slice),
        )?;
        decoder.finish(job);
        Ok(Some(value))
    } else {
        get_or_decode_args_v2(job)
    }
}

/// Go `ModifySchemaArgs`.
#[derive(Clone, Debug, Default, Serialize)]
pub struct ModifySchemaArgs {
    /// Destination charset.
    #[serde(
        rename = "to_charset",
        default,
        skip_serializing_if = "field_is_default"
    )]
    pub to_charset: GoField<GoString>,
    /// Destination collation.
    #[serde(
        rename = "to_collate",
        default,
        skip_serializing_if = "field_is_default"
    )]
    pub to_collate: GoField<GoString>,
    /// Nullable placement policy reference.
    #[serde(
        rename = "policy_ref",
        default,
        skip_serializing_if = "field_shared_pointer_is_none"
    )]
    pub policy_ref: GoField<Option<GoShared<PolicyRefInfo>>>,
}

impl JobArgs for ModifySchemaArgs {
    fn into_job_args_value(value: Option<GoShared<Self>>) -> JobArgsValue {
        JobArgsValue::ModifySchema(value)
    }

    fn from_job_args_value(value: &JobArgsValue) -> Option<Option<GoShared<Self>>> {
        match value {
            JobArgsValue::ModifySchema(value) => Some(value.clone()),
            _ => None,
        }
    }

    fn get_args_v1(value: Option<&GoShared<Self>>, job: &Job) -> GoSharedSlice<GoAny> {
        let value = value.expect("nil *ModifySchemaArgs receiver").read();
        if job.type_ == ActionType::ACTION_MODIFY_SCHEMA_CHARSET_AND_COLLATE {
            return GoSharedSlice::from_vec(vec![
                ColumnDefaultValue::Str(value.to_charset.get()).into(),
                ColumnDefaultValue::Str(value.to_collate.get()).into(),
            ]);
        }
        GoSharedSlice::from_vec(vec![typed_pointer_any(
            model_type("PolicyRefInfo", GoTypeKind::Struct),
            value.policy_ref.get(),
        )])
    }

    fn decode_v1(job: &mut Job) -> Result<Option<GoShared<Self>>, serde_json::Error> {
        let value = GoShared::new(Self::default());
        let mut decoder = V1Decoder::new(job)?;
        if job.type_ == ActionType::ACTION_MODIFY_SCHEMA_CHARSET_AND_COLLATE {
            decoder.decode(
                &value.read().to_charset,
                builtin_type("string", GoTypeKind::String),
            )?;
            decoder.decode(
                &value.read().to_collate,
                builtin_type("string", GoTypeKind::String),
            )?;
        } else {
            decoder.decode(
                &value.read().policy_ref,
                model_type("PolicyRefInfo", GoTypeKind::Struct).pointer_to(),
            )?;
        }
        decoder.finish(job);
        Ok(Some(value))
    }
}

/// Go `GetModifySchemaArgs`.
pub fn get_modify_schema_args(
    job: &mut Job,
) -> Result<Option<GoShared<ModifySchemaArgs>>, serde_json::Error> {
    get_or_decode_args(job)
}

/// Go `CreateTableArgs`.
#[derive(Clone, Debug, Default, Serialize)]
pub struct CreateTableArgs {
    /// Table metadata pointer.
    #[serde(
        rename = "table_info",
        default,
        skip_serializing_if = "field_shared_pointer_is_none"
    )]
    pub table_info: GoField<Option<GoShared<TableInfo>>>,
    /// Create-view replace flag.
    #[serde(
        rename = "on_exist_replace",
        default,
        skip_serializing_if = "field_is_default"
    )]
    pub on_exist_replace: GoField<bool>,
    /// Replaced view identifier.
    #[serde(
        rename = "old_view_tbl_id",
        default,
        skip_serializing_if = "field_is_default"
    )]
    pub old_view_table_id: GoField<i64>,
    /// Submission-time foreign-key check flag.
    #[serde(rename = "fk_check", default, skip_serializing_if = "field_is_default")]
    pub fk_check: GoField<bool>,
}

impl JobArgs for CreateTableArgs {
    fn into_job_args_value(value: Option<GoShared<Self>>) -> JobArgsValue {
        JobArgsValue::CreateTable(value)
    }

    fn from_job_args_value(value: &JobArgsValue) -> Option<Option<GoShared<Self>>> {
        match value {
            JobArgsValue::CreateTable(value) => Some(value.clone()),
            _ => None,
        }
    }

    fn get_args_v1(value: Option<&GoShared<Self>>, job: &Job) -> GoSharedSlice<GoAny> {
        let value = value.expect("nil *CreateTableArgs receiver").read();
        let table = || {
            typed_pointer_any(
                model_type("TableInfo", GoTypeKind::Struct),
                value.table_info.get(),
            )
        };
        match job.type_ {
            ActionType::ACTION_CREATE_TABLE => GoSharedSlice::from_vec(vec![
                table(),
                ColumnDefaultValue::Bool(value.fk_check.get()).into(),
            ]),
            ActionType::ACTION_CREATE_VIEW => GoSharedSlice::from_vec(vec![
                table(),
                ColumnDefaultValue::Bool(value.on_exist_replace.get()).into(),
                ColumnDefaultValue::Int(value.old_view_table_id.get()).into(),
            ]),
            ActionType::ACTION_CREATE_SEQUENCE => GoSharedSlice::from_vec(vec![table()]),
            _ => GoSharedSlice::default(),
        }
    }

    fn decode_v1(job: &mut Job) -> Result<Option<GoShared<Self>>, serde_json::Error> {
        let table_info = GoShared::new(TableInfo::default());
        let value = GoShared::new(Self {
            table_info: GoField::new(Some(table_info.clone())),
            ..Default::default()
        });
        let mut decoder = V1Decoder::new(job)?;
        match job.type_ {
            ActionType::ACTION_CREATE_TABLE => {
                decoder.decode_pointee(&table_info, model_type("TableInfo", GoTypeKind::Struct))?;
                decoder.decode(
                    &value.read().fk_check,
                    builtin_type("bool", GoTypeKind::Bool),
                )?;
            }
            ActionType::ACTION_CREATE_VIEW => {
                decoder.decode_pointee(&table_info, model_type("TableInfo", GoTypeKind::Struct))?;
                decoder.decode(
                    &value.read().on_exist_replace,
                    builtin_type("bool", GoTypeKind::Bool),
                )?;
                decoder.decode(
                    &value.read().old_view_table_id,
                    builtin_type("int64", GoTypeKind::Int64),
                )?;
            }
            ActionType::ACTION_CREATE_SEQUENCE => {
                decoder.decode_pointee(&table_info, model_type("TableInfo", GoTypeKind::Struct))?;
            }
            _ => {}
        }
        decoder.finish(job);
        Ok(Some(value))
    }
}

/// Go `GetCreateTableArgs`.
pub fn get_create_table_args(
    job: &mut Job,
) -> Result<Option<GoShared<CreateTableArgs>>, serde_json::Error> {
    get_or_decode_args(job)
}

/// Go `BatchCreateTableArgs`.
#[derive(Clone, Debug, Default, Serialize)]
pub struct BatchCreateTableArgs {
    /// Create-table argument pointers in source order.
    #[serde(
        rename = "tables",
        default,
        skip_serializing_if = "field_shared_pointer_slice_is_empty"
    )]
    pub tables: GoField<GoSharedPointerSlice<CreateTableArgs>>,
}

impl JobArgs for BatchCreateTableArgs {
    fn into_job_args_value(value: Option<GoShared<Self>>) -> JobArgsValue {
        JobArgsValue::BatchCreateTable(value)
    }

    fn from_job_args_value(value: &JobArgsValue) -> Option<Option<GoShared<Self>>> {
        match value {
            JobArgsValue::BatchCreateTable(value) => Some(value.clone()),
            _ => None,
        }
    }

    fn get_args_v1(value: Option<&GoShared<Self>>, _job: &Job) -> GoSharedSlice<GoAny> {
        let value = value.expect("nil *BatchCreateTableArgs receiver").read();
        let tables = value.tables.get();
        let infos = GoSharedPointerSlice::from_handles(
            tables
                .iter_handles()
                .map(|table| {
                    table
                        .expect("nil *CreateTableArgs in BatchCreateTableArgs.Tables")
                        .read()
                        .table_info
                        .get()
                })
                .collect(),
        );
        let fk_check = tables
            .get(0)
            .expect("BatchCreateTableArgs.Tables[0] is nil or missing")
            .read()
            .fk_check
            .get();
        GoSharedSlice::from_vec(vec![
            typed_value_any(builtin_type("[]*model.TableInfo", GoTypeKind::Slice), infos),
            ColumnDefaultValue::Bool(fk_check).into(),
        ])
    }

    fn decode_v1(job: &mut Job) -> Result<Option<GoShared<Self>>, serde_json::Error> {
        let table_infos = GoField::<GoSharedPointerSlice<TableInfo>>::default();
        let fk_check = GoField::<bool>::default();
        let mut decoder = V1Decoder::new(job)?;
        decoder.decode(
            &table_infos,
            builtin_type("[]*model.TableInfo", GoTypeKind::Slice),
        )?;
        decoder.decode(&fk_check, builtin_type("bool", GoTypeKind::Bool))?;
        decoder.finish(job);

        let table_infos = table_infos.get();
        let fk_check = fk_check.get();
        let tables = GoSharedPointerSlice::from_handles(
            table_infos
                .iter_handles()
                .map(|table_info| {
                    Some(GoShared::new(CreateTableArgs {
                        table_info: GoField::new(table_info),
                        fk_check: GoField::new(fk_check),
                        ..Default::default()
                    }))
                })
                .collect(),
        );
        Ok(Some(GoShared::new(Self {
            tables: GoField::new(tables),
        })))
    }
}

/// Go `GetBatchCreateTableArgs`.
pub fn get_batch_create_table_args(
    job: &mut Job,
) -> Result<Option<GoShared<BatchCreateTableArgs>>, serde_json::Error> {
    get_or_decode_args(job)
}

/// Go `TruncateTableArgs`.
#[derive(Clone, Debug, Default, Serialize)]
pub struct TruncateTableArgs {
    /// Submission-time foreign-key check flag.
    #[serde(rename = "fk_check", default, skip_serializing_if = "field_is_default")]
    pub fk_check: GoField<bool>,
    /// New physical table identifier.
    #[serde(
        rename = "new_table_id",
        default,
        skip_serializing_if = "field_is_default"
    )]
    pub new_table_id: GoField<i64>,
    /// Replacement partition identifiers.
    #[serde(
        rename = "new_partition_ids",
        default,
        skip_serializing_if = "field_shared_slice_is_empty"
    )]
    pub new_partition_ids: GoField<GoSharedSlice<i64>>,
    /// Previous partition identifiers.
    #[serde(
        rename = "old_partition_ids",
        default,
        skip_serializing_if = "field_shared_slice_is_empty"
    )]
    pub old_partition_ids: GoField<GoSharedSlice<i64>>,
    /// Runtime-only policy-bearing new partitions.
    #[serde(skip)]
    pub new_part_ids_with_policy: GoField<GoSharedSlice<i64>>,
    /// Runtime-only policy-bearing old partitions.
    #[serde(skip)]
    pub old_part_ids_with_policy: GoField<GoSharedSlice<i64>>,
    /// Runtime-only affected-partition update switch.
    #[serde(skip)]
    pub should_update_affected_partitions: GoField<bool>,
}

impl JobArgs for TruncateTableArgs {
    fn into_job_args_value(value: Option<GoShared<Self>>) -> JobArgsValue {
        JobArgsValue::TruncateTable(value)
    }

    fn from_job_args_value(value: &JobArgsValue) -> Option<Option<GoShared<Self>>> {
        match value {
            JobArgsValue::TruncateTable(value) => Some(value.clone()),
            _ => None,
        }
    }

    fn get_args_v1(value: Option<&GoShared<Self>>, job: &Job) -> GoSharedSlice<GoAny> {
        let value = value.expect("nil *TruncateTableArgs receiver").read();
        if job.type_ == ActionType::ACTION_TRUNCATE_TABLE {
            return GoSharedSlice::from_vec(vec![
                ColumnDefaultValue::Int(value.new_table_id.get()).into(),
                ColumnDefaultValue::Bool(value.fk_check.get()).into(),
                typed_value_any(
                    builtin_type("[]int64", GoTypeKind::Slice),
                    value.new_partition_ids.get(),
                ),
                typed_value_any(
                    builtin_type("int", GoTypeKind::Int64),
                    value.old_partition_ids.read().len() as i64,
                ),
            ]);
        }
        GoSharedSlice::from_vec(vec![
            typed_value_any(
                builtin_type("[]int64", GoTypeKind::Slice),
                value.old_partition_ids.get(),
            ),
            typed_value_any(
                builtin_type("[]int64", GoTypeKind::Slice),
                value.new_partition_ids.get(),
            ),
        ])
    }

    fn decode_v1(job: &mut Job) -> Result<Option<GoShared<Self>>, serde_json::Error> {
        let value = GoShared::new(Self::default());
        let mut decoder = V1Decoder::new(job)?;
        if job.type_ == ActionType::ACTION_TRUNCATE_TABLE {
            decoder.decode(
                &value.read().new_table_id,
                builtin_type("int64", GoTypeKind::Int64),
            )?;
            decoder.decode(
                &value.read().fk_check,
                builtin_type("bool", GoTypeKind::Bool),
            )?;
            decoder.decode(
                &value.read().new_partition_ids,
                builtin_type("[]int64", GoTypeKind::Slice),
            )?;
        } else {
            decoder.decode(
                &value.read().old_partition_ids,
                builtin_type("[]int64", GoTypeKind::Slice),
            )?;
            decoder.decode(
                &value.read().new_partition_ids,
                builtin_type("[]int64", GoTypeKind::Slice),
            )?;
        }
        decoder.finish(job);
        Ok(Some(value))
    }
}

impl FinishedJobArgs for TruncateTableArgs {
    fn get_finished_args_v1(value: Option<&GoShared<Self>>, job: &Job) -> GoSharedSlice<GoAny> {
        let value = value.expect("nil *TruncateTableArgs receiver").read();
        let old_ids = || {
            typed_value_any(
                builtin_type("[]int64", GoTypeKind::Slice),
                value.old_partition_ids.get(),
            )
        };
        if job.type_ == ActionType::ACTION_TRUNCATE_TABLE {
            return GoSharedSlice::from_vec(vec![
                ColumnDefaultValue::Bytes(GoAnyBytes::from_vec(Vec::new())).into(),
                old_ids(),
            ]);
        }
        GoSharedSlice::from_vec(vec![old_ids()])
    }
}

/// Go `GetTruncateTableArgs`.
pub fn get_truncate_table_args(
    job: &mut Job,
) -> Result<Option<GoShared<TruncateTableArgs>>, serde_json::Error> {
    get_or_decode_args(job)
}

/// Go `GetFinishedTruncateTableArgs`.
pub fn get_finished_truncate_table_args(
    job: &mut Job,
) -> Result<Option<GoShared<TruncateTableArgs>>, serde_json::Error> {
    if job.version != JobVersion::V1 {
        return get_or_decode_args_v2(job);
    }
    let value = GoShared::new(TruncateTableArgs::default());
    let mut decoder = V1Decoder::new(job)?;
    if job.type_ == ActionType::ACTION_TRUNCATE_TABLE {
        let start_key = GoField::<GoByteSlice>::default();
        decoder.decode(&start_key, builtin_type("[]uint8", GoTypeKind::Slice))?;
    }
    decoder.decode(
        &value.read().old_partition_ids,
        builtin_type("[]int64", GoTypeKind::Slice),
    )?;
    decoder.finish(job);
    Ok(Some(value))
}

/// Go `TableIDIndexID`: one table/index pair whose index range is deleted.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct TableIDIndexID {
    /// Physical table identifier.
    #[serde(rename = "TableID")]
    pub table_id: i64,
    /// Index identifier.
    #[serde(rename = "IndexID")]
    pub index_id: i64,
}

/// Go `TablePartitionArgs`.
#[derive(Clone, Debug, Default, Serialize)]
pub struct TablePartitionArgs {
    /// Affected partition names.
    #[serde(
        rename = "part_names",
        default,
        skip_serializing_if = "field_shared_slice_is_empty"
    )]
    pub part_names: GoField<GoSharedSlice<GoString>>,
    /// Partition metadata. V1 decoding always allocates this pointee.
    #[serde(
        rename = "part_info",
        default,
        skip_serializing_if = "field_shared_pointer_is_none"
    )]
    pub part_info: GoField<Option<GoShared<PartitionInfo>>>,
    /// Finished-job physical table identifiers.
    #[serde(
        rename = "old_physical_tbl_ids",
        default,
        skip_serializing_if = "field_shared_slice_is_empty"
    )]
    pub old_physical_table_ids: GoField<GoSharedSlice<i64>>,
    /// Finished-job global index identifiers.
    #[serde(
        rename = "old_global_indexes",
        default,
        skip_serializing_if = "field_shared_slice_is_empty"
    )]
    pub old_global_indexes: GoField<GoSharedSlice<TableIDIndexID>>,
    /// Runtime-only replacement partition identifiers.
    #[serde(skip)]
    pub new_partition_ids: GoField<GoSharedSlice<i64>>,
}

impl JobArgs for TablePartitionArgs {
    fn into_job_args_value(value: Option<GoShared<Self>>) -> JobArgsValue {
        JobArgsValue::TablePartition(value)
    }

    fn from_job_args_value(value: &JobArgsValue) -> Option<Option<GoShared<Self>>> {
        match value {
            JobArgsValue::TablePartition(value) => Some(value.clone()),
            _ => None,
        }
    }

    fn get_args_v1(value: Option<&GoShared<Self>>, job: &Job) -> GoSharedSlice<GoAny> {
        let value = value.expect("nil *TablePartitionArgs receiver").read();
        if job.type_ == ActionType::ACTION_ADD_TABLE_PARTITION {
            return GoSharedSlice::from_vec(vec![typed_pointer_any(
                model_type("PartitionInfo", GoTypeKind::Struct),
                value.part_info.get(),
            )]);
        }
        let part_names = || {
            typed_value_any(
                builtin_type("[]string", GoTypeKind::Slice),
                value.part_names.get(),
            )
        };
        if job.type_ == ActionType::ACTION_DROP_TABLE_PARTITION {
            return GoSharedSlice::from_vec(vec![part_names()]);
        }
        GoSharedSlice::from_vec(vec![
            part_names(),
            typed_pointer_any(
                model_type("PartitionInfo", GoTypeKind::Struct),
                value.part_info.get(),
            ),
        ])
    }

    fn decode_v1(job: &mut Job) -> Result<Option<GoShared<Self>>, serde_json::Error> {
        let part_names = GoField::<GoSharedSlice<GoString>>::default();
        let part_info = GoShared::new(PartitionInfo::default());
        let mut decoder = V1Decoder::new(job)?;
        if job.type_ == ActionType::ACTION_ADD_TABLE_PARTITION {
            if job.state == JobState::ROLLINGBACK {
                decoder.decode(&part_names, builtin_type("[]string", GoTypeKind::Slice))?;
            } else {
                decoder
                    .decode_pointee(&part_info, model_type("PartitionInfo", GoTypeKind::Struct))?;
            }
        } else if job.type_ == ActionType::ACTION_DROP_TABLE_PARTITION {
            decoder.decode(&part_names, builtin_type("[]string", GoTypeKind::Slice))?;
        } else {
            decoder.decode(&part_names, builtin_type("[]string", GoTypeKind::Slice))?;
            decoder.decode_pointee(&part_info, model_type("PartitionInfo", GoTypeKind::Struct))?;
        }
        decoder.finish(job);
        Ok(Some(GoShared::new(Self {
            part_names: GoField::new(part_names.get()),
            part_info: GoField::new(Some(part_info)),
            ..Default::default()
        })))
    }
}

impl FinishedJobArgs for TablePartitionArgs {
    fn get_finished_args_v1(value: Option<&GoShared<Self>>, job: &Job) -> GoSharedSlice<GoAny> {
        assert!(
            job.type_ != ActionType::ACTION_ADD_TABLE_PARTITION
                || job.state == JobState::ROLLBACK_DONE,
            "add table partition job should not call getFinishedArgsV1 if not rollback"
        );
        let value = value.expect("nil *TablePartitionArgs receiver").read();
        GoSharedSlice::from_vec(vec![
            typed_value_any(
                builtin_type("[]int64", GoTypeKind::Slice),
                value.old_physical_table_ids.get(),
            ),
            typed_value_any(
                model_type("TableIDIndexID", GoTypeKind::Struct).slice_of(),
                value.old_global_indexes.get(),
            ),
        ])
    }
}

/// Go `GetTablePartitionArgs`.
pub fn get_table_partition_args(
    job: &mut Job,
) -> Result<Option<GoShared<TablePartitionArgs>>, serde_json::Error> {
    let value =
        get_or_decode_args::<TablePartitionArgs>(job)?.expect("nil *TablePartitionArgs receiver");
    if value.read().part_info.get().is_none() {
        value
            .read()
            .part_info
            .set(Some(GoShared::new(PartitionInfo::default())));
    }
    Ok(Some(value))
}

/// Go `GetFinishedTablePartitionArgs`.
pub fn get_finished_table_partition_args(
    job: &mut Job,
) -> Result<Option<GoShared<TablePartitionArgs>>, serde_json::Error> {
    if job.version != JobVersion::V1 {
        return get_or_decode_args_v2(job);
    }
    let old_physical_table_ids = GoField::<GoSharedSlice<i64>>::default();
    let old_global_indexes = GoField::<GoSharedSlice<TableIDIndexID>>::default();
    let mut decoder = V1Decoder::new(job)?;
    decoder.decode(
        &old_physical_table_ids,
        builtin_type("[]int64", GoTypeKind::Slice),
    )?;
    decoder.decode(
        &old_global_indexes,
        model_type("TableIDIndexID", GoTypeKind::Struct).slice_of(),
    )?;
    decoder.finish(job);
    Ok(Some(GoShared::new(TablePartitionArgs {
        old_physical_table_ids: GoField::new(old_physical_table_ids.get()),
        old_global_indexes: GoField::new(old_global_indexes.get()),
        ..Default::default()
    })))
}

/// Go `FillRollbackArgsForAddPartition`.
pub fn fill_rollback_args_for_add_partition(
    job: &mut Job,
    args: Option<&GoShared<TablePartitionArgs>>,
) {
    assert_eq!(
        job.type_,
        ActionType::ACTION_ADD_TABLE_PARTITION,
        "only for add partition job"
    );
    let part_names = args
        .expect("nil *TablePartitionArgs receiver")
        .read()
        .part_names
        .get();
    let mut fake = Job {
        version: job.version,
        type_: ActionType::ACTION_DROP_TABLE_PARTITION,
        ..Default::default()
    };
    fake.fill_args(Some(GoShared::new(TablePartitionArgs {
        part_names: GoField::new(part_names),
        ..Default::default()
    })));
    job.set_v1_decoded_args(fake.decoded_args());
}

/// Go `ExchangeTablePartitionArgs`.
#[derive(Clone, Debug, Default, Serialize)]
pub struct ExchangeTablePartitionArgs {
    /// Exchanged partition identifier.
    #[serde(
        rename = "partition_id",
        default,
        skip_serializing_if = "field_is_default"
    )]
    pub partition_id: GoField<i64>,
    /// Partitioned-table schema identifier.
    #[serde(
        rename = "pt_schema_id",
        default,
        skip_serializing_if = "field_is_default"
    )]
    pub partitioned_table_schema_id: GoField<i64>,
    /// Partitioned-table identifier.
    #[serde(
        rename = "pt_table_id",
        default,
        skip_serializing_if = "field_is_default"
    )]
    pub partitioned_table_id: GoField<i64>,
    /// Exchanged partition name.
    #[serde(
        rename = "partition_name",
        default,
        skip_serializing_if = "field_is_default"
    )]
    pub partition_name: GoField<GoString>,
    /// Whether row validation is required.
    #[serde(
        rename = "with_validation",
        default,
        skip_serializing_if = "field_is_default"
    )]
    pub with_validation: GoField<bool>,
}

impl JobArgs for ExchangeTablePartitionArgs {
    fn into_job_args_value(value: Option<GoShared<Self>>) -> JobArgsValue {
        JobArgsValue::ExchangeTablePartition(value)
    }

    fn from_job_args_value(value: &JobArgsValue) -> Option<Option<GoShared<Self>>> {
        match value {
            JobArgsValue::ExchangeTablePartition(value) => Some(value.clone()),
            _ => None,
        }
    }

    fn get_args_v1(value: Option<&GoShared<Self>>, _job: &Job) -> GoSharedSlice<GoAny> {
        let value = value
            .expect("nil *ExchangeTablePartitionArgs receiver")
            .read();
        GoSharedSlice::from_vec(vec![
            ColumnDefaultValue::Int(value.partition_id.get()).into(),
            ColumnDefaultValue::Int(value.partitioned_table_schema_id.get()).into(),
            ColumnDefaultValue::Int(value.partitioned_table_id.get()).into(),
            ColumnDefaultValue::Str(value.partition_name.get()).into(),
            ColumnDefaultValue::Bool(value.with_validation.get()).into(),
        ])
    }

    fn decode_v1(job: &mut Job) -> Result<Option<GoShared<Self>>, serde_json::Error> {
        let value = GoShared::new(Self::default());
        let mut decoder = V1Decoder::new(job)?;
        decoder.decode(
            &value.read().partition_id,
            builtin_type("int64", GoTypeKind::Int64),
        )?;
        decoder.decode(
            &value.read().partitioned_table_schema_id,
            builtin_type("int64", GoTypeKind::Int64),
        )?;
        decoder.decode(
            &value.read().partitioned_table_id,
            builtin_type("int64", GoTypeKind::Int64),
        )?;
        decoder.decode(
            &value.read().partition_name,
            builtin_type("string", GoTypeKind::String),
        )?;
        decoder.decode(
            &value.read().with_validation,
            builtin_type("bool", GoTypeKind::Bool),
        )?;
        decoder.finish(job);
        Ok(Some(value))
    }
}

/// Go `GetExchangeTablePartitionArgs`.
pub fn get_exchange_table_partition_args(
    job: &mut Job,
) -> Result<Option<GoShared<ExchangeTablePartitionArgs>>, serde_json::Error> {
    get_or_decode_args(job)
}

/// Go `RebaseAutoIDArgs`.
#[derive(Clone, Debug, Default, Serialize)]
pub struct RebaseAutoIDArgs {
    /// Replacement auto-ID base.
    #[serde(rename = "new_base", default, skip_serializing_if = "field_is_default")]
    pub new_base: GoField<i64>,
    /// Whether the requested base is forced.
    #[serde(rename = "force", default, skip_serializing_if = "field_is_default")]
    pub force: GoField<bool>,
}

impl JobArgs for RebaseAutoIDArgs {
    fn into_job_args_value(value: Option<GoShared<Self>>) -> JobArgsValue {
        JobArgsValue::RebaseAutoId(value)
    }

    fn from_job_args_value(value: &JobArgsValue) -> Option<Option<GoShared<Self>>> {
        match value {
            JobArgsValue::RebaseAutoId(value) => Some(value.clone()),
            _ => None,
        }
    }

    fn get_args_v1(value: Option<&GoShared<Self>>, _job: &Job) -> GoSharedSlice<GoAny> {
        let value = value.expect("nil *RebaseAutoIDArgs receiver").read();
        GoSharedSlice::from_vec(vec![
            ColumnDefaultValue::Int(value.new_base.get()).into(),
            ColumnDefaultValue::Bool(value.force.get()).into(),
        ])
    }

    fn decode_v1(job: &mut Job) -> Result<Option<GoShared<Self>>, serde_json::Error> {
        let value = GoShared::new(Self::default());
        let mut decoder = V1Decoder::new(job)?;
        decoder.decode(
            &value.read().new_base,
            builtin_type("int64", GoTypeKind::Int64),
        )?;
        decoder.decode(&value.read().force, builtin_type("bool", GoTypeKind::Bool))?;
        decoder.finish(job);
        Ok(Some(value))
    }
}

/// Go `GetRebaseAutoIDArgs`.
pub fn get_rebase_auto_id_args(
    job: &mut Job,
) -> Result<Option<GoShared<RebaseAutoIDArgs>>, serde_json::Error> {
    get_or_decode_args(job)
}

/// Go `ModifyTableCommentArgs`.
#[derive(Clone, Debug, Default, Serialize)]
pub struct ModifyTableCommentArgs {
    /// Replacement table comment as arbitrary Go-string bytes.
    #[serde(rename = "comment", default, skip_serializing_if = "field_is_default")]
    pub comment: GoField<GoString>,
}

impl JobArgs for ModifyTableCommentArgs {
    fn into_job_args_value(value: Option<GoShared<Self>>) -> JobArgsValue {
        JobArgsValue::ModifyTableComment(value)
    }

    fn from_job_args_value(value: &JobArgsValue) -> Option<Option<GoShared<Self>>> {
        match value {
            JobArgsValue::ModifyTableComment(value) => Some(value.clone()),
            _ => None,
        }
    }

    fn get_args_v1(value: Option<&GoShared<Self>>, _job: &Job) -> GoSharedSlice<GoAny> {
        let value = value.expect("nil *ModifyTableCommentArgs receiver").read();
        GoSharedSlice::from_vec(vec![ColumnDefaultValue::Str(value.comment.get()).into()])
    }

    fn decode_v1(job: &mut Job) -> Result<Option<GoShared<Self>>, serde_json::Error> {
        let value = GoShared::new(Self::default());
        let mut decoder = V1Decoder::new(job)?;
        decoder.decode(
            &value.read().comment,
            builtin_type("string", GoTypeKind::String),
        )?;
        decoder.finish(job);
        Ok(Some(value))
    }
}

/// Go `GetModifyTableCommentArgs`.
pub fn get_modify_table_comment_args(
    job: &mut Job,
) -> Result<Option<GoShared<ModifyTableCommentArgs>>, serde_json::Error> {
    get_or_decode_args(job)
}

/// Go `ModifyTableCharsetAndCollateArgs`.
#[derive(Clone, Debug, Default, Serialize)]
pub struct ModifyTableCharsetAndCollateArgs {
    /// Destination charset as arbitrary Go-string bytes.
    #[serde(
        rename = "to_charset",
        default,
        skip_serializing_if = "field_is_default"
    )]
    pub to_charset: GoField<GoString>,
    /// Destination collation as arbitrary Go-string bytes.
    #[serde(
        rename = "to_collate",
        default,
        skip_serializing_if = "field_is_default"
    )]
    pub to_collate: GoField<GoString>,
    /// Whether existing column metadata is overwritten.
    #[serde(
        rename = "needs_overwrite_cols",
        default,
        skip_serializing_if = "field_is_default"
    )]
    pub needs_overwrite_columns: GoField<bool>,
}

impl JobArgs for ModifyTableCharsetAndCollateArgs {
    fn into_job_args_value(value: Option<GoShared<Self>>) -> JobArgsValue {
        JobArgsValue::ModifyTableCharsetAndCollate(value)
    }

    fn from_job_args_value(value: &JobArgsValue) -> Option<Option<GoShared<Self>>> {
        match value {
            JobArgsValue::ModifyTableCharsetAndCollate(value) => Some(value.clone()),
            _ => None,
        }
    }

    fn get_args_v1(value: Option<&GoShared<Self>>, _job: &Job) -> GoSharedSlice<GoAny> {
        let value = value
            .expect("nil *ModifyTableCharsetAndCollateArgs receiver")
            .read();
        GoSharedSlice::from_vec(vec![
            ColumnDefaultValue::Str(value.to_charset.get()).into(),
            ColumnDefaultValue::Str(value.to_collate.get()).into(),
            ColumnDefaultValue::Bool(value.needs_overwrite_columns.get()).into(),
        ])
    }

    fn decode_v1(job: &mut Job) -> Result<Option<GoShared<Self>>, serde_json::Error> {
        let value = GoShared::new(Self::default());
        let mut decoder = V1Decoder::new(job)?;
        decoder.decode(
            &value.read().to_charset,
            builtin_type("string", GoTypeKind::String),
        )?;
        decoder.decode(
            &value.read().to_collate,
            builtin_type("string", GoTypeKind::String),
        )?;
        decoder.decode(
            &value.read().needs_overwrite_columns,
            builtin_type("bool", GoTypeKind::Bool),
        )?;
        decoder.finish(job);
        Ok(Some(value))
    }
}

/// Go `GetModifyTableCharsetAndCollateArgs`.
pub fn get_modify_table_charset_and_collate_args(
    job: &mut Job,
) -> Result<Option<GoShared<ModifyTableCharsetAndCollateArgs>>, serde_json::Error> {
    get_or_decode_args(job)
}

/// Go `IndexOp` (a byte), used by version-1 index arguments.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct IndexOp(pub u8);

impl IndexOp {
    /// Go `OpAddIndex`.
    pub const ADD_INDEX: Self = Self(0);
    /// Go `OpDropIndex`.
    pub const DROP_INDEX: Self = Self(1);
    /// Go `OpRollbackAddIndex`.
    pub const ROLLBACK_ADD_INDEX: Self = Self(2);
}

/// Go `IndexArg.GetColumnarIndexType`, expressed over the only two fields the
/// source rule reads so that it does not fabricate the still-deferred AST
/// argument surface.
#[must_use]
pub fn index_arg_columnar_index_type(
    columnar_index_type: ColumnarIndexType,
    is_columnar: bool,
) -> ColumnarIndexType {
    if columnar_index_type == ColumnarIndexType::NA && is_columnar {
        ColumnarIndexType::VECTOR
    } else {
        columnar_index_type
    }
}

/// Go `RenameTableArgs`, the data boundary consumed by
/// `GetRenameTablesArgsFromV1`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RenameTableArgs {
    /// Original schema identifier.
    #[serde(rename = "old_schema_id", default, skip_serializing_if = "is_zero_i64")]
    pub old_schema_id: i64,
    /// Original schema name.
    #[serde(rename = "old_schema_name")]
    pub old_schema_name: CiString,
    /// Destination table name.
    #[serde(rename = "new_table_name")]
    pub new_table_name: CiString,
    /// Original table name (used by multi-table rename).
    #[serde(rename = "old_table_name")]
    pub old_table_name: CiString,
    /// Destination schema identifier.
    #[serde(rename = "new_schema_id", default, skip_serializing_if = "is_zero_i64")]
    pub new_schema_id: i64,
    /// Table identifier.
    #[serde(rename = "table_id", default, skip_serializing_if = "is_zero_i64")]
    pub table_id: i64,
    /// Runtime-only Go field (`json:"-"`).
    #[serde(skip)]
    pub old_schema_id_for_schema_diff: i64,
}

const fn is_zero_i64(value: &i64) -> bool {
    *value == 0
}

/// Go `GetRenameTablesArgsFromV1`.
///
/// Length mismatches intentionally panic at the first missing parallel entry,
/// exactly like Go's unchecked slice indexing. Extra entries are ignored
/// because the source iterates only `oldSchemaIDs`.
#[must_use]
pub fn rename_tables_args_from_v1(
    old_schema_ids: &[i64],
    old_schema_names: &[CiString],
    old_table_names: &[CiString],
    new_schema_ids: &[i64],
    new_table_names: &[CiString],
    table_ids: &[i64],
) -> Vec<RenameTableArgs> {
    old_schema_ids
        .iter()
        .enumerate()
        .map(|(index, old_schema_id)| RenameTableArgs {
            old_schema_id: *old_schema_id,
            old_schema_name: old_schema_names[index].clone(),
            old_table_name: old_table_names[index].clone(),
            new_schema_id: new_schema_ids[index],
            new_table_name: new_table_names[index].clone(),
            table_id: table_ids[index],
            old_schema_id_for_schema_diff: 0,
        })
        .collect()
}

#[cfg(test)]
#[path = "job_args_tests.rs"]
pub(crate) mod tests;
