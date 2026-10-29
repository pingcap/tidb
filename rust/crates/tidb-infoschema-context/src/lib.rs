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

//! Go `pkg/infoschema/context`: the infoschema interfaces packages below
//! `infoschema` read schema metadata through.

/// The slice of Go `infoschemactx.MetaOnlyInfoSchema` that metabuild stores.
///
/// boundary: Go `pkg/infoschema/context.MetaOnlyInfoSchema` — metabuild only
/// keeps the value for later constraint checks, so one identifying method
/// stands in for the full meta-only interface.
pub trait MetaOnlyInfoSchema {
    /// Go `MetaOnlyInfoSchema.SchemaMetaVersion`.
    fn schema_meta_version(&self) -> i64;
}
