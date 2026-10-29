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

//! Error prototypes defined by the Go package(s) named per entry.
//!
//! GENERATED mechanically from the Go sources by scanning every
//! package-level `Class*.NewStd` / `NewStdErr` definition; the (class,
//! code) set of every module is cross-checked against the real Go
//! registry (`terror.ErrClassToMySQLCodes`) by
//! `tests/catalog_source.rs`. Do not hand-edit entries.

use std::sync::LazyLock;

use super::{plain_err, std_err, Class};
use tidb_terror::TerrorError;
use tidb_errno::errcode;

/// Source `util/workloadrepo.errWrongValueForVar` (`pkg/util/workloadrepo/const.go:46`).
pub static ERR_WRONG_VALUE_FOR_VAR: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Util, errcode::ErrWrongValueForVar));
/// Source `util/workloadrepo.errUnsupportedEtcdRequired` (`pkg/util/workloadrepo/const.go:47`).
pub static ERR_UNSUPPORTED_ETCD_REQUIRED: LazyLock<TerrorError> = LazyLock::new(|| {
    plain_err(
        Class::Util,
        errcode::ErrNotSupportedYet,
        "etcd client required for workload repository",
    )
});
/// Source `util/workloadrepo.errWorkloadNotStarted` (`pkg/util/workloadrepo/const.go:48`).
pub static ERR_WORKLOAD_NOT_STARTED: LazyLock<TerrorError> = LazyLock::new(|| {
    plain_err(
        Class::Util,
        errcode::ErrNotSupportedYet,
        "Workload repository is not enabled",
    )
});
/// Source `util/workloadrepo.errCouldNotStartSnapshot` (`pkg/util/workloadrepo/const.go:49`).
pub static ERR_COULD_NOT_START_SNAPSHOT: LazyLock<TerrorError> = LazyLock::new(|| {
    plain_err(
        Class::Util,
        errcode::ErrUnknown,
        "Snapshot initiation failed",
    )
});
