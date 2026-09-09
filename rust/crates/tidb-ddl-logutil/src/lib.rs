// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Complete transcreation of pinned Go `pkg/ddl/logutil`.

use std::sync::LazyLock;
use std::time::Duration;

use tidb_log::{Field, Value};
use tidb_util::logutil::{
    bg_logger, sample_logger_factory, Logger, SampledLogger, LOG_FIELD_CATEGORY,
};

fn category_field(category: &str) -> Field {
    Field::new(LOG_FIELD_CATEGORY, Value::Str(category.to_owned()))
}

static SAMPLE_LOGGER: LazyLock<SampledLogger> = LazyLock::new(|| {
    sample_logger_factory(Duration::from_secs(60), 3, vec![category_field("ddl")])()
});

/// Go `DDLLogger`.
pub fn ddl_logger() -> Logger {
    bg_logger().with_fields(&[category_field("ddl")])
}

/// Go `DDLUpgradingLogger`.
pub fn ddl_upgrading_logger() -> Logger {
    bg_logger().with_fields(&[category_field("ddl-upgrading")])
}

/// Go `DDLIngestLogger`.
pub fn ddl_ingest_logger() -> Logger {
    bg_logger().with_fields(&[category_field("ddl-ingest")])
}

/// Go `SampleLogger`: one process-shared logger that admits the first three
/// entries per level/message bucket in each one-minute window.
pub fn sample_logger() -> SampledLogger {
    SAMPLE_LOGGER.clone()
}
