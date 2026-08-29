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

//! Go `pkg/statistics/handle/logutil`.

use std::sync::LazyLock;
use std::time::Duration;

use tidb_log::{Field, Value};
use tidb_util::logutil::{
    bg_logger, err_verbose_logger, sample_err_verbose_logger_factory, sample_logger_factory,
    Logger, SampledLogger, LOG_FIELD_CATEGORY,
};

fn category_field() -> Field {
    Field::new(LOG_FIELD_CATEGORY, Value::Str("stats".to_owned()))
}

static SAMPLE_LOGGER: LazyLock<SampledLogger> = LazyLock::new(|| {
    sample_logger_factory(Duration::from_secs(5 * 60), 1, vec![category_field()])()
});

static SAMPLE_ERR_VERBOSE_LOGGER: LazyLock<SampledLogger> = LazyLock::new(|| {
    sample_err_verbose_logger_factory(Duration::from_secs(10 * 60), 1, vec![category_field()])()
});

/// Go `StatsLogger`.
pub fn stats_logger() -> Logger {
    bg_logger().with_fields(&[category_field()])
}

/// Go `StatsErrVerboseLogger`.
pub fn stats_err_verbose_logger() -> Logger {
    err_verbose_logger().with_fields(&[category_field()])
}

/// Go `StatsSampleLogger`.
pub fn stats_sample_logger() -> SampledLogger {
    SAMPLE_LOGGER.clone()
}

/// Go `StatsErrVerboseSampleLogger`.
pub fn stats_err_verbose_sample_logger() -> SampledLogger {
    SAMPLE_ERR_VERBOSE_LOGGER.clone()
}
