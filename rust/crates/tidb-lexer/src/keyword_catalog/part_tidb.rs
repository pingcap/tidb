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

//! `tidb` section of the TiDB SQL keyword catalog (see `keyword_catalog/mod.rs`).

use super::Keyword;

pub(super) static KEYWORDS_TIDB: &[Keyword] = &[
    Keyword {
        word: "ADMIN",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "BATCH",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "BUCKETS",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "BUILTINS",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "CANCEL",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "CARDINALITY",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "CMSKETCH",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "COLUMN_STATS_USAGE",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "CORRELATION",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "DDL",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "DEPENDENCY",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "DEPTH",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "DISTRIBUTE",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "DISTRIBUTION",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "DISTRIBUTIONS",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "DRY",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "HISTOGRAMS_IN_FLIGHT",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "JOB",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "JOBS",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "LITE",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "NDVRATE",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "NODE_ID",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "NODE_STATE",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "OPTIMISTIC",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "PESSIMISTIC",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "POLICIES",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "RAW",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "REGION",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "REGIONS",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "RESET",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "RUN",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "SAMPLERATE",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "SAMPLES",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "SESSION_STATES",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "SPLIT",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "STATISTICS",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "STATS",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "STATS_BUCKETS",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "STATS_DELTA",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "STATS_EXTENDED",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "STATS_HEALTHY",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "STATS_HISTOGRAMS",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "STATS_LOCKED",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "STATS_META",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "STATS_TOPN",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "TIDB",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "TIFLASH",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "TOPN",
        reserved: false,
        section: "tidb",
    },
    Keyword {
        word: "WIDTH",
        reserved: false,
        section: "tidb",
    },
];
