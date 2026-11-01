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

//! The integration-corpus replay engine (`tidb-server`'s
//! `integration_corpus_matches_recordings` runs it over the production
//! stack) and shared support for the result differential suites.

pub mod enrolled_topics;
pub mod mysql_tester;
pub mod mysqltest_script;
pub mod plan_statement;
pub mod replay;
pub mod result_label;
