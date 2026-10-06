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

//! Shared support for the result differential suites.
//! Helper tests run once here; integration binaries retain process isolation.

pub mod enrolled_topics;
pub mod integration_plan_property;
pub mod mysqltest_connections;
pub mod mysqltest_script;
pub mod result_label;
