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

//! Complete native transcreation of Go `pkg/dxf/operator`.

mod compose;
mod operator;
mod pipeline;
mod wrapper;

pub use compose::{compose, SimpleDataChannel, WithSink, WithSource};
pub use operator::{
    AsyncOperator, Context, NoResult, Operator, OperatorError, TaskMayPanic, TunableOperator,
    Worker,
};
pub use pipeline::AsyncPipeline;
pub use wrapper::SimpleDataSource;

#[cfg(test)]
#[allow(non_camel_case_types)]
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct stringTask(String);

#[cfg(test)]
impl TaskMayPanic<OperatorError> for stringTask {
    fn recover_args(&self) -> (String, String, Option<OperatorError>) {
        (String::new(), String::new(), None)
    }
}

#[cfg(test)]
#[allow(non_camel_case_types)]
struct strCnt {
    string: stringTask,
    count: usize,
}

#[cfg(test)]
mod pipeline_test;
