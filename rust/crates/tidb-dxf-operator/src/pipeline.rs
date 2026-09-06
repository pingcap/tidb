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

use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::{Operator, OperatorError, TunableOperator};

/// Go `AsyncPipeline`.
pub struct AsyncPipeline {
    operators: Vec<Arc<dyn Operator>>,
    started: AtomicBool,
}

impl fmt::Display for AsyncPipeline {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.pipeline_string())
    }
}

impl AsyncPipeline {
    /// Go `NewAsyncPipeline`.
    pub fn new(operators: Vec<Arc<dyn Operator>>) -> Self {
        Self {
            operators,
            started: AtomicBool::new(false),
        }
    }

    /// Go `Execute`.
    pub fn execute(&self) -> Result<(), OperatorError> {
        for (index, operator) in self.operators.iter().enumerate() {
            if let Err(error) = operator.open() {
                for opened in self.operators[..index].iter().rev() {
                    let _ = opened.close();
                }
                return Err(error);
            }
        }
        self.started.store(true, Ordering::Release);
        Ok(())
    }

    /// Go `IsStarted`.
    pub fn is_started(&self) -> bool {
        self.started.load(Ordering::Acquire)
    }

    /// Go `Close`.
    pub fn close(&self) -> Result<(), OperatorError> {
        let mut first_error = None;
        for operator in &self.operators {
            if let Err(error) = operator.close() {
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }
        self.started.store(false, Ordering::Release);
        first_error.map_or(Ok(()), Err)
    }

    /// Go `String`.
    pub fn pipeline_string(&self) -> String {
        format!(
            "AsyncPipeline[{}]",
            self.operators
                .iter()
                .map(|operator| operator.operator_string())
                .collect::<Vec<_>>()
                .join(" -> ")
        )
    }

    /// Go `GetReaderAndWriter`.
    pub fn reader_and_writer(
        &self,
    ) -> (Option<&dyn TunableOperator>, Option<&dyn TunableOperator>) {
        if self.operators.len() != 4 {
            return (None, None);
        }
        (self.operators[1].tunable(), self.operators[2].tunable())
    }
}
