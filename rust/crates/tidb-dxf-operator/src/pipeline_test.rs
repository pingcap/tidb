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

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use regex::Regex;

use crate::wrapper::{SimpleOperator, SimpleSink};
use crate::{compose, strCnt, stringTask, AsyncPipeline, Context, Operator, OperatorError};

#[deny(unused_must_use)]
#[test]
fn go_constructor_return_values_can_be_ignored() {
    let context = Arc::new(Context::new());
    AsyncPipeline::new(Vec::new());
    crate::SimpleDataSource::new(Arc::clone(&context), Vec::<stringTask>::new());
    SimpleSink::new(Arc::clone(&context), |_value: stringTask| {});
    SimpleOperator::new(
        Arc::clone(&context),
        |_value: stringTask| stringTask(String::new()),
        1,
    );
}

#[deny(unused_must_use)]
#[test]
fn go_pipeline_query_returns_can_be_ignored() {
    let pipeline = AsyncPipeline::new(Vec::new());
    pipeline.is_started();
    pipeline.pipeline_string();
    pipeline.reader_and_writer();
}

#[test]
fn pipeline_async_multi_operators_without_error() {
    let words = "Bob hiT a ball, the hIt BALL flew far after it was hit.";
    let tasks = words
        .split(' ')
        .map(|word| stringTask(word.to_owned()))
        .collect::<Vec<_>>();

    for mock_error in [false, true] {
        let context = Arc::new(Context::new());
        let most_common_word = Arc::new(Mutex::new(stringTask(String::new())));
        let source = Arc::new(crate::SimpleDataSource::new(
            Arc::clone(&context),
            tasks.clone(),
        ));
        let lower = Arc::new(SimpleOperator::new(
            Arc::clone(&context),
            |task: stringTask| stringTask(task.0.to_lowercase()),
            3,
        ));
        let non_alphanumeric = Regex::new("[^a-zA-Z0-9]+").unwrap();
        let trimmer = Arc::new(SimpleOperator::new(
            Arc::clone(&context),
            move |task: stringTask| {
                stringTask(non_alphanumeric.replace_all(&task.0, "").into_owned())
            },
            3,
        ));
        let counts = Arc::new(Mutex::new(HashMap::<stringTask, usize>::new()));
        let counter_context = Arc::clone(&context);
        let counter = Arc::new(SimpleOperator::new(
            Arc::clone(&context),
            move |task: stringTask| {
                let mut counts = counts
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let count = counts.entry(task.clone()).or_default();
                *count += 1;
                let result = strCnt {
                    string: task,
                    count: *count,
                };
                drop(counts);
                if mock_error {
                    counter_context.on_error(OperatorError("mock error for testing".to_owned()));
                }
                result
            },
            3,
        ));
        let maximum = Arc::new(Mutex::new(0usize));
        let collected_word = Arc::clone(&most_common_word);
        let collector = Arc::new(SimpleSink::new(
            Arc::clone(&context),
            move |value: strCnt| {
                let mut maximum = maximum
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if value.count > *maximum {
                    *maximum = value.count;
                    *collected_word
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner) = value.string;
                }
            },
        ));

        compose(source.as_ref(), lower.as_ref());
        compose(lower.as_ref(), trimmer.as_ref());
        compose(trimmer.as_ref(), counter.as_ref());
        compose(counter.as_ref(), collector.as_ref());

        let operators: Vec<Arc<dyn Operator>> = vec![
            source.clone(),
            lower.clone(),
            trimmer.clone(),
            counter.clone(),
            collector.clone(),
        ];
        let pipeline = AsyncPipeline::new(operators);
        assert_eq!(
            pipeline.pipeline_string(),
            "AsyncPipeline[SimpleDataSource[operator.stringTask] -> simpleOperator(AsyncOp[operator.stringTask, operator.stringTask]) -> simpleOperator(AsyncOp[operator.stringTask, operator.stringTask]) -> simpleOperator(AsyncOp[operator.stringTask, operator.strCnt]) -> simpleSink]"
        );
        pipeline.execute().unwrap();
        let result = pipeline.close();
        if mock_error {
            assert!(result.is_err());
        } else {
            result.unwrap();
            assert_eq!(
                *most_common_word
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner),
                stringTask("hit".to_owned())
            );
        }
    }
}
