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

//! Go `register.go`.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, RwLock};

use tidb_dxf::{Task, TaskType};
use tidb_gocontext::Context;

use crate::{Param, TaskExecutor};

/// Go `FactoryFn`: creates the task type's executor.
pub type FactoryFn = Arc<dyn Fn(&Context, Task, Param) -> Arc<dyn TaskExecutor> + Send + Sync>;

/// Go `factoryFns`.
pub(crate) static FACTORY_FNS: LazyLock<RwLock<HashMap<TaskType, FactoryFn>>> = LazyLock::new(Default::default);

/// Go `RegisterTaskType`.
pub fn register_task_type(task_type: TaskType, factory: FactoryFn) {
    FACTORY_FNS.write().unwrap_or_else(std::sync::PoisonError::into_inner).insert(task_type, factory);
}

/// Go `GetTaskExecutorFactory`.
#[must_use]
pub fn get_task_executor_factory(task_type: &TaskType) -> Option<FactoryFn> {
    FACTORY_FNS.read().unwrap_or_else(std::sync::PoisonError::into_inner).get(task_type).cloned()
}

/// Go `ClearTaskExecutors`: only used in test.
pub fn clear_task_executors() {
    FACTORY_FNS.write().unwrap_or_else(std::sync::PoisonError::into_inner).clear();
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Go `TestRegisterTaskType`.
    #[test]
    fn test_register_task_type() {
        // other case might add task types, so we need to clear it first
        clear_task_executors();
        let factory_fn: FactoryFn = Arc::new(|_, _, _| unreachable!("the test creates no executor"));
        let len = || FACTORY_FNS.read().unwrap().len();
        register_task_type(TaskType::new("test1"), Arc::clone(&factory_fn));
        assert_eq!(len(), 1);
        register_task_type(TaskType::new("test2"), Arc::clone(&factory_fn));
        assert_eq!(len(), 2);
        // register again
        register_task_type(TaskType::new("test2"), factory_fn);
        assert_eq!(len(), 2);
    }
}
