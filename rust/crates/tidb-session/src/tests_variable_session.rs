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

//! Variable-package tests that need a live `SessionVars`, which stays in
//! this crate until Go's `SessionVars` is ported into `tidb-variable`.

use crate::session_vars::*;
use crate::sysvar::*;
use crate::vars::SessionVars;
use tidb_vardef::{defaults, tidb_vars as names};

#[test]
fn setter_scope_contract_matches_go() {
    let mut vars = crate::vars::SessionVars::new();
    for definition in SYS_VARS {
        if !definition.has_session_scope() {
            let error = vars
                .set_system(definition.name, definition.value.to_owned())
                .expect_err("SESSION must reject a non-session variable");
            assert!(
                matches!(
                    error,
                    crate::vars::VarError::ReadOnlyVariable(_)
                        | crate::vars::VarError::GlobalOnlyVariable(_)
                        | crate::vars::VarError::UnknownSystemVariable(_)
                ),
                "unexpected SESSION error for {}: {error:?}",
                definition.name
            );
        }
        if !definition.has_global_scope() && !definition.has_instance_scope() {
            let error = vars
                .set_global(definition.name, definition.value.to_owned())
                .expect_err("GLOBAL must reject a non-global/non-instance variable");
            assert!(
                matches!(
                    error,
                    crate::vars::VarError::ReadOnlyVariable(_)
                        | crate::vars::VarError::SessionOnlyVariable(_)
                ),
                "unexpected GLOBAL error for {}: {error:?}",
                definition.name
            );
        }
    }
}

/// Go `TestInstanceConfigHasMatchingSysvar`: every serialized `[instance]`
/// option must have a same-named system variable for unified routing.

#[cfg(test)]
mod closure_tests {
    use super::*;

    // Go `TestConcurrencyVariables`, now through the real SET path: setting
    // the variable changes the derived field; unset ones follow the raised
    // executor concurrency.
    #[test]
    fn set_system_var_drives_the_derived_concurrency() {
        let mut vars = SessionVars::new();

        // Untouched session derives exactly NewSessionVars' defaults.
        assert_eq!(Concurrency::from_vars(&vars), Concurrency::default());
        assert_eq!(MemQuota::from_vars(&vars), MemQuota::default());
        assert_eq!(BatchSize::from_vars(&vars), BatchSize::default());

        vars.set_system(names::TIDB_WINDOW_CONCURRENCY, "2".to_owned())
            .unwrap();
        vars.set_system(names::TIDB_MERGE_JOIN_CONCURRENCY, "2".to_owned())
            .unwrap();
        vars.set_system(names::TIDB_STREAM_AGG_CONCURRENCY, "2".to_owned())
            .unwrap();
        let concurrency = Concurrency::from_vars(&vars);
        assert_eq!(concurrency.window_concurrency(), 2);
        assert_eq!(concurrency.merge_join_concurrency(), 2);
        assert_eq!(concurrency.stream_agg_concurrency(), 2);

        // Raising the executor concurrency moves the still-unset getters.
        let raised = defaults::DEF_EXECUTOR_CONCURRENCY + 1;
        vars.set_system(names::TIDB_EXECUTOR_CONCURRENCY, raised.to_string())
            .unwrap();
        let concurrency = Concurrency::from_vars(&vars);
        assert_eq!(concurrency.executor_concurrency, raised);
        assert_eq!(concurrency.index_lookup_concurrency(), raised);
        assert_eq!(concurrency.index_lookup_join_concurrency(), raised);
        assert_eq!(concurrency.hash_join_concurrency(), raised);
        assert_eq!(concurrency.window_concurrency(), 2);
        assert_eq!(concurrency.merge_join_concurrency(), 2);
    }

    // The mem-quota and batch-size closures parse the stored text with their
    // Go defaults as the fallback.
    #[test]
    fn set_system_var_drives_quotas_and_batch_sizes() {
        let mut vars = SessionVars::new();
        vars.set_system(names::TIDB_MEM_QUOTA_QUERY, "2097152".to_owned())
            .unwrap();
        vars.set_system(names::TIDB_INDEX_JOIN_BATCH_SIZE, "123".to_owned())
            .unwrap();
        vars.set_system(names::TIDB_MAX_CHUNK_SIZE, "2048".to_owned())
            .unwrap();

        assert_eq!(MemQuota::from_vars(&vars).mem_quota_query, 2_097_152);
        let batch = BatchSize::from_vars(&vars);
        assert_eq!(batch.index_join_batch_size, 123);
        assert_eq!(batch.max_chunk_size, 2048);
        // Untouched fields keep their defaults.
        assert_eq!(batch.index_lookup_size, defaults::DEF_INDEX_LOOKUP_SIZE);
    }
}

