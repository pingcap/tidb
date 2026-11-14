// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `pkg/dxf/importinto/taskkey`.
/// Go `ForJob`.
pub fn for_job(job_id: i64) -> String {
    if tidb_config_kerneltype::is_next_gen() {
        return for_job_in_keyspace(&tidb_keyspace::get_keyspace_name_by_settings(), job_id);
    }
    format!("{}/{job_id}", tidb_dxf::IMPORT_INTO)
}
/// Go `ForJobInKeyspace` and `forJobInKeyspace`.
pub fn for_job_in_keyspace(keyspace: &str, job_id: i64) -> String {
    if tidb_config_kerneltype::is_next_gen() {
        return format!("{keyspace}/{}/{job_id}", tidb_dxf::IMPORT_INTO);
    }
    for_job(job_id)
}

#[cfg(test)]
mod tests {
    /// Go capture `zzprobe/o15_taskkey.TestTaskKey`, classic and nextgen.
    #[test]
    fn test_task_key() {
        use tidb_config::config_tree::config::{
            get_global_config, store_global_config, update_global,
        };
        struct Restore(std::sync::Arc<tidb_config::config_tree::config::Config>);
        impl Drop for Restore {
            /// Go `TestTaskKey` deferred configuration restoration.
            fn drop(&mut self) {
                store_global_config(self.0.clone());
            }
        }
        let _restore = Restore(get_global_config());
        update_global(|config| config.keyspace_name = "tenant".into());
        for id in [-1, 0, 9527] {
            let classic = format!("ImportInto/{id}");
            if tidb_config_kerneltype::is_next_gen() {
                assert_eq!(super::for_job(id), format!("tenant/{classic}"));
                assert_eq!(
                    super::for_job_in_keyspace("other", id),
                    format!("other/{classic}")
                );
                assert_eq!(super::for_job_in_keyspace("", id), format!("/{classic}"));
            } else {
                assert_eq!(super::for_job(id), classic);
                assert_eq!(super::for_job_in_keyspace("other", id), classic);
                assert_eq!(super::for_job_in_keyspace("", id), classic);
            }
        }
    }
}
