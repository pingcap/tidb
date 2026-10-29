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

//! Go's store bootstrap (`pkg/session/bootstrap.go`) runs on the node
//! (`crate::mysql_bootstrap`): a store is bootstrapped once, before any
//! session opens on it, and every session consumes the bootstrapped
//! infoschema as Go's `createSessionWithOpt` does.

#[cfg(test)]
mod tests {
    use crate::Session;

    /// A classic-kernel store has Go's 58 bootstrap jobs in its DDL history
    /// (jobs 3 .. 116) and its next user table is 117 with job 118.
    #[test]
    fn a_classic_store_has_the_bootstrap_jobs_in_its_history() {
        if tidb_config_kerneltype::is_next_gen() {
            return;
        }
        let mut session = Session::new();
        let text = crate::tests_support::row_text;
        let jobs = text(session.run("ADMIN SHOW DDL JOBS 100"));
        assert_eq!(jobs.len(), 58);
        assert_eq!(
            (jobs[0][0].as_str(), jobs[0][3].as_str()),
            ("116", "alter resource group")
        );
        assert_eq!((jobs[57][0].as_str(), jobs[57][1].as_str()), ("3", "sys"));
        session.run("CREATE TABLE test.layout_t (a INT)").unwrap();
        let newest = text(session.run("ADMIN SHOW DDL JOBS 1"));
        assert_eq!(newest[0][0], "118");
        assert_eq!(newest[0][6], "117");
    }
}
