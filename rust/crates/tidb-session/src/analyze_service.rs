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

//! `ANALYZE TABLE`'s executor lives in `tidb_executor::analyze::exec`
//! (Go `pkg/executor/analyze.go`); this is the session as it sees it.


impl tidb_executor::analyze::exec::AnalyzeExec for crate::Session {
    fn approximate_table_counts(
        &self,
    ) -> std::sync::Arc<dyn tidb_pdhelper::ApproximateTableCountProvider> {
        std::sync::Arc::clone(&self.approximate_table_counts)
    }

    fn drain_context_warnings(&mut self, ctx: &tidb_sessionctx::StmtContext) {
        crate::Session::drain_context_warnings(self, ctx);
    }

    fn publish_table_delta(&self) {
        crate::Session::publish_table_delta(self);
    }

    fn begin_routed_statement_warnings(&mut self) {
        crate::Session::begin_routed_statement_warnings(self);
    }

    fn append_routed_warning(&mut self, code: u16, message: String) {
        crate::Session::append_routed_warning(self, code, message);
    }

    fn append_routed_note(&mut self, code: u16, message: String) {
        crate::Session::append_routed_note(self, code, message);
    }

    fn catalog_ignoring_poison(&self) -> std::sync::MutexGuard<'_, tidb_executor::Catalog> {
        self.catalog
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}
