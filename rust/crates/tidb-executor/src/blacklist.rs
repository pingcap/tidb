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

//! `mysql.expr_pushdown_blacklist` and `mysql.opt_rule_blacklist`, and the
//! `ADMIN RELOAD` that publishes each: Go
//! `executor/reload_expr_pushdown_blacklist.go` and
//! `executor/opt_rule_blacklist.go`.
//!
//! Both are the same shape -- a table an operator writes with ordinary DML,
//! and a statement that reads it into the structure the optimizer consults.
//! Nothing is read from the table at plan time, in Go or here: the published
//! copy is what decides, so an `INSERT` alone changes nothing until the
//! `ADMIN RELOAD` that follows it.
//!
//! Go publishes into two process-wide atomics
//! (`expression.DefaultExprPushDownBlacklist`,
//! `plannercore.DefaultDisabledLogicalRulesList`) because its optimizer runs
//! far from any session. This tier holds them on the session, which is
//! the same reach -- one instance -- without a global that parallel tests
//! would share.

pub use tidb_sessionctx::pushdown_blacklists::PushdownBlacklists;
