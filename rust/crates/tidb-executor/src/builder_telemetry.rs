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

//! Go `executorBuilder`'s statement-level telemetry (`builder.go`):
//! `setTelemetryInfo`, which `buildDDL` runs first, and `build`'s
//! CREATE/ALTER USER arm. The plan-level writes (CTE, table lookups, index
//! merge, compact partitions) are in the builders that build those plans.

use tidb_ast::{AlterPartitionAction, AlterTableAction, CreateUserPasswordOption, DdlStmt, PartitionType, Stmt};
use tidb_sessionctx::telemetry_info::TelemetryInfo;

/// Go `setTelemetryInfo` for a DDL and the account-lock count for
/// CREATE/ALTER USER: what building `stmt`'s executor records.
pub fn set_telemetry_info(ti: &mut TelemetryInfo, stmt: &Stmt) {
    match stmt {
        Stmt::Ddl(ddl) => set_ddl_telemetry_info(ti, ddl),
        Stmt::Admin(admin) => {
            let lock_options = match &**admin {
                tidb_ast::AdminStmt::CreateUser { password_options, .. } => password_options,
                tidb_ast::AdminStmt::AlterUser(alter) => &alter.password_options,
                _ => return,
            };
            let telemetry = ti.account_lock_telemetry();
            telemetry.create_or_alter_user += 1;
            // Multiple lock options are supported for the parser, but only
            // the last one option takes effect.
            for option in lock_options.iter().rev() {
                match option {
                    CreateUserPasswordOption::AccountLock => {
                        telemetry.lock_user += 1;
                        break;
                    }
                    CreateUserPasswordOption::AccountUnlock => {
                        telemetry.unlock_user += 1;
                        break;
                    }
                    _ => {}
                }
            }
        }
        _ => {}
    }
}

/// Go `executorBuilder.setTelemetryInfo`.
fn set_ddl_telemetry_info(ti: &mut TelemetryInfo, ddl: &DdlStmt) {
    match ddl {
        DdlStmt::AlterTable(s) => {
            if s.actions.len() > 1 {
                ti.use_multi_schema_change = true;
            }
            for spec in &s.actions {
                let AlterTableAction::Partition(action) = spec else {
                    continue;
                };
                match action {
                    AlterPartitionAction::FirstPartitionLessThan { .. } => {
                        ti.partition_telemetry().use_drop_interval_partition = true;
                    }
                    AlterPartitionAction::LastPartitionLessThan { .. } => {
                        ti.partition_telemetry().use_add_interval_partition = true;
                    }
                    AlterPartitionAction::Exchange { .. } => ti.use_exchange_partition = true,
                    AlterPartitionAction::Reorganize { .. } => {
                        ti.partition_telemetry().use_reorganize_partition = true;
                    }
                    _ => {}
                }
            }
        }
        DdlStmt::CreateTable(s) => {
            let Some(p) = &s.partitioning else {
                return;
            };
            let telemetry = ti.partition_telemetry();
            telemetry.table_partition_max_partitions_num = p.method.count.max(p.definitions.len() as u64);
            telemetry.use_table_partition = true;
            let columns = p.method.columns.len();
            match p.method.kind {
                PartitionType::RANGE => {
                    if p.subpartition.is_none() {
                        if columns > 0 {
                            telemetry.use_table_partition_range_columns = true;
                            if columns > 1 {
                                telemetry.use_table_partition_range_columns_gt1 = true;
                            }
                            if columns > 2 {
                                telemetry.use_table_partition_range_columns_gt2 = true;
                            }
                            if columns > 3 {
                                telemetry.use_table_partition_range_columns_gt3 = true;
                            }
                        } else {
                            telemetry.use_table_partition_range = true;
                        }
                        if p.method.interval.is_some() {
                            telemetry.use_create_interval_partition = true;
                        }
                    }
                }
                PartitionType::HASH => {
                    if p.subpartition.is_none() {
                        telemetry.use_table_partition_hash = true;
                    }
                }
                PartitionType::LIST => {
                    if p.subpartition.is_none() {
                        if columns > 0 {
                            telemetry.use_table_partition_list_columns = true;
                        } else {
                            telemetry.use_table_partition_list = true;
                        }
                    }
                }
                _ => {}
            }
        }
        DdlStmt::FlashbackToTimestamp(_) => ti.use_flashback_to_cluster = true,
        _ => {}
    }
}
