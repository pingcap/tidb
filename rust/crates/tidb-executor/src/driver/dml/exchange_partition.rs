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

//! The writes EXCHANGE PARTITION restricts during its write-only state
//! (`TableInfo.ExchangePartitionInfo`): a row written into the standalone
//! table must belong in the exchanged partition and pass the partitioned
//! table's constraints (Go `checkRowForExchangePartition`), and a row
//! written into the exchanged partition must pass the standalone table's
//! constraints (Go `checkConstraintForExchangePartition`).

use std::sync::Arc;

use tidb_datatype::Datum;

use crate::kv_table::KvTable;
use crate::{Catalog, DriverError, TableEntry};

/// The other table of an ongoing EXCHANGE PARTITION, resolved from the
/// session's catalog before the written table is borrowed for the write.
pub(crate) enum ExchangePartitionCheck {
    /// The written table is the standalone one.
    Standalone {
        partitioned: Arc<KvTable>,
        partition_id: i64,
        standalone_id: i64,
    },
    /// The written table is the partitioned one.
    Partitioned {
        standalone: Arc<KvTable>,
        partition_id: i64,
    },
}

/// The check `database`.`name`'s writes need, `None` outside an exchange.
///
/// # Errors
///
/// Go's errors for a standalone table whose partitioned table cannot be
/// found.
pub(crate) fn exchange_partition_check(
    catalog: &Catalog,
    database: &str,
    name: &str,
) -> Result<Option<ExchangePartitionCheck>, DriverError> {
    let Some(TableEntry::Kv(table)) = catalog.table_in(database, name) else {
        return Ok(None);
    };
    let Some((other_id, partition_id)) = table.exchange_partition_info() else {
        return Ok(None);
    };
    let other = catalog
        .table_names_by_id(other_id)
        .and_then(|(schema, other)| match catalog.table_in(&schema, &other) {
            Some(TableEntry::Kv(other)) => Some(Arc::clone(other)),
            _ => None,
        });
    if table.partition().is_none() {
        let partitioned = other.ok_or_else(|| {
            DriverError::unsupported("exchange partition process table by id failed".to_owned())
        })?;
        if partitioned.partition().is_none() {
            return Err(DriverError::unsupported(
                "exchange partition process assert table partition failed".to_owned(),
            ));
        }
        return Ok(Some(ExchangePartitionCheck::Standalone {
            partitioned,
            partition_id,
            standalone_id: table.table_id,
        }));
    }
    Ok(other.map(|standalone| ExchangePartitionCheck::Partitioned {
        standalone,
        partition_id,
    }))
}

impl ExchangePartitionCheck {
    /// Go `checkRowForExchangePartition` for the standalone table's row,
    /// before its generated columns are filled.
    pub(crate) fn check_standalone_row(
        &self,
        row: &[Datum],
        ctx: &crate::StmtContext,
    ) -> Result<(), DriverError> {
        let Self::Standalone {
            partitioned,
            partition_id,
            standalone_id,
        } = self
        else {
            return Ok(());
        };
        // Go `CheckForExchangePartition`.
        let located = partitioned
            .locate_physical_id(row, ctx)
            .map_err(super::kv_write_error)?;
        if located != *partition_id && located != *standalone_id {
            return Err(DriverError::from(
                tidb_error::catalog::table::ERR_ROW_DOES_NOT_MATCH_GIVEN_PARTITION_SET
                    .generate_with_stack_by_args(&[]),
            ));
        }
        if ctx.enable_check_constraint() {
            partitioned
                .validate_check_constraints(row, ctx)
                .map_err(super::kv_write_error)?;
        }
        Ok(())
    }

    /// Go `checkConstraintForExchangePartition` for a row the partitioned
    /// table `table` writes into the exchanged partition.
    pub(crate) fn check_partitioned_row(
        &self,
        table: &KvTable,
        row: &[Datum],
        ctx: &crate::StmtContext,
    ) -> Result<(), DriverError> {
        let Self::Partitioned {
            standalone,
            partition_id,
        } = self
        else {
            return Ok(());
        };
        if !ctx.enable_check_constraint() {
            return Ok(());
        }
        let located = table
            .locate_physical_id(row, ctx)
            .map_err(super::kv_write_error)?;
        if located != *partition_id {
            return Ok(());
        }
        standalone
            .validate_check_constraints(row, ctx)
            .map_err(super::kv_write_error)
    }
}
