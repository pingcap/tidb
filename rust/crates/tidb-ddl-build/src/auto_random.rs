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

use tidb_table::kv_table::{AutoIdError, AutoRandomError, AutoRandomSpec};
use tidb_error::mysql::MysqlError;

use crate::errors;
use tidb_datatype::{FieldType, FieldTypeCode};

/// Validates Go's `setTableAutoRandomBits` contract and returns the persisted
/// layout. `handle_offsets` is empty when the table has no clustered primary
/// key and otherwise names the clustered key columns in key order.
pub fn validate(
    create: &tidb_ast::CreateTableStmt,
    fields: &[FieldType],
    handle_offsets: &[usize],
) -> Result<Option<AutoRandomSpec>, MysqlError> {
    let mut found = None;
    for (offset, definition) in create.columns.iter().enumerate() {
        let Some(option) = definition.options.iter().find_map(|option| match option {
            tidb_ast::ColumnOption::AutoRandom(option) => Some(option),
            _ => None,
        }) else {
            continue;
        };
        if fields[offset].code() != FieldTypeCode::LongLong {
            return Err(tidb_error::mysql::MysqlError::from_terror(
                &tidb_util::dbterror::ERR_INVALID_AUTO_RANDOM.generate_with_stack_by_args(&[
                    (format!(
                "auto_random option must be defined on `bigint` column, but not on `{}` column",
                fields[offset].compact_str(true)
            ))
                    .to_string()
                    .into(),
                ]),
            ));
        }
        if handle_offsets.first().copied() != Some(offset) {
            return Err(tidb_error::mysql::MysqlError::from_terror(
                &tidb_util::dbterror::ERR_INVALID_AUTO_RANDOM.generate_with_stack_by_args(&[
                    (if handle_offsets.is_empty() {
                        "auto_random is only supported on the tables with clustered primary key"
                            .to_owned()
                    } else {
                        format!(
                            "column '{}' must be the first column in primary key",
                            definition.name
                        )
                    })
                    .to_string()
                    .into(),
                ]),
            ));
        }
        if definition
            .options
            .iter()
            .any(|option| matches!(option, tidb_ast::ColumnOption::AutoIncrement))
        {
            return Err(tidb_error::mysql::MysqlError::from_terror(
                &tidb_util::dbterror::ERR_INVALID_AUTO_RANDOM.generate_with_stack_by_args(&[
                    ("auto_random is incompatible with auto_increment".to_owned())
                        .to_string()
                        .into(),
                ]),
            ));
        }
        if definition
            .options
            .iter()
            .any(|option| matches!(option, tidb_ast::ColumnOption::Default(_)))
        {
            return Err(tidb_error::mysql::MysqlError::from_terror(
                &tidb_util::dbterror::ERR_INVALID_AUTO_RANDOM.generate_with_stack_by_args(&[
                    ("auto_random is incompatible with default".to_owned())
                        .to_string()
                        .into(),
                ]),
            ));
        }

        let shard_bits = option.shard_bits.unwrap_or(5);
        if shard_bits == 0 {
            return Err(tidb_error::mysql::MysqlError::from_terror(
                &tidb_util::dbterror::ERR_INVALID_AUTO_RANDOM.generate_with_stack_by_args(&[
                    ("the value of auto_random should be positive".to_owned())
                        .to_string()
                        .into(),
                ]),
            ));
        }
        if shard_bits > 15 {
            return Err(tidb_error::mysql::MysqlError::from_terror(
                &tidb_util::dbterror::ERR_INVALID_AUTO_RANDOM.generate_with_stack_by_args(&[
                    (format!(
                "max allowed auto_random shard bits is 15, but got {shard_bits} on column `{}`",
                definition.name
            ))
                    .to_string()
                    .into(),
                ]),
            ));
        }
        let range_bits = option.range_bits.unwrap_or(64);
        if !(32..=64).contains(&range_bits) {
            return Err(tidb_error::mysql::MysqlError::from_terror(
                &tidb_util::dbterror::ERR_INVALID_AUTO_RANDOM.generate_with_stack_by_args(&[
                    (format!(
                        "auto_random range bits must be between 32 and 64, but got {range_bits}"
                    ))
                    .to_string()
                    .into(),
                ]),
            ));
        }
        let unsigned = fields[offset].is_unsigned();
        if range_bits - shard_bits - u64::from(!unsigned) < 27 {
            return Err(tidb_error::mysql::MysqlError::from_terror(&tidb_util::dbterror::ERR_INVALID_AUTO_RANDOM.generate_with_stack_by_args(&[("auto_random ID space is too small, please decrease the shard bits or increase the range bits"
                    .to_owned()).to_string().into()])));
        }
        found = Some(AutoRandomSpec {
            offset,
            shard_bits,
            range_bits,
            unsigned,
        });
    }
    Ok(found)
}

/// Maps the allocator-layer AUTO_RANDOM failure to TiDB's client error.
pub fn rebase_error(error: AutoRandomError) -> MysqlError {
    match error {
        AutoRandomError::NotApplicable => tidb_error::mysql::MysqlError::from_terror(&tidb_util::dbterror::ERR_INVALID_AUTO_RANDOM.generate_with_stack_by_args(&[("alter auto_random_base of a non auto_random table".to_owned()).to_string().into()])),
        AutoRandomError::RebaseOverflow { base, maximum } => tidb_error::mysql::MysqlError::from_terror(&tidb_util::dbterror::ERR_INVALID_AUTO_RANDOM.generate_with_stack_by_args(&[(format!(
                "alter auto_random_base to {base} overflows the incremental bits, max allowed base is {maximum}"
            )).to_string().into()])),
        AutoRandomError::InvalidDefinition(reason) => tidb_error::mysql::MysqlError::from_terror(&tidb_util::dbterror::ERR_INVALID_AUTO_RANDOM.generate_with_stack_by_args(&[(reason).to_string().into()])),
        AutoRandomError::AutoId(AutoIdError::Exhausted) => tidb_error::mysql::MysqlError::from_terror(&tidb_error::catalog::autoid::ERR_AUTOINC_READ_FAILED.generate_with_stack_by_args(&[])),
        AutoRandomError::AutoId(AutoIdError::OutOfRange { value, type_name }) => {
            tidb_error::mysql::MysqlError::from_terror(&tidb_error::catalog::types::ERR_OVERFLOW.generate_with_stack(format!("constant {} overflows {}", (value), (type_name))))
        }
        AutoRandomError::AutoId(AutoIdError::Store(detail)) => {
            errors::auto_id_unavailable(detail.0)
        }
        AutoRandomError::ExplicitInsertDisabled => {
            unreachable!("a rebase does not inspect row values")
        }
    }
}
