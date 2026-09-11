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

//! Generated-column checks on the prospective MODIFY/CHANGE schema.

use crate::generated_column::GeneratedColumn;
use crate::{DriverError, KvTable, StmtContext};
use tidb_ast::{ColumnDef, ColumnOption};
use tidb_datatype::{FieldType, FieldTypeCode};

// Go noReorgDataStrict, used before generated-column validation.
fn no_reorganization(old: &FieldType, new: &FieldType, indexed: bool) -> bool {
    use FieldTypeCode::*;
    let same_sign = old.is_unsigned() == new.is_unsigned();
    let integer = |code| matches!(code, Tiny | Short | Int24 | Long | LongLong);
    let string = |code| {
        matches!(
            code,
            Varchar | VarString | String | Blob | TinyBlob | MediumBlob | LongBlob
        )
    };
    let no_truncation = same_sign
        && !(new.flen() > 0 && (new.flen() < old.flen() || new.decimal() < old.decimal()));
    if old.code() == new.code() {
        return match old.code() {
            NewDecimal => same_sign && old.flen() == new.flen() && old.decimal() == new.decimal(),
            Enum | Set => new.elems_snapshot().starts_with(&old.elems_snapshot()),
            Tiny | Short | Int24 | Long | LongLong => same_sign,
            String if old.is_binary_string() => old.flen() == new.flen(),
            VectorFloat32 => new.flen() == -1 || old.flen() == new.flen(),
            _ => no_truncation,
        };
    }
    if matches!(old.code(), Varchar | VarString) && new.code() == String {
        return false;
    }
    if old.code() == String && matches!(new.code(), Varchar | VarString) && indexed {
        return false;
    }
    if string(old.code()) && string(new.code()) {
        return no_truncation;
    }
    integer(old.code())
        && integer(new.code())
        && same_sign
        && new.code().default_length_and_decimal().0 >= old.code().default_length_and_decimal().0
}

pub(super) fn build(
    table: &KvTable,
    offset: usize,
    def: &ColumnDef,
    field_type: &FieldType,
    position: Option<usize>,
    ctx: &StmtContext,
) -> Result<Option<GeneratedColumn>, DriverError> {
    let old = &table.columns()[offset];
    let option = def.options.iter().find_map(|option| match option {
        ColumnOption::Generated {
            expression, stored, ..
        } => Some((expression, *stored)),
        _ => None,
    });
    let indexed = table
        .indexes()
        .iter()
        .any(|index| index.column_offsets.contains(&offset));
    if (old.generated.is_some() || option.is_some())
        && !no_reorganization(&old.field_type, field_type, indexed)
    {
        return Err(DriverError::UnsupportedModifyColumn(if option.is_some() {
            "new column is generated"
        } else {
            "old column is generated"
        }));
    }
    if old.generated.as_ref().is_some_and(|g| !g.stored)
        != option.is_some_and(|(_, stored)| !stored)
    {
        return Err(DriverError::UnsupportedOnGeneratedColumn(
            "Changing the STORED status".to_owned(),
        ));
    }
    let mut candidate = table.columns().to_vec();
    candidate[offset].name = def.name.clone();
    candidate[offset].field_type = field_type.clone();
    let generated = if let Some((expression, stored)) = option {
        let names = candidate.iter().map(|c| c.name.clone()).collect::<Vec<_>>();
        let types = candidate
            .iter()
            .map(|c| c.field_type.clone())
            .collect::<Vec<_>>();
        Some(
            crate::generated_column::build_added_generated_column_with_like_default_escape(
                &def.name,
                expression,
                stored,
                &names,
                &types,
                &ctx.session_zone(),
                ctx.like_default_escape(),
            )
            .map_err(super::generated_column_error)?,
        )
    } else {
        None
    };
    candidate[offset].generated = generated.clone();
    if let Some(position) = position.filter(|p| *p != offset) {
        let column = candidate.remove(offset);
        candidate.insert(position, column);
    }
    for (at, column) in candidate.iter().enumerate() {
        let Some(generated) = &column.generated else {
            continue;
        };
        for dependency in &generated.dependencies {
            let Some((dep_at, dep)) = candidate
                .iter()
                .enumerate()
                .find(|(_, c)| c.name.eq_ignore_ascii_case(dependency))
            else {
                return Err(super::generated_column_error(
                    crate::generated_column::GeneratedDdlError::UnknownDependency(
                        dependency.clone(),
                    ),
                ));
            };
            if dep.generated.is_some() && dep_at >= at {
                return Err(DriverError::GeneratedColumnNonPrior);
            }
        }
    }
    if let Some(generated) = &generated {
        if !ctx.auto_increment_in_generated()
            && generated.dependencies.iter().any(|name| {
                candidate.iter().any(|c| {
                    c.name.eq_ignore_ascii_case(name)
                        && c.field_type
                            .has_flag(tidb_datatype::FieldTypeFlags::AUTO_INCREMENT)
                })
            })
        {
            return Err(DriverError::DdlCoded {
                errno: 3109,
                message: format!(
                    "Generated column '{}' cannot refer to auto-increment column.",
                    def.name
                ),
            });
        }
        let same_expression = old
            .generated
            .as_ref()
            .is_some_and(|g| g.expr_text == generated.expr_text);
        if !same_expression && generated.stored {
            return Err(DriverError::UnsupportedOnGeneratedColumn(
                "modifying a stored column".to_owned(),
            ));
        }
        if indexed && (!same_expression || old.field_type != *field_type) {
            return Err(DriverError::DdlCoded {
                errno: 3106,
                message: "Unsupported modification for generated columns covered by an index"
                    .to_owned(),
            });
        }
    }
    Ok(generated)
}
