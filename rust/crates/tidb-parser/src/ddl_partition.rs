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

//! Typed partition fragments shared by CREATE TABLE and ALTER TABLE.

use super::*;
use tidb_ast::{
    AddPartitionSpec, AlterPartitionAction, Expr, PartitionDefinition, PartitionDefinitionClause,
    PartitionIndexUpdate, PartitionInterval, PartitionMaintenanceOp, PartitionMethod,
    PartitionType, PartitionValue, SubPartitionDefinition, TableOption, TablePartitioning,
};

/// Direct source-shaped translation of Go's `parsePartitionOptions`.  CREATE
/// TABLE owns this construction payload; ALTER actions only reuse individual
/// definitions below.
pub(super) fn parse_table_partitioning(parser: &mut Parser) -> PResult<TablePartitioning> {
    parser.expect_kw("PARTITION")?;
    parser.expect_kw("BY")?;
    let method = parse_partition_method(parser, false)?;

    let mut method = method;
    if parser.is_kw("PARTITIONS") {
        parser.bump();
        method.count = parse_partition_count(parser, "partitions")?;
    }

    let mut subpartition = if parser.is_kw("SUBPARTITION") {
        parser.bump();
        parser.expect_kw("BY")?;
        let mut method = parse_partition_method(parser, true)?;
        if parser.is_kw("SUBPARTITIONS") {
            parser.bump();
            method.count = parse_partition_count(parser, "subpartitions")?;
        }
        Some(method)
    } else {
        None
    };

    let mut definitions = Vec::new();
    if parser.is_op("(") {
        parser.bump();
        if parser.is_op(")") {
            return Err(parser.err_here("partition definitions list cannot be empty"));
        }
        definitions.push(parse_partition_definition(parser)?);
        while parser.is_op(",") {
            parser.bump();
            definitions.push(parse_partition_definition(parser)?);
        }
        parser.expect_op(")")?;
    }

    let mut update_indexes = Vec::new();
    if parser.is_kw("UPDATE") {
        parser.bump();
        parser.expect_kw("INDEXES")?;
        parser.expect_op("(")?;
        loop {
            let name = parser.parse_name_or_keyword()?;
            let global = if parser.is_kw("GLOBAL") {
                parser.bump();
                true
            } else {
                parser.expect_kw("LOCAL")?;
                false
            };
            update_indexes.push(PartitionIndexUpdate { name, global });
            if !parser.is_op(",") {
                break;
            }
            parser.bump();
        }
        parser.expect_op(")")?;
    }

    validate_partitioning(&mut method, &mut subpartition, &definitions, parser)?;
    Ok(TablePartitioning {
        method,
        subpartition,
        definitions,
        update_indexes,
    })
}

fn parse_partition_method(parser: &mut Parser, subpartition: bool) -> PResult<PartitionMethod> {
    let linear = if parser.is_kw("LINEAR") {
        parser.bump();
        true
    } else {
        false
    };
    let kind = if parser.is_kw("HASH") {
        parser.bump();
        PartitionType::Hash
    } else if parser.is_kw("KEY") {
        parser.bump();
        PartitionType::Key
    } else if !subpartition && parser.is_kw("RANGE") {
        parser.bump();
        PartitionType::Range
    } else if !subpartition && parser.is_kw("LIST") {
        parser.bump();
        PartitionType::List
    } else if !subpartition && parser.is_kw("SYSTEM_TIME") {
        parser.bump();
        PartitionType::SystemTime
    } else if subpartition {
        return Err(parser.err_here("only HASH/KEY partitions are supported for subpartitions"));
    } else {
        return Err(parser.err_here("expected partition method"));
    };
    if linear && !matches!(kind, PartitionType::Hash | PartitionType::Key) {
        return Err(parser.err_here("LINEAR is valid only for HASH or KEY partitioning"));
    }

    let mut method = PartitionMethod {
        kind,
        linear,
        expr: None,
        columns: Vec::new(),
        key_algorithm: None,
        unit: None,
        limit: 0,
        count: 0,
        interval: None,
    };
    match kind {
        PartitionType::None => {
            return Err(parser.err_here("NONE is not a partition method"));
        }
        PartitionType::Hash => method.expr = Some(parse_partition_expr(parser)?),
        PartitionType::Key => {
            if parser.is_kw("ALGORITHM") {
                parser.bump();
                if parser.is_op("=") {
                    parser.bump();
                }
                let algorithm = parse_partition_count(parser, "KEY ALGORITHM")?;
                if !(1..=2).contains(&algorithm) {
                    return Err(parser.err_here("KEY ALGORITHM must be 1 or 2"));
                }
                method.key_algorithm = Some(algorithm);
            }
            method.columns = parse_partition_columns(parser, false)?;
        }
        PartitionType::Range | PartitionType::List => {
            if parser.is_kw("COLUMNS") || parser.is_kw("FIELDS") {
                parser.bump();
                method.columns = parse_partition_columns(parser, true)?;
            } else {
                method.expr = Some(parse_partition_expr(parser)?);
            }
            if kind == PartitionType::Range && parser.is_kw("INTERVAL") {
                parser.bump();
                parser.expect_op("(")?;
                let expr = parser.parse_expr(prec::NONE)?;
                let unit = if parser.peek().kind == TokenKind::Keyword {
                    Some(parser.bump().text.to_ascii_uppercase())
                } else {
                    None
                };
                parser.expect_op(")")?;
                let first_range_end = if parser.is_kw("FIRST") {
                    Some(parse_partition_less_than_bound(parser, "FIRST")?)
                } else {
                    None
                };
                let last_range_end = if parser.is_kw("LAST") {
                    Some(parse_partition_less_than_bound(parser, "LAST")?)
                } else {
                    None
                };
                if first_range_end.is_some() && last_range_end.is_none() {
                    return Err(
                        parser.err_here("FIRST PARTITION must be followed by LAST PARTITION")
                    );
                }
                let null_partition = if parser.is_kw("NULL") {
                    parser.bump();
                    parser.expect_kw("PARTITION")?;
                    true
                } else {
                    false
                };
                let maxvalue_partition = if parser.is_kw("MAXVALUE") {
                    parser.bump();
                    parser.expect_kw("PARTITION")?;
                    true
                } else {
                    false
                };
                method.interval = Some(PartitionInterval {
                    expr,
                    unit,
                    first_range_end,
                    last_range_end,
                    null_partition,
                    maxvalue_partition,
                });
            }
        }
        PartitionType::SystemTime => {
            if parser.is_kw("INTERVAL") {
                parser.bump();
                method.expr = Some(parser.parse_expr(prec::NONE)?);
                if parser.peek().kind != TokenKind::Keyword {
                    return Err(parser.err_here("expected SYSTEM_TIME interval unit"));
                }
                method.unit = Some(parser.bump().text.to_ascii_uppercase());
            }
            if parser.is_kw("LIMIT") {
                parser.bump();
                // Go uses `parseUint64` here rather than the `PARTITIONS`
                // non-zero validator, so zero remains a representable source
                // value even though it restores as absence.
                method.limit = parse_partition_uint(parser, "SYSTEM_TIME LIMIT")?;
            }
            if method.expr.is_some() && method.limit > 0 {
                return Err(
                    parser.err_here("SYSTEM_TIME partition cannot have both INTERVAL and LIMIT")
                );
            }
        }
    }
    Ok(method)
}

fn parse_partition_expr(parser: &mut Parser) -> PResult<Expr> {
    parser.expect_op("(")?;
    if parser.is_op(")") {
        return Err(parser.err_here("invalid empty partition expression"));
    }
    let expr = parser.parse_expr(prec::NONE)?;
    parser.expect_op(")")?;
    Ok(expr)
}

fn parse_partition_columns(parser: &mut Parser, non_empty: bool) -> PResult<Vec<String>> {
    parser.expect_op("(")?;
    let mut columns = Vec::new();
    if !parser.is_op(")") {
        columns.push(parser.parse_name_or_keyword()?);
        while parser.is_op(",") {
            parser.bump();
            columns.push(parser.parse_name_or_keyword()?);
        }
    }
    parser.expect_op(")")?;
    if non_empty && columns.is_empty() {
        return Err(parser.err_here("COLUMNS partition requires at least one column"));
    }
    Ok(columns)
}

fn parse_partition_count(parser: &mut Parser, what: &str) -> PResult<u64> {
    let count = parse_partition_uint(parser, what)?;
    if count == 0 {
        return Err(parser.err_here(&format!("{what} count must be positive")));
    }
    Ok(count)
}

fn parse_partition_uint(parser: &mut Parser, what: &str) -> PResult<u64> {
    let token = parser.peek().clone();
    if token.kind != TokenKind::IntLit {
        return Err(parser.err_here(&format!("expected {what} count")));
    }
    parser.bump();
    token
        .text
        .parse::<u64>()
        .map_err(|_| parser.err_here(&format!("{what} count out of range")))
}

fn parse_partition_less_than_bound(parser: &mut Parser, position: &str) -> PResult<Expr> {
    parser.expect_kw(position)?;
    parser.expect_kw("PARTITION")?;
    parser.expect_kw("LESS")?;
    parser.expect_kw("THAN")?;
    parse_partition_expr(parser)
}

fn validate_partitioning(
    method: &mut PartitionMethod,
    subpartition: &mut Option<PartitionMethod>,
    definitions: &[PartitionDefinition],
    parser: &Parser,
) -> PResult<()> {
    if !definitions.is_empty() {
        if method.count != 0 && method.count != definitions.len() as u64 {
            return Err(parser.err_here("partition count does not match definitions"));
        }
        method.count = definitions.len() as u64;
    }
    match method.kind {
        PartitionType::Hash | PartitionType::Key if method.count == 0 => method.count = 1,
        PartitionType::Range | PartitionType::List
            if method.interval.is_none() && definitions.is_empty() =>
        {
            return Err(parser.err_here("RANGE/LIST partitions must be defined"));
        }
        PartitionType::SystemTime if definitions.len() < 2 => {
            return Err(parser.err_here("SYSTEM_TIME requires HISTORY and CURRENT partitions"));
        }
        _ => {}
    }
    for definition in definitions {
        validate_definition(method, definition, parser)?;
    }
    let sub_counts: Vec<usize> = definitions
        .iter()
        .map(|definition| definition.sub_partitions.len())
        .collect();
    if let Some(subpartition) = subpartition {
        if let Some(first) = sub_counts.first() {
            if sub_counts.iter().any(|count| count != first) {
                return Err(parser.err_here("subpartition counts differ"));
            }
            if *first != 0 && subpartition.count != 0 && subpartition.count != *first as u64 {
                return Err(parser.err_here("subpartition count does not match definitions"));
            }
            if *first != 0 {
                subpartition.count = *first as u64;
            }
        }
    } else if sub_counts.iter().any(|count| *count != 0) {
        return Err(parser.err_here("subpartition definitions require SUBPARTITION BY"));
    }
    Ok(())
}

fn validate_definition(
    method: &PartitionMethod,
    definition: &PartitionDefinition,
    parser: &Parser,
) -> PResult<()> {
    let columns = method.columns.len();
    match (&method.kind, &definition.clause) {
        (PartitionType::Hash | PartitionType::Key, PartitionDefinitionClause::None) => Ok(()),
        (PartitionType::Range, PartitionDefinitionClause::LessThan(values)) => {
            if columns == 0 && values.len() != 1 || columns > 0 && values.len() != columns {
                Err(parser.err_here("RANGE partition value count does not match columns"))
            } else {
                Ok(())
            }
        }
        (PartitionType::List, PartitionDefinitionClause::In(values)) => {
            let mut expected = None;
            for value in values {
                match value {
                    PartitionValue::MaxValue => {
                        return Err(parser.err_here("MAXVALUE is not valid in VALUES IN"));
                    }
                    PartitionValue::Default => continue,
                    PartitionValue::Expr(_) => {
                        // Go's AST accepts a scalar value for one-column
                        // LIST COLUMNS; only multi-column methods require a
                        // tuple payload.
                        if columns > 1 {
                            return Err(parser.err_here("LIST COLUMNS values require tuples"));
                        }
                        expected.get_or_insert(1usize);
                    }
                    PartitionValue::Tuple(tuple) => {
                        if columns == 0 || tuple.len() != columns {
                            return Err(parser
                                .err_here("LIST partition value count does not match columns"));
                        }
                        expected.get_or_insert(tuple.len());
                    }
                }
            }
            Ok(())
        }
        (PartitionType::List, PartitionDefinitionClause::Default) => Ok(()),
        (PartitionType::SystemTime, PartitionDefinitionClause::History { .. }) => Ok(()),
        _ => Err(parser.err_here("partition definition clause does not match partition method")),
    }
}

/// Recognizes every partition-owned `ALTER TABLE` action before consuming a
/// token. `None` is therefore a strict no-op for the outer DDL dispatcher.
pub(super) fn parse_alter_partition_action(
    parser: &mut Parser,
) -> PResult<Option<AlterPartitionAction>> {
    if let Some(action) = super::alter::repartition::parse(parser)? {
        return Ok(Some(action));
    }
    if parser.is_kw("FIRST") {
        let expr = parse_partition_less_than_bound(parser, "FIRST")?;
        let if_exists = if parser.is_kw("IF") {
            parser.bump();
            parser.expect_kw("EXISTS")?;
            true
        } else {
            false
        };
        return Ok(Some(AlterPartitionAction::FirstPartitionLessThan {
            expr,
            if_exists,
        }));
    }
    if parser.is_kw("LAST") {
        let expr = parse_partition_less_than_bound(parser, "LAST")?;
        let no_write_to_binlog = parse_no_write_to_binlog(parser);
        return Ok(Some(AlterPartitionAction::LastPartitionLessThan {
            expr,
            no_write_to_binlog,
        }));
    }
    if parser.is_kw("MERGE") && parser.is_kw_at(1, "FIRST") {
        parser.bump();
        let expr = parse_partition_less_than_bound(parser, "FIRST")?;
        return Ok(Some(AlterPartitionAction::MergeFirstPartitionLessThan {
            expr,
        }));
    }
    // Direct Go `parseAlterPartition` transition. ATTRIBUTES and placement
    // policy have distinct Go spec kinds but both carry a partition target;
    // generic root table options must not claim this branch.
    if parser.is_kw("PARTITION") {
        parser.bump();
        let partition = parser.parse_name()?;
        if parser.is_kw("ATTRIBUTES") {
            parser.bump();
            if parser.is_op("=") {
                parser.bump();
            }
            let attributes = if parser.is_kw("DEFAULT") {
                parser.bump();
                None
            } else {
                let token = parser.peek().clone();
                if token.kind != TokenKind::Str {
                    return Err(
                        parser.err_here("expected partition ATTRIBUTES string literal or DEFAULT")
                    );
                }
                parser.bump();
                Some(decode_string(&token.text))
            };
            return Ok(Some(AlterPartitionAction::SetAttributes {
                partition,
                attributes,
            }));
        }
        if !parser.is_kw("PLACEMENT") {
            return Err(parser.err_here("expected partition ATTRIBUTES or PLACEMENT POLICY"));
        }
        let Some(TableOption::PlacementPolicy(policy)) = parser.parse_table_option()? else {
            unreachable!("PLACEMENT must parse as a placement-policy table option");
        };
        return Ok(Some(AlterPartitionAction::SetPlacementPolicy {
            partition,
            policy,
        }));
    }
    if parser.is_kw("ADD") && parser.is_kw_at(1, "PARTITION") {
        parser.bump();
        parser.bump();
        let if_not_exists = if parser.is_kw("IF") {
            parser.bump();
            parser.expect_kw("NOT")?;
            parser.expect_kw("EXISTS")?;
            true
        } else {
            false
        };
        let no_write_to_binlog = parse_no_write_to_binlog(parser);
        let spec = if parser.is_kw("PARTITIONS") {
            parser.bump();
            let token = parser.peek().clone();
            if token.kind != TokenKind::IntLit {
                return Err(parser.err_here("expected partition count"));
            }
            parser.bump();
            AddPartitionSpec::Count(
                token
                    .text
                    .parse::<u64>()
                    .map_err(|_| parser.err_here("partition count out of range"))?,
            )
        } else if parser.is_op("(") {
            parser.bump();
            let mut definitions = vec![parse_partition_definition(parser)?];
            while parser.is_op(",") {
                parser.bump();
                definitions.push(parse_partition_definition(parser)?);
            }
            parser.expect_op(")")?;
            AddPartitionSpec::Definitions(definitions)
        } else {
            // Go's `parseAlterAdd` accepts the empty payload.  It leaves both
            // `Num` and `PartDefinitions` at their zero values, and the AST
            // restore consequently emits the bare `ADD PARTITION` action.
            AddPartitionSpec::Count(0)
        };
        return Ok(Some(AlterPartitionAction::Add {
            if_not_exists,
            no_write_to_binlog,
            spec,
        }));
    }
    if parser.is_kw("DROP") && parser.is_kw_at(1, "PARTITION") {
        parser.bump();
        parser.bump();
        let if_exists = if parser.is_kw("IF") {
            parser.bump();
            parser.expect_kw("EXISTS")?;
            true
        } else {
            false
        };
        return Ok(Some(AlterPartitionAction::Drop {
            if_exists,
            names: parse_partition_name_list(parser)?,
        }));
    }
    if parser.is_kw("EXCHANGE") {
        parser.bump();
        parser.expect_kw("PARTITION")?;
        let partition = parser.parse_name()?;
        parser.expect_kw("WITH")?;
        parser.expect_kw("TABLE")?;
        let table = parser.parse_name_path()?;
        let with_validation = if parser.is_kw("WITHOUT") {
            parser.bump();
            parser.expect_kw("VALIDATION")?;
            false
        } else {
            if parser.is_kw("WITH") {
                parser.bump();
                parser.expect_kw("VALIDATION")?;
            }
            true
        };
        return Ok(Some(AlterPartitionAction::Exchange {
            partition,
            table,
            with_validation,
        }));
    }
    if parser.is_kw("REORGANIZE") {
        parser.bump();
        parser.expect_kw("PARTITION")?;
        let no_write_to_binlog = parse_no_write_to_binlog(parser);
        if is_action_end(parser) {
            return Ok(Some(AlterPartitionAction::Reorganize {
                no_write_to_binlog,
                names: Vec::new(),
                definitions: Vec::new(),
            }));
        }
        let names = parse_partition_name_list(parser)?;
        parser.expect_kw("INTO")?;
        parser.expect_op("(")?;
        let mut definitions = vec![parse_partition_definition(parser)?];
        while parser.is_op(",") {
            parser.bump();
            definitions.push(parse_partition_definition(parser)?);
        }
        parser.expect_op(")")?;
        return Ok(Some(AlterPartitionAction::Reorganize {
            no_write_to_binlog,
            names,
            definitions,
        }));
    }
    if parser.is_kw("COALESCE") {
        parser.bump();
        parser.expect_kw("PARTITION")?;
        let no_write_to_binlog = parse_no_write_to_binlog(parser);
        let token = parser.peek().clone();
        if token.kind != TokenKind::IntLit {
            return Err(parser.err_here("expected partition count"));
        }
        parser.bump();
        return Ok(Some(AlterPartitionAction::Coalesce {
            no_write_to_binlog,
            count: token
                .text
                .parse::<u64>()
                .map_err(|_| parser.err_here("partition count out of range"))?,
        }));
    }
    if parser.is_kw("TRUNCATE") {
        parser.bump();
        parser.expect_kw("PARTITION")?;
        let all = parser.is_kw("ALL");
        let names = if all {
            parser.bump();
            Vec::new()
        } else {
            parse_partition_name_list(parser)?
        };
        return Ok(Some(AlterPartitionAction::Truncate { all, names }));
    }
    if parser.is_kw("CHECK") {
        parser.bump();
        parser.expect_kw("PARTITION")?;
        let all = parser.is_kw("ALL");
        let names = if all {
            parser.bump();
            Vec::new()
        } else {
            parse_partition_name_list(parser)?
        };
        return Ok(Some(AlterPartitionAction::Check { all, names }));
    }
    if parser.is_kw("IMPORT") {
        parser.bump();
        parser.expect_kw("PARTITION")?;
        let all = parser.is_kw("ALL");
        let names = if all {
            parser.bump();
            Vec::new()
        } else {
            parse_partition_name_list(parser)?
        };
        parser.expect_kw("TABLESPACE")?;
        return Ok(Some(AlterPartitionAction::ImportTablespace { all, names }));
    }
    if parser.is_kw("DISCARD") {
        parser.bump();
        parser.expect_kw("PARTITION")?;
        let all = parser.is_kw("ALL");
        let names = if all {
            parser.bump();
            Vec::new()
        } else {
            parse_partition_name_list(parser)?
        };
        parser.expect_kw("TABLESPACE")?;
        return Ok(Some(AlterPartitionAction::DiscardTablespace { all, names }));
    }
    // `SPLIT PRIMARY KEY|INDEX ...` belongs to the region-splitting parser
    // outside this partition-action envelope. Claim only the Go
    // `SPLIT MAXVALUE PARTITION` production here; consuming every `SPLIT`
    // token would steal those unrelated ALTER statements.
    if parser.is_kw("SPLIT") && parser.is_kw_at(1, "MAXVALUE") {
        parser.bump();
        parser.expect_kw("MAXVALUE")?;
        parser.expect_kw("PARTITION")?;
        parser.expect_kw("LESS")?;
        parser.expect_kw("THAN")?;
        let expr = parse_partition_expr(parser)?;
        return Ok(Some(AlterPartitionAction::SplitMaxValuePartition { expr }));
    }
    if parser.is_kw("REMOVE") && parser.is_kw_at(1, "PARTITIONING") {
        parser.bump();
        parser.expect_kw("PARTITIONING")?;
        return Ok(Some(AlterPartitionAction::RemovePartitioning));
    }
    if parser.is_kw("REBUILD") || parser.is_kw("OPTIMIZE") || parser.is_kw("REPAIR") {
        let operation = if parser.is_kw("REBUILD") {
            PartitionMaintenanceOp::Rebuild
        } else if parser.is_kw("OPTIMIZE") {
            PartitionMaintenanceOp::Optimize
        } else {
            PartitionMaintenanceOp::Repair
        };
        parser.bump();
        parser.expect_kw("PARTITION")?;
        let no_write_to_binlog = parse_no_write_to_binlog(parser);
        let all = parser.is_kw("ALL");
        let names = if all {
            parser.bump();
            Vec::new()
        } else {
            parse_partition_name_list(parser)?
        };
        return Ok(Some(AlterPartitionAction::Maintain {
            operation,
            no_write_to_binlog,
            all,
            names,
        }));
    }
    Ok(None)
}

fn parse_partition_definition(parser: &mut Parser) -> PResult<PartitionDefinition> {
    parser.expect_kw("PARTITION")?;
    let name = parser.parse_name_or_keyword()?;
    let clause = if parser.is_kw("VALUES") {
        parser.bump();
        if parser.is_kw("LESS") {
            parser.bump();
            parser.expect_kw("THAN")?;
            let values = if parser.is_kw("MAXVALUE") {
                parser.bump();
                vec![PartitionValue::MaxValue]
            } else {
                parser.expect_op("(")?;
                let mut values = vec![if parser.is_kw("MAXVALUE") {
                    parser.bump();
                    PartitionValue::MaxValue
                } else if parser.is_kw("DEFAULT") {
                    return Err(parser.err_here("DEFAULT is not valid in VALUES LESS THAN"));
                } else {
                    PartitionValue::Expr(parser.parse_expr(prec::NONE)?)
                }];
                while parser.is_op(",") {
                    parser.bump();
                    values.push(if parser.is_kw("MAXVALUE") {
                        parser.bump();
                        PartitionValue::MaxValue
                    } else if parser.is_kw("DEFAULT") {
                        return Err(parser.err_here("DEFAULT is not valid in VALUES LESS THAN"));
                    } else {
                        PartitionValue::Expr(parser.parse_expr(prec::NONE)?)
                    });
                }
                parser.expect_op(")")?;
                values
            };
            PartitionDefinitionClause::LessThan(values)
        } else if parser.is_kw("IN") {
            parser.bump();
            parser.expect_op("(")?;
            let mut values = vec![parse_partition_value(parser, true)?];
            while parser.is_op(",") {
                parser.bump();
                values.push(parse_partition_value(parser, true)?);
            }
            parser.expect_op(")")?;
            if matches!(values.as_slice(), [PartitionValue::Default]) {
                PartitionDefinitionClause::Default
            } else {
                PartitionDefinitionClause::In(values)
            }
        } else {
            return Err(parser.err_here("expected LESS THAN or IN after partition VALUES"));
        }
    } else if parser.is_kw("DEFAULT") {
        parser.bump();
        PartitionDefinitionClause::Default
    } else if parser.is_kw("HISTORY") || parser.is_kw("CURRENT") {
        let current = parser.is_kw("CURRENT");
        parser.bump();
        PartitionDefinitionClause::History { current }
    } else {
        PartitionDefinitionClause::None
    };

    let mut options = Vec::new();
    while let Some(option) = parser.parse_table_option()? {
        options.push(option);
    }
    let mut sub_partitions = Vec::new();
    if parser.is_op("(") && parser.is_kw_at(1, "SUBPARTITION") {
        parser.bump();
        loop {
            parser.expect_kw("SUBPARTITION")?;
            let name = parser.parse_name_or_keyword()?;
            let mut options = Vec::new();
            while let Some(option) = parser.parse_table_option()? {
                options.push(option);
            }
            sub_partitions.push(SubPartitionDefinition { name, options });
            if !parser.is_op(",") {
                break;
            }
            parser.bump();
        }
        parser.expect_op(")")?;
    }
    Ok(PartitionDefinition {
        name,
        clause,
        options,
        sub_partitions,
    })
}

pub(super) fn parse_partition_value(
    parser: &mut Parser,
    allow_default: bool,
) -> PResult<PartitionValue> {
    if parser.is_kw("MAXVALUE") {
        parser.bump();
        return Ok(PartitionValue::MaxValue);
    }
    if parser.is_kw("DEFAULT") {
        if !allow_default {
            return Err(parser.err_here("DEFAULT is not valid in VALUES LESS THAN"));
        }
        parser.bump();
        return Ok(PartitionValue::Default);
    }
    if parser.is_op("(") {
        parser.bump();
        let mut values = vec![parser.parse_expr(prec::NONE)?];
        while parser.is_op(",") {
            parser.bump();
            values.push(parser.parse_expr(prec::NONE)?);
        }
        parser.expect_op(")")?;
        return Ok(PartitionValue::Tuple(values));
    }
    Ok(PartitionValue::Expr(parser.parse_expr(prec::NONE)?))
}

pub(super) fn parse_partition_name_list(parser: &mut Parser) -> PResult<Vec<String>> {
    let mut names = vec![parser.parse_name()?];
    // A comma belongs to this payload only when another identifier follows.
    // Otherwise leave it for the outer AlterTableStmt spec loop (for example,
    // `DROP PARTITION p0, ADD COLUMN c INT`).
    while parser.is_op(",") && parser.peek_n(1).kind == TokenKind::Ident {
        parser.bump();
        names.push(parser.parse_name()?);
    }
    Ok(names)
}

pub(super) fn parse_no_write_to_binlog(parser: &mut Parser) -> bool {
    if parser.is_kw("NO_WRITE_TO_BINLOG") {
        parser.bump();
        true
    } else {
        false
    }
}

pub(super) fn is_action_end(parser: &Parser) -> bool {
    parser.peek().kind == TokenKind::Eof || parser.is_op(";") || parser.is_op(",")
}
