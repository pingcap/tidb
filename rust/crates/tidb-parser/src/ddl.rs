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
// See the License for the specific language governing permissions and
// limitations under the License.

//! `CREATE TABLE`, `ALTER TABLE`, and the standalone `RENAME TABLE` /
//! `DROP TABLE` statements. Called from `crate::Parser::parse_statement`.

use tidb_ast::{
    AdminStmt, AlterOrderItem, AlterPartitionAction, AlterTableAction, AlterTableAlgorithm,
    AlterTableStmt, AnalyzeTableStmt, ColumnOption, ColumnPosition, CompactReplicaKind,
    CreateTableTemporary, CreateViewStmt, DatabaseOption, DdlStmt, DropIndexAlgorithm,
    DropIndexLock, DropIndexStmt, DropTableStmt, DropTemporary, Expr, FlashbackDatabaseStmt,
    FlashbackTableStmt, FlashbackToTimestampStmt, IndexConstraintKind, OptimizeTableStmt,
    QueryStmt, RecoverTableStmt, RenameTableStmt, RepairTableStmt, SetOprTermBody, SplitOption,
    SplitRegionStmt, SplitTarget, Stmt, TableLock, TableLockType, UserSpec, ViewAlgorithm,
    ViewCheckOption, ViewSecurity,
};
use tidb_lexer::{canonical_charset, canonical_collation, TokenKind};

use crate::{decode_string, prec, PResult, Parser};

#[path = "ddl/alter.rs"]
mod alter;
#[path = "ddl/check.rs"]
mod check;
#[path = "ddl/column.rs"]
mod column;
#[path = "ddl/column/generated.rs"]
mod column_generated;
#[path = "ddl/column/options.rs"]
mod column_options;
#[path = "ddl/column/time.rs"]
mod column_time;
#[path = "ddl/create.rs"]
mod create;
#[path = "ddl/create_split.rs"]
mod create_split;
#[path = "ddl/field_type.rs"]
mod field_type;
#[path = "ddl/index.rs"]
mod index;
#[path = "ddl/column/inline_key.rs"]
mod inline_key;
#[path = "ddl_partition.rs"]
mod partition;
#[path = "ddl/table_option.rs"]
mod table_option;

impl Parser {
    pub(crate) fn parse_recover_table(&mut self) -> PResult<RecoverTableStmt> {
        self.expect_kw("RECOVER")?;
        self.expect_kw("TABLE")?;
        if self.is_kw("BY") {
            self.bump();
            self.expect_kw("JOB")?;
            let token = self.peek().clone();
            if token.kind != TokenKind::IntLit {
                return Err(self.err_here("expected RECOVER TABLE job ID"));
            }
            self.bump();
            let job_id = token
                .text
                .parse::<i64>()
                .map_err(|_| self.err_here("RECOVER TABLE job ID out of range"))?;
            return Ok(RecoverTableStmt {
                job_id,
                table: None,
                job_num: 0,
            });
        }
        let table = self.parse_table_name()?;
        let job_num = if self.peek().kind == TokenKind::IntLit {
            self.bump()
                .text
                .parse::<i64>()
                .map_err(|_| self.err_here("RECOVER TABLE job count out of range"))?
        } else {
            0
        };
        Ok(RecoverTableStmt {
            job_id: 0,
            table: Some(table),
            job_num,
        })
    }

    pub(crate) fn parse_flashback_statement(&mut self) -> PResult<DdlStmt> {
        self.expect_kw("FLASHBACK")?;
        if self.is_kw("CLUSTER") {
            self.bump();
            return Ok(DdlStmt::FlashbackToTimestamp(Box::new(
                self.parse_flashback_timestamp_tail(Vec::new(), String::new())?,
            )));
        }
        if self.is_kw("TABLE") {
            self.bump();
            let mut tables = vec![self.parse_table_name()?];
            while self.is_op(",") {
                self.bump();
                tables.push(self.parse_table_name()?);
            }
            if self.is_flashback_timestamp_tail() {
                return Ok(DdlStmt::FlashbackToTimestamp(Box::new(
                    self.parse_flashback_timestamp_tail(tables, String::new())?,
                )));
            }
            let new_name = if self.is_kw("TO") {
                self.bump();
                self.parse_any_token_name()
            } else {
                String::new()
            };
            return Ok(DdlStmt::FlashbackTable(Box::new(FlashbackTableStmt {
                table: tables.into_iter().next(),
                new_name,
            })));
        }
        if self.is_kw("DATABASE") || self.is_kw("SCHEMA") {
            self.bump();
            let database_name = self.parse_ident_like_name()?;
            if self.is_flashback_timestamp_tail() {
                return Ok(DdlStmt::FlashbackToTimestamp(Box::new(
                    self.parse_flashback_timestamp_tail(Vec::new(), database_name)?,
                )));
            }
            let new_name = if self.is_kw("TO") {
                self.bump();
                Some(self.parse_any_token_name())
            } else {
                None
            };
            return Ok(DdlStmt::FlashbackDatabase(Box::new(
                FlashbackDatabaseStmt {
                    name: database_name,
                    new_name,
                },
            )));
        }
        Err(self.err_here("expected CLUSTER, TABLE, DATABASE, or SCHEMA"))
    }

    fn is_flashback_timestamp_tail(&self) -> bool {
        (self.is_kw("TO") && self.is_kw_at(1, "TIMESTAMP") && self.peek_n(2).kind == TokenKind::Str)
            || (self.is_kw("TO") && self.is_kw_at(1, "TSO"))
    }

    fn parse_flashback_timestamp_tail(
        &mut self,
        tables: Vec<Vec<String>>,
        database_name: String,
    ) -> PResult<FlashbackToTimestampStmt> {
        self.expect_kw("TO")?;
        if self.is_kw("TIMESTAMP") {
            self.bump();
            if self.peek().kind != TokenKind::Str {
                return Err(self.err_here("FLASHBACK TIMESTAMP requires a string literal"));
            }
            let timestamp = crate::decode_string(&self.bump().text);
            return Ok(FlashbackToTimestampStmt {
                flashback_ts: Some(Expr::RawString(timestamp)),
                flashback_tso: 0,
                tables,
                database_name,
            });
        }
        self.expect_kw("TSO")?;
        if self.peek().kind != TokenKind::IntLit {
            return Err(self.err_here("FLASHBACK TSO requires a positive integer"));
        }
        let flashback_tso = self
            .bump()
            .text
            .parse::<u64>()
            .map_err(|_| self.err_here("FLASHBACK TSO out of range"))?;
        if flashback_tso == 0 {
            return Err(self.err_here("FLASHBACK TSO must be positive"));
        }
        Ok(FlashbackToTimestampStmt {
            flashback_ts: None,
            flashback_tso,
            tables,
            database_name,
        })
    }

    pub(crate) fn parse_optimize_table(&mut self) -> PResult<OptimizeTableStmt> {
        self.expect_kw("OPTIMIZE")?;
        let no_write_to_binlog = if self.is_kw("LOCAL") || self.is_kw("NO_WRITE_TO_BINLOG") {
            self.bump();
            true
        } else {
            false
        };
        if self.is_kw("TABLE") || self.is_kw("TABLES") {
            self.bump();
        } else {
            return Err(self.err_here("expected TABLE"));
        }
        let mut tables = vec![self.parse_table_name()?];
        while self.is_op(",") {
            self.bump();
            tables.push(self.parse_table_name()?);
        }
        Ok(OptimizeTableStmt {
            no_write_to_binlog,
            tables,
        })
    }

    pub(crate) fn parse_repair_table(&mut self) -> PResult<RepairTableStmt> {
        self.expect_kw("ADMIN")?;
        self.expect_kw("REPAIR")?;
        self.expect_kw("TABLE")?;
        let table = self.parse_table_name()?;
        let create = self.parse_create_table()?;
        Ok(RepairTableStmt { table, create })
    }

    /// Parses Go's `parseCreateDatabaseStmt`, keeping its options as typed
    /// AST data rather than accepting them and then losing them before
    /// canonical restore. Go's parser test proves `CREATE SCHEMA` maps to the
    /// same `CreateDatabaseStmt` and restores as `CREATE DATABASE`.
    pub(crate) fn parse_create_database(&mut self) -> PResult<(bool, String, Vec<DatabaseOption>)> {
        self.expect_kw("CREATE")?;
        if self.is_kw("DATABASE") || self.is_kw("SCHEMA") {
            self.bump();
        } else {
            return Err(self.err_here("expected DATABASE or SCHEMA"));
        }
        let if_not_exists = self.parse_if_not_exists()?;
        let name = self.parse_ident_like_name()?;
        let options = self.parse_database_options()?;
        Ok((if_not_exists, name, options))
    }

    /// Parses Go's `ALTER {DATABASE|SCHEMA} [name] option [, option ...]`.
    /// A missing name means the current default database; `CHARSET` itself is
    /// deliberately NOT a name-omitting option starter because Go's grammar
    /// resolves that ambiguous one-word spelling as a database name.
    pub(crate) fn parse_alter_database(
        &mut self,
    ) -> PResult<(Option<String>, Vec<DatabaseOption>)> {
        self.expect_kw("ALTER")?;
        if self.is_kw("DATABASE") || self.is_kw("SCHEMA") {
            self.bump();
        } else {
            return Err(self.err_here("expected DATABASE or SCHEMA"));
        }
        let name = if self.starts_alter_database_option() {
            None
        } else {
            Some(self.parse_ident_like_name()?)
        };
        let options = self.parse_database_options()?;
        if options.is_empty() {
            return Err(self.err_here("expected an ALTER DATABASE option"));
        }
        Ok((name, options))
    }

    fn starts_alter_database_option(&self) -> bool {
        self.is_kw("CHARACTER")
            || self.is_kw("CHAR")
            || self.is_kw("COLLATE")
            || self.is_kw("DEFAULT")
            || self.is_kw("ENCRYPTION")
            || self.is_kw("PLACEMENT")
            || self.is_kw("SET")
    }

    /// Direct structural port of Go's `parseDatabaseOptions`. The shared DDL
    /// charset validator keeps this on TiDB's supported seven-name subset;
    /// recognized-but-unsupported MySQL names fail at the same parser boundary.
    fn parse_database_options(&mut self) -> PResult<Vec<DatabaseOption>> {
        let mut options = Vec::new();
        loop {
            let had_default = if self.is_kw("DEFAULT") {
                self.bump();
                true
            } else {
                false
            };

            let option = if self.is_kw("CHARACTER") || self.is_kw("CHAR") {
                self.bump();
                if self.is_kw("SET") {
                    self.bump();
                }
                if self.is_op("=") {
                    self.bump();
                }
                let raw = self.parse_table_option_word()?;
                let name = field_type::canonical_field_charset(&raw)
                    .ok_or_else(|| {
                        self.err_here(&format!("[parser:1115]Unknown character set: '{raw}'"))
                    })?
                    .to_owned();
                DatabaseOption::CharacterSet(name)
            } else if self.is_kw("CHARSET") {
                self.bump();
                if self.is_op("=") {
                    self.bump();
                }
                let raw = self.parse_table_option_word()?;
                let name = field_type::canonical_field_charset(&raw)
                    .ok_or_else(|| {
                        self.err_here(&format!("[parser:1115]Unknown character set: '{raw}'"))
                    })?
                    .to_owned();
                DatabaseOption::CharacterSet(name)
            } else if self.is_kw("COLLATE") {
                self.bump();
                if self.is_op("=") {
                    self.bump();
                }
                let raw = self.parse_table_option_word()?;
                let collation = canonical_collation(&raw).ok_or_else(|| {
                    self.err_here(&format!("[ddl:1273]Unknown collation: '{raw}'"))
                })?;
                DatabaseOption::Collate(collation.to_owned())
            } else if self.is_kw("ENCRYPTION") {
                self.bump();
                if self.is_op("=") {
                    self.bump();
                }
                let token = self.peek().clone();
                if token.kind != TokenKind::Str {
                    return Err(self.err_here("expected a string literal after ENCRYPTION"));
                }
                self.bump();
                let value = decode_string(&token.text);
                if !matches!(value.as_str(), "Y" | "y" | "N" | "n") {
                    return Err(self.err_here(&format!(
                        "[parser:1525]Incorrect argument (should be Y or N) value: '{value}'"
                    )));
                }
                DatabaseOption::Encryption(value)
            } else if self.is_kw("PLACEMENT") {
                self.bump();
                if self.is_kw("POLICY") {
                    self.bump();
                }
                let name = if self.is_kw("SET") && self.is_kw_at(1, "DEFAULT") {
                    self.bump();
                    self.bump();
                    "DEFAULT".to_owned()
                } else {
                    if self.is_op("=") {
                        self.bump();
                    }
                    if self.is_kw("DEFAULT") {
                        self.bump();
                        "DEFAULT".to_owned()
                    } else {
                        self.parse_any_token_name()
                    }
                };
                DatabaseOption::PlacementPolicy(name)
            } else if self.is_kw("SET") && self.is_kw_at(1, "TIFLASH") {
                self.bump();
                self.bump();
                self.expect_kw("REPLICA")?;
                let token = self.peek().clone();
                if token.kind != TokenKind::IntLit {
                    return Err(self.err_here("expected an integer after SET TIFLASH REPLICA"));
                }
                self.bump();
                let count = token
                    .text
                    .parse()
                    .map_err(|_| self.err_here("TIFLASH replica count is out of range"))?;
                let mut labels = Vec::new();
                if self.is_kw("LOCATION") {
                    self.bump();
                    self.expect_kw("LABELS")?;
                    loop {
                        let token = self.peek().clone();
                        if token.kind != TokenKind::Str {
                            return Err(self.err_here("expected a TiFlash location label string"));
                        }
                        self.bump();
                        labels.push(decode_string(&token.text));
                        if !self.is_op(",") {
                            break;
                        }
                        self.bump();
                    }
                }
                DatabaseOption::SetTiFlashReplica { count, labels }
            } else if had_default {
                return Err(self.err_here(
                    "expected CHARACTER SET/CHARSET/COLLATE/ENCRYPTION/PLACEMENT POLICY after DEFAULT",
                ));
            } else {
                break;
            };
            options.push(option);
        }
        Ok(options)
    }

    /// Parses Go's standalone `LOCK TABLE[S]` grammar from
    /// `set_explain_parser.go`. The source parser deliberately accepts a
    /// missing mode and leaves Go's zero-value `TableLockNone` in the AST,
    /// which canonical restore then prints as `NONE`.
    pub(crate) fn parse_lock_tables(&mut self) -> PResult<Vec<TableLock>> {
        self.expect_kw("LOCK")?;
        if self.is_kw("TABLES") {
            self.bump();
        } else {
            self.expect_kw("TABLE")?;
        }

        let mut locks = vec![self.parse_table_lock()?];
        while self.is_op(",") {
            self.bump();
            locks.push(self.parse_table_lock()?);
        }
        Ok(locks)
    }

    /// Parses one `table_name READ [LOCAL] | WRITE [LOCAL]` lock element.
    ///
    /// This intentionally has no `else` error arm after the table path. Go's
    /// `parseTableLock` returns its zero-value lock type when no mode follows,
    /// and `TableLockType::None` is observable in the restored SQL.
    fn parse_table_lock(&mut self) -> PResult<TableLock> {
        let table = self.parse_lock_table_path()?;
        let lock_type = if self.is_kw("READ") {
            self.bump();
            if self.is_kw("LOCAL") {
                self.bump();
                TableLockType::ReadLocal
            } else {
                TableLockType::Read
            }
        } else if self.is_kw("WRITE") {
            self.bump();
            if self.is_kw("LOCAL") {
                self.bump();
                TableLockType::WriteLocal
            } else {
                TableLockType::Write
            }
        } else {
            TableLockType::None
        };
        Ok(TableLock { table, lock_type })
    }

    /// Direct equivalent of Go's `parseTableName` for the narrow table-lock
    /// grammar. Unlike ordinary Rust name paths, Go allows any keyword token
    /// in this unambiguous table-name slot and accepts `*.table` too.
    fn parse_lock_table_path(&mut self) -> PResult<Vec<String>> {
        if self.is_op("*") && self.is_op_at(1, ".") {
            self.bump();
            self.bump();
            return Ok(vec!["*".to_owned(), self.parse_lock_table_name()?]);
        }

        let mut table = vec![self.parse_lock_table_name()?];
        if self.is_op(".") {
            self.bump();
            table.push(self.parse_lock_table_name()?);
        }
        Ok(table)
    }

    fn parse_lock_table_name(&mut self) -> PResult<String> {
        match self.peek().kind {
            TokenKind::Ident | TokenKind::Keyword | TokenKind::CharsetIntroducer => {
                Ok(self.bump().text)
            }
            _ => Err(self.err_here("expected LOCK TABLES table name")),
        }
    }

    /// Parses Go's `UNLOCK TABLE[S]` grammar from `admin_stmt_parser.go`.
    pub(crate) fn parse_unlock_tables(&mut self) -> PResult<()> {
        self.expect_kw("UNLOCK")?;
        if self.is_kw("TABLES") {
            self.bump();
            Ok(())
        } else {
            self.expect_kw("TABLE")
        }
    }

    /// Parses Go's `DROP [HYPO] INDEX [IF EXISTS] name ON table` grammar,
    /// including its shared two-slot `ALGORITHM` / `LOCK` suffix. `DEFAULT`
    /// suffixes intentionally become absent because Go's AST restore omits
    /// them; `HYPO` is retained structurally but likewise omitted on restore.
    pub(crate) fn parse_drop_index(&mut self) -> PResult<DropIndexStmt> {
        self.expect_kw("DROP")?;
        let is_hypo = if self.is_kw("HYPO") {
            self.bump();
            true
        } else {
            false
        };
        self.expect_kw("INDEX")?;
        let if_exists = self.parse_if_exists()?;
        let name = crate::table_name_token_text(self.bump());
        self.expect_kw("ON")?;
        let table = self.parse_table_name()?;
        let (algorithm, lock) = self.parse_drop_index_lock_and_algorithm()?;
        Ok(DropIndexStmt {
            is_hypo,
            if_exists,
            name,
            table,
            algorithm,
            lock,
        })
    }

    /// Direct source translation of Go's `parseIndexLockAndAlgorithm`: its
    /// two passes intentionally allow either source order and overwrite a
    /// repeated characteristic with the later one, while restore emits the
    /// fixed algorithm-then-lock order.
    fn parse_drop_index_lock_and_algorithm(
        &mut self,
    ) -> PResult<(Option<DropIndexAlgorithm>, Option<DropIndexLock>)> {
        let mut algorithm = None;
        let mut lock = None;
        for _ in 0..2 {
            if self.is_kw("ALGORITHM") {
                self.bump();
                if self.is_op("=") {
                    self.bump();
                }
                algorithm = if self.is_kw("DEFAULT") {
                    self.bump();
                    None
                } else if self.is_kw("INPLACE") {
                    self.bump();
                    Some(DropIndexAlgorithm::Inplace)
                } else if self.is_kw("COPY") {
                    self.bump();
                    Some(DropIndexAlgorithm::Copy)
                } else if self.is_kw("INSTANT") {
                    self.bump();
                    Some(DropIndexAlgorithm::Instant)
                } else {
                    return Err(self.err_here("expected DROP INDEX algorithm"));
                };
            } else if self.is_kw("LOCK") {
                self.bump();
                if self.is_op("=") {
                    self.bump();
                }
                lock = if self.is_kw("DEFAULT") {
                    self.bump();
                    None
                } else if self.is_kw("NONE") {
                    self.bump();
                    Some(DropIndexLock::None)
                } else if self.is_kw("SHARED") {
                    self.bump();
                    Some(DropIndexLock::Shared)
                } else if self.is_kw("EXCLUSIVE") {
                    self.bump();
                    Some(DropIndexLock::Exclusive)
                } else {
                    return Err(self.err_here("expected DROP INDEX lock"));
                };
            } else {
                break;
            }
        }
        Ok((algorithm, lock))
    }

    /// Parses Go's standalone region-management grammar:
    /// `SPLIT [REGION FOR] [PARTITION] TABLE name [PARTITION (...)] [INDEX name]`
    /// followed by either `BY` point tuples or `BETWEEN` bounds.
    pub(crate) fn parse_split_region(&mut self) -> PResult<SplitRegionStmt> {
        self.expect_kw("SPLIT")?;
        let region_for = if self.is_kw("REGION") {
            self.bump();
            self.expect_kw("FOR")?;
            true
        } else {
            false
        };
        let partition_syntax = if self.is_kw("PARTITION") {
            self.bump();
            true
        } else {
            false
        };
        self.expect_kw("TABLE")?;
        let table = self.parse_name_path()?;
        let partitions = if self.is_kw("PARTITION") {
            self.bump();
            self.parse_split_partition_names()?
        } else {
            Vec::new()
        };
        let index = if self.is_kw("INDEX") {
            self.bump();
            Some(self.parse_name_or_keyword()?)
        } else {
            None
        };
        let option = self.parse_split_option()?;
        Ok(SplitRegionStmt {
            region_for,
            partition_syntax,
            table,
            partitions,
            index,
            option,
        })
    }

    /// Parses the exact parenthesized partition-name list used by standalone
    /// `SPLIT ... TABLE`.  Go's hand parser accepts identifier-like names in
    /// this one grammar, unlike the stricter DML table-reference partition
    /// helper.
    fn parse_split_partition_names(&mut self) -> PResult<Vec<String>> {
        self.expect_op("(")?;
        let mut names = vec![self.parse_name_or_keyword()?];
        while self.is_op(",") {
            self.bump();
            names.push(self.parse_name_or_keyword()?);
        }
        self.expect_op(")")?;
        Ok(names)
    }

    /// Parses the common Go `SplitOption` grammar without converting a split
    /// tuple into `Expr::Row`: the AST's own restore has a distinct ordinary
    /// parenthesized tuple representation.
    fn parse_split_option(&mut self) -> PResult<SplitOption> {
        self.parse_split_option_shape(true, false)
    }

    fn parse_create_table_split_option(&mut self) -> PResult<SplitOption> {
        self.parse_split_option_shape(false, true)
    }

    fn parse_split_option_shape(
        &mut self,
        allow_empty_bounds: bool,
        require_parenthesized: bool,
    ) -> PResult<SplitOption> {
        if self.is_kw("BY") {
            self.bump();
            let mut points = vec![self.parse_split_tuple(false, require_parenthesized)?];
            while self.is_op(",") {
                self.bump();
                points.push(self.parse_split_tuple(false, require_parenthesized)?);
            }
            Ok(SplitOption::By(points))
        } else if self.is_kw("BETWEEN") {
            self.bump();
            let lower = self.parse_split_tuple(allow_empty_bounds, require_parenthesized)?;
            self.expect_kw("AND")?;
            let upper = self.parse_split_tuple(allow_empty_bounds, require_parenthesized)?;
            self.expect_kw("REGIONS")?;
            let regions = self.parse_split_region_count()?;
            Ok(SplitOption::Between {
                lower,
                upper,
                regions,
            })
        } else {
            Ok(SplitOption::Between {
                lower: Vec::new(),
                upper: Vec::new(),
                regions: 0,
            })
        }
    }

    fn parse_split_region_count(&mut self) -> PResult<i64> {
        let negative = if self.is_op("-") {
            self.bump();
            true
        } else {
            if self.is_op("+") {
                self.bump();
            }
            false
        };
        let token = self.peek().clone();
        if token.kind != TokenKind::IntLit {
            return Err(self.err_here("expected split region count"));
        }
        self.bump();
        let magnitude = token
            .text
            .parse::<u64>()
            .map_err(|_| self.err_here("split region count out of range"))?;
        if negative {
            if magnitude == (i64::MAX as u64) + 1 {
                Ok(i64::MIN)
            } else {
                i64::try_from(magnitude)
                    .map(|value| -value)
                    .map_err(|_| self.err_here("split region count out of range"))
            }
        } else {
            i64::try_from(magnitude).map_err(|_| self.err_here("split region count out of range"))
        }
    }

    /// Parses one split-key tuple.  Empty tuples are valid only in `BETWEEN`
    /// bounds; `BY ()` is a genuine parser error in Go.
    fn parse_split_tuple(
        &mut self,
        allow_empty: bool,
        require_parenthesized: bool,
    ) -> PResult<Vec<tidb_ast::Expr>> {
        if !self.is_op("(") {
            if require_parenthesized {
                return Err(self.err_here("expected parenthesized split key values"));
            }
            return Ok(vec![self.parse_expr(prec::NONE)?]);
        }
        self.expect_op("(")?;
        if self.is_op(")") {
            if !allow_empty {
                return Err(self.err_here("expected split key value"));
            }
            self.bump();
            return Ok(Vec::new());
        }
        let mut values = vec![self.parse_expr(prec::NONE)?];
        while self.is_op(",") {
            self.bump();
            values.push(self.parse_expr(prec::NONE)?);
        }
        self.expect_op(")")?;
        Ok(values)
    }

    /// Parses Go's core `CREATE VIEW` production:
    ///
    /// `CREATE [OR REPLACE] [ALGORITHM = {UNDEFINED|MERGE|TEMPTABLE}]`
    /// `VIEW name [(columns)] AS [(] query [)] [WITH [LOCAL|CASCADED] CHECK OPTION]`.
    ///
    /// Go's view AST always carries and restores the default `CURRENT_USER`
    /// definer and `SQL SECURITY DEFINER`; explicit clauses replace those
    /// typed payloads. Execution still has an explicit unsupported boundary
    /// because the seed catalog has no privilege-aware view semantics.
    pub(crate) fn parse_create_view(&mut self) -> PResult<CreateViewStmt> {
        self.expect_kw("CREATE")?;
        let or_replace = if self.is_kw("OR") {
            self.bump();
            self.expect_kw("REPLACE")?;
            true
        } else {
            false
        };
        let algorithm = if self.is_kw("ALGORITHM") {
            self.bump();
            self.expect_op("=")?;
            // This intentionally mirrors Go's hand parser: any algorithm
            // spelling other than MERGE/TEMPTABLE is stored as the
            // UNDEFINED default and therefore restores as UNDEFINED.
            if self.is_kw("MERGE") {
                self.bump();
                ViewAlgorithm::Merge
            } else if self.is_kw("TEMPTABLE") {
                self.bump();
                ViewAlgorithm::Temptable
            } else {
                self.bump();
                ViewAlgorithm::Undefined
            }
        } else {
            ViewAlgorithm::Undefined
        };
        let definer = if self.is_kw("DEFINER") {
            self.bump();
            self.expect_op("=")?;
            self.parse_user_spec()?
        } else {
            UserSpec {
                current_user: true,
                user: String::new(),
                host: String::new(),
            }
        };
        let security = if self.is_kw("SQL") {
            self.bump();
            self.expect_kw("SECURITY")?;
            if self.is_kw("INVOKER") {
                self.bump();
                ViewSecurity::Invoker
            } else {
                self.expect_kw("DEFINER")?;
                ViewSecurity::Definer
            }
        } else {
            ViewSecurity::Definer
        };
        self.expect_kw("VIEW")?;
        let name = self.parse_name_path()?;
        let mut columns = Vec::new();
        if self.is_op("(") {
            self.bump();
            loop {
                columns.push(self.parse_name_or_keyword()?);
                if self.is_op(",") {
                    self.bump();
                } else {
                    break;
                }
            }
            self.expect_op(")")?;
        }
        self.expect_kw("AS")?;
        let query_start = self.peek().offset;
        // A sole `AS (SELECT ...)` keeps its outer braces in the view AST,
        // while `(SELECT ...) UNION (SELECT ...)` is a set operation whose
        // braces belong to the individual terms. Parse the first parenthesized
        // term here so the same typed `SetOprStmt` can represent both forms
        // without dropping or inventing a second pair of braces.
        let (query, query_parenthesized) = if self.is_op("(") {
            self.bump();
            let inner = if self.is_kw("WITH") {
                self.parse_with_select()?
            } else {
                self.parse_select_or_setopr()?
            };
            self.expect_op(")")?;
            if self.peek_set_op().is_some() {
                let first = match inner {
                    QueryStmt::Select(select) => SetOprTermBody::Select(select),
                    QueryStmt::SetOpr(setopr) => SetOprTermBody::Nested(setopr),
                };
                (
                    QueryStmt::SetOpr(Box::new(self.parse_setopr_rest(true, first)?)),
                    false,
                )
            } else {
                (inner, true)
            }
        } else {
            let query = if self.is_kw("WITH") {
                self.parse_with_select()?
            } else {
                self.parse_select_or_setopr()?
            };
            (query, false)
        };
        let query_end = self.peek().offset;
        let mut query = tidb_ast::NodeBox::new(query);
        if query_end > query_start {
            query.set_text(
                None,
                self.source[query_start..query_end]
                    .trim()
                    .as_bytes()
                    .to_vec(),
            );
        }
        let check_option = if self.is_kw("WITH") {
            self.bump();
            let local = if self.is_kw("LOCAL") {
                self.bump();
                true
            } else {
                if self.is_kw("CASCADED") {
                    self.bump();
                }
                false
            };
            self.expect_kw("CHECK")?;
            self.expect_kw("OPTION")?;
            if local {
                ViewCheckOption::Local
            } else {
                ViewCheckOption::Cascaded
            }
        } else {
            ViewCheckOption::Cascaded
        };
        Ok(CreateViewStmt {
            or_replace,
            algorithm,
            definer,
            security,
            name,
            columns,
            query,
            query_parenthesized,
            check_option,
        })
    }
    /// Direct translation of Go's mandatory global-temporary `ON COMMIT`
    /// tail. Go's AST carries only the DELETE/PRESERVE boolean, so preserving
    /// it here is sufficient for byte-identical restore while executor support
    /// remains intentionally absent.
    fn parse_global_temporary_on_commit(
        &mut self,
        temporary: CreateTableTemporary,
    ) -> PResult<bool> {
        if temporary != CreateTableTemporary::Global {
            if self.is_kw("ON") {
                return Err(self.err_here("ON COMMIT requires GLOBAL TEMPORARY TABLE"));
            }
            return Ok(false);
        }
        self.expect_kw("ON")?;
        self.expect_kw("COMMIT")?;
        if self.is_kw("DELETE") {
            self.bump();
            self.expect_kw("ROWS")?;
            Ok(true)
        } else if self.is_kw("PRESERVE") {
            self.bump();
            self.expect_kw("ROWS")?;
            Ok(false)
        } else {
            Err(self.err_here("expected DELETE or PRESERVE after ON COMMIT"))
        }
    }

    /// Parses `ALTER TABLE name` followed by an ordered list of actions:
    /// `ADD [COLUMN] col
    /// ... [FIRST|AFTER col]`, `DROP [COLUMN] name`, `DROP PARTITION [IF
    /// EXISTS] name [, name ...]`, `MODIFY [COLUMN] col
    /// ... [FIRST|AFTER col]`, `DROP {INDEX|KEY} [IF EXISTS] name`, `CHANGE [COLUMN] old_name col ...
    /// [FIRST|AFTER col]`, `RENAME [TO|AS] name`, `EXCHANGE PARTITION name
    /// WITH TABLE name [WITH|WITHOUT VALIDATION]`, `ADD [CONSTRAINT [name]]
    /// {INDEX|KEY} [name] (parts...) [COMMENT|GLOBAL|INVISIBLE|WHERE]`, `ADD [CONSTRAINT [name]] UNIQUE
    /// [INDEX|KEY] [name] (cols)`, or `ADD [CONSTRAINT [name]] CHECK (expr)
    /// [[NOT] ENFORCED]`, `SET [HYPO] TIFLASH REPLICA count [LOCATION LABELS
    /// 'label', ...]`, `COMPACT [PARTITION name [, name ...]] [TIFLASH|TIKV
    /// [REPLICA]]`, or `ADD PARTITION [IF NOT EXISTS] [NO_WRITE_TO_BINLOG]
    /// {PARTITIONS count | (typed definitions)}` — the only actions modelled; every other
    /// form (`ADD [CONSTRAINT] FOREIGN KEY`, ...) is an honest
    /// `ParseError`.
    pub(crate) fn parse_alter_table_statement(&mut self) -> PResult<Stmt> {
        self.expect_kw("ALTER")?;
        if self.is_kw("IGNORE") {
            self.bump();
        }
        self.expect_kw("TABLE")?;
        let name = self.parse_table_name()?;
        if self.is_kw("ANALYZE") {
            return self.parse_alter_analyze_partition(name);
        }
        Ok(Stmt::Ddl(tidb_ast::NodeBox::new(
            tidb_ast::DdlStmt::AlterTable(Box::new(self.parse_alter_table_after_name(name)?)),
        )))
    }

    fn parse_alter_analyze_partition(&mut self, table: Vec<String>) -> PResult<Stmt> {
        self.expect_kw("ANALYZE")?;
        self.expect_kw("PARTITION")?;
        let partitions = self.parse_ident_like_name_list()?;
        let target = if self.is_kw("INDEX") {
            self.bump();
            tidb_ast::AnalyzeTarget::Index(self.parse_ident_like_name_list()?)
        } else {
            tidb_ast::AnalyzeTarget::Default
        };
        let options = self.parse_analyze_options()?;
        Ok(Stmt::Admin(tidb_ast::NodeBox::new(
            AdminStmt::AnalyzeTable(Box::new(AnalyzeTableStmt {
                tables: vec![table],
                partitions,
                no_write_to_binlog: false,
                target,
                options,
            })),
        )))
    }

    fn parse_alter_table_after_name(&mut self, name: Vec<String>) -> PResult<AlterTableStmt> {
        let mut actions = Vec::new();
        let Some(action) = self.parse_alter_table_action()? else {
            return Ok(AlterTableStmt { name, actions });
        };
        let compact = matches!(action, AlterTableAction::Compact { .. });
        let terminal = matches!(
            action,
            AlterTableAction::Partition(
                AlterPartitionAction::RemovePartitioning | AlterPartitionAction::Repartition(_)
            )
        );
        actions.push(action);

        // Go diverts COMPACT to CompactTableStmt immediately after parsing
        // the common ALTER TABLE name. It can own internal partition-list
        // commas, but cannot participate in AlterTableStmt.Specs.
        if compact || terminal {
            return Ok(AlterTableStmt { name, actions });
        }

        loop {
            if self.is_op(",") {
                self.bump();
                // REMOVE PARTITIONING is Go's terminal AlterTablePartitionOpt
                // and is rejected after a comma by the source grammar.
                if self.is_kw("REMOVE") {
                    return Err(self.err_here("REMOVE PARTITIONING must not follow a comma"));
                }
                if self.is_kw("PARTITION") && self.is_kw_at(1, "BY") {
                    return Err(self.err_here("PARTITION BY must not follow a comma"));
                }
                if self.is_kw("COMPACT") {
                    return Err(self.err_here("COMPACT must be the only ALTER TABLE action"));
                }
                let Some(action) = self.parse_alter_table_action()? else {
                    return Err(self.err_here("expected ALTER TABLE action after comma"));
                };
                let terminal = matches!(
                    action,
                    AlterTableAction::Partition(
                        AlterPartitionAction::RemovePartitioning
                            | AlterPartitionAction::Repartition(_)
                    )
                );
                actions.push(action);
                if terminal {
                    break;
                }
                continue;
            }

            // Go permits this terminal partition option without the ordinary
            // comma separator and restores it with one space.
            if self.is_kw("REMOVE") || (self.is_kw("PARTITION") && self.is_kw_at(1, "BY")) {
                let action = self
                    .parse_alter_table_action()?
                    .expect("REMOVE was recognized as an ALTER TABLE action");
                actions.push(action);
            } else if self.is_kw("PARTITION") {
                // Go's AlterTableSpecList accepts the partition-option spec
                // directly after another spec without a comma. Its restore
                // later treats this as a separate placement-only spec.
                let action = self
                    .parse_alter_table_action()?
                    .expect("PARTITION was recognized as an ALTER TABLE action");
                actions.push(action);
            }
            break;
        }

        Ok(AlterTableStmt { name, actions })
    }

    /// Parses exactly one typed `ALTER TABLE` action. `None` is a strict
    /// no-op so the statement-level loop owns spec separators and ordering.
    fn parse_alter_table_action(&mut self) -> PResult<Option<AlterTableAction>> {
        if self.is_masking_policy_alter_action() {
            return self.parse_masking_policy_alter_action().map(Some);
        }
        if self.is_kw("ENABLE") || self.is_kw("DISABLE") {
            let enabled = self.is_kw("ENABLE");
            self.bump();
            self.expect_kw("KEYS")?;
            return Ok(Some(AlterTableAction::SetKeysEnabled(enabled)));
        }
        // Go's `parseAlterTableSpec` owns WITH/WITHOUT VALIDATION as a
        // standalone ordered specification. Keep the lookahead exact: a
        // bare WITH/WITHOUT is not an ALTER action and must remain a parse
        // error rather than being consumed by a wider branch.
        if self.is_kw("WITH") || self.is_kw("WITHOUT") {
            let with = self.is_kw("WITH");
            self.bump();
            self.expect_kw("VALIDATION")?;
            return Ok(Some(if with {
                AlterTableAction::WithValidation
            } else {
                AlterTableAction::WithoutValidation
            }));
        }
        if let Some(action) = partition::parse_alter_partition_action(self)? {
            return Ok(Some(AlterTableAction::Partition(action)));
        }
        let action = if let Some(action) = alter::lock::parse(self)? {
            action
        } else if let Some(action) = alter::cache::parse(self)? {
            action
        } else if let Some(action) = alter::ttl::parse(self)? {
            action
        } else if let Some(action) = alter::auto_id_options::parse(self)? {
            action
        } else if self.is_kw("FORCE") {
            self.bump();
            AlterTableAction::Force
        } else if self.is_kw("ALGORITHM") {
            self.bump();
            self.accept_optional_equals();
            let token = self.peek().clone();
            if !matches!(token.kind, TokenKind::Ident | TokenKind::Keyword) {
                return Err(self.err_here("expected ALTER TABLE algorithm"));
            }
            self.bump();
            let algorithm = match token.text.to_ascii_uppercase().as_str() {
                "DEFAULT" => AlterTableAlgorithm::Default,
                "COPY" => AlterTableAlgorithm::Copy,
                "INPLACE" => AlterTableAlgorithm::Inplace,
                "INSTANT" => AlterTableAlgorithm::Instant,
                _ => return Err(self.err_here("unknown ALTER TABLE algorithm")),
            };
            AlterTableAction::Algorithm(algorithm)
        } else if self.is_kw("READ") {
            self.bump();
            let read_only = if self.is_kw("ONLY") {
                self.bump();
                true
            } else {
                self.expect_kw("WRITE")?;
                false
            };
            AlterTableAction::ReadOnly(read_only)
        } else if self.is_kw("SECONDARY_LOAD") || self.is_kw("SECONDARY_UNLOAD") {
            let load = self.is_kw("SECONDARY_LOAD");
            self.bump();
            self.warn(if load {
                "The SECONDARY_LOAD clause is parsed but not implement yet."
            } else {
                "The SECONDARY_UNLOAD VALIDATION clause is parsed but not implement yet."
            });
            AlterTableAction::SecondaryLoad(load)
        } else if self.is_kw("IMPORT") || self.is_kw("DISCARD") {
            let import = self.is_kw("IMPORT");
            self.bump();
            self.expect_kw("TABLESPACE")?;
            self.warn(if import {
                "The IMPORT TABLESPACE clause is parsed but ignored by all storage engines."
            } else {
                "The DISCARD TABLESPACE clause is parsed but ignored by all storage engines."
            });
            AlterTableAction::TablespaceImport(import)
        } else if self.is_kw("CONVERT") {
            AlterTableAction::ConvertCharacterSet {
                charset: self.parse_alter_convert_character_set()?,
                collation: self.parse_optional_alter_collation()?,
            }
        } else if self.is_kw("ATTRIBUTES") {
            // Direct Go `parseAlterTableOptions` transition. ATTRIBUTES is
            // its own `AlterTableAttributes` spec: it accepts an optional
            // equals sign followed by exactly DEFAULT or a string literal.
            self.bump();
            if self.is_op("=") {
                self.bump();
            }
            let attributes = if self.is_kw("DEFAULT") {
                self.bump();
                None
            } else {
                let token = self.peek().clone();
                if token.kind != TokenKind::Str {
                    return Err(self.err_here("expected ATTRIBUTES string literal or DEFAULT"));
                }
                self.bump();
                Some(decode_string(&token.text))
            };
            AlterTableAction::SetAttributes(tidb_ast::AttributesSpec { attributes })
        } else if self.is_kw("STATS_OPTIONS") {
            // Go gives STATS_OPTIONS its own AlterTableStatsOptions spec and
            // StatsOptionsSpec child rather than treating it as a table
            // option. Preserve the optional equals sign and the closed
            // DEFAULT-or-string payload.
            self.bump();
            if self.is_op("=") {
                self.bump();
            }
            let options = if self.is_kw("DEFAULT") {
                self.bump();
                None
            } else {
                let token = self.peek().clone();
                if token.kind != TokenKind::Str {
                    return Err(self.err_here("expected STATS_OPTIONS string literal or DEFAULT"));
                }
                self.bump();
                Some(decode_string(&token.text))
            };
            AlterTableAction::SetStatsOptions(tidb_ast::StatsOptionsSpec { options })
        } else if let Some(first_option) = self.parse_table_option()? {
            // Go delegates every remaining option to the shared
            // `parseTableOption` loop. One parser owns CREATE and ALTER table
            // option boundaries; no second selector list can drift.
            let mut options = vec![first_option];
            while let Some(option) = self.parse_table_option()? {
                options.push(option);
            }
            AlterTableAction::SetTableOptions { options }
        } else if self.is_kw("ADD") && self.is_kw_at(1, "STATS_EXTENDED") {
            self.bump();
            self.bump();
            let if_not_exists = self.parse_if_not_exists()?;
            let name = self.parse_ident_like_name()?;
            let stats_type = if self.is_kw("CARDINALITY") {
                self.bump();
                tidb_ast::ExtendedStatsType::Cardinality
            } else if self.is_kw("DEPENDENCY") {
                self.bump();
                tidb_ast::ExtendedStatsType::Dependency
            } else if self.is_kw("CORRELATION") {
                self.bump();
                tidb_ast::ExtendedStatsType::Correlation
            } else {
                return Err(self.err_here(
                    "expected CARDINALITY, DEPENDENCY, or CORRELATION after STATS_EXTENDED name",
                ));
            };
            self.expect_op("(")?;
            let mut columns = vec![self.parse_ident_like_name()?];
            while self.is_op(",") {
                self.bump();
                columns.push(self.parse_ident_like_name()?);
            }
            self.expect_op(")")?;
            AlterTableAction::AddStatistics {
                if_not_exists,
                name,
                stats_type,
                columns,
            }
        } else if self.is_kw("DROP") && self.is_kw_at(1, "STATS_EXTENDED") {
            self.bump();
            self.bump();
            AlterTableAction::DropStatistics {
                if_exists: self.parse_if_exists()?,
                name: self.parse_ident_like_name()?,
            }
        } else if self.is_kw("ADD") {
            self.bump();
            if self.is_kw("PARTITION") {
                unreachable!("partition action was consumed by ddl_partition")
            } else {
                let saw_constraint = self.is_kw("CONSTRAINT");
                let constraint_name = if saw_constraint {
                    self.bump();
                    self.try_parse_name()?
                } else {
                    None
                };
                if self.is_kw("PRIMARY") {
                    self.bump();
                    self.expect_kw("KEY")?;
                    let inline_name = self.try_parse_name()?;
                    let name = constraint_name.or(inline_name);
                    let is_empty_index = name.as_deref() == Some("");
                    AlterTableAction::AddIndexConstraint(self.parse_index_constraint(
                        IndexConstraintKind::PrimaryKey,
                        false,
                        name,
                        is_empty_index,
                        true,
                    )?)
                } else if self.is_kw("UNIQUE") {
                    self.bump();
                    if self.is_kw("KEY") || self.is_kw("INDEX") {
                        self.bump();
                    }
                    let kind = IndexConstraintKind::Unique;
                    let inline_name = self.try_parse_name()?;
                    let name = constraint_name.or(inline_name);
                    let is_empty_index = name.as_deref() == Some("");
                    AlterTableAction::AddIndexConstraint(self.parse_index_constraint(
                        kind,
                        false,
                        name,
                        is_empty_index,
                        true,
                    )?)
                } else if self.is_kw("FULLTEXT") {
                    self.bump();
                    if self.is_kw("INDEX") || self.is_kw("KEY") {
                        self.bump();
                    }
                    let inline_name = self.try_parse_name()?;
                    let name = constraint_name.or(inline_name);
                    AlterTableAction::AddIndexConstraint(self.parse_index_constraint(
                        IndexConstraintKind::Fulltext,
                        false,
                        name,
                        false,
                        false,
                    )?)
                } else if self.is_kw("VECTOR") {
                    self.bump();
                    self.expect_kw("INDEX")?;
                    let if_not_exists = self.parse_if_not_exists()?;
                    let inline_name = self.try_parse_name()?;
                    let name = constraint_name.or(inline_name);
                    let is_empty_index = name.as_deref() == Some("");
                    AlterTableAction::AddIndexConstraint(self.parse_index_constraint(
                        IndexConstraintKind::Vector,
                        if_not_exists,
                        name,
                        is_empty_index,
                        true,
                    )?)
                } else if self.is_kw("COLUMNAR") {
                    self.bump();
                    self.expect_kw("INDEX")?;
                    let if_not_exists = self.parse_if_not_exists()?;
                    let inline_name = self.try_parse_name()?;
                    let name = constraint_name.or(inline_name);
                    let is_empty_index = name.as_deref() == Some("");
                    AlterTableAction::AddIndexConstraint(self.parse_index_constraint(
                        IndexConstraintKind::Columnar,
                        if_not_exists,
                        name,
                        is_empty_index,
                        true,
                    )?)
                } else if self.is_kw("INDEX") || self.is_kw("KEY") {
                    // Same Go ConstraintIndex normalization as CREATE TABLE.
                    let kind = IndexConstraintKind::Index;
                    self.bump();
                    let if_not_exists = self.parse_if_not_exists()?;
                    let inline_name = self.try_parse_name()?;
                    let name = constraint_name.or(inline_name);
                    let is_empty_index = name.as_deref() == Some("");
                    AlterTableAction::AddIndexConstraint(self.parse_index_constraint(
                        kind,
                        if_not_exists,
                        name,
                        is_empty_index,
                        true,
                    )?)
                } else if self.is_kw("FOREIGN") {
                    self.bump();
                    self.expect_kw("KEY")?;
                    let if_not_exists = self.parse_if_not_exists()?;
                    let inline_name = self.try_parse_name()?;
                    AlterTableAction::AddForeignKey(self.parse_foreign_key_constraint(
                        constraint_name.or(inline_name),
                        if_not_exists,
                    )?)
                } else if self.is_kw("CHECK") {
                    self.bump();
                    let (check, injected_not_null) =
                        self.parse_check_constraint(constraint_name, false)?;
                    debug_assert!(!injected_not_null);
                    AlterTableAction::AddCheck(check)
                } else if saw_constraint {
                    return Err(self.err_here("unsupported ALTER TABLE ADD CONSTRAINT kind"));
                } else {
                    if self.is_kw("COLUMN") {
                        self.bump();
                    }
                    let if_not_exists = self.parse_if_not_exists()?;
                    // Go's `parseAlterAdd` accepts the table-element list
                    // form after either `ADD` or `ADD COLUMN`. Keep grouped
                    // columns and table constraints as their own action so
                    // restore retains parentheses and Go's column-then-
                    // constraint ordering.
                    if self.is_op("(") {
                        self.bump();
                        let mut columns = Vec::new();
                        let mut constraints = Vec::new();
                        loop {
                            if self.is_table_constraint_start() {
                                constraints.push(self.parse_table_constraint()?);
                            } else {
                                columns.push(self.parse_column_def()?);
                            }
                            if self.is_op(",") {
                                self.bump();
                                continue;
                            }
                            break;
                        }
                        self.expect_op(")")?;
                        AlterTableAction::AddColumns {
                            if_not_exists,
                            columns,
                            constraints,
                        }
                    } else {
                        let column = self.parse_column_def()?;
                        let position = self.parse_column_position()?;
                        AlterTableAction::AddColumn {
                            if_not_exists,
                            column,
                            position,
                        }
                    }
                }
            }
        } else if self.is_kw("DROP") {
            self.bump();
            if let Some(action) = alter::drop_foreign_key::parse(self)? {
                action
            } else if let Some(action) = alter::drop_check::parse(self)? {
                action
            } else if let Some(action) = alter::drop_primary_key::parse(self)? {
                action
            } else if self.is_kw("PARTITION") {
                unreachable!("partition action was consumed by ddl_partition")
            } else if self.is_kw("INDEX") || self.is_kw("KEY") {
                self.bump();
                let if_exists = if self.is_kw("IF") {
                    self.bump();
                    self.expect_kw("EXISTS")?;
                    true
                } else {
                    false
                };
                AlterTableAction::DropIndex {
                    if_exists,
                    name: self.parse_ident_like_name()?,
                }
            } else {
                if self.is_kw("COLUMN") {
                    self.bump();
                }
                let if_exists = self.parse_if_exists()?;
                let name = self.parse_ident_like_name()?;
                // MySQL's optional RESTRICT/CASCADE suffix is accepted by
                // Go and intentionally omitted by AlterTableSpec.Restore.
                if self.is_kw("RESTRICT") || self.is_kw("CASCADE") {
                    self.bump();
                }
                AlterTableAction::DropColumn { if_exists, name }
            }
        } else if let Some(action) = alter::index_visibility::parse(self)? {
            action
        } else if let Some(action) = alter::check::parse(self)? {
            action
        } else if let Some(action) = alter::column_default::parse(self)? {
            action
        } else if self.is_kw("MODIFY") {
            self.bump();
            if self.is_kw("COLUMN") {
                self.bump();
            }
            let if_exists = self.parse_if_exists()?;
            let column = self.parse_column_def()?;
            let position = self.parse_column_position()?;
            AlterTableAction::ModifyColumn {
                if_exists,
                column,
                position,
            }
        } else if self.is_kw("CHANGE") {
            self.bump();
            if self.is_kw("COLUMN") {
                self.bump();
            }
            let if_exists = self.parse_if_exists()?;
            let old_name = self.parse_column_name_path()?;
            let column = self.parse_column_def()?;
            let position = self.parse_column_position()?;
            AlterTableAction::ChangeColumn {
                if_exists,
                old_name,
                column,
                position,
            }
        } else if let Some(action) = alter::rename_column::parse(self)? {
            action
        } else if let Some(action) = alter::rename_index::parse(self)? {
            action
        } else if self.is_kw("RENAME") {
            self.bump();
            if self.is_kw("TO") || self.is_kw("AS") || self.is_op("=") {
                self.bump();
            }
            AlterTableAction::RenameTable {
                new_name: self.parse_table_name()?,
            }
        } else if self.is_kw("ORDER") {
            self.bump();
            self.expect_kw("BY")?;
            let mut items = Vec::new();
            loop {
                let column = self.parse_column_name_path()?;
                let desc = if self.is_kw("DESC") {
                    self.bump();
                    true
                } else {
                    if self.is_kw("ASC") {
                        self.bump();
                    }
                    false
                };
                items.push(AlterOrderItem { column, desc });
                if !self.is_op(",") {
                    break;
                }
                self.bump();
            }
            AlterTableAction::OrderByColumns { items }
        } else if self.is_kw("SET")
            && (self.is_kw_at(1, "TIFLASH")
                || (self.is_kw_at(1, "HYPO") && self.is_kw_at(2, "TIFLASH")))
        {
            // Direct translation of Go `parseAlterTableSpec`'s TiFlash
            // branch (`pkg/parser/ddl_alter_parser.go`): HYPO affects the
            // AST payload but Go restore intentionally omits it.
            self.bump(); // SET
            let hypo = if self.is_kw("HYPO") {
                self.bump();
                true
            } else {
                false
            };
            self.expect_kw("TIFLASH")?;
            self.expect_kw("REPLICA")?;
            let token = self.peek().clone();
            if token.kind != TokenKind::IntLit {
                return Err(self.err_here("expected TiFlash replica count"));
            }
            self.bump();
            let count = token
                .text
                .parse::<u64>()
                .map_err(|_| self.err_here("TiFlash replica count out of range"))?;
            let mut labels = Vec::new();
            if self.is_kw("LOCATION") {
                self.bump();
                self.expect_kw("LABELS")?;
                loop {
                    if self.peek().kind != TokenKind::Str {
                        return Err(self.err_here("expected TiFlash location label"));
                    }
                    labels.push(crate::decode_string(&self.bump().text));
                    if self.is_op(",") {
                        self.bump();
                    } else {
                        break;
                    }
                }
            }
            AlterTableAction::SetTiFlashReplica {
                hypo,
                count,
                labels,
            }
        } else if self.is_kw("COMPACT") {
            // Go's `parseCompactTableStmt` is a separate Go AST node, but
            // its input starts after the same ALTER TABLE name boundary and
            // has an entirely self-contained typed payload.
            self.bump();
            let mut partitions = Vec::new();
            if self.is_kw("PARTITION") {
                self.bump();
                partitions.push(self.parse_ident_like_name()?);
                while self.is_op(",") {
                    self.bump();
                    partitions.push(self.parse_ident_like_name()?);
                }
            }
            let replica_kind = if self.is_kw("TIFLASH") {
                self.bump();
                if self.is_kw("REPLICA") {
                    self.bump();
                }
                CompactReplicaKind::TiFlash
            // `TIKV` is an identifier-class token in this lexer (unlike
            // Go's hand-parser `IsKeyword("TIKV")` abstraction), so match
            // its token text at this grammar-owned boundary.
            } else if self.peek().kind == TokenKind::Ident
                && self.peek().end_offset - self.peek().offset == 4
                && self.peek().text.eq_ignore_ascii_case("TIKV")
            {
                self.bump();
                if self.is_kw("REPLICA") {
                    self.bump();
                }
                CompactReplicaKind::TiKv
            } else {
                CompactReplicaKind::All
            };
            AlterTableAction::Compact {
                partitions,
                replica_kind,
            }
        } else if self.is_kw("SPLIT") {
            // Direct translation of Go's `parseSplitRegionSpec`: ALTER TABLE
            // owns the table name, while the shared split payload owns the
            // record/index target and split-key form.
            self.bump();
            let target = if self.is_kw("TABLE") {
                self.bump();
                SplitTarget::Table
            } else if self.is_kw("PRIMARY") {
                self.bump();
                self.expect_kw("KEY")?;
                SplitTarget::PrimaryKey
            } else if self.is_kw("INDEX") {
                self.bump();
                if self.peek().kind == TokenKind::Str || !crate::is_ident_like_name(self.peek()) {
                    return Err(self.err_here("expected split index name"));
                }
                SplitTarget::Index(self.parse_ident_like_name()?)
            } else if self.is_kw("REGION") {
                // Go accepts `SPLIT REGION BETWEEN ...` as the implicit
                // table-level spelling.
                self.bump();
                SplitTarget::Table
            } else {
                SplitTarget::Table
            };
            AlterTableAction::SplitRegion {
                target,
                option: self.parse_split_option()?,
            }
        } else {
            return Ok(None);
        };
        Ok(Some(action))
    }

    /// Parses Go's `CONVERT TO { CHARACTER SET | CHARSET | CHAR SET }
    /// charset` branch. The Go AST's `Default` bit is represented by `None`
    /// so restore can produce `CHARACTER SET DEFAULT` without inventing a
    /// charset name.
    fn parse_alter_convert_character_set(&mut self) -> PResult<Option<String>> {
        self.expect_kw("CONVERT")?;
        self.expect_kw("TO")?;
        if self.is_kw("CHARACTER") || self.is_kw("CHAR") {
            self.bump();
            self.expect_kw("SET")?;
        } else {
            self.expect_kw("CHARSET")?;
        }
        if self.is_kw("DEFAULT") {
            self.bump();
            Ok(None)
        } else {
            Ok(Some(self.parse_alter_charset_name()?))
        }
    }

    fn parse_optional_alter_collation(&mut self) -> PResult<Option<String>> {
        if !self.is_kw("COLLATE") {
            return Ok(None);
        }
        self.bump();
        let raw = self.parse_table_option_word()?;
        let collation =
            canonical_collation(&raw).ok_or_else(|| self.err_here("unknown collation"))?;
        Ok(Some(collation.to_ascii_uppercase()))
    }

    /// Go's `parseTableOption` validates table charset names against
    /// `charset.GetCharsetInfo`, unlike a general identifier slot. Reuse the
    /// generated lexer registry so aliases (notably `utf8mb3`) canonicalize
    /// through the same source-derived table before AST restore.
    fn parse_alter_charset_name(&mut self) -> PResult<String> {
        let raw = self.parse_table_option_word()?;
        canonical_charset(&raw)
            .map(|charset| charset.to_ascii_uppercase())
            .ok_or_else(|| self.err_here("unknown character set"))
    }

    /// Parses `RENAME TABLE old1 TO new1 [, old2 TO new2 ...]` — a
    /// different top-level statement from `ALTER TABLE ... RENAME`, though
    /// both rename a table.
    pub(crate) fn parse_rename_table(&mut self) -> PResult<RenameTableStmt> {
        self.expect_kw("RENAME")?;
        self.expect_kw("TABLE")?;
        let mut pairs = Vec::new();
        loop {
            let old = self.parse_table_name()?;
            self.expect_kw("TO")?;
            let new = self.parse_table_name()?;
            pairs.push((old, new));
            if self.is_op(",") {
                self.bump();
                continue;
            }
            break;
        }
        Ok(RenameTableStmt { pairs })
    }

    /// Parses `DROP TABLE[S] [IF EXISTS] name [, name2, ...] [RESTRICT |
    /// CASCADE]` — the plural spelling is a Go grammar alias that restores as
    /// singular; the trailing modifier is accepted and discarded (see
    /// [`tidb_ast::DropTableStmt`]'s own doc for why).
    pub(crate) fn parse_drop_table(&mut self) -> PResult<DropTableStmt> {
        self.expect_kw("DROP")?;
        // Optional `[GLOBAL] TEMPORARY` modifier before `TABLE`.
        let temporary = if self.is_kw("GLOBAL") {
            self.bump();
            self.expect_kw("TEMPORARY")?;
            DropTemporary::Global
        } else if self.is_kw("TEMPORARY") {
            self.bump();
            DropTemporary::Local
        } else {
            DropTemporary::None
        };
        if self.is_kw("TABLE") || self.is_kw("TABLES") {
            self.bump();
        } else {
            return Err(self.err_here("expected keyword TABLE"));
        }
        let if_exists = if self.is_kw("IF") {
            self.bump();
            self.expect_kw("EXISTS")?;
            true
        } else {
            false
        };
        let mut names = Vec::new();
        loop {
            names.push(self.parse_table_name()?);
            if self.is_op(",") {
                self.bump();
                continue;
            }
            break;
        }
        if self.is_kw("RESTRICT") || self.is_kw("CASCADE") {
            self.bump();
        }
        Ok(DropTableStmt {
            temporary,
            if_exists,
            names,
        })
    }

    /// Parses an optional `FIRST` / `AFTER col` column-position suffix,
    /// `Default` (leave/append at the current end) if neither is written.
    fn parse_column_position(&mut self) -> PResult<ColumnPosition> {
        if self.is_kw("FIRST") {
            self.bump();
            Ok(ColumnPosition::First)
        } else if self.is_kw("AFTER") {
            self.bump();
            Ok(ColumnPosition::After(self.parse_ident_like_name()?))
        } else {
            Ok(ColumnPosition::Default)
        }
    }

    pub(super) fn accept_optional_equals(&mut self) {
        if self.is_op("=") {
            self.bump();
        }
    }

    /// Mirrors Go `ast.ColumnDef.Validate`: generated columns cannot combine
    /// with `DEFAULT`, `ON UPDATE`, or `AUTO_INCREMENT`.
    fn validate_generated_column_options(&self, options: &[ColumnOption]) -> PResult<()> {
        let generated = options
            .iter()
            .any(|option| matches!(option, ColumnOption::Generated { .. }));
        if !generated {
            return Ok(());
        }
        let illegal = options.iter().rev().find_map(|option| match option {
            ColumnOption::AutoIncrement => Some("AUTO_INCREMENT"),
            ColumnOption::Default(_) => Some("DEFAULT"),
            ColumnOption::OnUpdate(_) => Some("ON UPDATE"),
            _ => None,
        });
        match illegal {
            Some(option) => {
                Err(self.err_here(&format!("Incorrect usage of {option} and generated column")))
            }
            None => Ok(()),
        }
    }
}
