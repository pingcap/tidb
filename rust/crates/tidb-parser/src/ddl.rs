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
    AdminStmt, AlterOrderItem, AlterPartitionAction, AlterTableAction, AlterTableStmt,
    AnalyzeTableStmt, ColumnOption, ColumnPosition, CompactReplicaKind, CreateTableTemporary,
    CreateViewStmt, DatabaseOption, DropIndexAlgorithm, DropIndexLock, DropIndexStmt,
    DropTableStmt, DropTemporary, FlashbackDatabaseStmt, IndexConstraintKind, OptimizeTableStmt,
    QueryStmt, RenameTableStmt, RepairTableStmt, SetOprTermBody, SplitOption, SplitRegionStmt,
    SplitTarget, Stmt, TableLock, TableLockType, TableOption, UserSpec, ViewAlgorithm,
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
    pub(crate) fn parse_flashback_database(&mut self) -> PResult<FlashbackDatabaseStmt> {
        self.expect_kw("FLASHBACK")?;
        if self.is_kw("DATABASE") || self.is_kw("SCHEMA") {
            self.bump();
        } else {
            return Err(self.err_here("expected DATABASE or SCHEMA"));
        }
        let name = self.parse_name_or_keyword()?;
        let new_name = if self.is_kw("TO") {
            self.bump();
            Some(self.parse_name_or_keyword()?)
        } else {
            None
        };
        Ok(FlashbackDatabaseStmt { name, new_name })
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
        let mut tables = vec![self.parse_name_path()?];
        while self.is_op(",") {
            self.bump();
            tables.push(self.parse_name_path()?);
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
        let table = self.parse_name_path()?;
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
        let name = self.parse_name_or_keyword()?;
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
            Some(self.parse_name_or_keyword()?)
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

    /// Direct structural port of Go's `parseDatabaseOptions`. The generated
    /// lexer charset table is itself sourced from TiDB's `charset.go`, so
    /// invalid names fail at the same parser boundary instead of becoming a
    /// Rust-only accepted statement. Collation lookup has a wider catalog
    /// than the seed value domain; we still preserve its canonical AST form.
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
                self.expect_kw("SET")?;
                if self.is_op("=") {
                    self.bump();
                }
                let raw = self.parse_table_option_word()?;
                let name = canonical_charset(&raw)
                    .ok_or_else(|| self.err_here("unknown character set"))?
                    .to_owned();
                DatabaseOption::CharacterSet(name)
            } else if self.is_kw("CHARSET") {
                self.bump();
                if self.is_op("=") {
                    self.bump();
                }
                let raw = self.parse_table_option_word()?;
                let name = canonical_charset(&raw)
                    .ok_or_else(|| self.err_here("unknown character set"))?
                    .to_owned();
                DatabaseOption::CharacterSet(name)
            } else if self.is_kw("COLLATE") {
                self.bump();
                if self.is_op("=") {
                    self.bump();
                }
                let raw = self.parse_table_option_word()?;
                let collation =
                    canonical_collation(&raw).ok_or_else(|| self.err_here("unknown collation"))?;
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
                    return Err(self.err_here("ENCRYPTION value must be Y or N"));
                }
                DatabaseOption::Encryption(value)
            } else if self.is_kw("PLACEMENT") {
                self.bump();
                self.expect_kw("POLICY")?;
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
                        self.parse_placement_policy_name()?
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
        let name = self.parse_name_or_keyword()?;
        self.expect_kw("ON")?;
        let table = self.parse_name_path()?;
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
        if self.is_kw("BY") {
            self.bump();
            let mut points = vec![self.parse_split_tuple(false)?];
            while self.is_op(",") {
                self.bump();
                points.push(self.parse_split_tuple(false)?);
            }
            Ok(SplitOption::By(points))
        } else if self.is_kw("BETWEEN") {
            self.bump();
            let lower = self.parse_split_tuple(true)?;
            self.expect_kw("AND")?;
            let upper = self.parse_split_tuple(true)?;
            self.expect_kw("REGIONS")?;
            let token = self.peek().clone();
            if token.kind != TokenKind::IntLit {
                return Err(self.err_here("expected split region count"));
            }
            self.bump();
            let regions = token
                .text
                .parse::<i64>()
                .map_err(|_| self.err_here("split region count out of range"))?;
            Ok(SplitOption::Between {
                lower,
                upper,
                regions,
            })
        } else {
            Err(self.err_here("expected BY or BETWEEN in SPLIT statement"))
        }
    }

    /// Parses one split-key tuple.  Empty tuples are valid only in `BETWEEN`
    /// bounds; `BY ()` is a genuine parser error in Go.
    fn parse_split_tuple(&mut self, allow_empty: bool) -> PResult<Vec<tidb_ast::Expr>> {
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
            query: Box::new(query),
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
        self.expect_kw("TABLE")?;
        let name = self.parse_name_path()?;
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
        let mut partitions = vec![self.parse_name()?];
        while self.is_op(",") {
            self.bump();
            partitions.push(self.parse_name()?);
        }
        Ok(Stmt::Admin(tidb_ast::NodeBox::new(
            AdminStmt::AnalyzeTable(Box::new(AnalyzeTableStmt {
                tables: vec![table],
                partitions,
                no_write_to_binlog: false,
                target: tidb_ast::AnalyzeTarget::Default,
                options: Vec::new(),
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
        } else if let Some(action) = alter::auto_increment::parse(self)? {
            action
        } else if let Some(action) = alter::auto_id_options::parse(self)? {
            action
        } else if self.is_kw("CONVERT") {
            AlterTableAction::ConvertCharacterSet {
                charset: self.parse_alter_convert_character_set()?,
                collation: self.parse_optional_alter_collation()?,
            }
        } else if self.is_kw("AFFINITY") {
            // Direct Go `parseTableOption` transition: AFFINITY takes an
            // optional equals sign followed by a string literal only. It is
            // not a generic identifier-valued ALTER option; preserving that
            // boundary prevents accepting Go-rejected numeric/bare forms.
            self.bump();
            if self.is_op("=") {
                self.bump();
            }
            let token = self.peek().clone();
            if token.kind != TokenKind::Str {
                return Err(self.err_here("expected AFFINITY string literal"));
            }
            self.bump();
            AlterTableAction::SetAffinity {
                level: decode_string(&token.text),
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
            AlterTableAction::SetAttributes { attributes }
        } else if let Some(action) = alter::shard_row_id_bits::parse(self)? {
            action
        } else if self.starts_alter_table_charset_or_collation_option() {
            AlterTableAction::SetTableOptions {
                options: self.parse_alter_table_charset_collation_options()?,
            }
        } else if self.is_kw("ENGINE_ATTRIBUTE") {
            // Go's `parseAlterTableOptions` delegates ENGINE_ATTRIBUTE to the
            // shared `parseTableOption` production. Keep adjacent copies in
            // one source-shaped option list (the comma-separated case is
            // owned by the outer ALTER TABLE spec loop).
            let mut options = Vec::new();
            loop {
                let Some(option) = self.parse_table_option()? else {
                    unreachable!("ENGINE_ATTRIBUTE starts a table option");
                };
                options.push(option);
                if !self.is_kw("ENGINE_ATTRIBUTE") {
                    break;
                }
            }
            AlterTableAction::SetTableOptions { options }
        } else if self.starts_alter_table_generic_option() {
            // Go's `parseAlterTableOptions` delegates these physical/MERGE
            // options to the shared `parseTableOption` production. Keep the
            // selector deliberately narrow: this ring owns only the exact
            // standalone INSERT_METHOD, PRE_SPLIT_REGIONS, and UNION forms
            // still present in the static Go-accepted queue.
            let mut options = Vec::new();
            while let Some(option) = self.parse_table_option()? {
                options.push(option);
                if !self.starts_alter_table_generic_option() {
                    break;
                }
            }
            AlterTableAction::SetTableOptions { options }
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
                    name: self.parse_name()?,
                }
            } else {
                if self.is_kw("COLUMN") {
                    self.bump();
                }
                let if_exists = self.parse_if_exists()?;
                AlterTableAction::DropColumn {
                    if_exists,
                    name: self.parse_name()?,
                }
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
            let column = self.parse_column_def()?;
            let position = self.parse_column_position()?;
            AlterTableAction::ModifyColumn { column, position }
        } else if self.is_kw("CHANGE") {
            self.bump();
            if self.is_kw("COLUMN") {
                self.bump();
            }
            let old_name = self.parse_name()?;
            let column = self.parse_column_def()?;
            let position = self.parse_column_position()?;
            AlterTableAction::ChangeColumn {
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
            if self.is_kw("TO") || self.is_kw("AS") {
                self.bump();
            }
            AlterTableAction::RenameTable {
                new_name: self.parse_name_path()?,
            }
        } else if self.is_kw("ORDER") {
            self.bump();
            self.expect_kw("BY")?;
            let mut items = Vec::new();
            loop {
                let column = self.parse_name_path()?;
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
                    if self.is_op(",") && self.peek_n(1).kind == TokenKind::Str {
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
                partitions.push(self.parse_name()?);
                while self.is_op(",") {
                    self.bump();
                    partitions.push(self.parse_name()?);
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
                SplitTarget::Index(self.parse_name_or_keyword()?)
            } else if self.is_kw("REGION") {
                // Go accepts `SPLIT REGION BETWEEN ...` as the implicit
                // table-level spelling.
                self.bump();
                SplitTarget::Table
            } else if self.is_kw("BY") || self.is_kw("BETWEEN") {
                SplitTarget::Table
            } else {
                return Err(self.err_here("expected SPLIT TABLE, PRIMARY KEY, or INDEX"));
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

    /// Mirrors Go's `parseAlterTableOptions` narrow `TableOptionCharset` /
    /// `TableOptionCollate` path. A single ALTER spec may contain multiple
    /// adjacent options without commas (for example, `COLLATE x CHARSET y`),
    /// and Go restores that source order as space-separated table options.
    /// Keep this deliberately separate from CREATE TABLE's broad option loop:
    /// this statement family has different `CONVERT TO` handling and no
    /// implicit comma acceptance in this seed's one-action AST.
    fn parse_alter_table_charset_collation_options(&mut self) -> PResult<Vec<TableOption>> {
        let mut options = Vec::new();
        while self.starts_alter_table_charset_or_collation_option() {
            if self.is_kw("DEFAULT") {
                self.bump();
            }
            if self.is_kw("CHARACTER") || self.is_kw("CHAR") {
                self.bump();
                self.expect_kw("SET")?;
                if self.is_op("=") {
                    self.bump();
                }
                options.push(TableOption::CharacterSet(self.parse_alter_charset_name()?));
            } else if self.is_kw("CHARSET") {
                self.bump();
                if self.is_op("=") {
                    self.bump();
                }
                options.push(TableOption::CharacterSet(self.parse_alter_charset_name()?));
            } else {
                self.expect_kw("COLLATE")?;
                if self.is_op("=") {
                    self.bump();
                }
                let raw = self.parse_table_option_word()?;
                let collation =
                    canonical_collation(&raw).ok_or_else(|| self.err_here("unknown collation"))?;
                options.push(TableOption::Collate(collation.to_ascii_uppercase()));
            }
        }
        Ok(options)
    }

    /// Whether the next Go `parseTableOption` production belongs to this
    /// typed ALTER TABLE option slice. `DEFAULT` is only a prefix here when
    /// it actually introduces a charset or collation option; treating a bare
    /// DEFAULT as an option would accept a statement Go rejects.
    fn starts_alter_table_charset_or_collation_option(&self) -> bool {
        let mut offset = 0;
        if self.is_kw("DEFAULT") {
            offset = 1;
        }
        self.is_kw_at(offset, "CHARACTER")
            || self.is_kw_at(offset, "CHAR")
            || self.is_kw_at(offset, "CHARSET")
            || self.is_kw_at(offset, "COLLATE")
    }

    fn starts_alter_table_generic_option(&self) -> bool {
        self.is_kw("ENGINE")
            || self.is_kw("ROW_FORMAT")
            || self.is_kw("KEY_BLOCK_SIZE")
            || self.is_kw("INSERT_METHOD")
            || self.is_kw("PRE_SPLIT_REGIONS")
            || self.is_kw("UNION")
            || self.is_kw("PLACEMENT")
            || self.peek().text.eq_ignore_ascii_case("COMMENT")
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
            let old = self.parse_name_path()?;
            self.expect_kw("TO")?;
            let new = self.parse_name_path()?;
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
            names.push(self.parse_name_path()?);
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
            Ok(ColumnPosition::After(self.parse_name()?))
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
