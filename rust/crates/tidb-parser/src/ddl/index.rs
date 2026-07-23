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

//! Shared index, index-part, and foreign-key action parser routines.

use tidb_ast::{
    CreateIndexStmt, ForeignKeyConstraintDefinition, ForeignKeyMatch, ForeignKeyReference,
    IndexAlgorithm, IndexConstraintDefinition, IndexConstraintKind, IndexKind, IndexLock,
    IndexOnlineDdl, IndexOptions, IndexPart, IndexPreSplitRegions, IndexType, IndexVisibility,
    PrimaryKeyStorage, ReferentialAction,
};
use tidb_lexer::TokenKind;

use crate::{prec, PResult, Parser};

impl Parser {
    pub(super) fn parse_optional_index_name(&mut self) -> PResult<Option<String>> {
        // Go consumes every `isIdentLike` token as the optional name first.
        // Thus `INDEX TYPE (a)` names the index `type`; a method needs the
        // unambiguous `INDEX USING BTREE (a)` or `INDEX TYPE TYPE BTREE (a)`.
        if self.is_ident_like_name() {
            Ok(Some(self.parse_ident_like_name()?))
        } else {
            Ok(None)
        }
    }

    pub(crate) fn parse_create_index(&mut self) -> PResult<CreateIndexStmt> {
        self.expect_kw("CREATE")?;
        let kind = if self.is_kw("UNIQUE") {
            self.bump();
            IndexKind::Unique
        } else if self.is_kw("FULLTEXT") {
            self.bump();
            IndexKind::Fulltext
        } else if self.is_kw("SPATIAL") {
            self.bump();
            IndexKind::Spatial
        } else if self.is_kw("VECTOR") {
            self.bump();
            IndexKind::Vector
        } else if self.is_kw("COLUMNAR") {
            self.bump();
            IndexKind::Columnar
        } else {
            IndexKind::Ordinary
        };
        self.expect_kw("INDEX")?;
        let if_not_exists = self.parse_if_not_exists()?;
        let name = if self.is_ident_like_name() {
            self.parse_ident_like_name()?
        } else {
            String::new()
        };
        let pre_index_type = self.parse_optional_index_type()?;
        self.expect_kw("ON")?;
        let table = self.parse_table_name()?;
        // Go accepts explicit ASC and restores it as the default.
        let parts = self.parse_index_parts()?;
        // Unlike the other DDL statement envelopes, standalone CREATE INDEX
        // has now moved atomically to the source-shaped IndexOptions payload.
        // This keeps pre-ON and post-ON type clauses lossless while leaving
        // CREATE TABLE and ALTER TABLE routes on their separate contracts.
        let mut options = self.parse_index_options()?;
        if options.index_type.is_none() {
            options.index_type = pre_index_type;
        }
        let online = self.parse_create_index_online_ddl()?;
        Ok(CreateIndexStmt {
            kind,
            if_not_exists,
            name,
            table,
            parts,
            options,
            online,
        })
    }

    /// Parses `USING`/`TYPE` index methods in either CREATE INDEX position.
    fn parse_optional_index_type(&mut self) -> PResult<Option<IndexType>> {
        if self.is_kw("USING") || self.is_kw("TYPE") {
            self.bump();
            Ok(Some(self.parse_index_type()?))
        } else {
            Ok(None)
        }
    }

    /// Parses the method vocabulary accepted by Go's `resolveIndexType`.
    fn parse_index_type(&mut self) -> PResult<IndexType> {
        let index_type = if self.is_kw("BTREE") {
            IndexType::Btree
        } else if self.is_kw("HASH") {
            IndexType::Hash
        } else if self.is_kw("RTREE") {
            IndexType::Rtree
        } else if self.is_kw("HNSW") {
            IndexType::Hnsw
        } else if self.is_kw("HYPO") {
            IndexType::Hypo
        } else if self.is_kw("INVERTED") {
            IndexType::Inverted
        } else {
            return Err(self.err_here("expected index type"));
        };
        self.bump();
        Ok(index_type)
    }

    /// Parses Go's repeatable `IndexOption` sequence for standalone CREATE
    /// INDEX. Repeated scalar clauses intentionally overwrite the earlier
    /// value, exactly as the source parser's single option struct does.
    pub(super) fn parse_index_options(&mut self) -> PResult<IndexOptions> {
        let mut options = IndexOptions::default();
        loop {
            if self.is_kw("USING") || self.is_kw("TYPE") {
                self.bump();
                options.index_type = Some(self.parse_index_type()?);
            } else if self.is_kw("KEY_BLOCK_SIZE") {
                self.bump();
                if self.is_op("=") {
                    self.bump();
                }
                let token = self.peek().clone();
                if token.kind != TokenKind::IntLit {
                    return Err(self.err_here("expected KEY_BLOCK_SIZE integer"));
                }
                self.bump();
                options.key_block_size = Some(
                    token
                        .text
                        .parse::<u64>()
                        .map_err(|_| self.err_here("KEY_BLOCK_SIZE out of range"))?,
                );
            } else if self.is_kw("ADD_COLUMNAR_REPLICA_ON_DEMAND") {
                self.bump();
                options.add_columnar_replica_on_demand = 1;
            } else if self.is_kw("COMMENT") {
                self.bump();
                options.comment = Some(self.parse_string_literal("expected index comment")?);
            } else if self.is_kw("WITH") {
                self.bump();
                self.expect_kw("PARSER")?;
                options.parser_name = Some(self.parse_non_string_ident_like_name()?);
                self.warn("The WITH PARASER clause is parsed but ignored by all storage engines.");
            } else if self.is_kw("VISIBLE") {
                self.bump();
                options.visibility = Some(IndexVisibility::Visible);
            } else if self.is_kw("INVISIBLE") {
                self.bump();
                options.visibility = Some(IndexVisibility::Invisible);
            } else if self.is_kw("CLUSTERED") {
                self.bump();
                options.primary_key_storage = Some(PrimaryKeyStorage::Clustered);
            } else if self.is_kw("NONCLUSTERED") {
                self.bump();
                options.primary_key_storage = Some(PrimaryKeyStorage::NonClustered);
            } else if self.is_kw("PRE_SPLIT_REGIONS") {
                self.bump();
                if self.is_op("=") {
                    self.bump();
                }
                options.pre_split_regions = Some(if self.is_op("(") {
                    self.bump();
                    let split = self.parse_split_option()?;
                    self.expect_op(")")?;
                    IndexPreSplitRegions::Boundaries(split)
                } else {
                    let token = self.peek().clone();
                    if token.kind != TokenKind::IntLit {
                        return Err(self.err_here("expected PRE_SPLIT_REGIONS integer"));
                    }
                    self.bump();
                    IndexPreSplitRegions::Count(
                        token
                            .text
                            .parse::<i64>()
                            .map_err(|_| self.err_here("PRE_SPLIT_REGIONS out of range"))?,
                    )
                });
            } else if self.is_kw("WHERE") {
                self.bump();
                options.condition = Some(self.parse_expr(prec::NONE)?);
            } else if self.is_kw("GLOBAL") {
                self.bump();
                options.global = true;
            } else if self.is_kw("LOCAL") {
                self.bump();
                options.global = false;
            } else if self.is_kw("SECONDARY_ENGINE_ATTRIBUTE") {
                self.bump();
                if self.is_op("=") {
                    self.bump();
                }
                options.secondary_engine_attribute =
                    Some(self.parse_string_literal("expected SECONDARY_ENGINE_ATTRIBUTE string")?);
            } else {
                return Ok(options);
            }
        }
    }

    /// Parses at most two `LOCK`/`ALGORITHM` clauses. Their source order is
    /// retained only semantically; AST restore canonicalizes algorithm first.
    fn parse_create_index_online_ddl(&mut self) -> PResult<IndexOnlineDdl> {
        let mut online = IndexOnlineDdl::default();
        for _ in 0..2 {
            if self.is_kw("LOCK") {
                self.bump();
                if self.is_op("=") {
                    self.bump();
                }
                if self.is_kw("DEFAULT") {
                    self.bump();
                    online.lock = None;
                } else if self.is_kw("NONE") {
                    self.bump();
                    online.lock = Some(IndexLock::None);
                } else if self.is_kw("SHARED") {
                    self.bump();
                    online.lock = Some(IndexLock::Shared);
                } else if self.is_kw("EXCLUSIVE") {
                    self.bump();
                    online.lock = Some(IndexLock::Exclusive);
                } else {
                    return Err(self.err_here("expected LOCK mode"));
                }
            } else if self.is_kw("ALGORITHM") {
                self.bump();
                if self.is_op("=") {
                    self.bump();
                }
                if self.is_kw("DEFAULT") {
                    self.bump();
                    online.algorithm = None;
                } else if self.is_kw("COPY") {
                    self.bump();
                    online.algorithm = Some(IndexAlgorithm::Copy);
                } else if self.is_kw("INPLACE") {
                    self.bump();
                    online.algorithm = Some(IndexAlgorithm::Inplace);
                } else if self.is_kw("INSTANT") {
                    self.bump();
                    online.algorithm = Some(IndexAlgorithm::Instant);
                } else {
                    return Err(self.err_here("expected ALGORITHM mode"));
                }
            } else {
                break;
            }
        }
        Ok(online)
    }

    /// Parses the index-bearing portion shared by CREATE TABLE constraints
    /// and ALTER TABLE ADD constraints. It is the direct counterpart to Go's
    /// `parseIndexDefinition`: pre-list USING/TYPE merges into post-list
    /// options only when the latter did not override the method.
    pub(super) fn parse_index_constraint(
        &mut self,
        kind: IndexConstraintKind,
        if_not_exists: bool,
        name: Option<String>,
        is_empty_index: bool,
        allows_pre_index_type: bool,
    ) -> PResult<IndexConstraintDefinition> {
        let pre_index_type = if allows_pre_index_type
            && (self.is_kw("USING") || (name.is_some() && self.is_kw("TYPE")))
        {
            self.parse_optional_index_type()?
        } else {
            None
        };
        let parts = self.parse_index_parts()?;
        let mut options = self.parse_index_options()?;
        if options.index_type.is_none() {
            options.index_type = pre_index_type;
        }
        Ok(IndexConstraintDefinition {
            kind,
            if_not_exists,
            name,
            is_empty_index,
            parts,
            options,
        })
    }

    /// Parses Go's `FOREIGN KEY ... REFERENCES` payload without flattening
    /// its key parts, optional referenced-part list, or MATCH mode.
    pub(super) fn parse_foreign_key_constraint(
        &mut self,
        name: Option<String>,
        if_not_exists: bool,
    ) -> PResult<ForeignKeyConstraintDefinition> {
        let parts = self.parse_index_parts()?;
        let reference = self.parse_foreign_key_reference()?;
        Ok(ForeignKeyConstraintDefinition {
            name,
            if_not_exists,
            parts,
            reference,
        })
    }

    /// Parses Go's `ast.ReferenceDef`, the single reusable payload behind
    /// both table-level `FOREIGN KEY (...) REFERENCES ...` constraints and a
    /// column's own `REFERENCES ...` option.
    pub(super) fn parse_foreign_key_reference(&mut self) -> PResult<ForeignKeyReference> {
        self.expect_kw("REFERENCES")?;
        let table = Some(self.parse_table_name()?);
        let reference_parts = if self.is_op("(") {
            Some(self.parse_index_parts()?)
        } else {
            None
        };
        let match_type = if self.is_kw("MATCH") {
            self.bump();
            let match_type = if self.is_kw("FULL") {
                self.bump();
                ForeignKeyMatch::Full
            } else if self.is_kw("PARTIAL") {
                self.bump();
                ForeignKeyMatch::Partial
            } else if self.is_kw("SIMPLE") {
                self.bump();
                ForeignKeyMatch::Simple
            } else {
                // Go's `parseReferenceDef` consumes MATCH and only assigns a
                // mode when the next token is FULL, PARTIAL, or SIMPLE.  It
                // does not reject a bare MATCH; the token is effectively
                // ignored and the following clause is parsed normally.
                // Preserve that source boundary instead of making the Rust
                // parser stricter than TiDB's yacc grammar.
                ForeignKeyMatch::None
            };
            self.warn("The MATCH clause is parsed but ignored by all storage engines.");
            match_type
        } else {
            ForeignKeyMatch::None
        };
        let mut on_delete = None;
        let mut on_update = None;
        let mut has_on_delete = false;
        let mut has_on_update = false;
        while self.is_kw("ON") && (self.is_kw_at(1, "DELETE") || self.is_kw_at(1, "UPDATE")) {
            self.bump();
            if self.is_kw("DELETE") {
                if has_on_delete {
                    return Err(self.err_here("duplicate ON DELETE"));
                }
                has_on_delete = true;
                self.bump();
                let action = self.parse_referential_action()?;
                on_delete = (action != ReferentialAction::NoOption).then_some(action);
            } else if self.is_kw("UPDATE") {
                if has_on_update {
                    return Err(self.err_here("duplicate ON UPDATE"));
                }
                has_on_update = true;
                self.bump();
                let action = self.parse_referential_action()?;
                on_update = (action != ReferentialAction::NoOption).then_some(action);
            }
        }
        Ok(ForeignKeyReference {
            table,
            parts: reference_parts,
            match_type,
            on_delete,
            on_update,
        })
    }

    /// Parses Go's shared `IndexPartSpecificationList`: column parts may
    /// carry a prefix length and direction, while functional parts use an
    /// additional pair of parentheses. PRIMARY/UNIQUE keys and ordinary
    /// indexes intentionally meet here so no CREATE TABLE route can erase
    /// index-part syntax that Go keeps in the AST.
    pub(super) fn parse_index_parts(&mut self) -> PResult<Vec<IndexPart>> {
        self.expect_op("(")?;
        let mut parts = Vec::new();
        loop {
            let mut part = if self.is_op("(") {
                self.bump();
                let expr = self.parse_expr(prec::NONE)?;
                self.expect_op(")")?;
                IndexPart::Expr { expr, desc: false }
            } else {
                let name = self.parse_ident_like_name()?;
                let prefix_len = if self.is_op("(") {
                    self.bump();
                    let token = self.bump();
                    let prefix_len = token
                        .text
                        .parse::<i64>()
                        .map_err(|_| self.err_here("expected index prefix length"))?;
                    self.expect_op(")")?;
                    Some(prefix_len)
                } else {
                    None
                };
                IndexPart::Column {
                    name,
                    prefix_len,
                    desc: false,
                }
            };
            if self.is_kw("DESC") {
                self.bump();
                match &mut part {
                    IndexPart::Column { desc, .. } | IndexPart::Expr { desc, .. } => *desc = true,
                }
            } else if self.is_kw("ASC") {
                // Go parses ASC but restores the default direction silently.
                self.bump();
            }
            parts.push(part);
            if !self.is_op(",") {
                break;
            }
            self.bump();
        }
        self.expect_op(")")?;
        Ok(parts)
    }

    /// Parses a `FOREIGN KEY`'s `ON DELETE`/`ON UPDATE` action, positioned
    /// right after the `DELETE`/`UPDATE` keyword.
    pub(super) fn parse_referential_action(&mut self) -> PResult<ReferentialAction> {
        if self.is_kw("CASCADE") {
            self.bump();
            Ok(ReferentialAction::Cascade)
        } else if self.is_kw("RESTRICT") {
            self.bump();
            Ok(ReferentialAction::Restrict)
        } else if self.is_kw("SET") {
            self.bump();
            if self.is_kw("NULL") {
                self.bump();
                Ok(ReferentialAction::SetNull)
            } else if self.is_kw("DEFAULT") {
                self.bump();
                self.warn("The SET DEFAULT clause is parsed but ignored by all storage engines.");
                Ok(ReferentialAction::SetDefault)
            } else {
                Ok(ReferentialAction::NoOption)
            }
        } else if self.is_kw("NO") {
            self.bump();
            // Go's `parseReferAction` treats ACTION as optional after NO;
            // both spellings restore canonically as NO ACTION.
            if self.is_kw("ACTION") {
                self.bump();
            }
            Ok(ReferentialAction::NoAction)
        } else {
            Ok(ReferentialAction::NoOption)
        }
    }
}
