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

//! Optimizer-hint parsing: the `/*+ ... */` comment scanner and the
//! per-hint-name productions it dispatches to.
//!
//! This mirrors Go's SEPARATE hint grammar in `pkg/parser/hintparser.y`,
//! which the main grammar invokes over a hint comment's inner text; here the
//! same split is a nested sub-`Parser` over that text rather than a second
//! generated parser.

use super::*;

impl Parser {
    /// Parses one hint inside a `/*+ ... */` comment, dispatching on the
    /// hint's own name — see [`tidb_ast::Hint`]'s own doc for exactly
    /// which names/shapes are modelled. Called on the NESTED sub-`Parser`
    /// [`parse_hint_comment`] constructs over the comment's own inner
    /// text, reusing this same token-cursor infrastructure rather than a
    /// bespoke hint-only lexer/parser.
    fn parse_one_hint(&mut self) -> PResult<Hint> {
        if !matches!(self.peek().kind, TokenKind::Ident | TokenKind::Keyword) {
            return Err(self.err_here("expected an optimizer hint name"));
        }
        let name = self.bump().text.to_ascii_uppercase();
        match name.as_str() {
            "JOIN_FIXED_ORDER" if !self.is_op("(") => Ok(Hint {
                name,
                kind: HintKind::Nullary { qb_name: None },
            }),
            "INL_JOIN"
            | "INL_HASH_JOIN"
            | "INL_MERGE_JOIN"
            | "HASH_JOIN"
            | "HASH_JOIN_BUILD"
            | "HASH_JOIN_PROBE"
            | "BROADCAST_JOIN"
            | "SHUFFLE_JOIN"
            | "NO_HASH_JOIN"
            | "MERGE_JOIN"
            | "NO_MERGE_JOIN"
            | "TIDB_SMJ"
            | "TIDB_INLJ"
            | "TIDB_HJ"
            | "NO_INDEX_JOIN"
            | "NO_INDEX_HASH_JOIN"
            | "NO_INDEX_MERGE_JOIN" => {
                self.expect_op("(")?;
                // An OPTIONAL leading `@qb_name`, read directly from
                // `pkg/parser/hintparser.go`'s `parseTableLevelHint`
                // (calls the SAME shared `parseQBName()` the
                // `MAX_EXECUTION_TIME`/`NTH_PLAN`/`QB_NAME` arms already
                // use) — see `tidb_ast::HintKind::Tables`'s own doc.
                let qb_name = if self.peek().kind == TokenKind::UserVar {
                    Some(decode_at_name(&self.bump().text))
                } else {
                    None
                };
                let mut tables = Vec::new();
                if !self.is_op(")") {
                    tables.push(self.parse_hint_table()?);
                    while self.is_op(",") {
                        self.bump();
                        tables.push(self.parse_hint_table()?);
                    }
                }
                self.expect_op(")")?;
                Ok(Hint {
                    name,
                    kind: HintKind::Tables { qb_name, tables },
                })
            }
            // `MERGE` is a genuine PARSE/RESTORE asymmetry in real TiDB,
            // confirmed via `godump restore` after this project's own
            // coverage measurement caught it: it PARSES a table list
            // exactly like `MERGE_JOIN`/etc. above (`MERGE(t1, t2)` is
            // valid grammar), but ALWAYS restores as bare `MERGE()`,
            // discarding the parsed tables entirely — real TiDB's own
            // restore code puts `"merge"` in its argument-less bucket
            // even though `parseOneHint` dispatches it through the
            // SAME table-list parser as `MERGE_JOIN`. `NO_MERGE`
            // (distinct from `NO_MERGE_JOIN`, which IS a normal
            // table-list hint) is a genuinely different, real MySQL
            // compatibility hint that real TiDB doesn't support AT ALL
            // — parsed only far enough to skip its own args, producing
            // NO hint node, a real, narrower divergence from this
            // project's own "unrecognized name" `ParseError`
            // (deliberately not replicated — see `parse_one_hint`'s own
            // final `_ =>` arm).
            "MERGE" => {
                self.expect_op("(")?;
                if !self.is_op(")") {
                    self.parse_hint_table()?;
                    while self.is_op(",") {
                        self.bump();
                        self.parse_hint_table()?;
                    }
                }
                self.expect_op(")")?;
                Ok(Hint {
                    name,
                    kind: HintKind::Nullary { qb_name: None },
                })
            }
            "USE_INDEX"
            | "FORCE_INDEX"
            | "USE_INDEX_MERGE"
            | "IGNORE_INDEX"
            | "INDEX_LOOKUP_PUSHDOWN"
            | "NO_INDEX_LOOKUP_PUSHDOWN"
            | "ORDER_INDEX"
            | "NO_ORDER_INDEX" => {
                self.expect_op("(")?;
                // Direct translation of Go's `parseIndexLevelHint`: every
                // index-level spelling has the SAME optional query-block
                // prefix, one required hint table, optional comma, and
                // optional index-name list.
                let qb_name = if self.peek().kind == TokenKind::UserVar {
                    Some(decode_at_name(&self.bump().text))
                } else {
                    None
                };
                let table = self.parse_hint_table()?;
                // Optional comma between the table and the index list
                // (confirmed via `godump restore`: both `USE_INDEX(t idx)`
                // and `USE_INDEX(t, idx)` parse and restore identically).
                if self.is_op(",") {
                    self.bump();
                }
                let mut indexes = Vec::new();
                if !self.is_op(")") {
                    indexes.push(self.parse_charset_name()?);
                    while self.is_op(",") {
                        self.bump();
                        indexes.push(self.parse_charset_name()?);
                    }
                }
                self.expect_op(")")?;
                Ok(Hint {
                    name,
                    kind: HintKind::Index {
                        qb_name,
                        table,
                        indexes,
                    },
                })
            }
            // `LEADING` gets its own recursive arm: real TiDB's own
            // `parseLeadingTableList` calls `parseLeadingElement()`
            // unconditionally once before ever checking for a comma, so
            // `LEADING()` (empty) is a genuine parse failure there (real
            // TiDB drops the hint silently with a warning; confirmed via
            // `godump restore` — `LEADING()` restores with NO hint at
            // all, unlike `INL_JOIN()`, which restores fine). This
            // project's own narrower, `ParseError`-over-silent-drop
            // convention (see `tidb_ast::Hint`'s own doc) applies the
            // same way here: requiring at least one table below makes
            // `LEADING()` a `ParseError` instead of silently vanishing.
            // The recursive tree and optional hint-level `@qb` prefix are
            // preserved in `HintKind::Leading` so restore matches Go.
            "LEADING" => {
                self.expect_op("(")?;
                let qb_name = if self.peek().kind == TokenKind::UserVar {
                    Some(decode_at_name(&self.bump().text))
                } else {
                    None
                };
                let mut elements = vec![self.parse_leading_element()?];
                while self.is_op(",") {
                    self.bump();
                    elements.push(self.parse_leading_element()?);
                }
                self.expect_op(")")?;
                Ok(Hint {
                    name,
                    // Go's `parseLeadingHint` accepts an optional hint-level
                    // QB prefix before the recursive table list. Preserve it
                    // in the same tree instead of flattening nested groups.
                    kind: HintKind::Leading { qb_name, elements },
                })
            }
            "SET_VAR" => {
                self.expect_op("(")?;
                let var_name = self.parse_charset_name()?;
                self.expect_op("=")?;
                let value = self.parse_hint_value()?;
                self.expect_op(")")?;
                Ok(Hint {
                    name,
                    kind: HintKind::SetVar { var_name, value },
                })
            }
            "USE_TOJA" | "USE_CASCADES" => {
                self.expect_op("(")?;
                let qb_name = if self.peek().kind == TokenKind::UserVar {
                    Some(decode_at_name(&self.bump().text))
                } else {
                    None
                };
                let value = if self.is_kw("TRUE") {
                    self.bump();
                    true
                } else if self.is_kw("FALSE") {
                    self.bump();
                    false
                } else {
                    return Err(self.err_here("expected TRUE or FALSE"));
                };
                self.expect_op(")")?;
                Ok(Hint {
                    name,
                    kind: HintKind::Bool { qb_name, value },
                })
            }
            "WRITE_SLOW_LOG" => {
                if !self.is_op("(") {
                    return Ok(Hint {
                        name,
                        kind: HintKind::Nullary { qb_name: None },
                    });
                }
                self.bump();
                let value = if self.is_kw("TRUE") {
                    self.bump();
                    true
                } else if self.is_kw("FALSE") {
                    self.bump();
                    false
                } else {
                    return Err(self.err_here("expected TRUE or FALSE"));
                };
                self.expect_op(")")?;
                Ok(Hint {
                    name,
                    kind: HintKind::Bool {
                        qb_name: None,
                        value,
                    },
                })
            }
            // `RESOURCE_GROUP(name)` — a single BARE identifier argument
            // (confirmed via `godump restore`: `RESOURCE_GROUP(default)`
            // parses, so `parse_charset_name` — which accepts any
            // identifier-OR-keyword token, the SAME lenient acceptance
            // `SET_VAR`'s own `var_name` above already relies on — is the
            // right fit, not the narrower `parse_name`). No `@qb_name`
            // suffix is accepted here — real TiDB's own
            // `parseResourceGroupHint` only ever calls `parseIdentifier`,
            // never `parseHintTable`, confirmed via `godump restore`:
            // `RESOURCE_GROUP(rg1@sel_1)` is real TiDB's own silent-drop-
            // with-warning case (the whole hint vanishes from restore),
            // so it stays a genuine `ParseError` here — the SAME
            // narrower, `ParseError`-over-silent-drop convention already
            // applied to `LEADING()`/`USE_TOJA(1)`.
            "RESOURCE_GROUP" => {
                self.expect_op("(")?;
                let qb_name = if self.peek().kind == TokenKind::UserVar {
                    Some(decode_at_name(&self.bump().text))
                } else {
                    None
                };
                let group_name = self.parse_charset_name()?;
                self.expect_op(")")?;
                Ok(Hint {
                    name,
                    kind: HintKind::Name {
                        qb_name,
                        name: group_name,
                    },
                })
            }
            "QUERY_TYPE" => {
                self.expect_op("(")?;
                let qb_name = if self.peek().kind == TokenKind::UserVar {
                    Some(decode_at_name(&self.bump().text))
                } else {
                    None
                };
                if !self.peek().text.eq_ignore_ascii_case("OLAP")
                    && !self.peek().text.eq_ignore_ascii_case("OLTP")
                {
                    return Err(self.err_here("expected OLAP or OLTP"));
                }
                let value = self.bump().text.to_ascii_uppercase();
                self.expect_op(")")?;
                Ok(Hint {
                    name,
                    kind: HintKind::Keyword { qb_name, value },
                })
            }
            "MEMORY_QUOTA" => {
                self.expect_op("(")?;
                let qb_name = if self.peek().kind == TokenKind::UserVar {
                    Some(decode_at_name(&self.bump().text))
                } else {
                    None
                };
                if self.peek().kind != TokenKind::IntLit {
                    return Err(self.err_here("expected memory quota integer"));
                }
                let value = self
                    .bump()
                    .text
                    .parse::<u64>()
                    .map_err(|_| self.err_here("invalid memory quota"))?;
                let multiplier = if self.peek().text.eq_ignore_ascii_case("MB") {
                    self.bump();
                    1_048_576_u64
                } else if self.peek().text.eq_ignore_ascii_case("GB") {
                    self.bump();
                    1_073_741_824_u64
                } else {
                    return Err(self.err_here("expected MB or GB"));
                };
                let bytes = value
                    .checked_mul(multiplier)
                    .and_then(|bytes| i64::try_from(bytes).ok())
                    .ok_or_else(|| self.err_here("memory quota overflow"))?;
                self.expect_op(")")?;
                Ok(Hint {
                    name,
                    kind: HintKind::MemoryQuota { qb_name, bytes },
                })
            }
            "TIME_RANGE" => {
                self.expect_op("(")?;
                if self.peek().kind != TokenKind::Str {
                    return Err(self.err_here("expected TIME_RANGE start string"));
                }
                let from = decode_string(&self.bump().text);
                self.expect_op(",")?;
                if self.peek().kind != TokenKind::Str {
                    return Err(self.err_here("expected TIME_RANGE end string"));
                }
                let to = decode_string(&self.bump().text);
                self.expect_op(")")?;
                Ok(Hint {
                    name,
                    kind: HintKind::TimeRange { from, to },
                })
            }
            // `NAME([@qb_name] N)` — an OPTIONAL leading query-block name
            // before a mandatory integer, read directly from real TiDB's
            // own `pkg/parser/hintparser.go`: `parseMaxExecTimeHint`/
            // `parseNthPlanHint` both call the SAME shared `parseQBName()`
            // immediately after `(`, matching `parse_hint_table`'s own
            // `@qb_name` detection (`TokenKind::UserVar`), just in the
            // PREFIX position instead of the suffix — see
            // `tidb_ast::HintKind::Number`'s own doc.
            "MAX_EXECUTION_TIME" | "NTH_PLAN" => {
                self.expect_op("(")?;
                let qb_name = if self.peek().kind == TokenKind::UserVar {
                    Some(decode_at_name(&self.bump().text))
                } else {
                    None
                };
                if self.peek().kind != TokenKind::IntLit {
                    return Err(self.err_here("expected an integer hint argument"));
                }
                let value: i64 = self
                    .bump()
                    .text
                    .parse()
                    .map_err(|_| self.err_here("invalid integer hint argument"))?;
                self.expect_op(")")?;
                Ok(Hint {
                    name,
                    kind: HintKind::Number { qb_name, value },
                })
            }
            // `QB_NAME(name [, ViewNameList])` — the optional path is
            // dot-separated `name[@sel_N]` or bare `@sel_N` entries.
            // This must not reuse `parse_hint_table`: its `db.table`
            // handling would consume the ViewNameList separator. See
            // `pkg/parser/hintparser.go`'s `parseQBNameHint` and
            // `tidb_ast::HintKind::QbName` for the typed/restoration
            // contract.
            "QB_NAME" => {
                self.expect_op("(")?;
                let qb_name = match self.peek().kind {
                    TokenKind::Ident | TokenKind::Keyword => self.bump().text,
                    TokenKind::CharsetIntroducer => {
                        let token = self.bump();
                        self.source[token.offset..token.end_offset].to_owned()
                    }
                    TokenKind::BitLit
                        if self.peek().text.to_ascii_lowercase().starts_with("0b") =>
                    {
                        self.bump().text
                    }
                    TokenKind::BitLit => {
                        return Err(self.err_here("Cannot use bit-value literal"));
                    }
                    TokenKind::HexLit
                        if self.peek().text.to_ascii_lowercase().starts_with("0x") =>
                    {
                        self.bump().text
                    }
                    TokenKind::HexLit => {
                        return Err(self.err_here("Cannot use hexadecimal literal"));
                    }
                    TokenKind::DecLit | TokenKind::FloatLit => {
                        return Err(self.err_here("Cannot use decimal number"));
                    }
                    _ => return Err(self.err_here("expected a query-block name")),
                };
                let mut views = Vec::new();
                if self.is_op(",") {
                    self.bump();
                    loop {
                        views.push(self.parse_qb_name_view()?);
                        if !self.is_op(".") {
                            break;
                        }
                        self.bump();
                    }
                }
                self.expect_op(")")?;
                Ok(Hint {
                    name,
                    kind: HintKind::QbName { qb_name, views },
                })
            }
            // `READ_FROM_STORAGE([@qb] STORE[t, ...], STORE2[t2, ...],
            // ...)` — see `tidb_ast::HintKind::ReadFromStorage`'s own
            // doc for the exact restore shape. Real TiDB's own
            // `parseStorageHint` treats an unrecognized store name (not
            // `TIKV`/`TIFLASH`) as a silent-drop-the-rest case (its own
            // `default:` arm skips to the close paren and returns
            // whatever groups were already built) — NOT replicated
            // here, since it's a genuinely obscure malformed-input edge
            // case with zero corpus coverage to verify against; this
            // project's own general `ParseError`-over-silent-drop
            // convention applies instead (see [`tidb_ast::Hint`]'s own
            // doc), the SAME choice already made for `LEADING()`'s own
            // empty-table-list case.
            "READ_FROM_STORAGE" => {
                self.expect_op("(")?;
                let qb_name = if self.peek().kind == TokenKind::UserVar {
                    Some(decode_at_name(&self.bump().text))
                } else {
                    None
                };
                let mut groups = Vec::new();
                loop {
                    if !matches!(self.peek().kind, TokenKind::Ident | TokenKind::Keyword) {
                        return Err(self.err_here("expected TIKV or TIFLASH"));
                    }
                    let store = self.bump().text.to_ascii_uppercase();
                    if store != "TIKV" && store != "TIFLASH" {
                        return Err(self.err_here("expected TIKV or TIFLASH"));
                    }
                    // The bracketed table list is OPTIONAL — real TiDB's
                    // own `parseStorageHint` only enters it via `if
                    // hp.match('[')`, so a bare `TIKV` with no list at
                    // all is also valid grammar (not exercised by the
                    // corpus, but cheap to mirror exactly since it falls
                    // straight out of the same `if`).
                    let mut tables = Vec::new();
                    if self.is_op("[") {
                        self.bump();
                        tables.push(self.parse_hint_table()?);
                        while self.is_op(",") {
                            self.bump();
                            tables.push(self.parse_hint_table()?);
                        }
                        self.expect_op("]")?;
                    }
                    groups.push((store, tables));
                    if !self.is_op(",") {
                        break;
                    }
                    self.bump();
                }
                self.expect_op(")")?;
                Ok(Hint {
                    name,
                    kind: HintKind::ReadFromStorage { qb_name, groups },
                })
            }
            "STREAM_AGG"
            | "HASH_AGG"
            | "MPP_1PHASE_AGG"
            | "MPP_2PHASE_AGG"
            | "AGG_TO_COP"
            | "NO_DECORRELATE"
            | "NO_INDEX_MERGE"
            | "IGNORE_PLAN_CACHE"
            | "LIMIT_TO_COP"
            | "USE_PLAN_CACHE"
            | "SEMI_JOIN_REWRITE"
            | "STRAIGHT_JOIN"
            | "READ_CONSISTENT_REPLICA" => {
                // The parens are optional; when present they may contain
                // one query-block name. Restore always shows the parens;
                // see `tidb_ast::HintKind::Nullary`.
                let qb_name = if self.is_op("(") {
                    self.bump();
                    let qb_name = if self.peek().kind == TokenKind::UserVar {
                        Some(decode_at_name(&self.bump().text))
                    } else {
                        None
                    };
                    self.expect_op(")")?;
                    qb_name
                } else {
                    None
                };
                Ok(Hint {
                    name,
                    kind: HintKind::Nullary { qb_name },
                })
            }
            _ => Err(self.err_here("unsupported optimizer hint")),
        }
    }

    /// Parses one hint's table argument: `name [@qb_name]` (see
    /// [`tidb_ast::HintTable`]'s own doc — no partition list, no alias,
    /// unlike a `FROM`-clause [`TableRef`]). The query-block suffix lexes
    /// as a `UserVar` token (`@name`, indistinguishable at the token-kind
    /// level from `@@name`). Decode its payload with the shared `@`-name
    /// helper so bare, quoted, and escaped query-block names all reach the
    /// AST as logical names before restore, matching Go's hint lexer.
    fn parse_hint_table(&mut self) -> PResult<HintTable> {
        let mut name = self.parse_charset_name()?;
        // An optional `db.table` schema qualifier — read directly from
        // `pkg/parser/hintparser.go`'s own `parseHintTable`, which
        // checks for a `.` immediately after the first identifier
        // before ever considering the `@qb_name` suffix below. Every
        // OTHER hint table list in the real-TiDB integration-test
        // corpus this project measures coverage against only ever uses
        // unqualified names, so `db_name` stays `None` there — this is
        // exercised only via `HintKind::ReadFromStorage`'s own corpus
        // target (`` READ_FROM_STORAGE(TIKV[`s`.`t`]) ``).
        let db_name = if self.is_op(".") {
            self.bump();
            let table = self.parse_charset_name()?;
            Some(std::mem::replace(&mut name, table))
        } else {
            None
        };
        let qb_name = if self.peek().kind == TokenKind::UserVar {
            Some(decode_at_name(&self.bump().text))
        } else {
            None
        };
        let mut partitions = Vec::new();
        if self.is_kw("PARTITION") {
            self.bump();
            self.expect_op("(")?;
            partitions.push(self.parse_charset_name()?);
            while self.is_op(",") {
                self.bump();
                partitions.push(self.parse_charset_name()?);
            }
            self.expect_op(")")?;
        }
        Ok(HintTable {
            db_name,
            name,
            qb_name,
            partitions,
        })
    }

    /// Parses one recursive Go `LeadingList` element: either a plain hint
    /// table or a parenthesized nested list. The nested shape is required for
    /// `LEADING((t1, t2), sub)` and restores with its parentheses intact.
    fn parse_leading_element(&mut self) -> PResult<LeadingElement> {
        if self.is_op("(") {
            self.bump();
            let mut elements = vec![self.parse_leading_element()?];
            while self.is_op(",") {
                self.bump();
                elements.push(self.parse_leading_element()?);
            }
            self.expect_op(")")?;
            Ok(LeadingElement::Group(elements))
        } else {
            Ok(LeadingElement::Table(self.parse_hint_table()?))
        }
    }

    /// Parses one `QB_NAME` ViewNameList entry: `name [@sel_N]` or bare
    /// `@sel_N`. Unlike a general hint-table argument, a dot after this
    /// entry belongs to the ViewNameList itself rather than a schema
    /// qualifier.
    fn parse_qb_name_view(&mut self) -> PResult<HintTable> {
        if self.peek().kind == TokenKind::UserVar {
            return Ok(HintTable {
                db_name: None,
                name: String::new(),
                qb_name: Some(decode_at_name(&self.bump().text)),
                partitions: Vec::new(),
            });
        }
        let name = self.parse_charset_name()?;
        let qb_name = if self.peek().kind == TokenKind::UserVar {
            Some(decode_at_name(&self.bump().text))
        } else {
            None
        };
        Ok(HintTable {
            db_name: None,
            name,
            qb_name,
            partitions: Vec::new(),
        })
    }

    /// Skips an optional `(...)` argument group — used by
    /// `parse_hint_comment` when dropping a hint occurrence whose name
    /// real TiDB either doesn't recognize at all, or recognizes but
    /// always treats as unsupported (see that function's own doc).
    /// Depth-tracks nested parens so a paren-heavy argument list (were
    /// one ever present) is skipped past its own true matching close,
    /// not just the first `)` seen.
    fn skip_hint_args(&mut self) {
        if !self.is_op("(") {
            return;
        }
        self.bump(); // (
        let mut depth = 1i32;
        while depth > 0 && !self.at_eof() {
            if self.is_op("(") {
                depth += 1;
            } else if self.is_op(")") {
                depth -= 1;
            }
            self.bump();
        }
    }

    /// Parses a `SET_VAR` hint's own value: a string literal (decoded),
    /// an integer/decimal literal (raw text), an optionally-signed
    /// integer/decimal, or a bare identifier/keyword (`SET_VAR(x=on)`,
    /// `SET_VAR(x=legacy)`) — covering every shape found in real TiDB's
    /// own integration-test corpus. Restore always re-quotes the result
    /// as a string regardless of which of these it came from (see
    /// [`tidb_ast::HintKind::SetVar`]'s own doc), so the exact original
    /// shape doesn't need to be preserved past this point.
    fn parse_hint_value(&mut self) -> PResult<String> {
        match self.peek().kind {
            TokenKind::Str => Ok(decode_string(&self.bump().text)),
            TokenKind::IntLit => {
                let value = self.bump().text;
                value
                    .parse::<u64>()
                    .map(|_| value)
                    .map_err(|_| self.err_here("integer value is out of range"))
            }
            TokenKind::DecLit => {
                let value = self.bump().text;
                if !value.contains(['.', 'e', 'E']) && value.parse::<u64>().is_err() {
                    Err(self.err_here("integer value is out of range"))
                } else {
                    Ok(value)
                }
            }
            TokenKind::FloatLit => Ok(self.bump().text),
            TokenKind::Ident | TokenKind::Keyword => Ok(self.bump().text),
            TokenKind::Op if self.is_op("-") || self.is_op("+") => {
                let sign = self.bump().text;
                match self.peek().kind {
                    TokenKind::IntLit | TokenKind::DecLit | TokenKind::FloatLit => {
                        let digits = self.bump().text;
                        Ok(if sign == "-" {
                            format!("-{digits}")
                        } else {
                            digits
                        })
                    }
                    _ => Err(self.err_here("expected a number after +/- in hint value")),
                }
            }
            _ => Err(self.err_here("expected a SET_VAR hint value")),
        }
    }
}

/// One warning or syntax error produced while parsing an optimizer-hint list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HintDiagnostic {
    /// Source-compatible diagnostic text.
    pub message: String,
}

/// Complete result of the standalone optimizer-hint parser.
#[derive(Debug, Clone, PartialEq)]
pub struct HintParseResult {
    /// Successfully parsed hints, in source order.
    pub hints: Vec<Hint>,
    /// Recoverable hint diagnostics, in source order.
    pub diagnostics: Vec<HintDiagnostic>,
}

/// Parses one complete `/*+ ... */` optimizer-hint comment.
///
/// This is the Rust-native equivalent of `pkg/parser.ParseHint`: malformed or
/// unsupported hint occurrences produce diagnostics and are skipped while
/// later occurrences remain parseable. `ansi_quotes` mirrors the only SQL-mode
/// bit consulted by the source hint scanner, and `initial_line` is retained in
/// syntax diagnostics for callers embedding a comment in a larger statement.
pub fn parse_hint(input: &str, ansi_quotes: bool, initial_line: usize) -> HintParseResult {
    let Some(inner) = input
        .strip_prefix("/*+")
        .and_then(|value| value.strip_suffix("*/"))
    else {
        return HintParseResult {
            hints: Vec::new(),
            diagnostics: vec![hint_syntax_diagnostic(initial_line)],
        };
    };

    let mut parser = Parser::new_hint_with_ansi_quotes(inner, ansi_quotes);
    let mut hints = Vec::new();
    let mut diagnostics = Vec::new();
    while !parser.at_eof() {
        let name = matches!(parser.peek().kind, TokenKind::Ident | TokenKind::Keyword)
            .then(|| parser.peek().text.to_ascii_uppercase());

        if let Some(name) = name.as_deref() {
            let source_name = parser.peek().text.clone();
            let unsupported = is_always_unsupported_hint_name(name)
                || (name == "JOIN_FIXED_ORDER" && parser.peek_n(1).text == "(");
            if unsupported {
                parser.bump();
                parser.skip_hint_args();
                diagnostics.push(HintDiagnostic {
                    message: format!(
                        "[parser:8061]Optimizer hint {source_name} is not supported by TiDB and is ignored"
                    ),
                });
            } else if !is_recognized_hint_token_name(name) {
                parser.bump();
                if parser.is_op("(") && parser.peek_n(1).text == ")" {
                    parser.bump();
                    parser.bump();
                    diagnostics.push(hint_syntax_diagnostic(initial_line));
                } else {
                    parser.skip_hint_args();
                    diagnostics.push(HintDiagnostic {
                        message: format!(
                            "[parser:8061]Optimizer hint {source_name} is not supported by TiDB and is ignored"
                        ),
                    });
                }
            } else {
                parse_standalone_hint_occurrence(
                    &mut parser,
                    &mut hints,
                    &mut diagnostics,
                    initial_line,
                );
            }
        } else {
            parse_standalone_hint_occurrence(
                &mut parser,
                &mut hints,
                &mut diagnostics,
                initial_line,
            );
        }

        if parser.is_op(",") {
            parser.bump();
        }
    }

    if hints.is_empty() && diagnostics.is_empty() {
        diagnostics.push(hint_syntax_diagnostic(initial_line));
    }
    HintParseResult { hints, diagnostics }
}

fn parse_standalone_hint_occurrence(
    parser: &mut Parser,
    hints: &mut Vec<Hint>,
    diagnostics: &mut Vec<HintDiagnostic>,
    initial_line: usize,
) {
    let start = parser.pos;
    match parser.parse_one_hint() {
        Ok(Hint {
            name,
            kind: HintKind::ReadFromStorage { qb_name, groups },
        }) => {
            hints.extend(groups.into_iter().map(|group| Hint {
                name: name.clone(),
                kind: HintKind::ReadFromStorage {
                    qb_name: qb_name.clone(),
                    groups: vec![group],
                },
            }));
        }
        Ok(hint) => hints.push(hint),
        Err(error) => {
            if matches!(
                error.message.as_str(),
                "Cannot use decimal number"
                    | "Cannot use bit-value literal"
                    | "Cannot use hexadecimal literal"
                    | "integer value is out of range"
            ) {
                diagnostics.push(HintDiagnostic {
                    message: error.message,
                });
            }
            diagnostics.push(hint_syntax_diagnostic(initial_line));
            parser.pos = start;
            parser.bump();
            parser.skip_hint_args();
        }
    }
}

fn hint_syntax_diagnostic(initial_line: usize) -> HintDiagnostic {
    HintDiagnostic {
        message: format!("Optimizer hint syntax error at line {initial_line} "),
    }
}

/// Parses a `/*+ ... */` hint comment token's own raw text (INCLUDING the
/// `/*+`/`*/` delimiters, exactly as `tidb_lexer` emits it for a
/// `TokenKind::HintComment` token) into its own hints. Re-lexes the inner
/// text with a fresh, fully self-contained nested [`Parser`] (see its own
/// `new`) rather than a bespoke hint-only lexer, reusing the SAME
/// token-cursor primitives (`peek`/`bump`/`is_kw`/`expect_op`/...) every
/// other parsing function in this crate already uses — real TiDB's own
/// hint grammar has its OWN dedicated ~1200-line lexer/parser
/// (`pkg/parser/hintparser.go`) covering roughly 30 distinct hint shapes;
/// this covers only the four shapes confirmed (via a stratified sample of
/// real TiDB's own integration-test corpus) to account for the
/// overwhelming majority of real-world hint usage — see
/// [`tidb_ast::Hint`]'s own doc for the exact scope boundary.
pub(crate) fn parse_hint_comment(text: &str, initial_line: usize) -> HintParseResult {
    parse_hint(text, false, initial_line)
}

/// Whether `name` (already uppercased) is one of the ~85 hint names real
/// TiDB's own lexer recognizes as a SPECIAL hint token (`hintTokenMap`,
/// `pkg/parser/misc.go`) — read directly from that map, not guessed.
/// Anything NOT in this list tokenizes as a generic `hintIdentifier`
/// there, which `parseOneHint`'s own `default:` case ALWAYS treats as
/// "warn and drop" — see `Parser::parse_hint_comment`'s own doc for how
/// this is used (a name real TiDB doesn't recognize at all can never
/// carry real content, by construction, so it's always safe to drop —
/// UNLIKE a name that simply isn't yet in THIS crate's own smaller
/// `parse_one_hint` dispatch, e.g. `READ_FROM_STORAGE`, which IS in
/// this list and so is deliberately left alone here, kept a
/// `ParseError` by `parse_one_hint`'s own `_ =>` arm instead).
fn is_recognized_hint_token_name(name: &str) -> bool {
    matches!(
        name,
        "JOIN_FIXED_ORDER"
            | "JOIN_ORDER"
            | "JOIN_PREFIX"
            | "JOIN_SUFFIX"
            | "BKA"
            | "NO_BKA"
            | "BNL"
            | "NO_BNL"
            | "HASH_JOIN"
            | "HASH_JOIN_BUILD"
            | "HASH_JOIN_PROBE"
            | "NO_HASH_JOIN"
            | "MERGE"
            | "NO_MERGE"
            | "INDEX_MERGE"
            | "NO_INDEX_MERGE"
            | "MRR"
            | "NO_MRR"
            | "NO_ICP"
            | "NO_RANGE_OPTIMIZATION"
            | "SKIP_SCAN"
            | "NO_SKIP_SCAN"
            | "SEMIJOIN"
            | "NO_SEMIJOIN"
            | "MAX_EXECUTION_TIME"
            | "SET_VAR"
            | "RESOURCE_GROUP"
            | "QB_NAME"
            | "HYPO_INDEX"
            | "AGG_TO_COP"
            | "LIMIT_TO_COP"
            | "IGNORE_PLAN_CACHE"
            | "WRITE_SLOW_LOG"
            | "HASH_AGG"
            | "MPP_1PHASE_AGG"
            | "MPP_2PHASE_AGG"
            | "IGNORE_INDEX"
            | "INL_HASH_JOIN"
            | "INDEX_HASH_JOIN"
            | "NO_INDEX_HASH_JOIN"
            | "INL_JOIN"
            | "INDEX_JOIN"
            | "NO_INDEX_JOIN"
            | "INL_MERGE_JOIN"
            | "INDEX_MERGE_JOIN"
            | "NO_INDEX_MERGE_JOIN"
            | "MEMORY_QUOTA"
            | "NO_SWAP_JOIN_INPUTS"
            | "QUERY_TYPE"
            | "READ_CONSISTENT_REPLICA"
            | "READ_FROM_STORAGE"
            | "BROADCAST_JOIN"
            | "SHUFFLE_JOIN"
            | "MERGE_JOIN"
            | "NO_MERGE_JOIN"
            | "STREAM_AGG"
            | "SWAP_JOIN_INPUTS"
            | "USE_INDEX_MERGE"
            | "USE_INDEX"
            | "ORDER_INDEX"
            | "NO_ORDER_INDEX"
            | "INDEX_LOOKUP_PUSHDOWN"
            | "NO_INDEX_LOOKUP_PUSHDOWN"
            | "USE_PLAN_CACHE"
            | "USE_TOJA"
            | "TIME_RANGE"
            | "USE_CASCADES"
            | "NTH_PLAN"
            | "FORCE_INDEX"
            | "STRAIGHT_JOIN"
            | "LEADING"
            | "SEMI_JOIN_REWRITE"
            | "NO_DECORRELATE"
            | "TIDB_HJ"
            | "TIDB_INLJ"
            | "TIDB_SMJ"
            | "OLAP"
            | "OLTP"
            | "TIKV"
            | "TIFLASH"
            | "PARTITION"
            | "FALSE"
            | "TRUE"
            | "MB"
            | "GB"
            | "DUPSWEEDOUT"
            | "FIRSTMATCH"
            | "LOOSESCAN"
            | "MATERIALIZATION"
    )
}

/// Whether `name` (already uppercased) is one of the "unsupported MySQL
/// hint" names real TiDB's own `parseOneHint` recognizes by name but
/// ALWAYS routes to `parseUnsupportedHint` (`pkg/parser/hintparser.go`,
/// the `case hintBKA, hintNoBKA, ...:` bucket) — genuinely recognized
/// syntax, but real TiDB itself never attaches any semantic content to
/// it regardless of args, always warn-and-drop. `NO_MERGE` (distinct
/// from the real, content-bearing `NO_MERGE_JOIN`) is the one confirmed
/// via the real corpus (`godump restore`: `merge(q) no_merge(q1)` keeps
/// `MERGE()` but drops `no_merge(q1)` entirely) — the rest of this
/// bucket is included too since it's the SAME verified Go-source case,
/// even though none of those individually appear in the real-TiDB
/// integration-test corpus this project measures coverage against.
fn is_always_unsupported_hint_name(name: &str) -> bool {
    matches!(
        name,
        "BKA"
            | "NO_BKA"
            | "BNL"
            | "NO_BNL"
            | "NO_MERGE"
            | "INDEX_MERGE"
            | "MRR"
            | "NO_MRR"
            | "NO_ICP"
            | "NO_RANGE_OPTIMIZATION"
            | "SKIP_SCAN"
            | "NO_SKIP_SCAN"
            | "SEMIJOIN"
            | "NO_SEMIJOIN"
    )
}
