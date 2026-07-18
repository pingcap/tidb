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

//! Complete structural translation of `pkg/parser/ddl_resource_group_parser.go`.

use tidb_ast::{
    AlterResourceGroupStmt, CreateResourceGroupStmt, DdlStmt, DropResourceGroupStmt,
    ResourceGroupBackgroundOption, ResourceGroupBurstable, ResourceGroupOption,
    ResourceGroupPriority, ResourceGroupRate, ResourceGroupRunawayAction,
    ResourceGroupRunawayOption, ResourceGroupRunawayRule, ResourceGroupRunawayWatch,
    ResourceGroupRunawayWatchType,
};
use tidb_lexer::TokenKind;

use crate::{decode_string, PResult, Parser};

impl Parser {
    pub(crate) fn is_resource_group_source_statement(&self) -> bool {
        (self.is_kw("CREATE") || self.is_kw("ALTER") || self.is_kw("DROP"))
            && self.is_kw_at(1, "RESOURCE")
            && self.is_kw_at(2, "GROUP")
    }

    pub(crate) fn parse_resource_group_source_statement(&mut self) -> PResult<DdlStmt> {
        if self.is_kw("CREATE") {
            Ok(DdlStmt::CreateResourceGroup(Box::new(
                self.parse_create_resource_group()?,
            )))
        } else if self.is_kw("ALTER") {
            Ok(DdlStmt::AlterResourceGroup(Box::new(
                self.parse_alter_resource_group()?,
            )))
        } else {
            Ok(DdlStmt::DropResourceGroup(Box::new(
                self.parse_drop_resource_group()?,
            )))
        }
    }

    fn parse_create_resource_group(&mut self) -> PResult<CreateResourceGroupStmt> {
        self.expect_kw("CREATE")?;
        self.expect_kw("RESOURCE")?;
        self.expect_kw("GROUP")?;
        let if_not_exists = self.parse_if_not_exists()?;
        let name = self.parse_resource_group_token_name();
        let options = self.parse_resource_group_options()?;
        Ok(CreateResourceGroupStmt {
            if_not_exists,
            name,
            options,
        })
    }

    fn parse_alter_resource_group(&mut self) -> PResult<AlterResourceGroupStmt> {
        self.expect_kw("ALTER")?;
        self.expect_kw("RESOURCE")?;
        self.expect_kw("GROUP")?;
        let if_exists = self.parse_if_exists()?;
        let name = self.parse_resource_group_token_name();
        let options = self.parse_resource_group_options()?;
        Ok(AlterResourceGroupStmt {
            if_exists,
            name,
            options,
        })
    }

    fn parse_drop_resource_group(&mut self) -> PResult<DropResourceGroupStmt> {
        self.expect_kw("DROP")?;
        self.expect_kw("RESOURCE")?;
        self.expect_kw("GROUP")?;
        let if_exists = self.parse_if_exists()?;
        Ok(DropResourceGroupStmt {
            if_exists,
            name: self.parse_resource_group_token_name(),
        })
    }

    /// Go deliberately uses `next()` rather than an identifier production in
    /// all three statement forms. Preserve that token-shaped name slot,
    /// including reserved words, quoted strings, and the empty EOF token.
    fn parse_resource_group_token_name(&mut self) -> String {
        let token = self.bump();
        if token.kind == TokenKind::Str {
            decode_string(&token.text)
        } else {
            token.text
        }
    }

    fn parse_resource_group_options(&mut self) -> PResult<Vec<ResourceGroupOption>> {
        let mut options = Vec::new();
        loop {
            // Exactly one comma is optional before each option. This preserves
            // the Go loop's acceptance of a leading/trailing comma without
            // accidentally accepting an arbitrary comma run.
            if self.is_op(",") {
                self.bump();
            }
            let Some(option) = self.parse_resource_group_option()? else {
                break;
            };
            if options
                .iter()
                .any(|existing| same_resource_group_option_kind(existing, &option))
            {
                return Err(self.err_here("duplicated resource group option"));
            }
            options.push(option);
        }
        Ok(options)
    }

    fn parse_resource_group_option(&mut self) -> PResult<Option<ResourceGroupOption>> {
        if self.is_kw("RU_PER_SEC") {
            self.bump();
            self.accept_eq();
            let rate = if self.is_kw("UNLIMITED") {
                self.bump();
                ResourceGroupRate::Unlimited
            } else if self.peek().kind == TokenKind::IntLit {
                let token = self.bump();
                ResourceGroupRate::Limited(
                    token
                        .text
                        .parse()
                        .map_err(|_| self.err_here("RU_PER_SEC is out of range"))?,
                )
            } else {
                // Go's zero-value AST is observable when the value is omitted.
                ResourceGroupRate::Limited(0)
            };
            Ok(Some(ResourceGroupOption::RuPerSec(rate)))
        } else if self.is_kw("PRIORITY") {
            self.bump();
            self.accept_eq();
            let priority = if self.is_kw("LOW") {
                self.bump();
                ResourceGroupPriority::Low
            } else if self.is_kw("MEDIUM") {
                self.bump();
                ResourceGroupPriority::Medium
            } else if self.is_kw("HIGH") {
                self.bump();
                ResourceGroupPriority::High
            } else {
                return Err(self.err_here("expected LOW, MEDIUM, or HIGH resource group priority"));
            };
            Ok(Some(ResourceGroupOption::Priority(priority)))
        } else if self.is_kw("BURSTABLE") {
            self.bump();
            let policy = if self.is_op("=") {
                self.bump();
                if self.is_kw("UNLIMITED") {
                    self.bump();
                    ResourceGroupBurstable::Unlimited
                } else if self.is_kw("MODERATED") {
                    self.bump();
                    ResourceGroupBurstable::Moderated
                } else if self.is_kw("OFF") {
                    self.bump();
                    ResourceGroupBurstable::Off
                } else {
                    // Go leaves its zero-value enum here. An unexpected token
                    // still becomes a trailing-token error at the top level.
                    ResourceGroupBurstable::Off
                }
            } else {
                ResourceGroupBurstable::Moderated
            };
            Ok(Some(ResourceGroupOption::Burstable(policy)))
        } else if self.is_kw("QUERY_LIMIT") {
            self.bump();
            self.accept_eq();
            if self.is_kw("NULL") {
                self.bump();
                return Ok(Some(ResourceGroupOption::QueryLimit(Vec::new())));
            }
            self.expect_op("(")?;
            let mut options = Vec::new();
            loop {
                if self.is_op(")") {
                    self.bump();
                    break;
                }
                if self.is_op(",") {
                    self.bump();
                }
                if let Some(option) = self.parse_runaway_option()? {
                    if duplicate_runaway_option(&options, &option) {
                        return Err(self.err_here("duplicated runaway option"));
                    }
                    options.push(option);
                } else if self.at_eof() {
                    return Err(self.err_here("unclosed '(' in QUERY_LIMIT"));
                } else if self.is_op(")") {
                    self.bump();
                    break;
                } else {
                    // The Go source skips an unexpected token and keeps
                    // looking for a known sub-option or the closing paren.
                    self.bump();
                }
            }
            Ok(Some(ResourceGroupOption::QueryLimit(options)))
        } else if self.is_kw("BACKGROUND") {
            self.bump();
            self.accept_eq();
            if self.is_kw("NULL") {
                self.bump();
                return Ok(Some(ResourceGroupOption::Background(Vec::new())));
            }
            self.expect_op("(")?;
            let mut options = Vec::new();
            loop {
                if self.is_op(")") {
                    self.bump();
                    break;
                }
                if self.is_op(",") {
                    self.bump();
                }
                if self.is_kw("TASK_TYPES") {
                    self.bump();
                    self.accept_eq();
                    let tasks = if self.peek().kind == TokenKind::Str {
                        let token = self.bump();
                        decode_string(&token.text)
                    } else {
                        String::new()
                    };
                    let option = ResourceGroupBackgroundOption::TaskTypes(tasks);
                    if duplicate_background_option(&options, &option) {
                        return Err(self.err_here("duplicated background option"));
                    }
                    options.push(option);
                } else if self.is_kw("UTILIZATION_LIMIT") {
                    self.bump();
                    self.accept_eq();
                    if self.peek().kind != TokenKind::IntLit {
                        return Err(self.err_here("UTILIZATION_LIMIT requires an integer value"));
                    }
                    let token = self.bump();
                    let option = ResourceGroupBackgroundOption::UtilizationLimit(
                        token
                            .text
                            .parse()
                            .map_err(|_| self.err_here("UTILIZATION_LIMIT is out of range"))?,
                    );
                    if duplicate_background_option(&options, &option) {
                        return Err(self.err_here("duplicated background option"));
                    }
                    options.push(option);
                } else if self.is_op(")") || self.at_eof() {
                    // Unlike QUERY_LIMIT, the Go source accepts EOF here and
                    // restores the options accumulated so far.
                    if self.is_op(")") {
                        self.bump();
                    }
                    break;
                } else {
                    self.bump();
                }
            }
            Ok(Some(ResourceGroupOption::Background(options)))
        } else {
            Ok(None)
        }
    }

    fn parse_runaway_option(&mut self) -> PResult<Option<ResourceGroupRunawayOption>> {
        if self.is_kw("EXEC_ELAPSED") {
            self.bump();
            self.accept_eq();
            let value = if self.peek().kind == TokenKind::Str {
                let token = self.bump();
                decode_string(&token.text)
            } else {
                String::new()
            };
            Ok(Some(ResourceGroupRunawayOption::Rule(
                ResourceGroupRunawayRule::ExecElapsed(value),
            )))
        } else if self.is_kw("PROCESSED_KEYS") {
            self.bump();
            self.accept_eq();
            Ok(Some(ResourceGroupRunawayOption::Rule(
                ResourceGroupRunawayRule::ProcessedKeys(self.parse_optional_int64_literal()),
            )))
        } else if self.is_kw("RU") {
            self.bump();
            self.accept_eq();
            Ok(Some(ResourceGroupRunawayOption::Rule(
                ResourceGroupRunawayRule::RequestUnit(self.parse_optional_int64_literal()),
            )))
        } else if self.is_kw("ACTION") {
            self.bump();
            self.accept_eq();
            let action = if self.is_kw("COOLDOWN") {
                self.bump();
                ResourceGroupRunawayAction::Cooldown
            } else if self.is_kw("KILL") {
                self.bump();
                ResourceGroupRunawayAction::Kill
            } else if self.is_kw("DRYRUN") {
                self.bump();
                ResourceGroupRunawayAction::DryRun
            } else if self.is_kw("SWITCH_GROUP") {
                self.bump();
                self.expect_op("(")?;
                let name = self.parse_resource_group_token_name();
                self.expect_op(")")?;
                ResourceGroupRunawayAction::SwitchGroup(name)
            } else {
                return Err(self.err_here("expected resource group runaway action"));
            };
            Ok(Some(ResourceGroupRunawayOption::Action(action)))
        } else if self.is_kw("WATCH") {
            self.bump();
            self.accept_eq();
            let watch_type = if self.is_kw("SIMILAR") {
                self.bump();
                ResourceGroupRunawayWatchType::Similar
            } else if self.is_kw("EXACT") {
                self.bump();
                ResourceGroupRunawayWatchType::Exact
            } else if self.is_kw("PLAN") {
                self.bump();
                ResourceGroupRunawayWatchType::Plan
            } else {
                return Err(self.err_here("expected SIMILAR, EXACT, or PLAN after WATCH"));
            };
            let duration = if self.is_kw("DURATION") {
                self.bump();
                self.accept_eq();
                if self.is_kw("UNLIMITED") {
                    self.bump();
                    None
                } else if self.peek().kind == TokenKind::Str {
                    let token = self.bump();
                    let duration = decode_string(&token.text);
                    (!duration.eq_ignore_ascii_case("UNLIMITED")).then_some(duration)
                } else {
                    None
                }
            } else {
                None
            };
            Ok(Some(ResourceGroupRunawayOption::Watch(
                ResourceGroupRunawayWatch {
                    watch_type,
                    duration,
                },
            )))
        } else {
            Ok(None)
        }
    }

    fn parse_optional_int64_literal(&mut self) -> i64 {
        if self.peek().kind != TokenKind::IntLit {
            return 0;
        }
        let token = self.bump();
        token
            .text
            .parse::<u64>()
            .ok()
            .and_then(|value| i64::try_from(value).ok())
            .unwrap_or(0)
    }

    fn accept_eq(&mut self) {
        if self.is_op("=") {
            self.bump();
        }
    }
}

fn same_resource_group_option_kind(
    existing: &ResourceGroupOption,
    candidate: &ResourceGroupOption,
) -> bool {
    matches!(
        (existing, candidate),
        (
            ResourceGroupOption::RuPerSec(_),
            ResourceGroupOption::RuPerSec(_)
        ) | (
            ResourceGroupOption::Priority(_),
            ResourceGroupOption::Priority(_)
        ) | (
            ResourceGroupOption::Burstable(_),
            ResourceGroupOption::Burstable(_)
        ) | (
            ResourceGroupOption::QueryLimit(_),
            ResourceGroupOption::QueryLimit(_)
        ) | (
            ResourceGroupOption::Background(_),
            ResourceGroupOption::Background(_)
        )
    )
}

fn duplicate_runaway_option(
    existing: &[ResourceGroupRunawayOption],
    candidate: &ResourceGroupRunawayOption,
) -> bool {
    match candidate {
        ResourceGroupRunawayOption::Rule(_) => false,
        ResourceGroupRunawayOption::Action(_) => existing
            .iter()
            .any(|option| matches!(option, ResourceGroupRunawayOption::Action(_))),
        ResourceGroupRunawayOption::Watch(_) => existing
            .iter()
            .any(|option| matches!(option, ResourceGroupRunawayOption::Watch(_))),
    }
}

fn duplicate_background_option(
    existing: &[ResourceGroupBackgroundOption],
    candidate: &ResourceGroupBackgroundOption,
) -> bool {
    existing.iter().any(|option| {
        matches!(
            (option, candidate),
            (
                ResourceGroupBackgroundOption::TaskTypes(_),
                ResourceGroupBackgroundOption::TaskTypes(_)
            ) | (
                ResourceGroupBackgroundOption::UtilizationLimit(_),
                ResourceGroupBackgroundOption::UtilizationLimit(_)
            )
        )
    })
}
