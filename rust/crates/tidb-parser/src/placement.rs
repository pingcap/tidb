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

//! Complete structural translation of `pkg/parser/ddl_placement_parser.go`.

use tidb_ast::{
    AlterPlacementPolicyStmt, CreatePlacementPolicyStmt, DdlStmt, DropPlacementPolicyStmt,
    PlacementOption,
};
use tidb_lexer::TokenKind;

use crate::{decode_string, is_reserved, PResult, Parser};

type StringOptionConstructor = fn(String) -> PlacementOption;

struct CountOptionSpec {
    constructor: fn(u64) -> PlacementOption,
    must_be_positive: bool,
}

impl Parser {
    pub(crate) fn is_placement_policy_source_statement(&self) -> bool {
        ((self.is_kw("CREATE") || self.is_kw("ALTER") || self.is_kw("DROP"))
            && self.is_kw_at(1, "PLACEMENT")
            && self.is_kw_at(2, "POLICY"))
            || (self.is_kw("CREATE")
                && self.is_kw_at(1, "OR")
                && self.is_kw_at(2, "REPLACE")
                && self.is_kw_at(3, "PLACEMENT")
                && self.is_kw_at(4, "POLICY"))
    }

    pub(crate) fn parse_placement_policy_source_statement(&mut self) -> PResult<DdlStmt> {
        if self.is_kw("CREATE") {
            Ok(DdlStmt::CreatePlacementPolicy(Box::new(
                self.parse_create_placement_policy()?,
            )))
        } else if self.is_kw("ALTER") {
            Ok(DdlStmt::AlterPlacementPolicy(Box::new(
                self.parse_alter_placement_policy()?,
            )))
        } else {
            Ok(DdlStmt::DropPlacementPolicy(Box::new(
                self.parse_drop_placement_policy()?,
            )))
        }
    }

    fn parse_create_placement_policy(&mut self) -> PResult<CreatePlacementPolicyStmt> {
        self.expect_kw("CREATE")?;
        let or_replace = if self.is_kw("OR") {
            self.bump();
            self.expect_kw("REPLACE")?;
            true
        } else {
            false
        };
        self.expect_kw("PLACEMENT")?;
        self.expect_kw("POLICY")?;
        let if_not_exists = self.parse_if_not_exists()?;
        let name = self.parse_placement_policy_name()?;
        let options = self.parse_placement_options()?;
        Ok(CreatePlacementPolicyStmt {
            or_replace,
            if_not_exists,
            name,
            options,
        })
    }

    fn parse_alter_placement_policy(&mut self) -> PResult<AlterPlacementPolicyStmt> {
        self.expect_kw("ALTER")?;
        self.expect_kw("PLACEMENT")?;
        self.expect_kw("POLICY")?;
        let if_exists = self.parse_if_exists()?;
        let name = self.parse_placement_policy_name()?;
        let options = self.parse_placement_options()?;
        Ok(AlterPlacementPolicyStmt {
            if_exists,
            name,
            options,
        })
    }

    fn parse_drop_placement_policy(&mut self) -> PResult<DropPlacementPolicyStmt> {
        self.expect_kw("DROP")?;
        self.expect_kw("PLACEMENT")?;
        self.expect_kw("POLICY")?;
        Ok(DropPlacementPolicyStmt {
            if_exists: self.parse_if_exists()?,
            name: self.parse_placement_policy_name()?,
        })
    }

    pub(crate) fn parse_placement_policy_name(&mut self) -> PResult<String> {
        let token = self.peek().clone();
        match token.kind {
            TokenKind::Str => {
                self.bump();
                Ok(decode_string(&token.text))
            }
            TokenKind::Ident => Ok(self.bump().text),
            TokenKind::Keyword if !is_reserved(&token.text) => Ok(self.bump().text),
            _ => Err(self.err_here("expected placement policy name")),
        }
    }

    fn parse_placement_options(&mut self) -> PResult<Vec<PlacementOption>> {
        let mut options = Vec::new();
        while let Some(option) = self.parse_placement_option()? {
            options.push(option);
            if self.is_op(",") {
                self.bump();
            }
        }
        Ok(options)
    }

    pub(crate) fn parse_placement_option(&mut self) -> PResult<Option<PlacementOption>> {
        let name = self.peek().text.to_ascii_uppercase();
        let string_constructor: Option<StringOptionConstructor> = match name.as_str() {
            "PRIMARY_REGION" => Some(PlacementOption::PrimaryRegion),
            "REGIONS" => Some(PlacementOption::Regions),
            "SCHEDULE" => Some(PlacementOption::Schedule),
            "CONSTRAINTS" => Some(PlacementOption::Constraints),
            "LEADER_CONSTRAINTS" => Some(PlacementOption::LeaderConstraints),
            "FOLLOWER_CONSTRAINTS" => Some(PlacementOption::FollowerConstraints),
            "VOTER_CONSTRAINTS" => Some(PlacementOption::VoterConstraints),
            "LEARNER_CONSTRAINTS" => Some(PlacementOption::LearnerConstraints),
            "SURVIVAL_PREFERENCES" => Some(PlacementOption::SurvivalPreferences),
            _ => None,
        };
        if let Some(constructor) = string_constructor {
            self.bump();
            if self.is_op("=") {
                self.bump();
            }
            let token = self.peek().clone();
            if token.kind != TokenKind::Str {
                return Err(self.err_here("expected a string placement option value"));
            }
            self.bump();
            let value = decode_string(&token.text);
            return Ok(Some(constructor(value)));
        }

        let count_constructor: Option<CountOptionSpec> = match name.as_str() {
            "FOLLOWERS" => Some(CountOptionSpec {
                constructor: PlacementOption::Followers,
                must_be_positive: true,
            }),
            "VOTERS" => Some(CountOptionSpec {
                constructor: PlacementOption::Voters,
                must_be_positive: false,
            }),
            "LEARNERS" => Some(CountOptionSpec {
                constructor: PlacementOption::Learners,
                must_be_positive: false,
            }),
            _ => None,
        };
        let Some(spec) = count_constructor else {
            return Ok(None);
        };
        self.bump();
        if self.is_op("=") {
            self.bump();
        }
        let token = self.peek().clone();
        if token.kind != TokenKind::IntLit {
            return Err(self.err_here("expected an unsigned placement replica count"));
        }
        self.bump();
        let value = token
            .text
            .parse()
            .map_err(|_| self.err_here("placement replica count is out of range"))?;
        if spec.must_be_positive && value == 0 {
            return Err(self.err_here("FOLLOWERS must be positive"));
        }
        Ok(Some((spec.constructor)(value)))
    }
}
