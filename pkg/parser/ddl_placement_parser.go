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

package parser

import (
	"github.com/pingcap/tidb/pkg/parser/ast"
)

// parseCreatePlacementPolicyStmt parses CREATE PLACEMENT POLICY statements.
func (p *HandParser) parseCreatePlacementPolicyStmt() ast.StmtNode {
	stmt := Alloc[ast.CreatePlacementPolicyStmt](p.arena)
	p.expect(57389)
	if _, ok := p.accept(57509); ok {
		p.expect(57530)
		stmt.OrReplace = true
	}
	p.expect(58054)
	p.expect(57838)

	stmt.IfNotExists = p.acceptIfNotExists()

	stmt.PolicyName = ast.NewCIStr(p.next().Lit)

	for {
		if opt := p.parsePlacementOption(); opt != nil {
			stmt.PlacementOptions = append(stmt.PlacementOptions, opt)
			p.accept(',') // Optional comma between options
		} else {
			break
		}
	}
	return stmt
}

// parseAlterPlacementPolicyStmt parses ALTER PLACEMENT POLICY statements.
func (p *HandParser) parseAlterPlacementPolicyStmt() ast.StmtNode {
	stmt := Alloc[ast.AlterPlacementPolicyStmt](p.arena)
	p.expect(57365)
	p.expect(58054)
	p.expect(57838)

	stmt.IfExists = p.acceptIfExists()

	stmt.PolicyName = ast.NewCIStr(p.next().Lit)

	for {
		if opt := p.parsePlacementOption(); opt != nil {
			stmt.PlacementOptions = append(stmt.PlacementOptions, opt)
			p.accept(',') // Optional comma between options
		} else {
			break
		}
	}
	return stmt
}

// parseDropPlacementPolicyStmt parses DROP PLACEMENT POLICY statements.
func (p *HandParser) parseDropPlacementPolicyStmt() ast.StmtNode {
	stmt := Alloc[ast.DropPlacementPolicyStmt](p.arena)
	// p.expect(57415) - already consumed
	p.expect(58054)
	p.expect(57838)

	stmt.IfExists = p.acceptIfExists()

	stmt.PolicyName = ast.NewCIStr(p.next().Lit)
	return stmt
}

// parsePlacementStringOption is a helper that handles: accept(tok) = expect(stringLit).
func (p *HandParser) parsePlacementStringOption(tokKeyword int, optType ast.PlacementOptionType) *ast.PlacementOption {
	if _, ok := p.accept(tokKeyword); ok {
		opt := Alloc[ast.PlacementOption](p.arena)
		opt.Tp = optType
		p.accept(58202)
		if tok, ok := p.expect(57353); ok {
			opt.StrValue = tok.Lit
		}
		return opt
	}
	return nil
}

// parsePlacementUintOption is a helper that handles: accept(tok) = parseUint64().
func (p *HandParser) parsePlacementUintOption(tokKeyword int, optType ast.PlacementOptionType) *ast.PlacementOption {
	if _, ok := p.accept(tokKeyword); ok {
		opt := Alloc[ast.PlacementOption](p.arena)
		opt.Tp = optType
		p.accept(58202)
		opt.UintValue = p.parseUint64()
		// Validate: FOLLOWERS must be positive (matches grammar validation).
		if optType == ast.PlacementOptionFollowerCount && opt.UintValue == 0 {
			p.error(p.peek().Offset, "FOLLOWERS must be positive")
			return nil
		}
		return opt
	}
	return nil
}

// parsePlacementOption parses a single placement option.
func (p *HandParser) parsePlacementOption() *ast.PlacementOption {
	// String options: PRIMARY_REGION, REGIONS, SCHEDULE, CONSTRAINTS, *_CONSTRAINTS, SURVIVAL_PREFERENCES
	stringOpts := []struct {
		tok int
		tp  ast.PlacementOptionType
	}{
		{58059, ast.PlacementOptionPrimaryRegion},
		{58174, ast.PlacementOptionRegions},
		{58072, ast.PlacementOptionSchedule},
		{58005, ast.PlacementOptionConstraints},
		{58040, ast.PlacementOptionLeaderConstraints},
		{58023, ast.PlacementOptionFollowerConstraints},
		{58118, ast.PlacementOptionVoterConstraints},
		{58042, ast.PlacementOptionLearnerConstraints},
		{58088, ast.PlacementOptionSurvivalPreferences},
	}
	for _, so := range stringOpts {
		if opt := p.parsePlacementStringOption(so.tok, so.tp); opt != nil {
			return opt
		}
	}

	// Uint options: FOLLOWERS, VOTERS, LEARNERS
	uintOpts := []struct {
		tok int
		tp  ast.PlacementOptionType
	}{
		{58024, ast.PlacementOptionFollowerCount},
		{58119, ast.PlacementOptionVoterCount},
		{58043, ast.PlacementOptionLearnerCount},
	}
	for _, uo := range uintOpts {
		if opt := p.parsePlacementUintOption(uo.tok, uo.tp); opt != nil {
			return opt
		}
	}

	return nil
}
