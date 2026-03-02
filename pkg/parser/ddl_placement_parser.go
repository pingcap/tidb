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
	p.expect(create)
	if _, ok := p.accept(or); ok {
		p.expect(replace)
		stmt.OrReplace = true
	}
	p.expect(placement)
	p.expect(policy)

	stmt.IfNotExists = p.acceptIfNotExists()

	if tok, ok := p.expectIdentLike(); ok {
		stmt.PolicyName = ast.NewCIStr(tok.Lit)
	}

	for {
		opt := p.parsePlacementOption()
		if opt == nil {
			break
		}
		stmt.PlacementOptions = append(stmt.PlacementOptions, opt)
		p.accept(',') // Optional comma between options
	}
	return stmt
}

// parseAlterPlacementPolicyStmt parses ALTER PLACEMENT POLICY statements.
func (p *HandParser) parseAlterPlacementPolicyStmt() ast.StmtNode {
	stmt := Alloc[ast.AlterPlacementPolicyStmt](p.arena)
	p.expect(alter)
	p.expect(placement)
	p.expect(policy)

	stmt.IfExists = p.acceptIfExists()

	if tok, ok := p.expectIdentLike(); ok {
		stmt.PolicyName = ast.NewCIStr(tok.Lit)
	}

	for {
		opt := p.parsePlacementOption()
		if opt == nil {
			break
		}
		stmt.PlacementOptions = append(stmt.PlacementOptions, opt)
		p.accept(',') // Optional comma between options
	}
	return stmt
}

// parseDropPlacementPolicyStmt parses DROP PLACEMENT POLICY statements.
func (p *HandParser) parseDropPlacementPolicyStmt() ast.StmtNode {
	stmt := Alloc[ast.DropPlacementPolicyStmt](p.arena)
	// p.expect(drop) - already consumed
	p.expect(placement)
	p.expect(policy)

	stmt.IfExists = p.acceptIfExists()

	if tok, ok := p.expectIdentLike(); ok {
		stmt.PolicyName = ast.NewCIStr(tok.Lit)
	}
	return stmt
}

// parsePlacementStringOption is a helper that handles: accept(tok) = expect(stringLit).
func (p *HandParser) parsePlacementStringOption(tokKeyword int, optType ast.PlacementOptionType) *ast.PlacementOption {
	if _, ok := p.accept(tokKeyword); ok {
		opt := Alloc[ast.PlacementOption](p.arena)
		opt.Tp = optType
		p.accept(eq)
		if tok, ok := p.expect(stringLit); ok {
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
		p.accept(eq)
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
		{primaryRegion, ast.PlacementOptionPrimaryRegion},
		{regions, ast.PlacementOptionRegions},
		{schedule, ast.PlacementOptionSchedule},
		{constraints, ast.PlacementOptionConstraints},
		{leaderConstraints, ast.PlacementOptionLeaderConstraints},
		{followerConstraints, ast.PlacementOptionFollowerConstraints},
		{voterConstraints, ast.PlacementOptionVoterConstraints},
		{learnerConstraints, ast.PlacementOptionLearnerConstraints},
		{survivalPreferences, ast.PlacementOptionSurvivalPreferences},
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
		{followers, ast.PlacementOptionFollowerCount},
		{voters, ast.PlacementOptionVoterCount},
		{learners, ast.PlacementOptionLearnerCount},
	}
	for _, uo := range uintOpts {
		if opt := p.parsePlacementUintOption(uo.tok, uo.tp); opt != nil {
			return opt
		}
	}

	return nil
}
