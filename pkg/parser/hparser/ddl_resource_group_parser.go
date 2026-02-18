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

package hparser

import (
	"strings"

	"github.com/pingcap/tidb/pkg/parser/ast"
)

// parseCreateResourceGroupStmt parses CREATE RESOURCE GROUP statements.
func (p *HandParser) parseCreateResourceGroupStmt() ast.StmtNode {
	stmt := Alloc[ast.CreateResourceGroupStmt](p.arena)
	p.expect(tokCreate)
	p.expect(tokResourceGroup) // tokResource
	p.expect(tokGroup)

	stmt.IfNotExists = p.acceptIfNotExists()

	stmt.ResourceGroupName = ast.NewCIStr(p.next().Lit)

	stmt.ResourceGroupOptionList = p.parseResourceGroupOptionList()
	return stmt
}

// parseAlterResourceGroupStmt parses ALTER RESOURCE GROUP statements.
func (p *HandParser) parseAlterResourceGroupStmt() ast.StmtNode {
	stmt := Alloc[ast.AlterResourceGroupStmt](p.arena)
	p.expect(tokAlter)
	p.expect(tokResourceGroup) // tokResource
	p.expect(tokGroup)

	stmt.IfExists = p.acceptIfExists()

	stmt.ResourceGroupName = ast.NewCIStr(p.next().Lit)

	stmt.ResourceGroupOptionList = p.parseResourceGroupOptionList()
	return stmt
}

// parseDropResourceGroupStmt parses DROP RESOURCE GROUP statements.
func (p *HandParser) parseDropResourceGroupStmt() ast.StmtNode {
	stmt := Alloc[ast.DropResourceGroupStmt](p.arena)
	// p.expect(tokDrop) - already consumed
	p.expect(tokResourceGroup) // tokResource
	p.expect(tokGroup)

	stmt.IfExists = p.acceptIfExists()

	stmt.ResourceGroupName = ast.NewCIStr(p.next().Lit)
	return stmt
}

// parseResourceGroupOption parses a single resource group option.
func (p *HandParser) parseResourceGroupOption() *ast.ResourceGroupOption {
	opt := &ast.ResourceGroupOption{}
	if _, ok := p.accept(tokRuPerSec); ok {
		p.accept(tokEq)
		opt.Tp = ast.ResourceRURate
		if _, ok := p.accept(tokUnlimited); ok {
			opt.Burstable = ast.BurstableUnlimited
		} else if t, ok := p.accept(tokIntLit); ok {
			switch v := t.Item.(type) {
			case int64:
				opt.UintValue = uint64(v)
			case uint64:
				opt.UintValue = v
			}
		}
		return opt
	} else if _, ok := p.accept(tokPriority); ok {
		p.accept(tokEq)
		opt.Tp = ast.ResourcePriority
		if _, ok := p.accept(tokLow); ok {
			opt.UintValue = ast.LowPriorityValue
		} else if _, ok := p.accept(tokMedium); ok {
			opt.UintValue = ast.MediumPriorityValue
		} else if _, ok := p.accept(tokHigh); ok {
			opt.UintValue = ast.HighPriorityValue
		}
		return opt
	} else if _, ok := p.accept(tokBurstable); ok {
		opt.Tp = ast.ResourceBurstable
		if _, ok := p.accept(tokEq); ok {
			if _, ok := p.accept(tokUnlimited); ok {
				opt.Burstable = ast.BurstableUnlimited
			} else if _, ok := p.accept(tokModerated); ok {
				opt.Burstable = ast.BurstableModerated
			} else if _, ok := p.accept(tokOff); ok {
				opt.Burstable = ast.BurstableDisable
			}
		} else {
			// bare BURSTABLE without = means MODERATED
			opt.Burstable = ast.BurstableModerated
		}
		return opt
	} else if _, ok := p.accept(tokQueryLim); ok {
		p.accept(tokEq)
		opt.Tp = ast.ResourceGroupRunaway
		// QUERY_LIMIT = NULL or QUERY_LIMIT = (...)
		if _, ok := p.accept(tokNull); ok {
			// NULL means empty list -> disable
			return opt
		}
		p.expect('(')
		seenRunawayTypes := make(map[ast.RunawayOptionType]bool)
		for {
			if _, ok := p.accept(')'); ok {
				break
			}
			p.accept(',') // optional comma separator
			runawayOpt := p.parseRunawayOption()
			if runawayOpt != nil {
				// Only ACTION and WATCH can appear at most once. Multiple rules
				// (EXEC_ELAPSED, RU, PROCESSED_KEYS) are allowed.
				if runawayOpt.Tp != ast.RunawayRule && seenRunawayTypes[runawayOpt.Tp] {
					p.error(p.peek().Offset, "duplicate QUERY_LIMIT sub-option")
					return opt
				}
				seenRunawayTypes[runawayOpt.Tp] = true
				opt.RunawayOptionList = append(opt.RunawayOptionList, runawayOpt)
			} else {
				// Skip unexpected tokens; error on unclosed parentheses
				if p.peek().Tp == EOF {
					p.error(p.peek().Offset, "unclosed '(' in QUERY_LIMIT")
					return opt
				}
				if p.peek().Tp == ')' {
					p.accept(')')
					break
				}
				p.next()
			}
		}
		return opt
	} else if _, ok := p.accept(tokBackground); ok {
		p.accept(tokEq)
		opt.Tp = ast.ResourceGroupBackground
		if _, ok := p.accept(tokNull); ok {
			return opt
		}
		p.expect('(')
		for {
			if _, ok := p.accept(')'); ok {
				break
			}
			p.accept(',')
			if _, ok := p.accept(tokTaskTypes); ok {
				p.accept(tokEq)
				bgOpt := &ast.ResourceGroupBackgroundOption{
					Type: ast.BackgroundOptionTaskNames,
				}
				if tok, ok := p.accept(tokStringLit); ok {
					bgOpt.StrValue = tok.Lit
				}
				opt.BackgroundOptions = append(opt.BackgroundOptions, bgOpt)
			} else if _, ok := p.accept(tokUtilizationLimit); ok {
				p.accept(tokEq)
				bgOpt := &ast.ResourceGroupBackgroundOption{
					Type: ast.BackgroundUtilizationLimit,
				}
				t, ok := p.expect(tokIntLit) // must be integer
				if !ok {
					p.error(t.Offset, "UTILIZATION_LIMIT requires an integer value")
					return opt
				}
				switch v := t.Item.(type) {
				case int64:
					bgOpt.UintValue = uint64(v)
				case uint64:
					bgOpt.UintValue = v
				}
				opt.BackgroundOptions = append(opt.BackgroundOptions, bgOpt)
			} else {
				if p.peek().Tp == ')' || p.peek().Tp == EOF {
					p.accept(')')
					break
				}
				p.next()
			}
		}
		return opt
	}
	return nil
}

// parseRunawayOption parses a single runaway sub-option (EXEC_ELAPSED, ACTION, WATCH).
func (p *HandParser) parseRunawayOption() *ast.ResourceGroupRunawayOption {
	if _, ok := p.accept(tokExecElapsed); ok {
		p.accept(tokEq)
		rule := &ast.ResourceGroupRunawayRuleOption{Tp: ast.RunawayRuleExecElapsed}
		if tok, ok := p.accept(tokStringLit); ok {
			rule.ExecElapsed = tok.Lit
		}
		return &ast.ResourceGroupRunawayOption{
			Tp:         ast.RunawayRule,
			RuleOption: rule,
		}
	} else if _, ok := p.accept(tokProcessedKeys); ok {
		p.accept(tokEq)
		rule := &ast.ResourceGroupRunawayRuleOption{Tp: ast.RunawayRuleProcessedKeys}
		if t, ok := p.accept(tokIntLit); ok {
			rule.ProcessedKeys = t.Item.(int64)
		}
		return &ast.ResourceGroupRunawayOption{
			Tp:         ast.RunawayRule,
			RuleOption: rule,
		}
	} else if _, ok := p.accept(tokRU); ok {
		p.accept(tokEq)
		rule := &ast.ResourceGroupRunawayRuleOption{Tp: ast.RunawayRuleRequestUnit}
		if t, ok := p.accept(tokIntLit); ok {
			rule.RequestUnit = t.Item.(int64)
		}
		return &ast.ResourceGroupRunawayOption{
			Tp:         ast.RunawayRule,
			RuleOption: rule,
		}
	} else if _, ok := p.accept(tokAction); ok {
		p.accept(tokEq)
		action := &ast.ResourceGroupRunawayActionOption{}
		if _, ok := p.accept(tokCooldown); ok {
			action.Type = ast.RunawayActionCooldown
		} else if _, ok := p.accept(tokKill); ok {
			action.Type = ast.RunawayActionKill
		} else if _, ok := p.accept(tokDryRun); ok {
			action.Type = ast.RunawayActionDryRun
		} else if _, ok := p.accept(tokSwitchGroup); ok {
			action.Type = ast.RunawayActionSwitchGroup
			p.expect('(')
			tok := p.next()
			action.SwitchGroupName = ast.NewCIStr(tok.Lit)
			p.expect(')')
		}
		return &ast.ResourceGroupRunawayOption{
			Tp:           ast.RunawayAction,
			ActionOption: action,
		}
	} else if _, ok := p.accept(tokWatch); ok {
		p.accept(tokEq)
		watch := &ast.ResourceGroupRunawayWatchOption{}
		if _, ok := p.accept(tokSimilar); ok {
			watch.Type = ast.WatchSimilar
		} else if _, ok := p.accept(tokExact); ok {
			watch.Type = ast.WatchExact
		} else if p.peek().IsKeyword("PLAN") {
			p.next()
			watch.Type = ast.WatchPlan
		}
		// DURATION = '...' or DURATION = UNLIMITED
		if _, ok := p.accept(tokDuration); ok {
			p.accept(tokEq)
			if _, ok := p.accept(tokUnlimited); ok {
				watch.Duration = "" // empty string = unlimited in Restore
			} else if tok, ok := p.accept(tokStringLit); ok {
				if strings.EqualFold(tok.Lit, "UNLIMITED") {
					watch.Duration = "" // 'UNLIMITED' string treated as keyword
				} else {
					watch.Duration = tok.Lit
				}
			}
		}
		return &ast.ResourceGroupRunawayOption{
			Tp:          ast.RunawayWatch,
			WatchOption: watch,
		}
	}
	return nil
}

// parseResourceGroupOptionList parses a list of resource group options.
func (p *HandParser) parseResourceGroupOptionList() []*ast.ResourceGroupOption {
	var opts []*ast.ResourceGroupOption
	for {
		p.accept(',') // optional comma between options
		if opt := p.parseResourceGroupOption(); opt != nil {
			opts = append(opts, opt)
		} else {
			break
		}
	}
	return opts
}
