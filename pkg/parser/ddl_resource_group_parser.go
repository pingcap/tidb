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
	"strings"

	"github.com/pingcap/tidb/pkg/parser/ast"
)

// parseCreateResourceGroupStmt parses CREATE RESOURCE GROUP statements.
func (p *HandParser) parseCreateResourceGroupStmt() ast.StmtNode {
	stmt := Alloc[ast.CreateResourceGroupStmt](p.arena)
	p.expect(57389)
	p.expect(57869) // 57869
	p.expect(57438)

	stmt.IfNotExists = p.acceptIfNotExists()

	stmt.ResourceGroupName = ast.NewCIStr(p.next().Lit)

	stmt.ResourceGroupOptionList = p.parseResourceGroupOptionList()
	return stmt
}

// parseAlterResourceGroupStmt parses ALTER RESOURCE GROUP statements.
func (p *HandParser) parseAlterResourceGroupStmt() ast.StmtNode {
	stmt := Alloc[ast.AlterResourceGroupStmt](p.arena)
	p.expect(57365)
	p.expect(57869) // 57869
	p.expect(57438)

	stmt.IfExists = p.acceptIfExists()

	stmt.ResourceGroupName = ast.NewCIStr(p.next().Lit)

	stmt.ResourceGroupOptionList = p.parseResourceGroupOptionList()
	return stmt
}

// parseDropResourceGroupStmt parses DROP RESOURCE GROUP statements.
func (p *HandParser) parseDropResourceGroupStmt() ast.StmtNode {
	stmt := Alloc[ast.DropResourceGroupStmt](p.arena)
	// p.expect(57415) - already consumed
	p.expect(57869) // 57869
	p.expect(57438)

	stmt.IfExists = p.acceptIfExists()

	stmt.ResourceGroupName = ast.NewCIStr(p.next().Lit)
	return stmt
}

// parseResourceGroupOption parses a single resource group option.
func (p *HandParser) parseResourceGroupOption() *ast.ResourceGroupOption {
	opt := &ast.ResourceGroupOption{}
	if _, ok := p.accept(58070); ok {
		p.accept(58202)
		opt.Tp = ast.ResourceRURate
		if _, ok := p.accept(58110); ok {
			opt.Burstable = ast.BurstableUnlimited
		} else if t, ok := p.accept(58197); ok {
			switch v := t.Item.(type) {
			case int64:
				opt.UintValue = uint64(v)
			case uint64:
				opt.UintValue = v
			}
		}
		return opt
	} else if _, ok := p.accept(58060); ok {
		p.accept(58202)
		opt.Tp = ast.ResourcePriority
		if _, ok := p.accept(58045); ok {
			opt.UintValue = ast.LowPriorityValue
		} else if _, ok := p.accept(58047); ok {
			opt.UintValue = ast.MediumPriorityValue
		} else if _, ok := p.accept(58029); ok {
			opt.UintValue = ast.HighPriorityValue
		}
		return opt
	} else if _, ok := p.accept(58002); ok {
		opt.Tp = ast.ResourceBurstable
		if _, ok := p.accept(58202); ok {
			if _, ok := p.accept(58110); ok {
				opt.Burstable = ast.BurstableUnlimited
			} else if _, ok := p.accept(58111); ok {
				opt.Burstable = ast.BurstableModerated
			} else if _, ok := p.accept(57810); ok {
				opt.Burstable = ast.BurstableDisable
			}
		} else {
			// bare BURSTABLE without = means MODERATED
			opt.Burstable = ast.BurstableModerated
		}
		return opt
	} else if _, ok := p.accept(58062); ok {
		p.accept(58202)
		opt.Tp = ast.ResourceGroupRunaway
		// QUERY_LIMIT = NULL or QUERY_LIMIT = (...)
		if _, ok := p.accept(57502); ok {
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
	} else if _, ok := p.accept(57995); ok {
		p.accept(58202)
		opt.Tp = ast.ResourceGroupBackground
		if _, ok := p.accept(57502); ok {
			return opt
		}
		p.expect('(')
		for {
			if _, ok := p.accept(')'); ok {
				break
			}
			p.accept(',')
			if _, ok := p.accept(58091); ok {
				p.accept(58202)
				bgOpt := &ast.ResourceGroupBackgroundOption{
					Type: ast.BackgroundOptionTaskNames,
				}
				if tok, ok := p.accept(57353); ok {
					bgOpt.StrValue = tok.Lit
				}
				opt.BackgroundOptions = append(opt.BackgroundOptions, bgOpt)
			} else if _, ok := p.accept(58113); ok {
				p.accept(58202)
				bgOpt := &ast.ResourceGroupBackgroundOption{
					Type: ast.BackgroundUtilizationLimit,
				}
				t, ok := p.expect(58197) // must be integer
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
	if _, ok := p.accept(58018); ok {
		p.accept(58202)
		rule := &ast.ResourceGroupRunawayRuleOption{Tp: ast.RunawayRuleExecElapsed}
		if tok, ok := p.accept(57353); ok {
			rule.ExecElapsed = tok.Lit
		}
		return &ast.ResourceGroupRunawayOption{
			Tp:         ast.RunawayRule,
			RuleOption: rule,
		}
	} else if _, ok := p.accept(58061); ok {
		p.accept(58202)
		rule := &ast.ResourceGroupRunawayRuleOption{Tp: ast.RunawayRuleProcessedKeys}
		if t, ok := p.accept(58197); ok {
			rule.ProcessedKeys = t.Item.(int64)
		}
		return &ast.ResourceGroupRunawayOption{
			Tp:         ast.RunawayRule,
			RuleOption: rule,
		}
	} else if _, ok := p.accept(58068); ok {
		p.accept(58202)
		rule := &ast.ResourceGroupRunawayRuleOption{Tp: ast.RunawayRuleRequestUnit}
		if t, ok := p.accept(58197); ok {
			rule.RequestUnit = t.Item.(int64)
		}
		return &ast.ResourceGroupRunawayOption{
			Tp:         ast.RunawayRule,
			RuleOption: rule,
		}
	} else if _, ok := p.accept(57596); ok {
		p.accept(58202)
		action := &ast.ResourceGroupRunawayActionOption{}
		if _, ok := p.accept(58006); ok {
			action.Type = ast.RunawayActionCooldown
		} else if _, ok := p.accept(57469); ok {
			action.Type = ast.RunawayActionKill
		} else if _, ok := p.accept(58014); ok {
			action.Type = ast.RunawayActionDryRun
		} else if _, ok := p.accept(58089); ok {
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
	} else if _, ok := p.accept(58121); ok {
		p.accept(58202)
		watch := &ast.ResourceGroupRunawayWatchOption{}
		if _, ok := p.accept(58073); ok {
			watch.Type = ast.WatchSimilar
		} else if _, ok := p.accept(58017); ok {
			watch.Type = ast.WatchExact
		} else if p.peek().IsKeyword("PLAN") {
			p.next()
			watch.Type = ast.WatchPlan
		}
		// DURATION = '...' or DURATION = UNLIMITED
		if _, ok := p.accept(58093); ok {
			p.accept(58202)
			if _, ok := p.accept(58110); ok {
				watch.Duration = "" // empty string = unlimited in Restore
			} else if tok, ok := p.accept(57353); ok {
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
