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

// parseCreateTableOptions parses table options like ENGINE=InnoDB, CHARSET=utf8
func (p *HandParser) parseCreateTableOptions() []*ast.TableOption {
	var options []*ast.TableOption
	for {
		opt := p.parseTableOption()
		if opt == nil {
			break
		}
		options = append(options, opt)
		p.accept(',') // Options can be space or comma separated
	}
	return options
}

// parseTableOption parses a single table option.
func (p *HandParser) parseTableOption() *ast.TableOption {
	opt := Alloc[ast.TableOption](p.arena)
	switch p.peek().Tp {
	case 57599:
		p.next()
		p.accept(58202)
		if tok, ok := p.expect(57353); ok {
			opt.Tp = ast.TableOptionAffinity
			opt.StrValue = tok.Lit
		}
	case 57405:
		// DEFAULT [CHARACTER SET | CHARSET | COLLATE] = value
		// The Restore for CHARACTER SET and COLLATE already emits "DEFAULT " prefix,
		// so we just consume the DEFAULT keyword and delegate to the inner option.
		p.next()
		return p.parseTableOption()
	case 57703:
		p.parseTableOptionString(opt, ast.TableOptionEngine)
	case 57639, 57382, 57381:
		// yacc CharsetKw: CHARACTER SET | CHARSET | CHAR SET
		p.next()
		if p.peek().Tp == 57541 {
			p.next()
		}
		p.accept(58202)
		if tok, ok := p.expectAny(57346, 57353, 57373); ok {
			opt.Tp = ast.TableOptionCharset
			opt.StrValue = strings.ToLower(tok.Lit)
		}
	case 57384:
		p.next()
		p.accept(58202)
		if tok, ok := p.expectAny(57346, 57353, 57373); ok {
			opt.Tp = ast.TableOptionCollate
			opt.StrValue = strings.ToLower(tok.Lit)
		}
	case 57432:
		p.next()
		if !p.parseForceAutoOption(opt) {
			p.syntaxError(p.peek().Offset)
			return nil
		}
	case 57612:
		p.parseTableOptionUint(opt, ast.TableOptionAutoIncrement)
	case 57614:
		p.parseTableOptionUint(opt, ast.TableOptionAutoRandomBase)
	case 57655, 57665, 57829, 57698, 57890:
		var optTp ast.TableOptionType
		switch p.peek().Tp {
		case 57655:
			optTp = ast.TableOptionComment
		case 57665:
			optTp = ast.TableOptionConnection
		case 57829:
			optTp = ast.TableOptionPassword
		case 57698:
			optTp = ast.TableOptionEncryption
		default:
			optTp = ast.TableOptionSecondaryEngineAttribute
		}
		p.parseTableOptionStringLit(opt, optTp)
	case 57779:
		p.parseTableOptionUint(opt, ast.TableOptionMaxRows)
	case 57789:
		p.parseTableOptionUint(opt, ast.TableOptionMinRows)
	case 57616:
		p.parseTableOptionUint(opt, ast.TableOptionAvgRowLength)
	case 57882:
		p.next()
		p.accept(58202)
		p.parseTableOptionRowFormat(opt)
	case 57760:
		p.parseTableOptionUint(opt, ast.TableOptionKeyBlockSize)
	case 57660:
		p.parseTableOptionString(opt, ast.TableOptionCompression)
	case 58054:
		p.next()
		p.accept(57838)
		p.accept(58202)
		opt.Tp = ast.TableOptionPlacementPolicy
		// Value can be identifier, string literal, or DEFAULT keyword
		if tok, ok := p.expectAny(57346, 57353, 57405); ok {
			opt.StrValue = tok.Lit
		}

	case 57641, 57946:
		if p.peek().Tp == 57641 {
			p.parseTableOptionUint(opt, ast.TableOptionCheckSum)
		} else {
			p.parseTableOptionUint(opt, ast.TableOptionTableCheckSum)
		}
	case 57930:
		p.parseTableOptionDefaultOrDiscard(opt, ast.TableOptionStatsPersistent)
	case 57926, 57931:
		var optTp ast.TableOptionType
		if p.peek().Tp == 57926 {
			optTp = ast.TableOptionStatsAutoRecalc
		} else {
			optTp = ast.TableOptionStatsSamplePages
		}
		p.next()
		p.accept(58202)
		opt.Tp = optTp
		if _, ok := p.accept(57405); ok {
			opt.Default = true
		} else {
			opt.UintValue = p.parseUint64()
			if optTp == ast.TableOptionStatsAutoRecalc && opt.UintValue != 0 && opt.UintValue != 1 {
				p.syntaxError(p.peek().Offset)
				return nil
			}
		}
	case 57686:
		p.parseTableOptionUint(opt, ast.TableOptionDelayKeyWrite)
	case 57901:
		p.parseTableOptionUint(opt, ast.TableOptionShardRowID)
	case 57965:
		p.next()
		p.accept(58202)
		opt.Tp = ast.TableOptionTTL
		// TTL = col_name + INTERVAL N UNIT
		if tok, ok := p.expectAny(57346, 57353); ok {
			opt.ColumnName = Alloc[ast.ColumnName](p.arena)
			opt.ColumnName.Name = ast.NewCIStr(tok.Lit)
		}
		p.accept('+') // consume '+'
		p.expect(57462)
		opt.Value = p.parseExpression(precNone).(ast.ValueExpr)
		opt.TimeUnitValue = p.parseTimeUnit()
	case 57966:
		p.next()
		p.accept(58202)
		opt.Tp = ast.TableOptionTTLEnable
		if tok, ok := p.expect(57353); ok {
			switch strings.ToUpper(tok.Lit) {
			case "ON":
				opt.BoolValue = true
			case "OFF":
				opt.BoolValue = false
			default:
				p.error(tok.Offset, "expected ON or OFF for TTL_ENABLE")
				return nil
			}
		}
	case 57967:
		p.next()
		p.accept(58202)
		return p.parseTableOptionTTLJobInterval(opt)
	case 57820:
		p.parseTableOptionDefaultOrDiscard(opt, ast.TableOptionPackKeys)
	case 57751:
		p.next()
		p.accept(58202)
		opt.Tp = ast.TableOptionInsertMethod
		if tok, ok := p.expectAny(57346, 57799, 57724, 57763); ok {
			opt.StrValue = tok.Lit
			if tok.Tp == 57724 {
				opt.StrValue = "FIRST"
			} else if tok.Tp == 57763 {
				opt.StrValue = "LAST"
			} else if tok.Tp == 57799 {
				opt.StrValue = "NO"
			}
		}
	case 57889:
		p.next()
		p.accept(58202)
		if _, ok := p.accept(57502); ok {
			opt.Tp = ast.TableOptionSecondaryEngineNull
		} else if tok, ok := p.expectAny(57353, 57346); ok {
			opt.Tp = ast.TableOptionSecondaryEngine
			opt.StrValue = tok.Lit
		}
	case 57934:
		p.next()
		if _, ok := p.accept(57703); ok {
			// STORAGE ENGINE [=] engine_name (partition option)
			p.accept(58202)
			opt.Tp = ast.TableOptionEngine
			if tok, ok := p.expectAny(57346, 57353); ok {
				opt.StrValue = tok.Lit
			}
		} else if tok, ok := p.expectAny(57692, 57784); ok {
			opt.Tp = ast.TableOptionStorageMedia
			opt.StrValue = strings.ToUpper(tok.Lit)
		}
	case 57945:
		p.parseTableOptionString(opt, ast.TableOptionTablespace)
	case 57802:
		p.parseTableOptionUint(opt, ast.TableOptionNodegroup)
	case 57679, 57449:
		// DATA DIRECTORY / INDEX DIRECTORY = 'path'
		var optTp ast.TableOptionType
		if p.peek().Tp == 57679 {
			optTp = ast.TableOptionDataDirectory
		} else {
			optTp = ast.TableOptionIndexDirectory
		}
		p.next()
		p.expect(57688)
		p.accept(58202)
		opt.Tp = optTp
		if tok, ok := p.expect(57353); ok {
			opt.StrValue = tok.Lit
		}
	case 57822, 57823, 57824, 57961:
		var optTp ast.TableOptionType
		switch p.peek().Tp {
		case 57822:
			optTp = ast.TableOptionPageChecksum
		case 57823:
			optTp = ast.TableOptionPageCompressed
		case 57824:
			optTp = ast.TableOptionPageCompressionLevel
		default:
			optTp = ast.TableOptionTransactional
		}
		p.parseTableOptionUint(opt, optTp)
	case 57744:
		p.parseTableOptionString(opt, ast.TableOptionIetfQuotes)
	case 57896:
		// SEQUENCE [=] {1|0}
		p.next()
		p.accept(58202)
		opt.Tp = ast.TableOptionSequence
		if tok, ok := p.expect(58197); ok {
			opt.UintValue = tokenItemToUint64(tok.Item)
		}
	case 57610:
		p.next()
		p.accept(58202)
		opt.Tp = ast.TableOptionAutoextendSize
		// Value can be like '4M', '64K', etc. (identifier) or a plain integer.
		if tok, ok := p.expectAny(57346, 58197, 58196); ok {
			opt.StrValue = tok.Lit
		}
	case 57611:
		p.parseTableOptionUint(opt, ast.TableOptionAutoIdCache)
	case 57842:
		p.parseTableOptionUint(opt, ast.TableOptionPreSplitRegion)
	case 57568:
		p.next()
		p.accept(58202)
		p.expect('(')
		opt.Tp = ast.TableOptionUnion
		if p.peek().Tp != ')' {
			for {
				tn := p.parseTableName()
				if tn != nil {
					opt.TableNames = append(opt.TableNames, tn)
				}
				if _, ok := p.accept(','); !ok {
					break
				}
			}
		}
		p.expect(')')
	case 58183:
		p.next()
		p.accept(58202)
		opt.Tp = ast.TableOptionStatsBuckets
		if _, ok := p.accept(57405); ok {
			opt.Default = true
		} else if tok, ok := p.expect(58197); ok {
			opt.UintValue = tokenItemToUint64(tok.Item)
		} else {
			p.error(tok.Offset, "STATS_BUCKETS requires an integer value")
			return nil
		}
	case 58190:
		p.next()
		p.accept(58202)
		opt.Tp = ast.TableOptionStatsTopN
		if _, ok := p.accept(57405); ok {
			opt.Default = true
		} else if tok, ok := p.expect(58197); ok {
			opt.UintValue = tokenItemToUint64(tok.Item)
		} else {
			p.error(tok.Offset, "STATS_TOPN requires an integer value")
			return nil
		}
	case 57932:
		p.next()
		p.accept(58202)
		opt.Tp = ast.TableOptionStatsSampleRate
		if _, ok := p.accept(57405); ok {
			opt.Default = true
		} else {
			// Accepts int or float literal
			if tok, ok := p.expectAny(58197, 58196); ok {
				opt.Value = ast.NewValueExpr(tok.Item, "", "")
			}
		}
	case 57927:
		p.next()
		p.accept(58202)
		opt.Tp = ast.TableOptionStatsColsChoice
		if _, ok := p.accept(57405); ok {
			opt.Default = true
		} else if tok, ok := p.accept(57353); ok {
			opt.StrValue = tok.Lit
		}
	case 57928:
		p.next()
		p.accept(58202)
		opt.Tp = ast.TableOptionStatsColList
		if _, ok := p.accept(57405); ok {
			opt.Default = true
		} else if tok, ok := p.accept(57353); ok {
			opt.StrValue = tok.Lit
		}
	default:
		return nil
	}
	return opt
}

// parseTableOptionRowFormat populates a ROW_FORMAT table option.
// Caller already consumed ROW_FORMAT and optional '='.
func (p *HandParser) parseTableOptionRowFormat(opt *ast.TableOption) {
	opt.Tp = ast.TableOptionRowFormat
	tok := p.next()
	if tok.Tp == 57405 {
		opt.UintValue = uint64(ast.RowFormatDefault)
		return
	}
	switch strings.ToUpper(tok.Lit) {
	case "DYNAMIC":
		opt.UintValue = uint64(ast.RowFormatDynamic)
	case "FIXED":
		opt.UintValue = uint64(ast.RowFormatFixed)
	case "COMPRESSED":
		opt.UintValue = uint64(ast.RowFormatCompressed)
	case "REDUNDANT":
		opt.UintValue = uint64(ast.RowFormatRedundant)
	case "COMPACT":
		opt.UintValue = uint64(ast.RowFormatCompact)
	case "TOKUDB_DEFAULT":
		opt.UintValue = uint64(ast.TokuDBRowFormatDefault)
	case "TOKUDB_FAST":
		opt.UintValue = uint64(ast.TokuDBRowFormatFast)
	case "TOKUDB_SMALL":
		opt.UintValue = uint64(ast.TokuDBRowFormatSmall)
	case "TOKUDB_ZLIB":
		opt.UintValue = uint64(ast.TokuDBRowFormatZlib)
	case "TOKUDB_ZSTD":
		opt.UintValue = uint64(ast.TokuDBRowFormatZstd)
	case "TOKUDB_QUICKLZ":
		opt.UintValue = uint64(ast.TokuDBRowFormatQuickLZ)
	case "TOKUDB_LZMA":
		opt.UintValue = uint64(ast.TokuDBRowFormatLzma)
	case "TOKUDB_SNAPPY":
		opt.UintValue = uint64(ast.TokuDBRowFormatSnappy)
	case "TOKUDB_UNCOMPRESSED":
		opt.UintValue = uint64(ast.TokuDBRowFormatUncompressed)
	default:
		opt.UintValue = uint64(ast.RowFormatDefault)
	}
}

// parseTableOptionTTLJobInterval parses TTL_JOB_INTERVAL value with validation.
// Caller already consumed TTL_JOB_INTERVAL and optional '='.
func (p *HandParser) parseTableOptionTTLJobInterval(opt *ast.TableOption) *ast.TableOption {
	tok, ok := p.expect(57353)
	if !ok {
		return nil
	}
	opt.Tp = ast.TableOptionTTLJobInterval
	opt.StrValue = tok.Lit
	val := strings.TrimSpace(opt.StrValue)

	if strings.HasPrefix(val, "@") {
		p.error(tok.Offset, "invalid TTL_JOB_INTERVAL")
		return nil
	}

	// Find split point between number and unit
	i := 0
	for i < len(val) && (val[i] >= '0' && val[i] <= '9' || val[i] == '.') {
		i++
	}

	unitStr := strings.TrimSpace(val[i:])
	validUnits := []string{"YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND", "MICROSECOND", "h", "m", "s", "ms", "us", "ns"}
	found := false
	for _, u := range validUnits {
		if strings.EqualFold(unitStr, u) {
			found = true
			break
		}
	}
	if !found && unitStr != "" {
		p.error(tok.Offset, "invalid TTL_JOB_INTERVAL unit")
		return nil
	}
	if i > 0 && strings.Count(val[:i], ".") > 1 {
		p.error(tok.Offset, "invalid TTL_JOB_INTERVAL number")
		return nil
	}
	return opt
}

// parseForceAutoOption parses FORCE AUTO_INCREMENT|AUTO_RANDOM_BASE [=] value.
// FORCE must already be consumed. Returns true if a valid option was parsed.
func (p *HandParser) parseForceAutoOption(opt *ast.TableOption) bool {
	var optTp ast.TableOptionType
	switch p.peek().Tp {
	case 57612:
		optTp = ast.TableOptionAutoIncrement
	case 57614:
		optTp = ast.TableOptionAutoRandomBase
	default:
		return false
	}
	p.parseTableOptionUint(opt, optTp)
	opt.BoolValue = true
	return true
}

func (p *HandParser) parseTableOptionUint(opt *ast.TableOption, optTp ast.TableOptionType) {
	p.next()
	p.accept(58202)
	opt.Tp = optTp
	if tok, ok := p.expectAny(58197, 58196); ok {
		opt.UintValue = tokenItemToUint64(tok.Item)
	}
}

func (p *HandParser) parseTableOptionString(opt *ast.TableOption, optTp ast.TableOptionType) {
	p.next()
	p.accept(58202)
	opt.Tp = optTp
	tok := p.peek()
	if tok.Tp == 57353 || isIdentLike(tok.Tp) {
		opt.StrValue = p.next().Lit
	} else {
		p.error(tok.Offset, "expected identifier or string literal")
	}
}

// parseTableOptionStringLit parses a table option with only string literal value (no identifier):
// tok [=] 'stringValue'
func (p *HandParser) parseTableOptionStringLit(opt *ast.TableOption, optTp ast.TableOptionType) {
	p.next()
	p.accept(58202)
	opt.Tp = optTp
	if tok, ok := p.expect(57353); ok {
		opt.StrValue = tok.Lit
	}
}

// parseTableOptionDefaultOrDiscard parses a table option that TiDB doesn't fully support:
// always sets Default=true. If not DEFAULT, consumes and discards the integer value.
func (p *HandParser) parseTableOptionDefaultOrDiscard(opt *ast.TableOption, optTp ast.TableOptionType) {
	p.next()
	p.accept(58202)
	opt.Tp = optTp
	opt.Default = true
	if _, ok := p.accept(57405); !ok {
		p.parseUint64()
	}
}
