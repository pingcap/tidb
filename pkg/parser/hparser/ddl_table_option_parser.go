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
	case tokAffinity:
		p.next()
		p.accept(tokEq)
		if tok, ok := p.expect(tokStringLit); ok {
			opt.Tp = ast.TableOptionAffinity
			opt.StrValue = tok.Lit
		}
	case tokDefault:
		// DEFAULT [CHARACTER SET | CHARSET | COLLATE] = value
		// The Restore for CHARACTER SET and COLLATE already emits "DEFAULT " prefix,
		// so we just consume the DEFAULT keyword and delegate to the inner option.
		p.next()
		return p.parseTableOption()
	case tokEngine:
		p.parseTableOptionString(opt, ast.TableOptionEngine)
	case tokCharsetKwd, tokCharacter, tokChar:
		// yacc CharsetKw: CHARACTER SET | CHARSET | CHAR SET
		p.next()
		if p.peek().Tp == tokSet {
			p.next()
		}
		p.accept(tokEq)
		if tok, ok := p.expectAny(tokIdentifier, tokStringLit, tokBinary); ok {
			opt.Tp = ast.TableOptionCharset
			opt.StrValue = strings.ToLower(tok.Lit)
		}
	case tokCollate:
		p.next()
		p.accept(tokEq)
		if tok, ok := p.expectAny(tokIdentifier, tokStringLit, tokBinary); ok {
			opt.Tp = ast.TableOptionCollate
			opt.StrValue = strings.ToLower(tok.Lit)
		}
	case tokForce:
		p.next()
		if !p.parseForceAutoOption(opt) {
			p.syntaxError(p.peek().Offset)
			return nil
		}
	case tokAutoInc:
		p.parseTableOptionUint(opt, ast.TableOptionAutoIncrement)
	case tokAutoRandomBase:
		p.parseTableOptionUint(opt, ast.TableOptionAutoRandomBase)
	case tokComment, tokConnection, tokPassword, tokEncryption, tokSecondaryEngineAttribute:
		var optTp ast.TableOptionType
		switch p.peek().Tp {
		case tokComment:
			optTp = ast.TableOptionComment
		case tokConnection:
			optTp = ast.TableOptionConnection
		case tokPassword:
			optTp = ast.TableOptionPassword
		case tokEncryption:
			optTp = ast.TableOptionEncryption
		default:
			optTp = ast.TableOptionSecondaryEngineAttribute
		}
		p.parseTableOptionStringLit(opt, optTp)
	case tokMaxRows:
		p.parseTableOptionUint(opt, ast.TableOptionMaxRows)
	case tokMinRows:
		p.parseTableOptionUint(opt, ast.TableOptionMinRows)
	case tokAvgRowLength:
		p.parseTableOptionUint(opt, ast.TableOptionAvgRowLength)
	case tokRowFormat:
		p.next()
		p.accept(tokEq)
		p.parseTableOptionRowFormat(opt)
	case tokKeyBlockSize:
		p.parseTableOptionUint(opt, ast.TableOptionKeyBlockSize)
	case tokCompression:
		p.parseTableOptionString(opt, ast.TableOptionCompression)
	case tokPlacement:
		p.next()
		p.accept(tokPolicy)
		p.accept(tokEq)
		opt.Tp = ast.TableOptionPlacementPolicy
		// Value can be identifier, string literal, or DEFAULT keyword
		if tok, ok := p.expectAny(tokIdentifier, tokStringLit, tokDefault); ok {
			opt.StrValue = tok.Lit
		}

	case tokChecksum, tokTableChecksum:
		if p.peek().Tp == tokChecksum {
			p.parseTableOptionUint(opt, ast.TableOptionCheckSum)
		} else {
			p.parseTableOptionUint(opt, ast.TableOptionTableCheckSum)
		}
	case tokStatsPersistent:
		p.parseTableOptionDefaultOrDiscard(opt, ast.TableOptionStatsPersistent)
	case tokStatsAutoRecalc, tokStatsSamplePages:
		var optTp ast.TableOptionType
		if p.peek().Tp == tokStatsAutoRecalc {
			optTp = ast.TableOptionStatsAutoRecalc
		} else {
			optTp = ast.TableOptionStatsSamplePages
		}
		p.next()
		p.accept(tokEq)
		opt.Tp = optTp
		if _, ok := p.accept(tokDefault); ok {
			opt.Default = true
		} else {
			opt.UintValue = p.parseUint64()
			if optTp == ast.TableOptionStatsAutoRecalc && opt.UintValue != 0 && opt.UintValue != 1 {
				p.syntaxError(p.peek().Offset)
				return nil
			}
		}
	case tokDelayKeyWrite:
		p.parseTableOptionUint(opt, ast.TableOptionDelayKeyWrite)
	case tokShardRowIDBits:
		p.parseTableOptionUint(opt, ast.TableOptionShardRowID)
	case tokTTL:
		p.next()
		p.accept(tokEq)
		opt.Tp = ast.TableOptionTTL
		// TTL = col_name + INTERVAL N UNIT
		if tok, ok := p.expectAny(tokIdentifier, tokStringLit); ok {
			opt.ColumnName = Alloc[ast.ColumnName](p.arena)
			opt.ColumnName.Name = ast.NewCIStr(tok.Lit)
		}
		p.accept('+') // consume '+'
		p.expect(tokInterval)
		opt.Value = p.parseExpression(precNone).(ast.ValueExpr)
		opt.TimeUnitValue = p.parseTimeUnit()
	case tokTTLEnable:
		p.next()
		p.accept(tokEq)
		opt.Tp = ast.TableOptionTTLEnable
		if tok, ok := p.expect(tokStringLit); ok {
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
	case tokTTLJobInterval:
		p.next()
		p.accept(tokEq)
		return p.parseTableOptionTTLJobInterval(opt)
	case tokPackKeys:
		p.parseTableOptionDefaultOrDiscard(opt, ast.TableOptionPackKeys)
	case tokInsertMethod:
		p.next()
		p.accept(tokEq)
		opt.Tp = ast.TableOptionInsertMethod
		if tok, ok := p.expectAny(tokIdentifier, tokNo, tokFirst, tokLast); ok {
			opt.StrValue = tok.Lit
			if tok.Tp == tokFirst {
				opt.StrValue = "FIRST"
			} else if tok.Tp == tokLast {
				opt.StrValue = "LAST"
			} else if tok.Tp == tokNo {
				opt.StrValue = "NO"
			}
		}
	case tokSecondaryEngine:
		p.next()
		p.accept(tokEq)
		if _, ok := p.accept(tokNull); ok {
			opt.Tp = ast.TableOptionSecondaryEngineNull
		} else if tok, ok := p.expectAny(tokStringLit, tokIdentifier); ok {
			opt.Tp = ast.TableOptionSecondaryEngine
			opt.StrValue = tok.Lit
		}
	case tokStorage:
		p.next()
		if _, ok := p.accept(tokEngine); ok {
			// STORAGE ENGINE [=] engine_name (partition option)
			p.accept(tokEq)
			opt.Tp = ast.TableOptionEngine
			if tok, ok := p.expectAny(tokIdentifier, tokStringLit); ok {
				opt.StrValue = tok.Lit
			}
		} else if tok, ok := p.expectAny(tokDisk, tokMemory); ok {
			opt.Tp = ast.TableOptionStorageMedia
			opt.StrValue = strings.ToUpper(tok.Lit)
		}
	case tokTablespace:
		p.parseTableOptionString(opt, ast.TableOptionTablespace)
	case tokNodegroup:
		p.parseTableOptionUint(opt, ast.TableOptionNodegroup)
	case tokData, tokIndex:
		// DATA DIRECTORY / INDEX DIRECTORY = 'path'
		var optTp ast.TableOptionType
		if p.peek().Tp == tokData {
			optTp = ast.TableOptionDataDirectory
		} else {
			optTp = ast.TableOptionIndexDirectory
		}
		p.next()
		p.expect(tokDirectory)
		p.accept(tokEq)
		opt.Tp = optTp
		if tok, ok := p.expect(tokStringLit); ok {
			opt.StrValue = tok.Lit
		}
	case tokPageChecksum, tokPageCompressed, tokPageCompressionLevel, tokTransactional:
		var optTp ast.TableOptionType
		switch p.peek().Tp {
		case tokPageChecksum:
			optTp = ast.TableOptionPageChecksum
		case tokPageCompressed:
			optTp = ast.TableOptionPageCompressed
		case tokPageCompressionLevel:
			optTp = ast.TableOptionPageCompressionLevel
		default:
			optTp = ast.TableOptionTransactional
		}
		p.parseTableOptionUint(opt, optTp)
	case tokIETFQuotes:
		p.parseTableOptionString(opt, ast.TableOptionIetfQuotes)
	case tokSequence:
		// SEQUENCE [=] {1|0}
		p.next()
		p.accept(tokEq)
		opt.Tp = ast.TableOptionSequence
		if tok, ok := p.expect(tokIntLit); ok {
			opt.UintValue = tokenItemToUint64(tok.Item)
		}
	case tokAutoextendSize:
		p.next()
		p.accept(tokEq)
		opt.Tp = ast.TableOptionAutoextendSize
		// Value can be like '4M', '64K', etc. (identifier) or a plain integer.
		if tok, ok := p.expectAny(tokIdentifier, tokIntLit, tokDecLit); ok {
			opt.StrValue = tok.Lit
		}
	case tokAutoIdCache:
		p.parseTableOptionUint(opt, ast.TableOptionAutoIdCache)
	case tokPreSplitRegions:
		p.parseTableOptionUint(opt, ast.TableOptionPreSplitRegion)
	case tokUnion:
		p.next()
		p.accept(tokEq)
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
	case tokStatsBuckets:
		p.next()
		p.accept(tokEq)
		opt.Tp = ast.TableOptionStatsBuckets
		if _, ok := p.accept(tokDefault); ok {
			opt.Default = true
		} else if tok, ok := p.expect(tokIntLit); ok {
			opt.UintValue = tokenItemToUint64(tok.Item)
		} else {
			p.error(tok.Offset, "STATS_BUCKETS requires an integer value")
			return nil
		}
	case tokStatsTopN:
		p.next()
		p.accept(tokEq)
		opt.Tp = ast.TableOptionStatsTopN
		if _, ok := p.accept(tokDefault); ok {
			opt.Default = true
		} else if tok, ok := p.expect(tokIntLit); ok {
			opt.UintValue = tokenItemToUint64(tok.Item)
		} else {
			p.error(tok.Offset, "STATS_TOPN requires an integer value")
			return nil
		}
	case tokStatsSampleRate:
		p.next()
		p.accept(tokEq)
		opt.Tp = ast.TableOptionStatsSampleRate
		if _, ok := p.accept(tokDefault); ok {
			opt.Default = true
		} else {
			// Accepts int or float literal
			if tok, ok := p.expectAny(tokIntLit, tokDecLit); ok {
				opt.Value = ast.NewValueExpr(tok.Item, "", "")
			}
		}
	case tokStatsColChoice:
		p.next()
		p.accept(tokEq)
		opt.Tp = ast.TableOptionStatsColsChoice
		if _, ok := p.accept(tokDefault); ok {
			opt.Default = true
		} else if tok, ok := p.accept(tokStringLit); ok {
			opt.StrValue = tok.Lit
		}
	case tokStatsColList:
		p.next()
		p.accept(tokEq)
		opt.Tp = ast.TableOptionStatsColList
		if _, ok := p.accept(tokDefault); ok {
			opt.Default = true
		} else if tok, ok := p.accept(tokStringLit); ok {
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
	if tok.Tp == tokDefault {
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
	tok, ok := p.expect(tokStringLit)
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
	case tokAutoInc:
		optTp = ast.TableOptionAutoIncrement
	case tokAutoRandomBase:
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
	p.accept(tokEq)
	opt.Tp = optTp
	if tok, ok := p.expectAny(tokIntLit, tokDecLit); ok {
		opt.UintValue = tokenItemToUint64(tok.Item)
	}
}

func (p *HandParser) parseTableOptionString(opt *ast.TableOption, optTp ast.TableOptionType) {
	p.next()
	p.accept(tokEq)
	opt.Tp = optTp
	tok := p.peek()
	if tok.Tp == tokStringLit || isIdentLike(tok.Tp) {
		opt.StrValue = p.next().Lit
	} else {
		p.error(tok.Offset, "expected identifier or string literal")
	}
}

// parseTableOptionStringLit parses a table option with only string literal value (no identifier):
// tok [=] 'stringValue'
func (p *HandParser) parseTableOptionStringLit(opt *ast.TableOption, optTp ast.TableOptionType) {
	p.next()
	p.accept(tokEq)
	opt.Tp = optTp
	if tok, ok := p.expect(tokStringLit); ok {
		opt.StrValue = tok.Lit
	}
}

// parseTableOptionDefaultOrDiscard parses a table option that TiDB doesn't fully support:
// always sets Default=true. If not DEFAULT, consumes and discards the integer value.
func (p *HandParser) parseTableOptionDefaultOrDiscard(opt *ast.TableOption, optTp ast.TableOptionType) {
	p.next()
	p.accept(tokEq)
	opt.Tp = optTp
	opt.Default = true
	if _, ok := p.accept(tokDefault); !ok {
		p.parseUint64()
	}
}
