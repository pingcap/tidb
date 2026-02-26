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
	"github.com/pingcap/tidb/pkg/parser/charset"
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
	opt := p.arena.AllocTableOption()
	switch p.peek().Tp {
	case affinity:
		p.next()
		p.accept(eq)
		if tok, ok := p.expect(stringLit); ok {
			opt.Tp = ast.TableOptionAffinity
			opt.StrValue = tok.Lit
		}
	case defaultKwd:
		// DEFAULT [CHARACTER SET | CHARSET | COLLATE] = value
		// The Restore for CHARACTER SET and COLLATE already emits "DEFAULT " prefix,
		// so we just consume the DEFAULT keyword and delegate to the inner option.
		p.next()
		return p.parseTableOption()
	case engine:
		p.parseTableOptionString(opt, ast.TableOptionEngine)
	case charsetKwd, character, charType:
		// yacc CharsetKw: CHARACTER SET | CHARSET | CHAR SET
		p.next()
		if p.peek().Tp == set {
			p.next()
		}
		p.accept(eq)
		if tok, ok := p.acceptStringName(); ok {
			opt.Tp = ast.TableOptionCharset
			cs, err := charset.GetCharsetInfo(tok.Lit)
			if err != nil {
				p.errs = append(p.errs,
					ErrUnknownCharacterSet.GenWithStackByArgs(tok.Lit))
				return nil
			}
			opt.StrValue = cs.Name
		} else if tok, ok := p.accept(binaryType); ok {
			opt.Tp = ast.TableOptionCharset
			cs, err := charset.GetCharsetInfo(tok.Lit)
			if err != nil {
				p.errs = append(p.errs,
					ErrUnknownCharacterSet.GenWithStackByArgs(tok.Lit))
				return nil
			}
			opt.StrValue = cs.Name
		}
	case collate:
		p.next()
		p.accept(eq)
		if tok, ok := p.acceptStringName(); ok {
			opt.Tp = ast.TableOptionCollate
			coll, err := charset.GetCollationByName(tok.Lit)
			if err != nil {
				p.errs = append(p.errs, err)
				return nil
			}
			opt.StrValue = coll.Name
		} else if tok, ok := p.accept(binaryType); ok {
			opt.Tp = ast.TableOptionCollate
			coll, err := charset.GetCollationByName(tok.Lit)
			if err != nil {
				p.errs = append(p.errs, err)
				return nil
			}
			opt.StrValue = coll.Name
		}
	case force:
		p.next()
		if !p.parseForceAutoOption(opt) {
			p.syntaxError(p.peek().Offset)
			return nil
		}
	case autoIncrement:
		p.parseTableOptionUint(opt, ast.TableOptionAutoIncrement)
	case autoRandomBase:
		p.parseTableOptionUint(opt, ast.TableOptionAutoRandomBase)
	case comment, connection, password, encryption, engine_attribute, secondaryEngineAttribute:
		isEncryption := p.peek().Tp == encryption
		var optTp ast.TableOptionType
		switch p.peek().Tp {
		case comment:
			optTp = ast.TableOptionComment
		case connection:
			optTp = ast.TableOptionConnection
		case password:
			optTp = ast.TableOptionPassword
		case encryption:
			optTp = ast.TableOptionEncryption
		case engine_attribute:
			optTp = ast.TableOptionEngineAttribute
		default:
			optTp = ast.TableOptionSecondaryEngineAttribute
		}
		p.parseTableOptionStringLit(opt, optTp)
		if isEncryption {
			switch opt.StrValue {
			case "Y", "y":
				p.warnNear(p.peek().Offset, "The ENCRYPTION clause is parsed but ignored by all storage engines.")
			case "N", "n":
				// valid, no warning
			default:
				p.errs = append(p.errs, ErrWrongValue.GenWithStackByArgs("argument (should be Y or N)", opt.StrValue))
				return nil
			}
		}
	case maxRows:
		p.parseTableOptionUint(opt, ast.TableOptionMaxRows)
	case minRows:
		p.parseTableOptionUint(opt, ast.TableOptionMinRows)
	case avgRowLength:
		p.parseTableOptionUint(opt, ast.TableOptionAvgRowLength)
	case rowFormat:
		p.next()
		p.accept(eq)
		p.parseTableOptionRowFormat(opt)
	case keyBlockSize:
		p.parseTableOptionUint(opt, ast.TableOptionKeyBlockSize)
	case compression:
		p.parseTableOptionString(opt, ast.TableOptionCompression)
	case placement:
		p.next()
		p.accept(policy)
		p.accept(eq)
		opt.Tp = ast.TableOptionPlacementPolicy
		// Value can be identifier, string literal, or DEFAULT keyword
		if tok, ok := p.acceptStringName(); ok {
			opt.StrValue = tok.Lit
		} else if _, ok := p.accept(defaultKwd); ok {
			opt.StrValue = "DEFAULT"
		}

	case checksum, tableChecksum:
		if p.peek().Tp == checksum {
			p.parseTableOptionUint(opt, ast.TableOptionCheckSum)
		} else {
			p.parseTableOptionUint(opt, ast.TableOptionTableCheckSum)
		}
	case statsPersistent:
		p.parseTableOptionDefaultOrDiscard(opt, ast.TableOptionStatsPersistent)
	case statsAutoRecalc, statsSamplePages:
		var optTp ast.TableOptionType
		var warnMsg string
		if p.peek().Tp == statsAutoRecalc {
			optTp = ast.TableOptionStatsAutoRecalc
			warnMsg = "The STATS_AUTO_RECALC is parsed but ignored by all storage engines."
		} else {
			optTp = ast.TableOptionStatsSamplePages
			warnMsg = "The STATS_SAMPLE_PAGES is parsed but ignored by all storage engines."
		}
		p.next()
		p.accept(eq)
		opt.Tp = optTp
		if _, ok := p.accept(defaultKwd); ok {
			opt.Default = true
		} else {
			opt.UintValue = p.parseUint64()
			if optTp == ast.TableOptionStatsAutoRecalc && opt.UintValue != 0 && opt.UintValue != 1 {
				p.syntaxError(p.peek().Offset)
				return nil
			}
		}
		p.warnNear(p.peek().Offset, "%s", warnMsg)
	case delayKeyWrite:
		p.parseTableOptionUint(opt, ast.TableOptionDelayKeyWrite)
	case shardRowIDBits:
		p.parseTableOptionUint(opt, ast.TableOptionShardRowID)
	case ttl:
		p.next()
		p.accept(eq)
		opt.Tp = ast.TableOptionTTL
		// TTL = col_name + INTERVAL N UNIT
		if tok, ok := p.expectIdentLike(); ok {
			opt.ColumnName = p.arena.AllocColumnName()
			opt.ColumnName.Name = ast.NewCIStr(tok.Lit)
		}
		p.accept('+') // consume '+'
		p.expect(interval)
		opt.Value = p.parseExpression(precNone).(ast.ValueExpr)
		opt.TimeUnitValue = p.parseTimeUnit()
	case ttlEnable:
		p.next()
		p.accept(eq)
		opt.Tp = ast.TableOptionTTLEnable
		if tok, ok := p.expect(stringLit); ok {
			switch strings.ToUpper(tok.Lit) {
			case "ON":
				opt.BoolValue = true
			case "OFF":
				opt.BoolValue = false
			default:
				p.syntaxErrorAt(tok)
				return nil
			}
		}
	case ttlJobInterval:
		p.next()
		p.accept(eq)
		return p.parseTableOptionTTLJobInterval(opt)
	case packKeys:
		p.parseTableOptionDefaultOrDiscard(opt, ast.TableOptionPackKeys)
	case insertMethod:
		p.next()
		p.accept(eq)
		opt.Tp = ast.TableOptionInsertMethod
		if tok, ok := p.expectAny(identifier, no, first, last); ok {
			opt.StrValue = tok.Lit
			if tok.Tp == first {
				opt.StrValue = "FIRST"
			} else if tok.Tp == last {
				opt.StrValue = "LAST"
			} else if tok.Tp == no {
				opt.StrValue = "NO"
			}
		}
	case secondaryEngine:
		p.next()
		p.accept(eq)
		if _, ok := p.accept(null); ok {
			opt.Tp = ast.TableOptionSecondaryEngineNull
		} else if tok, ok := p.expectAny(stringLit, identifier); ok {
			opt.Tp = ast.TableOptionSecondaryEngine
			opt.StrValue = tok.Lit
		}
		p.warnNear(p.peek().Offset, "The SECONDARY_ENGINE clause is parsed but ignored by all storage engines.")
	case storage:
		p.next()
		if _, ok := p.accept(engine); ok {
			// STORAGE ENGINE [=] engine_name (partition option)
			p.accept(eq)
			opt.Tp = ast.TableOptionEngine
			if tok, ok := p.acceptStringName(); ok {
				opt.StrValue = tok.Lit
			}
		} else if tok, ok := p.expectAny(disk, memory); ok {
			opt.Tp = ast.TableOptionStorageMedia
			opt.StrValue = strings.ToUpper(tok.Lit)
			p.warnNear(p.peek().Offset, "The STORAGE clause is parsed but ignored by all storage engines.")
		}
	case tablespace:
		p.parseTableOptionString(opt, ast.TableOptionTablespace)
	case nodegroup:
		p.parseTableOptionUint(opt, ast.TableOptionNodegroup)
	case data, index:
		// DATA DIRECTORY / INDEX DIRECTORY = 'path'
		var optTp ast.TableOptionType
		if p.peek().Tp == data {
			optTp = ast.TableOptionDataDirectory
		} else {
			optTp = ast.TableOptionIndexDirectory
		}
		p.next()
		p.expect(directory)
		p.accept(eq)
		opt.Tp = optTp
		if tok, ok := p.expect(stringLit); ok {
			opt.StrValue = tok.Lit
		}
	case pageChecksum, pageCompressed, pageCompressionLevel, transactional:
		var optTp ast.TableOptionType
		var optName string
		switch p.peek().Tp {
		case pageChecksum:
			optTp = ast.TableOptionPageChecksum
			optName = "PAGE_CHECKSUM"
		case pageCompressed:
			optTp = ast.TableOptionPageCompressed
			optName = "PAGE_COMPRESSED"
		case pageCompressionLevel:
			optTp = ast.TableOptionPageCompressionLevel
			optName = "PAGE_COMPRESSION_LEVEL"
		default:
			optTp = ast.TableOptionTransactional
			optName = "TRANSACTIONAL"
		}
		p.parseTableOptionUint(opt, optTp)
		p.warnNear(p.peek().Offset, "The %s option is parsed but ignored by all storage engines.", optName)
	case ietfQuotes:
		p.parseTableOptionString(opt, ast.TableOptionIetfQuotes)
		p.warnNear(p.peek().Offset, "The IETF_QUOTES option is parsed but ignored by all storage engines.")
	case sequence:
		// SEQUENCE [=] {1|0}
		p.next()
		p.accept(eq)
		opt.Tp = ast.TableOptionSequence
		if tok, ok := p.expect(intLit); ok {
			opt.UintValue = tokenItemToUint64(tok.Item)
		}
		p.warnNear(p.peek().Offset,
			"The SEQUENCE option is parsed but ignored by all storage engines. Use CREATE SEQUENCE instead.")
	case autoextendSize:
		p.next()
		p.accept(eq)
		opt.Tp = ast.TableOptionAutoextendSize
		// Value can be like '4M', '64K', etc. (identifier) or a plain integer.
		if tok, ok := p.expectAny(identifier, intLit, decLit); ok {
			opt.StrValue = tok.Lit
		}
		p.warnNear(p.peek().Offset, "The AUTOEXTEND_SIZE option is parsed but ignored by all storage engines.")
	case autoIdCache:
		p.parseTableOptionUint(opt, ast.TableOptionAutoIdCache)
	case preSplitRegions:
		p.parseTableOptionUint(opt, ast.TableOptionPreSplitRegion)
	case union:
		p.next()
		p.accept(eq)
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
		p.warnNear(p.peek().Offset, "The UNION option is parsed but ignored by all storage engines.")
	case statsBuckets:
		p.next()
		p.accept(eq)
		opt.Tp = ast.TableOptionStatsBuckets
		if _, ok := p.accept(defaultKwd); ok {
			opt.Default = true
		} else if tok, ok := p.expect(intLit); ok {
			opt.UintValue = tokenItemToUint64(tok.Item)
		} else {
			p.error(tok.Offset, "STATS_BUCKETS requires an integer value")
			return nil
		}
	case statsTopN:
		p.next()
		p.accept(eq)
		opt.Tp = ast.TableOptionStatsTopN
		if _, ok := p.accept(defaultKwd); ok {
			opt.Default = true
		} else if tok, ok := p.expect(intLit); ok {
			opt.UintValue = tokenItemToUint64(tok.Item)
		} else {
			p.error(tok.Offset, "STATS_TOPN requires an integer value")
			return nil
		}
	case statsSampleRate:
		p.next()
		p.accept(eq)
		opt.Tp = ast.TableOptionStatsSampleRate
		if _, ok := p.accept(defaultKwd); ok {
			opt.Default = true
		} else {
			// Accepts int or float literal
			if tok, ok := p.expectAny(intLit, decLit); ok {
				opt.Value = ast.NewValueExpr(tok.Item, "", "")
			}
		}
	case statsColChoice:
		p.next()
		p.accept(eq)
		opt.Tp = ast.TableOptionStatsColsChoice
		if _, ok := p.accept(defaultKwd); ok {
			opt.Default = true
		} else if tok, ok := p.accept(stringLit); ok {
			opt.StrValue = tok.Lit
		}
	case statsColList:
		p.next()
		p.accept(eq)
		opt.Tp = ast.TableOptionStatsColList
		if _, ok := p.accept(defaultKwd); ok {
			opt.Default = true
		} else if tok, ok := p.accept(stringLit); ok {
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
	if tok.Tp == defaultKwd {
		opt.UintValue = ast.RowFormatDefault
		return
	}
	switch strings.ToUpper(tok.Lit) {
	case "DYNAMIC":
		opt.UintValue = ast.RowFormatDynamic
	case "FIXED":
		opt.UintValue = ast.RowFormatFixed
	case "COMPRESSED":
		opt.UintValue = ast.RowFormatCompressed
	case "REDUNDANT":
		opt.UintValue = ast.RowFormatRedundant
	case "COMPACT":
		opt.UintValue = ast.RowFormatCompact
	case "TOKUDB_DEFAULT":
		opt.UintValue = ast.TokuDBRowFormatDefault
	case "TOKUDB_FAST":
		opt.UintValue = ast.TokuDBRowFormatFast
	case "TOKUDB_SMALL":
		opt.UintValue = ast.TokuDBRowFormatSmall
	case "TOKUDB_ZLIB":
		opt.UintValue = ast.TokuDBRowFormatZlib
	case "TOKUDB_ZSTD":
		opt.UintValue = ast.TokuDBRowFormatZstd
	case "TOKUDB_QUICKLZ":
		opt.UintValue = ast.TokuDBRowFormatQuickLZ
	case "TOKUDB_LZMA":
		opt.UintValue = ast.TokuDBRowFormatLzma
	case "TOKUDB_SNAPPY":
		opt.UintValue = ast.TokuDBRowFormatSnappy
	case "TOKUDB_UNCOMPRESSED":
		opt.UintValue = ast.TokuDBRowFormatUncompressed
	default:
		opt.UintValue = ast.RowFormatDefault
	}
}

// parseTableOptionTTLJobInterval parses TTL_JOB_INTERVAL value with validation.
// Caller already consumed TTL_JOB_INTERVAL and optional '='.
func (p *HandParser) parseTableOptionTTLJobInterval(opt *ast.TableOption) *ast.TableOption {
	tok, ok := p.expect(stringLit)
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
	validUnits := []string{
		"YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND", "MICROSECOND",
		"d", "h", "m", "s", "ms", "us", "ns",
	}
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
	case autoIncrement:
		optTp = ast.TableOptionAutoIncrement
	case autoRandomBase:
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
	p.accept(eq)
	opt.Tp = optTp
	if tok, ok := p.expectAny(intLit, decLit); ok {
		opt.UintValue = tokenItemToUint64(tok.Item)
	}
}

func (p *HandParser) parseTableOptionString(opt *ast.TableOption, optTp ast.TableOptionType) {
	p.next()
	p.accept(eq)
	opt.Tp = optTp
	tok := p.peek()
	if tok.Tp == stringLit || isIdentLike(tok.Tp) {
		opt.StrValue = p.next().Lit
	} else {
		p.syntaxErrorAt(tok)
	}
}

// parseTableOptionStringLit parses a table option with only string literal value (no identifier):
// tok [=] 'stringValue'
func (p *HandParser) parseTableOptionStringLit(opt *ast.TableOption, optTp ast.TableOptionType) {
	p.next()
	p.accept(eq)
	opt.Tp = optTp
	if tok, ok := p.expect(stringLit); ok {
		opt.StrValue = tok.Lit
	}
}

// parseTableOptionDefaultOrDiscard parses a table option that TiDB doesn't fully support:
// always sets Default=true. If not DEFAULT, consumes and discards the integer value.
func (p *HandParser) parseTableOptionDefaultOrDiscard(opt *ast.TableOption, optTp ast.TableOptionType) {
	p.next()
	p.accept(eq)
	opt.Tp = optTp
	opt.Default = true
	if _, ok := p.accept(defaultKwd); !ok {
		p.parseUint64()
	}
}
