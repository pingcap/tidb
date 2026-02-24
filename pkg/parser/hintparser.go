// Copyright 2020 PingCAP, Inc.
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
	"math"
	"strconv"
	"strings"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
)

// Hint token constants used by the hint scanner (hintTokenMap in misc.go)
// and the parser dispatch logic below.
const (
	hintAggToCop              = change
	hintBCJoin                = daySecond
	hintBKA                   = doubleAtIdentifier
	hintBNL                   = hintComment
	hintDupsWeedOut           = foreign
	hintFalse                 = float4Type
	hintFirstMatch            = from
	hintForceIndex            = enclosed
	hintGB                    = force
	hintHashAgg               = check
	hintHashJoin              = pipes
	hintHashJoinBuild         = odbcDateType
	hintHashJoinProbe         = odbcTimeType
	hintHypoIndex             = caseKwd
	hintIdentifier            = asof
	hintIgnoreIndex           = constraint
	hintIgnorePlanCache       = charType
	hintIndexHashJoin         = cross
	hintIndexJoin             = continueKwd
	hintIndexLookUpPushDown   = distinct
	hintIndexMerge            = alter
	hintIndexMergeJoin        = currentTime
	hintInlHashJoin           = create
	hintInlJoin               = currentDate
	hintInlMergeJoin          = currentRole
	hintIntLit                = identifier
	hintInvalid               = toTimestamp
	hintJoinFixedOrder        = optionallyEnclosedBy
	hintJoinOrder             = underscoreCS
	hintJoinPrefix            = stringLit
	hintJoinSuffix            = singleAtIdentifier
	hintLeading               = except
	hintLimitToCop            = elseIfKwd
	hintLooseScan             = fulltext
	hintMB                    = forKwd
	hintMRR                   = and
	hintMaterialization       = generated
	hintMaxExecutionTime      = both
	hintMemoryQuota           = currentUser
	hintMerge                 = add
	hintMpp1PhaseAgg          = collate
	hintMpp2PhaseAgg          = column
	hintNoBKA                 = invalid
	hintNoBNL                 = andand
	hintNoDecorrelate         = exit
	hintNoHashJoin            = odbcTimestampType
	hintNoICP                 = as
	hintNoIndexHashJoin       = cumeDist
	hintNoIndexJoin           = convert
	hintNoIndexLookUpPushDown = distinctRow
	hintNoIndexMerge          = analyze
	hintNoIndexMergeJoin      = currentTs
	hintNoMRR                 = array
	hintNoMerge               = all
	hintNoOrderIndex          = describe
	hintNoRangeOptimization   = asc
	hintNoSMJoin              = dayMinute
	hintNoSemijoin            = blobType
	hintNoSkipScan            = bigIntType
	hintNoSwapJoinInputs      = cursor
	hintNthPlan               = elseKwd
	hintOLAP                  = explain
	hintOLTP                  = falseKwd
	hintOrderIndex            = desc
	hintPartition             = fetch
	hintQBName                = cascade
	hintQueryType             = database
	hintReadConsistentReplica = databases
	hintReadFromStorage       = dayHour
	hintResourceGroup         = call
	hintSMJoin                = dayMicrosecond
	hintSemiJoinRewrite       = exists
	hintSemijoin              = binaryType
	hintSetVar                = by
	hintShuffleJoin           = decimalType
	hintSingleAtIdentifier    = toTSO
	hintSkipScan              = between
	hintStraightJoin          = escaped
	hintStreamAgg             = defaultKwd
	hintStringLit             = memberof
	hintSwapJoinInputs        = delayed
	hintTiFlash               = floatType
	hintTiKV                  = firstValue
	hintTimeRange             = drop
	hintTrue                  = float8Type
	hintUseCascades           = dual
	hintUseIndex              = denseRank
	hintUseIndexMerge         = deleteKwd
	hintUsePlanCache          = div
	hintUseToja               = doubleType
	hintWriteSlowLog          = character
)

// hintToken is a lexed hint token with its metadata.
type hintToken struct {
	tp    int
	ident string
	num   uint64
}

// lex reads the next token from the hint scanner.
func (hp *hintParser) lex() hintToken {
	var lval hintLexVal
	tp := hp.lexer.Lex(&lval)
	if tp <= 0 {
		return hintToken{tp: 0} // EOF
	}
	return hintToken{tp: tp, ident: lval.ident, num: lval.number}
}

// peek returns the next token without consuming it.
func (hp *hintParser) peek() hintToken {
	if !hp.hasPeeked {
		hp.peeked = hp.lex()
		hp.hasPeeked = true
	}
	return hp.peeked
}

// next consumes and returns the next token.
func (hp *hintParser) next() hintToken {
	if hp.hasPeeked {
		hp.hasPeeked = false
		return hp.peeked
	}
	return hp.lex()
}

// match consumes the next token if it matches the given type. Returns true if matched.
func (hp *hintParser) match(tp int) bool {
	if hp.peek().tp == tp {
		hp.next()
		return true
	}
	return false
}

// expect consumes the next token and returns it.
// If it doesn't match, logs a parse error and returns false.
func (hp *hintParser) expect(tp int) (hintToken, bool) {
	tok := hp.next()
	if tok.tp != tp {
		hp.parseError()
		return tok, false
	}
	return tok, true
}

// skipToCloseParen skips all tokens until a matching ')' is found (for error recovery).
func (hp *hintParser) skipToCloseParen() {
	depth := 1
	for depth > 0 {
		tok := hp.next()
		if tok.tp == 0 { // EOF
			return
		}
		switch tok.tp {
		case '(':
			depth++
		case ')':
			depth--
		}
	}
}

// parseError emits a hint parse error with position info from the scanner.
func (hp *hintParser) parseError() {
	hp.lexer.AppendError(hp.lexer.Errorf(""))
}

// parseHintList parses the full optimizer hint comment: a list of hints separated
// by commas or whitespace (commas are optional for top-level separation).
func (hp *hintParser) parseHintList() []*ast.TableOptimizerHint {
	var result []*ast.TableOptimizerHint

	for hp.peek().tp != 0 { // until EOF
		hints := hp.parseOneHint()
		for _, h := range hints {
			if h != nil {
				result = append(result, h)
			}
		}

		// Optional comma between hints
		hp.match(',')
	}

	// If no hints were parsed and no errors were generated,
	// produce a parse error (matches old parser behavior for empty input).
	if len(result) == 0 {
		warns, errs := hp.lexer.Errors()
		if len(warns)+len(errs) == 0 {
			hp.parseError()
			hp.lastErrorAsWarn()
		}
	}

	return result
}

// parseOneHint dispatches to the appropriate parser based on the hint name.
// Some hints (like READ_FROM_STORAGE) can produce multiple hints, so we return a slice.
func (hp *hintParser) parseOneHint() []*ast.TableOptimizerHint {
	tok := hp.next()
	if tok.tp == 0 {
		return nil
	}

	name := tok.ident
	switch tok.tp {
	// ---- Nullary hints (no parens required, or with optional parens) ----
	case hintStraightJoin:
		if hp.peek().tp == '(' {
			return hp.parseNullaryWithParens(name)
		}
		return []*ast.TableOptimizerHint{{HintName: ast.NewCIStr(name)}}

	case hintJoinFixedOrder:
		if hp.peek().tp == '(' {
			// JOIN_FIXED_ORDER with parens is treated as unsupported
			return hp.parseUnsupportedHint(name)
		}
		return []*ast.TableOptimizerHint{{HintName: ast.NewCIStr(name)}}

	// These nullary hints accept optional parens and return valid hint nodes
	case hintAggToCop, hintReadConsistentReplica, hintIgnorePlanCache,
		hintHashAgg, hintMpp1PhaseAgg, hintMpp2PhaseAgg,
		hintStreamAgg,
		hintLimitToCop:
		if hp.peek().tp == '(' {
			return hp.parseNullaryWithParens(name)
		}
		return []*ast.TableOptimizerHint{{HintName: ast.NewCIStr(name)}}

	// ---- Table-level hints: NAME([qb] table, ...) ----
	case hintHashJoin, hintHashJoinBuild, hintHashJoinProbe,
		hintNoHashJoin, hintMerge, hintNoMerge,
		hintSMJoin, hintNoSMJoin, hintBCJoin, hintShuffleJoin,
		hintSwapJoinInputs, hintNoSwapJoinInputs,
		hintInlJoin, hintInlHashJoin, hintInlMergeJoin,
		hintIndexJoin, hintNoIndexJoin,
		hintIndexHashJoin, hintNoIndexHashJoin,
		hintIndexMergeJoin, hintNoIndexMergeJoin,
		hintIndexMerge, hintNoIndexMerge:
		return hp.parseTableLevelHint(name)

	// ---- Join-order hints: NAME([qb] table, ...) — same parse as table-level ----
	case hintJoinOrder, hintJoinPrefix, hintJoinSuffix:
		return hp.parseTableLevelHint(name)

	// ---- Index-level hints: NAME([qb] table [idx, ...]) ----
	case hintUseIndex, hintForceIndex, hintIgnoreIndex,
		hintUseIndexMerge, hintOrderIndex, hintNoOrderIndex,
		hintHypoIndex,
		hintIndexLookUpPushDown, hintNoIndexLookUpPushDown:
		return hp.parseIndexLevelHint(name)

	// ---- Boolean hints: NAME([qb] TRUE/FALSE) ----
	case hintUseToja, hintUseCascades:
		return hp.parseBooleanHint(name)

	// ---- WRITE_SLOW_LOG: without parens → nullary; with parens → boolean ----
	case hintWriteSlowLog:
		if hp.peek().tp == '(' {
			return hp.parseBooleanHint(name)
		}
		return []*ast.TableOptimizerHint{{HintName: ast.NewCIStr(name)}}

	// ---- SET_VAR(name = value) ----
	case hintSetVar:
		return hp.parseSetVarHint(name)

	// ---- MAX_EXECUTION_TIME([qb] N) ----
	case hintMaxExecutionTime:
		return hp.parseMaxExecTimeHint(name)

	// ---- NTH_PLAN([qb] N) ----
	case hintNthPlan:
		return hp.parseNthPlanHint(name)

	// ---- MEMORY_QUOTA([qb] N MB/GB) ----
	case hintMemoryQuota:
		return hp.parseMemoryQuotaHint(name)

	// ---- QUERY_TYPE([qb] OLAP/OLTP) ----
	case hintQueryType:
		return hp.parseQueryTypeHint(name)

	// ---- QB_NAME(name) ----
	case hintQBName:
		return hp.parseQBNameHint(name)

	// ---- RESOURCE_GROUP([qb] name) ----
	case hintResourceGroup:
		return hp.parseResourceGroupHint(name)

	// ---- TIME_RANGE(from, to) ----
	case hintTimeRange:
		return hp.parseTimeRangeHint(name)

	// ---- LEADING([qb] tbl|(...), ...) ----
	case hintLeading:
		return hp.parseLeadingHint(name)

	// ---- READ_FROM_STORAGE([qb] TIKV[t,...], TIFLASH[t,...], ...) ----
	case hintReadFromStorage:
		return hp.parseStorageHint(name)

	// ---- SEMIJOIN / NO_SEMIJOIN([qb] strategy, ...) ----
	case hintSemijoin, hintNoSemijoin:
		return hp.parseSemijoinHint(name)

	// ---- SEMI_JOIN_REWRITE([qb]) ---- like nullary with parens
	case hintSemiJoinRewrite, hintNoDecorrelate:
		return hp.parseNullaryWithParens(name)

	// ---- USE_PLAN_CACHE ----
	case hintUsePlanCache:
		return hp.parseNullaryWithParens(name)

	// ---- Unsupported MySQL hints (just emit a warning) ----
	case hintBKA, hintNoBKA, hintBNL, hintNoBNL,
		hintMRR, hintNoMRR, hintNoICP,
		hintNoRangeOptimization, hintSkipScan, hintNoSkipScan:
		return hp.parseUnsupportedHint(name)

	default:
		// Unknown hint — treat as unsupported (matching old grammar behavior
		// where unknown identifiers matched UnsupportedTableLevelOptimizerHintName)
		if hp.peek().tp == '(' {
			return hp.parseUnsupportedHint(name)
		}
		hp.warnUnsupportedHint(name)
		return nil
	}
}

// ─── Nullary Hints ────────────────────────────────────────────────

func (hp *hintParser) parseNullaryWithParens(name string) []*ast.TableOptimizerHint {
	if _, ok := hp.expect('('); !ok {
		return nil
	}
	qb := hp.parseQBName()
	if _, ok := hp.expect(')'); !ok {
		hp.skipToCloseParen()
		return nil
	}
	return []*ast.TableOptimizerHint{{
		HintName: ast.NewCIStr(name),
		QBName:   ast.NewCIStr(qb),
	}}
}

// ─── Table-Level Hints ────────────────────────────────────────────

func (hp *hintParser) parseTableLevelHint(name string) []*ast.TableOptimizerHint {
	if _, ok := hp.expect('('); !ok {
		return nil
	}

	qb := hp.parseQBName()
	h := &ast.TableOptimizerHint{
		HintName: ast.NewCIStr(name),
		QBName:   ast.NewCIStr(qb),
	}

	// Parse table list
	if hp.peek().tp != ')' {
		tbl := hp.parseHintTable()
		h.Tables = append(h.Tables, tbl)
		for hp.match(',') {
			tbl = hp.parseHintTable()
			h.Tables = append(h.Tables, tbl)
		}
	}

	if _, ok := hp.expect(')'); !ok {
		hp.skipToCloseParen()
	}
	return []*ast.TableOptimizerHint{h}
}

// ─── Index-Level Hints ────────────────────────────────────────────

func (hp *hintParser) parseIndexLevelHint(name string) []*ast.TableOptimizerHint {
	if _, ok := hp.expect('('); !ok {
		return nil
	}

	qb := hp.parseQBName()

	// USE_INDEX_MERGE allows empty arguments: USE_INDEX_MERGE()
	if strings.EqualFold(name, "use_index_merge") && hp.peek().tp == ')' {
		hp.next() // consume ')'
		return []*ast.TableOptimizerHint{{
			HintName: ast.NewCIStr(name),
			QBName:   ast.NewCIStr(qb),
		}}
	}

	tbl := hp.parseHintTable()

	h := &ast.TableOptimizerHint{
		HintName: ast.NewCIStr(name),
		QBName:   ast.NewCIStr(qb),
		Tables:   []ast.HintTable{tbl},
	}

	// Optional comma between table and index list (CommaOpt in grammar)
	hp.match(',')

	// Parse optional index name list
	if hp.peek().tp != ')' {
		idx := hp.parseIdentifier()
		h.Indexes = append(h.Indexes, ast.NewCIStr(idx))
		for hp.match(',') {
			idx = hp.parseIdentifier()
			h.Indexes = append(h.Indexes, ast.NewCIStr(idx))
		}
	}

	if _, ok := hp.expect(')'); !ok {
		hp.skipToCloseParen()
	}
	return []*ast.TableOptimizerHint{h}
}

// ─── Boolean Hints ────────────────────────────────────────────────

func (hp *hintParser) parseBooleanHint(name string) []*ast.TableOptimizerHint {
	if _, ok := hp.expect('('); !ok {
		return nil
	}

	qb := hp.parseQBName()
	tok := hp.next()
	var val bool
	switch tok.tp {
	case hintTrue:
		val = true
	case hintFalse:
		val = false
	default:
		hp.skipToCloseParen()
		return nil
	}

	if _, ok := hp.expect(')'); !ok {
		hp.skipToCloseParen()
	}
	return []*ast.TableOptimizerHint{{
		HintName: ast.NewCIStr(name),
		QBName:   ast.NewCIStr(qb),
		HintData: val,
	}}
}

// ─── SET_VAR ──────────────────────────────────────────────────────

// parseHintValue parses a single hint value: identifier, integer, negative number,
// float (decimal), or string literal. Returns (value, true) on success.
//
// The hint lexer tokenizes "-1" as '-' + int, "0.01" as a single decLit token
// (converted to hintIdentifier). This method handles all these forms.
func (hp *hintParser) parseHintValue() (string, bool) {
	tok := hp.peek()
	if tok.tp == hintInvalid || tok.tp == 0 {
		hp.next()
		hp.parseError()
		return "", false
	}
	switch tok.tp {
	case hintStringLit:
		hp.next()
		return tok.ident, true // can be "" for empty strings like sql_mode=""
	case '-':
		// Negative number: - followed by int or float identifier
		hp.next() // consume '-'
		numTok := hp.next()
		var value string
		if numTok.tp == hintIntLit {
			value = "-" + strconv.FormatUint(numTok.num, 10)
		} else {
			value = "-" + numTok.ident
		}
		// Check for float: -N.M
		if hp.peek().tp == '.' {
			hp.next() // consume '.'
			fracTok := hp.next()
			if fracTok.tp == hintIntLit {
				value += "." + strconv.FormatUint(fracTok.num, 10)
			}
		}
		return value, true
	default:
		hp.next()
		value := hp.identOrNumber(tok)
		if value == "" {
			hp.parseError()
			return "", false
		}
		// Check for float: N.M (when lexer didn't merge into a single decLit)
		if hp.peek().tp == '.' {
			hp.next() // consume '.'
			fracTok := hp.next()
			if fracTok.tp == hintIntLit {
				value += "." + strconv.FormatUint(fracTok.num, 10)
			}
		}
		return value, true
	}
}

func (hp *hintParser) parseSetVarHint(name string) []*ast.TableOptimizerHint {
	if _, ok := hp.expect('('); !ok {
		return nil
	}
	varName := hp.parseIdentifier()
	if _, ok := hp.expect('='); !ok {
		hp.skipToCloseParen()
		return nil
	}
	value, ok := hp.parseHintValue()
	if !ok {
		hp.skipToCloseParen()
		return nil
	}
	if _, ok := hp.expect(')'); !ok {
		hp.skipToCloseParen()
		return nil
	}
	return []*ast.TableOptimizerHint{{
		HintName: ast.NewCIStr(name),
		HintData: ast.HintSetVar{
			VarName: varName,
			Value:   value,
		},
	}}
}

// ─── MAX_EXECUTION_TIME ──────────────────────────────────────────

func (hp *hintParser) parseMaxExecTimeHint(name string) []*ast.TableOptimizerHint {
	if _, ok := hp.expect('('); !ok {
		return nil
	}
	qb := hp.parseQBName()
	tok := hp.next()
	if tok.tp != hintIntLit {
		hp.skipToCloseParen()
		return nil
	}
	if _, ok := hp.expect(')'); !ok {
		hp.skipToCloseParen()
	}
	return []*ast.TableOptimizerHint{{
		HintName: ast.NewCIStr(name),
		QBName:   ast.NewCIStr(qb),
		HintData: tok.num,
	}}
}

// ─── NTH_PLAN ────────────────────────────────────────────────────

func (hp *hintParser) parseNthPlanHint(name string) []*ast.TableOptimizerHint {
	if _, ok := hp.expect('('); !ok {
		return nil
	}
	qb := hp.parseQBName()
	tok := hp.next()
	if tok.tp != hintIntLit {
		hp.skipToCloseParen()
		return nil
	}
	if _, ok := hp.expect(')'); !ok {
		hp.skipToCloseParen()
	}
	return []*ast.TableOptimizerHint{{
		HintName: ast.NewCIStr(name),
		QBName:   ast.NewCIStr(qb),
		HintData: int64(tok.num),
	}}
}

// ─── MEMORY_QUOTA ────────────────────────────────────────────────

func (hp *hintParser) parseMemoryQuotaHint(name string) []*ast.TableOptimizerHint {
	if _, ok := hp.expect('('); !ok {
		return nil
	}
	qb := hp.parseQBName()
	numTok := hp.next()
	if numTok.tp != hintIntLit {
		hp.skipToCloseParen()
		return nil
	}
	unitTok := hp.next()
	var multiplier uint64
	switch unitTok.tp {
	case hintMB:
		multiplier = 1024 * 1024
	case hintGB:
		multiplier = 1024 * 1024 * 1024
	default:
		hp.skipToCloseParen()
		return nil
	}

	if _, ok := hp.expect(')'); !ok {
		hp.skipToCloseParen()
	}

	maxValue := uint64(math.MaxInt64) / multiplier
	if numTok.num <= maxValue {
		return []*ast.TableOptimizerHint{{
			HintName: ast.NewCIStr(name),
			QBName:   ast.NewCIStr(qb),
			HintData: int64(numTok.num * multiplier),
		}}
	}
	hp.lexer.AppendError(ErrWarnMemoryQuotaOverflow.GenWithStackByArgs(math.MaxInt))
	hp.lastErrorAsWarn()
	return nil
}

// ─── QUERY_TYPE ──────────────────────────────────────────────────

func (hp *hintParser) parseQueryTypeHint(name string) []*ast.TableOptimizerHint {
	if _, ok := hp.expect('('); !ok {
		return nil
	}
	qb := hp.parseQBName()
	tok := hp.next()
	var qtName string
	switch tok.tp {
	case hintOLAP:
		qtName = "OLAP"
	case hintOLTP:
		qtName = "OLTP"
	default:
		hp.skipToCloseParen()
		return nil
	}
	if _, ok := hp.expect(')'); !ok {
		hp.skipToCloseParen()
	}
	return []*ast.TableOptimizerHint{{
		HintName: ast.NewCIStr(name),
		QBName:   ast.NewCIStr(qb),
		HintData: ast.NewCIStr(qtName),
	}}
}

// ─── QB_NAME ─────────────────────────────────────────────────────

func (hp *hintParser) parseQBNameHint(name string) []*ast.TableOptimizerHint {
	if _, ok := hp.expect('('); !ok {
		return nil
	}
	tok := hp.next()
	// QB_NAME only accepts identifier tokens (not integers, strings, or @ident)
	if tok.tp != hintIdentifier && !hp.isHintKeyword(tok.tp) {
		hp.skipToCloseParen()
		hp.parseError()
		return nil
	}
	h := &ast.TableOptimizerHint{
		HintName: ast.NewCIStr(name),
		QBName:   ast.NewCIStr(tok.ident),
	}
	// Optional second argument: qb_name(name, viewName[.subViewName[...]])
	// The planner peels off Tables entries one-by-one during nested view
	// resolution, so v1.v produces [{TableName: v1}, {TableName: v}].
	if hp.match(',') {
		for {
			viewTok := hp.next()
			if viewTok.tp == hintSingleAtIdentifier {
				// @sel_N — query block reference, stored as QBName
				h.Tables = append(h.Tables, ast.HintTable{
					QBName: ast.NewCIStr(viewTok.ident),
				})
				break // @sel_N is always the last segment
			}
			if viewTok.tp != hintIdentifier && !hp.isHintKeyword(viewTok.tp) {
				hp.skipToCloseParen()
				hp.parseError()
				return nil
			}
			h.Tables = append(h.Tables, ast.HintTable{
				TableName: ast.NewCIStr(viewTok.ident),
			})
			if !hp.match('.') {
				break
			}
		}
	}
	if _, ok := hp.expect(')'); !ok {
		hp.skipToCloseParen()
		return nil
	}
	return []*ast.TableOptimizerHint{h}
}

// ─── RESOURCE_GROUP ──────────────────────────────────────────────

func (hp *hintParser) parseResourceGroupHint(name string) []*ast.TableOptimizerHint {
	if _, ok := hp.expect('('); !ok {
		return nil
	}
	qb := hp.parseQBName()
	ident := hp.parseIdentifier()
	if _, ok := hp.expect(')'); !ok {
		hp.skipToCloseParen()
	}
	return []*ast.TableOptimizerHint{{
		HintName: ast.NewCIStr(name),
		QBName:   ast.NewCIStr(qb),
		HintData: ident,
	}}
}

// ─── TIME_RANGE ──────────────────────────────────────────────────

func (hp *hintParser) parseTimeRangeHint(name string) []*ast.TableOptimizerHint {
	if _, ok := hp.expect('('); !ok {
		return nil
	}
	fromTok := hp.next()
	// TIME_RANGE requires string literal arguments
	if fromTok.tp != hintStringLit {
		hp.skipToCloseParen()
		hp.parseError()
		return nil
	}
	if _, ok := hp.expect(','); !ok {
		hp.skipToCloseParen()
		return nil
	}
	toTok := hp.next()
	if toTok.tp != hintStringLit {
		hp.skipToCloseParen()
		hp.parseError()
		return nil
	}
	if _, ok := hp.expect(')'); !ok {
		hp.skipToCloseParen()
	}
	return []*ast.TableOptimizerHint{{
		HintName: ast.NewCIStr(name),
		HintData: ast.HintTimeRange{
			From: fromTok.ident,
			To:   toTok.ident,
		},
	}}
}

// ─── LEADING ─────────────────────────────────────────────────────

func (hp *hintParser) parseLeadingHint(name string) []*ast.TableOptimizerHint {
	if _, ok := hp.expect('('); !ok {
		return nil
	}
	qb := hp.parseQBName()
	leadingList := hp.parseLeadingTableList()
	if _, ok := hp.expect(')'); !ok {
		hp.skipToCloseParen()
	}

	h := &ast.TableOptimizerHint{
		HintName: ast.NewCIStr(name),
		QBName:   ast.NewCIStr(qb),
		HintData: leadingList,
	}
	if leadingList != nil {
		h.Tables = ast.FlattenLeadingList(leadingList)
	}
	return []*ast.TableOptimizerHint{h}
}

func (hp *hintParser) parseLeadingTableList() *ast.LeadingList {
	ll := &ast.LeadingList{}
	elem := hp.parseLeadingElement()
	ll.Items = append(ll.Items, elem)
	for hp.match(',') {
		elem = hp.parseLeadingElement()
		ll.Items = append(ll.Items, elem)
	}
	return ll
}

func (hp *hintParser) parseLeadingElement() interface{} {
	if hp.match('(') {
		inner := hp.parseLeadingTableList()
		hp.expect(')')
		return inner
	}
	tbl := hp.parseHintTable()
	return &tbl
}

// ─── READ_FROM_STORAGE ──────────────────────────────────────────

func (hp *hintParser) parseStorageHint(name string) []*ast.TableOptimizerHint {
	if _, ok := hp.expect('('); !ok {
		return nil
	}
	qb := hp.parseQBName()

	var results []*ast.TableOptimizerHint
	for {
		storeTok := hp.next()
		var storeName string
		switch storeTok.tp {
		case hintTiKV:
			storeName = "TIKV"
		case hintTiFlash:
			storeName = "TIFLASH"
		default:
			hp.skipToCloseParen()
			return results
		}

		h := &ast.TableOptimizerHint{
			HintName: ast.NewCIStr(name),
			QBName:   ast.NewCIStr(qb),
			HintData: ast.NewCIStr(storeName),
		}

		// Parse table list in brackets: TIKV[t1, t2]
		if hp.match('[') {
			tbl := hp.parseHintTable()
			h.Tables = append(h.Tables, tbl)
			for hp.match(',') {
				tbl = hp.parseHintTable()
				h.Tables = append(h.Tables, tbl)
			}
			hp.expect(']')
		}

		results = append(results, h)

		if !hp.match(',') {
			break
		}
	}

	if _, ok := hp.expect(')'); !ok {
		hp.skipToCloseParen()
	}
	return results
}

// ─── SEMIJOIN / NO_SEMIJOIN ──────────────────────────────────────

func (hp *hintParser) parseSemijoinHint(name string) []*ast.TableOptimizerHint {
	if _, ok := hp.expect('('); !ok {
		return nil
	}
	qb := hp.parseQBName()

	h := &ast.TableOptimizerHint{
		HintName: ast.NewCIStr(name),
		QBName:   ast.NewCIStr(qb),
	}

	// Parse optional strategy list
	if hp.peek().tp != ')' {
		strategy := hp.parseIdentifier()
		h.Tables = append(h.Tables, ast.HintTable{TableName: ast.NewCIStr(strategy)})
		for hp.match(',') {
			strategy = hp.parseIdentifier()
			h.Tables = append(h.Tables, ast.HintTable{TableName: ast.NewCIStr(strategy)})
		}
	}

	if _, ok := hp.expect(')'); !ok {
		hp.skipToCloseParen()
	}
	return []*ast.TableOptimizerHint{h}
}

// ─── Unsupported Hints ───────────────────────────────────────────

func (hp *hintParser) parseUnsupportedHint(name string) []*ast.TableOptimizerHint {
	if hp.match('(') {
		hp.skipToCloseParen()
	}
	hp.warnUnsupportedHint(name)
	return nil
}

// ─── Common Helpers ──────────────────────────────────────────────

// parseQBName parses an optional @qb_name prefix. Returns "" if not present.
func (hp *hintParser) parseQBName() string {
	if hp.peek().tp == hintSingleAtIdentifier {
		tok := hp.next()
		return tok.ident
	}
	return ""
}

// parseHintTable parses: Identifier [. Identifier] [QueryBlockOpt] [PartitionListOpt]
func (hp *hintParser) parseHintTable() ast.HintTable {
	name1 := hp.parseIdentifier()

	var tbl ast.HintTable
	if hp.match('.') {
		// db.table
		name2 := hp.parseIdentifier()
		tbl.DBName = ast.NewCIStr(name1)
		tbl.TableName = ast.NewCIStr(name2)
	} else {
		tbl.TableName = ast.NewCIStr(name1)
	}

	tbl.QBName = ast.NewCIStr(hp.parseQBName())
	tbl.PartitionList = hp.parsePartitionListOpt()
	return tbl
}

// parsePartitionListOpt parses: [PARTITION(name, ...)]
func (hp *hintParser) parsePartitionListOpt() []ast.CIStr {
	if hp.peek().tp != hintPartition {
		return nil
	}
	hp.next() // consume PARTITION
	if _, ok := hp.expect('('); !ok {
		return nil
	}
	var parts []ast.CIStr
	ident := hp.parseIdentifier()
	parts = append(parts, ast.NewCIStr(ident))
	for hp.match(',') {
		ident = hp.parseIdentifier()
		parts = append(parts, ast.NewCIStr(ident))
	}
	hp.expect(')')
	return parts
}

// parseIdentifier reads the next token and returns its identifier value.
// Almost all hint tokens can be used as identifiers (since hint keywords
// are context-sensitive). This matches the `Identifier` non-terminal in
// the original grammar which accepted almost all hint keyword tokens.
func (hp *hintParser) parseIdentifier() string {
	tok := hp.next()
	if tok.ident != "" {
		return tok.ident
	}
	// Integer literal → treat as identifier
	if tok.tp == hintIntLit {
		return strconv.FormatUint(tok.num, 10)
	}
	return ""
}

// identOrNumber converts a token to a string value, handling both identifiers
// and integer literals.
func (*hintParser) identOrNumber(tok hintToken) string {
	if tok.ident != "" {
		return tok.ident
	}
	if tok.tp == hintIntLit {
		return strconv.FormatUint(tok.num, 10)
	}
	return ""
}

// isHintKeyword returns true if the token type is a hint keyword
// (e.g., TRUE, FALSE, TIKV, TIFLASH, PARTITION, etc.) that can be
// used as an identifier in contexts like QB_NAME.
func (*hintParser) isHintKeyword(tp int) bool {
	// Hint keywords are in the range memberof+ but not special tokens
	switch tp {
	case hintIntLit, hintInvalid, hintSingleAtIdentifier, hintStringLit, 0:
		return false
	}
	return tp >= memberof && tp <= nthValue
}

//revive:disable:exported
var (
	ErrWarnOptimizerHintUnsupportedHint = terror.ClassParser.NewStd(mysql.ErrWarnOptimizerHintUnsupportedHint)
	ErrWarnOptimizerHintInvalidToken    = terror.ClassParser.NewStd(mysql.ErrWarnOptimizerHintInvalidToken)
	ErrWarnMemoryQuotaOverflow          = terror.ClassParser.NewStd(mysql.ErrWarnMemoryQuotaOverflow)
	ErrWarnOptimizerHintParseError      = terror.ClassParser.NewStd(mysql.ErrWarnOptimizerHintParseError)
	ErrWarnOptimizerHintInvalidInteger  = terror.ClassParser.NewStd(mysql.ErrWarnOptimizerHintInvalidInteger)
	ErrWarnOptimizerHintWrongPos        = terror.ClassParser.NewStd(mysql.ErrWarnOptimizerHintWrongPos)
)

//revive:enable:exported

// hintLexVal holds the semantic value for a hint token.
type hintLexVal struct {
	ident  string
	number uint64
}

// hintScanner implements the hint lexer interface.
type hintScanner struct {
	Scanner
}

func (hs *hintScanner) Errorf(format string, args ...interface{}) error {
	inner := hs.Scanner.Errorf(format, args...)
	return ErrParse.GenWithStackByArgs("Optimizer hint syntax error at", inner)
}

func (hs *hintScanner) Lex(lval *hintLexVal) int {
	tok, pos, lit := hs.scan()
	hs.lastScanOffset = pos.Offset
	var errorTokenType string

	switch tok {
	case intLit:
		n, e := strconv.ParseUint(lit, 10, 64)
		if e != nil {
			hs.AppendError(ErrWarnOptimizerHintInvalidInteger.GenWithStackByArgs(lit))
			return hintInvalid
		}
		lval.number = n
		return hintIntLit

	case singleAtIdentifier:
		lval.ident = lit
		return hintSingleAtIdentifier

	case identifier:
		lval.ident = lit
		if tok1, ok := hintTokenMap[strings.ToUpper(lit)]; ok {
			return tok1
		}
		return hintIdentifier

	case stringLit:
		lval.ident = lit
		if hs.sqlMode.HasANSIQuotesMode() && hs.r.s[pos.Offset] == '"' {
			return hintIdentifier
		}
		return hintStringLit

	case bitLit:
		if strings.HasPrefix(lit, "0b") {
			lval.ident = lit
			return hintIdentifier
		}
		errorTokenType = "bit-value literal"

	case hexLit:
		if strings.HasPrefix(lit, "0x") {
			lval.ident = lit
			return hintIdentifier
		}
		errorTokenType = "hexadecimal literal"

	case quotedIdentifier:
		lval.ident = lit
		return hintIdentifier

	case eq:
		return '='

	case floatLit, decLit:
		// Accept floating-point/decimal numbers as identifiers for set_var values.
		lval.ident = lit
		return hintIdentifier

	default:
		if tok <= 0x7f {
			return tok
		}
		errorTokenType = "unknown token"
	}

	hs.AppendError(ErrWarnOptimizerHintInvalidToken.GenWithStackByArgs(errorTokenType, lit, tok))
	return hintInvalid
}

// hintParser is the hand-written recursive-descent hint parser.
type hintParser struct {
	lexer     hintScanner
	result    []*ast.TableOptimizerHint
	hasPeeked bool
	peeked    hintToken
}

func newHintParser() *hintParser {
	return &hintParser{}
}

func (hp *hintParser) parse(input string, sqlMode mysql.SQLMode, initPos Pos) ([]*ast.TableOptimizerHint, []error) {
	hp.result = nil
	hp.hasPeeked = false
	hp.lexer.reset(input[3:])
	hp.lexer.SetSQLMode(sqlMode)
	hp.lexer.r.updatePos(Pos{
		Line:   initPos.Line,
		Col:    initPos.Col + 3, // skipped the initial '/*+'
		Offset: 0,
	})
	hp.lexer.inBangComment = true // skip the final '*/' (we need the '*/' for reporting warnings)

	hp.result = hp.parseHintList()

	warns, errs := hp.lexer.Errors()
	if len(errs) == 0 {
		errs = warns
	}
	return hp.result, errs
}

// ParseHint parses an optimizer hint (the interior of `/*+ ... */`).
func ParseHint(input string, sqlMode mysql.SQLMode, initPos Pos) ([]*ast.TableOptimizerHint, []error) {
	hp := newHintParser()
	return hp.parse(input, sqlMode, initPos)
}

func (hp *hintParser) warnUnsupportedHint(name string) {
	warn := ErrWarnOptimizerHintUnsupportedHint.FastGenByArgs(name)
	hp.lexer.warns = append(hp.lexer.warns, warn)
}

func (hp *hintParser) lastErrorAsWarn() {
	hp.lexer.lastErrorAsWarn()
}
