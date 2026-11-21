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

package stmtctx

import (
	"regexp"
	"slices"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
)

// maskingRedactedValue replaces a data value in a diagnostic of a statement
// that reads a masked column. It is the marker of tidb_redact_log = ON.
const maskingRedactedValue = "?"

// txnRetryableMark is kv.TxnRetryableMark, which cannot be imported here.
const txnRetryableMark = "[try again later]"

// errMaskingDiagnosticHidden replaces an execution error that has no error
// code of its own, so nothing tells which part of its text is a data value.
var errMaskingDiagnosticHidden = errors.NewNoStackError(
	"the error message is hidden because the statement reads a column masked by a masking policy")

// maskingExtraRedactArgPos lists the errors that embed data values in the
// arguments at the given positions although errno does not mark them for
// tidb_redact_log. The errors that errno marks need no entry here.
var maskingExtraRedactArgPos = map[uint16][]int{
	errno.ErrRegexp:                     {0},
	errno.ErrUnknownTimeZone:            {0},
	errno.ErrWrongValueForType:          {1},
	errno.ErrWrongStringLength:          {0},
	errno.ErrUnknownLocale:              {0},
	errno.ErrInvalidJSONData:            {1},
	errno.ErrInvalidJSONTextInParam:     {2, 3},
	errno.ErrInvalidJSONType:            {2},
	errno.ErrCannotConvertString:        {0},
	errno.ErrInvalidWeekModeFormat:      {0},
	errno.ErrLazyUniquenessCheckFailure: {0},
	errno.ErrUserLockWrongName:          {0},
	errno.ErrTimeStampInDSTTransition:   {0},
	errno.ErrTooBigPrecision:            {0},
}

// maskingNoDataArgCodes lists the errors whose arguments only name objects,
// users and rows, so they are kept when they are not known to be data values.
var maskingNoDataArgCodes = map[uint16]struct{}{
	errno.ErrDBaccessDenied:             {},
	errno.ErrTableaccessDenied:          {},
	errno.ErrColumnaccessDenied:         {},
	errno.ErrSpecificAccessDenied:       {},
	errno.ErrAccessDeniedToMaskedColumn: {},
	errno.ErrBadDB:                      {},
	errno.ErrNoSuchTable:                {},
	errno.ErrBadField:                   {},
	errno.ErrBadNull:                    {},
	errno.ErrDataTooLong:                {},
}

// maskingTemplateAliases lists, for an error code, the other errors whose
// message template is also reported with that code.
var maskingTemplateAliases = map[uint16][]uint16{
	errno.ErrTruncatedWrongValue: {errno.ErrWrongValue},
}

var (
	// maskingRedactArgPosByCode and maskingRedactArgPosByTemplate hold the data
	// value positions of every data-carrying error. maskingRedactedTemplates
	// maps the template of each of them to the one without its data values,
	// and the latter to itself.
	maskingRedactArgPosByCode     = make(map[uint16][]int)
	maskingRedactArgPosByTemplate = make(map[string][]int)
	maskingRedactedTemplates      = make(map[string]string)
	printfVerb                    = regexp.MustCompile(`%[-+# 0]*\d*(?:\.\d+)?[a-zA-Z]`)
)

func init() {
	for code, msg := range errno.MySQLErrName {
		pos := msg.RedactArgPos
		if extra, ok := maskingExtraRedactArgPos[code]; ok {
			pos = extra
		}
		if len(pos) == 0 {
			continue
		}
		maskingRedactArgPosByCode[code] = pos
		maskingRedactArgPosByTemplate[msg.Raw] = pos
		// Replace the verbs rather than the arguments: a data value is not
		// always formatted as a string.
		var redacted strings.Builder
		last := 0
		for i, verb := range printfVerb.FindAllStringIndex(msg.Raw, -1) {
			if slices.Contains(pos, i) {
				redacted.WriteString(msg.Raw[last:verb[0]])
				redacted.WriteString(maskingRedactedValue)
				last = verb[1]
			}
		}
		redacted.WriteString(msg.Raw[last:])
		maskingRedactedTemplates[msg.Raw] = redacted.String()
		maskingRedactedTemplates[redacted.String()] = redacted.String()
	}
}

// SetRedactDiagnostics marks that the statement reads a column that a masking
// policy masks for the current session. The statement evaluates predicates
// and other internal expressions on the original values, so from now on the
// data values in its warnings and execution errors are redacted.
func (sc *StatementContext) SetRedactDiagnostics() {
	if sc.RedactDiagnostics {
		return
	}
	sc.RedactDiagnostics = true
	if h, ok := sc.WarnHandler.(interface{ SetRedactor(func(error) error) }); ok {
		h.SetRedactor(redactMaskingWarning)
	}
}

// RedactExecError returns the error to report for err, which was raised by
// executing the statement. Unless the statement reads a masked column, it is
// err itself. Otherwise the data values of a data-carrying error are replaced
// while its code and message shape are kept, the arguments of another error
// with an error code are replaced as any of them may be a data value, and an
// error without an error code of its own is replaced as a whole.
func (sc *StatementContext) RedactExecError(err error) error {
	if !sc.RedactDiagnostics || err == nil {
		return err
	}
	return redactMaskingDiagnostic(err, true)
}

// evalWarnAppender records the warnings raised by evaluating and converting
// values. When the statement reads a masked column, they are redacted like an
// execution error: they may embed a data value in any argument or in plain
// text.
type evalWarnAppender StatementContext

// AppendWarning implements contextutil.WarnAppender.
func (a *evalWarnAppender) AppendWarning(warn error) {
	sc := (*StatementContext)(a)
	sc.AppendWarning(sc.redactEvalWarning(warn))
}

// AppendNote implements contextutil.WarnAppender.
func (a *evalWarnAppender) AppendNote(warn error) {
	sc := (*StatementContext)(a)
	sc.AppendNote(sc.redactEvalWarning(warn))
}

func (sc *StatementContext) redactEvalWarning(warn error) error {
	if !sc.RedactDiagnostics || warn == nil {
		return warn
	}
	return redactMaskingDiagnostic(warn, true)
}

// EvalWarnAppender returns the appender of the warnings raised by evaluating
// and converting values, as opposed to the advice of the planner and the
// executors that the statement context records directly.
func (sc *StatementContext) EvalWarnAppender() contextutil.WarnAppender {
	return (*evalWarnAppender)(sc)
}

// redactMaskingWarning redacts a warning that the statement context records
// directly. Unlike an evaluation warning, a warning without an error code of
// its own and the arguments that are not known to be data values are kept:
// the planner reports its advice as plain text or with the names of the
// objects it refers to.
func redactMaskingWarning(warn error) error {
	return redactMaskingDiagnostic(warn, false)
}

// redactMaskingDiagnostic redacts err. evaluated tells that err may come from
// evaluating data, so a value may be in any argument of err.
func redactMaskingDiagnostic(err error, evaluated bool) error {
	te, ok := errors.Cause(err).(*terror.Error)
	if ok {
		if redacted := redactMaskingDataValues(te); redacted != nil {
			return keepTxnRetryableMark(te, redacted)
		}
	}
	if !evaluated || err == nil {
		return err
	}
	if !ok || terror.ToSQLError(te).Code == mysql.ErrUnknown {
		return errMaskingDiagnosticHidden
	}
	code := terror.ToSQLError(te).Code
	if _, noData := maskingNoDataArgCodes[code]; noData {
		return err
	}
	if len(te.Args()) > 0 {
		// Nothing tells which arguments are data values.
		return keepTxnRetryableMark(te, te.FastGen(printfVerb.ReplaceAllString(te.MessageTemplate(), maskingRedactedValue)))
	}
	if msg, ok := errno.MySQLErrName[code]; ok && te.MessageTemplate() == msg.Raw {
		// The message is the template of the code, which holds no value.
		return err
	}
	// The message was formatted before, for example by the store that
	// evaluated the expression, so it is rebuilt from the template of the code.
	return keepTxnRetryableMark(te, te.FastGen(redactMaskingText("", code)))
}

// keepTxnRetryableMark keeps the mark of a transaction error that can be
// retried at the end of its redacted message: clients rely on it.
func keepTxnRetryableMark(te *terror.Error, redacted error) error {
	if !strings.HasSuffix(te.GetMsg(), txnRetryableMark) {
		return redacted
	}
	rte, ok := errors.Cause(redacted).(*terror.Error)
	if !ok || strings.HasSuffix(rte.GetMsg(), txnRetryableMark) {
		return redacted
	}
	return te.FastGen("%s "+txnRetryableMark, strings.TrimRight(rte.GetMsg(), " "))
}

// redactMaskingDataValues returns te with its data values replaced, or nil if
// te is not a data-carrying error. Redacting its result again changes nothing.
func redactMaskingDataValues(te *terror.Error) error {
	tmpl, args := te.MessageTemplate(), te.Args()
	if pos, ok := maskingRedactArgPosByTemplate[tmpl]; ok && len(args) == len(printfVerb.FindAllStringIndex(tmpl, -1)) {
		kept := make([]any, 0, len(args))
		for i, arg := range args {
			if !slices.Contains(pos, i) {
				kept = append(kept, arg)
			}
		}
		return te.FastGen(maskingRedactedTemplates[tmpl], kept...)
	}
	code := uint16(te.Code())
	if _, ok := maskingRedactArgPosByCode[code]; !ok {
		return nil
	}
	if len(args) > 0 {
		if maskingRedactedTemplates[tmpl] == tmpl {
			return te
		}
		// The message does not follow a known template, so any of its
		// arguments may be a data value.
		tmpl = ""
	}
	// The message is plain text, such as a diagnostic that a store returns.
	return te.FastGen(redactMaskingText(tmpl, code))
}

// redactMaskingText redacts the text of a data-carrying error with the given
// code. The text before the first data value consists of the template and of
// arguments that are not data, so it is kept when it follows a template of
// the code. Everything from the first data value on is rebuilt from the
// template.
func redactMaskingText(text string, code uint16) string {
	for _, c := range append([]uint16{code}, maskingTemplateAliases[code]...) {
		msg, ok := errno.MySQLErrName[c]
		if !ok {
			continue
		}
		pos := maskingRedactArgPosByCode[c]
		verbs := printfVerb.FindAllStringIndex(msg.Raw, -1)
		if len(pos) == 0 || len(verbs) == 0 {
			continue
		}
		first := min(slices.Min(pos), len(verbs)-1)
		rest, last, matched := text, 0, true
		for i := 0; i <= first && matched; i++ {
			literal := msg.Raw[last:verbs[i][0]]
			last = verbs[i][1]
			if i == 0 {
				matched = strings.HasPrefix(rest, literal)
				rest = strings.TrimPrefix(rest, literal)
				continue
			}
			// Skip an argument that is not data up to the text that follows it.
			idx := strings.Index(rest, literal)
			matched = literal != "" && idx >= 0
			if matched {
				rest = rest[idx+len(literal):]
			}
		}
		if matched {
			return text[:len(text)-len(rest)] + printfVerb.ReplaceAllString(msg.Raw[verbs[first][0]:], maskingRedactedValue)
		}
	}
	if msg, ok := errno.MySQLErrName[code]; ok {
		return printfVerb.ReplaceAllString(msg.Raw, maskingRedactedValue)
	}
	return maskingRedactedValue
}
