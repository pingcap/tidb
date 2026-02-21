// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

// Copyright 2015 PingCAP, Inc.
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

package expression

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression/expropt"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/plancodec"
)

func (c *tidbDecodeSQLDigestsFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}

	privChecker, err := c.GetPrivilegeChecker(ctx.GetEvalCtx())
	if err != nil {
		return nil, err
	}

	if !privChecker.RequestVerification("", "", "", mysql.ProcessPriv) {
		return nil, errSpecificAccessDenied.GenWithStackByArgs("PROCESS")
	}

	var argTps []types.EvalType
	if len(args) > 1 {
		argTps = []types.EvalType{types.ETString, types.ETInt}
	} else {
		argTps = []types.EvalType{types.ETString}
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, argTps...)
	if err != nil {
		return nil, err
	}
	sig := &builtinTiDBDecodeSQLDigestsSig{baseBuiltinFunc: bf}
	return sig, nil
}

type builtinTiDBDecodeSQLDigestsSig struct {
	baseBuiltinFunc
	expropt.SessionVarsPropReader
	expropt.SQLExecutorPropReader
	expropt.PrivilegeCheckerPropReader
}

// RequiredOptionalEvalProps implements the RequireOptionalEvalProps interface.
func (b *builtinTiDBDecodeSQLDigestsSig) RequiredOptionalEvalProps() OptionalEvalPropKeySet {
	return b.SessionVarsPropReader.RequiredOptionalEvalProps() |
		b.SQLExecutorPropReader.RequiredOptionalEvalProps() |
		b.PrivilegeCheckerPropReader.RequiredOptionalEvalProps()
}

func (b *builtinTiDBDecodeSQLDigestsSig) Clone() builtinFunc {
	newSig := &builtinTiDBDecodeSQLDigestsSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinTiDBDecodeSQLDigestsSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	privChecker, err := b.GetPrivilegeChecker(ctx)
	if err != nil {
		return "", true, err
	}

	if !privChecker.RequestVerification("", "", "", mysql.ProcessPriv) {
		return "", true, errSpecificAccessDenied.GenWithStackByArgs("PROCESS")
	}

	args := b.getArgs()
	digestsStr, isNull, err := args[0].EvalString(ctx, row)
	if err != nil {
		return "", true, err
	}
	if isNull {
		return "", true, nil
	}

	stmtTruncateLength := int64(0)
	if len(args) > 1 {
		stmtTruncateLength, isNull, err = args[1].EvalInt(ctx, row)
		if err != nil {
			return "", true, err
		}
		if isNull {
			stmtTruncateLength = 0
		}
	}

	var digests []any
	err = json.Unmarshal([]byte(digestsStr), &digests)
	if err != nil {
		const errMsgMaxLength = 32
		if len(digestsStr) > errMsgMaxLength {
			digestsStr = digestsStr[:errMsgMaxLength] + "..."
		}
		tc := typeCtx(ctx)
		tc.AppendWarning(errIncorrectArgs.FastGen("The argument can't be unmarshalled as JSON array: '%s'", digestsStr))
		return "", true, nil
	}

	// Query the SQL Statements by digests.
	retriever := NewSQLDigestTextRetriever()
	for _, item := range digests {
		if item != nil {
			digest, ok := item.(string)
			if ok {
				retriever.SQLDigestsMap[digest] = ""
			}
		}
	}

	vars, err := b.GetSessionVars(ctx)
	if err != nil {
		return "", true, err
	}

	// Querying may take some time and it takes a context.Context as argument, which is not available here.
	// We simply create a context with a timeout here.
	timeout := time.Duration(vars.GetMaxExecutionTime()) * time.Millisecond
	if timeout == 0 || timeout > 20*time.Second {
		timeout = 20 * time.Second
	}
	goCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	exec, err := b.GetSQLExecutor(ctx)
	if err != nil {
		return "", true, err
	}

	err = retriever.RetrieveGlobal(goCtx, exec)
	if err != nil {
		if errors.Cause(err) == context.DeadlineExceeded || errors.Cause(err) == context.Canceled {
			return "", true, errUnknown.GenWithStack("Retrieving cancelled internally with error: %v", err)
		}

		tc := typeCtx(ctx)
		tc.AppendWarning(errUnknown.FastGen("Retrieving statements information failed with error: %v", err))
		return "", true, nil
	}

	// Collect the result.
	result := make([]any, len(digests))
	for i, item := range digests {
		if item == nil {
			continue
		}
		if digest, ok := item.(string); ok {
			if stmt, ok := retriever.SQLDigestsMap[digest]; ok && len(stmt) > 0 {
				// Truncate too-long statements if necessary.
				if stmtTruncateLength > 0 && int64(len(stmt)) > stmtTruncateLength {
					stmt = stmt[:stmtTruncateLength] + "..."
				}
				result[i] = stmt
			}
		}
	}

	resultStr, err := json.Marshal(result)
	if err != nil {
		tc := typeCtx(ctx)
		tc.AppendWarning(errUnknown.FastGen("Marshalling result as JSON failed with error: %v", err))
		return "", true, nil
	}

	return string(resultStr), false, nil
}

type tidbEncodeSQLDigestFunctionClass struct {
	baseFunctionClass
}

func (c *tidbEncodeSQLDigestFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}

	argTps := []types.EvalType{types.ETString}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, argTps...)
	if err != nil {
		return nil, err
	}
	sig := &builtinTiDBEncodeSQLDigestSig{bf}
	return sig, nil
}

type builtinTiDBEncodeSQLDigestSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinTiDBEncodeSQLDigestSig) Clone() builtinFunc {
	newSig := &builtinTiDBEncodeSQLDigestSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinTiDBEncodeSQLDigestSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	orgSQLStr, isNull, err := b.getArgs()[0].EvalString(ctx, row)
	if err != nil {
		return "", true, err
	}
	if isNull {
		return "", true, nil
	}
	return parser.DigestHash(orgSQLStr).String(), false, nil
}

type tidbDecodePlanFunctionClass struct {
	baseFunctionClass
}

func (c *tidbDecodePlanFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	if c.funcName == ast.TiDBDecodePlan {
		return &builtinTiDBDecodePlanSig{bf}, nil
	} else if c.funcName == ast.TiDBDecodeBinaryPlan {
		return &builtinTiDBDecodeBinaryPlanSig{bf}, nil
	}
	return nil, errors.New("unknown decode plan function")
}

type builtinTiDBDecodePlanSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinTiDBDecodePlanSig) Clone() builtinFunc {
	newSig := &builtinTiDBDecodePlanSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinTiDBDecodePlanSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	planString, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	planTree, err := plancodec.DecodePlan(planString)
	if err != nil {
		return planString, false, nil
	}
	return planTree, false, nil
}

type builtinTiDBDecodeBinaryPlanSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinTiDBDecodeBinaryPlanSig) Clone() builtinFunc {
	newSig := &builtinTiDBDecodeBinaryPlanSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinTiDBDecodeBinaryPlanSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	planString, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	planTree, err := plancodec.DecodeBinaryPlan(planString)
	if err != nil {
		tc := typeCtx(ctx)
		tc.AppendWarning(err)
		return "", false, nil
	}
	return planTree, false, nil
}

type nextValFunctionClass struct {
	baseFunctionClass
}

func (c *nextValFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETString)
	if err != nil {
		return nil, err
	}
	sig := &builtinNextValSig{baseBuiltinFunc: bf}
	bf.tp.SetFlen(10)
	return sig, nil
}

type builtinNextValSig struct {
	baseBuiltinFunc
	expropt.SequenceOperatorPropReader
	expropt.SessionVarsPropReader
	expropt.PrivilegeCheckerPropReader
}

func (b *builtinNextValSig) RequiredOptionalEvalProps() OptionalEvalPropKeySet {
	return b.SequenceOperatorPropReader.RequiredOptionalEvalProps() |
		b.SessionVarsPropReader.RequiredOptionalEvalProps() |
		b.PrivilegeCheckerPropReader.RequiredOptionalEvalProps()
}

func (b *builtinNextValSig) Clone() builtinFunc {
	newSig := &builtinNextValSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinNextValSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	sequenceName, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	db, seq := getSchemaAndSequence(sequenceName)
	if len(db) == 0 {
		db = ctx.CurrentDB()
	}
	// Check the tableName valid.
	sequence, err := b.GetSequenceOperator(ctx, db, seq)
	if err != nil {
		return 0, false, err
	}
	// Get session vars
	vars, err := b.GetSessionVars(ctx)
	if err != nil {
		return 0, false, err
	}
	// Do the privilege check.
	user := vars.User
	privChecker, err := b.GetPrivilegeChecker(ctx)
	if err != nil {
		return 0, false, err
	}
	if !privChecker.RequestVerification(db, seq, "", mysql.InsertPriv) {
		return 0, false, errSequenceAccessDenied.GenWithStackByArgs("INSERT", user.AuthUsername, user.AuthHostname, seq)
	}
	nextVal, err := sequence.GetSequenceNextVal()
	if err != nil {
		return 0, false, err
	}
	// update the sequenceState.
	vars.SequenceState.UpdateState(sequence.GetSequenceID(), nextVal)
	return nextVal, false, nil
}

type lastValFunctionClass struct {
	baseFunctionClass
}

func (c *lastValFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETString)
	if err != nil {
		return nil, err
	}
	sig := &builtinLastValSig{baseBuiltinFunc: bf}
	bf.tp.SetFlen(10)
	return sig, nil
}

type builtinLastValSig struct {
	baseBuiltinFunc
	expropt.SequenceOperatorPropReader
	expropt.SessionVarsPropReader
	expropt.PrivilegeCheckerPropReader
}

func (b *builtinLastValSig) RequiredOptionalEvalProps() OptionalEvalPropKeySet {
	return b.SequenceOperatorPropReader.RequiredOptionalEvalProps() |
		b.SessionVarsPropReader.RequiredOptionalEvalProps() |
		b.PrivilegeCheckerPropReader.RequiredOptionalEvalProps()
}

func (b *builtinLastValSig) Clone() builtinFunc {
	newSig := &builtinLastValSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLastValSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	sequenceName, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	db, seq := getSchemaAndSequence(sequenceName)
	if len(db) == 0 {
		db = ctx.CurrentDB()
	}
	// Check the tableName valid.
	sequence, err := b.GetSequenceOperator(ctx, db, seq)
	if err != nil {
		return 0, false, err
	}
	// Get session vars
	vars, err := b.GetSessionVars(ctx)
	if err != nil {
		return 0, false, err
	}
	// Do the privilege check.
	user := vars.User
	privChecker, err := b.GetPrivilegeChecker(ctx)
	if err != nil {
		return 0, false, err
	}
	if !privChecker.RequestVerification(db, seq, "", mysql.SelectPriv) {
		return 0, false, errSequenceAccessDenied.GenWithStackByArgs("SELECT", user.AuthUsername, user.AuthHostname, seq)
	}
	return vars.SequenceState.GetLastValue(sequence.GetSequenceID())
}

type setValFunctionClass struct {
	baseFunctionClass
}

func (c *setValFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETString, types.ETInt)
	if err != nil {
		return nil, err
	}
	sig := &builtinSetValSig{baseBuiltinFunc: bf}
	bf.tp.SetFlen(args[1].GetType(ctx.GetEvalCtx()).GetFlen())
	return sig, nil
}

type builtinSetValSig struct {
	baseBuiltinFunc
	expropt.SequenceOperatorPropReader
	expropt.SessionVarsPropReader
	expropt.PrivilegeCheckerPropReader
}

func (b *builtinSetValSig) RequiredOptionalEvalProps() OptionalEvalPropKeySet {
	return b.SequenceOperatorPropReader.RequiredOptionalEvalProps() |
		b.SessionVarsPropReader.RequiredOptionalEvalProps() |
		b.PrivilegeCheckerPropReader.RequiredOptionalEvalProps()
}

func (b *builtinSetValSig) Clone() builtinFunc {
	newSig := &builtinSetValSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinSetValSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	sequenceName, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	db, seq := getSchemaAndSequence(sequenceName)
	if len(db) == 0 {
		db = ctx.CurrentDB()
	}
	// Check the tableName valid.
	sequence, err := b.GetSequenceOperator(ctx, db, seq)
	if err != nil {
		return 0, false, err
	}
	// Get session vars
	vars, err := b.GetSessionVars(ctx)
	if err != nil {
		return 0, false, err
	}
	// Do the privilege check.
	user := vars.User
	privChecker, err := b.GetPrivilegeChecker(ctx)
	if err != nil {
		return 0, false, err
	}
	if !privChecker.RequestVerification(db, seq, "", mysql.InsertPriv) {
		return 0, false, errSequenceAccessDenied.GenWithStackByArgs("INSERT", user.AuthUsername, user.AuthHostname, seq)
	}
	setValue, isNull, err := b.args[1].EvalInt(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	return sequence.SetSequenceVal(setValue)
}

func getSchemaAndSequence(sequenceName string) (string, string) {
	res := strings.Split(sequenceName, ".")
	if len(res) == 1 {
		return "", res[0]
	}
	return res[0], res[1]
}

type formatBytesFunctionClass struct {
	baseFunctionClass
}

func (c *formatBytesFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETReal)
	if err != nil {
		return nil, err
	}
	charset, collate := ctx.GetCharsetInfo()
	bf.tp.SetCharset(charset)
	bf.tp.SetCollate(collate)
	sig := &builtinFormatBytesSig{bf}
	return sig, nil
}

type builtinFormatBytesSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinFormatBytesSig) Clone() builtinFunc {
	newSig := &builtinFormatBytesSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// formatBytes evals a builtinFormatBytesSig.
// See https://dev.mysql.com/doc/refman/8.0/en/performance-schema-functions.html#function_format-bytes
func (b *builtinFormatBytesSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	val, isNull, err := b.args[0].EvalReal(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	return GetFormatBytes(val), false, nil
}

type formatNanoTimeFunctionClass struct {
	baseFunctionClass
}

func (c *formatNanoTimeFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETReal)
	if err != nil {
		return nil, err
	}
	charset, collate := ctx.GetCharsetInfo()
	bf.tp.SetCharset(charset)
	bf.tp.SetCollate(collate)
	sig := &builtinFormatNanoTimeSig{bf}
	return sig, nil
}

type builtinFormatNanoTimeSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinFormatNanoTimeSig) Clone() builtinFunc {
	newSig := &builtinFormatNanoTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// formatNanoTime evals a builtinFormatNanoTimeSig, as time unit in TiDB is always nanosecond, not picosecond.
// See https://dev.mysql.com/doc/refman/8.0/en/performance-schema-functions.html#function_format-pico-time
func (b *builtinFormatNanoTimeSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	val, isNull, err := b.args[0].EvalReal(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	return GetFormatNanoTime(val), false, nil
}
