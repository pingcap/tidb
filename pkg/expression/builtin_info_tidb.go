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
	"encoding/hex"
	"encoding/json"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/expression/expropt"
	infoschema "github.com/pingcap/tidb/pkg/infoschema/context"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/store/helper"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
)

type tidbMVCCInfoFunctionClass struct {
	baseFunctionClass
}

func (c *tidbMVCCInfoFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	sig := &builtinTiDBMVCCInfoSig{baseBuiltinFunc: bf}
	return sig, nil
}

type builtinTiDBMVCCInfoSig struct {
	baseBuiltinFunc
	expropt.KVStorePropReader
	expropt.PrivilegeCheckerPropReader
}

// RequiredOptionalEvalProps implements the RequireOptionalEvalProps interface.
func (b *builtinTiDBMVCCInfoSig) RequiredOptionalEvalProps() OptionalEvalPropKeySet {
	return b.KVStorePropReader.RequiredOptionalEvalProps() |
		b.PrivilegeCheckerPropReader.RequiredOptionalEvalProps()
}

func (b *builtinTiDBMVCCInfoSig) Clone() builtinFunc {
	newSig := &builtinTiDBMVCCInfoSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinTiDBMVCCInfoSig.
func (b *builtinTiDBMVCCInfoSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	privChecker, err := b.GetPrivilegeChecker(ctx)
	if err != nil {
		return "", false, err
	}
	if !privChecker.RequestVerification("", "", "", mysql.SuperPriv) {
		return "", false, plannererrors.ErrSpecificAccessDenied.FastGenByArgs("SUPER")
	}
	s, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	encodedKey, err := hex.DecodeString(s)
	if err != nil {
		return "", false, err
	}
	store, err := b.GetKVStore(ctx)
	if err != nil {
		return "", isNull, err
	}
	hStore, ok := store.(helper.Storage)
	if !ok {
		return "", isNull, errors.New("storage is not a helper.Storage")
	}
	h := helper.NewHelper(hStore)
	resp, err := h.GetMvccByEncodedKey(encodedKey)
	if err != nil {
		return "", false, err
	}
	type mvccInfoResult struct {
		Key  string                        `json:"key"`
		Resp *kvrpcpb.MvccGetByKeyResponse `json:"mvcc"`
	}
	mvccInfo := []*mvccInfoResult{{s, resp}}
	if tablecodec.IsIndexKey(encodedKey) && !tablecodec.IsTempIndexKey(encodedKey) {
		tablecodec.IndexKey2TempIndexKey(encodedKey)
		hexStr := hex.EncodeToString(encodedKey)
		res, err := h.GetMvccByEncodedKey(encodedKey)
		if err != nil {
			return "", false, err
		}
		if res.Info != nil && (len(res.Info.Writes) > 0 || len(res.Info.Values) > 0 || res.Info.Lock != nil) {
			mvccInfo = append(mvccInfo, &mvccInfoResult{hexStr, res})
		}
	}
	js, err := json.Marshal(mvccInfo)
	if err != nil {
		return "", false, err
	}
	return string(js), false, nil
}

type tidbEncodeRecordKeyClass struct {
	baseFunctionClass
}

func (c *tidbEncodeRecordKeyClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	evalTps := make([]types.EvalType, 0, len(args))
	evalTps = append(evalTps, types.ETString, types.ETString)
	for _, arg := range args[2:] {
		evalTps = append(evalTps, arg.GetType(ctx.GetEvalCtx()).EvalType())
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, evalTps...)
	if err != nil {
		return nil, err
	}
	sig := &builtinTiDBEncodeRecordKeySig{baseBuiltinFunc: bf}
	return sig, nil
}

type builtinTiDBEncodeRecordKeySig struct {
	baseBuiltinFunc
	expropt.InfoSchemaPropReader
	expropt.SessionVarsPropReader
	expropt.PrivilegeCheckerPropReader
}

// RequiredOptionalEvalProps implements the RequireOptionalEvalProps interface.
func (b *builtinTiDBEncodeRecordKeySig) RequiredOptionalEvalProps() OptionalEvalPropKeySet {
	return b.InfoSchemaPropReader.RequiredOptionalEvalProps() |
		b.SessionVarsPropReader.RequiredOptionalEvalProps() |
		b.PrivilegeCheckerPropReader.RequiredOptionalEvalProps()
}

func (b *builtinTiDBEncodeRecordKeySig) Clone() builtinFunc {
	newSig := &builtinTiDBEncodeRecordKeySig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinTiDBEncodeRecordKeySig.
func (b *builtinTiDBEncodeRecordKeySig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	is, err := b.GetLatestInfoSchema(ctx)
	if err != nil {
		return "", true, err
	}
	if EncodeRecordKeyFromRow == nil {
		return "", false, errors.New("EncodeRecordKeyFromRow is not initialized")
	}
	privChecker, err := b.GetPrivilegeChecker(ctx)
	if err != nil {
		return "", false, err
	}
	recordKey, isNull, err := EncodeRecordKeyFromRow(ctx, privChecker, is, b.args, row)
	if isNull || err != nil {
		if errors.ErrorEqual(err, plannererrors.ErrSpecificAccessDenied) {
			sv, err2 := b.GetSessionVars(ctx)
			if err2 != nil {
				return "", isNull, err
			}
			tblName, isNull, err2 := b.args[1].EvalString(ctx, row)
			if err2 != nil || isNull {
				return "", isNull, err
			}
			return "", isNull, plannererrors.ErrTableaccessDenied.FastGenByArgs("SELECT", sv.User.AuthUsername, sv.User.AuthHostname, tblName)
		}
		return "", isNull, err
	}
	return hex.EncodeToString(recordKey), false, nil
}

type tidbEncodeIndexKeyClass struct {
	baseFunctionClass
}

func (c *tidbEncodeIndexKeyClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	evalTps := make([]types.EvalType, 0, len(args))
	evalTps = append(evalTps, types.ETString, types.ETString, types.ETString)
	for _, arg := range args[3:] {
		evalTps = append(evalTps, arg.GetType(ctx.GetEvalCtx()).EvalType())
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, evalTps...)
	if err != nil {
		return nil, err
	}
	sig := &builtinTiDBEncodeIndexKeySig{baseBuiltinFunc: bf}
	return sig, nil
}

type builtinTiDBEncodeIndexKeySig struct {
	baseBuiltinFunc
	expropt.InfoSchemaPropReader
	expropt.SessionVarsPropReader
	expropt.PrivilegeCheckerPropReader
}

// RequiredOptionalEvalProps implements the RequireOptionalEvalProps interface.
func (b *builtinTiDBEncodeIndexKeySig) RequiredOptionalEvalProps() OptionalEvalPropKeySet {
	return b.InfoSchemaPropReader.RequiredOptionalEvalProps() |
		b.SessionVarsPropReader.RequiredOptionalEvalProps() |
		b.PrivilegeCheckerPropReader.RequiredOptionalEvalProps()
}

func (b *builtinTiDBEncodeIndexKeySig) Clone() builtinFunc {
	newSig := &builtinTiDBEncodeIndexKeySig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinTiDBEncodeIndexKeySig.
func (b *builtinTiDBEncodeIndexKeySig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	is, err := b.GetLatestInfoSchema(ctx)
	if err != nil {
		return "", true, err
	}
	if EncodeIndexKeyFromRow == nil {
		return "", false, errors.New("EncodeIndexKeyFromRow is not initialized")
	}
	privChecker, err := b.GetPrivilegeChecker(ctx)
	if err != nil {
		return "", false, err
	}
	idxKey, isNull, err := EncodeIndexKeyFromRow(ctx, privChecker, is, b.args, row)
	if isNull || err != nil {
		if errors.ErrorEqual(err, plannererrors.ErrSpecificAccessDenied) {
			sv, err2 := b.GetSessionVars(ctx)
			if err2 != nil {
				return "", isNull, err
			}
			tblName, isNull, err2 := b.args[1].EvalString(ctx, row)
			if err2 != nil || isNull {
				return "", isNull, err
			}
			return "", isNull, plannererrors.ErrTableaccessDenied.FastGenByArgs("SELECT", sv.User.AuthUsername, sv.User.AuthHostname, tblName)
		}
		return "", isNull, err
	}
	return hex.EncodeToString(idxKey), false, nil
}

type tidbDecodeKeyFunctionClass struct {
	baseFunctionClass
}

func (c *tidbDecodeKeyFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	sig := &builtinTiDBDecodeKeySig{baseBuiltinFunc: bf}
	return sig, nil
}

// DecodeKeyFromString is used to decode key by expressions
var DecodeKeyFromString func(types.Context, infoschema.MetaOnlyInfoSchema, string) string

// EncodeRecordKeyFromRow is used to encode record key by expressions.
var EncodeRecordKeyFromRow func(ctx EvalContext, checker expropt.PrivilegeChecker, is infoschema.MetaOnlyInfoSchema, args []Expression, row chunk.Row) ([]byte, bool, error)

// EncodeIndexKeyFromRow is used to encode index key by expressions.
var EncodeIndexKeyFromRow func(ctx EvalContext, checker expropt.PrivilegeChecker, is infoschema.MetaOnlyInfoSchema, args []Expression, row chunk.Row) ([]byte, bool, error)

type builtinTiDBDecodeKeySig struct {
	baseBuiltinFunc
	expropt.InfoSchemaPropReader
}

// RequiredOptionalEvalProps implements the RequireOptionalEvalProps interface.
func (b *builtinTiDBDecodeKeySig) RequiredOptionalEvalProps() OptionalEvalPropKeySet {
	return b.InfoSchemaPropReader.RequiredOptionalEvalProps()
}

func (b *builtinTiDBDecodeKeySig) Clone() builtinFunc {
	newSig := &builtinTiDBDecodeKeySig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinTiDBDecodeKeySig.
func (b *builtinTiDBDecodeKeySig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	s, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	is, err := b.GetLatestInfoSchema(ctx)
	if err != nil {
		return "", true, err
	}

	if fn := DecodeKeyFromString; fn != nil {
		s = fn(ctx.TypeCtx(), is, s)
	}
	return s, false, nil
}

type tidbDecodeSQLDigestsFunctionClass struct {
	baseFunctionClass
	expropt.PrivilegeCheckerPropReader
}

