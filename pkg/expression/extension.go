// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	"context"
	"strings"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression/contextopt"
	"github.com/pingcap/tidb/pkg/extension"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/sem"
)

var extensionFuncs sync.Map

func registerExtensionFunc(def *extension.FunctionDef) error {
	if def == nil {
		return errors.New("extension function def is nil")
	}

	if err := def.Validate(); err != nil {
		return err
	}

	lowerName := strings.ToLower(def.Name)
	if _, ok := funcs[lowerName]; ok {
		return errors.Errorf("extension function name '%s' conflict with builtin", def.Name)
	}

	class, err := newExtensionFuncClass(def)
	if err != nil {
		return err
	}

	_, exist := extensionFuncs.LoadOrStore(lowerName, class)
	if exist {
		return errors.Errorf("duplicated extension function name '%s'", def.Name)
	}

	return nil
}

func removeExtensionFunc(name string) {
	extensionFuncs.Delete(name)
}

type extensionFuncClass struct {
	baseFunctionClass
	funcDef extension.FunctionDef
	flen    int
}

func newExtensionFuncClass(def *extension.FunctionDef) (*extensionFuncClass, error) {
	var flen int
	switch def.EvalTp {
	case types.ETString:
		flen = mysql.MaxFieldVarCharLength
		if def.EvalStringFunc == nil {
			return nil, errors.New("eval function is nil")
		}
	case types.ETInt:
		flen = mysql.MaxIntWidth
		if def.EvalIntFunc == nil {
			return nil, errors.New("eval function is nil")
		}
	default:
		return nil, errors.Errorf("unsupported extension function ret type: '%v'", def.EvalTp)
	}

	maxArgs := len(def.ArgTps)
	minArgs := maxArgs - def.OptionalArgsLen
	return &extensionFuncClass{
		baseFunctionClass: baseFunctionClass{def.Name, minArgs, maxArgs},
		flen:              flen,
		funcDef:           *def,
	}, nil
}

func (c *extensionFuncClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := checkPrivileges(ctx.GetEvalCtx(), &c.funcDef); err != nil {
		return nil, err
	}

	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, c.funcDef.EvalTp, c.funcDef.ArgTps[:len(args)]...)
	if err != nil {
		return nil, err
	}

	// Though currently, `getFunction` does not require too much information that makes it safe to be cached,
	// we still skip the plan cache for extension functions because there are no strong requirements to do it.
	// Skipping the plan cache can make the behavior simple.
	ctx.SetSkipPlanCache("extension function should not be cached")
	bf.tp.SetFlen(c.flen)
	sig := &extensionFuncSig{baseBuiltinFunc: bf, FunctionDef: c.funcDef}
	return sig, nil
}

func checkPrivileges(ctx EvalContext, fnDef *extension.FunctionDef) error {
	fn := fnDef.RequireDynamicPrivileges
	if fn == nil {
		return nil
	}

	semEnabled := sem.IsEnabled()
	privs := fn(semEnabled)
	if len(privs) == 0 {
		return nil
	}

	for _, priv := range privs {
		if !ctx.RequestDynamicVerification(priv, false) {
			msg := priv
			if !semEnabled {
				msg = "SUPER or " + msg
			}
			return errSpecificAccessDenied.GenWithStackByArgs(msg)
		}
	}

	return nil
}

var _ extension.FunctionContext = extensionFnContext{}

type extensionFuncSig struct {
	baseBuiltinFunc
	contextopt.SessionVarsPropReader

	extension.FunctionDef
}

func (b *extensionFuncSig) Clone() builtinFunc {
	newSig := &extensionFuncSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.FunctionDef = b.FunctionDef
	return newSig
}

func (b *extensionFuncSig) RequiredOptionalEvalProps() OptionalEvalPropKeySet {
	return b.SessionVarsPropReader.RequiredOptionalEvalProps()
}

func (b *extensionFuncSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	if err := checkPrivileges(ctx, &b.FunctionDef); err != nil {
		return "", true, err
	}

	vars, err := b.GetSessionVars(ctx)
	if err != nil {
		return "", true, err
	}

	if b.EvalTp == types.ETString {
		fnCtx := newExtensionFnContext(ctx, vars, b)
		return b.EvalStringFunc(fnCtx, row)
	}
	return b.baseBuiltinFunc.evalString(ctx, row)
}

func (b *extensionFuncSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	if err := checkPrivileges(ctx, &b.FunctionDef); err != nil {
		return 0, true, err
	}

	vars, err := b.GetSessionVars(ctx)
	if err != nil {
		return 0, true, err
	}

	if b.EvalTp == types.ETInt {
		fnCtx := newExtensionFnContext(ctx, vars, b)
		return b.EvalIntFunc(fnCtx, row)
	}
	return b.baseBuiltinFunc.evalInt(ctx, row)
}

type extensionFnContext struct {
	context.Context
	ctx  EvalContext
	vars *variable.SessionVars
	sig  *extensionFuncSig
}

func newExtensionFnContext(ctx EvalContext, vars *variable.SessionVars, sig *extensionFuncSig) extensionFnContext {
	return extensionFnContext{Context: context.TODO(), ctx: ctx, vars: vars, sig: sig}
}

func (b extensionFnContext) EvalArgs(row chunk.Row) ([]types.Datum, error) {
	if len(b.sig.args) == 0 {
		return nil, nil
	}

	result := make([]types.Datum, 0, len(b.sig.args))
	for _, arg := range b.sig.args {
		val, err := arg.Eval(b.ctx, row)
		if err != nil {
			return nil, err
		}
		result = append(result, val)
	}

	return result, nil
}

func (b extensionFnContext) ConnectionInfo() *variable.ConnectionInfo {
	return b.vars.ConnectionInfo
}

func (b extensionFnContext) User() *auth.UserIdentity {
	return b.vars.User
}

func (b extensionFnContext) ActiveRoles() []*auth.RoleIdentity {
	return b.vars.ActiveRoles
}

func (b extensionFnContext) CurrentDB() string {
	return b.ctx.CurrentDB()
}

func init() {
	extension.RegisterExtensionFunc = registerExtensionFunc
	extension.RemoveExtensionFunc = removeExtensionFunc
}
