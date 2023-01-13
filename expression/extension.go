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
	"github.com/pingcap/tidb/extension"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/sem"
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

func (c *extensionFuncClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.checkPrivileges(ctx); err != nil {
		return nil, err
	}

	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, c.funcDef.EvalTp, c.funcDef.ArgTps[:len(args)]...)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(c.flen)
	sig := &extensionFuncSig{context.TODO(), bf, c.funcDef}
	return sig, nil
}

func (c *extensionFuncClass) checkPrivileges(ctx sessionctx.Context) error {
	fn := c.funcDef.RequireDynamicPrivileges
	if fn == nil {
		return nil
	}

	semEnabled := sem.IsEnabled()
	privs := fn(semEnabled)
	if len(privs) == 0 {
		return nil
	}

	manager := privilege.GetPrivilegeManager(ctx)
	activeRoles := ctx.GetSessionVars().ActiveRoles

	for _, priv := range privs {
		if !manager.RequestDynamicVerification(activeRoles, priv, false) {
			msg := priv
			if !semEnabled {
				msg = "SUPER or " + msg
			}
			return errSpecificAccessDenied.GenWithStackByArgs(msg)
		}
	}

	return nil
}

var _ extension.FunctionContext = &extensionFuncSig{}

type extensionFuncSig struct {
	context.Context
	baseBuiltinFunc
	extension.FunctionDef
}

func (b *extensionFuncSig) Clone() builtinFunc {
	newSig := &extensionFuncSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.FunctionDef = b.FunctionDef
	return newSig
}

func (b *extensionFuncSig) evalString(row chunk.Row) (string, bool, error) {
	if b.EvalTp == types.ETString {
		return b.EvalStringFunc(b, row)
	}
	return b.baseBuiltinFunc.evalString(row)
}

func (b *extensionFuncSig) evalInt(row chunk.Row) (int64, bool, error) {
	if b.EvalTp == types.ETInt {
		return b.EvalIntFunc(b, row)
	}
	return b.baseBuiltinFunc.evalInt(row)
}

func (b *extensionFuncSig) EvalArgs(row chunk.Row) ([]types.Datum, error) {
	if len(b.args) == 0 {
		return nil, nil
	}

	result := make([]types.Datum, 0, len(b.args))
	for _, arg := range b.args {
		val, err := arg.Eval(row)
		if err != nil {
			return nil, err
		}
		result = append(result, val)
	}

	return result, nil
}

func (b *extensionFuncSig) ConnectionInfo() *variable.ConnectionInfo {
	return b.ctx.GetSessionVars().ConnectionInfo
}

func (b *extensionFuncSig) User() *auth.UserIdentity {
	return b.ctx.GetSessionVars().User
}

func (b *extensionFuncSig) ActiveRoles() []*auth.RoleIdentity {
	return b.ctx.GetSessionVars().ActiveRoles
}

func (b *extensionFuncSig) CurrentDB() string {
	return b.ctx.GetSessionVars().CurrentDB
}

func init() {
	extension.RegisterExtensionFunc = registerExtensionFunc
	extension.RemoveExtensionFunc = removeExtensionFunc
}
