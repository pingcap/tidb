package expression

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tipb/go-tipb"
)

// arrayElementFunctionClass's logic is basically the same as that of jsonExtractFunctionClass.
type arrayElementFunctionClass struct {
	baseFunctionClass
}

type builtinArrayElementSig struct {
	baseBuiltinFunc
}

func (b *builtinArrayElementSig) Clone() builtinFunc {
	newSig := &builtinArrayElementSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *arrayElementFunctionClass) verifyArgs(ctx EvalContext, args []Expression) error {
	if err := c.baseFunctionClass.verifyArgs(args); err != nil {
		return err
	}
	return verifyJSONArgsType(ctx, c.funcName, true, args, 0)
}

func (c *arrayElementFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(ctx.GetEvalCtx(), args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, 0, len(args))
	argTps = append(argTps, types.ETJson)
	for range args[1:] {
		argTps = append(argTps, types.ETString)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETJson, argTps...)
	if err != nil {
		return nil, err
	}
	sig := &builtinArrayElementSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonExtractSig)
	return sig, nil
}

func (b *builtinArrayElementSig) evalJSON(ctx EvalContext, row chunk.Row) (res types.BinaryJSON, isNull bool, err error) {
	res, isNull, err = b.args[0].EvalJSON(ctx, row)
	if isNull || err != nil {
		return
	}
	if res.TypeCode != types.JSONTypeCodeArray {
		return res, true, nil
	}
	idxStr := strings.Builder{}
	idxStr.WriteString("$")
	for _, arg := range b.args[1:] {
		v, isNull, err := arg.EvalString(ctx, row)
		if isNull || err != nil {
			return res, isNull, err
		}
		idx, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return res, true, fmt.Errorf("invalid array index: %v", v)
		}
		if idx < 0 {
			return res, true, nil
		}
		idxStr.WriteByte('[')
		idxStr.WriteString(v)
		idxStr.WriteByte(']')
	}
	pathExpr, err := types.ParseJSONPathExpr(idxStr.String())
	if err != nil {
		return res, true, err
	}
	pathExprs := []types.JSONPathExpression{pathExpr}
	var found bool
	if res, found = res.Extract(pathExprs); !found {
		return res, true, nil
	}
	return res, false, nil
}
