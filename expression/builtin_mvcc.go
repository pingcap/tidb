package expression

import (
	"fmt"
	"io/ioutil"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

var (
	_ functionClass = &mvccGetInfoFunctionClass{}
)

var (
	_ builtinFunc = &builtinMVCCGetInfoSig{}
)

type mvccGetInfoFunctionClass struct {
	baseFunctionClass
}

func (c *mvccGetInfoFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}

	argTps := []types.EvalType{types.ETString}
	switch len(args) {
	case 2:
		// there are 2 parameters for /mvcc/hex/{hex}
		argTps = append(argTps, types.ETString)
	case 3:
		return nil, errors.Trace(fmt.Errorf("invalid argument count: 3"))
	case 4:
		// there are 4 parameters for /mvcc/key/{db}/{table}/{handle}
		// there are 4 parameters for /mvcc/txn/{startTs}/{db}/{table}
		argTps = append(argTps, types.ETString, types.ETString, types.ETInt)
	default:
		// there are 5 to infinite parameters for /mvcc/index/{db}/{table}/{index}/{handle}?${c1}={v1}&${c2}=${v2}
		argTps = append(argTps, types.ETString, types.ETString, types.ETInt, types.ETString)
		for i := 0; i < len(args)-5; i++ {
			argTps = append(argTps, types.ETString)
		}
	}

	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, argTps...)
	if err != nil {
		return nil, err
	}
	sig := &builtinMVCCGetInfoSig{bf}
	return sig, nil
}

type builtinMVCCGetInfoSig struct {
	baseBuiltinFunc
}

func (b *builtinMVCCGetInfoSig) Clone() builtinFunc {
	newSig := &builtinMVCCGetInfoSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinMVCCGetInfoSig) evalString(row chunk.Row) (string, bool, error) {
	method, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}

	switch method {
	case "hex":
		return b.getByHex(row)
	case "handle":
		return b.getByHandle(row)
	case "startTs":
		return b.getByStartTs(row)
	case "index":
		return b.getByIndex(row)
	default:
		return "", true, fmt.Errorf("invalid method %s to get mvcc info", method)
	}
}

func (b *builtinMVCCGetInfoSig) getByHex(row chunk.Row) (string, bool, error) {
	hexKey, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	return b.fetch(fmt.Sprintf("/mvcc/hex/%s", hexKey))
}

func (b *builtinMVCCGetInfoSig) getByHandle(row chunk.Row) (string, bool, error) {
	db, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}

	table, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}

	handle, isNull, err := b.args[3].EvalInt(b.ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	return b.fetch(fmt.Sprintf("/mvcc/key/%s/%s/%v", db, table, handle))
}

func (b *builtinMVCCGetInfoSig) getByStartTs(row chunk.Row) (string, bool, error) {
	db, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}

	table, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}

	startTs, isNull, err := b.args[3].EvalInt(b.ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	return b.fetch(fmt.Sprintf("/mvcc/txn/%v/%s/%s", startTs, db, table))
}

func (b *builtinMVCCGetInfoSig) getByIndex(row chunk.Row) (string, bool, error) {
	db, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}

	table, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}

	handle, isNull, err := b.args[3].EvalInt(b.ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}

	index, isNull, err := b.args[4].EvalString(b.ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}

	var queries []string
	for _, a := range b.getArgs()[5:] {
		s, isNull, err := a.EvalString(b.ctx, row)
		if isNull || err != nil {
			return s, isNull, err
		}
		queries = append(queries, s)
	}

	uri := fmt.Sprintf("/mvcc/index/%s/%s/%s/%v", db, table, index, handle)
	if len(queries) != 0 {
		uri = fmt.Sprintf("%s?%s", uri, strings.Join(queries, "&"))
	}
	return b.fetch(uri)
}

func (b *builtinMVCCGetInfoSig) fetch(uri string) (string, bool, error) {
	client := newTiDBServerClient()
	resp, err := client.fetchStatus(uri)
	if err != nil {
		return "", true, err
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", true, err
	}
	return string(body), false, nil
}
