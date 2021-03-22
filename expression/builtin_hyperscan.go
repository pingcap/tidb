package expression

import (
	"fmt"
	"strings"

	hs "github.com/flier/gohs/hyperscan"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tipb/go-tipb"
)

var (
	_ functionClass = &hsMatchFunctionClass{}
)

var (
	_ builtinFunc = &builtinHsMatchSig{}
)

var (
	HSMatch           = "hs_match"
	errAlreadyMatched = fmt.Errorf("Already Matched")
)

type builtinHsMatchSig struct {
	baseBuiltinFunc
	db hs.BlockDatabase
}

type hsMatchFunctionClass struct {
	baseFunctionClass
}

func (c *hsMatchFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTp := []types.EvalType{types.ETString, types.ETString}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, argTp...)
	if err != nil {
		return nil, err
	}
	bf.tp.Flen = 1
	sig := &builtinHsMatchSig{bf, nil}
	sig.setPbCode(tipb.ScalarFuncSig_LikeSig)
	return sig, nil
}

func (b *builtinHsMatchSig) Clone() builtinFunc {
	newSig := &builtinHsMatchSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	if b.db != nil {
		newSig.db = b.cloneDb()
	}
	return newSig
}

func (b *builtinHsMatchSig) cloneDb() hs.BlockDatabase {
	var (
		ret  hs.BlockDatabase
		err  error
		data []byte
	)
	data, err = b.db.Marshal()
	if err != nil {
		return nil
	}
	ret, err = hs.UnmarshalBlockDatabase(data)
	if err != nil {
		return nil
	}
	return ret
}

func (b *builtinHsMatchSig) evalInt(row chunk.Row) (int64, bool, error) {
	valStr, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	patternStr, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	if b.db == nil {
		db, err := b.buildBlockDB(patternStr)
		if err != nil {
			return 0, true, ErrRegexp.GenWithStackByArgs(err.Error())
		}
		b.db = db
	}
	_, matched := b.hsMatch(valStr)
	return boolToInt64(matched), false, nil
}

func (b *builtinHsMatchSig) buildBlockDB(patterns string) (hs.BlockDatabase, error) {
	lines := strings.Split(patterns, "\n")
	pats := make([]*hs.Pattern, 0, len(lines))
	for id, reg := range lines {
		if reg == "" {
			continue
		}
		pat, err := hs.ParsePattern(reg)
		if err != nil {
			return nil, err
		}
		pat.Id = id
		pats = append(pats, pat)
	}
	builder := hs.DatabaseBuilder{
		Patterns: pats,
		Mode:     hs.BlockMode,
		Platform: hs.PopulatePlatform(),
	}
	db, err := builder.Build()
	return db.(hs.BlockDatabase), err
}

func (b *builtinHsMatchSig) hsMatch(val string) (int, bool) {
	matched := false
	matchedId := 0
	handler := func(id uint, from, to uint64, flags uint, context interface{}) error {
		if !matched {
			matched = true
			matchedId = int(id)
			return errAlreadyMatched
		}
		return nil
	}
	err := b.db.Scan([]byte(val), nil, handler, nil)
	if err != nil && err.(hs.HsError) != hs.ErrScanTerminated {
		return 0, false
	}
	return matchedId, matched
}
