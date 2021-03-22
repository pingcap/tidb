package expression

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	hs "github.com/flier/gohs/hyperscan"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tipb/go-tipb"
	//"github.com/pingcap/tidb/util/logutil"
	//"go.uber.org/zap"
)

var (
	_ functionClass = &hsMatchFunctionClass{}
	_ functionClass = &hsMatchDbFunctionClass{}
	_ functionClass = &hsMatchJsonFunctionClass{}
	_ functionClass = &hsBuildDbJsonFunctionClass{}
)

var (
	_ builtinFunc = &builtinHsMatchSig{}
	_ builtinFunc = &builtinHsMatchDbSig{}
	_ builtinFunc = &builtinHsMatchJsonSig{}
	_ builtinFunc = &builtinHsBuildDbJsonSig{}
)

var (
	HSMatch           = "hs_match"
	HSMatchDb         = "hs_match_db"
	HSMatchJson       = "hs_match_json"
	HSBuildDBJson     = "hs_build_db_json"
	errAlreadyMatched = fmt.Errorf("Already Matched")
)

const (
	ScalarFuncSig_HsMatch       tipb.ScalarFuncSig = 4320
	ScalarFuncSig_HsMatchDb     tipb.ScalarFuncSig = 4321
	ScalarFuncSig_HsMatchJson   tipb.ScalarFuncSig = 4322
	ScalarFuncSig_HsBuildDbJson tipb.ScalarFuncSig = 4323
)

type baseBuiltinHsSig struct {
	baseBuiltinFunc
	db hs.BlockDatabase
}

type builtinHsMatchSig struct {
	baseBuiltinHsSig
}

type builtinHsMatchDbSig struct {
	baseBuiltinHsSig
}

type builtinHsMatchJsonSig struct {
	baseBuiltinHsSig
}

type builtinHsBuildDbJsonSig struct {
	baseBuiltinHsSig
}

type hsMatchFunctionClass struct {
	baseFunctionClass
}

type hsMatchDbFunctionClass struct {
	baseFunctionClass
}

type hsMatchJsonFunctionClass struct {
	baseFunctionClass
}

type hsBuildDbJsonFunctionClass struct {
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
	sig := &builtinHsMatchSig{baseBuiltinHsSig{bf, nil}}
	sig.setPbCode(ScalarFuncSig_HsMatch)
	return sig, nil
}

func (c *hsMatchDbFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTp := []types.EvalType{types.ETString, types.ETString}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, argTp...)
	if err != nil {
		return nil, err
	}
	bf.tp.Flen = 1
	sig := &builtinHsMatchDbSig{baseBuiltinHsSig{bf, nil}}
	sig.setPbCode(ScalarFuncSig_HsMatchDb)
	return sig, nil
}

func (c *hsMatchJsonFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTp := []types.EvalType{types.ETString, types.ETString}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, argTp...)
	if err != nil {
		return nil, err
	}
	bf.tp.Flen = 1
	sig := &builtinHsMatchJsonSig{baseBuiltinHsSig{bf, nil}}
	sig.setPbCode(ScalarFuncSig_HsMatchJson)
	return sig, nil
}

func (c *hsBuildDbJsonFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTp := []types.EvalType{types.ETString}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, argTp...)
	if err != nil {
		return nil, err
	}
	bf.tp.Flen = 1
	sig := &builtinHsBuildDbJsonSig{baseBuiltinHsSig{bf, nil}}
	sig.setPbCode(ScalarFuncSig_HsBuildDbJson)
	return sig, nil
}

func (b *baseBuiltinHsSig) cloneDb() hs.BlockDatabase {
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

func (b *baseBuiltinHsSig) hsMatch(val string) (int, bool) {
	matched := false
	matchedId := 0
	if b.db == nil {
		return matchedId, matched
	}
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

func (b *baseBuiltinHsSig) buildHsBlockDB(patterns []*hs.Pattern) (hs.BlockDatabase, error) {
	if len(patterns) == 0 {
		return nil, nil
	}
	builder := hs.DatabaseBuilder{
		Patterns: patterns,
		Mode:     hs.BlockMode,
		Platform: hs.PopulatePlatform(),
	}
	db, err := builder.Build()
	if err != nil {
		return nil, err
	}
	return db.(hs.BlockDatabase), err
}

type hsPatternObj struct {
	Id      int    `json:"id,omitempty"`
	Pattern string `json:"pattern"`
}

func (b *baseBuiltinHsSig) buildBlockDBFromJson(patternsJson string) (hs.BlockDatabase, error) {
	patterns := make([]hsPatternObj, 0)
	err := json.Unmarshal([]byte(patternsJson), &patterns)
	if err != nil {
		return nil, err
	}

	pats := make([]*hs.Pattern, 0, len(patterns))
	for id, reg := range patterns {
		if reg.Pattern == "" {
			continue
		}
		pat, err := hs.ParsePattern(reg.Pattern)
		if err != nil {
			return nil, err
		}
		pat.Id = reg.Id
		if reg.Id == 0 {
			pat.Id = id
		}
		pats = append(pats, pat)
	}
	return b.buildHsBlockDB(pats)
}

func (b *baseBuiltinHsSig) buildBlockDBFromLines(patterns string) (hs.BlockDatabase, error) {
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
	return b.buildHsBlockDB(pats)
}

func (b *baseBuiltinHsSig) buildBlockDBFromHex(hexData string) (hs.BlockDatabase, error) {
	data, err := hex.DecodeString(hexData)
	if err != nil {
		return nil, err
	}
	return hs.UnmarshalBlockDatabase(data)
}

func (b *builtinHsMatchSig) Clone() builtinFunc {
	newSig := &builtinHsMatchSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	if b.db != nil {
		newSig.db = b.cloneDb()
	}
	return newSig
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
		db, err := b.buildBlockDBFromLines(patternStr)
		if err != nil {
			return 0, true, ErrRegexp.GenWithStackByArgs(err.Error())
		}
		b.db = db
	}
	_, matched := b.hsMatch(valStr)
	return boolToInt64(matched), false, nil
}

func (b *builtinHsMatchSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	bufVal, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(bufVal)
	if err := b.args[0].VecEvalString(b.ctx, input, bufVal); err != nil {
		return err
	}

	bufPat, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(bufPat)

	if err := b.args[1].VecEvalString(b.ctx, input, bufPat); err != nil {
		return err
	}
	if b.db == nil && n > 0 {
		for i := 0; i < n; i++ {
			if bufPat.IsNull(i) {
				continue
			}
			db, err := b.buildBlockDBFromLines(bufPat.GetString(i))
			if err != nil {
				return err
			}
			b.db = db
			break
		}
	}

	result.ResizeInt64(n, false)
	result.MergeNulls(bufVal, bufPat)
	i64s := result.Int64s()
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		_, matched := b.hsMatch(bufVal.GetString(i))
		i64s[i] = boolToInt64(matched)
	}
	return nil
}

func (b *builtinHsMatchSig) vectorized() bool {
	return true
}

func (b *builtinHsMatchJsonSig) Clone() builtinFunc {
	newSig := &builtinHsMatchJsonSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	if b.db != nil {
		newSig.db = b.cloneDb()
	}
	return newSig
}

func (b *builtinHsMatchJsonSig) evalInt(row chunk.Row) (int64, bool, error) {
	valStr, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	patternJsonStr, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	if b.db == nil {
		db, err := b.buildBlockDBFromJson(patternJsonStr)
		if err != nil {
			return 0, true, ErrRegexp.GenWithStackByArgs(err.Error())
		}
		b.db = db
	}
	_, matched := b.hsMatch(valStr)
	return boolToInt64(matched), false, nil
}

func (b *builtinHsMatchJsonSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	bufVal, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(bufVal)
	if err := b.args[0].VecEvalString(b.ctx, input, bufVal); err != nil {
		return err
	}

	bufPat, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(bufPat)

	if err := b.args[1].VecEvalString(b.ctx, input, bufPat); err != nil {
		return err
	}
	if b.db == nil && n > 0 {
		for i := 0; i < n; i++ {
			if bufPat.IsNull(i) {
				continue
			}
			db, err := b.buildBlockDBFromJson(bufPat.GetString(i))
			if err != nil {
				return err
			}
			b.db = db
			break
		}
	}

	result.ResizeInt64(n, false)
	result.MergeNulls(bufVal, bufPat)
	i64s := result.Int64s()
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		_, matched := b.hsMatch(bufVal.GetString(i))
		i64s[i] = boolToInt64(matched)
	}
	return nil
}

func (b *builtinHsMatchJsonSig) vectorized() bool {
	return true
}

func (b *builtinHsBuildDbJsonSig) Clone() builtinFunc {
	newSig := &builtinHsBuildDbJsonSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinHsBuildDbJsonSig) evalString(row chunk.Row) (string, bool, error) {
	patternJsonStr, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	db, err := b.buildBlockDBFromJson(patternJsonStr)
	if err != nil {
		return "", true, ErrRegexp.GenWithStackByArgs(err.Error())
	}
	data, err := db.Marshal()
	if err != nil {
		return "", true, ErrRegexp.GenWithStackByArgs(err.Error())
	}
	return hex.EncodeToString(data), false, nil
}

func (b *builtinHsMatchDbSig) evalInt(row chunk.Row) (int64, bool, error) {
	valStr, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	dbHex, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	if b.db == nil {
		db, err := b.buildBlockDBFromHex(dbHex)
		if err != nil {
			return 0, true, ErrRegexp.GenWithStackByArgs(err.Error())
		}
		b.db = db
	}
	_, matched := b.hsMatch(valStr)
	return boolToInt64(matched), false, nil
}

func (b *builtinHsMatchDbSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	bufVal, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(bufVal)
	if err := b.args[0].VecEvalString(b.ctx, input, bufVal); err != nil {
		return err
	}

	bufPat, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(bufPat)

	if err := b.args[1].VecEvalString(b.ctx, input, bufPat); err != nil {
		return err
	}
	if b.db == nil && n > 0 {
		for i := 0; i < n; i++ {
			if bufPat.IsNull(i) {
				continue
			}
			db, err := b.buildBlockDBFromHex(bufPat.GetString(i))
			if err != nil {
				return err
			}
			b.db = db
			break
		}
	}

	result.ResizeInt64(n, false)
	result.MergeNulls(bufVal, bufPat)
	i64s := result.Int64s()
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		_, matched := b.hsMatch(bufVal.GetString(i))
		i64s[i] = boolToInt64(matched)
	}
	return nil
}

func (b *builtinHsMatchDbSig) vectorized() bool {
	return true
}

func (b *builtinHsMatchDbSig) Clone() builtinFunc {
	newSig := &builtinHsMatchDbSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	if b.db != nil {
		newSig.db = b.cloneDb()
	}
	return newSig
}
