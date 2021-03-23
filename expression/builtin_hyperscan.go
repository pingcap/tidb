package expression

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"runtime"
	"strings"

	hs "github.com/flier/gohs/hyperscan"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tipb/go-tipb"
	//"go.uber.org/zap"
)

var (
	// Build DB functions
	_ functionClass = &hsBuildDbJsonFunctionClass{}

	// Match any functions
	_ functionClass = &hsMatchFunctionClass{}
	_ functionClass = &hsMatchJsonFunctionClass{}
	_ functionClass = &hsMatchDbFunctionClass{}

	// Match all functions
	_ functionClass = &hsMatchAllFunctionClass{}
	_ functionClass = &hsMatchAllJsonFunctionClass{}

	// Match all ids functions
	_ functionClass = &hsMatchIdsFunctionClass{}
	_ functionClass = &hsMatchIdsJsonFunctionClass{}
	_ functionClass = &hsMatchIdsDbFunctionClass{}
)

var (
	_ builtinFunc = &builtinHsMatchSig{}
	_ builtinFunc = &builtinHsBuildDbSig{}
)

var (
	HSBuildDBJson = "hs_build_db_json"

	HSMatch     = "hs_match"
	HSMatchJson = "hs_match_json"
	HSMatchDb   = "hs_match_db"

	HSMatchAll     = "hs_match_all"
	HSMatchAllJson = "hs_match_all_json"

	HSMatchIds     = "hs_match_ids"
	HSMatchIdsJson = "hs_match_ids_json"
	HSMatchIdsDb   = "hs_match_ids_db"

	errAlreadyMatched    = fmt.Errorf("Already Matched")
	errInvalidEncodeType = fmt.Errorf("Invalid hpyerscan database encode type should be (hex | base64)")
)

const (
	// Build DB functions
	ScalarFuncSig_HsBuildDbJson tipb.ScalarFuncSig = 4320

	// Match functions
	ScalarFuncSig_HsMatch        tipb.ScalarFuncSig = 4331
	ScalarFuncSig_HsMatchDb      tipb.ScalarFuncSig = 4332
	ScalarFuncSig_HsMatchJson    tipb.ScalarFuncSig = 4333
	ScalarFuncSig_HsMatchAll     tipb.ScalarFuncSig = 4334
	ScalarFuncSig_HsMatchAllJson tipb.ScalarFuncSig = 4335
	ScalarFuncSig_HsMatchIds     tipb.ScalarFuncSig = 4336
	ScalarFuncSig_HsMatchIdsDb   tipb.ScalarFuncSig = 4337
	ScalarFuncSig_HsMatchIdsJson tipb.ScalarFuncSig = 4338

	hsSourceType_Lines  = 1
	hsSourceType_JSON   = 2
	hsSourceType_Hex    = 3
	hsSourceType_Base64 = 4

	hsEncodeType_Hex    = 1
	hsEncodeType_Base64 = 2

	hsMatchType_Any = 1
	hsMatchType_All = 2
)

type baseBuiltinHsSig struct {
	baseBuiltinFunc
	sourceType  int
	matchType   int
	numPatterns int
	db          hs.BlockDatabase
}

// Sig classes
type builtinHsMatchSig struct {
	baseBuiltinHsSig
}

type builtinHsBuildDbSig struct {
	baseBuiltinFunc
	sourceType int
	encodeType int
}

// End Sig classes

// Function classes

// Build DB functions
type hsBuildDbJsonFunctionClass struct {
	baseFunctionClass
}

// Match functions
type hsMatchFunctionClass struct {
	baseFunctionClass
}

type hsMatchDbFunctionClass struct {
	baseFunctionClass
}

type hsMatchJsonFunctionClass struct {
	baseFunctionClass
}

type hsMatchIdsFunctionClass struct {
	baseFunctionClass
}

type hsMatchIdsJsonFunctionClass struct {
	baseFunctionClass
}

type hsMatchIdsDbFunctionClass struct {
	baseFunctionClass
}

type hsMatchAllFunctionClass struct {
	baseFunctionClass
}

type hsMatchAllDbFunctionClass struct {
	baseFunctionClass
}

type hsMatchAllJsonFunctionClass struct {
	baseFunctionClass
}

// End Function classes

func (c *hsBuildDbJsonFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTp := []types.EvalType{types.ETString, types.ETString}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, argTp...)
	if err != nil {
		return nil, err
	}
	bf.tp.Flen = 1
	sig := &builtinHsBuildDbSig{bf, hsSourceType_JSON, 0}
	sig.setPbCode(ScalarFuncSig_HsBuildDbJson)
	return sig, nil
}

func (c *hsMatchFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTp := []types.EvalType{types.ETString, types.ETString}
	base, err := newBaseBuiltinHsSig(c.funcName, ctx, args, argTp, types.ETInt, hsSourceType_Lines, hsMatchType_Any)
	if err != nil {
		return nil, err
	}
	sig := &builtinHsMatchSig{base}
	sig.setPbCode(ScalarFuncSig_HsMatch)
	return sig, nil
}

func (c *hsMatchDbFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTp := []types.EvalType{types.ETString, types.ETString}
	base, err := newBaseBuiltinHsSig(c.funcName, ctx, args, argTp, types.ETInt, hsSourceType_Hex, hsMatchType_Any)
	if err != nil {
		return nil, err
	}
	sig := &builtinHsMatchSig{base}
	sig.setPbCode(ScalarFuncSig_HsMatchDb)
	return sig, nil
}

func (c *hsMatchJsonFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTp := []types.EvalType{types.ETString, types.ETString}
	base, err := newBaseBuiltinHsSig(c.funcName, ctx, args, argTp, types.ETInt, hsSourceType_JSON, hsMatchType_Any)
	if err != nil {
		return nil, err
	}
	sig := &builtinHsMatchSig{base}
	sig.setPbCode(ScalarFuncSig_HsMatchJson)
	return sig, nil
}

func (c *hsMatchIdsFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTp := []types.EvalType{types.ETString, types.ETString}
	base, err := newBaseBuiltinHsSig(c.funcName, ctx, args, argTp, types.ETString, hsSourceType_Lines, hsMatchType_All)
	if err != nil {
		return nil, err
	}
	sig := &builtinHsMatchSig{base}
	sig.setPbCode(ScalarFuncSig_HsMatch)
	return sig, nil
}

func (c *hsMatchIdsJsonFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTp := []types.EvalType{types.ETString, types.ETString}
	base, err := newBaseBuiltinHsSig(c.funcName, ctx, args, argTp, types.ETString, hsSourceType_JSON, hsMatchType_All)
	if err != nil {
		return nil, err
	}
	sig := &builtinHsMatchSig{base}
	sig.setPbCode(ScalarFuncSig_HsMatchJson)
	return sig, nil
}

func (c *hsMatchIdsDbFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTp := []types.EvalType{types.ETString, types.ETString}
	base, err := newBaseBuiltinHsSig(c.funcName, ctx, args, argTp, types.ETString, hsSourceType_Hex, hsMatchType_All)
	if err != nil {
		return nil, err
	}
	sig := &builtinHsMatchSig{base}
	sig.setPbCode(ScalarFuncSig_HsMatchDb)
	return sig, nil
}

func (c *hsMatchAllFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTp := []types.EvalType{types.ETString, types.ETString}
	base, err := newBaseBuiltinHsSig(c.funcName, ctx, args, argTp, types.ETInt, hsSourceType_Lines, hsMatchType_All)
	if err != nil {
		return nil, err
	}
	sig := &builtinHsMatchSig{base}
	sig.setPbCode(ScalarFuncSig_HsMatchAll)
	return sig, nil
}

func (c *hsMatchAllJsonFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTp := []types.EvalType{types.ETString, types.ETString}
	base, err := newBaseBuiltinHsSig(c.funcName, ctx, args, argTp, types.ETInt, hsSourceType_JSON, hsMatchType_All)
	if err != nil {
		return nil, err
	}
	sig := &builtinHsMatchSig{base}
	sig.setPbCode(ScalarFuncSig_HsMatchAllJson)
	return sig, nil
}

func newBaseBuiltinHsSig(name string, ctx sessionctx.Context, args []Expression, argType []types.EvalType, retType types.EvalType, sourceType, matchType int) (baseBuiltinHsSig, error) {
	bf, err := newBaseBuiltinFuncWithTp(ctx, name, args, retType, argType...)
	if err != nil {
		return baseBuiltinHsSig{}, err
	}
	bf.tp.Flen = 1
	return baseBuiltinHsSig{bf, sourceType, matchType, 0, nil}, nil
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
	runtime.SetFinalizer(ret, func(db hs.BlockDatabase) {
		logutil.BgLogger().Info("[DEBUG] destroy hyperscan database")
		db.Close()
	})
	return ret
}

func (b *baseBuiltinHsSig) hsMatch(val string) bool {
	switch b.matchType {
	case hsMatchType_All:
		return b.hsMatchAll(val)
	case hsMatchType_Any:
		_, ret := b.hsMatchAny(val)
		return ret
	}
	return false
}

func (b *baseBuiltinHsSig) hsMatchAll(val string) bool {
	matchCount := 0
	if b.db == nil {
		return false
	}
	handler := func(id uint, from, to uint64, flags uint, context interface{}) error {
		matchCount++
		return nil
	}
	err := b.db.Scan([]byte(val), nil, handler, nil)
	if err != nil && err.(hs.HsError) != hs.ErrScanTerminated {
		return false
	}
	return matchCount >= b.numPatterns
}

func (b *baseBuiltinHsSig) hsMatchAny(val string) (int, bool) {
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

func (b *baseBuiltinHsSig) hsMatchIds(val string) []string {
	matched := false
	matchedIds := make([]string, 0)
	if b.db == nil {
		return matchedIds
	}
	handler := func(id uint, from, to uint64, flags uint, context interface{}) error {
		matchedIds = append(matchedIds, fmt.Sprintf("%d", id))
		matched = true
		// If match any just return the first match pattern ID
		if matched && b.matchType == hsMatchType_Any {
			return errAlreadyMatched
		}
		return nil
	}
	err := b.db.Scan([]byte(val), nil, handler, nil)
	if err != nil && err.(hs.HsError) != hs.ErrScanTerminated {
		return nil
	}
	return matchedIds
}

func buildHsBlockDB(patterns []*hs.Pattern) (hs.BlockDatabase, int, error) {
	if len(patterns) == 0 {
		return nil, 0, nil
	}
	builder := hs.DatabaseBuilder{
		Patterns: patterns,
		Mode:     hs.BlockMode,
		Platform: hs.PopulatePlatform(),
	}
	db, err := builder.Build()
	if err != nil {
		return nil, 0, err
	}
	runtime.SetFinalizer(db.(hs.BlockDatabase), func(cdb hs.BlockDatabase) {
		logutil.BgLogger().Info("[DEBUG] destroy hyperscan database")
		cdb.Close()
	})
	return db.(hs.BlockDatabase), len(patterns), err
}

type hsPatternObj struct {
	Id      int    `json:"id,omitempty"`
	Pattern string `json:"pattern"`
}

func buildBlockDBFromJson(patternsJson string) (hs.BlockDatabase, int, error) {
	patterns := make([]hsPatternObj, 0)
	err := json.Unmarshal([]byte(patternsJson), &patterns)
	if err != nil {
		return nil, 0, err
	}

	pats := make([]*hs.Pattern, 0, len(patterns))
	pid := 1
	for _, reg := range patterns {
		if reg.Pattern == "" {
			continue
		}
		pat, err := hs.ParsePattern(reg.Pattern)
		if err != nil {
			return nil, 0, err
		}
		pat.Id = reg.Id
		if reg.Id == 0 {
			pat.Id = pid
		}
		pats = append(pats, pat)
		pid++
	}
	return buildHsBlockDB(pats)
}

func buildBlockDBFromLines(patterns string) (hs.BlockDatabase, int, error) {
	lines := strings.Split(patterns, "\n")
	pats := make([]*hs.Pattern, 0, len(lines))
	pid := 1
	for _, reg := range lines {
		if reg == "" {
			continue
		}
		pat, err := hs.ParsePattern(reg)
		if err != nil {
			return nil, 0, err
		}
		pat.Id = pid
		pats = append(pats, pat)
		pid++
	}
	return buildHsBlockDB(pats)
}

func buildBlockDBFromHex(hexData string) (hs.BlockDatabase, int, error) {
	data, err := hex.DecodeString(hexData)
	if err != nil {
		return nil, 0, err
	}
	db, err := hs.UnmarshalBlockDatabase(data)
	return db, 0, err
}

func buildBlockDBFromBase64(base64Data string) (hs.BlockDatabase, int, error) {
	data, err := base64.StdEncoding.DecodeString(base64Data)
	if err != nil {
		return nil, 0, err
	}
	db, err := hs.UnmarshalBlockDatabase(data)
	return db, 0, err
}

func buildBlockDB(source string, sourceTp int) (hs.BlockDatabase, int, error) {
	switch sourceTp {
	case hsSourceType_JSON:
		return buildBlockDBFromJson(source)
	case hsSourceType_Hex:
		return buildBlockDBFromHex(source)
	case hsSourceType_Base64:
		return buildBlockDBFromBase64(source)
	default:
		return buildBlockDBFromLines(source)
	}
}

func (b *baseBuiltinHsSig) buildBlockDB(source string) (hs.BlockDatabase, error) {
	db, num, err := buildBlockDB(source, b.sourceType)
	b.numPatterns = num
	return db, err
}

func (b *baseBuiltinHsSig) evalInt(row chunk.Row) (int64, bool, error) {
	valStr, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	patternSource, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	if b.db == nil {
		db, err := b.buildBlockDB(patternSource)
		if err != nil {
			return 0, true, ErrRegexp.GenWithStackByArgs(err.Error())
		}
		b.db = db
	}
	matched := b.hsMatch(valStr)
	return boolToInt64(matched), false, nil
}

func (b *builtinHsMatchSig) evalString(row chunk.Row) (string, bool, error) {
	valStr, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	patternSource, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}

	if b.db == nil {
		db, err := b.buildBlockDB(patternSource)
		if err != nil {
			return "", true, ErrRegexp.GenWithStackByArgs(err.Error())
		}
		b.db = db
	}
	matchedIds := b.hsMatchIds(valStr)
	if len(matchedIds) == 0 {
		return "", false, nil
	}
	return strings.Join(matchedIds, ","), false, nil
}

func (b *baseBuiltinHsSig) cloneFromBaseHsSig(source *baseBuiltinHsSig) {
	b.sourceType = source.sourceType
	b.matchType = source.matchType
	b.numPatterns = source.numPatterns
	if source.db != nil {
		b.db = b.cloneDb()
	}
}

func (b *builtinHsMatchSig) Clone() builtinFunc {
	newSig := &builtinHsMatchSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.cloneFromBaseHsSig(&b.baseBuiltinHsSig)
	return newSig
}

func (b *builtinHsBuildDbSig) Clone() builtinFunc {
	newSig := &builtinHsBuildDbSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.sourceType = b.sourceType
	newSig.encodeType = b.encodeType
	return newSig
}

func (b *builtinHsBuildDbSig) evalString(row chunk.Row) (string, bool, error) {
	patternSource, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}

	encodeType, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}

	encTp := strings.ToLower(encodeType)
	switch encTp {
	case "hex":
		b.encodeType = hsEncodeType_Hex
	case "base64":
		b.encodeType = hsEncodeType_Base64
	default:
		return "", false, errInvalidEncodeType
	}

	db, err := b.buildBlockDB(patternSource)
	if err != nil {
		return "", true, ErrRegexp.GenWithStackByArgs(err.Error())
	}
	data, err := db.Marshal()
	if err != nil {
		return "", true, ErrRegexp.GenWithStackByArgs(err.Error())
	}
	switch b.encodeType {
	case hsEncodeType_Base64:
		return base64.StdEncoding.EncodeToString(data), false, nil
	default:
		return hex.EncodeToString(data), false, nil
	}
}

func (b *builtinHsBuildDbSig) buildBlockDB(source string) (hs.BlockDatabase, error) {
	db, _, err := buildBlockDB(source, b.sourceType)
	return db, err
}
