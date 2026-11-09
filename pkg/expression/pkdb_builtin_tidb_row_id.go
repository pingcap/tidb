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

package expression

import (
	"bytes"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/collate"
)

const tidbRowIDPredicateFunction = "tidb_row_id_predicate"
const tidbRowIDRangePredicateFunction = "tidb_row_id_range_predicate"

// TiDBRowIDHandleKind identifies the storage handle represented by the bound
// handle expressions of an internal TIDB_ROW_ID scalar.
type TiDBRowIDHandleKind uint8

const (
	// TiDBRowIDExtraHandle identifies an implicit _tidb_rowid handle.
	TiDBRowIDExtraHandle TiDBRowIDHandleKind = iota
	// TiDBRowIDIntHandle identifies an integer clustered-primary-key handle.
	TiDBRowIDIntHandle
	// TiDBRowIDCommonHandle identifies a clustered common handle.
	TiDBRowIDCommonHandle
)

type tidbRowIDFunctionClass struct {
	baseFunctionClass
}

func (c *tidbRowIDFunctionClass) getFunction(_ BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	// The public arguments name a table binding rather than scalar values.
	// Only the planner has enough information to construct the executable
	// internal signature through NewTiDBRowID.
	return nil, errIncorrectArgs.GenWithStackByArgs(ast.TiDBRowID)
}

type builtinTiDBRowIDSig struct {
	baseBuiltinFunc
	handleKind   TiDBRowIDHandleKind
	tableInfo    *model.TableInfo
	primaryIndex *model.IndexInfo
}

type builtinTiDBRowIDPredicateSig struct {
	baseBuiltinFunc
	not bool
}

// TiDBRowIDRangeOp identifies a direct ordered TIDB_ROW_ID predicate.
type TiDBRowIDRangeOp uint8

const (
	// TiDBRowIDRangeLT represents TIDB_ROW_ID(...) < bound.
	TiDBRowIDRangeLT TiDBRowIDRangeOp = iota
	// TiDBRowIDRangeLE represents TIDB_ROW_ID(...) <= bound.
	TiDBRowIDRangeLE
	// TiDBRowIDRangeGT represents TIDB_ROW_ID(...) > bound.
	TiDBRowIDRangeGT
	// TiDBRowIDRangeGE represents TIDB_ROW_ID(...) >= bound.
	TiDBRowIDRangeGE
	// TiDBRowIDRangeBetween represents an inclusive BETWEEN predicate.
	TiDBRowIDRangeBetween
)

type builtinTiDBRowIDRangePredicateSig struct {
	baseBuiltinFunc
	op  TiDBRowIDRangeOp
	not bool
}

// GetTiDBRowIDPredicate returns the planner-bound parts of a direct
// TIDB_ROW_ID predicate. It deliberately does not recognize ordinary scalar
// comparisons involving TIDB_ROW_ID.
func GetTiDBRowIDPredicate(expr Expression) (
	rowID Expression,
	candidates []Expression,
	not bool,
	ok bool,
) {
	scalar, ok := expr.(*ScalarFunction)
	if !ok || scalar.FuncName.L != tidbRowIDPredicateFunction {
		return nil, nil, false, false
	}
	sig, ok := scalar.Function.(*builtinTiDBRowIDPredicateSig)
	if !ok || len(sig.args) < 2 {
		return nil, nil, false, false
	}
	return sig.args[0], sig.args[1:], sig.not, true
}

// NewTiDBRowIDPredicate constructs a planner-only direct locator predicate.
// Every non-NULL candidate is validated before a result is returned, including
// candidates after an earlier IN match.
func NewTiDBRowIDPredicate(
	ctx BuildContext,
	not bool,
	rowID Expression,
	candidates ...Expression,
) (Expression, error) {
	if rowID == nil || len(candidates) == 0 {
		return nil, errors.New("TIDB_ROW_ID predicate requires locator and candidate expressions")
	}
	args := make([]Expression, 0, len(candidates)+1)
	args = append(args, rowID)
	for _, candidate := range candidates {
		if candidate == nil {
			return nil, errors.New("TIDB_ROW_ID predicate candidate is nil")
		}
		args = append(args, candidate)
	}

	retType := types.NewFieldType(mysql.TypeTiny)
	base, err := newBaseBuiltinFunc(ctx, tidbRowIDPredicateFunction, args, retType)
	if err != nil {
		return nil, err
	}
	sig := &builtinTiDBRowIDPredicateSig{
		baseBuiltinFunc: base,
		not:             not,
	}
	return &ScalarFunction{
		FuncName: pmodel.NewCIStr(tidbRowIDPredicateFunction),
		RetType:  retType,
		Function: sig,
	}, nil
}

// GetTiDBRowIDRangePredicate returns the planner-bound parts of a direct
// ordered TIDB_ROW_ID predicate.
func GetTiDBRowIDRangePredicate(expr Expression) (
	rowID Expression,
	bounds []Expression,
	op TiDBRowIDRangeOp,
	not bool,
	ok bool,
) {
	scalar, ok := expr.(*ScalarFunction)
	if !ok || scalar.FuncName.L != tidbRowIDRangePredicateFunction {
		return nil, nil, 0, false, false
	}
	sig, ok := scalar.Function.(*builtinTiDBRowIDRangePredicateSig)
	if !ok || len(sig.args) < 2 {
		return nil, nil, 0, false, false
	}
	return sig.args[0], sig.args[1:], sig.op, sig.not, true
}

// NewTiDBRowIDRangePredicate constructs a planner-only direct ordered locator
// predicate. Every non-NULL bound is decoded and checked against the bound
// table before the comparison is evaluated.
func NewTiDBRowIDRangePredicate(
	ctx BuildContext,
	op TiDBRowIDRangeOp,
	not bool,
	rowID Expression,
	bounds ...Expression,
) (Expression, error) {
	expectedBounds := 1
	if op == TiDBRowIDRangeBetween {
		expectedBounds = 2
	}
	if rowID == nil || len(bounds) != expectedBounds || op > TiDBRowIDRangeBetween {
		return nil, errors.New("invalid TIDB_ROW_ID range predicate")
	}
	args := make([]Expression, 0, len(bounds)+1)
	args = append(args, rowID)
	for _, bound := range bounds {
		if bound == nil {
			return nil, errors.New("TIDB_ROW_ID range bound is nil")
		}
		args = append(args, bound)
	}

	retType := types.NewFieldType(mysql.TypeTiny)
	base, err := newBaseBuiltinFunc(ctx, tidbRowIDRangePredicateFunction, args, retType)
	if err != nil {
		return nil, err
	}
	sig := &builtinTiDBRowIDRangePredicateSig{
		baseBuiltinFunc: base,
		op:              op,
		not:             not,
	}
	return &ScalarFunction{
		FuncName: pmodel.NewCIStr(tidbRowIDRangePredicateFunction),
		RetType:  retType,
		Function: sig,
	}, nil
}

func (b *builtinTiDBRowIDPredicateSig) Clone() builtinFunc {
	clone := &builtinTiDBRowIDPredicateSig{not: b.not}
	clone.cloneFrom(&b.baseBuiltinFunc)
	return clone
}

func (*builtinTiDBRowIDPredicateSig) vectorized() bool {
	return false
}

func (b *builtinTiDBRowIDPredicateSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	rowID, rowIDIsNull, err := b.args[0].EvalString(ctx, row)
	if err != nil || rowIDIsNull {
		return 0, rowIDIsNull, err
	}

	matched := false
	hasNull := false
	for _, candidate := range b.args[1:] {
		token, isNull, err := candidate.EvalString(ctx, row)
		if err != nil {
			return 0, false, err
		}
		if isNull {
			hasNull = true
			continue
		}
		_, _, _, canonical, err := tablecodec.DecodeTiDBRowID(token)
		if err != nil {
			return 0, false, err
		}
		matched = matched || canonical == rowID
	}

	if matched {
		if b.not {
			return 0, false, nil
		}
		return 1, false, nil
	}
	if hasNull {
		return 0, true, nil
	}
	if b.not {
		return 1, false, nil
	}
	return 0, false, nil
}

func (b *builtinTiDBRowIDRangePredicateSig) Clone() builtinFunc {
	clone := &builtinTiDBRowIDRangePredicateSig{op: b.op, not: b.not}
	clone.cloneFrom(&b.baseBuiltinFunc)
	return clone
}

func (*builtinTiDBRowIDRangePredicateSig) vectorized() bool {
	return false
}

func (b *builtinTiDBRowIDRangePredicateSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	rowToken, rowIsNull, err := b.args[0].EvalString(ctx, row)
	if err != nil || rowIsNull {
		return 0, rowIsNull, err
	}
	_, _, rowKey, _, err := tablecodec.DecodeTiDBRowID(rowToken)
	if err != nil {
		return 0, false, err
	}

	boundKeys := make([][]byte, len(b.args)-1)
	boundNulls := make([]bool, len(boundKeys))
	for i, bound := range b.args[1:] {
		token, isNull, err := bound.EvalString(ctx, row)
		if err != nil {
			return 0, false, err
		}
		boundNulls[i] = isNull
		if isNull {
			continue
		}
		boundKeys[i], err = validateTiDBRowIDRangeBound(b.args[0], token)
		if err != nil {
			return 0, false, err
		}
	}

	var matched bool
	var isNull bool
	if b.op == TiDBRowIDRangeBetween {
		lowerMatched := !boundNulls[0] && bytes.Compare(rowKey, boundKeys[0]) >= 0
		upperMatched := !boundNulls[1] && bytes.Compare(rowKey, boundKeys[1]) <= 0
		switch {
		case !boundNulls[0] && !lowerMatched, !boundNulls[1] && !upperMatched:
			matched = false
		case boundNulls[0] || boundNulls[1]:
			isNull = true
		default:
			matched = true
		}
	} else if boundNulls[0] {
		isNull = true
	} else {
		cmp := bytes.Compare(rowKey, boundKeys[0])
		switch b.op {
		case TiDBRowIDRangeLT:
			matched = cmp < 0
		case TiDBRowIDRangeLE:
			matched = cmp <= 0
		case TiDBRowIDRangeGT:
			matched = cmp > 0
		case TiDBRowIDRangeGE:
			matched = cmp >= 0
		}
	}
	if isNull {
		return 0, true, nil
	}
	if b.not {
		matched = !matched
	}
	if matched {
		return 1, false, nil
	}
	return 0, false, nil
}

func validateTiDBRowIDRangeBound(rowID Expression, token string) ([]byte, error) {
	scalar, ok := rowID.(*ScalarFunction)
	if !ok || scalar.FuncName.L != ast.TiDBRowID {
		return nil, tablecodec.ErrInvalidTiDBRowID
	}
	sig, ok := scalar.Function.(*builtinTiDBRowIDSig)
	if !ok || sig.tableInfo == nil {
		return nil, tablecodec.ErrInvalidTiDBRowID
	}
	physicalID, handle, rawKey, _, err := tablecodec.DecodeTiDBRowID(token)
	if err != nil {
		return nil, err
	}
	if (sig.handleKind == TiDBRowIDCommonHandle) == handle.IsInt() {
		return nil, tablecodec.ErrInvalidTiDBRowID
	}
	partitionInfo := sig.tableInfo.GetPartitionInfo()
	if partitionInfo == nil {
		if physicalID != sig.tableInfo.ID {
			return nil, tablecodec.ErrInvalidTiDBRowID
		}
		return rawKey, nil
	}
	for _, definition := range partitionInfo.Definitions {
		if definition.ID == physicalID {
			return rawKey, nil
		}
	}
	return nil, tablecodec.ErrInvalidTiDBRowID
}

// NewTiDBRowID constructs the planner-only scalar that encodes a bound row's
// physical table ID and handle. It returns SQL NULL when the bound row is a
// NULL-extended outer-join row. tableInfo and primaryIndex are cloned before
// they become evaluation metadata.
func NewTiDBRowID(
	ctx BuildContext,
	physicalID Expression,
	handleKind TiDBRowIDHandleKind,
	tableInfo *model.TableInfo,
	primaryIndex *model.IndexInfo,
	handleArgs ...Expression,
) (Expression, error) {
	if physicalID == nil {
		return nil, errors.New("TIDB_ROW_ID physical ID expression is nil")
	}
	if handleKind > TiDBRowIDCommonHandle {
		return nil, errors.Errorf("unknown TIDB_ROW_ID handle kind %d", handleKind)
	}
	if handleKind == TiDBRowIDCommonHandle {
		if tableInfo == nil || primaryIndex == nil || len(handleArgs) == 0 || len(handleArgs) != len(primaryIndex.Columns) {
			return nil, errors.New("invalid TIDB_ROW_ID common-handle metadata")
		}
	} else if len(handleArgs) != 1 {
		return nil, errors.New("TIDB_ROW_ID integer handle requires one expression")
	}
	for _, arg := range handleArgs {
		if arg == nil {
			return nil, errors.New("TIDB_ROW_ID handle expression is nil")
		}
	}

	args := make([]Expression, 0, 1+len(handleArgs))
	args = append(args, physicalID)
	args = append(args, handleArgs...)
	retType := types.NewFieldTypeBuilder().
		SetType(mysql.TypeVarString).
		SetFlen(mysql.MaxFieldVarCharLength).
		SetCharset(charset.CharsetASCII).
		SetCollate(charset.CollationASCII).
		BuildP()
	base, err := newBaseBuiltinFunc(ctx, ast.TiDBRowID, args, retType)
	if err != nil {
		return nil, err
	}
	base.tp = retType
	base.SetCharsetAndCollation(charset.CharsetASCII, charset.CollationASCII)
	base.setCollator(collate.GetCollator(charset.CollationASCII))

	sig := &builtinTiDBRowIDSig{
		baseBuiltinFunc: base,
		handleKind:      handleKind,
	}
	if tableInfo != nil {
		sig.tableInfo = tableInfo.Clone()
	}
	if primaryIndex != nil {
		sig.primaryIndex = primaryIndex.Clone()
	}
	return &ScalarFunction{
		FuncName: pmodel.NewCIStr(ast.TiDBRowID),
		RetType:  retType,
		Function: sig,
	}, nil
}

func (b *builtinTiDBRowIDSig) Clone() builtinFunc {
	clone := &builtinTiDBRowIDSig{handleKind: b.handleKind}
	clone.cloneFrom(&b.baseBuiltinFunc)
	if b.tableInfo != nil {
		clone.tableInfo = b.tableInfo.Clone()
	}
	if b.primaryIndex != nil {
		clone.primaryIndex = b.primaryIndex.Clone()
	}
	return clone
}

func (*builtinTiDBRowIDSig) vectorized() bool {
	return true
}

func (b *builtinTiDBRowIDSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	physicalID, isNull, err := b.args[0].EvalInt(ctx, row)
	if err != nil {
		return "", false, err
	}
	if isNull {
		return "", true, nil
	}
	if physicalID <= 0 {
		return "", false, errors.New("invalid TIDB_ROW_ID physical table ID")
	}

	handle, isNull, err := b.buildHandle(ctx, row)
	if err != nil {
		return "", false, err
	}
	if isNull {
		return "", true, nil
	}
	token, err := tablecodec.EncodeTiDBRowID(physicalID, handle)
	return token, false, err
}

func (b *builtinTiDBRowIDSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	rowCount := input.NumRows()
	result.ReserveString(rowCount)
	for i := range rowCount {
		token, isNull, err := b.evalString(ctx, input.GetRow(i))
		if err != nil {
			return err
		}
		if isNull {
			result.AppendNull()
		} else {
			result.AppendString(token)
		}
	}
	return nil
}

func (b *builtinTiDBRowIDSig) buildHandle(ctx EvalContext, row chunk.Row) (kv.Handle, bool, error) {
	if b.handleKind != TiDBRowIDCommonHandle {
		handle, isNull, err := b.args[1].EvalInt(ctx, row)
		if err != nil {
			return nil, false, err
		}
		if isNull {
			return nil, true, nil
		}
		return kv.IntHandle(handle), false, nil
	}

	datums := make([]types.Datum, 0, len(b.primaryIndex.Columns))
	for i, indexColumn := range b.primaryIndex.Columns {
		datum, err := b.args[i+1].Eval(ctx, row)
		if err != nil {
			return nil, false, err
		}
		if indexColumn.Offset < 0 || indexColumn.Offset >= len(b.tableInfo.Columns) {
			return nil, false, errors.New("invalid TIDB_ROW_ID common handle value")
		}
		if datum.IsNull() {
			return nil, true, nil
		}
		columnType := &b.tableInfo.Columns[indexColumn.Offset].FieldType
		datum, err = datum.ConvertTo(ctx.TypeCtx(), columnType)
		if err != nil {
			return nil, false, err
		}
		datums = append(datums, datum)
	}
	tablecodec.TruncateIndexValues(b.tableInfo, b.primaryIndex, datums)
	encoded, err := codec.EncodeKey(ctx.Location(), nil, datums...)
	errCtx := ctx.ErrCtx()
	err = errCtx.HandleError(err)
	if err != nil {
		return nil, false, err
	}
	handle, err := kv.NewCommonHandle(encoded)
	return handle, false, err
}
