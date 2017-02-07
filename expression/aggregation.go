// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/charset"
	"github.com/pingcap/tidb/util/types"
)

// AggregationFunction stands for aggregate functions.
type AggregationFunction interface {
	fmt.Stringer
	json.Marshaler
	// Update during executing.
	Update(row []types.Datum, groupKey []byte, ctx context.Context) error

	// StreamUpdate updates data using streaming algo.
	StreamUpdate(row []types.Datum, ctx context.Context) error

	// SetMode sets aggFunctionMode for aggregate function.
	SetMode(mode AggFunctionMode)

	// GetMode gets aggFunctionMode from aggregate function.
	GetMode() AggFunctionMode

	// GetGroupResult will be called when all data have been processed.
	GetGroupResult(groupKey []byte) types.Datum

	// GetStreamResult gets a result using streaming agg.
	GetStreamResult() types.Datum

	// GetArgs stands for getting all arguments.
	GetArgs() []Expression

	// GetName gets the aggregation function name.
	GetName() string

	// SetArgs sets argument by index.
	SetArgs(args []Expression)

	// Clear collects the mapper's memory.
	Clear()

	// IsDistinct indicates if the aggregate function contains distinct attribute.
	IsDistinct() bool

	// SetContext sets the aggregate evaluation context.
	SetContext(ctx map[string](*aggEvaluateContext))

	// Equal checks whether two aggregation functions are equal.
	Equal(agg AggregationFunction, ctx context.Context) bool

	// Clone copies an aggregate function totally.
	Clone() AggregationFunction

	// GetType gets field type of aggregate function.
	GetType() *types.FieldType

	// CalculateDefaultValue gets the default value when the aggregate function's input is null.
	// The input stands for the schema of Aggregation's child. If the function can't produce a default value, the second
	// return value will be false.
	CalculateDefaultValue(schema *Schema, ctx context.Context) (types.Datum, bool)
}

// aggEvaluateContext is used to store intermediate result when calculating aggregate functions.
type aggEvaluateContext struct {
	DistinctChecker *distinctChecker
	Count           int64
	Value           types.Datum
	Buffer          *bytes.Buffer // Buffer is used for group_concat.
	GotFirstRow     bool          // It will check if the agg has met the first row key.
}

// NewAggFunction creates a new AggregationFunction.
func NewAggFunction(funcType string, funcArgs []Expression, distinct bool) AggregationFunction {
	switch tp := strings.ToLower(funcType); tp {
	case ast.AggFuncSum:
		return &sumFunction{aggFunction: newAggFunc(tp, funcArgs, distinct)}
	case ast.AggFuncCount:
		return &countFunction{aggFunction: newAggFunc(tp, funcArgs, distinct)}
	case ast.AggFuncAvg:
		return &avgFunction{aggFunction: newAggFunc(tp, funcArgs, distinct)}
	case ast.AggFuncGroupConcat:
		return &concatFunction{aggFunction: newAggFunc(tp, funcArgs, distinct)}
	case ast.AggFuncMax:
		return &maxMinFunction{aggFunction: newAggFunc(tp, funcArgs, distinct), isMax: true}
	case ast.AggFuncMin:
		return &maxMinFunction{aggFunction: newAggFunc(tp, funcArgs, distinct), isMax: false}
	case ast.AggFuncFirstRow:
		return &firstRowFunction{aggFunction: newAggFunc(tp, funcArgs, distinct)}
	}
	return nil
}

type aggCtxMapper map[string]*aggEvaluateContext

// AggFunctionMode stands for the aggregation function's mode.
type AggFunctionMode int

const (
	// CompleteMode function accepts origin data.
	CompleteMode AggFunctionMode = iota
	// FinalMode function accepts partial data.
	FinalMode
)

type aggFunction struct {
	name         string
	mode         AggFunctionMode
	Args         []Expression
	Distinct     bool
	resultMapper aggCtxMapper
	streamCtx    *aggEvaluateContext
}

// Equal implements AggregationFunction interface.
func (af *aggFunction) Equal(b AggregationFunction, ctx context.Context) bool {
	if af.GetName() != b.GetName() {
		return false
	}
	if af.Distinct != b.IsDistinct() {
		return false
	}
	if len(af.GetArgs()) == len(b.GetArgs()) {
		for i, argA := range af.GetArgs() {
			if !argA.Equal(b.GetArgs()[i], ctx) {
				return false
			}
		}
	}
	return true
}

// String implements fmt.Stringer interface.
func (af *aggFunction) String() string {
	result := af.name + "("
	for i, arg := range af.Args {
		result += arg.String()
		if i+1 != len(af.Args) {
			result += ", "
		}
	}
	result += ")"
	return result
}

// MarshalJSON implements json.Marshaler interface.
func (af *aggFunction) MarshalJSON() ([]byte, error) {
	buffer := bytes.NewBufferString(fmt.Sprintf("\"%s\"", af))
	return buffer.Bytes(), nil
}

func newAggFunc(name string, args []Expression, dist bool) aggFunction {
	return aggFunction{
		name:         name,
		Args:         args,
		resultMapper: make(aggCtxMapper, 0),
		Distinct:     dist,
	}
}

// CalculateDefaultValue implements AggregationFunction interface.
func (af *aggFunction) CalculateDefaultValue(schema *Schema, ctx context.Context) (types.Datum, bool) {
	return types.Datum{}, false
}

// IsDistinct implements AggregationFunction interface.
func (af *aggFunction) IsDistinct() bool {
	return af.Distinct
}

// Clear implements AggregationFunction interface.
func (af *aggFunction) Clear() {
	af.resultMapper = make(aggCtxMapper, 0)
	af.streamCtx = nil
}

// GetName implements AggregationFunction interface.
func (af *aggFunction) GetName() string {
	return af.name
}

// SetMode implements AggregationFunction interface.
func (af *aggFunction) SetMode(mode AggFunctionMode) {
	af.mode = mode
}

// GetMode implements AggregationFunction interface.
func (af *aggFunction) GetMode() AggFunctionMode {
	return af.mode
}

// GetArgs implements AggregationFunction interface.
func (af *aggFunction) GetArgs() []Expression {
	return af.Args
}

// SetArgs implements AggregationFunction interface.
func (af *aggFunction) SetArgs(args []Expression) {
	af.Args = args
}

func (af *aggFunction) getContext(groupKey []byte) *aggEvaluateContext {
	ctx, ok := af.resultMapper[string(groupKey)]
	if !ok {
		ctx = &aggEvaluateContext{}
		if af.Distinct {
			ctx.DistinctChecker = createDistinctChecker()
		}
		af.resultMapper[string(groupKey)] = ctx
	}
	return ctx
}

func (af *aggFunction) getStreamedContext() *aggEvaluateContext {
	if af.streamCtx == nil {
		af.streamCtx = &aggEvaluateContext{}
		if af.Distinct {
			af.streamCtx.DistinctChecker = createDistinctChecker()
		}
	}
	return af.streamCtx
}

// SetContext implements AggregationFunction interface.
func (af *aggFunction) SetContext(ctx map[string](*aggEvaluateContext)) {
	af.resultMapper = ctx
}

func (af *aggFunction) updateSum(row []types.Datum, groupKey []byte, ectx context.Context) error {
	ctx := af.getContext(groupKey)
	a := af.Args[0]
	value, err := a.Eval(row)
	if err != nil {
		return errors.Trace(err)
	}
	if value.IsNull() {
		return nil
	}
	if af.Distinct {
		d, err1 := ctx.DistinctChecker.Check([]interface{}{value.GetValue()})
		if err1 != nil {
			return errors.Trace(err1)
		}
		if !d {
			return nil
		}
	}
	ctx.Value, err = calculateSum(ectx.GetSessionVars().StmtCtx, ctx.Value, value)
	if err != nil {
		return errors.Trace(err)
	}
	ctx.Count++
	return nil
}

func (af *aggFunction) streamUpdateSum(row []types.Datum, ectx context.Context) error {
	ctx := af.getStreamedContext()
	a := af.Args[0]
	value, err := a.Eval(row)
	if err != nil {
		return errors.Trace(err)
	}
	if value.IsNull() {
		return nil
	}
	if af.Distinct {
		d, err1 := ctx.DistinctChecker.Check([]interface{}{value.GetValue()})
		if err1 != nil {
			return errors.Trace(err1)
		}
		if !d {
			return nil
		}
	}
	ctx.Value, err = calculateSum(ectx.GetSessionVars().StmtCtx, ctx.Value, value)
	if err != nil {
		return errors.Trace(err)
	}
	ctx.Count++
	return nil
}

type sumFunction struct {
	aggFunction
}

// Clone implements AggregationFunction interface.
func (sf *sumFunction) Clone() AggregationFunction {
	nf := *sf
	for i, arg := range sf.Args {
		nf.Args[i] = arg.Clone()
	}
	nf.resultMapper = make(aggCtxMapper)
	return &nf
}

// Update implements AggregationFunction interface.
func (sf *sumFunction) Update(row []types.Datum, groupKey []byte, ctx context.Context) error {
	return sf.updateSum(row, groupKey, ctx)
}

// StreamUpdate implements AggregationFunction interface.
func (sf *sumFunction) StreamUpdate(row []types.Datum, ectx context.Context) error {
	return sf.streamUpdateSum(row, ectx)
}

// GetGroupResult implements AggregationFunction interface.
func (sf *sumFunction) GetGroupResult(groupKey []byte) (d types.Datum) {
	return sf.getContext(groupKey).Value
}

// GetStreamResult implements AggregationFunction interface.
func (sf *sumFunction) GetStreamResult() (d types.Datum) {
	if sf.streamCtx == nil {
		return
	}
	d = sf.streamCtx.Value
	sf.streamCtx = nil
	return
}

// CalculateDefaultValue implements AggregationFunction interface.
func (sf *sumFunction) CalculateDefaultValue(schema *Schema, ctx context.Context) (d types.Datum, valid bool) {
	arg := sf.Args[0]
	result, err := EvaluateExprWithNull(ctx, schema, arg)
	if err != nil {
		log.Warnf("Evaluate expr with null failed in function %s, err msg is %s", sf, err.Error())
		return d, false
	}
	if con, ok := result.(*Constant); ok {
		d, err = calculateSum(ctx.GetSessionVars().StmtCtx, d, con.Value)
		if err != nil {
			log.Warnf("CalculateSum failed in function %s, err msg is %s", sf, err.Error())
		}
		return d, err == nil
	}
	return d, false
}

// GetType implements AggregationFunction interface.
func (sf *sumFunction) GetType() *types.FieldType {
	ft := types.NewFieldType(mysql.TypeNewDecimal)
	ft.Charset = charset.CharsetBin
	ft.Collate = charset.CollationBin
	ft.Decimal = sf.Args[0].GetType().Decimal
	return ft
}

type countFunction struct {
	aggFunction
}

// Clone implements AggregationFunction interface.
func (cf *countFunction) Clone() AggregationFunction {
	nf := *cf
	for i, arg := range cf.Args {
		nf.Args[i] = arg.Clone()
	}
	nf.resultMapper = make(aggCtxMapper)
	return &nf
}

// CalculateDefaultValue implements AggregationFunction interface.
func (cf *countFunction) CalculateDefaultValue(schema *Schema, ctx context.Context) (d types.Datum, valid bool) {
	for _, arg := range cf.Args {
		result, err := EvaluateExprWithNull(ctx, schema, arg)
		if err != nil {
			log.Warnf("Evaluate expr with null failed in function %s, err msg is %s", cf, err.Error())
			return d, false
		}
		if con, ok := result.(*Constant); ok {
			if con.Value.IsNull() {
				return types.NewDatum(0), true
			}
		} else {
			return d, false
		}
	}
	return types.NewDatum(1), true
}

// GetType implements AggregationFunction interface.
func (cf *countFunction) GetType() *types.FieldType {
	ft := types.NewFieldType(mysql.TypeLonglong)
	ft.Flen = 21
	ft.Charset = charset.CharsetBin
	ft.Collate = charset.CollationBin
	return ft
}

// Update implements AggregationFunction interface.
func (cf *countFunction) Update(row []types.Datum, groupKey []byte, ectx context.Context) error {
	ctx := cf.getContext(groupKey)
	var vals []interface{}
	if cf.Distinct {
		vals = make([]interface{}, 0, len(cf.Args))
	}
	for _, a := range cf.Args {
		value, err := a.Eval(row)
		if err != nil {
			return errors.Trace(err)
		}
		if value.GetValue() == nil {
			return nil
		}
		if cf.mode == FinalMode {
			ctx.Count += value.GetInt64()
		}
		if cf.Distinct {
			vals = append(vals, value.GetValue())
		}
	}
	if cf.Distinct {
		d, err := ctx.DistinctChecker.Check(vals)
		if err != nil {
			return errors.Trace(err)
		}
		if !d {
			return nil
		}
	}
	if cf.mode == CompleteMode {
		ctx.Count++
	}
	return nil
}

// StreamUpdate implements AggregationFunction interface.
func (cf *countFunction) StreamUpdate(row []types.Datum, ectx context.Context) error {
	ctx := cf.getStreamedContext()
	var vals []interface{}
	if cf.Distinct {
		vals = make([]interface{}, 0, len(cf.Args))
	}
	for _, a := range cf.Args {
		value, err := a.Eval(row)
		if err != nil {
			return errors.Trace(err)
		}
		if value.GetValue() == nil {
			return nil
		}
		if cf.Distinct {
			vals = append(vals, value.GetValue())
		}
	}
	if cf.Distinct {
		d, err := ctx.DistinctChecker.Check(vals)
		if err != nil {
			return errors.Trace(err)
		}
		if !d {
			return nil
		}
	}
	ctx.Count++
	return nil
}

// GetGroupResult implements AggregationFunction interface.
func (cf *countFunction) GetGroupResult(groupKey []byte) (d types.Datum) {
	d.SetInt64(cf.getContext(groupKey).Count)
	return d
}

// GetStreamResult implements AggregationFunction interface.
func (cf *countFunction) GetStreamResult() (d types.Datum) {
	if cf.streamCtx == nil {
		return types.NewDatum(0)
	}
	d.SetInt64(cf.streamCtx.Count)
	cf.streamCtx = nil
	return
}

type avgFunction struct {
	aggFunction
}

// Clone implements AggregationFunction interface.
func (af *avgFunction) Clone() AggregationFunction {
	nf := *af
	for i, arg := range af.Args {
		nf.Args[i] = arg.Clone()
	}
	nf.resultMapper = make(aggCtxMapper)
	return &nf
}

// GetType implements AggregationFunction interface.
func (af *avgFunction) GetType() *types.FieldType {
	ft := types.NewFieldType(mysql.TypeNewDecimal)
	ft.Charset = charset.CharsetBin
	ft.Collate = charset.CollationBin
	ft.Decimal = af.Args[0].GetType().Decimal
	return ft
}

func (af *avgFunction) updateAvg(row []types.Datum, groupKey []byte, ectx context.Context) error {
	ctx := af.getContext(groupKey)
	a := af.Args[1]
	value, err := a.Eval(row)
	if err != nil {
		return errors.Trace(err)
	}
	if value.IsNull() {
		return nil
	}
	if af.Distinct {
		d, err1 := ctx.DistinctChecker.Check([]interface{}{value.GetValue()})
		if err1 != nil {
			return errors.Trace(err1)
		}
		if !d {
			return nil
		}
	}
	ctx.Value, err = calculateSum(ectx.GetSessionVars().StmtCtx, ctx.Value, value)
	if err != nil {
		return errors.Trace(err)
	}
	count, err := af.Args[0].Eval(row)
	if err != nil {
		return errors.Trace(err)
	}
	ctx.Count += count.GetInt64()
	return nil
}

// Update implements AggregationFunction interface.
func (af *avgFunction) Update(row []types.Datum, groupKey []byte, ctx context.Context) error {
	if af.mode == FinalMode {
		return af.updateAvg(row, groupKey, ctx)
	}
	return af.updateSum(row, groupKey, ctx)
}

// StreamUpdate implements AggregationFunction interface.
func (af *avgFunction) StreamUpdate(row []types.Datum, ctx context.Context) error {
	return af.streamUpdateSum(row, ctx)
}

func (af *avgFunction) calculateResult(ctx *aggEvaluateContext) (d types.Datum) {
	switch ctx.Value.Kind() {
	case types.KindFloat64:
		t := ctx.Value.GetFloat64() / float64(ctx.Count)
		d.SetValue(t)
	case types.KindMysqlDecimal:
		x := ctx.Value.GetMysqlDecimal()
		y := types.NewDecFromInt(ctx.Count)
		to := new(types.MyDecimal)
		types.DecimalDiv(x, y, to, types.DivFracIncr)
		to.Round(to, ctx.Value.Frac()+types.DivFracIncr)
		d.SetMysqlDecimal(to)
	}
	return
}

// GetGroupResult implements AggregationFunction interface.
func (af *avgFunction) GetGroupResult(groupKey []byte) types.Datum {
	ctx := af.getContext(groupKey)
	return af.calculateResult(ctx)
}

// GetStreamResult implements AggregationFunction interface.
func (af *avgFunction) GetStreamResult() (d types.Datum) {
	if af.streamCtx == nil {
		return
	}
	d = af.calculateResult(af.streamCtx)
	af.streamCtx = nil
	return
}

type concatFunction struct {
	aggFunction
}

// Clone implements AggregationFunction interface.
func (cf *concatFunction) Clone() AggregationFunction {
	nf := *cf
	for i, arg := range cf.Args {
		nf.Args[i] = arg.Clone()
	}
	nf.resultMapper = make(aggCtxMapper)
	return &nf
}

// GetType implements AggregationFunction interface.
func (cf *concatFunction) GetType() *types.FieldType {
	return types.NewFieldType(mysql.TypeVarString)
}

// Update implements AggregationFunction interface.
func (cf *concatFunction) Update(row []types.Datum, groupKey []byte, ectx context.Context) error {
	ctx := cf.getContext(groupKey)
	vals := make([]interface{}, 0, len(cf.Args))
	for _, a := range cf.Args {
		value, err := a.Eval(row)
		if err != nil {
			return errors.Trace(err)
		}
		if value.GetValue() == nil {
			return nil
		}
		vals = append(vals, value.GetValue())
	}
	if cf.Distinct {
		d, err := ctx.DistinctChecker.Check(vals)
		if err != nil {
			return errors.Trace(err)
		}
		if !d {
			return nil
		}
	}
	if ctx.Buffer == nil {
		ctx.Buffer = &bytes.Buffer{}
	} else {
		// now use comma separator
		ctx.Buffer.WriteString(",")
	}
	for _, val := range vals {
		ctx.Buffer.WriteString(fmt.Sprintf("%v", val))
	}
	// TODO: if total length is greater than global var group_concat_max_len, truncate it.
	return nil
}

// StreamUpdate implements AggregationFunction interface.
func (cf *concatFunction) StreamUpdate(row []types.Datum, ectx context.Context) error {
	ctx := cf.getStreamedContext()
	vals := make([]interface{}, 0, len(cf.Args))
	for _, a := range cf.Args {
		value, err := a.Eval(row)
		if err != nil {
			return errors.Trace(err)
		}
		if value.GetValue() == nil {
			return nil
		}
		vals = append(vals, value)
	}
	if cf.Distinct {
		d, err := ctx.DistinctChecker.Check(vals)
		if err != nil {
			return errors.Trace(err)
		}
		if !d {
			return nil
		}
	}
	if ctx.Buffer == nil {
		ctx.Buffer = &bytes.Buffer{}
	} else {
		// now use comma separator
		ctx.Buffer.WriteString(",")
	}
	for _, val := range vals {
		ctx.Buffer.WriteString(fmt.Sprintf("%v", val))
	}
	// TODO: if total length is greater than global var group_concat_max_len, truncate it.
	return nil
}

// GetGroupResult implements AggregationFunction interface.
func (cf *concatFunction) GetGroupResult(groupKey []byte) (d types.Datum) {
	ctx := cf.getContext(groupKey)
	if ctx.Buffer != nil {
		d.SetString(ctx.Buffer.String())
	} else {
		d.SetNull()
	}
	return d
}

// GetStreamResult implements AggregationFunction interface.
func (cf *concatFunction) GetStreamResult() (d types.Datum) {
	if cf.streamCtx == nil {
		return
	}
	if cf.streamCtx.Buffer != nil {
		d.SetString(cf.streamCtx.Buffer.String())
	} else {
		d.SetNull()
	}
	cf.streamCtx = nil
	return
}

type maxMinFunction struct {
	aggFunction
	isMax bool
}

// Clone implements AggregationFunction interface.
func (mmf *maxMinFunction) Clone() AggregationFunction {
	nf := *mmf
	for i, arg := range mmf.Args {
		nf.Args[i] = arg.Clone()
	}
	nf.resultMapper = make(aggCtxMapper)
	return &nf
}

// CalculateDefaultValue implements AggregationFunction interface.
func (mmf *maxMinFunction) CalculateDefaultValue(schema *Schema, ctx context.Context) (d types.Datum, valid bool) {
	arg := mmf.Args[0]
	result, err := EvaluateExprWithNull(ctx, schema, arg)
	if err != nil {
		log.Warnf("Evaluate expr with null failed in function %s, err msg is %s", mmf, err.Error())
		return d, false
	}
	if con, ok := result.(*Constant); ok {
		return con.Value, true
	}
	return d, false
}

// GetType implements AggregationFunction interface.
func (mmf *maxMinFunction) GetType() *types.FieldType {
	return mmf.Args[0].GetType()
}

// GetGroupResult implements AggregationFunction interface.
func (mmf *maxMinFunction) GetGroupResult(groupKey []byte) (d types.Datum) {
	return mmf.getContext(groupKey).Value
}

// GetStreamResult implements AggregationFunction interface.
func (mmf *maxMinFunction) GetStreamResult() (d types.Datum) {
	if mmf.streamCtx == nil {
		return
	}
	d = mmf.streamCtx.Value
	mmf.streamCtx = nil
	return
}

// Update implements AggregationFunction interface.
func (mmf *maxMinFunction) Update(row []types.Datum, groupKey []byte, ectx context.Context) error {
	ctx := mmf.getContext(groupKey)
	if len(mmf.Args) != 1 {
		return errors.New("Wrong number of args for AggFuncMaxMin")
	}
	a := mmf.Args[0]
	value, err := a.Eval(row)
	if err != nil {
		return errors.Trace(err)
	}
	if ctx.Value.IsNull() {
		ctx.Value = value
	}
	if value.IsNull() {
		return nil
	}
	var c int
	c, err = ctx.Value.CompareDatum(ectx.GetSessionVars().StmtCtx, value)
	if err != nil {
		return errors.Trace(err)
	}
	if (mmf.isMax && c == -1) || (!mmf.isMax && c == 1) {
		ctx.Value = value
	}
	return nil
}

// StreamUpdate implements AggregationFunction interface.
func (mmf *maxMinFunction) StreamUpdate(row []types.Datum, ectx context.Context) error {
	ctx := mmf.getStreamedContext()
	if len(mmf.Args) != 1 {
		return errors.New("Wrong number of args for AggFuncMaxMin")
	}
	a := mmf.Args[0]
	value, err := a.Eval(row)
	if err != nil {
		return errors.Trace(err)
	}
	if ctx.Value.IsNull() {
		ctx.Value = value
	}
	if value.IsNull() {
		return nil
	}
	var c int
	c, err = ctx.Value.CompareDatum(ectx.GetSessionVars().StmtCtx, value)
	if err != nil {
		return errors.Trace(err)
	}
	if (mmf.isMax && c == -1) || (!mmf.isMax && c == 1) {
		ctx.Value = value
	}
	return nil
}

type firstRowFunction struct {
	aggFunction
}

// Clone implements AggregationFunction interface.
func (ff *firstRowFunction) Clone() AggregationFunction {
	nf := *ff
	for i, arg := range ff.Args {
		nf.Args[i] = arg.Clone()
	}
	nf.resultMapper = make(aggCtxMapper)
	return &nf
}

// GetType implements AggregationFunction interface.
func (ff *firstRowFunction) GetType() *types.FieldType {
	return ff.Args[0].GetType()
}

// Update implements AggregationFunction interface.
func (ff *firstRowFunction) Update(row []types.Datum, groupKey []byte, ectx context.Context) error {
	ctx := ff.getContext(groupKey)
	if ctx.GotFirstRow {
		return nil
	}
	if len(ff.Args) != 1 {
		return errors.New("Wrong number of args for AggFuncFirstRow")
	}
	value, err := ff.Args[0].Eval(row)
	if err != nil {
		return errors.Trace(err)
	}
	ctx.Value = value
	ctx.GotFirstRow = true
	return nil
}

// StreamUpdate implements AggregationFunction interface.
func (ff *firstRowFunction) StreamUpdate(row []types.Datum, ectx context.Context) error {
	ctx := ff.getStreamedContext()
	if ctx.GotFirstRow {
		return nil
	}
	if len(ff.Args) != 1 {
		return errors.New("Wrong number of args for AggFuncFirstRow")
	}
	value, err := ff.Args[0].Eval(row)
	if err != nil {
		return errors.Trace(err)
	}
	ctx.Value = value
	ctx.GotFirstRow = true
	return nil
}

// GetGroupResult implements AggregationFunction interface.
func (ff *firstRowFunction) GetGroupResult(groupKey []byte) types.Datum {
	return ff.getContext(groupKey).Value
}

// GetStreamResult implements AggregationFunction interface.
func (ff *firstRowFunction) GetStreamResult() (d types.Datum) {
	if ff.streamCtx == nil {
		return
	}
	d = ff.streamCtx.Value
	ff.streamCtx = nil
	return
}

// CalculateDefaultValue implements AggregationFunction interface.
func (ff *firstRowFunction) CalculateDefaultValue(schema *Schema, ctx context.Context) (d types.Datum, valid bool) {
	arg := ff.Args[0]
	result, err := EvaluateExprWithNull(ctx, schema, arg)
	if err != nil {
		log.Warnf("Evaluate expr with null failed in function %s, err msg is %s", ff, err.Error())
		return d, false
	}
	if con, ok := result.(*Constant); ok {
		return con.Value, true
	}
	return d, false
}
