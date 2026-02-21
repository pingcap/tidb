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
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression/expropt"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/model"
	"github.com/pingcap/tidb/pkg/util/modelruntime"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
)

var (
	_ functionClass = &modelPredictFunctionClass{}
	_ functionClass = &modelPredictOutputFunctionClass{}

	_ builtinFunc = &builtinModelPredictSig{}
	_ builtinFunc = &builtinModelPredictOutputSig{}
)

var (
	modelPredictOutputHook func(modelName, outputName string, inputs []float32) (float64, error)
	modelPredictBatchHook  func(modelName, outputName string, inputs [][]float32) ([]float64, error)
)

type modelPredictFunctionClass struct {
	baseFunctionClass
}

func (c *modelPredictFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, 0, len(args))
	for i := range args {
		if i == 0 {
			argTps = append(argTps, types.ETString)
		} else {
			argTps = append(argTps, types.ETReal)
		}
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETJson, argTps...)
	if err != nil {
		return nil, err
	}
	ctx.SetSkipPlanCache("model_predict is not cacheable")
	return &builtinModelPredictSig{baseBuiltinFunc: bf}, nil
}

type modelPredictOutputFunctionClass struct {
	baseFunctionClass
}

func (c *modelPredictOutputFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, 0, len(args))
	if len(args) >= 1 {
		argTps = append(argTps, types.ETString)
	}
	if len(args) >= 2 {
		argTps = append(argTps, types.ETString)
	}
	for i := 2; i < len(args); i++ {
		argTps = append(argTps, types.ETReal)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETReal, argTps...)
	if err != nil {
		return nil, err
	}
	ctx.SetSkipPlanCache("model_predict is not cacheable")
	return &builtinModelPredictOutputSig{baseBuiltinFunc: bf}, nil
}

type builtinModelPredictSig struct {
	baseBuiltinFunc
	expropt.SessionVarsPropReader
	expropt.SQLExecutorPropReader
	expropt.PrivilegeCheckerPropReader

	initOnce sync.Once
	initErr  error
	meta     modelPredictMeta
}

// RequiredOptionalEvalProps implements the RequireOptionalEvalProps interface.
func (b *builtinModelPredictSig) RequiredOptionalEvalProps() OptionalEvalPropKeySet {
	return b.SessionVarsPropReader.RequiredOptionalEvalProps() |
		b.SQLExecutorPropReader.RequiredOptionalEvalProps() |
		b.PrivilegeCheckerPropReader.RequiredOptionalEvalProps()
}

func (b *builtinModelPredictSig) Clone() builtinFunc {
	newSig := &builtinModelPredictSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinModelPredictSig) evalJSON(ctx EvalContext, row chunk.Row) (types.BinaryJSON, bool, error) {
	if err := checkModelPredictEnabled(ctx, b.PrivilegeCheckerPropReader); err != nil {
		return types.BinaryJSON{}, true, err
	}
	if err := b.init(ctx, 1); err != nil {
		return types.BinaryJSON{}, true, err
	}
	inputs, err := evalInputFloats(ctx, row, b.getArgs()[1:])
	if err != nil {
		return types.BinaryJSON{}, true, err
	}
	outputs, err := b.meta.predict(inputs)
	if err != nil {
		return types.BinaryJSON{}, true, err
	}
	result := make(map[string]any, len(outputs))
	for i, name := range b.meta.outputNames {
		result[name] = float64(outputs[i])
	}
	return types.CreateBinaryJSON(result), false, nil
}

func (b *builtinModelPredictSig) init(ctx EvalContext, inputOffset int) error {
	b.initOnce.Do(func() {
		modelName, err := evalConstString(ctx, b.getArgs()[0], "model name")
		if err != nil {
			b.initErr = err
			return
		}
		vars, err := b.GetSessionVars(ctx)
		if err != nil {
			b.initErr = err
			return
		}
		exec, err := b.GetSQLExecutor(ctx)
		if err != nil {
			b.initErr = err
			return
		}
		meta, err := loadModelPredictMeta(ctx, vars, exec, modelName, len(b.getArgs())-inputOffset)
		if err != nil {
			b.initErr = err
			return
		}
		b.meta = *meta
	})
	return b.initErr
}

type builtinModelPredictOutputSig struct {
	baseBuiltinFunc
	expropt.SessionVarsPropReader
	expropt.SQLExecutorPropReader
	expropt.PrivilegeCheckerPropReader

	initOnce sync.Once
	initErr  error
	meta     modelPredictMeta
	outIdx   int
	outName  string
}

// RequiredOptionalEvalProps implements the RequireOptionalEvalProps interface.
func (b *builtinModelPredictOutputSig) RequiredOptionalEvalProps() OptionalEvalPropKeySet {
	return b.SessionVarsPropReader.RequiredOptionalEvalProps() |
		b.SQLExecutorPropReader.RequiredOptionalEvalProps() |
		b.PrivilegeCheckerPropReader.RequiredOptionalEvalProps()
}

func (b *builtinModelPredictOutputSig) Clone() builtinFunc {
	newSig := &builtinModelPredictOutputSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinModelPredictOutputSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	if err := checkModelPredictEnabled(ctx, b.PrivilegeCheckerPropReader); err != nil {
		return 0, true, err
	}
	if err := b.init(ctx, 2); err != nil {
		return 0, true, err
	}
	inputs, err := evalInputFloats(ctx, row, b.getArgs()[2:])
	if err != nil {
		return 0, true, err
	}
	if hook := modelPredictOutputHook; hook != nil {
		out, err := hook(b.meta.modelName, b.outName, inputs)
		if err != nil {
			return 0, true, err
		}
		return out, false, nil
	}
	outputs, err := b.meta.predict(inputs)
	if err != nil {
		return 0, true, err
	}
	if b.outIdx < 0 || b.outIdx >= len(outputs) {
		return 0, true, errors.New("model output index out of range")
	}
	return float64(outputs[b.outIdx]), false, nil
}

func (b *builtinModelPredictOutputSig) vectorized() bool {
	return true
}

func (b *builtinModelPredictOutputSig) vecEvalReal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	if err := checkModelPredictEnabled(ctx, b.PrivilegeCheckerPropReader); err != nil {
		return err
	}
	if err := b.init(ctx, 2); err != nil {
		return err
	}
	n := input.NumRows()
	result.ResizeFloat64(n, false)
	if n == 0 {
		return nil
	}
	argCount := len(b.getArgs()) - 2
	argCols := make([]*chunk.Column, argCount)
	for i := 0; i < argCount; i++ {
		col, err := b.bufAllocator.get()
		if err != nil {
			return err
		}
		defer b.bufAllocator.put(col)
		if err := b.getArgs()[i+2].VecEvalReal(ctx, input, col); err != nil {
			return err
		}
		argCols[i] = col
	}
	inputs := make([][]float32, n)
	for rowIdx := 0; rowIdx < n; rowIdx++ {
		rowInputs := make([]float32, argCount)
		for i, col := range argCols {
			if col.IsNull(rowIdx) {
				return errors.New("model input cannot be null")
			}
			rowInputs[i] = float32(col.GetFloat64(rowIdx))
		}
		inputs[rowIdx] = rowInputs
	}

	var outputs []float64
	if hook := modelPredictBatchHook; hook != nil {
		out, err := hook(b.meta.modelName, b.outName, inputs)
		if err != nil {
			return err
		}
		outputs = out
	} else if b.meta.batchable {
		outBatch, err := b.meta.predictBatch(inputs)
		if err != nil {
			return err
		}
		outputs = make([]float64, n)
		for i, rowOut := range outBatch {
			if b.outIdx < 0 || b.outIdx >= len(rowOut) {
				return errors.New("model output index out of range")
			}
			outputs[i] = float64(rowOut[b.outIdx])
		}
	} else {
		outputs = make([]float64, n)
		for i, rowInputs := range inputs {
			if hook := modelPredictOutputHook; hook != nil {
				out, err := hook(b.meta.modelName, b.outName, rowInputs)
				if err != nil {
					return err
				}
				outputs[i] = out
				continue
			}
			rowOut, err := b.meta.predict(rowInputs)
			if err != nil {
				return err
			}
			if b.outIdx < 0 || b.outIdx >= len(rowOut) {
				return errors.New("model output index out of range")
			}
			outputs[i] = float64(rowOut[b.outIdx])
		}
	}
	if len(outputs) != n {
		return errors.New("model output batch size mismatch")
	}
	res := result.Float64s()
	for i := 0; i < n; i++ {
		res[i] = outputs[i]
	}
	return nil
}

func (b *builtinModelPredictOutputSig) init(ctx EvalContext, inputOffset int) error {
	b.initOnce.Do(func() {
		modelName, err := evalConstString(ctx, b.getArgs()[0], "model name")
		if err != nil {
			b.initErr = err
			return
		}
		outputName, err := evalConstString(ctx, b.getArgs()[1], "model output name")
		if err != nil {
			b.initErr = err
			return
		}
		vars, err := b.GetSessionVars(ctx)
		if err != nil {
			b.initErr = err
			return
		}
		exec, err := b.GetSQLExecutor(ctx)
		if err != nil {
			b.initErr = err
			return
		}
		meta, err := loadModelPredictMeta(ctx, vars, exec, modelName, len(b.getArgs())-inputOffset)
		if err != nil {
			b.initErr = err
			return
		}
		idx, ok := meta.outputIndex[strings.ToLower(outputName)]
		if !ok {
			b.initErr = errors.Errorf("model output %s not found", outputName)
			return
		}
		b.meta = *meta
		b.outIdx = idx
		b.outName = outputName
	})
	return b.initErr
}

type modelPredictMeta struct {
	modelID      int64
	version      int64
	modelName    string
	inputNames   []string
	outputNames  []string
	outputIndex  map[string]int
	artifactData []byte
	batchable    bool
	sessionKey   modelruntime.SessionKey
	sessionCache *modelruntime.SessionCache
	inferOpts    modelruntime.InferenceOptions
	allowCustom  bool
}

type modelSchemaColumn struct {
	name ast.CIStr
	tp   *types.FieldType
}

func (m *modelPredictMeta) predict(inputs []float32) ([]float32, error) {
	outputs, err := modelruntime.RunInferenceWithOptions(m.sessionCache, m.sessionKey, m.artifactData, m.inputNames, m.outputNames, inputs, m.inferOpts)
	if err != nil {
		return nil, m.wrapInferenceError(err)
	}
	return outputs, nil
}

func (m *modelPredictMeta) predictBatch(inputs [][]float32) ([][]float32, error) {
	if !m.batchable {
		results := make([][]float32, len(inputs))
		for i, row := range inputs {
			out, err := m.predict(row)
			if err != nil {
				return nil, err
			}
			results[i] = out
		}
		return results, nil
	}
	outputs, err := modelruntime.RunInferenceBatchWithOptions(m.sessionCache, m.sessionKey, m.artifactData, m.inputNames, m.outputNames, inputs, m.inferOpts)
	if err != nil {
		return nil, m.wrapInferenceError(err)
	}
	return outputs, nil
}

func (m *modelPredictMeta) wrapInferenceError(err error) error {
	if err == nil {
		return nil
	}
	if !m.allowCustom && modelruntime.IsCustomOpError(err) {
		return errors.Errorf("model %s uses custom ops; set %s=ON", m.modelName, vardef.TiDBEnableModelCustomOps)
	}
	return err
}

func loadModelPredictMeta(ctx EvalContext, vars *variable.SessionVars, exec expropt.SQLExecutor, modelName string, inputArgCount int) (*modelPredictMeta, error) {
	schemaName, modelIdent, err := splitModelName(modelName, ctx.CurrentDB())
	if err != nil {
		return nil, err
	}
	snapshot := snapshotForModelMeta(vars)
	execOpts := make([]sqlexec.OptionFuncAlias, 0, 1)
	if snapshot != 0 {
		execOpts = append(execOpts, sqlexec.ExecOptionWithSnapshot(snapshot))
	}
	innerCtx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnMeta)
	rows, _, err := exec.ExecRestrictedSQL(innerCtx, execOpts,
		"SELECT id FROM mysql.tidb_model WHERE db_name = %? AND model_name = %? AND deleted_at IS NULL",
		schemaName, modelIdent,
	)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(rows) == 0 {
		return nil, errModelNotExists.GenWithStackByArgs(schemaName, modelIdent)
	}
	modelID := rows[0].GetInt64(0)
	rows, _, err = exec.ExecRestrictedSQL(innerCtx, execOpts,
		"SELECT version, engine, location, checksum, input_schema, output_schema FROM mysql.tidb_model_version WHERE model_id = %? AND deleted_at IS NULL ORDER BY version DESC LIMIT 1",
		modelID,
	)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(rows) == 0 {
		return nil, errModelNotExists.GenWithStackByArgs(schemaName, modelIdent)
	}
	version := rows[0].GetInt64(0)
	engine := rows[0].GetString(1)
	location := rows[0].GetString(2)
	checksum := rows[0].GetString(3)
	inputSchema, err := schemaFromRow(rows[0].GetJSON(4))
	if err != nil {
		return nil, err
	}
	outputSchema, err := schemaFromRow(rows[0].GetJSON(5))
	if err != nil {
		return nil, err
	}
	if !strings.EqualFold(engine, "ONNX") {
		return nil, errors.Errorf("unsupported model engine: %s", engine)
	}
	inputCols, err := parseModelSchema(inputSchema)
	if err != nil {
		return nil, err
	}
	outputCols, err := parseModelSchema(outputSchema)
	if err != nil {
		return nil, err
	}
	if inputArgCount != len(inputCols) {
		return nil, errors.Errorf("model expects %d inputs but got %d", len(inputCols), inputArgCount)
	}
	if err := validateFloatColumns(inputCols); err != nil {
		return nil, err
	}
	if err := validateFloatColumns(outputCols); err != nil {
		return nil, err
	}
	loader := model.NewArtifactLoader(model.LoaderOptions{})
	artifact, err := loader.Load(context.Background(), model.ArtifactMeta{
		ModelID:  modelID,
		Version:  version,
		Engine:   engine,
		Location: location,
		Checksum: checksum,
	})
	if err != nil {
		return nil, err
	}
	onnxInputs, onnxOutputs, err := modelruntime.InspectModelIOInfo(artifact.Bytes)
	if err != nil {
		return nil, err
	}
	if len(onnxInputs) != len(inputCols) {
		return nil, errors.New("onnx input count does not match model schema")
	}
	if len(onnxOutputs) != len(outputCols) {
		return nil, errors.New("onnx output count does not match model schema")
	}
	inputNames := make([]string, len(onnxInputs))
	for i, info := range onnxInputs {
		if !strings.EqualFold(info.Name, inputCols[i].name.O) {
			return nil, errors.Errorf("onnx input %s does not match model schema %s", info.Name, inputCols[i].name.O)
		}
		inputNames[i] = info.Name
	}
	outputNames := make([]string, len(onnxOutputs))
	outputIndex := make(map[string]int, len(onnxOutputs))
	for i, info := range onnxOutputs {
		if !strings.EqualFold(info.Name, outputCols[i].name.O) {
			return nil, errors.Errorf("onnx output %s does not match model schema %s", info.Name, outputCols[i].name.O)
		}
		outputNames[i] = info.Name
		outputIndex[strings.ToLower(info.Name)] = i
	}
	batchable, err := resolveBatchableShape(onnxInputs, onnxOutputs)
	if err != nil {
		return nil, err
	}
	if !vardef.ModelAllowNondeterministic.Load() {
		nondet, err := modelruntime.ModelDeclaresNondeterministic(artifact.Bytes)
		if err != nil {
			return nil, err
		}
		if nondet {
			return nil, errors.Errorf("model %s is nondeterministic; set %s=ON to allow", modelIdent, vardef.TiDBModelAllowNondeterministic)
		}
	}
	sessionKey := modelruntime.SessionKeyFromParts(modelID, version, inputNames, outputNames)
	return &modelPredictMeta{
		modelID:      modelID,
		version:      version,
		modelName:    modelIdent,
		inputNames:   inputNames,
		outputNames:  outputNames,
		outputIndex:  outputIndex,
		artifactData: artifact.Bytes,
		batchable:    batchable,
		sessionKey:   sessionKey,
		sessionCache: modelruntime.GetProcessSessionCache(),
		inferOpts: modelruntime.InferenceOptions{
			MaxBatchSize: int(vardef.ModelMaxBatchSize.Load()),
			Timeout:      vardef.ModelTimeout.Load(),
		},
		allowCustom: vardef.EnableModelCustomOps.Load(),
	}, nil
}

func schemaFromRow(bj types.BinaryJSON) (string, error) {
	raw, err := bj.MarshalJSON()
	if err != nil {
		return "", errors.Trace(err)
	}
	return string(raw), nil
}

func parseModelSchema(schemaJSON string) ([]modelSchemaColumn, error) {
	var cols []string
	if err := json.Unmarshal([]byte(schemaJSON), &cols); err != nil {
		return nil, errors.Trace(err)
	}
	if len(cols) == 0 {
		return nil, errors.New("model schema is empty")
	}
	p := parser.New()
	out := make([]modelSchemaColumn, 0, len(cols))
	for _, col := range cols {
		stmt, err := p.ParseOneStmt(fmt.Sprintf("CREATE TABLE t (%s)", col), "", "")
		if err != nil {
			return nil, errors.Trace(err)
		}
		createStmt, ok := stmt.(*ast.CreateTableStmt)
		if !ok || len(createStmt.Cols) != 1 {
			return nil, errors.New("invalid model schema column definition")
		}
		colDef := createStmt.Cols[0]
		out = append(out, modelSchemaColumn{name: colDef.Name.Name, tp: colDef.Tp})
	}
	return out, nil
}

func validateFloatColumns(cols []modelSchemaColumn) error {
	for _, col := range cols {
		switch col.tp.GetType() {
		case mysql.TypeFloat, mysql.TypeDouble:
			continue
		default:
			return errors.Errorf("model column %s must be FLOAT or DOUBLE", col.name.O)
		}
	}
	return nil
}

func resolveBatchableShape(inputs, outputs []modelruntime.TensorInfo) (bool, error) {
	inputsScalar, inputsBatch := classifyTensorShapes(inputs)
	outputsScalar, outputsBatch := classifyTensorShapes(outputs)
	if inputsScalar && outputsScalar {
		return false, nil
	}
	if inputsBatch && outputsBatch {
		return true, nil
	}
	return false, errors.New("onnx input/output shapes must all be scalar or batchable")
}

func classifyTensorShapes(items []modelruntime.TensorInfo) (allScalar bool, allBatch bool) {
	allScalar = true
	allBatch = true
	for _, item := range items {
		if !isScalarShape(item.Shape) {
			allScalar = false
		}
		if !isBatchShape(item.Shape) {
			allBatch = false
		}
	}
	return allScalar, allBatch
}

func isScalarShape(shape []int64) bool {
	return len(shape) == 1 && (shape[0] == 1 || shape[0] == -1)
}

func isBatchShape(shape []int64) bool {
	return len(shape) == 2 && shape[1] == 1 && (shape[0] == 1 || shape[0] == -1)
}

func splitModelName(name, currentDB string) (schema string, model string, err error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", "", errors.New("model name is required")
	}
	parts := strings.Split(name, ".")
	switch len(parts) {
	case 1:
		if currentDB == "" {
			return "", "", errDatabaseNotExists.GenWithStackByArgs(currentDB)
		}
		return currentDB, parts[0], nil
	case 2:
		if parts[0] == "" || parts[1] == "" {
			return "", "", errors.New("invalid model name")
		}
		return parts[0], parts[1], nil
	default:
		return "", "", errors.New("invalid model name")
	}
}

func snapshotForModelMeta(vars *variable.SessionVars) uint64 {
	if vars == nil {
		return 0
	}
	if vars.SnapshotTS != 0 {
		return vars.SnapshotTS
	}
	if vars.TxnCtx != nil && vars.TxnCtx.StartTS != 0 {
		return vars.TxnCtx.StartTS
	}
	return 0
}

func checkModelPredictEnabled(ctx EvalContext, privReader expropt.PrivilegeCheckerPropReader) error {
	if !vardef.EnableModelInference.Load() {
		return errModelInferenceDisabled
	}
	checker, err := privReader.GetPrivilegeChecker(ctx)
	if err != nil {
		return err
	}
	if !checker.RequestDynamicVerification("MODEL_EXECUTE", false) {
		return errSpecificAccessDenied.GenWithStackByArgs("MODEL_EXECUTE")
	}
	return nil
}

func evalConstString(ctx EvalContext, expr Expression, label string) (string, error) {
	con, ok := expr.(*Constant)
	if !ok {
		return "", errors.Errorf("%s must be constant", label)
	}
	val, isNull, err := con.EvalString(ctx, chunk.Row{})
	if err != nil {
		return "", err
	}
	if isNull {
		return "", errors.Errorf("%s cannot be null", label)
	}
	return val, nil
}

func evalInputFloats(ctx EvalContext, row chunk.Row, args []Expression) ([]float32, error) {
	inputs := make([]float32, 0, len(args))
	for _, arg := range args {
		val, isNull, err := arg.EvalReal(ctx, row)
		if err != nil {
			return nil, err
		}
		if isNull {
			return nil, errors.New("model input cannot be null")
		}
		inputs = append(inputs, float32(val))
	}
	return inputs, nil
}
