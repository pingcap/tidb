// Copyright 2023 PingCAP, Inc.
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

package debugtrace

import (
	"bytes"
	"encoding/json"
	"runtime"

	"github.com/pingcap/tidb/pkg/planner/context"
)

// OptimizerDebugTraceRoot is for recording the optimizer debug trace.
// Each debug information, which is a "step" and can be any type, is placed
// in a "context" (baseDebugTraceContext) as an interface{}.
// Overall, the debug trace is a tree-like hierarchical structure of baseDebugTraceContext.
// This structure can reflect the function call hierarchy of each step during optimization.
// In the end, the entire recorded baseDebugTraceContext will be marshalled to JSON as the result.
//
// EnterContextCommon and LeaveContextCommon can be used to maintain the context easily.
// Usually you just need to add the code below at the beginning of a function:
//
//	if StmtCtx.EnableOptimizerDebugTrace {
//	 EnterContextCommon(ds.ctx)
//	 defer LeaveContextCommon(ds.ctx)
//	}
//
// To record debug information, AppendStepToCurrentContext and AppendStepWithNameToCurrentContext
// are provided as low-level methods.
// RecordAnyValuesWithNames handles some common logic for better usability, so it should
// be the most commonly used function for recording simple information.
// If the tracing logic is more complicated or need extra MarshalJSON logic, you should implement
// separate logic like in planner/core/debug_trace.go and statistics/debug_trace.go
type OptimizerDebugTraceRoot struct {
	traceCtx baseDebugTraceContext
	// currentCtx indicates in which baseDebugTraceContext we should record the debug information.
	currentCtx *baseDebugTraceContext
}

// MarshalJSON overrides the default MarshalJSON behavior and marshals the unexported traceCtx.
func (root *OptimizerDebugTraceRoot) MarshalJSON() ([]byte, error) {
	return EncodeJSONCommon(root.traceCtx.steps)
}

// baseDebugTraceContext is the core of the debug trace.
// The steps field can be used to record any information, or point to another baseDebugTraceContext.
type baseDebugTraceContext struct {
	name      string
	steps     []any
	parentCtx *baseDebugTraceContext
}

func (c *baseDebugTraceContext) MarshalJSON() ([]byte, error) {
	var tmp, content any
	if len(c.steps) > 1 {
		content = c.steps
	} else if len(c.steps) == 1 {
		content = c.steps[0]
	}
	if len(c.name) > 0 {
		tmp = map[string]any{
			c.name: content,
		}
	} else {
		tmp = content
	}
	return EncodeJSONCommon(tmp)
}

// AppendStepToCurrentContext records debug information to the current context of the debug trace.
func (root *OptimizerDebugTraceRoot) AppendStepToCurrentContext(step any) {
	root.currentCtx.steps = append(root.currentCtx.steps, step)
}

// AppendStepWithNameToCurrentContext records debug information and a name to the current context of the debug trace.
func (root *OptimizerDebugTraceRoot) AppendStepWithNameToCurrentContext(step any, name string) {
	tmp := map[string]any{
		name: step,
	}
	root.currentCtx.steps = append(root.currentCtx.steps, tmp)
}

// GetOrInitDebugTraceRoot returns the debug trace root.
// If it's not initialized, it will initialize it first.
func GetOrInitDebugTraceRoot(sctx context.PlanContext) *OptimizerDebugTraceRoot {
	stmtCtx := sctx.GetSessionVars().StmtCtx
	res, ok := stmtCtx.OptimizerDebugTrace.(*OptimizerDebugTraceRoot)
	if !ok || res == nil {
		trace := &OptimizerDebugTraceRoot{}
		trace.currentCtx = &trace.traceCtx
		// Though it's not needed in theory, we set the parent of the top level context to itself for safety.
		trace.traceCtx.parentCtx = &trace.traceCtx
		stmtCtx.OptimizerDebugTrace = trace
	}
	return stmtCtx.OptimizerDebugTrace.(*OptimizerDebugTraceRoot)
}

// EncodeJSONCommon contains some common logic for the debug trace,
// like disabling EscapeHTML and recording error.
func EncodeJSONCommon(input any) ([]byte, error) {
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	// If we do not set this to false, ">", "<", "&"... will be escaped to "\u003c","\u003e", "\u0026"...
	encoder.SetEscapeHTML(false)
	err := encoder.Encode(input)
	if err != nil {
		err = encoder.Encode(err)
	}
	return buf.Bytes(), err
}

// EnterContextCommon records the function name of the caller,
// then creates and enter a new context for this debug trace structure.
func EnterContextCommon(sctx context.PlanContext) {
	root := GetOrInitDebugTraceRoot(sctx)
	funcName := "Fail to get function name."
	pc, _, _, ok := runtime.Caller(1)
	if ok {
		funcName = runtime.FuncForPC(pc).Name()
	}
	newCtx := &baseDebugTraceContext{
		name:      funcName,
		parentCtx: root.currentCtx,
	}
	root.currentCtx.steps = append(root.currentCtx.steps, newCtx)
	root.currentCtx = newCtx
}

// LeaveContextCommon makes the debug trace goes to its parent context.
func LeaveContextCommon(sctx context.PlanContext) {
	root := GetOrInitDebugTraceRoot(sctx)
	root.currentCtx = root.currentCtx.parentCtx
}

// RecordAnyValuesWithNames is a general debug trace logic for recording some values of any type with a name.
// The vals arguments should be a slice like ["name1", value1, "name2", value2].
// The names must be string, the values can be any type.
func RecordAnyValuesWithNames(
	s context.PlanContext,
	vals ...any,
) {
	root := GetOrInitDebugTraceRoot(s)
	tmp := make(map[string]any, len(vals)/2)
	for i := 0; i < len(vals); i += 2 {
		str, _ := vals[i].(string)
		val := vals[i+1]
		tmp[str] = val
	}
	root.AppendStepToCurrentContext(tmp)
}
