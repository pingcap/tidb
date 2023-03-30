package debug_trace

import (
	"bytes"
	"encoding/json"
	"runtime"

	"github.com/pingcap/tidb/sessionctx"
)

type OptimizerDebugTraceRoot struct {
	ReceivedCommand ReceivedCmdInfo
	Optimizer       BaseDebugTraceContext

	currentCtx *BaseDebugTraceContext
}

func GetOrInitDebugTraceRoot(sctx sessionctx.Context) *OptimizerDebugTraceRoot {
	stmtCtx := sctx.GetSessionVars().StmtCtx
	res, ok := stmtCtx.OptimizerDebugTrace.(*OptimizerDebugTraceRoot)
	if !ok || res == nil {
		trace := &OptimizerDebugTraceRoot{}
		trace.currentCtx = &trace.Optimizer
		trace.Optimizer.parentCtx = &trace.Optimizer
		stmtCtx.OptimizerDebugTrace = trace
	}
	return stmtCtx.OptimizerDebugTrace.(*OptimizerDebugTraceRoot)
}

func EncodeJSONCommon(input interface{}) ([]byte, error) {
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	encoder.SetEscapeHTML(false)
	err := encoder.Encode(input)
	if err != nil {
		err = encoder.Encode(err)
	}
	return buf.Bytes(), err
}

func EnterContextCommon(sctx sessionctx.Context) {
	root := GetOrInitDebugTraceRoot(sctx)
	funcName := "Fail to get function name."
	pc, _, _, ok := runtime.Caller(1)
	if ok {
		funcName = runtime.FuncForPC(pc).Name()
	}
	newCtx := &BaseDebugTraceContext{}
	newCtx.parentCtx = root.currentCtx
	root.currentCtx.AppendStep(newCtx)
	root.currentCtx = newCtx
	newCtx.name = funcName
}

func LeaveContextCommon(sctx sessionctx.Context) (ret *BaseDebugTraceContext) {
	root := GetOrInitDebugTraceRoot(sctx)
	ret = root.currentCtx
	root.currentCtx = root.currentCtx.parentCtx
	return
}

func (root *OptimizerDebugTraceRoot) AppendStepToCurrentContext(step interface{}) {
	root.currentCtx.AppendStep(step)
}

func (root *OptimizerDebugTraceRoot) AppendStepWithNameToCurrentContext(step interface{}, name string) {
	tmp := map[string]interface{}{
		name: step,
	}
	root.currentCtx.AppendStep(tmp)
}

type BaseDebugTraceContext struct {
	name      string
	steps     []interface{}
	parentCtx *BaseDebugTraceContext
}

func (c *BaseDebugTraceContext) AppendStep(step interface{}) {
	c.steps = append(c.steps, step)
}

func (c *BaseDebugTraceContext) MarshalJSON() ([]byte, error) {
	var tmp, content interface{}
	if len(c.steps) > 1 {
		content = c.steps
	} else if len(c.steps) == 1 {
		content = c.steps[0]
	}
	if len(c.name) > 0 {
		tmp = map[string]interface{}{
			c.name: content,
		}
	} else {
		tmp = content
	}
	return EncodeJSONCommon(tmp)
}
