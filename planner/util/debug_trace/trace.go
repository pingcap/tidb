package debug_trace

import (
	"encoding/json"
	"github.com/pingcap/tidb/sessionctx"
	"strconv"
	"strings"
)

/*
 Below is debug trace for the received command from the client.
 Corresponding logic is in DebugTraceReceivedCommand(). Here only contains related type definitions
 because we want to place them instead of interface{} into OptimizerDebugTraceRoot.
*/

type ReceivedCmdInfo struct {
	Command         string
	ExecutedASTText string
	ExecuteStmtInfo *ExecuteInfo
}

type ExecuteInfo struct {
	PreparedSQL      string
	BinaryParamsInfo []BinaryParamInfo
	UseCursor        bool
}

type BinaryParamInfo struct {
	Type  string
	Value string
}

func (info *BinaryParamInfo) MarshalSON() ([]byte, error) {
	infoForMarshal := new(BinaryParamInfo)
	quote := `"`
	// We only need the escape functionality of %q, the quoting is not needed,
	// so we trim the \" prefix and suffix here.
	infoForMarshal.Type = strings.TrimSuffix(
		strings.TrimPrefix(
			strconv.Quote(info.Type),
			quote),
		quote)
	infoForMarshal.Value = strings.TrimSuffix(
		strings.TrimPrefix(
			strconv.Quote(info.Value),
			quote),
		quote)
	return json.Marshal(infoForMarshal)
}

/*
 Below is a general debug trace logic for recording function name and some values of any type.
 Usually we should implement specific logic for a trace point to record more meaningful information, but sometimes this
 can save us time to implement more traces.
*/

func DebugTraceAnyValuesWithNames(
	s sessionctx.Context,
	vals ...interface{},
) {
	root := GetOrInitDebugTraceRoot(s)
	tmp := make(map[string]interface{}, len(vals)/2)
	for i := 0; i < len(vals); i += 2 {
		str, _ := vals[i].(string)
		val := vals[i+1]
		tmp[str] = val
	}
	root.AppendStepToCurrentContext(tmp)
}
