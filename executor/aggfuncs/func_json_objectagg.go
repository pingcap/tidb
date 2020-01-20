package aggfuncs

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
)

type baseJSONObjectAgg struct {
	baseAggFunc
}

type partialResult4JsonObjectAgg struct {
	entries map[string]interface{}
}

func (e *baseJSONObjectAgg) AllocPartialResult() PartialResult {
	p := partialResult4JsonObjectAgg{}
	p.entries = make(map[string]interface{})
	return PartialResult(&p)
}

func (e *baseJSONObjectAgg) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4JsonObjectAgg)(pr)
	p.entries = make(map[string]interface{})
}

func (e *baseJSONObjectAgg) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4JsonObjectAgg)(pr)
	if len(p.entries) == 0 {
		chk.AppendNull(e.ordinal)
		return nil
	}

	// appendBinary does not support some type such as uint8、types.time，so convert is needed here
	for key, val := range p.entries {
		switch x := val.(type) {
		case *types.MyDecimal:
			float64Val, err := x.ToFloat64()
			if err != nil {
				return errors.Trace(err)
			}
			p.entries[key] = float64Val
		case []uint8, types.Time, types.Duration:
			strVal, err := types.ToString(x)
			if err != nil {
				return errors.Trace(err)
			}
			p.entries[key] = strVal
		default:
			p.entries[key] = val
		}
	}

	chk.AppendJSON(e.ordinal, json.CreateBinary(p.entries))
	return nil
}

func (e *baseJSONObjectAgg) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4JsonObjectAgg)(pr)
	for _, row := range rowsInGroup {
		key, err := e.args[0].Eval(row)
		if err != nil {
			return errors.Trace(err)
		}

		value, err := e.args[1].Eval(row)
		if err != nil {
			return errors.Trace(err)
		}

		if key.IsNull() {
			return json.ErrJSONDocumentNULLKey
		}

		// the result json's key is string, so it needs to convert the first arg to string
		keyString, err := key.ToString()
		if err != nil {
			return errors.Trace(err)
		}

		realVal := value.GetValue()
		switch x := realVal.(type) {
		case nil, bool, int64, uint64, float64, string, json.BinaryJSON, types.Time, types.Duration:
			p.entries[keyString] = realVal
		default:
			return json.ErrUnsupportedSecondArgumentType.GenWithStack("The second argument type %T is not supported now", x)
		}
	}
	return nil
}

type partial4JsonObjectAgg struct {
	baseJSONObjectAgg
}

func (e *partial4JsonObjectAgg) MergePartialResult(sctx sessionctx.Context, src PartialResult, dst PartialResult) error {
	p1, p2 := (*partialResult4JsonObjectAgg)(src), (*partialResult4JsonObjectAgg)(dst)
	// get the last value for the same key, eg: [id = 1, name = "a"],[id = 1, name = "b"]
	// json_objectagg(id, name) will get only {"1": "b"} instead of {"1": "a", "1": "b"}
	for k, v := range p1.entries {
		p2.entries[k] = v
	}
	return nil
}
