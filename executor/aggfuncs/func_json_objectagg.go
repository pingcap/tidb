package aggfuncs

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
)

type baseJsonObjectAgg struct {
	baseAggFunc
}

type partialResult4JsonObjectAgg struct {
	entries map[string]interface{}
}

func (e *baseJsonObjectAgg) AllocPartialResult() PartialResult {
	p := partialResult4JsonObjectAgg{}
	p.entries = make(map[string]interface{})
	return PartialResult(&p)
}

func (e *baseJsonObjectAgg) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4JsonObjectAgg)(pr)
	p.entries = make(map[string]interface{})
}

func (e *baseJsonObjectAgg) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4JsonObjectAgg)(pr)
	if len(p.entries) == 0 {
		chk.AppendNull(e.ordinal)
		return nil
	}
	// json_objectagg's key is string, so it needs to convert the first arg to string
	for key, val := range p.entries {
		switch x := val.(type) {
		case *types.MyDecimal:
			float64Val, err := x.ToFloat64()
			if err != nil {
				return errors.Trace(err)
			}
			p.entries[key] = float64Val
		case []uint8:
			//p.entries[key] = types.NewDatum(x).GetString()
			//v, err := types.NewDatum(x).ToString()
			byteVal, err := types.ToString(x)
			if err != nil {
				return errors.Trace(err)
			}
			p.entries[key] = byteVal
		case types.Time:
			timeVal, err := types.ToString(x)
			if err != nil {
				return errors.Trace(err)
			}
			p.entries[key] = timeVal
		default:
			p.entries[key] = val
		}
	}
	chk.AppendJSON(e.ordinal, json.CreateBinary(p.entries))
	return nil
}

func (e *baseJsonObjectAgg) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4JsonObjectAgg)(pr)
	for _, row := range rowsInGroup {
		key, err := e.args[0].Eval(row)
		if err != nil {
			return errors.Trace(err)
		}

		var value types.Datum
		value, err = e.args[1].Eval(row)
		if err != nil {
			return errors.Trace(err)
		}
		if key.IsNull() {
			err = errors.New("JSON documents may not contain NULL member names")
			return errors.Trace(err)
		}
		// if not to use key.ToString() instead of key.GetString(), the result may be null, because the key can be any types
		keyString, err := key.ToString()
		if err != nil {
			return errors.Trace(err)
		}
		p.entries[keyString] = value.GetValue()
	}
	return nil
}

type original4JsonObjectAgg struct {
	baseJsonObjectAgg
}

type partial4JsonObjectAgg struct {
	baseJsonObjectAgg
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
