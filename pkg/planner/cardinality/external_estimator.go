package cardinality

import (
	"bytes"
	"encoding/json"
	planutil "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
)

type estRequest struct {
	Table string     `json:"table_name"`
	Exprs []*cmpPred `json:"exprs"`
}

type cmpPred struct {
	Col   string      `json:"col"`
	OP    string      `json:"op"`
	Value interface{} `json:"value"`
}

var (
	opMap = map[string]string{
		ast.EQ: "=",
		ast.LT: "<",
		ast.LE: "<=",
		ast.GT: ">",
		ast.GE: ">=",
	}
)

func newCmpPred(sctx sessionctx.Context, expr expression.Expression) *cmpPred {
	cmp, isCmp := expr.(*expression.ScalarFunction)
	if !isCmp {
		return nil
	}

	op, ok := opMap[cmp.FuncName.L]
	if !ok {
		return nil
	}

	var col *expression.Column
	var con *expression.Constant
	args := cmp.GetArgs()
	if a0, ok := args[0].(*expression.Column); ok {
		if a1, ok := args[1].(*expression.Constant); ok {
			col, con = a0, a1
		}
	}
	if a0, ok := args[0].(*expression.Constant); ok {
		if a1, ok := args[1].(*expression.Column); ok {
			col, con = a1, a0
		}
	}
	if col == nil || con == nil {
		return nil
	}

	var v interface{}
	switch con.GetType().EvalType() {
	case types.ETInt:
		n, isNull, err := con.EvalInt(sctx, chunk.Row{})
		if isNull || err != nil {
			return nil
		}
		v = n
	case types.ETReal:
		d, isNull, err := con.EvalReal(sctx, chunk.Row{})
		if isNull || err != nil {
			return nil
		}
		v = d
	case types.ETDecimal:
		d, isNull, err := con.EvalDecimal(sctx, chunk.Row{})
		if isNull || err != nil {
			return nil
		}
		f, err := d.ToFloat64()
		if err != nil {
			return nil
		}
		v = f
	case types.ETDatetime, types.ETTimestamp, types.ETDuration:
		return nil // not supported
	case types.ETString, types.ETJson:
		v = con.String()
	}

	tmp := strings.Split(strings.ToLower(col.OrigName), ".")
	colName := tmp[len(tmp)-1]

	return &cmpPred{
		Col:   colName,
		OP:    op,
		Value: v,
	}
}

// SelectivityFromExternalEstimator ...
func SelectivityFromExternalEstimator(sctx sessionctx.Context, coll *statistics.HistColl, exprs []expression.Expression, filledPaths []*planutil.AccessPath) (result float64, ok bool, err error) {
	if sctx.GetSessionVars().ExternalCardinalityEstimator == "" {
		return 0, false, nil
	}

	is := sctx.GetDomainInfoSchema().(infoschema.InfoSchema)
	tbl, _ := infoschema.FindTableByTblOrPartID(is, coll.PhysicalID)
	if tbl == nil {
		return 0, false, nil
	}
	tableName := tbl.Meta().Name.L

	var cmpPreds []*cmpPred
	remaining := make([]expression.Expression, 2)
	for _, e := range exprs {
		cmpPred := newCmpPred(sctx, e)
		if cmpPred == nil {
			remaining = append(remaining, e)
			continue
		}
		cmpPreds = append(cmpPreds, cmpPred)
	}

	if len(cmpPreds) == 0 {
		return 0, false, nil
	}

	content, err := json.Marshal(&estRequest{
		Table: tableName,
		Exprs: cmpPreds,
	})
	if err != nil {
		return 0, false, err
	}

	//fmt.Println("===================")
	//fmt.Println(string(content))
	//fmt.Println("===================")

	req, err := http.NewRequest(http.MethodPost, sctx.GetSessionVars().ExternalCardinalityEstimator, bytes.NewBuffer(content))
	if err != nil {
		return 0, false, err
	}
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return 0, false, err
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, false, err
	}
	sel, err := strconv.ParseFloat(string(body), 64)
	if err != nil {
		return 0, false, err
	}

	if len(remaining) > 0 {
		remainingSel, _, err := Selectivity(sctx, coll, remaining, filledPaths)
		if err != nil {
			return 0, false, err
		}
		sel *= remainingSel
	}

	return sel, true, nil
}
