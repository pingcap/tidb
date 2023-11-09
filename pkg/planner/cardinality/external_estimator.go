package cardinality

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	"io"
	"net/http"
	"strconv"
	"strings"
)

type estRequest struct {
	table string
	exprs []*cmpPred
}

type cmpPred struct {
	col   string
	op    string
	value string
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

func newCmpPred(expr expression.Expression) *cmpPred {
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

	value := con.String()
	if con.GetType().EvalType() == types.ETString {
		value = fmt.Sprintf(`"%v"`, value)
	}

	return &cmpPred{
		col:   strings.ToLower(col.OrigName),
		op:    op,
		value: value,
	}
}

// SelectivityFromExternalEstimator ...
func SelectivityFromExternalEstimator(sctx sessionctx.Context, exprs []expression.Expression) (result float64, ok bool, err error) {
	if sctx.GetSessionVars().ExternalCardinalityEstimator == "" {
		return 0, false, nil
	}

	var cmpPreds []*cmpPred
	for _, e := range exprs {
		cmpPred := newCmpPred(e)
		if cmpPred == nil {
			return 0, false, nil
		}
		cmpPreds = append(cmpPreds, cmpPred)
	}

	content, err := json.Marshal(&estRequest{
		table: "", // TODO
		exprs: cmpPreds,
	})
	if err != nil {
		return 0, false, err
	}

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
	count, err := strconv.ParseFloat(string(body), 64)
	if err != nil {
		return 0, false, err
	}

	return count, true, nil
}
