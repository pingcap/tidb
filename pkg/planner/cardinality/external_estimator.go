package cardinality

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/pingcap/tidb/pkg/infoschema"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
)

type estRequest struct {
	Table string     `json:"table_name"`
	Exprs []*cmpPred `json:"exprs"`
}

type cmpPred struct {
	Col   string `json:"col"`
	OP    string `json:"op"`
	Value string `json:"value"`
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

	tmp := strings.Split(strings.ToLower(col.OrigName), ".")
	colName := tmp[len(tmp)-1]

	return &cmpPred{
		Col:   colName,
		OP:    op,
		Value: value,
	}
}

// SelectivityFromExternalEstimator ...
func SelectivityFromExternalEstimator(sctx sessionctx.Context, tableID int64, exprs []expression.Expression) (result float64, ok bool, err error) {
	if sctx.GetSessionVars().ExternalCardinalityEstimator == "" {
		return 0, false, nil
	}

	is := sctx.GetDomainInfoSchema().(infoschema.InfoSchema)
	tbl, _ := infoschema.FindTableByTblOrPartID(is, tableID)
	if tbl == nil {
		return 0, false, nil
	}
	tableName := tbl.Meta().Name.L

	var cmpPreds []*cmpPred
	for _, e := range exprs {
		cmpPred := newCmpPred(e)
		if cmpPred == nil {
			return 0, false, nil
		}
		cmpPreds = append(cmpPreds, cmpPred)
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

	return sel, true, nil
}
