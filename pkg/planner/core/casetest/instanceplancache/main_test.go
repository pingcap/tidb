// Copyright 2024 PingCAP, Inc.
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

package instanceplancache

import (
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/testkit"
)

var (
	typeInt      = "int"
	typeVarchar  = "varchar(255)"
	typeFloat    = "float"
	typeDouble   = "double"
	typeDecimal  = "decimal(10,2)"
	typeDatetime = "datetime"
)

func randomItem(items ...string) string {
	return items[rand.Intn(len(items))]
}

func randomItems(items ...string) []string {
	n := rand.Intn(len(items)-1) + 1
	res := make([]string, 0, n)
	used := make(map[string]bool)
	for i := 0; i < n; i++ {
		item := randomItem(items...)
		if used[item] {
			continue
		}
		res = append(res, item)
		used[item] = true
	}
	return res
}

func randomIntVal() string {
	switch rand.Intn(5) {
	case 0: // null
		return "null"
	case 1: // 0/positive/negative
		return randomItem("0", "-1", "1", "100000000", "-1000000000")
	case 2: // maxint32
		return randomItem("2147483648", "2147483647", "2147483646", "-2147483648", "-2147483647", "-2147483646")
	case 3: // maxint64
		return randomItem("9223372036854775807", "9223372036854775808", "9223372036854775806", "-9223372036854775807", "-9223372036854775808", "-9223372036854775806")
	default:
		return randomItem(fmt.Sprintf("%v", rand.Intn(3)+1000), fmt.Sprintf("-%v", rand.Intn(3)+1000),
			fmt.Sprintf("%v", rand.Intn(3)+1000000), fmt.Sprintf("-%v", rand.Intn(3)+1000000),
			fmt.Sprintf("%v", rand.Intn(3)+100000000000), fmt.Sprintf("-%v", rand.Intn(3)+100000000000),
			fmt.Sprintf("%v", rand.Intn(3)+1000000000000000), fmt.Sprintf("-%v", rand.Intn(3)+1000000000000000))
	}
}

func randomVarcharVal() string {
	switch rand.Intn(4) {
	case 0:
		return "null"
	case 1:
		return "''"
	case 2:
		return randomItem(fmt.Sprintf("'%v'", rand.Intn(1000)), fmt.Sprintf("'-%v'", rand.Intn(1000)))
	default:
		str := "weoiruklmdsSDFjfDSFpqru23h#@$@#r90ds8a90dhfksdjfl#@!@#~$@#^BFDSAFDS=========+_+-21KLEJSDKLX;FJP;ipo][1"
		start := rand.Intn(len(str))
		end := start + rand.Intn(len(str)-start)
		return fmt.Sprintf("'%v'", str[start:end])
	}
}

func randomFloat() string {
	switch rand.Intn(4) {
	case 0:
		return "null"
	case 1:
		return randomItem("0", "0.000000000", "0000.000", "-0", "-0.000000000", "-0000.000",
			"1", "1.000000000", "0001.000", "-1", "-1.000000000", "-0001.000",
			"0.00001", "0.000000001", "0000.0000000001", "-0.00001", "-0.000000001", "-0000.0000000001")
	case 2:
		return randomItem("1.234", "1.23456789", "1.234567890123456789", "-1.234", "-1.23456789", "-1.234567890123456789",
			"1234.567", "1234.567890123456789", "1234.567890123456789123456789", "-1234.567", "-1234.567890123456789", "-1234.567890123456789123456789",
			"0.00001", "0.000000001", "0000.0000000001", "-0.00001", "-0.000000001", "-0000.0000000001")
	default:
		return randomItem(fmt.Sprintf("%v", rand.Float32()), fmt.Sprintf("-%v", rand.Float32()),
			fmt.Sprintf("%v", rand.Float64()), fmt.Sprintf("-%v", rand.Float64()))
	}
}

func randomDatetime() string {
	switch rand.Intn(3) {
	case 0:
		return "null"
	case 1:
		return randomItem("'2024-01-01 00:00:00'", "'2024-01-01 00:00:00.000000'", "'2024-01-01 00:00:00.000000000'",
			"'2024-01-01 00:00:00.000000000+08:00'", "'2024-01-01 00:00:00.000000000+08:00'")
	default:
		t := time.Now().Add(time.Duration(rand.Intn(100000)) * time.Second)
		return fmt.Sprintf("'%v'", t.Format("2006-01-02 15:04:05.000000000"))
	}
}

func prepareTableData(t string, rows int, colTypes []string) []string {
	colValues := make([][]string, len(colTypes))
	for i, colType := range colTypes {
		colValues[i] = make([]string, 0, rows)
		for j := 0; j < rows; j++ {
			switch colType {
			case typeInt:
				colValues[i] = append(colValues[i], randomIntVal())
			case typeVarchar:
				colValues[i] = append(colValues[i], randomVarcharVal())
			case typeFloat, typeDouble, typeDecimal:
				colValues[i] = append(colValues[i], randomFloat())
			case typeDatetime:
				colValues[i] = append(colValues[i], randomDatetime())
			default:
				panic("not implemented")
			}
		}
	}
	var inserts []string
	for i := 0; i < rows; i++ {
		vals := make([]string, 0, len(colTypes))
		for j := range colTypes {
			vals = append(vals, colValues[j][i])
		}
		inserts = append(inserts, fmt.Sprintf("insert ignore into %s values (%s);", t, strings.Join(vals, ", ")))
	}
	return inserts
}

func prepareTables(n int) []string {
	nCols := 6
	sqls := make([]string, 0, n)
	for i := 0; i < n; i++ {
		cols := make([]string, 0, nCols)
		colNames := []string{"c0", "c1", "c2", "c3", "c4", "c5"}
		var colTypes []string
		for j := 0; j < nCols; j++ {
			colType := randomItem(typeInt, typeVarchar, typeFloat, typeDouble, typeDatetime)
			colTypes = append(colTypes, colType)
			cols = append(cols, fmt.Sprintf("c%d %v", j, colType))
		}
		pkCols := randomItems(colNames...)
		idx1 := randomItems(colNames...)
		idx2 := randomItems(colNames...)
		sqls = append(sqls, fmt.Sprintf("create table t%d (%s, primary key (%s), index idx1 (%s), index idx2 (%s));",
			i, strings.Join(cols, ", "), strings.Join(pkCols, ", "), strings.Join(idx1, ", "), strings.Join(idx2, ", ")))

		sqls = append(sqls, prepareTableData(fmt.Sprintf("t%d", i), 100, colTypes)...)
	}
	return sqls
}

func TestInstancePlanCache(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`set global tidb_enable_instance_plan_cache=1`)
	for _, s := range prepareTables(10) {
		tk.MustExec(s)
	}

	nWorkers := 5
	caseSync := new(sync.WaitGroup)
	exitSync := new(sync.WaitGroup)
	caseChs := make([]chan *testCase, nWorkers)
	for i := 0; i < nWorkers; i++ {
		caseChs[i] = make(chan *testCase, 1)
	}
	exitCh := make(chan bool)
	for i := 0; i < nWorkers; i++ {
		exitSync.Add(1)
		go executeWorker(testkit.NewTestKit(t, store), caseChs[i], exitCh, caseSync, exitSync)
	}

	for _, q := range queryPattern {
		c := prepareStmts(q, 10, 5)
		for i := 0; i < nWorkers; i++ {
			caseSync.Add(1)
			caseChs[i] <- c
		}
		caseSync.Wait()
	}

	close(exitCh)
	exitSync.Wait()
}

func executeWorker(tk *testkit.TestKit,
	caseCh chan *testCase, exit chan bool,
	caseSync, exitSync *sync.WaitGroup) {
	tk.MustExec("use test")
	defer exitSync.Done()

	for {
		select {
		case c := <-caseCh:
			tk.MustExec(c.prepStmt)
			for i := 0; i < len(c.selStmts); i++ {
				result := tk.MustQuery(c.selStmts[i]).Sort()
				tk.MustExec(c.setStmts[i])
				tk.MustQuery(c.execStmts[i]).Sort().Equal(result.Rows())
				tk.MustQuery(c.execStmts[i]).Sort().Equal(result.Rows())
				tk.MustQuery(c.execStmts[i]).Sort().Equal(result.Rows())
			}
			caseSync.Done()
		case <-exit:
			return
		}
	}
}

type testCase struct {
	prepStmt  string
	selStmts  []string
	setStmts  []string
	execStmts []string
}

func prepareStmts(q string, nTables, n int) *testCase {
	// random tables
	for strings.Contains(q, "{T}") {
		table := fmt.Sprintf("t%d", rand.Intn(nTables))
		q = strings.Replace(q, "{T}", table, 1)
	}

	// random parameters
	c := new(testCase)
	c.prepStmt = fmt.Sprintf("prepare stmt from '%s'", q)
	var numQuestionMarkers int
	for _, c := range q {
		if c == '?' {
			numQuestionMarkers++
		}
	}
	for i := 0; i < n; i++ {
		vals := genRandomValues(numQuestionMarkers)
		if len(vals) == 0 {
			continue
		}
		var setStmt, execStmt string
		for i, val := range vals {
			if i == 0 {
				setStmt = fmt.Sprintf("set @p%d=%s", i, val)
				execStmt = fmt.Sprintf("execute stmt using @p%d", i)
			} else {
				setStmt = fmt.Sprintf("%s, @p%d=%s", setStmt, i, val)
				execStmt = fmt.Sprintf("%s, @p%d", execStmt, i)
			}
		}

		selStmt := q
		for _, val := range vals {
			selStmt = strings.Replace(selStmt, "?", val, 1)
		}

		c.setStmts = append(c.setStmts, setStmt)
		c.execStmts = append(c.execStmts, execStmt)
		c.selStmts = append(c.selStmts, selStmt)
	}
	return c
}

func genRandomValues(numVals int) (vals []string) {
	for i := 0; i < numVals; i++ {
		switch rand.Intn(4) {
		case 0:
			vals = append(vals, randomIntVal())
		case 1:
			vals = append(vals, randomVarcharVal())
		case 2:
			vals = append(vals, randomFloat())
		case 3:
			vals = append(vals, randomDatetime())
		}
	}
	return
}

var queryPattern []string

func init() {
	// single table selection: select * from {T} where ...
	for i := 0; i < 100; i++ {
		queryPattern = append(queryPattern,
			fmt.Sprintf("select * from {T} where %s", randomFilters("", 5)))
	}

	// order & limit: select * from {T} where ... order by ... limit ...
	for i := 0; i < 30; i++ {
		queryPattern = append(queryPattern,
			fmt.Sprintf("select * from {T} where %s order by %s",
				randomFilters("", 5), randomItem("c0", "c1", "c2", "c3")))
		queryPattern = append(queryPattern,
			fmt.Sprintf("select * from {T} where %s limit 10",
				randomFilters("", 5)))
		queryPattern = append(queryPattern,
			fmt.Sprintf("select * from {T} where %s order by %s limit 10",
				randomFilters("", 5), randomItem("c0", "c1", "c2", "c3")))
	}

	// agg
	for i := 0; i < 30; i++ {
		queryPattern = append(queryPattern,
			fmt.Sprintf("select sum(c0) from {T} where %s group by %v",
				randomFilters("", 5), randomItem("c0", "c1", "c2", "c3")))
		queryPattern = append(queryPattern,
			fmt.Sprintf("select c0, c1, sum(c2) from {T} where %s group by c0, c1",
				randomFilters("", 5)))
	}

	// join
	for i := 0; i < 30; i++ {
		queryPattern = append(queryPattern,
			fmt.Sprintf("select * from {T} t1 join {T} t2 on t1.c0=t2.c0 where %s",
				randomFilters("t1", 5)))
		queryPattern = append(queryPattern,
			fmt.Sprintf("select * from {T} t1 join {T} t2 on t1.c0=t2.c0 where %s",
				randomFilters("t2", 5)))
		queryPattern = append(queryPattern,
			fmt.Sprintf("select * from {T} t1 join {T} t2 on t1.c0=t2.c0 where %s and %s",
				randomFilters("t2", 5), randomFilter("t1", 5)))
	}
}

func randomFilters(table string, nCols int) string {
	n := rand.Intn(3) + 1
	filters := make([]string, 0, n)
	for i := 0; i < n; i++ {
		filters = append(filters, randomFilter(table, nCols))
	}
	switch rand.Intn(2) {
	case 0:
		return strings.Join(filters, " and ")
	case 1:
		return strings.Join(filters, " or ")
	}
	return ""
}

func randomFilter(table string, nCols int) string {
	c := fmt.Sprintf("c%d", rand.Intn(nCols))
	if table != "" {
		c = table + "." + c
	}
	switch rand.Intn(10) {
	case 0:
		return fmt.Sprintf("%s=?", c)
	case 1:
		return fmt.Sprintf("%s>?", c)
	case 2:
		return fmt.Sprintf("%s<?", c)
	case 3:
		return fmt.Sprintf("%s between ? and ?", c)
	case 4:
		return fmt.Sprintf("%s in (?, ?, ?)", c)
	case 5:
		return fmt.Sprintf("%s in (?)", c)
	case 6:
		return fmt.Sprintf("%s is null", c)
	case 7:
		return fmt.Sprintf("%s is not null", c)
	case 8:
		return fmt.Sprintf("%s != ?", c)
	case 9:
		return fmt.Sprintf("%s like ?", c)
	}
	return ""
}
