// Copyright 2022 PingCAP, Inc.
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

package sqlancer

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/tools/go-sqlancer/pkg/connection"
	"github.com/pingcap/tidb/tools/go-sqlancer/pkg/executor"
	"github.com/pingcap/tidb/tools/go-sqlancer/pkg/generator"
	"github.com/pingcap/tidb/tools/go-sqlancer/pkg/knownbugs"
	"github.com/pingcap/tidb/tools/go-sqlancer/pkg/transformer"
	"github.com/pingcap/tidb/tools/go-sqlancer/pkg/types"
	"github.com/pingcap/tidb/tools/go-sqlancer/pkg/util"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

var (
	allColumnTypes = []string{"int", "float", "varchar"}
)

type testingApproach = int

const (
	approachPQS testingApproach = iota
	approachNoREC
	approachTLP
)

// SQLancer is a SQLancer
type SQLancer struct {
	generator.Generator
	conf     *Config
	executor *executor.Executor

	inWrite      sync.RWMutex
	batch        int
	roundInBatch int
}

// NewSQLancer ...
func NewSQLancer(conf *Config) (*SQLancer, error) {
	log.InitLogger(&log.Config{Level: conf.LogLevel, File: log.FileLogConfig{}})
	e, err := executor.New(conf.DSN, conf.DBName)
	if err != nil {
		return nil, err
	}
	return &SQLancer{
		conf:      conf,
		executor:  e,
		Generator: generator.Generator{Config: generator.Config{Hint: conf.EnableHint}},
	}, nil
}

// Start SQLancer
func (p *SQLancer) Start(ctx context.Context) {
	p.run(ctx)
	p.tearDown()
}

func (p *SQLancer) tearDown() {
	p.executor.Close()
}

// LoadSchema load table/view/index schema
func (p *SQLancer) LoadSchema() {
	rand.Seed(time.Now().UnixNano())
	p.Tables = make([]types.Table, 0)

	tables, err := p.executor.GetConn().FetchTables(p.conf.DBName)
	if err != nil {
		panic(err)
	}
	for _, i := range tables {
		t := types.Table{Name: types.CIStr(i)}
		columns, err := p.executor.GetConn().FetchColumns(p.conf.DBName, i)
		if err != nil {
			panic(err)
		}
		for _, column := range columns {
			col := types.Column{
				Table: types.CIStr(i),
				Name:  types.CIStr(column[0]),
				Null:  strings.EqualFold(column[2], "Yes"),
			}
			col.ParseType(column[1])
			t.Columns = append(t.Columns, col)
		}
		idx, err := p.executor.GetConn().FetchIndexes(p.conf.DBName, i)
		if err != nil {
			panic(err)
		}
		for _, j := range idx {
			t.Indexes = append(t.Indexes, types.CIStr(j))
		}
		p.Tables = append(p.Tables, t)
	}
}

// setUpDB clears dirty data, creates db, table and populates data
func (p *SQLancer) setUpDB(ctx context.Context) {
	_ = p.executor.Exec("drop database if exists " + p.conf.DBName)
	_ = p.executor.Exec("create database " + p.conf.DBName)
	_ = p.executor.Exec("use " + p.conf.DBName)

	p.createSchema(ctx)
	p.populateData()
	p.createExprIdx()
}

func (p *SQLancer) createSchema(ctx context.Context) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	g, _ := errgroup.WithContext(ctx)
	for index, columnTypes := range util.ComposeAllColumnTypes(-1, allColumnTypes) {
		tableIndex := index
		colTs := make([]string, len(columnTypes))
		copy(colTs, columnTypes)
		g.Go(func() error {
			sql, _ := p.executor.GenerateDDLCreateTable(tableIndex, colTs)
			return p.executor.Exec(sql.SQLStmt)
		})
	}
	if err := g.Wait(); err != nil {
		log.Error("create table failed", zap.Error(err))
	}

	err := p.executor.ReloadSchema()
	if err != nil {
		log.Error("reload data failed!")
	}
	for i := 0; i < r.Intn(10); i++ {
		sql, err := p.executor.GenerateDDLCreateIndex()
		if err != nil {
			log.L().Error("create index error", zap.Error(err))
		}
		err = p.executor.Exec(sql.SQLStmt)
		if err != nil {
			log.L().Error("create index failed", zap.String("sql", sql.SQLStmt), zap.Error(err))
		}
	}
	p.LoadSchema()
}

func (p *SQLancer) populateData() {
	var err error
	if err := p.executor.GetConn().Begin(); err != nil {
		log.L().Error("begin txn failed", zap.Error(err))
		return
	}
	for _, table := range p.executor.GetTables() {
		insertData := func() {
			sql, err := p.executor.GenerateDMLInsertByTable(table.Name.String())
			if err != nil {
				panic(errors.ErrorStack(err))
			}
			err = p.executor.Exec(sql.SQLStmt)
			if err != nil {
				log.L().Error("insert data failed", zap.String("sql", sql.SQLStmt), zap.Error(err))
			}
		}
		insertData()

		// update or delete
		for i := util.Rd(4); i > 0; i-- {
			tables := p.randTables()

			if err != nil {
				panic(errors.Trace(err))
			}
			if len(tables) == 0 {
				log.L().Panic("tables random by ChoosePivotedRow is empty")
			}
			var dmlStmt string
			switch util.Rd(2) {
			case 0:
				dmlStmt, err = p.DeleteStmt(tables, *table)
				if err != nil {
					// TODO: goto next generation
					log.L().Error("generate delete stmt failed", zap.Error(err))
				}
			default:
				dmlStmt, err = p.UpdateStmt(tables, *table)
				if err != nil {
					// TODO: goto next generation
					log.L().Error("generate update stmt failed", zap.Error(err))
				}
			}
			log.L().Info("Update/Delete statement", zap.String(table.Name.String(), dmlStmt))
			err = p.executor.Exec(dmlStmt)
			if err != nil {
				log.L().Error("update/delete data failed", zap.String("sql", dmlStmt), zap.Error(err))
				panic(err)
			}
		}

		countSQL := "select count(*) from " + table.Name.String()
		qi, err := p.executor.GetConn().Select(countSQL)
		if err != nil {
			log.L().Error("insert data failed", zap.String("sql", countSQL), zap.Error(err))
		}
		count := qi[0][0].ValString
		log.L().Debug("table check records count", zap.String(table.Name.String(), count))
		if c, _ := strconv.ParseUint(count, 10, 64); c == 0 {
			log.L().Info(table.Name.String() + " is empty after DELETE")
			insertData()
		}
	}
	if err := p.executor.GetConn().Commit(); err != nil {
		log.L().Error("commit txn failed", zap.Error(err))
		return
	}
}

func (p *SQLancer) createExprIdx() {
	if p.conf.EnableExprIndex {
		p.addExprIndex()
		// reload indexes created
		p.LoadSchema()
	}
}

func (p *SQLancer) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			if p.roundInBatch == 0 {
				p.refreshDatabase(ctx)
				p.batch++
			}
			p.progress()
			p.roundInBatch = (p.roundInBatch + 1) % 100
		}
	}
}

func (p *SQLancer) progress() {
	p.inWrite.RLock()
	defer func() {
		p.inWrite.RUnlock()
	}()
	var approaches []testingApproach
	// Because we creates view just in time with process(we creates views on first ViewCount rounds)
	// and current implementation depends on the PQS to ensures that there exists at lease one row in that view
	// so we must choose approachPQS in this scenario
	if p.roundInBatch < p.conf.ViewCount {
		approaches = []testingApproach{approachPQS}
	} else {
		if p.conf.EnableNoRECApproach {
			approaches = append(approaches, approachNoREC)
		}
		if p.conf.EnablePQSApproach {
			approaches = append(approaches, approachPQS)
		}
		if p.conf.EnableTLPApproach {
			approaches = append(approaches, approachTLP)
		}
	}
	approach := approaches[util.Rd(len(approaches))]
	switch approach {
	case approachPQS:
		// rand one pivot row for one table
		rawPivotRows, usedTables, err := p.ChoosePivotedRow()
		if err != nil {
			log.L().Fatal("choose pivot row failed", zap.Error(err))
		}
		selectAST, selectSQL, columns, pivotRows, err := p.GenPQSSelectStmt(rawPivotRows, usedTables)
		if err != nil {
			log.L().Fatal("generate PQS statement error", zap.Error(err))
		}
		_ = p.withTxn(util.RdBool(), func() error {
			resultRows, err := p.execSelect(selectSQL)
			if err != nil {
				log.L().Error("execSelect failed", zap.Error(err))
				return err
			}
			correct := p.verifyPQS(pivotRows, columns, resultRows)
			if !correct {
				// subSQL, err := p.minifySelect(selStmt, pivotRows, usedTables, columns)
				// if err != nil {
				// 	log.Error("occurred an error when try to simplify select", zap.String("sql", selectSQL), zap.Error(err))
				// 	fmt.Printf("query:\n%s\n", selectSQL)
				// } else {
				// 	fmt.Printf("query:\n%s\n", selectSQL)
				// 	if len(subSQL) < len(selectSQL) {
				// 		fmt.Printf("sub query:\n%s\n", subSQL)
				// 	}
				// }
				dust := knownbugs.NewDustbin([]ast.Node{selectAST}, pivotRows)
				if dust.IsKnownBug() {
					return nil
				}
				fmt.Printf("row:\n")
				p.printPivotRows(rawPivotRows)
				if p.roundInBatch < p.conf.ViewCount || p.conf.Silent {
					panic("PQS data verified failed")
				}
				return nil
			}
			if p.roundInBatch <= p.conf.ViewCount {
				if err := p.executor.GetConn().CreateViewBySelect(fmt.Sprintf("view_%d", p.roundInBatch), selectSQL, len(resultRows), columns); err != nil {
					log.L().Error("create view failed", zap.Error(err))
				}
			}
			if p.roundInBatch == p.conf.ViewCount {
				p.LoadSchema()
				if err := p.executor.ReloadSchema(); err != nil {
					panic(err)
				}
			}
			log.L().Info("check finished", zap.String("approach", "PQS"), zap.Int("batch", p.batch), zap.Int("round", p.roundInBatch), zap.Bool("result", correct))
			return nil
		})
	case approachNoREC, approachTLP:
		selectAst, _, genCtx, err := p.GenSelectStmt()
		if err != nil {
			log.L().Error("generate normal SQL statement failed", zap.Error(err))
		}
		var transformers []transformer.Transformer
		if approach == approachNoREC {
			transformers = append(transformers, transformer.NoREC)
		}

		if approach == approachTLP {
			transformers = append(
				transformers,
				&transformer.TLPTrans{
					Expr: &ast.ParenthesesExpr{Expr: p.ConditionClause(genCtx, 2)},
					Tp:   transformer.RandTLPType(),
				},
			)
		}
		_ = p.withTxn(util.RdBool(), func() error {
			nodesArr := transformer.RandTransformer(transformers...).Transform([]ast.ResultSetNode{selectAst})
			if len(nodesArr) < 2 {
				sql, _ := util.BufferOut(selectAst)
				log.L().Warn("no enough sqls were generated", zap.String("error sql", sql), zap.Int("node length", len(nodesArr)))
				return nil
			}
			sqlInOneGroup := make([]string, 0)
			resultSet := make([][]connection.QueryItems, 0)
			for _, node := range nodesArr {
				sql, err := util.BufferOut(node)
				if err != nil {
					log.L().Error("err on restoring", zap.Error(err))
				} else {
					resultRows, err := p.execSelect(sql)
					log.L().Debug(sql)
					if err != nil {
						log.L().Error("execSelect failed", zap.Error(err), zap.String("sql", sql))
						return err
					}
					resultSet = append(resultSet, resultRows)
				}
				sqlInOneGroup = append(sqlInOneGroup, sql)
			}
			correct := checkResultSet(resultSet, true)
			if !correct {
				log.Error("last round SQLs", zap.Strings("", sqlInOneGroup))
				if !p.conf.Silent {
					log.Fatal("NoREC/TLP data verified failed")
				}
			}
			log.L().Info("check finished", zap.String("approach", "NoREC"), zap.Int("batch", p.batch), zap.Int("round", p.roundInBatch), zap.Bool("result", correct))
			return nil
		})
	default:
		log.L().Fatal("unknown check approach", zap.Int("approach", approach))
	}
}

// if useExplicitTxn is set, a explicit transaction is used when doing action
// otherwise, uses auto-commit
func (p *SQLancer) withTxn(useExplicitTxn bool, action func() error) error {
	var err error
	// execute sql, ensure not null result set
	if useExplicitTxn {
		if err = p.executor.GetConn().Begin(); err != nil {
			log.L().Error("begin txn failed", zap.Error(err))
			return err
		}
		log.L().Debug("begin txn success")
		defer func() {
			if err = p.executor.GetConn().Commit(); err != nil {
				log.L().Error("commit txn failed", zap.Error(err))
				return
			}
			log.L().Debug("commit txn success")
		}()
	}
	return action()
}

func (p *SQLancer) addExprIndex() {
	for i := 0; i < util.Rd(10)+1; i++ {
		n := p.createExpressionIndex()
		if n == nil {
			continue
		}
		var sql string
		if sql, err := util.BufferOut(n); err != nil {
			// should never panic
			panic(errors.Trace(err))
		} else if _, err = p.executor.GetConn().Select(sql); err != nil {
			panic(errors.Trace(err))
		}
		fmt.Println("add one index on expression success SQL:" + sql)
	}
	fmt.Println("Create expression index successfully")
}

func (p *SQLancer) createExpressionIndex() *ast.CreateIndexStmt {
	table := p.Tables[util.Rd(len(p.Tables))]
	/* only contains a primary key col and a varchar col in `table_varchar`
	   it will cause panic when create an expression index on it
	   since a varchar can not do logic ops and other ops with numberic
	*/
	if table.Name.EqString("table_varchar") {
		return nil
	}
	columns := make([]types.Column, 0)
	// remove auto increment column for avoiding ERROR 3109:
	// `Generated column '' cannot refer to auto-increment column`
	for _, column := range table.Columns {
		if !column.Name.HasPrefix("id_") {
			columns = append(columns, column)
		}
	}
	var backup []types.Column
	copy(backup, table.Columns)
	table.Columns = columns

	exprs := make([]ast.ExprNode, 0)
	for x := 0; x < util.Rd(3)+1; x++ {
		gCtx := generator.NewGenCtx([]types.Table{table}, nil)
		gCtx.IsInExprIndex = true
		gCtx.EnableLeftRightJoin = false
		exprs = append(exprs, &ast.ParenthesesExpr{Expr: p.ConditionClause(gCtx, 1)})
	}
	node := ast.CreateIndexStmt{}
	node.IndexName = "idx_" + util.RdStringChar(5)
	node.Table = &ast.TableName{Name: table.Name.ToModel()}
	node.IndexPartSpecifications = make([]*ast.IndexPartSpecification, 0)
	for _, expr := range exprs {
		node.IndexPartSpecifications = append(node.IndexPartSpecifications, &ast.IndexPartSpecification{
			Expr: expr,
		})
	}
	node.IndexOption = &ast.IndexOption{}

	table.Columns = backup
	return &node
}

func (p *SQLancer) randTables() []types.Table {
	count := 1
	if len(p.Tables) > 1 {
		// avoid too deep joins
		if count = util.Rd(len(p.Tables)-1) + 1; count > 4 {
			count = util.Rd(4) + 1
		}
	}
	rand.Shuffle(len(p.Tables), func(i, j int) { p.Tables[i], p.Tables[j] = p.Tables[j], p.Tables[i] })
	usedTables := make([]types.Table, count)
	copy(usedTables, p.Tables[:count])
	return usedTables
}

// ChoosePivotedRow choose a row
// it may move to another struct
func (p *SQLancer) ChoosePivotedRow() (map[string]*connection.QueryItem, []types.Table, error) {
	result := make(map[string]*connection.QueryItem)
	usedTables := p.randTables()
	var reallyUsed []types.Table

	for _, i := range usedTables {
		sql := fmt.Sprintf("SELECT * FROM %s ORDER BY RAND() LIMIT 1;", i.Name)
		exeRes, err := p.execSelect(sql)
		if err != nil {
			panic(err)
		}
		if len(exeRes) > 0 {
			for _, c := range exeRes[0] {
				// panic(fmt.Sprintf("no rows in table %s", i.Column))
				tableColumn := types.Column{Table: i.Name, Name: types.CIStr(c.ValType.Name())}
				result[tableColumn.String()] = c
			}
			reallyUsed = append(reallyUsed, i)
		}
	}
	return result, reallyUsed, nil
}

// GenPQSSelectStmt is to generate SelectStmt for PQS
func (p *SQLancer) GenPQSSelectStmt(pivotRows map[string]*connection.QueryItem,
	usedTables []types.Table) (*ast.SelectStmt, string, []types.Column, map[string]*connection.QueryItem, error) {
	genCtx := generator.NewGenCtx(usedTables, pivotRows)
	genCtx.IsPQSMode = true

	return p.Generator.SelectStmt(genCtx, p.conf.Depth)
}

// ExecAndVerify is to execute sql and verify the result
func (p *SQLancer) ExecAndVerify(stmt *ast.SelectStmt, originRow map[string]*connection.QueryItem, columns []types.Column) (bool, error) {
	sql, err := util.BufferOut(stmt)
	if err != nil {
		return false, err
	}
	resultSets, err := p.execSelect(sql)
	if err != nil {
		return false, err
	}
	res := p.verifyPQS(originRow, columns, resultSets)
	return res, nil
}

// may not return string
func (p *SQLancer) execSelect(stmt string) ([]connection.QueryItems, error) {
	log.L().Debug("execSelect", zap.String("stmt", stmt))
	return p.executor.GetConn().Select(stmt)
}

func (p *SQLancer) verifyPQS(originRow map[string]*connection.QueryItem, columns []types.Column, resultSets []connection.QueryItems) bool {
	for _, row := range resultSets {
		if p.checkRow(originRow, columns, row) {
			return true
		}
	}
	return false
}

func (*SQLancer) checkRow(originRow map[string]*connection.QueryItem, columns []types.Column, resultSet connection.QueryItems) bool {
	for i, c := range columns {
		// fmt.Printf("i: %d, column: %+v, left: %+v, right: %+v", i, c, originRow[c], resultSet[i])
		if !compareQueryItem(originRow[c.GetAliasName().String()], resultSet[i]) {
			return false
		}
	}
	return true
}

func (*SQLancer) printPivotRows(pivotRows map[string]*connection.QueryItem) {
	var tableColumns types.Columns
	for column := range pivotRows {
		parsed := strings.Split(column, ".")
		table, col := parsed[0], parsed[1]
		tableColumns = append(tableColumns, types.Column{
			Table: types.CIStr(table),
			Name:  types.CIStr(col),
		})
	}

	sort.Sort(tableColumns)
	for _, column := range tableColumns {
		value := pivotRows[column.String()]
		fmt.Printf("%s.%s=%s\n", column.Table, column.Name, value.String())
	}
}

func compareQueryItem(left *connection.QueryItem, right *connection.QueryItem) bool {
	// if left.ValType.Name() != right.ValType.Name() {
	// 	return false
	// }
	if left.Null != right.Null {
		return false
	}

	return (left.Null && right.Null) || (left.ValString == right.ValString)
}

func (p *SQLancer) refreshDatabase(ctx context.Context) {
	p.inWrite.Lock()
	defer func() {
		p.inWrite.Unlock()
	}()
	log.L().Debug("refresh database")
	p.setUpDB(ctx)
}

// GenSelectStmt is to generate select statements
func (p *SQLancer) GenSelectStmt() (*ast.SelectStmt, string, *generator.GenCtx, error) {
	genCtx := generator.NewGenCtx(p.randTables(), nil)
	genCtx.IsPQSMode = false

	selectAST, selectSQL, _, _, err := p.Generator.SelectStmt(genCtx, p.conf.Depth)
	return selectAST, selectSQL, genCtx, err
}

// may sort input slice
func checkResultSet(set [][]connection.QueryItems, ignoreSort bool) bool {
	if len(set) < 2 {
		return true
	}

	if ignoreSort {
		for _, rows := range set {
			sort.SliceStable(rows, func(i, j int) bool {
				if len(rows[i]) > len(rows[j]) {
					return false
				} else if len(rows[i]) < len(rows[j]) {
					return true
				}
				for k := 0; k < len(rows[i]); k++ {
					if rows[i][k].String() > rows[j][k].String() {
						return false
					} else if rows[i][k].String() < rows[j][k].String() {
						return true
					}
				}
				return false
			})
		}
	}
	baseRows := set[0]
	for i := 1; i < len(set); i++ {
		if len(set[i]) != len(baseRows) {
			return false
		}
		for j := range set[i] {
			if len(set[i][j]) != len(baseRows[j]) {
				return false
			}
			for k := range set[i][j] {
				if !compareQueryItem(set[i][j][k], baseRows[j][k]) {
					return false
				}
			}
		}
	}
	return true
}
