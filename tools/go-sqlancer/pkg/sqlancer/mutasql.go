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
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	parserTypes "github.com/pingcap/tidb/parser/types"
	"github.com/pingcap/tidb/tools/go-sqlancer/pkg/connection"
	"github.com/pingcap/tidb/tools/go-sqlancer/pkg/executor"
	"github.com/pingcap/tidb/tools/go-sqlancer/pkg/generator"
	"github.com/pingcap/tidb/tools/go-sqlancer/pkg/mutation"
	"github.com/pingcap/tidb/tools/go-sqlancer/pkg/types"
	"github.com/pingcap/tidb/tools/go-sqlancer/pkg/types/mutasql"
	"github.com/pingcap/tidb/tools/go-sqlancer/pkg/util"
	"go.uber.org/zap"
)

// MutaSQL is to mutasql test
type MutaSQL struct {
	generator.Generator
	conf     *Config
	executor *executor.Executor

	pool      mutasql.Pool
	mutations []mutasql.Mutation
}

// NewMutaSQL it to create a new MutaSQL
func NewMutaSQL(conf *Config) (*MutaSQL, error) {
	log.InitLogger(&log.Config{Level: conf.LogLevel, File: log.FileLogConfig{}})
	e, err := executor.New(conf.DSN, conf.DBName)
	if err != nil {
		return nil, err
	}
	return &MutaSQL{
		conf:      conf,
		executor:  e,
		Generator: generator.Generator{Config: generator.Config{Hint: conf.EnableHint}},
		pool:      make(mutasql.Pool, 0),
		mutations: []mutasql.Mutation{
			&mutation.Rollback{},
			&mutation.AdditionSelect{},
		},
	}, nil
}

// Start is to begin test
func (m *MutaSQL) Start(ctx context.Context) {
	m.init()
	m.run(ctx)
	m.tearDown()
}

func (*MutaSQL) init() {
	// todo
}

func (m *MutaSQL) makeSeedQuery() {
	for i := 0; i < 5; i++ {
		m.makeTestCase()
	}
}

func (m *MutaSQL) makeTestCase() {
	newCase := mutasql.TestCase{Mutable: true}

	// generate some tables
	tableNum := util.Rd(3) + 1
	for i := 0; i < tableNum; i++ {
		d := new(mutasql.Dataset)
		tableName := util.RdStringChar(6)
		d.Table.Name = types.CIStr(tableName)
		d.Rows = make(map[string][]*connection.QueryItem)

		columnNum := util.Rd(4) + 1
		rowNum := util.Rd(16)
		for j := 0; j < columnNum; j++ {
			colName := util.RdStringChar(4)
			col := types.Column{
				Table: types.CIStr(tableName),
				Name:  types.CIStr(colName),
				Null:  util.RdBool(),
			}
			// notice we do NOT support timestamp/datetime
			switch util.Rd(5) {
			case 0:
				col.Type = "int"
				col.Length = 3 + util.Rd(13)
			case 1:
				col.Type = "bigint"
				col.Length = 3 + util.Rd(61)
			case 2:
				col.Type = "varchar"
				col.Length = 63 + util.Rd(64)
			case 3:
				col.Type = "float"
			case 4:
				col.Type = "text"
				col.Length = 63 + util.Rd(65473)
			case 5:
				col.Type = "timestamp"
			case 6:
				col.Type = "datetime"
			default:
				panic("no such type")
			}
			d.Table.Columns = append(d.Table.Columns, col)
			d.Rows[colName] = make([]*connection.QueryItem, 0)
			for k := 0; k < rowNum; k++ {
				d.Rows[colName] = append(d.Rows[colName], randQueryItem(col))
			}
		}

		newCase.D = append(newCase.D, d)
	}

	genCtx := generator.NewGenCtx(newCase.GetAllTables(), nil)
	genCtx.IsPQSMode = false

	selectAST, _, _, _, err := m.Generator.SelectStmt(genCtx, 3)
	if err != nil {
		log.L().Error("failed to make seed query", zap.Error(err))
		panic(fmt.Sprintf("failed to make seed query: %v", err))
	}

	newCase.Q = selectAST

	res, err := m.applyTestCase(&newCase, false)
	if err != nil {
		log.L().Error("execute sql error in seed query", zap.Error(err))
		panic("failed to execute seed query")
	}
	newCase.Result = res
	newCase.IsResultReady = true
	m.pool = append(m.pool, newCase)
}

func (m *MutaSQL) run(ctx context.Context) {
	for {
		i := 0
		select {
		case <-ctx.Done():
			return
		default:
			if i == 0 {
				i = 50
				m.refreshDB(ctx)
				m.pool = make([]mutasql.TestCase, 0)
				m.makeSeedQuery()
			}
			m.progress()
			i--
		}
	}
}

func (m *MutaSQL) tearDown() {
	m.executor.Close()
}

func (m *MutaSQL) refreshDB(ctx context.Context) {
	log.L().Info("refresh database")
	m.setUpDB(ctx)
}

func (m *MutaSQL) setUpDB(_ context.Context) {
	_ = m.executor.Exec("drop database if exists " + m.conf.DBName)
	_ = m.executor.Exec("create database " + m.conf.DBName)
	_ = m.executor.Exec("use " + m.conf.DBName)
}

func (*MutaSQL) verify(a, b []connection.QueryItems) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if len(a[i]) != len(b[i]) {
			return false
		}

		for j := range a[i] {
			if a[i][j].MustSame(b[i][j]) != nil {
				return false
			}
		}
	}
	return true
}

func (m *MutaSQL) progress() {
	if len(m.pool) == 0 {
		panic("no testcases in pool")
	}
	retry := 0
	for {
		testCase := m.pool[util.Rd(len(m.pool))]
		validMutations := make([]mutasql.Mutation, 0)
		for _, i := range m.mutations {
			if i.Condition(&testCase) {
				validMutations = append(validMutations, i)
			}
		}
		if len(validMutations) == 0 {
			log.L().Warn("no mutation satisfied")
			retry++
			if retry > 5 {
				panic("failed to choose mutation for 5 times")
			}
			continue
		}
		retry = 0
		mutation := validMutations[util.Rd(len(validMutations))]
		// TODO: remove duplicated testcases
		mutedCases, err := mutation.Mutate(&testCase, &m.Generator)
		if err != nil {
			log.L().Error("mutate error", zap.Error(err))
			continue
		}
		if len(mutedCases) < 2 {
			log.L().Error("no enough cases")
			continue
		}

		for _, testCase := range mutedCases {
			res, err := m.applyTestCase(testCase, true)
			if err != nil {
				log.L().Error("execute error", zap.Error(err))
				log.L().Error(testCase.String())
				panic("exec error")
			}
			testCase.Result = res
			testCase.IsResultReady = true
		}

		m.pool = append(m.pool, *mutedCases[0])
		for i := 1; i < len(mutedCases); i++ {
			if !m.verify(mutedCases[0].Result, mutedCases[i].Result) {
				log.L().Error("verify failed")
				m.PrintError(mutedCases[0], mutedCases[i])
				panic("verify error")
			}
			m.pool = append(m.pool, *mutedCases[i])
		}
	}
}

func (*MutaSQL) randTableNames(names []types.CIStr) map[string]string {
	result := make(map[string]string)
	for _, name := range names {
		if _, ok := result[name.String()]; !ok {
			result[name.String()] = util.RdStringChar(8)
		}
	}
	return result
}

func (m *MutaSQL) applyTestCase(t *mutasql.TestCase, rename bool) ([]connection.QueryItems, error) {
	tables := t.GetAllTables()
	originTableNames := make([]types.CIStr, 0)
	for _, i := range tables {
		originTableNames = append(originTableNames, i.Name)
	}
	tableNames := m.randTableNames(originTableNames)

	renamedCase := t
	if rename {
		renamedCase.ReplaceTableName(tableNames)
	}

	log.L().Info("rename complete")

	if err := m.execSQLsWithErrLog(renamedCase.BeforeInsert); err != nil {
		return nil, err
	}

	log.L().Info("before insert complete")

	err := m.populateData(renamedCase)
	if err != nil {
		return nil, err
	}

	log.L().Info("populate complete")

	if err = m.execSQLsWithErrLog(renamedCase.AfterInsert); err != nil {
		return nil, err
	}

	log.L().Info("after insert complete")

	sql, err := util.BufferOut(renamedCase.Q)
	if err != nil {
		return nil, err
	}

	log.L().Info("SQL EXEC: " + sql)
	res, err := m.executor.GetConn().Select(sql)
	if err != nil {
		return nil, err
	}

	if err = m.execSQLsWithErrLog(renamedCase.CleanUp); err != nil {
		return nil, err
	}

	return res, nil
}

func (m *MutaSQL) execSQLsWithErrLog(nodes []ast.Node) error {
	for _, node := range nodes {
		sql, err := util.BufferOut(node)
		if err != nil {
			return err
		}

		log.L().Info("SQL EXEC: " + sql)
		err = m.executor.Exec(sql)
		if err != nil {
			log.L().Error("sql execute error", zap.Error(err))
			return err
		}
	}
	return nil
}

// create table
func (m *MutaSQL) populateData(tc *mutasql.TestCase) error {
	var err error
	// exec Before sqls
	for _, dataset := range tc.D {
		if err = m.execSQLsWithErrLog(dataset.Before); err != nil {
			return err
		}

		err = m.createSchema(dataset.Table)
		if err != nil {
			log.L().Error("create schema error", zap.Error(err))
			return err
		}
		log.L().Info("create schema success")
		err = m.insertData(dataset.Table, dataset.Rows)
		if err != nil {
			log.L().Error("insert data error", zap.Error(err))
			return err
		}
		log.L().Info("insert data success")

		if err = m.execSQLsWithErrLog(dataset.After); err != nil {
			return err
		}
	}

	log.L().Info("populate data success")
	return nil
}

// TODO: if mutation would change CREATE TABLE clause? (such as expression index)
// use `ALTER TABLE xxx ADD PRIMARY KEY(xxx)` to add primary key in Dataset.After
// use `CREATE INDEX xxx ON xxx`
func (m *MutaSQL) createSchema(t types.Table) error {
	node := ast.CreateTableStmt{
		Table:       &ast.TableName{Name: model.NewCIStr(t.Name.String())},
		Cols:        []*ast.ColumnDef{},
		Constraints: []*ast.Constraint{},
		Options:     []*ast.TableOption{},
	}

	for _, col := range t.Columns {
		fieldType := parserTypes.NewFieldType(executor.Type2Tp(col.Type))
		fieldType.SetFlen(executor.DataType2Len(col.Type))
		node.Cols = append(node.Cols, &ast.ColumnDef{
			Name: &ast.ColumnName{Name: model.NewCIStr(col.Name.String())},
			Tp:   fieldType,
		})
	}

	sql, err := util.BufferOut(&node)
	if err != nil {
		return err
	}

	log.L().Info("SQL EXEC: " + sql)
	return m.executor.Exec(sql)
}

func (m *MutaSQL) insertData(t types.Table, columns map[string][]*connection.QueryItem) error {
	node := ast.InsertStmt{
		Table: &ast.TableRefsClause{
			TableRefs: &ast.Join{
				Left: &ast.TableName{Name: model.NewCIStr(t.Name.String())},
			},
		},
		Lists:   [][]ast.ExprNode{},
		Columns: []*ast.ColumnName{},
	}
	columnNames := make([]string, 0)
	columnTypes := make(map[string]string)
	for _, col := range t.Columns {
		columnTypes[col.Name.String()] = col.Type
	}
	for k := range columns {
		columnNames = append(columnNames, k)
		node.Columns = append(node.Columns, &ast.ColumnName{
			Table: model.NewCIStr(t.Name.String()),
			Name:  model.NewCIStr(k),
		})
	}

	if len(columnNames) == 0 {
		return nil
	}

	dataLen := len(columns[columnNames[0]])
	for i := 0; i < dataLen; i++ {
		// assume number of columnNames equals real column number in Table.Columns
		var values []ast.ExprNode
		for _, col := range columnNames {
			if val, err := transQueryItemToValue(columns[col][i], columnTypes[col]); err == nil {
				values = append(values, ast.NewValueExpr(val, "", ""))
			} else {
				return err
			}
		}
		node.Lists = append(node.Lists, values)
	}

	if len(node.Lists) == 0 {
		log.L().Warn("empty items to be inserted")
		return nil
	}

	sql, err := util.BufferOut(&node)
	if err != nil {
		return err
	}

	log.L().Info("SQL EXEC: " + sql)
	return m.executor.Exec(sql)
}

// PrintError is to print error message
func (*MutaSQL) PrintError(a, b *mutasql.TestCase) {
	log.L().Error("a: " + a.String())
	if a.IsResultReady {
		output := "Query Result_a:"
		for j, items := range a.Result {
			output += fmt.Sprintf("\n  [%d]:\n  ", j)
			for _, item := range items {
				output += fmt.Sprintf("  (%s)", item.String())
			}
		}
		log.L().Error(output)
	}
	log.L().Error("b: " + b.String())
	if b.IsResultReady {
		output := "Query Result_b:"
		for j, items := range b.Result {
			output += fmt.Sprintf("\n  [%d]:\n  ", j)
			for _, item := range items {
				output += fmt.Sprintf("  (%s)", item.String())
			}
		}
		log.L().Error(output)
	}
}

func transQueryItemToValue(q *connection.QueryItem, colType string) (interface{}, error) {
	if q.Null {
		return nil, nil
	}

	switch colType {
	case "varchar", "char", "text":
		return q.ValString, nil
	case "smallint", "int", "bigint":
		val, err := strconv.ParseInt(q.ValString, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid int format: %s", q.ValString)
		}
		return val, nil
	case "bool":
		if q.ValString == "0" || strings.ToUpper(q.ValString) == "FALSE" {
			return false, nil
		}
		if q.ValString == "1" || strings.ToUpper(q.ValString) == "TRUE" {
			return true, nil
		}
		return nil, fmt.Errorf("unrecognized bool value:%s", q.ValString)
	case "float", "decimal":
		val, err := strconv.ParseFloat(q.ValString, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid float format: %s", q.ValString)
		}
		return val, nil
	}
	return nil, fmt.Errorf("unknown type: %s", colType)
}

func randQueryItem(col types.Column) *connection.QueryItem {
	qi := new(connection.QueryItem)

	if col.Null && util.Rd(3) == 0 {
		qi.Null = true
		return qi
	}

	// ValType is useless
	qi.ValType = &sql.ColumnType{}
	switch col.Type {
	case "int", "bigint":
		var maxInt int64 = 1 << col.Length
		var val int64
		if util.Rd(3) > 0 {
			val = util.RdInt63(maxInt)
		}
		qi.ValString = fmt.Sprintf("%d", val)
	case "varchar", "text", "char":
		if util.Rd(4) > 0 {
			// qi.ValString = util.RdString(util.Rd(col.Length))
			qi.ValString = util.RdString(util.Rd(8))
		} else {
			qi.ValString = ""
		}
	case "float", "decimal":
		if util.Rd(3) > 0 {
			qi.ValString = fmt.Sprintf("%f", util.RdFloat64())
		} else {
			qi.ValString = "0"
		}
	case "timestamp", "datetime":
		// to be supported
	default:
		panic("no such type")
	}

	return qi
}
