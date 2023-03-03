// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package utils_test

import (
	"context"
	"strings"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/stretchr/testify/require"
)

type mockRestrictedSQLExecutor struct {
	rows      []chunk.Row
	fields    []*ast.ResultField
	errHappen bool
}

func (m *mockRestrictedSQLExecutor) ParseWithParams(ctx context.Context, sql string, args ...interface{}) (ast.StmtNode, error) {
	return nil, nil
}

func (m *mockRestrictedSQLExecutor) ExecRestrictedStmt(ctx context.Context, stmt ast.StmtNode, opts ...sqlexec.OptionFuncAlias) ([]chunk.Row, []*ast.ResultField, error) {
	return nil, nil, nil
}

func (m *mockRestrictedSQLExecutor) ExecRestrictedSQL(ctx context.Context, opts []sqlexec.OptionFuncAlias, sql string, args ...interface{}) ([]chunk.Row, []*ast.ResultField, error) {
	if m.errHappen {
		return nil, nil, errors.New("injected error")
	}

	if strings.Contains(sql, "show config") {
		return m.rows, m.fields, nil
	} else if strings.Contains(sql, "set config") && strings.Contains(sql, "gc.ratio-threshold") {
		value := args[0].(string)

		for _, r := range m.rows {
			d := types.Datum{}
			d.SetString(value, "")
			chunk.MutRow(r).SetDatum(3, d)
		}
	}
	return nil, nil, nil
}

func TestIsLogBackupEnabled(t *testing.T) {
	// config format:
	// MySQL [(none)]> show config where name="log-backup.enable";
	// +------+-----------------+-------------------+-------+
	// | Type | Instance        | Name              | Value |
	// +------+-----------------+-------------------+-------+
	// | tikv | 127.0.0.1:20161 | log-backup.enable | false |
	// | tikv | 127.0.0.1:20162 | log-backup.enable | false |
	// | tikv | 127.0.0.1:20160 | log-backup.enable | false |
	// +------+-----------------+-------------------+-------+
	fields := make([]*ast.ResultField, 4)
	tps := []*types.FieldType{
		types.NewFieldType(mysql.TypeString),
		types.NewFieldType(mysql.TypeString),
		types.NewFieldType(mysql.TypeString),
		types.NewFieldType(mysql.TypeString),
	}
	for i := 0; i < len(tps); i++ {
		rf := new(ast.ResultField)
		rf.Column = new(model.ColumnInfo)
		rf.Column.FieldType = *tps[i]
		fields[i] = rf
	}
	rows := make([]chunk.Row, 0, 1)

	// case 1: non of tikvs enabled log-backup expected false
	// tikv | 127.0.0.1:20161 | log-backup.enable | false |
	row := chunk.MutRowFromValues("tikv", " 127.0.0.1:20161", "log-backup.enable", "false").ToRow()
	rows = append(rows, row)
	s := &mockRestrictedSQLExecutor{rows: rows, fields: fields}
	enabled, err := utils.IsLogBackupEnabled(s)
	require.NoError(t, err)
	require.False(t, enabled)

	// case 2: one of tikvs enabled log-backup expected false
	// tikv | 127.0.0.1:20161 | log-backup.enable | false |
	// tikv | 127.0.0.1:20162 | log-backup.enable | true  |
	rows = nil
	row = chunk.MutRowFromValues("tikv", " 127.0.0.1:20161", "log-backup.enable", "false").ToRow()
	rows = append(rows, row)
	row = chunk.MutRowFromValues("tikv", " 127.0.0.1:20162", "log-backup.enable", "true").ToRow()
	rows = append(rows, row)
	s = &mockRestrictedSQLExecutor{rows: rows, fields: fields}
	enabled, err = utils.IsLogBackupEnabled(s)
	require.NoError(t, err)
	require.False(t, enabled)

	// case 3: all of tikvs enabled log-backup expected true
	// tikv | 127.0.0.1:20161 | log-backup.enable | true  |
	// tikv | 127.0.0.1:20162 | log-backup.enable | true  |
	// tikv | 127.0.0.1:20163 | log-backup.enable | true  |
	rows = nil
	row = chunk.MutRowFromValues("tikv", " 127.0.0.1:20161", "log-backup.enable", "true").ToRow()
	rows = append(rows, row)
	row = chunk.MutRowFromValues("tikv", " 127.0.0.1:20162", "log-backup.enable", "true").ToRow()
	rows = append(rows, row)
	row = chunk.MutRowFromValues("tikv", " 127.0.0.1:20163", "log-backup.enable", "true").ToRow()
	rows = append(rows, row)
	s = &mockRestrictedSQLExecutor{rows: rows, fields: fields}
	enabled, err = utils.IsLogBackupEnabled(s)
	require.NoError(t, err)
	require.True(t, enabled)

	// case 4: met error and expected false.
	s = &mockRestrictedSQLExecutor{errHappen: true}
	enabled, err = utils.IsLogBackupEnabled(s)
	require.Error(t, err)
	require.False(t, enabled)
}

func TestCheckLogBackupTaskExist(t *testing.T) {
	require.False(t, utils.CheckLogBackupTaskExist())
	utils.LogBackupTaskCountInc()
	require.True(t, utils.CheckLogBackupTaskExist())
	utils.LogBackupTaskCountDec()
	require.False(t, utils.CheckLogBackupTaskExist())
}

func TestGc(t *testing.T) {
	// config format:
	// MySQL [(none)]> show config where name = 'gc.ratio-threshold';
	// +------+-------------------+--------------------+-------+
	// | Type | Instance          | Name               | Value |
	// +------+-------------------+--------------------+-------+
	// | tikv | 172.16.6.46:3460  | gc.ratio-threshold | 1.1   |
	// | tikv | 172.16.6.47:3460  | gc.ratio-threshold | 1.1   |
	// +------+-------------------+--------------------+-------+
	fields := make([]*ast.ResultField, 4)
	tps := []*types.FieldType{
		types.NewFieldType(mysql.TypeString),
		types.NewFieldType(mysql.TypeString),
		types.NewFieldType(mysql.TypeString),
		types.NewFieldType(mysql.TypeString),
	}
	for i := 0; i < len(tps); i++ {
		rf := new(ast.ResultField)
		rf.Column = new(model.ColumnInfo)
		rf.Column.FieldType = *tps[i]
		fields[i] = rf
	}
	rows := make([]chunk.Row, 0, 2)
	row := chunk.MutRowFromValues("tikv", " 127.0.0.1:20161", "log-backup.enable", "1.1").ToRow()
	rows = append(rows, row)
	row = chunk.MutRowFromValues("tikv", " 127.0.0.1:20162", "log-backup.enable", "1.1").ToRow()
	rows = append(rows, row)

	s := &mockRestrictedSQLExecutor{rows: rows, fields: fields}
	ratio, err := utils.GetGcRatio(s)
	require.Nil(t, err)
	require.Equal(t, ratio, "1.1")

	err = utils.SetGcRatio(s, "-1.0")
	require.Nil(t, err)
	ratio, err = utils.GetGcRatio(s)
	require.Nil(t, err)
	require.Equal(t, ratio, "-1.0")
}

func TestRegionSplitInfo(t *testing.T) {
	// config format:
	// MySQL [(none)]> show config where name = 'coprocessor.region-split-size';
	// +------+-------------------+-------------------------------+-------+
	// | Type | Instance          | Name                          | Value |
	// +------+-------------------+-------------------------------+-------+
	// | tikv | 127.0.0.1:20161   | coprocessor.region-split-size | 10MB  |
	// +------+-------------------+-------------------------------+-------+
	// MySQL [(none)]> show config where name = 'coprocessor.region-split-keys';
	// +------+-------------------+-------------------------------+--------+
	// | Type | Instance          | Name                          | Value  |
	// +------+-------------------+-------------------------------+--------+
	// | tikv | 127.0.0.1:20161   | coprocessor.region-split-keys | 100000 |
	// +------+-------------------+-------------------------------+--------+

	fields := make([]*ast.ResultField, 4)
	tps := []*types.FieldType{
		types.NewFieldType(mysql.TypeString),
		types.NewFieldType(mysql.TypeString),
		types.NewFieldType(mysql.TypeString),
		types.NewFieldType(mysql.TypeString),
	}
	for i := 0; i < len(tps); i++ {
		rf := new(ast.ResultField)
		rf.Column = new(model.ColumnInfo)
		rf.Column.FieldType = *tps[i]
		fields[i] = rf
	}
	rows := make([]chunk.Row, 0, 1)
	row := chunk.MutRowFromValues("tikv", "127.0.0.1:20161", "coprocessor.region-split-size", "10MB").ToRow()
	rows = append(rows, row)
	s := &mockRestrictedSQLExecutor{rows: rows, fields: fields}
	require.Equal(t, utils.GetSplitSize(s), uint64(10000000))

	rows = make([]chunk.Row, 0, 1)
	row = chunk.MutRowFromValues("tikv", "127.0.0.1:20161", "coprocessor.region-split-keys", "100000").ToRow()
	rows = append(rows, row)
	s = &mockRestrictedSQLExecutor{rows: rows, fields: fields}
	require.Equal(t, utils.GetSplitKeys(s), int64(100000))
}
