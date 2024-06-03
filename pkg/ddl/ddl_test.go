// Copyright 2015 PingCAP, Inc.
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

package ddl

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

// DDLForTest exports for testing.
type DDLForTest interface {
	// SetInterceptor sets the interceptor.
	SetInterceptor(h Interceptor)
	NewReorgCtx(jobID int64, rowCount int64) *reorgCtx
	GetReorgCtx(jobID int64) *reorgCtx
	RemoveReorgCtx(id int64)
}

// SetInterceptor implements DDL.SetInterceptor interface.
func (d *ddl) SetInterceptor(i Interceptor) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.mu.interceptor = i
}

// IsReorgCanceled exports for testing.
func (rc *reorgCtx) IsReorgCanceled() bool {
	return rc.isReorgCanceled()
}

// NewReorgCtx exports for testing.
func (d *ddl) NewReorgCtx(jobID int64, rowCount int64) *reorgCtx {
	return d.newReorgCtx(jobID, rowCount)
}

// GetReorgCtx exports for testing.
func (d *ddl) GetReorgCtx(jobID int64) *reorgCtx {
	return d.getReorgCtx(jobID)
}

// RemoveReorgCtx exports for testing.
func (d *ddl) RemoveReorgCtx(id int64) {
	d.removeReorgCtx(id)
}

func createMockStore(t *testing.T) kv.Storage {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	return store
}

func TestGetIntervalFromPolicy(t *testing.T) {
	policy := []time.Duration{
		1 * time.Second,
		2 * time.Second,
	}
	var (
		val     time.Duration
		changed bool
	)

	val, changed = getIntervalFromPolicy(policy, 0)
	require.Equal(t, val, 1*time.Second)
	require.True(t, changed)

	val, changed = getIntervalFromPolicy(policy, 1)
	require.Equal(t, val, 2*time.Second)
	require.True(t, changed)

	val, changed = getIntervalFromPolicy(policy, 2)
	require.Equal(t, val, 2*time.Second)
	require.False(t, changed)

	val, changed = getIntervalFromPolicy(policy, 3)
	require.Equal(t, val, 2*time.Second)
	require.False(t, changed)
}

func colDefStrToFieldType(t *testing.T, str string, ctx sessionctx.Context) *types.FieldType {
	sqlA := "alter table t modify column a " + str
	stmt, err := parser.New().ParseOneStmt(sqlA, "", "")
	require.NoError(t, err)
	colDef := stmt.(*ast.AlterTableStmt).Specs[0].NewColumns[0]
	chs, coll := charset.GetDefaultCharsetAndCollate()
	col, _, err := buildColumnAndConstraint(ctx, 0, colDef, nil, chs, coll)
	require.NoError(t, err)
	return &col.FieldType
}

func TestModifyColumn(t *testing.T) {
	ctx := mock.NewContext()
	tests := []struct {
		origin string
		to     string
		err    error
	}{
		{"int", "bigint", nil},
		{"int", "int unsigned", nil},
		{"varchar(10)", "text", nil},
		{"varbinary(10)", "blob", nil},
		{"text", "blob", dbterror.ErrUnsupportedModifyCharset.GenWithStackByArgs("charset from utf8mb4 to binary")},
		{"varchar(10)", "varchar(8)", nil},
		{"varchar(10)", "varchar(11)", nil},
		{"varchar(10) character set utf8 collate utf8_bin", "varchar(10) character set utf8", nil},
		{"decimal(2,1)", "decimal(3,2)", nil},
		{"decimal(2,1)", "decimal(2,2)", nil},
		{"decimal(2,1)", "decimal(2,1)", nil},
		{"decimal(2,1)", "int", nil},
		{"decimal", "int", nil},
		{"decimal(2,1)", "bigint", nil},
		{"int", "varchar(10) character set gbk", dbterror.ErrUnsupportedModifyCharset.GenWithStackByArgs("charset from binary to gbk")},
		{"varchar(10) character set gbk", "int", dbterror.ErrUnsupportedModifyCharset.GenWithStackByArgs("charset from gbk to binary")},
		{"varchar(10) character set gbk", "varchar(10) character set utf8", dbterror.ErrUnsupportedModifyCharset.GenWithStackByArgs("charset from gbk to utf8")},
		{"varchar(10) character set gbk", "char(10) character set utf8", dbterror.ErrUnsupportedModifyCharset.GenWithStackByArgs("charset from gbk to utf8")},
		{"varchar(10) character set utf8", "char(10) character set gbk", dbterror.ErrUnsupportedModifyCharset.GenWithStackByArgs("charset from utf8 to gbk")},
		{"varchar(10) character set utf8", "varchar(10) character set gbk", dbterror.ErrUnsupportedModifyCharset.GenWithStackByArgs("charset from utf8 to gbk")},
		{"varchar(10) character set gbk", "varchar(255) character set gbk", nil},
	}
	for _, tt := range tests {
		ftA := colDefStrToFieldType(t, tt.origin, ctx)
		ftB := colDefStrToFieldType(t, tt.to, ctx)
		err := checkModifyTypes(ftA, ftB, false)
		if err == nil {
			require.NoErrorf(t, tt.err, "origin:%v, to:%v", tt.origin, tt.to)
		} else {
			require.EqualError(t, err, tt.err.Error())
		}
	}
}

func TestFieldCase(t *testing.T) {
	var fields = []string{"field", "Field"}
	colObjects := make([]*model.ColumnInfo, len(fields))
	for i, name := range fields {
		colObjects[i] = &model.ColumnInfo{
			Name: model.NewCIStr(name),
		}
	}
	err := checkDuplicateColumn(colObjects)
	require.EqualError(t, err, infoschema.ErrColumnExists.GenWithStackByArgs("Field").Error())
}

func TestIgnorableSpec(t *testing.T) {
	specs := []ast.AlterTableType{
		ast.AlterTableOption,
		ast.AlterTableAddColumns,
		ast.AlterTableAddConstraint,
		ast.AlterTableDropColumn,
		ast.AlterTableDropPrimaryKey,
		ast.AlterTableDropIndex,
		ast.AlterTableDropForeignKey,
		ast.AlterTableModifyColumn,
		ast.AlterTableChangeColumn,
		ast.AlterTableRenameTable,
		ast.AlterTableAlterColumn,
	}
	for _, spec := range specs {
		require.False(t, isIgnorableSpec(spec))
	}

	ignorableSpecs := []ast.AlterTableType{
		ast.AlterTableLock,
		ast.AlterTableAlgorithm,
	}
	for _, spec := range ignorableSpecs {
		require.True(t, isIgnorableSpec(spec))
	}
}

func TestBuildJobDependence(t *testing.T) {
	store := createMockStore(t)
	defer func() {
		require.NoError(t, store.Close())
	}()
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	// Add some non-add-index jobs.
	job1 := &model.Job{ID: 1, TableID: 1, Type: model.ActionAddColumn}
	job2 := &model.Job{ID: 2, TableID: 1, Type: model.ActionCreateTable}
	job3 := &model.Job{ID: 3, TableID: 2, Type: model.ActionDropColumn}
	job6 := &model.Job{ID: 6, TableID: 1, Type: model.ActionDropTable}
	job7 := &model.Job{ID: 7, TableID: 2, Type: model.ActionModifyColumn}
	job9 := &model.Job{ID: 9, SchemaID: 111, Type: model.ActionDropSchema}
	job11 := &model.Job{ID: 11, TableID: 2, Type: model.ActionRenameTable, Args: []any{int64(111), "old db name"}}
	err := kv.RunInNewTxn(ctx, store, false, func(ctx context.Context, txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		require.NoError(t, m.EnQueueDDLJob(job1))
		require.NoError(t, m.EnQueueDDLJob(job2))
		require.NoError(t, m.EnQueueDDLJob(job3))
		require.NoError(t, m.EnQueueDDLJob(job6))
		require.NoError(t, m.EnQueueDDLJob(job7))
		require.NoError(t, m.EnQueueDDLJob(job9))
		require.NoError(t, m.EnQueueDDLJob(job11))
		return nil
	})
	require.NoError(t, err)
	job4 := &model.Job{ID: 4, TableID: 1, Type: model.ActionAddIndex}
	err = kv.RunInNewTxn(ctx, store, false, func(ctx context.Context, txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		err := buildJobDependence(m, job4)
		require.NoError(t, err)
		require.Equal(t, job4.DependencyID, int64(2))
		return nil
	})
	require.NoError(t, err)
	job5 := &model.Job{ID: 5, TableID: 2, Type: model.ActionAddIndex}
	err = kv.RunInNewTxn(ctx, store, false, func(ctx context.Context, txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		err := buildJobDependence(m, job5)
		require.NoError(t, err)
		require.Equal(t, job5.DependencyID, int64(3))
		return nil
	})
	require.NoError(t, err)
	job8 := &model.Job{ID: 8, TableID: 3, Type: model.ActionAddIndex}
	err = kv.RunInNewTxn(ctx, store, false, func(ctx context.Context, txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		err := buildJobDependence(m, job8)
		require.NoError(t, err)
		require.Equal(t, job8.DependencyID, int64(0))
		return nil
	})
	require.NoError(t, err)
	job10 := &model.Job{ID: 10, SchemaID: 111, TableID: 3, Type: model.ActionAddIndex}
	err = kv.RunInNewTxn(ctx, store, false, func(ctx context.Context, txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		err := buildJobDependence(m, job10)
		require.NoError(t, err)
		require.Equal(t, job10.DependencyID, int64(9))
		return nil
	})
	require.NoError(t, err)
	job12 := &model.Job{ID: 12, SchemaID: 112, TableID: 2, Type: model.ActionAddIndex}
	err = kv.RunInNewTxn(ctx, store, false, func(ctx context.Context, txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		err := buildJobDependence(m, job12)
		require.NoError(t, err)
		require.Equal(t, job12.DependencyID, int64(11))
		return nil
	})
	require.NoError(t, err)
}

func TestError(t *testing.T) {
	kvErrs := []*terror.Error{
		dbterror.ErrDDLJobNotFound,
		dbterror.ErrCancelFinishedDDLJob,
		dbterror.ErrCannotCancelDDLJob,
	}
	for _, err := range kvErrs {
		code := terror.ToSQLError(err).Code
		require.NotEqual(t, mysql.ErrUnknown, code)
		require.Equal(t, uint16(err.Code()), code)
	}
}

func TestCheckDuplicateConstraint(t *testing.T) {
	constrNames := map[string]bool{}

	// Foreign Key
	err := checkDuplicateConstraint(constrNames, "f1", ast.ConstraintForeignKey)
	require.NoError(t, err)
	err = checkDuplicateConstraint(constrNames, "f1", ast.ConstraintForeignKey)
	require.EqualError(t, err, "[ddl:1826]Duplicate foreign key constraint name 'f1'")

	// Check constraint
	err = checkDuplicateConstraint(constrNames, "c1", ast.ConstraintCheck)
	require.NoError(t, err)
	err = checkDuplicateConstraint(constrNames, "c1", ast.ConstraintCheck)
	require.EqualError(t, err, "[ddl:3822]Duplicate check constraint name 'c1'.")

	// Unique contraints etc
	err = checkDuplicateConstraint(constrNames, "u1", ast.ConstraintUniq)
	require.NoError(t, err)
	err = checkDuplicateConstraint(constrNames, "u1", ast.ConstraintUniq)
	require.EqualError(t, err, "[ddl:1061]Duplicate key name 'u1'")
}
