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
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/ddl/testargsv1"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/metabuild"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/generic"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// DDLForTest exports for testing.
type DDLForTest interface {
	NewReorgCtx(jobID int64, rowCount int64) *reorgCtx
	GetReorgCtx(jobID int64) *reorgCtx
	RemoveReorgCtx(id int64)
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

func NewJobSubmitterForTest() *JobSubmitter {
	syncMap := generic.NewSyncMap[int64, chan struct{}](8)
	return &JobSubmitter{
		ddlJobDoneChMap: &syncMap,
	}
}

func (s *JobSubmitter) DDLJobDoneChMap() *generic.SyncMap[int64, chan struct{}] {
	return s.ddlJobDoneChMap
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

func colDefStrToFieldType(t *testing.T, str string, ctx *metabuild.Context) *types.FieldType {
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
	ctx := NewMetaBuildContextWithSctx(mock.NewContext())
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
			Name: pmodel.NewCIStr(name),
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

func TestGetTableDataKeyRanges(t *testing.T) {
	// case 1, empty flashbackIDs
	keyRanges := getTableDataKeyRanges([]int64{})
	require.Len(t, keyRanges, 1)
	require.Equal(t, keyRanges[0].StartKey, tablecodec.EncodeTablePrefix(0))
	require.Equal(t, keyRanges[0].EndKey, tablecodec.EncodeTablePrefix(meta.MaxGlobalID))

	// case 2, insert a execluded table ID
	keyRanges = getTableDataKeyRanges([]int64{3})
	require.Len(t, keyRanges, 2)
	require.Equal(t, keyRanges[0].StartKey, tablecodec.EncodeTablePrefix(0))
	require.Equal(t, keyRanges[0].EndKey, tablecodec.EncodeTablePrefix(3))
	require.Equal(t, keyRanges[1].StartKey, tablecodec.EncodeTablePrefix(4))
	require.Equal(t, keyRanges[1].EndKey, tablecodec.EncodeTablePrefix(meta.MaxGlobalID))

	// case 3, insert some execluded table ID
	keyRanges = getTableDataKeyRanges([]int64{3, 5, 9})
	require.Len(t, keyRanges, 4)
	require.Equal(t, keyRanges[0].StartKey, tablecodec.EncodeTablePrefix(0))
	require.Equal(t, keyRanges[0].EndKey, tablecodec.EncodeTablePrefix(3))
	require.Equal(t, keyRanges[1].StartKey, tablecodec.EncodeTablePrefix(4))
	require.Equal(t, keyRanges[1].EndKey, tablecodec.EncodeTablePrefix(5))
	require.Equal(t, keyRanges[2].StartKey, tablecodec.EncodeTablePrefix(6))
	require.Equal(t, keyRanges[2].EndKey, tablecodec.EncodeTablePrefix(9))
	require.Equal(t, keyRanges[3].StartKey, tablecodec.EncodeTablePrefix(10))
	require.Equal(t, keyRanges[3].EndKey, tablecodec.EncodeTablePrefix(meta.MaxGlobalID))
}

func TestMergeContinuousKeyRanges(t *testing.T) {
	cases := []struct {
		input  []keyRangeMayExclude
		expect []kv.KeyRange
	}{
		{
			[]keyRangeMayExclude{
				{
					r:       kv.KeyRange{StartKey: []byte{1}, EndKey: []byte{2}},
					exclude: true,
				},
			},
			[]kv.KeyRange{},
		},
		{
			[]keyRangeMayExclude{
				{
					r:       kv.KeyRange{StartKey: []byte{1}, EndKey: []byte{2}},
					exclude: false,
				},
			},
			[]kv.KeyRange{{StartKey: []byte{1}, EndKey: []byte{2}}},
		},
		{
			[]keyRangeMayExclude{
				{
					r:       kv.KeyRange{StartKey: []byte{1}, EndKey: []byte{2}},
					exclude: false,
				},
				{
					r:       kv.KeyRange{StartKey: []byte{3}, EndKey: []byte{4}},
					exclude: false,
				},
			},
			[]kv.KeyRange{{StartKey: []byte{1}, EndKey: []byte{4}}},
		},
		{
			[]keyRangeMayExclude{
				{
					r:       kv.KeyRange{StartKey: []byte{1}, EndKey: []byte{2}},
					exclude: false,
				},
				{
					r:       kv.KeyRange{StartKey: []byte{3}, EndKey: []byte{4}},
					exclude: true,
				},
				{
					r:       kv.KeyRange{StartKey: []byte{5}, EndKey: []byte{6}},
					exclude: false,
				},
			},
			[]kv.KeyRange{
				{StartKey: []byte{1}, EndKey: []byte{2}},
				{StartKey: []byte{5}, EndKey: []byte{6}},
			},
		},
		{
			[]keyRangeMayExclude{
				{
					r:       kv.KeyRange{StartKey: []byte{1}, EndKey: []byte{2}},
					exclude: true,
				},
				{
					r:       kv.KeyRange{StartKey: []byte{3}, EndKey: []byte{4}},
					exclude: true,
				},
				{
					r:       kv.KeyRange{StartKey: []byte{5}, EndKey: []byte{6}},
					exclude: false,
				},
			},
			[]kv.KeyRange{{StartKey: []byte{5}, EndKey: []byte{6}}},
		},
		{
			[]keyRangeMayExclude{
				{
					r:       kv.KeyRange{StartKey: []byte{1}, EndKey: []byte{2}},
					exclude: false,
				},
				{
					r:       kv.KeyRange{StartKey: []byte{3}, EndKey: []byte{4}},
					exclude: true,
				},
				{
					r:       kv.KeyRange{StartKey: []byte{5}, EndKey: []byte{6}},
					exclude: true,
				},
			},
			[]kv.KeyRange{{StartKey: []byte{1}, EndKey: []byte{2}}},
		},
		{
			[]keyRangeMayExclude{
				{
					r:       kv.KeyRange{StartKey: []byte{1}, EndKey: []byte{2}},
					exclude: true,
				},
				{
					r:       kv.KeyRange{StartKey: []byte{3}, EndKey: []byte{4}},
					exclude: false,
				},
				{
					r:       kv.KeyRange{StartKey: []byte{5}, EndKey: []byte{6}},
					exclude: true,
				},
			},
			[]kv.KeyRange{{StartKey: []byte{3}, EndKey: []byte{4}}},
		},
	}

	for i, ca := range cases {
		ranges := mergeContinuousKeyRanges(ca.input)
		require.Equal(t, ca.expect, ranges, "case %d", i)
	}
}

func TestDetectAndUpdateJobVersion(t *testing.T) {
	d := &ddl{ddlCtx: &ddlCtx{ctx: context.Background()}}

	reset := func() {
		model.SetJobVerInUse(model.JobVersion1)
	}
	t.Cleanup(reset)
	// other ut in the same address space might change it
	reset()
	require.Equal(t, model.JobVersion1, model.GetJobVerInUse())

	t.Run("in ut", func(t *testing.T) {
		reset()
		d.detectAndUpdateJobVersion()
		if testargsv1.ForceV1 {
			require.Equal(t, model.JobVersion1, model.GetJobVerInUse())
		} else {
			require.Equal(t, model.JobVersion2, model.GetJobVerInUse())
		}
	})

	d.etcdCli = &clientv3.Client{}
	mockGetAllServerInfo := func(t *testing.T, versions ...string) {
		serverInfos := make(map[string]*infosync.ServerInfo, len(versions))
		for i, v := range versions {
			serverInfos[fmt.Sprintf("node%d", i)] = &infosync.ServerInfo{
				ServerVersionInfo: infosync.ServerVersionInfo{Version: v}}
		}
		bytes, err := json.Marshal(serverInfos)
		require.NoError(t, err)
		inTerms := fmt.Sprintf("return(`%s`)", string(bytes))
		testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/domain/infosync/mockGetAllServerInfo", inTerms)
	}

	t.Run("all support v2, even with pre-release label", func(t *testing.T) {
		reset()
		mockGetAllServerInfo(t, "8.0.11-TiDB-v8.4.0-alpha-228-g650888fea7-dirty",
			"8.0.11-TiDB-v8.4.1", "8.0.11-TiDB-8.5.0-beta")
		d.detectAndUpdateJobVersion()
		require.Equal(t, model.JobVersion2, model.GetJobVerInUse())
	})

	t.Run("v1 first, later all support v2", func(t *testing.T) {
		reset()
		intervalBak := detectJobVerInterval
		t.Cleanup(func() {
			detectJobVerInterval = intervalBak
		})
		detectJobVerInterval = time.Millisecond
		// unknown version
		mockGetAllServerInfo(t, "unknown")
		iterateCnt := 0
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterDetectAndUpdateJobVersionOnce", func() {
			iterateCnt++
			if iterateCnt == 1 {
				require.Equal(t, model.JobVersion1, model.GetJobVerInUse())
				// user set version explicitly in config
				mockGetAllServerInfo(t, "9.0.0-xxx")
			} else if iterateCnt == 2 {
				require.Equal(t, model.JobVersion1, model.GetJobVerInUse())
				// invalid version
				mockGetAllServerInfo(t, "xxx")
			} else if iterateCnt == 3 {
				require.Equal(t, model.JobVersion1, model.GetJobVerInUse())
				// less than 8.4.0
				mockGetAllServerInfo(t, "8.0.11-TiDB-8.3.0")
			} else if iterateCnt == 4 {
				require.Equal(t, model.JobVersion1, model.GetJobVerInUse())
				// upgrade case
				mockGetAllServerInfo(t, "8.0.11-TiDB-v8.3.0", "8.0.11-TiDB-v8.3.0", "8.0.11-TiDB-v8.4.0")
			} else if iterateCnt == 5 {
				require.Equal(t, model.JobVersion1, model.GetJobVerInUse())
				// upgrade done
				mockGetAllServerInfo(t, "8.0.11-TiDB-v8.4.0", "8.0.11-TiDB-v8.4.0", "8.0.11-TiDB-v8.4.0")
			} else {
				require.Equal(t, model.JobVersion2, model.GetJobVerInUse())
			}
		})
		d.detectAndUpdateJobVersion()
		d.wg.Wait()
		require.EqualValues(t, 6, iterateCnt)
	})
}
