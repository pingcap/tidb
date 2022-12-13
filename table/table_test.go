// Copyright 2017 PingCAP, Inc.
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

package table

import (
	"testing"

	mysql "github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/stretchr/testify/require"
)

func TestErrorCode(t *testing.T) {
	require.Equal(t, mysql.ErrBadNull, int(terror.ToSQLError(ErrColumnCantNull).Code))
	require.Equal(t, mysql.ErrBadField, int(terror.ToSQLError(ErrUnknownColumn).Code))
	require.Equal(t, mysql.ErrFieldSpecifiedTwice, int(terror.ToSQLError(errDuplicateColumn).Code))
	require.Equal(t, mysql.ErrFieldGetDefaultFailed, int(terror.ToSQLError(errGetDefaultFailed).Code))
	require.Equal(t, mysql.ErrNoDefaultForField, int(terror.ToSQLError(ErrNoDefaultValue).Code))
	require.Equal(t, mysql.ErrIndexOutBound, int(terror.ToSQLError(ErrIndexOutBound).Code))
	require.Equal(t, mysql.ErrUnsupportedOp, int(terror.ToSQLError(ErrUnsupportedOp).Code))
	require.Equal(t, mysql.ErrRowNotFound, int(terror.ToSQLError(ErrRowNotFound).Code))
	require.Equal(t, mysql.ErrTableStateCantNone, int(terror.ToSQLError(ErrTableStateCantNone).Code))
	require.Equal(t, mysql.ErrColumnStateCantNone, int(terror.ToSQLError(ErrColumnStateCantNone).Code))
	require.Equal(t, mysql.ErrColumnStateNonPublic, int(terror.ToSQLError(ErrColumnStateNonPublic).Code))
	require.Equal(t, mysql.ErrIndexStateCantNone, int(terror.ToSQLError(ErrIndexStateCantNone).Code))
	require.Equal(t, mysql.ErrInvalidRecordKey, int(terror.ToSQLError(ErrInvalidRecordKey).Code))
	require.Equal(t, mysql.ErrTruncatedWrongValueForField, int(terror.ToSQLError(ErrTruncatedWrongValueForField).Code))
	require.Equal(t, mysql.ErrUnknownPartition, int(terror.ToSQLError(ErrUnknownPartition).Code))
	require.Equal(t, mysql.ErrNoPartitionForGivenValue, int(terror.ToSQLError(ErrNoPartitionForGivenValue).Code))
	require.Equal(t, mysql.ErrLockOrActiveTransaction, int(terror.ToSQLError(ErrLockOrActiveTransaction).Code))
}

func TestType(t *testing.T) {
	typeTest := Type(NormalTable)
	require.True(t, typeTest.IsNormalTable())
	typeTest = VirtualTable
	require.True(t, typeTest.IsVirtualTable())
	typeTest = ClusterTable
	require.True(t, typeTest.IsClusterTable())
}

func TestAddRecordOpts(t *testing.T) {
	opt := AddRecordOpt{
		CreateIdxOpt:  CreateIdxOpt{Untouched: true},
		ReserveAutoID: 0,
		IsUpdate:      false,
	}
	hint := WithReserveAutoIDHint(42)
	hint.ApplyOn(&opt)
	require.Equal(t, 42, opt.ReserveAutoID)

	update := isUpdate{}
	update.ApplyOn(&opt)
	require.True(t, opt.IsUpdate)

	createIdxOptFunc := func(in *CreateIdxOpt) {
		in.Untouched = false
	}
	(CreateIdxOptFunc).ApplyOn(createIdxOptFunc, &opt)
	require.False(t, opt.CreateIdxOpt.Untouched)
}
