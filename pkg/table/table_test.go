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
	"context"
	"testing"

	mysql "github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/parser/terror"
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

func TestOptions(t *testing.T) {
	ctx := context.WithValue(context.Background(), "test", "test")
	// NewAddRecordOpt without option
	addOpt := NewAddRecordOpt()
	require.Equal(t, AddRecordOpt{}, *addOpt)
	require.Equal(t, CreateIdxOpt{}, *(addOpt.GetCreateIdxOpt()))
	// NewAddRecordOpt with options
	addOpt = NewAddRecordOpt(WithCtx(ctx), IsUpdate, WithReserveAutoIDHint(12))
	require.Equal(t, AddRecordOpt{
		commonMutateOpt: commonMutateOpt{ctx: ctx},
		isUpdate:        true,
		reserveAutoID:   12,
	}, *addOpt)
	require.Equal(t, CreateIdxOpt{commonMutateOpt: commonMutateOpt{ctx: ctx}}, *(addOpt.GetCreateIdxOpt()))
	// NewUpdateRecordOpt without option
	updateOpt := NewUpdateRecordOpt()
	require.Equal(t, UpdateRecordOpt{}, *updateOpt)
	require.Equal(t, AddRecordOpt{}, *(updateOpt.GetAddRecordOpt()))
	require.Equal(t, CreateIdxOpt{}, *(updateOpt.GetCreateIdxOpt()))
	// NewUpdateRecordOpt with options
	updateOpt = NewUpdateRecordOpt(WithCtx(ctx))
	require.Equal(t, UpdateRecordOpt{commonMutateOpt: commonMutateOpt{ctx: ctx}}, *updateOpt)
	require.Equal(t, AddRecordOpt{commonMutateOpt: commonMutateOpt{ctx: ctx}}, *(updateOpt.GetAddRecordOpt()))
	require.Equal(t, CreateIdxOpt{commonMutateOpt: commonMutateOpt{ctx: ctx}}, *(updateOpt.GetCreateIdxOpt()))
	// NewCreateIdxOpt without option
	createIdxOpt := NewCreateIdxOpt()
	require.Equal(t, CreateIdxOpt{}, *createIdxOpt)
	// NewCreateIdxOpt with options
	createIdxOpt = NewCreateIdxOpt(WithCtx(ctx), WithIgnoreAssertion, FromBackfill)
	require.Equal(t, CreateIdxOpt{
		commonMutateOpt: commonMutateOpt{ctx: ctx},
		ignoreAssertion: true,
		fromBackFill:    true,
	}, *createIdxOpt)
}
