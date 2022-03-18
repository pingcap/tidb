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

package common

import (
	"fmt"
	"io"
	"testing"

	"github.com/pingcap/errors"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestNormalizeError(t *testing.T) {
	err := NormalizeError(nil)
	require.NoError(t, err)
	err = NormalizeError(io.EOF)
	require.True(t, berrors.Is(err, ErrUnknown))

	testCases := []struct {
		rfcErr       *errors.Error
		errMsg       string
		expectErr    *errors.Error
		expectErrMsg string
	}{
		{
			rfcErr:       berrors.ErrStorageUnknown,
			errMsg:       "ContentRange is empty",
			expectErr:    ErrStorageUnknown,
			expectErrMsg: "[Lightning:Storage:ErrStorageUnknown]ContentRange is empty",
		},
		{
			rfcErr:       berrors.ErrStorageInvalidConfig,
			errMsg:       "host not found in endpoint",
			expectErr:    ErrInvalidStorageConfig,
			expectErrMsg: "[Lightning:Storage:ErrInvalidStorageConfig]host not found in endpoint",
		},
		{
			rfcErr:       berrors.ErrStorageInvalidPermission,
			errMsg:       "check permission failed",
			expectErr:    ErrInvalidPermission,
			expectErrMsg: "[Lightning:Storage:ErrInvalidPermission]check permission failed",
		},
		{
			rfcErr:       berrors.ErrPDUpdateFailed,
			errMsg:       "create pd client failed",
			expectErr:    ErrUpdatePD,
			expectErrMsg: "[Lightning:PD:ErrUpdatePD]create pd client failed",
		},
		{
			rfcErr:       ErrInvalidConfig,
			errMsg:       "tikv-importer.backend must not be empty!",
			expectErr:    ErrInvalidConfig,
			expectErrMsg: "[Lightning:Config:ErrInvalidConfig]tikv-importer.backend must not be empty!",
		},
	}

	for _, tc := range testCases {
		err = errors.Annotate(tc.rfcErr, tc.errMsg)
		normalizedErr := NormalizeError(err)
		require.Error(t, normalizedErr)
		require.True(t, berrors.Is(normalizedErr, tc.expectErr))
		require.EqualError(t, normalizedErr, tc.expectErrMsg)
		stackTrace, ok := normalizedErr.(errors.StackTracer)
		require.True(t, ok)
		errStack := fmt.Sprintf("%+v", err.(errors.StackTracer).StackTrace())
		errStack2 := fmt.Sprintf("%+v", stackTrace.StackTrace())
		require.Equal(t, errStack, errStack2)
	}
}

func TestNormalizeOrWrapErr(t *testing.T) {
	err := NormalizeOrWrapErr(ErrInvalidArgument, nil)
	require.NoError(t, err)
	err = NormalizeOrWrapErr(ErrInvalidArgument, ErrInvalidConfig.GenWithStack("tikv-importer.backend must not be empty!"))
	require.Error(t, err)
	require.True(t, berrors.Is(err, ErrInvalidConfig))
	err = NormalizeOrWrapErr(ErrInvalidArgument, io.EOF)
	require.Error(t, err)
	require.True(t, berrors.Is(err, ErrInvalidArgument))
}
