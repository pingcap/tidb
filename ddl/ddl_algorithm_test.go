// Copyright 2018 PingCAP, Inc.
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

package ddl_test

import (
	"testing"

	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/stretchr/testify/require"
)

var (
	allAlgorithm = []ast.AlgorithmType{ast.AlgorithmTypeCopy,
		ast.AlgorithmTypeInplace, ast.AlgorithmTypeInstant}
)

type testCase struct {
	alterSpec          ast.AlterTableSpec
	supportedAlgorithm []ast.AlgorithmType
	expectedAlgorithm  []ast.AlgorithmType
}

func TestFindAlterAlgorithm(t *testing.T) {
	supportedInstantAlgorithms := []ast.AlgorithmType{ast.AlgorithmTypeDefault, ast.AlgorithmTypeCopy, ast.AlgorithmTypeInplace, ast.AlgorithmTypeInstant}
	expectedInstantAlgorithms := []ast.AlgorithmType{ast.AlgorithmTypeInstant, ast.AlgorithmTypeInstant, ast.AlgorithmTypeInstant, ast.AlgorithmTypeInstant}

	testCases := []testCase{
		{
			ast.AlterTableSpec{Tp: ast.AlterTableAddConstraint},
			[]ast.AlgorithmType{ast.AlgorithmTypeDefault, ast.AlgorithmTypeCopy, ast.AlgorithmTypeInplace},
			[]ast.AlgorithmType{ast.AlgorithmTypeInplace, ast.AlgorithmTypeInplace, ast.AlgorithmTypeInplace},
		},

		{ast.AlterTableSpec{Tp: ast.AlterTableAddColumns}, supportedInstantAlgorithms, expectedInstantAlgorithms},
		{ast.AlterTableSpec{Tp: ast.AlterTableDropColumn}, supportedInstantAlgorithms, expectedInstantAlgorithms},
		{ast.AlterTableSpec{Tp: ast.AlterTableDropPrimaryKey}, supportedInstantAlgorithms, expectedInstantAlgorithms},
		{ast.AlterTableSpec{Tp: ast.AlterTableDropIndex}, supportedInstantAlgorithms, expectedInstantAlgorithms},
		{ast.AlterTableSpec{Tp: ast.AlterTableDropForeignKey}, supportedInstantAlgorithms, expectedInstantAlgorithms},
		{ast.AlterTableSpec{Tp: ast.AlterTableRenameTable}, supportedInstantAlgorithms, expectedInstantAlgorithms},
		{ast.AlterTableSpec{Tp: ast.AlterTableRenameIndex}, supportedInstantAlgorithms, expectedInstantAlgorithms},

		// Alter table options.
		{ast.AlterTableSpec{Tp: ast.AlterTableOption, Options: []*ast.TableOption{{Tp: ast.TableOptionShardRowID}}}, supportedInstantAlgorithms, expectedInstantAlgorithms},
		{ast.AlterTableSpec{Tp: ast.AlterTableOption, Options: []*ast.TableOption{{Tp: ast.TableOptionAutoIncrement}}}, supportedInstantAlgorithms, expectedInstantAlgorithms},
		{ast.AlterTableSpec{Tp: ast.AlterTableOption, Options: []*ast.TableOption{{Tp: ast.TableOptionComment}}}, supportedInstantAlgorithms, expectedInstantAlgorithms},
		{ast.AlterTableSpec{Tp: ast.AlterTableOption, Options: []*ast.TableOption{{Tp: ast.TableOptionCharset}}}, supportedInstantAlgorithms, expectedInstantAlgorithms},
		{ast.AlterTableSpec{Tp: ast.AlterTableOption, Options: []*ast.TableOption{{Tp: ast.TableOptionCollate}}}, supportedInstantAlgorithms, expectedInstantAlgorithms},

		// TODO: after we support migrate the data of partitions, change below cases.
		{ast.AlterTableSpec{Tp: ast.AlterTableCoalescePartitions}, supportedInstantAlgorithms, expectedInstantAlgorithms},
		{ast.AlterTableSpec{Tp: ast.AlterTableAddPartitions}, supportedInstantAlgorithms, expectedInstantAlgorithms},
		{ast.AlterTableSpec{Tp: ast.AlterTableDropPartition}, supportedInstantAlgorithms, expectedInstantAlgorithms},
		{ast.AlterTableSpec{Tp: ast.AlterTableTruncatePartition}, supportedInstantAlgorithms, expectedInstantAlgorithms},
		{ast.AlterTableSpec{Tp: ast.AlterTableExchangePartition}, supportedInstantAlgorithms, expectedInstantAlgorithms},

		// TODO: after we support lock a table, change the below case.
		{ast.AlterTableSpec{Tp: ast.AlterTableLock}, supportedInstantAlgorithms, expectedInstantAlgorithms},
		// TODO: after we support changing the column type, below cases need to change.
		{ast.AlterTableSpec{Tp: ast.AlterTableModifyColumn}, supportedInstantAlgorithms, expectedInstantAlgorithms},
		{ast.AlterTableSpec{Tp: ast.AlterTableChangeColumn}, supportedInstantAlgorithms, expectedInstantAlgorithms},
		{ast.AlterTableSpec{Tp: ast.AlterTableAlterColumn}, supportedInstantAlgorithms, expectedInstantAlgorithms},
	}

	for _, tc := range testCases {
		runAlterAlgorithmTestCases(t, &tc)
	}
}

func runAlterAlgorithmTestCases(t *testing.T, tc *testCase) {
	unsupported := make([]ast.AlgorithmType, 0, len(allAlgorithm))
Loop:
	for _, alm := range allAlgorithm {
		for _, almSupport := range tc.supportedAlgorithm {
			if alm == almSupport {
				continue Loop
			}
		}

		unsupported = append(unsupported, alm)
	}

	var algorithm ast.AlgorithmType
	var err error

	// Test supported.
	for i, alm := range tc.supportedAlgorithm {
		algorithm, err = ddl.ResolveAlterAlgorithm(&tc.alterSpec, alm)
		if err != nil {
			require.True(t, ddl.ErrAlterOperationNotSupported.Equal(err))
		}
		require.Equal(t, tc.expectedAlgorithm[i], algorithm)
	}

	// Test unsupported.
	for _, alm := range unsupported {
		algorithm, err = ddl.ResolveAlterAlgorithm(&tc.alterSpec, alm)
		require.Equal(t, ast.AlgorithmTypeDefault, algorithm)
		require.Error(t, err)
		require.True(t, ddl.ErrAlterOperationNotSupported.Equal(err))
	}
}
