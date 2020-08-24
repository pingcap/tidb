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
// See the License for the specific language governing permissions and
// limitations under the License.

package ddl_test

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/ddl"
)

var _ = Suite(&testDDLAlgorithmSuite{})

var (
	allAlgorithm = []ast.AlgorithmType{ast.AlgorithmTypeCopy,
		ast.AlgorithmTypeInplace, ast.AlgorithmTypeInstant}
)

type testDDLAlgorithmSuite struct{}

type testCase struct {
	alterSpec          ast.AlterTableSpec
	supportedAlgorithm []ast.AlgorithmType
	defAlgorithm       ast.AlgorithmType
}

func (s *testDDLAlgorithmSuite) TestFindAlterAlgorithm(c *C) {
	instantAlgorithm := []ast.AlgorithmType{ast.AlgorithmTypeInstant}
	inplaceAlgorithm := []ast.AlgorithmType{ast.AlgorithmTypeInplace}

	testCases := []testCase{
		{ast.AlterTableSpec{Tp: ast.AlterTableAddConstraint}, inplaceAlgorithm, ast.AlgorithmTypeInplace},
		{ast.AlterTableSpec{Tp: ast.AlterTableAddColumns}, instantAlgorithm, ast.AlgorithmTypeInstant},
		{ast.AlterTableSpec{Tp: ast.AlterTableDropPrimaryKey}, instantAlgorithm, ast.AlgorithmTypeInstant},
		{ast.AlterTableSpec{Tp: ast.AlterTableDropIndex}, instantAlgorithm, ast.AlgorithmTypeInstant},
		{ast.AlterTableSpec{Tp: ast.AlterTableDropForeignKey}, instantAlgorithm, ast.AlgorithmTypeInstant},
		{ast.AlterTableSpec{Tp: ast.AlterTableRenameTable}, instantAlgorithm, ast.AlgorithmTypeInstant},
		{ast.AlterTableSpec{Tp: ast.AlterTableRenameIndex}, instantAlgorithm, ast.AlgorithmTypeInstant},

		// Alter table options.
		{ast.AlterTableSpec{Tp: ast.AlterTableOption, Options: []*ast.TableOption{{Tp: ast.TableOptionShardRowID}}}, instantAlgorithm, ast.AlgorithmTypeInstant},
		{ast.AlterTableSpec{Tp: ast.AlterTableOption, Options: []*ast.TableOption{{Tp: ast.TableOptionAutoIncrement}}}, instantAlgorithm, ast.AlgorithmTypeInstant},
		{ast.AlterTableSpec{Tp: ast.AlterTableOption, Options: []*ast.TableOption{{Tp: ast.TableOptionComment}}}, instantAlgorithm, ast.AlgorithmTypeInstant},
		{ast.AlterTableSpec{Tp: ast.AlterTableOption, Options: []*ast.TableOption{{Tp: ast.TableOptionCharset}}}, instantAlgorithm, ast.AlgorithmTypeInstant},
		{ast.AlterTableSpec{Tp: ast.AlterTableOption, Options: []*ast.TableOption{{Tp: ast.TableOptionCollate}}}, instantAlgorithm, ast.AlgorithmTypeInstant},

		// TODO: after we support migrate the data of partitions, change below cases.
		{ast.AlterTableSpec{Tp: ast.AlterTableCoalescePartitions}, instantAlgorithm, ast.AlgorithmTypeInstant},
		{ast.AlterTableSpec{Tp: ast.AlterTableAddPartitions}, instantAlgorithm, ast.AlgorithmTypeInstant},
		{ast.AlterTableSpec{Tp: ast.AlterTableDropPartition}, instantAlgorithm, ast.AlgorithmTypeInstant},
		{ast.AlterTableSpec{Tp: ast.AlterTableTruncatePartition}, instantAlgorithm, ast.AlgorithmTypeInstant},
		{ast.AlterTableSpec{Tp: ast.AlterTableExchangePartition}, instantAlgorithm, ast.AlgorithmTypeInstant},

		// TODO: after we support lock a table, change the below case.
		{ast.AlterTableSpec{Tp: ast.AlterTableLock}, instantAlgorithm, ast.AlgorithmTypeInstant},
		// TODO: after we support changing the column type, below cases need to change.
		{ast.AlterTableSpec{Tp: ast.AlterTableModifyColumn}, instantAlgorithm, ast.AlgorithmTypeInstant},
		{ast.AlterTableSpec{Tp: ast.AlterTableChangeColumn}, instantAlgorithm, ast.AlgorithmTypeInstant},
		{ast.AlterTableSpec{Tp: ast.AlterTableAlterColumn}, instantAlgorithm, ast.AlgorithmTypeInstant},
	}

	for _, tc := range testCases {
		runAlterAlgorithmTestCases(c, &tc)
	}
}

func runAlterAlgorithmTestCases(c *C, tc *testCase) {
	algorithm, err := ddl.ResolveAlterAlgorithm(&tc.alterSpec, ast.AlgorithmTypeDefault)
	c.Assert(err, IsNil)
	c.Assert(algorithm, Equals, tc.defAlgorithm)

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

	// Test supported.
	for _, alm := range tc.supportedAlgorithm {
		algorithm, err = ddl.ResolveAlterAlgorithm(&tc.alterSpec, alm)
		c.Assert(err, IsNil)
		c.Assert(algorithm, Equals, alm)
	}

	// Test unsupported.
	for _, alm := range unsupported {
		_, err = ddl.ResolveAlterAlgorithm(&tc.alterSpec, alm)
		c.Assert(err, NotNil, Commentf("Tp:%v, alm:%s", tc.alterSpec.Tp, alm))
		c.Assert(ddl.ErrAlterOperationNotSupported.Equal(err), IsTrue)
	}
}
