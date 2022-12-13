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

package ddl

import (
	"fmt"

	"github.com/pingcap/parser/ast"
)

// AlterAlgorithm is used to store supported alter algorithm.
// For now, TiDB only support AlterAlgorithmInplace and AlterAlgorithmInstant.
// The most alter operations are using instant algorithm, and only the add index is using inplace(not really inplace,
// because we never block the DML but costs some time to backfill the index data)
// See https://dev.mysql.com/doc/refman/8.0/en/alter-table.html#alter-table-performance.
type AlterAlgorithm struct {
	// supported MUST store algorithms in the order 'INSTANT, INPLACE, COPY'
	supported []ast.AlgorithmType
	// If the alter algorithm is not given, the defAlgorithm will be used.
	defAlgorithm ast.AlgorithmType
}

var (
	instantAlgorithm = &AlterAlgorithm{
		supported:    []ast.AlgorithmType{ast.AlgorithmTypeInstant},
		defAlgorithm: ast.AlgorithmTypeInstant,
	}

	inplaceAlgorithm = &AlterAlgorithm{
		supported:    []ast.AlgorithmType{ast.AlgorithmTypeInplace},
		defAlgorithm: ast.AlgorithmTypeInplace,
	}
)

func getProperAlgorithm(specify ast.AlgorithmType, algorithm *AlterAlgorithm) (ast.AlgorithmType, error) {
	if specify == ast.AlgorithmTypeDefault {
		return algorithm.defAlgorithm, nil
	}

	r := ast.AlgorithmTypeDefault

	for _, a := range algorithm.supported {
		if specify <= a {
			r = a
			break
		}
	}

	var err error
	if specify != r {
		err = ErrAlterOperationNotSupported.GenWithStackByArgs(fmt.Sprintf("ALGORITHM=%s", specify), fmt.Sprintf("Cannot alter table by %s", specify), fmt.Sprintf("ALGORITHM=%s", algorithm.defAlgorithm))
	}
	return r, err
}

// ResolveAlterAlgorithm resolves the algorithm of the alterSpec.
// If specify is the ast.AlterAlgorithmDefault, then the default algorithm of the alter action will be returned.
// If specify algorithm is not supported by the alter action, it will try to find a better algorithm in the order `INSTANT > INPLACE > COPY`, errAlterOperationNotSupported will be returned.
// E.g. INSTANT may be returned if specify=INPLACE
// If failed to choose any valid algorithm, AlgorithmTypeDefault and errAlterOperationNotSupported will be returned
func ResolveAlterAlgorithm(alterSpec *ast.AlterTableSpec, specify ast.AlgorithmType) (ast.AlgorithmType, error) {
	switch alterSpec.Tp {
	// For now, TiDB only support inplace algorithm and instant algorithm.
	case ast.AlterTableAddConstraint:
		return getProperAlgorithm(specify, inplaceAlgorithm)
	default:
		return getProperAlgorithm(specify, instantAlgorithm)
	}
}
