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

package mutation

import (
	"github.com/pingcap/tidb/tools/go-sqlancer/pkg/generator"
	"github.com/pingcap/tidb/tools/go-sqlancer/pkg/types/mutasql"
)

// AdditionSelect is to generate AdditionSelect
type AdditionSelect struct {
}

// Condition is to Condition
func (*AdditionSelect) Condition(tc *mutasql.TestCase) bool {
	return tc.Mutable && len(tc.GetAllTables()) > 0
}

// Mutate is to Mutate
func (*AdditionSelect) Mutate(tc *mutasql.TestCase, g *generator.Generator) ([]*mutasql.TestCase, error) {
	mutated := tc.Clone()
	origin := tc.Clone()

	genCtx := generator.NewGenCtx(tc.GetAllTables(), nil)
	genCtx.IsPQSMode = false

	selectAST, _, _, _, err := g.SelectStmt(genCtx, 3)
	if err != nil {
		return nil, err
	}

	mutated.AfterInsert = append(mutated.AfterInsert, selectAST)

	return []*mutasql.TestCase{&origin, &mutated}, nil
}
