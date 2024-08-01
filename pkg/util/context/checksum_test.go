// Copyright 2024 PingCAP, Inc.
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

package context_test

import (
	"testing"

	distsqlctx "github.com/pingcap/tidb/pkg/distsql/context"
	exprctx "github.com/pingcap/tidb/pkg/expression/context"
	"github.com/pingcap/tidb/pkg/expression/contextsession"
	"github.com/pingcap/tidb/pkg/expression/contextstatic"
	planctx "github.com/pingcap/tidb/pkg/planner/context"
	"github.com/pingcap/tidb/pkg/util/hack"
	rangerctx "github.com/pingcap/tidb/pkg/util/ranger/context"
	"github.com/stretchr/testify/require"
)

func TestImplementDetach(t *testing.T) {
	// This test will fail when the type of `ExprContext`, `BuildContext` or any other interface below
	// changed. Before update the checksum and make the test pass, please consider the following comments:

	// The `IntoStatic` method should be modified for the new fields, and make sure the generated
	// 	static interface is correct.
	require.Equal(t, uint64(0xc97c250887ab24c9), hack.CalcTypeChecksum[exprctx.ExprContext]())
	require.Equal(t, uint64(0xf4cf9344dcf26343), hack.CalcTypeChecksum[exprctx.BuildContext]())
	require.Equal(t, uint64(0x45cbd7a388d1a9b8), hack.CalcTypeChecksum[exprctx.EvalContext]())
	require.Equal(t, uint64(0xc0c84f99907c4285), hack.CalcTypeChecksum[contextsession.SessionExprContext]())
	require.Equal(t, uint64(0x947a42f0886ff057), hack.CalcTypeChecksum[contextsession.SessionEvalContext]())
	require.Equal(t, uint64(0xc40611086e3216cd), hack.CalcTypeChecksum[contextstatic.StaticExprContext]())
	require.Equal(t, uint64(0xdcf72f3125c9ed3b), hack.CalcTypeChecksum[contextstatic.StaticEvalContext]())

	// The `Detach` method shloud be modofied for the new fields
	require.Equal(t, uint64(0xfc9772c1ea0e7410), hack.CalcTypeChecksum[rangerctx.RangerContext]())
	require.Equal(t, uint64(0xc9373d8c26029076), hack.CalcTypeChecksum[distsqlctx.DistSQLContext]())
	require.Equal(t, uint64(0x5d0f526263a0d8f2), hack.CalcTypeChecksum[planctx.BuildPBContext]())
}
