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

package planctx

import (
	"testing"

	"github.com/pingcap/tidb/pkg/expression/exprstatic"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"github.com/pingcap/tidb/pkg/util/deeptest"
	"github.com/stretchr/testify/require"
)

func TestContextDetach(t *testing.T) {
	warnHandler := contextutil.NewStaticWarnHandler(5)
	obj := &BuildPBContext{
		ExprCtx:                            exprstatic.NewExprContext(),
		TiFlashFastScan:                    true,
		TiFlashFineGrainedShuffleBatchSize: 1,
		GroupConcatMaxLen:                  1,
		InExplainStmt:                      true,
		WarnHandler:                        warnHandler,
		ExtraWarnghandler:                  warnHandler,
	}

	ignorePath := []string{
		"$.ExprCtx",
		"$.Client",
		"$.WarnHandler",
		"$.ExtraWarnghandler",
	}
	deeptest.AssertRecursivelyNotEqual(t, obj, &BuildPBContext{}, deeptest.WithIgnorePath(ignorePath))

	staticObj := obj.Detach(obj.ExprCtx)

	deeptest.AssertDeepClonedEqual(t, obj, staticObj, deeptest.WithIgnorePath(ignorePath))

	require.Equal(t, obj.ExprCtx, staticObj.ExprCtx)
	require.Equal(t, obj.Client, staticObj.Client)
	require.Equal(t, obj.WarnHandler, staticObj.WarnHandler)
	require.Equal(t, obj.ExtraWarnghandler, staticObj.ExtraWarnghandler)
}
