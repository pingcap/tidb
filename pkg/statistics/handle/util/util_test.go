// Copyright 2025 PingCAP, Inc.
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

package util_test

import (
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestCallSCtxFailed(t *testing.T) {
	_, dom := testkit.CreateMockStoreAndDomain(t)

	var sctxWithFailure sessionctx.Context
	err := util.CallWithSCtx(dom.StatsHandle().SPool(), func(sctx sessionctx.Context) error {
		sctxWithFailure = sctx
		return errors.New("simulated error")
	})
	require.Error(t, err)
	require.Equal(t, "simulated error", err.Error())
	notReleased := infosync.ContainsInternalSessionForTest(sctxWithFailure)
	require.False(t, notReleased)
}
