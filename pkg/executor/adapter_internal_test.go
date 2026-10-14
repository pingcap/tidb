// Copyright 2026 PingCAP, Inc.
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

package executor

import (
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
	"github.com/stretchr/testify/require"
)

type maxExecutionTimeTestContext struct {
	*mock.Context
	processInfo *util.ProcessInfo
}

func (c *maxExecutionTimeTestContext) ShowProcess() *util.ProcessInfo {
	return c.processInfo
}

func TestCheckMaxExecutionTimeExceededPreservesPendingKillReason(t *testing.T) {
	sctx := &maxExecutionTimeTestContext{
		Context: mock.NewContext(),
		processInfo: &util.ProcessInfo{
			Time:             time.Now().Add(-time.Hour),
			MaxExecutionTime: 1,
		},
	}
	sctx.GetSessionVars().SQLKiller.SendKillSignal(sqlkiller.QueryInterrupted)

	err := checkMaxExecutionTimeExceeded(sctx)
	require.True(t, exeerrors.ErrQueryInterrupted.Equal(err), err)
}
