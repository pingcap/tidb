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

package config_test

import (
	"testing"
	"time"

	"github.com/pingcap/tidb/br/pkg/streamhelper/config"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestCheckPointLimit(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	require.Equal(t, time.Hour*48, config.DefaultTiDBConfig().GetCheckPointLagLimit())
	require.Equal(t, time.Hour*48, config.DefaultCommandConfig().GetCheckPointLagLimit())
	tk.MustExec("set @@global.tidb_advancer_check_point_lag_limit = '100h'")
	require.Equal(t, time.Hour*100, config.DefaultTiDBConfig().GetCheckPointLagLimit())
	require.Equal(t, time.Hour*48, config.DefaultCommandConfig().GetCheckPointLagLimit())
}
