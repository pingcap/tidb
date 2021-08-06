// Copyright 2020 PingCAP, Inc.
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

package stmtsummary

import (
	"testing"

	"github.com/pingcap/tidb/config"
	"github.com/stretchr/testify/require"
)

func TestSetInVariable(t *testing.T) {
	t.Parallel()
	sv := newSysVars()
	st := sv.getVariable(typeMaxStmtCount)
	require.Equal(t, int64(config.GetGlobalConfig().StmtSummary.MaxStmtCount), st)

	err := sv.setVariable(typeMaxStmtCount, "10", false)
	require.NoError(t, err)
	st = sv.getVariable(typeMaxStmtCount)
	require.Equal(t, int64(10), st)
	err = sv.setVariable(typeMaxStmtCount, "100", false)
	require.NoError(t, err)
	st = sv.getVariable(typeMaxStmtCount)
	require.Equal(t, int64(100), st)
	err = sv.setVariable(typeMaxStmtCount, "10", true)
	require.NoError(t, err)
	st = sv.getVariable(typeMaxStmtCount)
	require.Equal(t, int64(10), st)
	err = sv.setVariable(typeMaxStmtCount, "100", true)
	require.NoError(t, err)
	st = sv.getVariable(typeMaxStmtCount)
	require.Equal(t, int64(100), st)
	err = sv.setVariable(typeMaxStmtCount, "10", false)
	require.NoError(t, err)
	st = sv.getVariable(typeMaxStmtCount)
	require.Equal(t, int64(100), st)
	err = sv.setVariable(typeMaxStmtCount, "", true)
	require.NoError(t, err)
	st = sv.getVariable(typeMaxStmtCount)
	require.Equal(t, int64(10), st)
	err = sv.setVariable(typeMaxStmtCount, "", false)
	require.NoError(t, err)
	st = sv.getVariable(typeMaxStmtCount)
	require.Equal(t, int64(config.GetGlobalConfig().StmtSummary.MaxStmtCount), st)
}

func TestSetBoolVariable(t *testing.T) {
	t.Parallel()
	sv := newSysVars()
	en := sv.getVariable(typeEnable)
	require.Equal(t, config.GetGlobalConfig().StmtSummary.Enable, en > 0)

	err := sv.setVariable(typeEnable, "OFF", false)
	require.NoError(t, err)
	en = sv.getVariable(typeEnable)
	require.LessOrEqual(t, en, int64(0))
	err = sv.setVariable(typeEnable, "ON", false)
	require.NoError(t, err)
	en = sv.getVariable(typeEnable)
	require.Greater(t, en, int64(0))
	err = sv.setVariable(typeEnable, "OFF", true)
	require.NoError(t, err)
	en = sv.getVariable(typeEnable)
	require.LessOrEqual(t, en, int64(0))
	err = sv.setVariable(typeEnable, "ON", true)
	require.NoError(t, err)
	en = sv.getVariable(typeEnable)
	require.Greater(t, en, int64(0))
	err = sv.setVariable(typeEnable, "OFF", false)
	require.NoError(t, err)
	en = sv.getVariable(typeEnable)
	require.Greater(t, en, int64(0))
	err = sv.setVariable(typeEnable, "", true)
	require.NoError(t, err)
	en = sv.getVariable(typeEnable)
	require.LessOrEqual(t, en, int64(0))
	err = sv.setVariable(typeEnable, "ON", false)
	require.NoError(t, err)
	en = sv.getVariable(typeEnable)
	require.Greater(t, en, int64(0))
	err = sv.setVariable(typeEnable, "", false)
	require.NoError(t, err)
	en = sv.getVariable(typeEnable)
	require.Equal(t, config.GetGlobalConfig().StmtSummary.Enable, en > 0)
}

func TestMinValue(t *testing.T) {
	t.Parallel()
	sv := newSysVars()
	err := sv.setVariable(typeMaxStmtCount, "0", false)
	require.NoError(t, err)
	v := sv.getVariable(typeMaxStmtCount)
	require.Greater(t, v, int64(0))

	err = sv.setVariable(typeMaxSQLLength, "0", false)
	require.NoError(t, err)
	v = sv.getVariable(typeMaxSQLLength)
	require.Equal(t, int64(0), v)

	err = sv.setVariable(typeHistorySize, "0", false)
	require.NoError(t, err)
	v = sv.getVariable(typeHistorySize)
	require.Equal(t, int64(0), v)

	err = sv.setVariable(typeRefreshInterval, "0", false)
	require.NoError(t, err)
	v = sv.getVariable(typeRefreshInterval)
	require.Greater(t, v, int64(0))
}
