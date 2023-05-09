// Copyright 2021 PingCAP, Inc.
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

package domain

import (
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/tidb/util/replayer"
	"github.com/stretchr/testify/require"
)

func TestDumpGCFileParseTime(t *testing.T) {
	nowTime := time.Now()
	name1 := fmt.Sprintf("replayer_single_xxxxxx_%v.zip", nowTime.UnixNano())
	pt, err := parseTime(name1)
	require.NoError(t, err)
	require.True(t, pt.Equal(nowTime))

	name2 := fmt.Sprintf("replayer_single_xxxxxx_%v1.zip", nowTime.UnixNano())
	_, err = parseTime(name2)
	require.NotNil(t, err)

	name3 := fmt.Sprintf("replayer_single_xxxxxx_%v._zip", nowTime.UnixNano())
	_, err = parseTime(name3)
	require.NotNil(t, err)

	name4 := "extract_-brq6zKMarD9ayaifkHc4A==_1678168728477502000.zip"
	_, err = parseTime(name4)
	require.NoError(t, err)

	var pName string
	pName, err = replayer.GeneratePlanReplayerFileName(false, false, false)
	require.NoError(t, err)
	_, err = parseTime(pName)
	require.NoError(t, err)

	pName, err = replayer.GeneratePlanReplayerFileName(true, false, false)
	require.NoError(t, err)
	_, err = parseTime(pName)
	require.NoError(t, err)

	pName, err = replayer.GeneratePlanReplayerFileName(false, true, false)
	require.NoError(t, err)
	_, err = parseTime(pName)
	require.NoError(t, err)

	pName, err = replayer.GeneratePlanReplayerFileName(true, true, false)
	require.NoError(t, err)
	_, err = parseTime(pName)
	require.NoError(t, err)

	pName, err = replayer.GeneratePlanReplayerFileName(false, false, true)
	require.NoError(t, err)
	_, err = parseTime(pName)
	require.NoError(t, err)

	pName, err = replayer.GeneratePlanReplayerFileName(true, false, true)
	require.NoError(t, err)
	_, err = parseTime(pName)
	require.NoError(t, err)

	pName, err = replayer.GeneratePlanReplayerFileName(false, true, true)
	require.NoError(t, err)
	_, err = parseTime(pName)
	require.NoError(t, err)

	pName, err = replayer.GeneratePlanReplayerFileName(true, true, true)
	require.NoError(t, err)
	_, err = parseTime(pName)
	require.NoError(t, err)
}
