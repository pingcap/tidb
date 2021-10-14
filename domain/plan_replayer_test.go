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
	"testing"
	"time"
	"fmt"
	"os"
	"path/filepath"

	"github.com/stretchr/testify/require"
)

func TestPlanReplayerGC(t *testing.T) {
	startTime := time.Now()
	time := startTime.UnixNano()
	fileName := fmt.Sprintf("replayer_single_xxxxxx_%v.zip", time)
	err := os.MkdirAll(GetPlanReplayerDirName(), os.ModePerm)
	require.Nil(t, err)
	path := filepath.Join(GetPlanReplayerDirName(), fileName)
	zf, err := os.Create(path)
	require.Nil(t, err)
	zf.Close()

	handler := &planReplayer{}
	handler.planReplayerGC(0)

	_, err = os.Stat(path)
	require.NotNil(t, err)
	require.True(t, os.IsNotExist(err))
}
