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
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

type planReplayer struct {
	sync.Mutex
	planReplayerGCLease time.Duration
}

// GetPlanReplayerDirName returns plan replayer directory path.
// The path is related to the process id.
func GetPlanReplayerDirName() string {
	return filepath.Join(os.TempDir(), "replayer", strconv.Itoa(os.Getpid()))
}

func parseTime(s string) (time.Time, error) {
	startIdx := strings.LastIndex(s, "_")
	if startIdx == -1 {
		return time.Time{}, errors.New("failed to parse the file :" + s)
	}
	endIdx := strings.LastIndex(s, ".")
	if endIdx == -1 || endIdx <= startIdx+1 {
		return time.Time{}, errors.New("failed to parse the file :" + s)
	}
	i, err := strconv.ParseInt(s[startIdx+1:endIdx], 10, 64)
	if err != nil {
		return time.Time{}, errors.New("failed to parse the file :" + s)
	}
	return time.Unix(0, i), nil
}

func (p *planReplayer) planReplayerGC(t time.Duration) {
	p.Lock()
	defer p.Unlock()
	path := GetPlanReplayerDirName()
	files, err := ioutil.ReadDir(path)
	if err != nil {
		if !os.IsNotExist(err) {
			logutil.BgLogger().Warn("[PlanReplayer] open plan replayer directory failed", zap.Error(err))
		}
		return
	}

	gcTime := time.Now().Add(-t)
	for _, f := range files {
		createTime, err := parseTime(f.Name())
		if err != nil {
			logutil.BgLogger().Warn("[PlanReplayer] parseTime failed", zap.Error(err))
			continue
		}
		if !createTime.After(gcTime) {
			err := os.Remove(filepath.Join(path, f.Name()))
			if err != nil {
				logutil.BgLogger().Warn("[PlanReplayer] remove file failed", zap.Error(err))
				continue
			}
			logutil.BgLogger().Info(fmt.Sprintf("[PlanReplayer] GC %s", f.Name()))
		}
	}
}
