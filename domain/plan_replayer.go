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

func GetPlanReplayerDirName() string {
	return filepath.Join(os.TempDir(), "replayer", strconv.Itoa(os.Getpid()))
}

func parseTime(s string) (time.Time, error) {
	startIdx := strings.LastIndex(s, "_")
	if startIdx == -1 {
		return time.Time{}, errors.New("PlanRepalyerGC failed to parse the file")
	}
	endIdx := strings.LastIndex(s, ".")
	if endIdx == -1 || endIdx+1 >= startIdx {
		return time.Time{}, errors.New("PlanRepalyerGC failed to parse the file")
	}
	i, err := strconv.ParseInt(s[startIdx+1:endIdx], 10, 64)
	if err != nil {
		return time.Time{}, errors.New("PlanRepalyerGC failed to parse the file")
	}
	return time.Unix(0, i), nil
}

func (p *planReplayer) planReplayerGC(t time.Duration) {
	p.Lock()
	defer p.Unlock()
	path := GetPlanReplayerDirName()
	fmt.Println("[gc path]", path)
	files, err := ioutil.ReadDir(path)
	if err != nil {
		if !os.IsNotExist(err) {
			logutil.BgLogger().Warn("PlanRepalyerGC failed", zap.Error(err))
		}
		return
	}

	gcTime := time.Now().Add(-t)
	fmt.Println("[gcTime]", gcTime)
	for _, f := range files {
		fmt.Println("[f]", f.Name())
		createTime, err := parseTime(f.Name())
		fmt.Println("[createTime]", createTime)
		if err != nil {
			logutil.BgLogger().Warn("PlanReplayerGC faild", zap.Error(err))
		}
		if !gcTime.After(createTime) {
			err := os.Remove(filepath.Join(path, f.Name()))
			if err != nil {
				logutil.BgLogger().Warn("PlanReplayerGC faild", zap.Error(err))
			}
		}
	}
}
