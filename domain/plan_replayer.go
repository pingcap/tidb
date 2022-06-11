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

// dumpFileGcChecker is used to gc dump file in circle
// For now it is used by `plan replayer` and `trace plan` statement
type dumpFileGcChecker struct {
	sync.Mutex
	gcLease time.Duration
	paths   []string
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

func (p *dumpFileGcChecker) gcDumpFiles(t time.Duration) {
	p.Lock()
	defer p.Unlock()
	for _, path := range p.paths {
		p.gcDumpFilesByPath(path, t)
	}
}

func (p *dumpFileGcChecker) gcDumpFilesByPath(path string, t time.Duration) {
	files, err := ioutil.ReadDir(path)
	if err != nil {
		if !os.IsNotExist(err) {
			logutil.BgLogger().Warn("[dumpFileGcChecker] open plan replayer directory failed", zap.Error(err))
		}
	}

	gcTime := time.Now().Add(-t)
	for _, f := range files {
		fileName := f.Name()
		createTime, err := parseTime(fileName)
		if err != nil {
			logutil.BgLogger().Error("[dumpFileGcChecker] parseTime failed", zap.Error(err), zap.String("filename", fileName))
			continue
		}
		if !createTime.After(gcTime) {
			err := os.Remove(filepath.Join(path, f.Name()))
			if err != nil {
				logutil.BgLogger().Warn("[dumpFileGcChecker] remove file failed", zap.Error(err), zap.String("filename", fileName))
				continue
			}
			logutil.BgLogger().Info("dumpFileGcChecker successful", zap.String("filename", fileName))
		}
	}
}
