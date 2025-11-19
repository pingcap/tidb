// Copyright 2022 PingCAP, Inc.
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

package replayer

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/spf13/afero"
	"go.uber.org/zap"
)

// PlanReplayerTaskKey indicates key of a plan replayer task
type PlanReplayerTaskKey struct {
	SQLDigest  string
	PlanDigest string
}

// GeneratePlanReplayerFile generates plan replayer file
func GeneratePlanReplayerFile(isCapture, isContinuesCapture, enableHistoricalStatsForCapture bool) (*os.File, string, error) {
	path := GetPlanReplayerDirName()
	err := os.MkdirAll(path, os.ModePerm)
	if err != nil {
		return nil, "", errors.AddStack(err)
	}
	fileName, err := generatePlanReplayerFileName(isCapture, isContinuesCapture, enableHistoricalStatsForCapture)
	if err != nil {
		return nil, "", errors.AddStack(err)
	}
	zf, err := os.Create(filepath.Join(path, fileName))
	if err != nil {
		return nil, "", errors.AddStack(err)
	}
	return zf, fileName, err
}

// GeneratePlanReplayerFileName generates plan replayer capture task name
func GeneratePlanReplayerFileName(isCapture, isContinuesCapture, enableHistoricalStatsForCapture bool) (string, error) {
	return generatePlanReplayerFileName(isCapture, isContinuesCapture, enableHistoricalStatsForCapture)
}

func generatePlanReplayerFileName(isCapture, isContinuesCapture, enableHistoricalStatsForCapture bool) (string, error) {
	// Generate key and create zip file
	time := time.Now().UnixNano()
	if val, _err_ := failpoint.Eval(_curpkg_("InjectPlanReplayerFileNameTimeField")); _err_ == nil {
		time = int64(val.(int))
	}
	b := make([]byte, 16)
	//nolint: gosec
	_, err := rand.Read(b)
	if err != nil {
		return "", err
	}
	key := base64.URLEncoding.EncodeToString(b)
	// "capture_replayer" in filename has special meaning for the /plan_replayer/dump/ HTTP handler
	if isContinuesCapture || isCapture && enableHistoricalStatsForCapture {
		return fmt.Sprintf("capture_replayer_%v_%v.zip", key, time), nil
	}
	if isCapture && !enableHistoricalStatsForCapture {
		return fmt.Sprintf("capture_normal_replayer_%v_%v.zip", key, time), nil
	}
	return fmt.Sprintf("replayer_%v_%v.zip", key, time), nil
}

var (
	// PlanReplayerPath is plan replayer directory path
	PlanReplayerPath string
	// PlanReplayerPathOnce ensures PlanReplayerPath is initialized only once
	PlanReplayerPathOnce sync.Once
)

// GetPlanReplayerDirName returns plan replayer directory path.
// The path is related to the process id.
//
// This VFS is only for testing purposes,
// but in fact, this VFS has already implemented protocols such as S3 and NFS.
// it can be supported in the future.
func GetPlanReplayerDirName(vfs ...afero.Fs) string {
	PlanReplayerPathOnce.Do(func() {
		var fs afero.Fs
		fs = afero.NewOsFs()
		if vfs != nil {
			fs = vfs[0]
		}
		tidbLogDir := filepath.Dir(config.GetGlobalConfig().Log.File.Filename)
		tidbLogDir = filepath.Join(tidbLogDir, "replayer")
		tidbLogDir = filepath.Clean(tidbLogDir)
		if canWriteToFile(fs, tidbLogDir) {
			PlanReplayerPath = tidbLogDir
			logutil.BgLogger().Info("use log dir as plan replayer dir", zap.String("dir", PlanReplayerPath))
		} else {
			PlanReplayerPath = filepath.Join(config.GetGlobalConfig().TempDir, "replayer")
			logutil.BgLogger().Info("use temp dir as plan replayer dir", zap.String("dir", PlanReplayerPath))
		}
	})
	return PlanReplayerPath
}

func canWriteToFile(vfs afero.Fs, path string) bool {
	now := time.Now()
	timeStr := now.Format("20060102150405")
	filename := fmt.Sprintf("test_%s.txt", timeStr)
	path = filepath.Join(path, filename)
	if !canWriteToFileInternal(vfs, path) {
		logutil.BgLogger().Warn("cannot write to file", zap.String("path", path))
		return false
	}
	return true
}

func canWriteToFileInternal(vfs afero.Fs, path string) bool {
	// Open the file in write mode
	file, err := vfs.OpenFile(path, os.O_RDWR|os.O_CREATE, os.ModePerm)
	if err != nil {
		return false
	}
	defer func() {
		err = file.Close()
		intest.Assert(err == nil, "failed to close file")
		if err == nil {
			err = vfs.Remove(path)
			intest.Assert(err == nil, "failed to delete file")
		}
	}()
	// Try to write a single byte to the file
	_, err = file.Write([]byte{0})
	return err == nil
}
