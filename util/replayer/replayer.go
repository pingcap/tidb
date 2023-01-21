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
	"encoding/base64"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/config"
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

func generatePlanReplayerFileName(isCapture, isContinuesCapture, enableHistoricalStatsForCapture bool) (string, error) {
	// Generate key and create zip file
	time := time.Now().UnixNano()
	b := make([]byte, 16)
	//nolint: gosec
	_, err := rand.Read(b)
	if err != nil {
		return "", err
	}
	key := base64.URLEncoding.EncodeToString(b)
	if isContinuesCapture || isCapture && enableHistoricalStatsForCapture {
		return fmt.Sprintf("capture_replayer_%v_%v.zip", key, time), nil
	}
	return fmt.Sprintf("replayer_%v_%v.zip", key, time), nil
}

// GetPlanReplayerDirName returns plan replayer directory path.
// The path is related to the process id.
func GetPlanReplayerDirName() string {
	tidbLogDir := filepath.Dir(config.GetGlobalConfig().Log.File.Filename)
	return filepath.Join(tidbLogDir, "replayer")
}
