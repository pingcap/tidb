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
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
	"path/filepath"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/util/external"
)

// PlanReplayerTaskKey indicates key of a plan replayer task
type PlanReplayerTaskKey struct {
	SQLDigest  string
	PlanDigest string
}

// GeneratePlanReplayerFile generates plan replayer file
func GeneratePlanReplayerFile(ctx context.Context, isCapture, isContinuesCapture, enableHistoricalStatsForCapture bool) (io.WriteCloser, string, error) {
	path := GetPlanReplayerDirName()
	storage := external.GetExternalStorage()
	fileName, err := generatePlanReplayerFileName(isCapture, isContinuesCapture, enableHistoricalStatsForCapture)
	if err != nil {
		return nil, "", errors.AddStack(err)
	}
	zf, err := storage.Create(ctx, filepath.Join(path, fileName), nil)
	if err != nil {
		return nil, "", errors.AddStack(err)
	}
	wrapper := external.NewExternalFileWriterWrap(ctx, zf)
	return wrapper, fileName, err
}

// GeneratePlanReplayerFileName generates plan replayer capture task name
func GeneratePlanReplayerFileName(isCapture, isContinuesCapture, enableHistoricalStatsForCapture bool) (string, error) {
	return generatePlanReplayerFileName(isCapture, isContinuesCapture, enableHistoricalStatsForCapture)
}

func generatePlanReplayerFileName(isCapture, isContinuesCapture, enableHistoricalStatsForCapture bool) (string, error) {
	// Generate key and create zip file
	time := time.Now().UnixNano()
	failpoint.Inject("InjectPlanReplayerFileNameTimeField", func(val failpoint.Value) {
		time = int64(val.(int))
	})
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

// GetPlanReplayerDirName returns plan replayer directory name.
// The path is related to the process id.
func GetPlanReplayerDirName() string {
	return "replayer"
}

// GetPlanReplayerFullPathDirName returns plan replayer directory path.
func GetPlanReplayerFullPathDirName() string {
	return filepath.Join(external.GetBasePath(), "replayer")
}
