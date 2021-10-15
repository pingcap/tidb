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

package executor

import (
	"archive/zip"
	"context"
	"crypto/md5" // #nosec G501
	"encoding/hex"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
)

const replayerPath string = "/tmp/replayer"

// TTL of plan replayer files
const remainedInterval float64 = 3

// PlanReplayerInfo saves the information of plan replayer operation.
type PlanReplayerInfo interface {
	// Process dose the export/import work for reproducing sql queries.
	Process() (string, error)
}

// PlanReplayerSingleExec represents a plan replayer executor.
type PlanReplayerSingleExec struct {
	baseExecutor
	info *PlanReplayerSingleInfo
}

// PlanReplayerSingleInfo saves the information of plan replayer operation.
type PlanReplayerSingleInfo struct {
	ExecStmt ast.StmtNode
	Analyze  bool
	Load     bool
	File     string
	Ctx      sessionctx.Context
}

type fileInfo struct {
	StartTime time.Time
	Token     [16]byte
}

type fileList struct {
	FileInfo map[string]fileInfo
	TokenMap map[[16]byte]string
}

// planReplayerVarKeyType is a dummy type to avoid naming collision in context.
type planReplayerVarKeyType int

// String defines a Stringer function for debugging and pretty printing.
func (k planReplayerVarKeyType) String() string {
	return "plan_replayer_var"
}

// planReplayerFileListType is a dummy type to avoid naming collision in context.
type planReplayerFileListType int

// String defines a Stringer function for debugging and pretty printing.
func (k planReplayerFileListType) String() string {
	return "plan_replayer_file_list"
}

// PlanReplayerVarKey is a variable key for plan replayer.
const PlanReplayerVarKey planReplayerVarKeyType = 0

// PlanReplayerFileList is a variable key for plan replayer's file list.
const PlanReplayerFileList planReplayerFileListType = 0

// Next implements the Executor Next interface.
func (e *PlanReplayerSingleExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.GrowAndReset(e.maxChunkSize)
	if e.info.ExecStmt == nil {
		return errors.New("plan replayer: sql is empty")
	}
	val := e.ctx.Value(PlanReplayerVarKey)
	if val != nil {
		e.ctx.SetValue(PlanReplayerVarKey, nil)
		return errors.New("plan replayer: previous plan replayer option isn't closed normally")
	}
	e.ctx.SetValue(PlanReplayerVarKey, e.info)
	return nil
}

// Close implements the Executor Close interface.
func (e *PlanReplayerSingleExec) Close() error {
	return nil
}

// Open implements the Executor Open interface.
func (e *PlanReplayerSingleExec) Open(ctx context.Context) error {
	return nil
}

// Process dose the export/import work for reproducing sql queries.
func (e *PlanReplayerSingleInfo) Process() (string, error) {
	// TODO: plan replayer load will be developed later
	if e.Load {
		return "", nil
	}
	return e.dumpSingle()
}

func (e *PlanReplayerSingleInfo) dumpSingle() (string, error) {
	// Create path
	err := os.MkdirAll(replayerPath, os.ModePerm)
	if err != nil {
		return "", errors.New("plan replayer: cannot create plan replayer path")
	}

	// Create zip file
	startTime := time.Now()
	fileName := fmt.Sprintf("replayer_single_%v.zip", startTime.UnixNano())
	zf, err := os.Create(replayerPath + "/" + fileName)
	if err != nil {
		return "", errors.New("plan replayer: cannot create zip file")
	}
	val := e.Ctx.Value(PlanReplayerFileList)
	if val == nil {
		e.Ctx.SetValue(PlanReplayerFileList, fileList{FileInfo: make(map[string]fileInfo), TokenMap: make(map[[16]byte]string)})
	} else {
		// Clean outdated files
		Flist := val.(fileList).FileInfo
		TList := val.(fileList).TokenMap
		for k, v := range Flist {
			if time.Since(v.StartTime).Minutes() > remainedInterval {
				err := os.Remove(replayerPath + "/" + k)
				if err != nil {
					logutil.BgLogger().Warn(fmt.Sprintf("Cleaning outdated file %s failed.", k))
				}
				delete(Flist, k)
				delete(TList, v.Token)
			}
		}
	}
	// Generate Token
	token := md5.Sum([]byte(fmt.Sprintf("%s%d", fileName, rand.Int63()))) // #nosec G401 G404
	e.Ctx.Value(PlanReplayerFileList).(fileList).FileInfo[fileName] = fileInfo{StartTime: startTime, Token: token}
	e.Ctx.Value(PlanReplayerFileList).(fileList).TokenMap[token] = fileName

	// Create zip writer
	zw := zip.NewWriter(zf)
	defer func() {
		err := zw.Close()
		if err != nil {
			logutil.BgLogger().Warn("Closing zip writer failed.")
		}
		err = zf.Close()
		if err != nil {
			logutil.BgLogger().Warn("Closing zip file failed.")
		}
	}()

	// TODO: DUMP PLAN REPLAYER FILES IN ZIP WRITER
	return hex.EncodeToString(token[:]), nil
}
