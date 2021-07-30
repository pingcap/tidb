// Copyright 2018 PingCAP, Inc.
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

package executor

import (
	"archive/zip"
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
)

const recreatorPath string = "/tmp/recreator"

// TTL of plan recreator files
const remainedInterval float64 = 3

// PlanRecreatorInfo saves the information of plan recreator operation.
type PlanRecreatorInfo interface {
	// Process dose the export/import work for reproducing sql queries.
	Process() (string, error)
}

// PlanRecreatorSingleExec represents a plan recreator executor.
type PlanRecreatorSingleExec struct {
	baseExecutor
	info *PlanRecreatorSingleInfo
}

// PlanRecreatorSingleInfo saves the information of plan recreator operation.
type PlanRecreatorSingleInfo struct {
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

// planRecreatorVarKeyType is a dummy type to avoid naming collision in context.
type planRecreatorVarKeyType int

// String defines a Stringer function for debugging and pretty printing.
func (k planRecreatorVarKeyType) String() string {
	return "plan_recreator_var"
}

// planRecreatorFileListType is a dummy type to avoid naming collision in context.
type planRecreatorFileListType int

// String defines a Stringer function for debugging and pretty printing.
func (k planRecreatorFileListType) String() string {
	return "plan_recreator_file_list"
}

// PlanRecreatorVarKey is a variable key for plan recreator.
const PlanRecreatorVarKey planRecreatorVarKeyType = 0

// PlanRecreatorFileList is a variable key for plan recreator's file list.
const PlanRecreatorFileList planRecreatorFileListType = 0

// Next implements the Executor Next interface.
func (e *PlanRecreatorSingleExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.GrowAndReset(e.maxChunkSize)
	if e.info.ExecStmt == nil {
		return errors.New("plan Recreator: sql is empty")
	}
	val := e.ctx.Value(PlanRecreatorVarKey)
	if val != nil {
		e.ctx.SetValue(PlanRecreatorVarKey, nil)
		return errors.New("plan Recreator: previous plan recreator option isn't closed normally")
	}
	e.ctx.SetValue(PlanRecreatorVarKey, e.info)
	return nil
}

// Close implements the Executor Close interface.
func (e *PlanRecreatorSingleExec) Close() error {
	return nil
}

// Open implements the Executor Open interface.
func (e *PlanRecreatorSingleExec) Open(ctx context.Context) error {
	return nil
}

// Process dose the export/import work for reproducing sql queries.
func (e *PlanRecreatorSingleInfo) Process() (string, error) {
	// TODO: plan recreator load will be developed later
	if e.Load {
		return "", nil
	}
	return e.dumpSingle()
}

func (e *PlanRecreatorSingleInfo) dumpSingle() (string, error) {
	// Create path
	err := os.MkdirAll(recreatorPath, os.ModePerm)
	if err != nil {
		return "", errors.New("plan Recreator: cannot create plan recreator path")
	}

	// Create zip file
	startTime := time.Now()
	fileName := fmt.Sprintf("recreator_single_%v.zip", startTime.UnixNano())
	zf, err := os.Create(recreatorPath + "/" + fileName)
	if err != nil {
		return "", errors.New("plan Recreator: cannot create zip file")
	}
	val := e.Ctx.Value(PlanRecreatorFileList)
	if val == nil {
		e.Ctx.SetValue(PlanRecreatorFileList, fileList{FileInfo: make(map[string]fileInfo), TokenMap: make(map[[16]byte]string)})
	} else {
		// Clean outdated files
		Flist := val.(fileList).FileInfo
		TList := val.(fileList).TokenMap
		for k, v := range Flist {
			if time.Since(v.StartTime).Minutes() > remainedInterval {
				err := os.Remove(recreatorPath + "/" + k)
				if err != nil {
					logutil.BgLogger().Warn(fmt.Sprintf("Cleaning outdated file %s failed.", k))
				}
				delete(Flist, k)
				delete(TList, v.Token)
			}
		}
	}
	// Generate Token
	token := md5.Sum([]byte(fmt.Sprintf("%s%d", fileName, rand.Int63())))
	e.Ctx.Value(PlanRecreatorFileList).(fileList).FileInfo[fileName] = fileInfo{StartTime: startTime, Token: token}
	e.Ctx.Value(PlanRecreatorFileList).(fileList).TokenMap[token] = fileName

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

	// TODO: DUMP PLAN RECREATOR FILES IN ZIP WRITER
	return hex.EncodeToString(token[:]), nil
}
