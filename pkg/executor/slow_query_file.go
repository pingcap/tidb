// Copyright 2019 PingCAP, Inc.
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
	"bufio"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/memory"
)

type logFile struct {
	file       *os.File   // The opened file handle
	start      types.Time // The start time of the log file
	compressed bool       // The file is compressed or not
}

// getAllFiles is used to get all slow-log needed to parse, it is exported for test.
func (e *slowQueryRetriever) getAllFiles(ctx context.Context, sctx sessionctx.Context, logFilePath string) ([]logFile, error) {
	totalFileNum := 0
	if e.stats != nil {
		startTime := time.Now()
		defer func() {
			e.stats.initialize = time.Since(startTime)
			e.stats.totalFileNum = totalFileNum
		}()
	}
	var logFiles []logFile
	logDir := filepath.Dir(logFilePath)
	ext := filepath.Ext(logFilePath)
	prefix := logFilePath[:len(logFilePath)-len(ext)]
	handleErr := func(err error) error {
		// Ignore the error and append warning for usability.
		if err != io.EOF {
			sctx.GetSessionVars().StmtCtx.AppendWarning(err)
		}
		return nil
	}
	files, err := os.ReadDir(logDir)
	if err != nil {
		return nil, err
	}
	walkFn := func(path string, info os.DirEntry) error {
		if info.IsDir() {
			return nil
		}
		// All rotated log files have the same prefix with the original file.
		if !strings.HasPrefix(path, prefix) {
			return nil
		}
		compressed := strings.HasSuffix(path, ".gz")
		if err2 := ctx.Err(); err2 != nil {
			return err2
		}
		totalFileNum++
		file, err := os.OpenFile(path, os.O_RDONLY, os.ModePerm)
		if err != nil {
			return handleErr(err)
		}
		skip := false
		defer func() {
			if !skip {
				terror.Log(file.Close())
			}
		}()
		// Get the file start time.
		fileStartTime, err := e.getFileStartTime(ctx, file, compressed)
		if err != nil {
			return handleErr(err)
		}
		tz := sctx.GetSessionVars().Location()
		start := types.NewTime(types.FromGoTime(fileStartTime.In(tz)), mysql.TypeDatetime, types.MaxFsp)
		if e.checker.enableTimeCheck {
			notInAllTimeRanges := true
			for _, tr := range e.checker.timeRanges {
				if start.Compare(tr.endTime) <= 0 {
					notInAllTimeRanges = false
					break
				}
			}
			if notInAllTimeRanges {
				return nil
			}
		}

		// If we want to get the end time from a compressed file,
		// we need uncompress the whole file which is very slow and consume a lot of memory.
		if !compressed {
			// Get the file end time.
			fileEndTime, err := e.getFileEndTime(ctx, file)
			if err != nil {
				return handleErr(err)
			}
			if e.checker.enableTimeCheck {
				end := types.NewTime(types.FromGoTime(fileEndTime.In(tz)), mysql.TypeDatetime, types.MaxFsp)
				inTimeRanges := false
				for _, tr := range e.checker.timeRanges {
					if !(start.Compare(tr.endTime) > 0 || end.Compare(tr.startTime) < 0) {
						inTimeRanges = true
						break
					}
				}
				if !inTimeRanges {
					return nil
				}
			}
		}
		_, err = file.Seek(0, io.SeekStart)
		if err != nil {
			return handleErr(err)
		}
		logFiles = append(logFiles, logFile{
			file:       file,
			start:      start,
			compressed: compressed,
		})
		skip = true
		return nil
	}
	for _, file := range files {
		err := walkFn(filepath.Join(logDir, file.Name()), file)
		if err != nil {
			return nil, err
		}
	}
	// Sort by start time
	slices.SortFunc(logFiles, func(i, j logFile) int {
		return i.start.Compare(j.start)
	})
	// Assume no time range overlap in log files and remove unnecessary log files for compressed files.
	var ret []logFile
	for i, file := range logFiles {
		if i == len(logFiles)-1 || !file.compressed || !e.checker.enableTimeCheck {
			ret = append(ret, file)
			continue
		}
		start := logFiles[i].start
		// use next file.start as endTime
		end := logFiles[i+1].start
		inTimeRanges := false
		for _, tr := range e.checker.timeRanges {
			if !(start.Compare(tr.endTime) > 0 || end.Compare(tr.startTime) < 0) {
				inTimeRanges = true
				break
			}
		}
		if inTimeRanges {
			ret = append(ret, file)
		}
	}
	return ret, err
}

func (*slowQueryRetriever) getFileStartTime(ctx context.Context, file *os.File, compressed bool) (time.Time, error) {
	var t time.Time
	_, err := file.Seek(0, io.SeekStart)
	if err != nil {
		return t, err
	}
	var reader *bufio.Reader
	if !compressed {
		reader = bufio.NewReader(file)
	} else {
		gr, err := gzip.NewReader(file)
		if err != nil {
			return t, err
		}
		reader = bufio.NewReader(gr)
	}
	maxNum := 128
	for {
		lineByte, err := getOneLine(reader)
		if err != nil {
			return t, err
		}
		line := string(lineByte)
		if strings.HasPrefix(line, variable.SlowLogStartPrefixStr) {
			return ParseTime(line[len(variable.SlowLogStartPrefixStr):])
		}
		maxNum--
		if maxNum <= 0 {
			break
		}
		if err2 := ctx.Err(); err2 != nil {
			return t, err2
		}
	}
	return t, errors.Errorf("malform slow query file %v", file.Name())
}

func (e *slowQueryRetriever) getRuntimeStats() execdetails.RuntimeStats {
	return e.stats
}

type slowQueryRuntimeStats struct {
	totalFileNum int
	readFileNum  int
	readFile     time.Duration
	initialize   time.Duration
	readFileSize int64
	parseLog     int64
	concurrent   int
}

// String implements the RuntimeStats interface.
func (s *slowQueryRuntimeStats) String() string {
	return fmt.Sprintf("initialize: %s, read_file: %s, parse_log: {time:%s, concurrency:%v}, total_file: %v, read_file: %v, read_size: %s",
		execdetails.FormatDuration(s.initialize), execdetails.FormatDuration(s.readFile),
		execdetails.FormatDuration(time.Duration(s.parseLog)), s.concurrent,
		s.totalFileNum, s.readFileNum, memory.FormatBytes(s.readFileSize))
}

// Merge implements the RuntimeStats interface.
func (s *slowQueryRuntimeStats) Merge(rs execdetails.RuntimeStats) {
	tmp, ok := rs.(*slowQueryRuntimeStats)
	if !ok {
		return
	}
	s.totalFileNum += tmp.totalFileNum
	s.readFileNum += tmp.readFileNum
	s.readFile += tmp.readFile
	s.initialize += tmp.initialize
	s.readFileSize += tmp.readFileSize
	s.parseLog += tmp.parseLog
}

// Clone implements the RuntimeStats interface.
func (s *slowQueryRuntimeStats) Clone() execdetails.RuntimeStats {
	newRs := *s
	return &newRs
}

// Tp implements the RuntimeStats interface.
func (*slowQueryRuntimeStats) Tp() int {
	return execdetails.TpSlowQueryRuntimeStat
}

func (*slowQueryRetriever) getFileEndTime(ctx context.Context, file *os.File) (time.Time, error) {
	var t time.Time
	var tried int
	stat, err := file.Stat()
	if err != nil {
		return t, err
	}
	endCursor := stat.Size()
	maxLineNum := 128
	for {
		lines, readBytes, err := readLastLines(ctx, file, endCursor)
		if err != nil {
			return t, err
		}
		// read out the file
		if readBytes == 0 {
			break
		}
		endCursor -= int64(readBytes)
		for i := len(lines) - 1; i >= 0; i-- {
			if strings.HasPrefix(lines[i], variable.SlowLogStartPrefixStr) {
				return ParseTime(lines[i][len(variable.SlowLogStartPrefixStr):])
			}
		}
		tried += len(lines)
		if tried >= maxLineNum {
			break
		}
		if err2 := ctx.Err(); err2 != nil {
			return t, err2
		}
	}
	return t, errors.Errorf("invalid slow query file %v", file.Name())
}

const maxReadCacheSize = 1024 * 1024 * 64

// Read lines from the end of a file
// endCursor initial value should be the filesize
func readLastLines(ctx context.Context, file *os.File, endCursor int64) ([]string, int, error) {
	var lines []byte
	var firstNonNewlinePos int
	var cursor = endCursor
	var size int64 = 2048
	for {
		// stop if we are at the beginning
		// check it in the start to avoid read beyond the size
		if cursor <= 0 {
			break
		}
		if size < maxReadCacheSize {
			size = size * 2
		}
		if cursor < size {
			size = cursor
		}
		cursor -= size

		_, err := file.Seek(cursor, io.SeekStart)
		if err != nil {
			return nil, 0, err
		}
		chars := make([]byte, size)
		_, err = file.Read(chars)
		if err != nil {
			return nil, 0, err
		}
		lines = append(chars, lines...) // nozero

		// find first '\n' or '\r'
		for i := range len(chars) - 1 {
			if (chars[i] == '\n' || chars[i] == '\r') && chars[i+1] != '\n' && chars[i+1] != '\r' {
				firstNonNewlinePos = i + 1
				break
			}
		}
		if firstNonNewlinePos > 0 {
			break
		}
		if err2 := ctx.Err(); err2 != nil {
			return nil, 0, err2
		}
	}
	finalStr := string(lines[firstNonNewlinePos:])
	return strings.Split(strings.ReplaceAll(finalStr, "\r\n", "\n"), "\n"), len(finalStr), nil
}
