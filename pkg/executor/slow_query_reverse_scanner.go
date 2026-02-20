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
	"io"
	"os"
	"strings"
	"time"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/intest"
)

type slowLogReverseScanner struct {
	e  *slowQueryRetriever
	tz *time.Location

	file      *logFile
	endCursor int64

	// For compressed files we can only scan forward. We read all blocks and then
	// return them in reverse order.
	compressedBlocks []slowLogBlock
	compressedIdx    int

	// pendingBlock stores a log block in reverse order while scanning backwards.
	pendingBlock        []string
	pendingHasSQLSuffix bool

	cachedLines []string
	cachedIdx   int

	minStartTime types.Time
	hasMinStart  bool
	finished     bool
}

func newSlowLogReverseScanner(e *slowQueryRetriever, sctx sessionctx.Context) *slowLogReverseScanner {
	tz := sctx.GetSessionVars().Location()
	scanner := &slowLogReverseScanner{
		e:            e,
		tz:           tz,
		pendingBlock: make([]string, 0, 8),
		cachedIdx:    -1,
	}
	if e.checker != nil && e.checker.enableTimeCheck && len(e.checker.timeRanges) > 0 {
		minStart := e.checker.timeRanges[0].startTime
		for _, tr := range e.checker.timeRanges[1:] {
			if tr.startTime.Compare(minStart) < 0 {
				minStart = tr.startTime
			}
		}
		scanner.minStartTime = minStart
		scanner.hasMinStart = true
	}
	return scanner
}

// DashboardSlowLogReadBlockCnt4Test is only used in tests
var DashboardSlowLogReadBlockCnt4Test int

func (s *slowLogReverseScanner) nextBatch(ctx context.Context, batchSize uint64) ([]string, error) {
	if s.finished {
		return nil, nil
	}

	blocks := make([]slowLogBlock, 0, batchSize)
	for uint64(len(blocks)) < batchSize {
		block, err := s.nextBlock(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if intest.InTest {
			DashboardSlowLogReadBlockCnt4Test++
		}
		if s.hasMinStart && len(block) > 0 && strings.HasPrefix(block[0], variable.SlowLogStartPrefixStr) {
			t, err := ParseTime(block[0][len(variable.SlowLogStartPrefixStr):])
			if err == nil {
				timeValue := types.NewTime(types.FromGoTime(t.In(s.tz)), mysql.TypeDatetime, types.MaxFsp)
				if timeValue.Compare(s.minStartTime) < 0 {
					s.finished = true
					break
				}
			}
		}
		blocks = append(blocks, block)
	}

	if len(blocks) == 0 {
		return nil, nil
	}

	lines := make([]string, 0, len(blocks)*len(blocks[0]))
	for _, block := range blocks {
		lines = append(lines, block...) // nozero
	}
	return lines, nil
}

func (s *slowLogReverseScanner) nextBlock(ctx context.Context) (slowLogBlock, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		if s.compressedBlocks != nil {
			if s.compressedIdx < 0 {
				s.compressedBlocks = nil
				s.compressedIdx = 0
				s.file = nil
				continue
			}
			block := s.compressedBlocks[s.compressedIdx]
			s.compressedIdx--
			hasSQLSuffix := false
			for _, line := range block {
				if strings.HasSuffix(line, variable.SlowLogSQLSuffixStr) {
					if strings.HasPrefix(line, "use") || strings.HasPrefix(line, variable.SlowLogRowPrefixStr) {
						continue
					}
					hasSQLSuffix = true
					break
				}
			}
			if !hasSQLSuffix {
				if !s.pendingHasSQLSuffix {
					continue
				}
				merged := make(slowLogBlock, 0, len(block)+len(s.pendingBlock))
				merged = append(merged, block...)
				for j := len(s.pendingBlock) - 1; j >= 0; j-- {
					merged = append(merged, s.pendingBlock[j])
				}
				s.pendingBlock = s.pendingBlock[:0]
				s.pendingHasSQLSuffix = false
				return merged, nil
			}
			return block, nil
		}

		if s.file == nil {
			file := s.e.getNextFile()
			if file == nil {
				return nil, io.EOF
			}
			s.file = file
			s.cachedLines = nil
			s.cachedIdx = -1
			if file.compressed {
				if err := s.loadCompressedBlocks(ctx, file.file); err != nil {
					return nil, err
				}
				s.compressedIdx = len(s.compressedBlocks) - 1
				continue
			}
			stat, err := file.file.Stat()
			if err != nil {
				return nil, err
			}
			s.endCursor = stat.Size()
		}

		if s.cachedIdx < 0 {
			lines, readBytes, err := readLastLines(ctx, s.file.file, s.endCursor)
			if err != nil {
				return nil, err
			}
			if readBytes == 0 {
				// Reach the beginning of the current file.
				s.file = nil
				s.endCursor = 0
				s.cachedLines = nil
				s.cachedIdx = -1
				continue
			}
			s.endCursor -= int64(readBytes)
			s.cachedLines = lines
			s.cachedIdx = len(lines) - 1
			continue
		}

		line := s.cachedLines[s.cachedIdx]
		s.cachedIdx--
		if len(s.pendingBlock) == 0 && len(line) == 0 {
			continue
		}
		s.pendingBlock = append(s.pendingBlock, line)
		if strings.HasSuffix(line, variable.SlowLogSQLSuffixStr) {
			if strings.HasPrefix(line, "use") || strings.HasPrefix(line, variable.SlowLogRowPrefixStr) {
				continue
			}
			s.pendingHasSQLSuffix = true
		}
		// revert the block when we meet the start prefix. In reverse scanning, start
		// prefix means the end of a block.
		if strings.HasPrefix(line, variable.SlowLogStartPrefixStr) {
			if !s.pendingHasSQLSuffix {
				s.pendingBlock = s.pendingBlock[:0]
				s.pendingHasSQLSuffix = false
				continue
			}
			block := make(slowLogBlock, len(s.pendingBlock))
			for j := range s.pendingBlock {
				block[j] = s.pendingBlock[len(s.pendingBlock)-1-j]
			}
			s.pendingBlock = s.pendingBlock[:0]
			s.pendingHasSQLSuffix = false
			return block, nil
		}
	}
}

func (s *slowLogReverseScanner) loadCompressedBlocks(ctx context.Context, file *os.File) error {
	_, err := file.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}
	gr, err := gzip.NewReader(file)
	if err != nil {
		return err
	}
	defer func() { _ = gr.Close() }()

	reader := bufio.NewReader(gr)
	blocks := make([]slowLogBlock, 0)
	var (
		block        slowLogBlock
		hasStartFlag bool
	)
	for {
		if err2 := ctx.Err(); err2 != nil {
			return err2
		}
		lineByte, err := getOneLine(reader)
		if err != nil {
			// TODO(lance6716): only load one needed blocks to reduce memory usage.
			if err == io.EOF {
				if len(block) > 0 {
					blocks = append(blocks, block)
				}
				s.compressedBlocks = blocks
				return nil
			}
			return err
		}
		line := string(hack.String(lineByte))
		if strings.HasPrefix(line, variable.SlowLogStartPrefixStr) {
			if hasStartFlag {
				block = block[:0]
			} else {
				hasStartFlag = true
			}
		}
		if hasStartFlag {
			block = append(block, line)
			if strings.HasSuffix(line, variable.SlowLogSQLSuffixStr) {
				if strings.HasPrefix(line, "use") || strings.HasPrefix(line, variable.SlowLogRowPrefixStr) {
					continue
				}
				blocks = append(blocks, block)
				block = make(slowLogBlock, 0, 8)
				hasStartFlag = false
			}
		}
	}
}
