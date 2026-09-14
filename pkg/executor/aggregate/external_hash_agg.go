// Copyright 2026 PingCAP, Inc.
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

package aggregate

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/executor/aggfuncs"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/memory"
)

const (
	externalAggPartitions         = 256
	externalAggMinMemory          = 64 << 10
	externalAggFileVersion uint32 = 1
)

// ExternalHashAgg keeps the original aggregate modes and spills state to object
// storage. It does not own the storage or resume state across executions.
// Next is single-consumer; its child can still execute in parallel.
type ExternalHashAgg struct {
	exec.BaseExecutor
	mu            sync.Mutex
	lifeCtx       context.Context
	cancel        context.CancelFunc
	attemptPrefix string
	store         storeapi.Storage
	prefix        string
	memoryLimit   int64
	stateLimit    int64
	batchLimit    int64
	funcs         []aggfuncs.AggFunc
	mergeFuncs    []aggfuncs.AggFunc
	firstRows     []bool
	groupBy       []expression.Expression
	codec         *chunk.Codec
	spillTypes    []*types.FieldType
	serializer    *aggfuncs.SerializeHelper

	states        aggfuncs.AggPartialResultMapper
	keys          []string
	stateBytes    int64
	frameBytes    int64
	tracker       *memory.Tracker
	fileCounts    [externalAggPartitions]int
	nextPartition int
	outputCursor  int
	spilled       bool
	drained       bool
	opened        bool
	terminalErr   error
	input         *chunk.Chunk
	groupKeyBuf   [][]byte
}

// NewExternalHashAgg replaces a local aggregate without changing its input modes or
// output schema. prefix must identify an owned task namespace; each Open adds
// an independent attempt directory. memoryLimit includes serialization reserve.
func NewExternalHashAgg(
	sctx sessionctx.Context, schema *expression.Schema, id int, child exec.Executor,
	descs []*aggregation.AggFuncDesc, groupBy []expression.Expression,
	store storeapi.Storage, prefix string, memoryLimit int64,
) (*ExternalHashAgg, error) {
	if store == nil || memoryLimit < externalAggMinMemory {
		return nil, errors.New("ExternalHashAgg requires object storage and at least 64 KiB of memory")
	}
	if prefix == "" || path.IsAbs(prefix) || path.Clean(prefix) != prefix || strings.HasPrefix(prefix, "../") || prefix == "." {
		return nil, errors.New("ExternalHashAgg requires a relative task prefix")
	}
	if len(descs) != schema.Len() {
		return nil, errors.New("ExternalHashAgg aggregate output does not match schema")
	}
	e := &ExternalHashAgg{
		BaseExecutor: exec.NewBaseExecutor(sctx, schema, id, child),
		store:        store, prefix: path.Join(prefix, "agg"),
		memoryLimit: memoryLimit, stateLimit: memoryLimit / 2,
		batchLimit: min(memoryLimit/8, 1<<20),
		groupBy:    groupBy, serializer: aggfuncs.NewSerializeHelper(),
	}
	for i, desc := range descs {
		if desc.HasDistinct || len(desc.OrderByItems) != 0 {
			return nil, errors.Errorf("ExternalHashAgg does not support DISTINCT or ORDER BY in %s", desc.Name)
		}
		switch desc.Mode {
		case aggregation.CompleteMode, aggregation.FinalMode, aggregation.Partial1Mode, aggregation.Partial2Mode:
		default:
			return nil, errors.Errorf("ExternalHashAgg does not support aggregate mode %d", desc.Mode)
		}
		switch desc.Name {
		case ast.AggFuncCount, ast.AggFuncSum, ast.AggFuncSumInt, ast.AggFuncMin, ast.AggFuncMax, ast.AggFuncFirstRow:
		default:
			return nil, errors.Errorf("ExternalHashAgg does not support aggregate %s", desc.Name)
		}
		fn := aggfuncs.Build(sctx.GetExprCtx(), desc, i)
		mergeDesc := desc.Clone()
		// COUNT's CompleteMode implementation updates rows; its FinalMode
		// implementation also supplies the merge operation for spilled counts.
		if mergeDesc.Name == ast.AggFuncCount {
			mergeDesc.Mode = aggregation.FinalMode
		}
		mergeFn := aggfuncs.Build(sctx.GetExprCtx(), mergeDesc, i)
		if fn == nil || mergeFn == nil {
			return nil, errors.Errorf("ExternalHashAgg cannot build aggregate %s", desc.Name)
		}
		e.funcs = append(e.funcs, fn)
		e.firstRows = append(e.firstRows, desc.Name == ast.AggFuncFirstRow)
		e.mergeFuncs = append(e.mergeFuncs, mergeFn)
		e.spillTypes = append(e.spillTypes, types.NewFieldType(mysql.TypeVarString))
	}
	e.spillTypes = append(e.spillTypes, types.NewFieldType(mysql.TypeVarString))
	e.codec = chunk.NewCodec(e.spillTypes)
	return e, nil
}

// Open initializes the input and a fresh, bounded aggregate state.
func (e *ExternalHashAgg) Open(ctx context.Context) error {
	if err := e.OpenSelf(ctx); err != nil {
		return err
	}
	return e.BaseExecutor.Open(ctx)
}

// OpenSelf initializes aggregation state without reopening an already-open child.
func (e *ExternalHashAgg) OpenSelf(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.opened {
		return errors.New("ExternalHashAgg is already open")
	}
	e.opened = true
	e.lifeCtx, e.cancel = context.WithCancel(ctx)
	e.attemptPrefix = path.Join(e.prefix, uuid.NewString())
	e.terminalErr = nil
	e.drained, e.spilled = false, false
	e.fileCounts = [externalAggPartitions]int{}
	e.nextPartition, e.outputCursor = 0, 0
	e.states = aggfuncs.NewAggPartialResultMapper()
	e.keys = nil
	e.stateBytes, e.frameBytes = 0, 0
	e.tracker = memory.NewTracker(e.ID(), -1)
	e.tracker.AttachTo(e.Ctx().GetSessionVars().StmtCtx.MemTracker)
	e.input = chunk.New(exec.RetTypes(e.Children(0)), e.InitCap(), e.MaxChunkSize())
	return nil
}

func (e *ExternalHashAgg) consumeState(n int64) {
	e.stateBytes += n
	e.tracker.Consume(n)
}

func (e *ExternalHashAgg) getState(key string) []aggfuncs.PartialResult {
	if state, ok := e.states.M[key]; ok {
		return state
	}
	state := make([]aggfuncs.PartialResult, len(e.funcs))
	delta := int64(len(key) + 8*len(state))
	for i, fn := range e.funcs {
		var n int64
		state[i], n = fn.AllocPartialResult()
		delta += n
	}
	delta += e.states.Set(key, state)
	oldCap := cap(e.keys)
	e.keys = append(e.keys, key)
	delta += int64(cap(e.keys)-oldCap) * 16
	e.consumeState(delta)
	return state
}

func (e *ExternalHashAgg) clearState() {
	e.states = aggfuncs.NewAggPartialResultMapper()
	e.keys = nil
	e.tracker.Consume(-e.stateBytes)
	e.stateBytes = 0
	e.outputCursor = 0
}

func externalAggPartition(key string) int {
	return int(crc32.ChecksumIEEE([]byte(key)) % externalAggPartitions)
}

func (e *ExternalHashAgg) fileName(partition, seq int) string {
	return path.Join(e.attemptPrefix, fmt.Sprintf("%03d/%08d", partition, seq))
}

func (e *ExternalHashAgg) writeBatch(ctx context.Context, partition int, batch *chunk.Chunk) error {
	if batch.NumRows() == 0 {
		return nil
	}
	data := e.codec.Encode(batch)
	if e.stateBytes+e.frameBytes+2*int64(cap(data))+batch.MemoryUsage()+4 > e.memoryLimit {
		return errors.New("ExternalHashAgg serialization exceeds memory budget")
	}
	payload := make([]byte, 8, 8+len(data))
	binary.LittleEndian.PutUint32(payload, externalAggFileVersion)
	binary.LittleEndian.PutUint32(payload[4:], crc32.ChecksumIEEE(data))
	payload = append(payload, data...)
	temporaryBytes := int64(cap(payload)+cap(data)) + batch.MemoryUsage()
	e.tracker.Consume(temporaryBytes)
	defer e.tracker.Consume(-temporaryBytes)
	seq := e.fileCounts[partition]
	// Count the attempted key as well, so Close also cleans up ambiguous writes.
	e.fileCounts[partition]++
	if err := e.store.WriteFile(ctx, e.fileName(partition, seq), payload); err != nil {
		return errors.Annotate(err, "write ExternalHashAgg state")
	}
	batch.Reset()
	return nil
}

func (e *ExternalHashAgg) spill(ctx context.Context) error {
	e.spilled = true
	sort.Slice(e.keys, func(i, j int) bool { return externalAggPartition(e.keys[i]) < externalAggPartition(e.keys[j]) })
	batch := chunk.New(e.spillTypes, 1, e.MaxChunkSize())
	partition := -1
	for _, key := range e.keys {
		if err := ctx.Err(); err != nil {
			return err
		}
		p := externalAggPartition(key)
		if partition != -1 && p != partition {
			if err := e.writeBatch(ctx, partition, batch); err != nil {
				return err
			}
		}
		partition = p
		for i, fn := range e.funcs {
			fn.SerializePartialResult(e.states.M[key][i], batch, e.serializer)
		}
		batch.AppendString(len(e.funcs), key)
		if batch.NumRows() >= e.MaxChunkSize() || batch.MemoryUsage() >= e.batchLimit {
			if err := e.writeBatch(ctx, partition, batch); err != nil {
				return err
			}
		}
	}
	if err := e.writeBatch(ctx, partition, batch); err != nil {
		return err
	}
	e.clearState()
	return nil
}

func (e *ExternalHashAgg) readInput(ctx context.Context) error {
	evalCtx := e.Ctx().GetExprCtx().GetEvalCtx()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		e.tracker.Consume(-e.frameBytes)
		e.frameBytes = 0
		e.input.Reset()
		if err := exec.Next(ctx, e.Children(0), e.input); err != nil {
			return err
		}
		if e.input.NumRows() == 0 {
			break
		}
		var err error
		e.groupKeyBuf, err = GetGroupKey(e.Ctx(), e.input, e.groupKeyBuf, e.groupBy)
		if err != nil {
			return err
		}
		e.frameBytes = e.input.MemoryUsage() + int64(cap(e.groupKeyBuf))*24
		for _, key := range e.groupKeyBuf {
			e.frameBytes += int64(cap(key))
		}
		e.tracker.Consume(e.frameBytes)
		if e.frameBytes > e.memoryLimit/4 {
			return errors.New("ExternalHashAgg input buffer exceeds memory budget")
		}
		for i := range e.input.NumRows() {
			state := e.getState(string(e.groupKeyBuf[i]))
			rows := []chunk.Row{e.input.GetRow(i)}
			for j, fn := range e.funcs {
				delta, err := fn.UpdatePartialResult(evalCtx, rows, state[j])
				if err != nil {
					return err
				}
				e.consumeState(delta)
			}
			if e.stateBytes >= e.stateLimit {
				if err := e.spill(ctx); err != nil {
					return err
				}
			}
		}
	}
	e.tracker.Consume(-e.frameBytes)
	e.frameBytes = 0
	e.input, e.groupKeyBuf = nil, nil
	if e.spilled {
		return e.spill(ctx)
	}
	if len(e.keys) == 0 && len(e.groupBy) == 0 {
		e.getState("")
	}
	return nil
}

func (e *ExternalHashAgg) restore(ctx context.Context, partition int) error {
	for seq := range e.fileCounts[partition] {
		if err := ctx.Err(); err != nil {
			return err
		}
		data, err := e.store.ReadFile(ctx, e.fileName(partition, seq))
		if err != nil {
			return errors.Annotate(err, "read ExternalHashAgg state")
		}
		if err := e.restoreBatch(data); err != nil {
			return err
		}
	}
	return nil
}

func (e *ExternalHashAgg) restoreBatch(data []byte) (err error) {
	var temporaryBytes int64
	defer func() {
		e.tracker.Consume(-temporaryBytes)
		if r := recover(); r != nil {
			err = errors.Errorf("invalid ExternalHashAgg state: %v", r)
		}
	}()
	if len(data) < 8 || binary.LittleEndian.Uint32(data) != externalAggFileVersion ||
		binary.LittleEndian.Uint32(data[4:]) != crc32.ChecksumIEEE(data[8:]) {
		return errors.New("invalid ExternalHashAgg state header or checksum")
	}
	if int64(len(data))+e.stateBytes > e.memoryLimit {
		return errors.New("ExternalHashAgg restore exceeds memory budget")
	}
	batch, remaining := e.codec.Decode(data[8:])
	if len(remaining) != 0 || batch.NumCols() != len(e.spillTypes) {
		return errors.New("invalid ExternalHashAgg state layout")
	}
	temporaryBytes = int64(cap(data)) + batch.MemoryUsage()
	e.tracker.Consume(temporaryBytes)
	partials := make([][]aggfuncs.PartialResult, len(e.funcs))
	for i, fn := range e.funcs {
		var delta int64
		partials[i], delta = fn.DeserializePartialResult(batch)
		temporaryBytes += delta + int64(cap(partials[i]))*8
		e.tracker.Consume(delta + int64(cap(partials[i]))*8)
		if len(partials[i]) != batch.NumRows() {
			return errors.New("invalid ExternalHashAgg state rows")
		}
	}
	evalCtx := e.Ctx().GetExprCtx().GetEvalCtx()
	for i := range batch.NumRows() {
		key := batch.GetRow(i).GetString(len(e.funcs))
		_, existed := e.states.M[key]
		state := e.getState(strings.Clone(key))
		for j, fn := range e.mergeFuncs {
			if partials[j][i] == nil {
				return errors.New("invalid ExternalHashAgg aggregate state")
			}
			delta, err := fn.MergePartialResult(evalCtx, partials[j][i], state[j])
			if err != nil {
				return err
			}
			e.consumeState(delta)
			// FIRST_ROW transfers dynamic values without reporting their size.
			if !existed && e.firstRows[j] {
				e.consumeState(int64(len(batch.GetRow(i).GetBytes(j))))
			}
		}
		if e.stateBytes > e.stateLimit || e.stateBytes+temporaryBytes > e.memoryLimit {
			return errors.New("ExternalHashAgg restored partition exceeds memory budget")
		}
	}
	return nil
}

// Next returns final rows. Once an input or spill error occurs it stays terminal.
func (e *ExternalHashAgg) Next(ctx context.Context, out *chunk.Chunk) (err error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	out.Reset()
	if !e.opened {
		return errors.New("ExternalHashAgg is not open")
	}
	if e.terminalErr != nil {
		return e.terminalErr
	}
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(e.lifeCtx, cancel)
	defer func() { stop(); cancel() }()
	defer func() {
		if err != nil {
			e.terminalErr = err
		}
	}()
	if !e.drained {
		if err := e.readInput(ctx); err != nil {
			return err
		}
		e.drained = true
	}
	for out.NumRows() < e.MaxChunkSize() {
		if err := ctx.Err(); err != nil {
			return err
		}
		if e.outputCursor == len(e.keys) {
			if !e.spilled {
				return nil
			}
			e.clearState()
			for e.nextPartition < externalAggPartitions && e.fileCounts[e.nextPartition] == 0 {
				e.nextPartition++
			}
			if e.nextPartition == externalAggPartitions {
				return nil
			}
			if err := e.restore(ctx, e.nextPartition); err != nil {
				return err
			}
			e.nextPartition++
		}
		key := e.keys[e.outputCursor]
		for i, fn := range e.funcs {
			if err := fn.AppendFinalResult2Chunk(e.Ctx().GetExprCtx().GetEvalCtx(), e.states.M[key][i], out); err != nil {
				return err
			}
		}
		e.outputCursor++
	}
	return nil
}

// Close removes only this execution's state objects and releases its child.
func (e *ExternalHashAgg) Close() error {
	if e.cancel != nil {
		e.cancel()
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.opened {
		return nil
	}
	e.opened = false
	cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	var firstErr error
	for partition, count := range e.fileCounts {
		for seq := range count {
			name := e.fileName(partition, seq)
			if err := e.store.DeleteFile(cleanupCtx, name); err != nil && firstErr == nil {
				exists, checkErr := e.store.FileExists(cleanupCtx, name)
				if checkErr != nil || exists {
					firstErr = err
				}
			}
		}
	}
	e.clearState()
	e.tracker.Consume(-e.frameBytes)
	e.frameBytes = 0
	e.tracker.Detach()
	e.input, e.groupKeyBuf = nil, nil
	if err := e.BaseExecutor.Close(); firstErr == nil {
		firstErr = err
	}
	return firstErr
}
