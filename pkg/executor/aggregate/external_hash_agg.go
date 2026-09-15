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
	"strings"
	"sync"

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
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/memory"
)

const (
	externalAggPartitions           = 256
	externalAggMinMemory            = 64 << 10
	externalAggIOConcurrency        = 32
	externalAggFileVersion   uint32 = 2
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
	fixedStates   bool
	fixedFuncs    []bool
	directChunks  [externalAggPartitions][]*chunk.Chunk
	directState   []aggfuncs.PartialResult
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
	maxFileSizes  [externalAggPartitions]int
	readObjects   [externalAggIOConcurrency][]byte
	readCursor    int
	readCount     int
	readPartition int
	readSequence  int
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
		batchLimit: min(memoryLimit/128, 256<<10),
		groupBy:    groupBy, serializer: aggfuncs.NewSerializeHelper(),
		fixedStates: true,
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
		fixed := true
		switch desc.RetTp.EvalType() {
		case types.ETInt, types.ETReal, types.ETDecimal, types.ETDatetime, types.ETTimestamp, types.ETDuration:
		default:
			fixed = false
		}
		e.fixedFuncs = append(e.fixedFuncs, fixed)
		e.fixedStates = e.fixedStates && fixed
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
	e.maxFileSizes = [externalAggPartitions]int{}
	e.readObjects = [externalAggIOConcurrency][]byte{}
	e.readCursor, e.readCount, e.readPartition, e.readSequence = 0, 0, 0, 0
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
	var delta int64
	for i, fn := range e.funcs {
		var n int64
		state[i], n = fn.AllocPartialResult()
		delta += n
	}
	e.addState(key, state, delta)
	return state
}

func (e *ExternalHashAgg) addState(key string, state []aggfuncs.PartialResult, delta int64) {
	delta += int64(len(key) + 8*len(state))
	delta += e.states.Set(key, state)
	oldCap := cap(e.keys)
	e.keys = append(e.keys, key)
	delta += int64(cap(e.keys)-oldCap) * 16
	e.consumeState(delta)
}

func (e *ExternalHashAgg) clearState() {
	e.states = aggfuncs.NewAggPartialResultMapper()
	e.keys = nil
	e.directChunks = [externalAggPartitions][]*chunk.Chunk{}
	e.directState = nil
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

type externalAggSpillObject struct {
	name string
	data []byte
}

// groupKeysByPartition uses fixed-size counting buckets. Each key is placed once;
// keys within a partition need no ordering because the state map has unique keys.
func (e *ExternalHashAgg) groupKeysByPartition() {
	var ends, next [externalAggPartitions]int
	for _, key := range e.keys {
		ends[externalAggPartition(key)]++
	}
	total := 0
	for p, count := range ends {
		next[p] = total
		total += count
		ends[p] = total
	}
	for p, end := range ends {
		for next[p] < end {
			dst := externalAggPartition(e.keys[next[p]])
			e.keys[next[p]], e.keys[next[dst]] = e.keys[next[dst]], e.keys[next[p]]
			next[dst]++
		}
	}
}

func (e *ExternalHashAgg) writeObjects(ctx context.Context, objects []externalAggSpillObject) error {
	group, writeCtx := util.NewErrorGroupWithRecoverWithCtx(ctx)
	for _, object := range objects {
		group.Go(func() error {
			return errors.Annotate(e.store.WriteFile(writeCtx, object.name, object.data), "write ExternalHashAgg state")
		})
	}
	return group.Wait()
}

func (e *ExternalHashAgg) spill(ctx context.Context) error {
	e.spilled = true
	e.groupKeysByPartition()
	objectLimit := int(min(e.memoryLimit/32, 8<<20))
	uploadBudget := e.memoryLimit / 8
	batch := chunk.New(e.spillTypes, 1, e.MaxChunkSize())
	batchBytes := batch.MemoryUsage()
	e.tracker.Consume(batchBytes)
	var payload []byte
	var objects [externalAggIOConcurrency]externalAggSpillObject
	objectCount := 0
	var uploadBytes int64
	partition := -1
	defer func() {
		e.tracker.Consume(-int64(cap(payload)) - batchBytes - uploadBytes)
	}()
	flushObjects := func() error {
		err := e.writeObjects(ctx, objects[:objectCount])
		clear(objects[:objectCount])
		objectCount = 0
		e.tracker.Consume(-uploadBytes)
		uploadBytes = 0
		return err
	}
	writeObject := func() error {
		if len(payload) == 0 {
			return nil
		}
		size := int64(cap(payload))
		if objectCount > 0 && (objectCount == len(objects) || uploadBytes+size > uploadBudget) {
			if err := flushObjects(); err != nil {
				return err
			}
		}
		seq := e.fileCounts[partition]
		// Sequence reflects input order, regardless of upload completion order.
		e.fileCounts[partition]++
		e.maxFileSizes[partition] = max(e.maxFileSizes[partition], len(payload))
		objects[objectCount] = externalAggSpillObject{name: e.fileName(partition, seq), data: payload}
		objectCount++
		uploadBytes += size
		payload = nil
		return nil
	}
	writeBatch := func(batch *chunk.Chunk) error {
		if batch.NumRows() == 0 {
			return nil
		}
		data := e.codec.Encode(batch)
		e.tracker.Consume(int64(cap(data)))
		defer e.tracker.Consume(-int64(cap(data)))
		// Each object has a version followed by length/checksum/chunk frames.
		// SQL chunk size controls decoding work, not the number of S3 objects.
		if len(payload)+8+len(data) > objectLimit {
			if err := writeObject(); err != nil {
				return err
			}
		}
		required := len(payload) + 8 + len(data)
		if payload == nil {
			required += 4
		}
		if required > cap(payload) {
			// Grow with the data: small partition tails should not allocate the full
			// object limit. Account for both buffers while copying during growth.
			size := max(required, min(objectLimit, max(64<<10, 2*cap(payload))))
			if e.tracker.BytesConsumed()+int64(size) > e.memoryLimit {
				return errors.New("ExternalHashAgg serialization exceeds memory budget")
			}
			next := make([]byte, len(payload), size)
			e.tracker.Consume(int64(cap(next)))
			copy(next, payload)
			e.tracker.Consume(-int64(cap(payload)))
			payload = next
		}
		if len(payload) == 0 {
			payload = binary.LittleEndian.AppendUint32(payload, externalAggFileVersion)
		}
		if e.tracker.BytesConsumed() > e.memoryLimit {
			return errors.New("ExternalHashAgg serialization exceeds memory budget")
		}
		payload = binary.LittleEndian.AppendUint32(payload, uint32(len(data)))
		payload = binary.LittleEndian.AppendUint32(payload, crc32.ChecksumIEEE(data))
		payload = append(payload, data...)
		return nil
	}
	for _, key := range e.keys {
		if err := ctx.Err(); err != nil {
			return err
		}
		p := externalAggPartition(key)
		if partition != -1 && p != partition {
			if err := writeBatch(batch); err != nil {
				return err
			}
			batch.Reset()
			if err := writeObject(); err != nil {
				return err
			}
		}
		partition = p
		for i, fn := range e.funcs {
			fn.SerializePartialResult(e.states.M[key][i], batch, e.serializer)
		}
		batch.AppendString(len(e.funcs), key)
		nextBatchBytes := batch.MemoryUsage()
		e.tracker.Consume(nextBatchBytes - batchBytes)
		batchBytes = nextBatchBytes
		if batch.NumRows() >= e.MaxChunkSize() || batch.UsedMemoryUsage() >= e.batchLimit {
			if err := writeBatch(batch); err != nil {
				return err
			}
			batch.Reset()
		}
	}
	if err := writeBatch(batch); err != nil {
		return err
	}
	if err := writeObject(); err != nil {
		return err
	}
	for p, chunks := range e.directChunks {
		partition = p
		for _, buffered := range chunks {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := writeBatch(buffered); err != nil {
				return err
			}
		}
		if err := writeObject(); err != nil {
			return err
		}
	}
	if err := flushObjects(); err != nil {
		return err
	}
	e.clearState()
	return nil
}

// bufferRow reuses fixed-size states when preaggregation does not reduce rows.
// Expressions still execute in input order; only their serialized states survive.
func (e *ExternalHashAgg) bufferRow(rows []chunk.Row, key []byte) error {
	if e.directState == nil {
		e.directState = make([]aggfuncs.PartialResult, len(e.funcs))
		e.consumeState(int64(len(e.directState)) * 8)
		for i, fn := range e.funcs {
			if e.fixedFuncs[i] {
				state, delta := fn.AllocPartialResult()
				e.directState[i] = state
				e.consumeState(delta)
			}
		}
	}
	p := externalAggPartition(string(key))
	chunks := e.directChunks[p]
	var batch *chunk.Chunk
	if len(chunks) > 0 {
		batch = chunks[len(chunks)-1]
	}
	if batch == nil || batch.NumRows() >= e.MaxChunkSize() || batch.UsedMemoryUsage() >= e.batchLimit {
		batch = chunk.New(e.spillTypes, 1, e.MaxChunkSize())
		oldCap := cap(chunks)
		e.directChunks[p] = append(chunks, batch)
		e.consumeState(batch.MemoryUsage() + int64(cap(e.directChunks[p])-oldCap)*8)
	}
	before := batch.MemoryUsage()
	evalCtx := e.Ctx().GetExprCtx().GetEvalCtx()
	for i, fn := range e.funcs {
		state := e.directState[i]
		var temporaryBytes int64
		if e.fixedFuncs[i] {
			fn.ResetPartialResult(state)
		} else {
			// ResetPartialResult can retain variable-size values. Use a fresh
			// temporary state so retained data cannot escape memory accounting.
			state, temporaryBytes = fn.AllocPartialResult()
			e.consumeState(temporaryBytes)
		}
		delta, err := fn.UpdatePartialResult(evalCtx, rows, state)
		e.consumeState(delta)
		temporaryBytes += delta
		if err != nil {
			return err
		}
		fn.SerializePartialResult(state, batch, e.serializer)
		e.consumeState(-temporaryBytes)
	}
	batch.AppendBytes(len(e.funcs), key)
	e.consumeState(batch.MemoryUsage() - before)
	return nil
}

func (e *ExternalHashAgg) readInput(ctx context.Context) error {
	evalCtx := e.Ctx().GetExprCtx().GetEvalCtx()
	rows := make([]chunk.Row, 1)
	inputRows, directSpills := 0, 0
	direct := false
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
			rows[0] = e.input.GetRow(i)
			if direct {
				if err := e.bufferRow(rows, e.groupKeyBuf[i]); err != nil {
					return err
				}
				if e.stateBytes >= e.stateLimit {
					if err := e.spill(ctx); err != nil {
						return err
					}
					directSpills++
					// Periodically retry preaggregation if input locality changes.
					if directSpills == 8 {
						direct, directSpills = false, 0
					}
				}
				continue
			}
			inputRows++
			state, exists := e.states.M[string(e.groupKeyBuf[i])]
			if !exists {
				state = e.getState(string(e.groupKeyBuf[i]))
			}
			for j, fn := range e.funcs {
				delta, err := fn.UpdatePartialResult(evalCtx, rows, state[j])
				if err != nil {
					return err
				}
				e.consumeState(delta)
			}
			if e.stateBytes >= e.stateLimit {
				// Avoid allocating one set of states per input row when fewer
				// than 10% of rows combine within an in-memory batch.
				direct = len(e.keys)*10 > inputRows*9
				inputRows = 0
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
	for range e.fileCounts[partition] {
		if e.readCursor == e.readCount {
			if err := e.prefetchObjects(ctx); err != nil {
				return err
			}
		}
		data := e.readObjects[e.readCursor]
		err := e.restoreObject(ctx, data)
		e.tracker.Consume(-int64(cap(data)))
		e.readObjects[e.readCursor] = nil
		e.readCursor++
		if err != nil {
			return err
		}
	}
	return nil
}

// prefetchObjects spans partition boundaries so small partitions can share an
// I/O batch. Unconsumed objects stay accounted until restore or Close releases them.
func (e *ExternalHashAgg) prefetchObjects(ctx context.Context) error {
	e.readCursor, e.readCount = 0, 0
	var names [externalAggIOConcurrency]string
	var reserved int64
	for e.readPartition < externalAggPartitions && e.readCount < len(names) {
		p := e.readPartition
		if e.readSequence == e.fileCounts[p] {
			e.readPartition++
			e.readSequence = 0
			continue
		}
		// ReadFile may overallocate while growing its buffer. A per-partition
		// maximum bounds prefetch without retaining metadata for every file.
		size := max(512, 2*int64(e.maxFileSizes[p]))
		if e.readCount > 0 && reserved+size > e.memoryLimit/4 {
			break
		}
		names[e.readCount] = e.fileName(p, e.readSequence)
		e.readCount++
		e.readSequence++
		reserved += size
	}
	group, readCtx := util.NewErrorGroupWithRecoverWithCtx(ctx)
	for i := range e.readCount {
		group.Go(func() error {
			data, err := e.store.ReadFile(readCtx, names[i])
			e.readObjects[i] = data
			e.tracker.Consume(int64(cap(data)))
			return errors.Annotate(err, "read ExternalHashAgg state")
		})
	}
	return group.Wait()
}

func (e *ExternalHashAgg) restoreObject(ctx context.Context, data []byte) error {
	if len(data) < 12 || binary.LittleEndian.Uint32(data) != externalAggFileVersion {
		return errors.New("invalid ExternalHashAgg state header")
	}
	for data = data[4:]; len(data) != 0; {
		if err := ctx.Err(); err != nil {
			return err
		}
		if len(data) < 8 {
			return errors.New("invalid ExternalHashAgg state frame")
		}
		size := binary.LittleEndian.Uint32(data)
		if uint64(size) > uint64(len(data)-8) {
			return errors.New("invalid ExternalHashAgg state frame size")
		}
		frame := data[8 : 8+int(size)]
		if binary.LittleEndian.Uint32(data[4:]) != crc32.ChecksumIEEE(frame) {
			return errors.New("invalid ExternalHashAgg state checksum")
		}
		if err := e.restoreBatch(frame); err != nil {
			return err
		}
		data = data[8+int(size):]
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
	if e.tracker.BytesConsumed() > e.memoryLimit {
		return errors.New("ExternalHashAgg restore exceeds memory budget")
	}
	batch, remaining := e.codec.Decode(data)
	if len(remaining) != 0 || batch.NumCols() != len(e.spillTypes) {
		return errors.New("invalid ExternalHashAgg state layout")
	}
	temporaryBytes = batch.MemoryUsage()
	e.tracker.Consume(temporaryBytes)
	partials := make([][]aggfuncs.PartialResult, len(e.funcs))
	var partialBytes int64
	for i, fn := range e.funcs {
		var delta int64
		partials[i], delta = fn.DeserializePartialResult(batch)
		partialBytes += delta
		temporaryBytes += delta + int64(cap(partials[i]))*8
		e.tracker.Consume(delta + int64(cap(partials[i]))*8)
		if len(partials[i]) != batch.NumRows() {
			return errors.New("invalid ExternalHashAgg state rows")
		}
	}
	evalCtx := e.Ctx().GetExprCtx().GetEvalCtx()
	for i := range batch.NumRows() {
		key := batch.GetRow(i).GetString(len(e.funcs))
		state, existed := e.states.M[key]
		if !existed && e.fixedStates {
			// Fixed-size deserialized states own their values. Adopt them instead
			// of allocating empty states and merging every first occurrence.
			state = make([]aggfuncs.PartialResult, len(e.funcs))
			for j := range state {
				if partials[j][i] == nil {
					return errors.New("invalid ExternalHashAgg aggregate state")
				}
				state[j] = partials[j][i]
			}
			retained := partialBytes / int64(batch.NumRows())
			temporaryBytes -= retained
			e.tracker.Consume(-retained)
			e.addState(strings.Clone(key), state, retained)
		} else {
			if !existed {
				state = e.getState(strings.Clone(key))
			}
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
		}
		if e.stateBytes > e.stateLimit || e.tracker.BytesConsumed() > e.memoryLimit {
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

// Close releases execution resources. IMPORT INTO owns the task prefix and
// removes spill objects together with its global-sort files during task cleanup.
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
	for i, data := range e.readObjects {
		e.tracker.Consume(-int64(cap(data)))
		e.readObjects[i] = nil
	}
	e.clearState()
	e.tracker.Consume(-e.frameBytes)
	e.frameBytes = 0
	e.tracker.Detach()
	e.input, e.groupKeyBuf = nil, nil
	return e.BaseExecutor.Close()
}
