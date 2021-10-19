package executor

import (
	"encoding/base64"
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/mock"
)

func buildMockFieldType(tps ...byte) (retTypes []*types.FieldType) {
	for _, tp := range tps {
		retTypes = append(retTypes, &types.FieldType{
			Tp:      tp,
			Flen:    types.UnspecifiedLength,
			Decimal: types.UnspecifiedLength,
		})
	}
	return
}

func buildMockChunk(chkCap int, tps []*types.FieldType) (chk *chunk.Chunk) {
	chk = chunk.NewChunkWithCapacity(tps, chkCap)
	for rowIdx := 0; rowIdx < chkCap; rowIdx++ {
		for colIdx := 0; colIdx < len(tps); colIdx++ {
			switch tps[colIdx].Tp {
			case mysql.TypeLonglong:
				chk.AppendInt64(colIdx, rand.Int63())
			case mysql.TypeDouble:
				chk.AppendFloat32(colIdx, rand.Float32())
			case mysql.TypeVarString:
				buff := make([]byte, 10)
				rand.Read(buff)
				chk.AppendString(colIdx, base64.RawURLEncoding.EncodeToString(buff))
			case mysql.TypeNewDecimal:
				var d types.MyDecimal
				chk.AppendMyDecimal(colIdx, d.FromInt(int64(rand.Int())))
			default:
				panic(fmt.Sprintf("not impl in buildChunk, tp: %v", tps[colIdx].Tp))
			}
		}
	}
	return
}

func buildMockChunks(chkNum int, chkCap int, types []*types.FieldType) (ret []*chunk.Chunk) {
	rand.Seed(100)
	for i := 0; i < chkNum; i++ {
		ret = append(ret, buildMockChunk(chkCap, types))
	}
	return
}

func buildMockRetFieldTypes(buildTps []*types.FieldType, probeTps []*types.FieldType, outerIsRight bool) (ret []*types.FieldType) {
	if outerIsRight {
		ret = append(ret, buildTps...)
		ret = append(ret, probeTps...)
	} else {
		ret = append(ret, probeTps...)
		ret = append(ret, buildTps...)
	}
	return
}

func BenchmarkJoin2Chunk(b *testing.B) {
	// Prepare schema.
	fieldTypes := buildMockFieldType(mysql.TypeLonglong, mysql.TypeDouble, mysql.TypeLonglong, mysql.TypeLonglong, mysql.TypeDouble)
	// 1M rows for probe.
	probeChks := buildMockChunks(1024, variable.DefMaxChunkSize, fieldTypes)
	// 10240 rows for build.
	buildChks := buildMockChunks(10, variable.DefMaxChunkSize, fieldTypes)

	buildKeyColIdx := []int{0, 2, 3}
	probeKeyColIdx := []int{0, 2, 3}

	sctx := mock.NewContext()
	sctx.GetSessionVars().InitChunkSize = variable.DefInitChunkSize
	sctx.GetSessionVars().MaxChunkSize = variable.DefMaxChunkSize

	hCtx := &hashContext{
		allTypes:  fieldTypes,
		keyColIdx: buildKeyColIdx,
	}
	var totalCnt uint64
	for _, chk := range buildChks {
		totalCnt += uint64(chk.NumRows())
	}

	// Prepare HJ exec.
	hj := &HashJoinExec{
		baseExecutor: newBaseExecutor(sctx, nil, 0, nil, nil),
		concurrency:  5,
	}
	hj.rowContainer = newHashRowContainer(sctx, int(totalCnt), hCtx)
	for _, chk := range buildChks {
		if err := hj.rowContainer.PutChunk(chk, nil); err != nil {
			panic(err)
		}
	}
	// Inner child idx == 0; result join row: inner + outer(inner is before, outer is after).
	outerIsRight := true
	// Inner join doesn't care def val.
	var defVals []types.Datum
	// No filter.
	var filter []expression.Expression
	// ChildrenUsed is nil. So join output will be all left and right child output.
	var childrenUsed [][]bool
	hj.retFieldTypes = buildMockRetFieldTypes(fieldTypes, fieldTypes, outerIsRight)
	hj.joinResultCh = make(chan *hashjoinWorkerResult, hj.concurrency+1)
	hj.joinChkResourceCh = make([]chan *chunk.Chunk, hj.concurrency)
	hj.joiners = make([]joiner, hj.concurrency)
	hj.rowContainerForProbe = make([]*hashRowContainer, hj.concurrency)
	for i := uint(0); i < hj.concurrency; i++ {
		if i == 0 {
			hj.rowContainerForProbe[i] = hj.rowContainer
		} else {
			hj.rowContainerForProbe[i] = hj.rowContainer.ShallowCopy()
		}
		hj.joiners[i] = newJoiner(sctx, plannercore.InnerJoin, outerIsRight, defVals, filter, fieldTypes, fieldTypes, childrenUsed)
	}

	probeChkResourceCh := make(chan *chunk.Chunk, hj.concurrency)

	// Ignore prepare time.
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup

		// Init probe.
		for i := uint(0); i < hj.concurrency; i++ {
			hj.joinChkResourceCh[i] = make(chan *chunk.Chunk, 1)
			hj.joinChkResourceCh[i] <- newFirstChunk(hj)
		}

		doneCh := make(chan struct{})

		// Mock probe side fetcher
		go func() {
			for _, probeChk := range probeChks {
				probeChkResourceCh <- probeChk
			}
			close(doneCh)
		}()

		for j := uint(0); j < hj.concurrency; j++ {
			wg.Add(1)

			go func(workerID uint) {
				hCtx := &hashContext{
					allTypes:  fieldTypes,
					keyColIdx: probeKeyColIdx,
				}
				selected := make([]bool, 0, chunk.InitialCapacity)
				_, joinResult := hj.getNewJoinResult(workerID)

				for {
					select {
					case chk, ok := <-probeChkResourceCh:
						if !ok {
							panic("probeChkResourceCh closed unexpectedly")
						}
						_, joinResult = hj.join2Chunk(workerID, chk, hCtx, hj.rowContainerForProbe[workerID], joinResult, selected)
					case <-doneCh:
						wg.Done()
						return
					}
				}
			}(j)
		}

		// Mock main thread, receive joined data.
		go func(doneCh chan struct{}) {
			for {
				select {
				case res, ok := <-hj.joinResultCh:
					if !ok {
						panic("joinResultCh closed unexpectedly")
					}
					res.src <- newFirstChunk(hj)
				case <-doneCh:
					return
				}
			}
		}(doneCh)
		wg.Wait()

		for _, probeSideChk := range probeChks {
			probeSideChk.SetSel(nil)
		}
	}
}
