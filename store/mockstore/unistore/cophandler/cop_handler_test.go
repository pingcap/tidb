// Copyright 2019-present PingCAP, Inc.
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

package cophandler

import (
	"errors"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/ngaut/unistore/lockstore"
	"github.com/ngaut/unistore/tikv/dbreader"
	"github.com/ngaut/unistore/tikv/mvcc"
	"github.com/pingcap/badger"
	"github.com/pingcap/badger/y"
	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/rowcodec"
	"github.com/pingcap/tipb/go-tipb"
)

func TestT(t *testing.T) {
	TestingT(t)
}

type testSuite struct{}

func (ts testSuite) SetUpSuite(c *C) {}

func (ts testSuite) TearDownSuite(c *C) {}

var _ = Suite(testSuite{})

const (
	keyNumber         = 3
	tableID           = 0
	startTs           = 10
	ttl               = 60000
	dagRequestStartTs = 100
)

// wrapper of test data, including encoded data, column types etc.
type data struct {
	encodedTestKVDatas []*encodedTestKVData
	colInfos           []*tipb.ColumnInfo
	rows               map[int64][]types.Datum    // handle -> row
	colTypes           map[int64]*types.FieldType // colId -> fieldType
}

type encodedTestKVData struct {
	encodedRowKey   []byte
	encodedRowValue []byte
}

func initTestData(store *testStore, encodedKVDatas []*encodedTestKVData) []error {
	i := 0
	for _, kvData := range encodedKVDatas {
		mutation := makeATestMutaion(kvrpcpb.Op_Put, kvData.encodedRowKey,
			kvData.encodedRowValue)
		req := &kvrpcpb.PrewriteRequest{
			Mutations:    []*kvrpcpb.Mutation{mutation},
			PrimaryLock:  kvData.encodedRowKey,
			StartVersion: uint64(startTs + i),
			LockTtl:      ttl,
		}
		store.prewrite(req)
		commitError := store.commit([][]byte{kvData.encodedRowKey},
			uint64(startTs+i), uint64(startTs+i+1))
		if commitError != nil {
			return []error{commitError}
		}
		i += 2
	}
	return nil
}

func makeATestMutaion(op kvrpcpb.Op, key []byte, value []byte) *kvrpcpb.Mutation {
	return &kvrpcpb.Mutation{
		Op:    op,
		Key:   key,
		Value: value,
	}
}

func prepareTestTableData(c *C, keyNumber int, tableID int64) *data {
	stmtCtx := new(stmtctx.StatementContext)
	colIds := []int64{1, 2, 3}
	colTypes := []*types.FieldType{
		types.NewFieldType(mysql.TypeLonglong),
		types.NewFieldType(mysql.TypeString),
		types.NewFieldType(mysql.TypeDouble),
	}
	colInfos := make([]*tipb.ColumnInfo, 3)
	colTypeMap := map[int64]*types.FieldType{}
	for i := 0; i < 3; i++ {
		colInfos[i] = &tipb.ColumnInfo{
			ColumnId: colIds[i],
			Tp:       int32(colTypes[i].Tp),
		}
		colTypeMap[colIds[i]] = colTypes[i]
	}
	rows := map[int64][]types.Datum{}
	encodedTestKVDatas := make([]*encodedTestKVData, keyNumber)
	encoder := &rowcodec.Encoder{Enable: true}
	for i := 0; i < keyNumber; i++ {
		datum := types.MakeDatums(i, "abc", 10.0)
		rows[int64(i)] = datum
		rowEncodedData, err := tablecodec.EncodeRow(stmtCtx, datum, colIds, nil, nil, encoder)
		c.Assert(err, IsNil)
		rowKeyEncodedData := tablecodec.EncodeRowKeyWithHandle(tableID, kv.IntHandle(i))
		encodedTestKVDatas[i] = &encodedTestKVData{encodedRowKey: rowKeyEncodedData, encodedRowValue: rowEncodedData}
	}
	return &data{
		colInfos:           colInfos,
		encodedTestKVDatas: encodedTestKVDatas,
		rows:               rows,
		colTypes:           colTypeMap,
	}
}

func getTestPointRange(tableID int64, handle int64) kv.KeyRange {
	startKey := tablecodec.EncodeRowKeyWithHandle(tableID, kv.IntHandle(handle))
	endKey := make([]byte, len(startKey))
	copy(endKey, startKey)
	convertToPrefixNext(endKey)
	return kv.KeyRange{
		StartKey: startKey,
		EndKey:   endKey,
	}
}

// convert this key to the smallest key which is larger than the key given.
// see tikv/src/coprocessor/util.rs for more detail.
func convertToPrefixNext(key []byte) []byte {
	if len(key) == 0 {
		return []byte{0}
	}
	for i := len(key) - 1; i >= 0; i-- {
		if key[i] == 255 {
			key[i] = 0
		} else {
			key[i] += 1
			return key
		}
	}
	for i := 0; i < len(key); i++ {
		key[i] = 255
	}
	return append(key, 0)
}

// return whether these two keys are equal.
func isPrefixNext(key []byte, expected []byte) bool {
	key = convertToPrefixNext(key)
	if len(key) != len(expected) {
		return false
	}
	for i := 0; i < len(key); i++ {
		if key[i] != expected[i] {
			return false
		}
	}
	return true
}

// return a dag context according to dagReq and key ranges.
func newDagContext(store *testStore, keyRanges []kv.KeyRange, dagReq *tipb.DAGRequest, startTs uint64) *dagContext {
	sc := flagsToStatementContext(dagReq.Flags)
	txn := store.db.NewTransaction(false)
	dagCtx := &dagContext{
		evalContext: &evalContext{sc: sc},
		dbReader:    dbreader.NewDBReader(nil, []byte{255}, txn),
		lockStore:   store.locks,
		dagReq:      dagReq,
		startTS:     startTs,
	}
	if dagReq.Executors[0].Tp == tipb.ExecType_TypeTableScan {
		dagCtx.setColumnInfo(dagReq.Executors[0].TblScan.Columns)
	} else {
		dagCtx.setColumnInfo(dagReq.Executors[0].IdxScan.Columns)
	}
	dagCtx.keyRanges = make([]*coprocessor.KeyRange, len(keyRanges))
	for i, keyRange := range keyRanges {
		dagCtx.keyRanges[i] = &coprocessor.KeyRange{
			Start: keyRange.StartKey,
			End:   keyRange.EndKey,
		}
	}
	return dagCtx
}

// build and execute the executors according to the dagRequest and dagContext,
// return the result chunk data, rows count and err if occurs.
func buildExecutorsAndExecute(dagRequest *tipb.DAGRequest,
	dagCtx *dagContext) ([]tipb.Chunk, int, error) {
	closureExec, err := buildClosureExecutor(dagCtx, dagRequest)
	if err != nil {
		return nil, 0, err
	}
	if closureExec != nil {
		chunks, err := closureExec.execute()
		if err != nil {
			return nil, 0, err
		}
		return chunks, closureExec.rowCount, nil
	}
	return nil, 0, errors.New("closureExec creation failed")
}

// dagBuilder is used to build dag request
type dagBuilder struct {
	startTs       uint64
	executors     []*tipb.Executor
	outputOffsets []uint32
}

// return a default dagBuilder
func newDagBuilder() *dagBuilder {
	return &dagBuilder{executors: make([]*tipb.Executor, 0)}
}

func (dagBuilder *dagBuilder) setStartTs(startTs uint64) *dagBuilder {
	dagBuilder.startTs = startTs
	return dagBuilder
}

func (dagBuilder *dagBuilder) setOutputOffsets(outputOffsets []uint32) *dagBuilder {
	dagBuilder.outputOffsets = outputOffsets
	return dagBuilder
}

func (dagBuilder *dagBuilder) addTableScan(colInfos []*tipb.ColumnInfo, tableID int64) *dagBuilder {
	dagBuilder.executors = append(dagBuilder.executors, &tipb.Executor{
		Tp: tipb.ExecType_TypeTableScan,
		TblScan: &tipb.TableScan{
			Columns: colInfos,
			TableId: tableID,
		},
	})
	return dagBuilder
}

func (dagBuilder *dagBuilder) addSelection(expr *tipb.Expr) *dagBuilder {
	dagBuilder.executors = append(dagBuilder.executors, &tipb.Executor{
		Tp: tipb.ExecType_TypeSelection,
		Selection: &tipb.Selection{
			Conditions:       []*tipb.Expr{expr},
			XXX_unrecognized: nil,
		},
	})
	return dagBuilder
}

func (dagBuilder *dagBuilder) addLimit(limit uint64) *dagBuilder {
	dagBuilder.executors = append(dagBuilder.executors, &tipb.Executor{
		Tp:    tipb.ExecType_TypeLimit,
		Limit: &tipb.Limit{Limit: limit},
	})
	return dagBuilder
}

func (dagBuilder *dagBuilder) build() *tipb.DAGRequest {
	return &tipb.DAGRequest{
		Executors:     dagBuilder.executors,
		OutputOffsets: dagBuilder.outputOffsets,
	}
}

// see tikv/src/coprocessor/util.rs for more detail
func (ts testSuite) TestIsPrefixNext(c *C) {
	c.Assert(isPrefixNext([]byte{}, []byte{0}), IsTrue)
	c.Assert(isPrefixNext([]byte{0}, []byte{1}), IsTrue)
	c.Assert(isPrefixNext([]byte{1}, []byte{2}), IsTrue)
	c.Assert(isPrefixNext([]byte{255}, []byte{255, 0}), IsTrue)
	c.Assert(isPrefixNext([]byte{255, 255, 255}, []byte{255, 255, 255, 0}), IsTrue)
	c.Assert(isPrefixNext([]byte{1, 255}, []byte{2, 0}), IsTrue)
	c.Assert(isPrefixNext([]byte{0, 1, 255}, []byte{0, 2, 0}), IsTrue)
	c.Assert(isPrefixNext([]byte{0, 1, 255, 5}, []byte{0, 1, 255, 6}), IsTrue)
	c.Assert(isPrefixNext([]byte{0, 1, 5, 255}, []byte{0, 1, 6, 0}), IsTrue)
	c.Assert(isPrefixNext([]byte{0, 1, 255, 255}, []byte{0, 2, 0, 0}), IsTrue)
	c.Assert(isPrefixNext([]byte{0, 255, 255, 255}, []byte{1, 0, 0, 0}), IsTrue)
}

func (ts testSuite) TestPointGet(c *C) {
	// here would build mvccStore and server, and prepare
	// three rows data, just like the test data of table_scan.rs.
	// then init the store with the generated data.
	data := prepareTestTableData(c, keyNumber, tableID)
	store, err := newTestStore("cop_handler_test_db", "cop_handler_test_log")
	defer cleanTestStore(store)
	c.Assert(err, IsNil)
	errors := initTestData(store, data.encodedTestKVDatas)
	c.Assert(errors, IsNil)

	// point get should return nothing when handle is math.MinInt64
	handle := int64(math.MinInt64)
	dagRequest := newDagBuilder().
		setStartTs(dagRequestStartTs).
		addTableScan(data.colInfos, tableID).
		setOutputOffsets([]uint32{0, 1}).
		build()
	dagCtx := newDagContext(store, []kv.KeyRange{getTestPointRange(tableID, handle)},
		dagRequest, dagRequestStartTs)
	chunks, rowCount, err := buildExecutorsAndExecute(dagRequest, dagCtx)
	c.Assert(len(chunks), Equals, 0)
	c.Assert(err, IsNil)
	c.Assert(rowCount, Equals, 0)

	// point get should return one row when handle = 0
	handle = 0
	dagRequest = newDagBuilder().
		setStartTs(dagRequestStartTs).
		addTableScan(data.colInfos, tableID).
		setOutputOffsets([]uint32{0, 1}).
		build()
	dagCtx = newDagContext(store, []kv.KeyRange{getTestPointRange(tableID, handle)},
		dagRequest, dagRequestStartTs)
	chunks, rowCount, err = buildExecutorsAndExecute(dagRequest, dagCtx)
	c.Assert(err, IsNil)
	c.Assert(rowCount, Equals, 1)
	returnedRow, err := codec.Decode(chunks[0].RowsData, 2)
	c.Assert(err, IsNil)
	// returned row should has 2 cols
	c.Assert(len(returnedRow), Equals, 2)

	// verify the returned rows value as input
	expectedRow := data.rows[handle]
	eq, err := returnedRow[0].CompareDatum(nil, &expectedRow[0])
	c.Assert(err, IsNil)
	c.Assert(eq, Equals, 0)
	eq, err = returnedRow[1].CompareDatum(nil, &expectedRow[1])
	c.Assert(err, IsNil)
	c.Assert(eq, Equals, 0)
}

func (ts testSuite) TestClosureExecutor(c *C) {
	data := prepareTestTableData(c, keyNumber, tableID)
	store, err := newTestStore("cop_handler_test_db", "cop_handler_test_log")
	defer cleanTestStore(store)
	c.Assert(err, IsNil)
	errors := initTestData(store, data.encodedTestKVDatas)
	c.Assert(errors, IsNil)

	dagRequest := newDagBuilder().
		setStartTs(dagRequestStartTs).
		addTableScan(data.colInfos, tableID).
		addSelection(buildEQIntExpr(1, -1)).
		addLimit(1).
		setOutputOffsets([]uint32{0, 1}).
		build()

	dagCtx := newDagContext(store, []kv.KeyRange{getTestPointRange(tableID, 1)},
		dagRequest, dagRequestStartTs)
	_, rowCount, err := buildExecutorsAndExecute(dagRequest, dagCtx)
	c.Assert(err, IsNil)
	c.Assert(rowCount, Equals, 0)
}

func buildEQIntExpr(colID, val int64) *tipb.Expr {
	return &tipb.Expr{
		Tp:        tipb.ExprType_ScalarFunc,
		Sig:       tipb.ScalarFuncSig_EQInt,
		FieldType: expression.ToPBFieldType(types.NewFieldType(mysql.TypeLonglong)),
		Children: []*tipb.Expr{
			{
				Tp:        tipb.ExprType_ColumnRef,
				Val:       codec.EncodeInt(nil, colID),
				FieldType: expression.ToPBFieldType(types.NewFieldType(mysql.TypeLonglong)),
			},
			{
				Tp:        tipb.ExprType_Int64,
				Val:       codec.EncodeInt(nil, val),
				FieldType: expression.ToPBFieldType(types.NewFieldType(mysql.TypeLonglong)),
			},
		},
	}
}

type testStore struct {
	db      *badger.DB
	locks   *lockstore.MemStore
	dbPath  string
	logPath string
}

func (ts *testStore) prewrite(req *kvrpcpb.PrewriteRequest) {
	for _, m := range req.Mutations {
		lock := &mvcc.MvccLock{
			MvccLockHdr: mvcc.MvccLockHdr{
				StartTS:     req.StartVersion,
				ForUpdateTS: req.ForUpdateTs,
				TTL:         uint32(req.LockTtl),
				PrimaryLen:  uint16(len(req.PrimaryLock)),
				MinCommitTS: req.MinCommitTs,
				Op:          uint8(m.Op),
			},
			Primary: req.PrimaryLock,
			Value:   m.Value,
		}
		ts.locks.Put(m.Key, lock.MarshalBinary())
	}
}

func (ts *testStore) commit(keys [][]byte, startTS, commitTS uint64) error {
	return ts.db.Update(func(txn *badger.Txn) error {
		for _, key := range keys {
			lock := mvcc.DecodeLock(ts.locks.Get(key, nil))
			userMeta := mvcc.NewDBUserMeta(startTS, commitTS)
			err := txn.SetEntry(&badger.Entry{
				Key:      y.KeyWithTs(key, commitTS),
				Value:    lock.Value,
				UserMeta: userMeta,
			})
			if err != nil {
				return err
			}
			ts.locks.Delete(key)
		}
		return nil
	})
}

func newTestStore(dbPrefix string, logPrefix string) (*testStore, error) {
	dbPath, err := ioutil.TempDir("", dbPrefix)
	if err != nil {
		return nil, err
	}
	LogPath, err := ioutil.TempDir("", logPrefix)
	if err != nil {
		return nil, err
	}
	db, err := createTestDB(dbPath, LogPath)
	if err != nil {
		return nil, err
	}
	// Some raft store path problems could not be found using simple store in tests
	// writer := NewDBWriter(dbBundle, safePoint)
	kvPath := filepath.Join(dbPath, "kv")
	raftPath := filepath.Join(dbPath, "raft")
	snapPath := filepath.Join(dbPath, "snap")
	os.MkdirAll(kvPath, os.ModePerm)
	os.MkdirAll(raftPath, os.ModePerm)
	os.Mkdir(snapPath, os.ModePerm)
	return &testStore{
		db:      db,
		locks:   lockstore.NewMemStore(4096),
		dbPath:  dbPath,
		logPath: LogPath,
	}, nil
}

func createTestDB(dbPath, LogPath string) (*badger.DB, error) {
	subPath := fmt.Sprintf("/%d", 0)
	opts := badger.DefaultOptions
	opts.Dir = dbPath + subPath
	opts.ValueDir = LogPath + subPath
	opts.ManagedTxns = true
	return badger.Open(opts)
}

func cleanTestStore(store *testStore) {
	os.RemoveAll(store.dbPath)
	os.RemoveAll(store.logPath)
}
