// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	tcontext "github.com/pingcap/tidb/dumpling/context"
	pd "github.com/tikv/pd/client"
	pdgc "github.com/tikv/pd/client/clients/gc"
)

// mockGCStatesClient implements pdgc.GCStatesClient for unit tests.
type mockGCStatesClient struct {
	mu             sync.Mutex
	setBarrierErr  error // injected error for SetGCBarrier
	setBarrierInfo *pdgc.GCBarrierInfo
	setCalls       int
	delCalls       int
}

func (m *mockGCStatesClient) SetGCBarrier(_ context.Context, barrierID string, barrierTS uint64, ttl time.Duration) (*pdgc.GCBarrierInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.setCalls++
	if m.setBarrierErr != nil {
		return nil, m.setBarrierErr
	}
	info := &pdgc.GCBarrierInfo{BarrierID: barrierID, BarrierTS: barrierTS, TTL: ttl}
	m.setBarrierInfo = info
	return info, nil
}

func (m *mockGCStatesClient) DeleteGCBarrier(_ context.Context, _ string) (*pdgc.GCBarrierInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.delCalls++
	return nil, nil
}

func (m *mockGCStatesClient) GetGCState(_ context.Context) (pdgc.GCState, error) {
	return pdgc.GCState{}, nil
}

func (m *mockGCStatesClient) SetGlobalGCBarrier(_ context.Context, _ string, _ uint64, _ time.Duration) (*pdgc.GlobalGCBarrierInfo, error) {
	return nil, nil
}

func (m *mockGCStatesClient) DeleteGlobalGCBarrier(_ context.Context, _ string) (*pdgc.GlobalGCBarrierInfo, error) {
	return nil, nil
}

func (m *mockGCStatesClient) GetAllKeyspacesGCStates(_ context.Context) (pdgc.ClusterGCStates, error) {
	return pdgc.ClusterGCStates{}, nil
}

// mockPDClientForGC implements a minimal pd.Client for GC-related tests.
type mockPDClientForGC struct {
	pd.Client // embed to satisfy the full interface; only override what we need

	mu                     sync.Mutex
	updateSafePointCalls   int
	updateSafePointErr     error
	lastSafePointServiceID string
	lastSafePointTTL       int64
	lastSafePointTS        uint64
	gcStatesClient         *mockGCStatesClient
	closed                 atomic.Bool
}

func newMockPDClientForGC() *mockPDClientForGC {
	return &mockPDClientForGC{
		gcStatesClient: &mockGCStatesClient{},
	}
}

func (m *mockPDClientForGC) UpdateServiceGCSafePoint(_ context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.updateSafePointCalls++
	m.lastSafePointServiceID = serviceID
	m.lastSafePointTTL = ttl
	m.lastSafePointTS = safePoint
	if m.updateSafePointErr != nil {
		return 0, m.updateSafePointErr
	}
	return safePoint, nil
}

func (m *mockPDClientForGC) GetGCStatesClient(_ uint32) pdgc.GCStatesClient {
	return m.gcStatesClient
}

func (m *mockPDClientForGC) Close() {
	m.closed.Store(true)
}

type mockPoisonWriter struct {
	buf string
}

func (m *mockPoisonWriter) Write(_ context.Context, p []byte) (int, error) {
	s := string(p)
	if s == "poison" {
		return 0, fmt.Errorf("poison_error")
	}
	m.buf = s
	return len(s), nil
}

func (m *mockPoisonWriter) Close(_ context.Context) error {
	// noop
	return nil
}

type mockMetaIR struct {
	tarName string
	meta    string
	specCmt []string
}

func (m *mockMetaIR) SpecialComments() StringIter {
	return newStringIter(m.specCmt...)
}

func (m *mockMetaIR) TargetName() string {
	return m.tarName
}

func (m *mockMetaIR) MetaSQL() string {
	return m.meta
}

func newMockMetaIR(targetName string, meta string, specialComments []string) MetaIR {
	return &mockMetaIR{
		tarName: targetName,
		meta:    meta,
		specCmt: specialComments,
	}
}

type mockTableIR struct {
	dbName           string
	tblName          string
	chunIndex        int
	data             [][]driver.Value
	selectedField    string
	selectedLen      int
	specCmt          []string
	colTypes         []string
	colNames         []string
	escapeBackSlash  bool
	hasImplicitRowID bool
	rowErr           error
	rows             *sql.Rows
	SQLRowIter
}

func (m *mockTableIR) RawRows() *sql.Rows {
	return m.rows
}

func (m *mockTableIR) ShowCreateTable() string {
	return ""
}

func (m *mockTableIR) ShowCreateView() string {
	return ""
}

func (m *mockTableIR) AvgRowLength() uint64 {
	return 0
}

func (m *mockTableIR) HasImplicitRowID() bool {
	return m.hasImplicitRowID
}

func (m *mockTableIR) Start(_ *tcontext.Context, conn *sql.Conn) error {
	return nil
}

func (m *mockTableIR) DatabaseName() string {
	return m.dbName
}

func (m *mockTableIR) TableName() string {
	return m.tblName
}

func (m *mockTableIR) ChunkIndex() int {
	return m.chunIndex
}

func (m *mockTableIR) ColumnCount() uint {
	return uint(len(m.colTypes))
}

func (m *mockTableIR) ColumnTypes() []string {
	return m.colTypes
}

func (m *mockTableIR) ColumnNames() []string {
	return m.colNames
}

func (m *mockTableIR) SelectedField() string {
	return m.selectedField
}

func (m *mockTableIR) SelectedLen() int {
	return m.selectedLen
}

func (m *mockTableIR) SpecialComments() StringIter {
	return newStringIter(m.specCmt...)
}

func (m *mockTableIR) Rows() SQLRowIter {
	if m.SQLRowIter == nil {
		mockRows := sqlmock.NewRows(m.colTypes)
		for _, datum := range m.data {
			mockRows.AddRow(datum...)
		}
		db, mock, err := sqlmock.New()
		if err != nil {
			panic(fmt.Sprintf("sqlmock.New return error: %v", err))
		}
		defer db.Close()
		mock.ExpectQuery("select 1").WillReturnRows(mockRows)
		if m.rowErr != nil {
			mockRows.RowError(len(m.data)-1, m.rowErr)
		}
		rows, err := db.Query("select 1")
		if err != nil {
			panic(fmt.Sprintf("sqlmock.New return error: %v", err))
		}
		m.SQLRowIter = newRowIter(rows, len(m.colTypes))
		m.rows = rows
	}
	return m.SQLRowIter
}

func (m *mockTableIR) Close() error {
	return nil
}

func (m *mockTableIR) EscapeBackSlash() bool {
	return m.escapeBackSlash
}

func newMockTableIR(databaseName, tableName string, data [][]driver.Value, specialComments, colTypes []string) *mockTableIR {
	return &mockTableIR{
		dbName:        databaseName,
		tblName:       tableName,
		data:          data,
		specCmt:       specialComments,
		selectedField: "*",
		selectedLen:   len(colTypes),
		colTypes:      colTypes,
		SQLRowIter:    nil,
	}
}
