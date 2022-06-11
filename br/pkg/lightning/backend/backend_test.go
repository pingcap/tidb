package backend_test

import (
	"context"
	"database/sql/driver"
	"testing"
	"time"

	gmysql "github.com/go-sql-driver/mysql"
	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/mock"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

type backendSuite struct {
	controller  *gomock.Controller
	mockBackend *mock.MockBackend
	backend     backend.Backend
	ts          uint64
}

func createBackendSuite(c gomock.TestReporter) *backendSuite {
	controller := gomock.NewController(c)
	mockBackend := mock.NewMockBackend(controller)
	return &backendSuite{
		controller:  controller,
		mockBackend: mockBackend,
		backend:     backend.MakeBackend(mockBackend),
		ts:          oracle.ComposeTS(time.Now().Unix()*1000, 0),
	}
}

func (s *backendSuite) tearDownTest() {
	s.controller.Finish()
}

func TestOpenCloseImportCleanUpEngine(t *testing.T) {
	s := createBackendSuite(t)
	defer s.tearDownTest()
	ctx := context.Background()
	engineUUID := uuid.MustParse("902efee3-a3f9-53d4-8c82-f12fb1900cd1")

	openCall := s.mockBackend.EXPECT().
		OpenEngine(ctx, &backend.EngineConfig{}, engineUUID).
		Return(nil)
	closeCall := s.mockBackend.EXPECT().
		CloseEngine(ctx, nil, engineUUID).
		Return(nil).
		After(openCall)
	importCall := s.mockBackend.EXPECT().
		ImportEngine(ctx, engineUUID, gomock.Any(), gomock.Any()).
		Return(nil).
		After(closeCall)
	s.mockBackend.EXPECT().
		CleanupEngine(ctx, engineUUID).
		Return(nil).
		After(importCall)

	engine, err := s.backend.OpenEngine(ctx, &backend.EngineConfig{}, "`db`.`table`", 1)
	require.NoError(t, err)
	closedEngine, err := engine.Close(ctx, nil)
	require.NoError(t, err)
	err = closedEngine.Import(ctx, 1, 1)
	require.NoError(t, err)
	err = closedEngine.Cleanup(ctx)
	require.NoError(t, err)
}

func TestUnsafeCloseEngine(t *testing.T) {
	s := createBackendSuite(t)
	defer s.tearDownTest()

	ctx := context.Background()
	engineUUID := uuid.MustParse("7e3f3a3c-67ce-506d-af34-417ec138fbcb")

	closeCall := s.mockBackend.EXPECT().
		CloseEngine(ctx, nil, engineUUID).
		Return(nil)
	s.mockBackend.EXPECT().
		CleanupEngine(ctx, engineUUID).
		Return(nil).
		After(closeCall)

	closedEngine, err := s.backend.UnsafeCloseEngine(ctx, nil, "`db`.`table`", -1)
	require.NoError(t, err)
	err = closedEngine.Cleanup(ctx)
	require.NoError(t, err)
}

func TestUnsafeCloseEngineWithUUID(t *testing.T) {
	s := createBackendSuite(t)
	defer s.tearDownTest()

	ctx := context.Background()
	engineUUID := uuid.MustParse("f1240229-79e0-4d8d-bda0-a211bf493796")

	closeCall := s.mockBackend.EXPECT().
		CloseEngine(ctx, nil, engineUUID).
		Return(nil)
	s.mockBackend.EXPECT().
		CleanupEngine(ctx, engineUUID).
		Return(nil).
		After(closeCall)

	closedEngine, err := s.backend.UnsafeCloseEngineWithUUID(ctx, nil, "some_tag", engineUUID)
	require.NoError(t, err)
	err = closedEngine.Cleanup(ctx)
	require.NoError(t, err)
}

func TestWriteEngine(t *testing.T) {
	s := createBackendSuite(t)
	defer s.tearDownTest()

	ctx := context.Background()
	engineUUID := uuid.MustParse("902efee3-a3f9-53d4-8c82-f12fb1900cd1")

	rows1 := mock.NewMockRows(s.controller)
	rows2 := mock.NewMockRows(s.controller)

	s.mockBackend.EXPECT().
		OpenEngine(ctx, &backend.EngineConfig{}, engineUUID).
		Return(nil)

	mockWriter := mock.NewMockEngineWriter(s.controller)

	s.mockBackend.EXPECT().LocalWriter(ctx, gomock.Any(), gomock.Any()).
		Return(mockWriter, nil).AnyTimes()
	mockWriter.EXPECT().
		AppendRows(ctx, "`db`.`table`", []string{"c1", "c2"}, rows1).
		Return(nil)
	mockWriter.EXPECT().Close(ctx).Return(nil, nil).AnyTimes()
	mockWriter.EXPECT().
		AppendRows(ctx, "`db`.`table`", []string{"c1", "c2"}, rows2).
		Return(nil)

	engine, err := s.backend.OpenEngine(ctx, &backend.EngineConfig{}, "`db`.`table`", 1)
	require.NoError(t, err)
	writer, err := engine.LocalWriter(ctx, &backend.LocalWriterConfig{})
	require.NoError(t, err)
	err = writer.WriteRows(ctx, []string{"c1", "c2"}, rows1)
	require.NoError(t, err)
	err = writer.WriteRows(ctx, []string{"c1", "c2"}, rows2)
	require.NoError(t, err)
	_, err = writer.Close(ctx)
	require.NoError(t, err)
}

func TestWriteToEngineWithNothing(t *testing.T) {
	s := createBackendSuite(t)
	defer s.tearDownTest()

	ctx := context.Background()
	emptyRows := mock.NewMockRows(s.controller)
	mockWriter := mock.NewMockEngineWriter(s.controller)

	s.mockBackend.EXPECT().OpenEngine(ctx, &backend.EngineConfig{}, gomock.Any()).Return(nil)
	mockWriter.EXPECT().AppendRows(ctx, gomock.Any(), gomock.Any(), emptyRows).Return(nil)
	mockWriter.EXPECT().Close(ctx).Return(nil, nil)
	s.mockBackend.EXPECT().LocalWriter(ctx, &backend.LocalWriterConfig{}, gomock.Any()).Return(mockWriter, nil)

	engine, err := s.backend.OpenEngine(ctx, &backend.EngineConfig{}, "`db`.`table`", 1)
	require.NoError(t, err)
	writer, err := engine.LocalWriter(ctx, &backend.LocalWriterConfig{})
	require.NoError(t, err)
	err = writer.WriteRows(ctx, nil, emptyRows)
	require.NoError(t, err)
	_, err = writer.Close(ctx)
	require.NoError(t, err)
}

func TestOpenEngineFailed(t *testing.T) {
	s := createBackendSuite(t)
	defer s.tearDownTest()

	ctx := context.Background()

	s.mockBackend.EXPECT().OpenEngine(ctx, &backend.EngineConfig{}, gomock.Any()).
		Return(errors.New("fake unrecoverable open error"))

	_, err := s.backend.OpenEngine(ctx, &backend.EngineConfig{}, "`db`.`table`", 1)
	require.EqualError(t, err, "fake unrecoverable open error")
}

func TestWriteEngineFailed(t *testing.T) {
	s := createBackendSuite(t)
	defer s.tearDownTest()

	ctx := context.Background()
	rows := mock.NewMockRows(s.controller)

	s.mockBackend.EXPECT().OpenEngine(ctx, &backend.EngineConfig{}, gomock.Any()).Return(nil)
	mockWriter := mock.NewMockEngineWriter(s.controller)

	s.mockBackend.EXPECT().LocalWriter(ctx, gomock.Any(), gomock.Any()).Return(mockWriter, nil).AnyTimes()
	mockWriter.EXPECT().
		AppendRows(ctx, gomock.Any(), gomock.Any(), rows).
		Return(errors.Annotate(context.Canceled, "fake unrecoverable write error"))
	mockWriter.EXPECT().Close(ctx).Return(nil, nil)

	engine, err := s.backend.OpenEngine(ctx, &backend.EngineConfig{}, "`db`.`table`", 1)
	require.NoError(t, err)
	writer, err := engine.LocalWriter(ctx, &backend.LocalWriterConfig{})
	require.NoError(t, err)
	err = writer.WriteRows(ctx, nil, rows)
	require.Error(t, err)
	require.Regexp(t, "^fake unrecoverable write error", err.Error())
	_, err = writer.Close(ctx)
	require.NoError(t, err)
}

func TestWriteBatchSendFailedWithRetry(t *testing.T) {
	s := createBackendSuite(t)
	defer s.tearDownTest()

	ctx := context.Background()
	rows := mock.NewMockRows(s.controller)

	s.mockBackend.EXPECT().OpenEngine(ctx, &backend.EngineConfig{}, gomock.Any()).Return(nil)
	mockWriter := mock.NewMockEngineWriter(s.controller)

	s.mockBackend.EXPECT().LocalWriter(ctx, gomock.Any(), gomock.Any()).Return(mockWriter, nil).AnyTimes()
	mockWriter.EXPECT().AppendRows(ctx, gomock.Any(), gomock.Any(), rows).
		Return(errors.New("fake recoverable write batch error")).
		MinTimes(1)
	mockWriter.EXPECT().Close(ctx).Return(nil, nil).MinTimes(1)

	engine, err := s.backend.OpenEngine(ctx, &backend.EngineConfig{}, "`db`.`table`", 1)
	require.NoError(t, err)
	writer, err := engine.LocalWriter(ctx, &backend.LocalWriterConfig{})
	require.NoError(t, err)
	err = writer.WriteRows(ctx, nil, rows)
	require.Error(t, err)
	require.Regexp(t, "fake recoverable write batch error$", err.Error())
	_, err = writer.Close(ctx)
	require.NoError(t, err)
}

func TestImportFailedNoRetry(t *testing.T) {
	s := createBackendSuite(t)
	defer s.tearDownTest()

	ctx := context.Background()

	s.mockBackend.EXPECT().CloseEngine(ctx, nil, gomock.Any()).Return(nil)
	s.mockBackend.EXPECT().
		ImportEngine(ctx, gomock.Any(), gomock.Any(), gomock.Any()).
		Return(errors.Annotate(context.Canceled, "fake unrecoverable import error"))

	closedEngine, err := s.backend.UnsafeCloseEngine(ctx, nil, "`db`.`table`", 1)
	require.NoError(t, err)
	err = closedEngine.Import(ctx, 1, 1)
	require.Error(t, err)
	require.Regexp(t, "^fake unrecoverable import error", err.Error())
}

func TestImportFailedWithRetry(t *testing.T) {
	s := createBackendSuite(t)
	defer s.tearDownTest()

	ctx := context.Background()

	s.mockBackend.EXPECT().CloseEngine(ctx, nil, gomock.Any()).Return(nil)
	s.mockBackend.EXPECT().
		ImportEngine(ctx, gomock.Any(), gomock.Any(), gomock.Any()).
		Return(errors.Annotate(driver.ErrBadConn, "fake recoverable import error")).
		MinTimes(2)
	s.mockBackend.EXPECT().RetryImportDelay().Return(time.Duration(0)).AnyTimes()

	closedEngine, err := s.backend.UnsafeCloseEngine(ctx, nil, "`db`.`table`", 1)
	require.NoError(t, err)
	err = closedEngine.Import(ctx, 1, 1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "fake recoverable import error")
}

func TestImportFailedRecovered(t *testing.T) {
	s := createBackendSuite(t)
	defer s.tearDownTest()

	ctx := context.Background()

	s.mockBackend.EXPECT().CloseEngine(ctx, nil, gomock.Any()).Return(nil)
	s.mockBackend.EXPECT().
		ImportEngine(ctx, gomock.Any(), gomock.Any(), gomock.Any()).
		Return(gmysql.ErrInvalidConn)
	s.mockBackend.EXPECT().
		ImportEngine(ctx, gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil)
	s.mockBackend.EXPECT().RetryImportDelay().Return(time.Duration(0)).AnyTimes()

	closedEngine, err := s.backend.UnsafeCloseEngine(ctx, nil, "`db`.`table`", 1)
	require.NoError(t, err)
	err = closedEngine.Import(ctx, 1, 1)
	require.NoError(t, err)
}

//nolint:interfacer // change test case signature causes check panicking.
func TestClose(t *testing.T) {
	s := createBackendSuite(t)
	defer s.tearDownTest()

	s.mockBackend.EXPECT().Close().Return()

	s.backend.Close()
}

func TestMakeEmptyRows(t *testing.T) {
	s := createBackendSuite(t)
	defer s.tearDownTest()

	rows := mock.NewMockRows(s.controller)
	s.mockBackend.EXPECT().MakeEmptyRows().Return(rows)
	require.Equal(t, rows, s.mockBackend.MakeEmptyRows())
}

func TestNewEncoder(t *testing.T) {
	s := createBackendSuite(t)
	defer s.tearDownTest()

	encoder := mock.NewMockEncoder(s.controller)
	options := &kv.SessionOptions{SQLMode: mysql.ModeANSIQuotes, Timestamp: 1234567890}
	s.mockBackend.EXPECT().NewEncoder(nil, options).Return(encoder, nil)

	realEncoder, err := s.mockBackend.NewEncoder(nil, options)
	require.Equal(t, realEncoder, encoder)
	require.NoError(t, err)
}

func TestCheckDiskQuota(t *testing.T) {
	s := createBackendSuite(t)
	defer s.tearDownTest()

	uuid1 := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	uuid3 := uuid.MustParse("33333333-3333-3333-3333-333333333333")
	uuid5 := uuid.MustParse("55555555-5555-5555-5555-555555555555")
	uuid7 := uuid.MustParse("77777777-7777-7777-7777-777777777777")
	uuid9 := uuid.MustParse("99999999-9999-9999-9999-999999999999")

	fileSizes := []backend.EngineFileSize{
		{
			UUID:        uuid1,
			DiskSize:    1000,
			MemSize:     0,
			IsImporting: false,
		},
		{
			UUID:        uuid3,
			DiskSize:    2000,
			MemSize:     1000,
			IsImporting: true,
		},
		{
			UUID:        uuid5,
			DiskSize:    1500,
			MemSize:     3500,
			IsImporting: false,
		},
		{
			UUID:        uuid7,
			DiskSize:    0,
			MemSize:     7000,
			IsImporting: true,
		},
		{
			UUID:        uuid9,
			DiskSize:    4500,
			MemSize:     4500,
			IsImporting: false,
		},
	}

	s.mockBackend.EXPECT().EngineFileSizes().Return(fileSizes).Times(4)

	// No quota exceeded
	le, iple, ds, ms := s.backend.CheckDiskQuota(30000)
	require.Len(t, le, 0)
	require.Equal(t, 0, iple)
	require.Equal(t, int64(9000), ds)
	require.Equal(t, int64(16000), ms)

	// Quota exceeded, the largest one is out
	le, iple, ds, ms = s.backend.CheckDiskQuota(20000)
	require.Equal(t, []uuid.UUID{uuid9}, le)
	require.Equal(t, 0, iple)
	require.Equal(t, int64(9000), ds)
	require.Equal(t, int64(16000), ms)

	// Quota exceeded, the importing one should be ranked least priority
	le, iple, ds, ms = s.backend.CheckDiskQuota(12000)
	require.Equal(t, []uuid.UUID{uuid5, uuid9}, le)
	require.Equal(t, 0, iple)
	require.Equal(t, int64(9000), ds)
	require.Equal(t, int64(16000), ms)

	// Quota exceeded, the importing ones should not be visible
	le, iple, ds, ms = s.backend.CheckDiskQuota(5000)
	require.Equal(t, []uuid.UUID{uuid1, uuid5, uuid9}, le)
	require.Equal(t, 1, iple)
	require.Equal(t, int64(9000), ds)
	require.Equal(t, int64(16000), ms)
}
