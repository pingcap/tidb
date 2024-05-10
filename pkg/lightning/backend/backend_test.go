// Copyright 2023 PingCAP, Inc.
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

package backend_test

import (
	"context"
	"database/sql/driver"
	"testing"
	"time"

	gmysql "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/mock"
	"github.com/pingcap/tidb/pkg/lightning/backend"
	"github.com/pingcap/tidb/pkg/lightning/backend/encode"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/mock/gomock"
)

type backendSuite struct {
	controller  *gomock.Controller
	mockBackend *mock.MockBackend
	encBuilder  *mock.MockEncodingBuilder
	engineMgr   backend.EngineManager
	backend     backend.Backend
	ts          uint64
}

func createBackendSuite(c gomock.TestReporter) *backendSuite {
	controller := gomock.NewController(c)
	mockBackend := mock.NewMockBackend(controller)
	return &backendSuite{
		controller:  controller,
		mockBackend: mockBackend,
		engineMgr:   backend.MakeEngineManager(mockBackend),
		backend:     mockBackend,
		encBuilder:  mock.NewMockEncodingBuilder(controller),
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
		CloseEngine(ctx, &backend.EngineConfig{}, engineUUID).
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

	engine, err := s.engineMgr.OpenEngine(ctx, &backend.EngineConfig{}, "`db`.`table`", 1)
	require.NoError(t, err)
	closedEngine, err := engine.Close(ctx)
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

	closedEngine, err := s.engineMgr.UnsafeCloseEngine(ctx, nil, "`db`.`table`", -1)
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

	closedEngine, err := s.engineMgr.UnsafeCloseEngineWithUUID(ctx, nil, "some_tag", engineUUID, 0)
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
		AppendRows(ctx, []string{"c1", "c2"}, rows1).
		Return(nil)
	mockWriter.EXPECT().Close(ctx).Return(nil, nil).AnyTimes()
	mockWriter.EXPECT().
		AppendRows(ctx, []string{"c1", "c2"}, rows2).
		Return(nil)

	engine, err := s.engineMgr.OpenEngine(ctx, &backend.EngineConfig{}, "`db`.`table`", 1)
	require.NoError(t, err)
	writer, err := engine.LocalWriter(ctx, &backend.LocalWriterConfig{TableName: "`db`.`table`"})
	require.NoError(t, err)
	err = writer.AppendRows(ctx, []string{"c1", "c2"}, rows1)
	require.NoError(t, err)
	err = writer.AppendRows(ctx, []string{"c1", "c2"}, rows2)
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
	mockWriter.EXPECT().AppendRows(ctx, gomock.Any(), emptyRows).Return(nil)
	mockWriter.EXPECT().Close(ctx).Return(nil, nil)
	s.mockBackend.EXPECT().LocalWriter(ctx, gomock.Any(), gomock.Any()).Return(mockWriter, nil)

	engine, err := s.engineMgr.OpenEngine(ctx, &backend.EngineConfig{}, "`db`.`table`", 1)
	require.NoError(t, err)
	writer, err := engine.LocalWriter(ctx, &backend.LocalWriterConfig{TableName: "`db`.`table`"})
	require.NoError(t, err)
	err = writer.AppendRows(ctx, nil, emptyRows)
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

	_, err := s.engineMgr.OpenEngine(ctx, &backend.EngineConfig{}, "`db`.`table`", 1)
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
		AppendRows(ctx, gomock.Any(), rows).
		Return(errors.Annotate(context.Canceled, "fake unrecoverable write error"))
	mockWriter.EXPECT().Close(ctx).Return(nil, nil)

	engine, err := s.engineMgr.OpenEngine(ctx, &backend.EngineConfig{}, "`db`.`table`", 1)
	require.NoError(t, err)
	writer, err := engine.LocalWriter(ctx, &backend.LocalWriterConfig{TableName: "`db`.`table`"})
	require.NoError(t, err)
	err = writer.AppendRows(ctx, nil, rows)
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
	mockWriter.EXPECT().AppendRows(ctx, gomock.Any(), rows).
		Return(errors.New("fake recoverable write batch error")).
		MinTimes(1)
	mockWriter.EXPECT().Close(ctx).Return(nil, nil).MinTimes(1)

	engine, err := s.engineMgr.OpenEngine(ctx, &backend.EngineConfig{}, "`db`.`table`", 1)
	require.NoError(t, err)
	writer, err := engine.LocalWriter(ctx, &backend.LocalWriterConfig{TableName: "`db`.`table`"})
	require.NoError(t, err)
	err = writer.AppendRows(ctx, nil, rows)
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

	closedEngine, err := s.engineMgr.UnsafeCloseEngine(ctx, nil, "`db`.`table`", 1)
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

	closedEngine, err := s.engineMgr.UnsafeCloseEngine(ctx, nil, "`db`.`table`", 1)
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

	closedEngine, err := s.engineMgr.UnsafeCloseEngine(ctx, nil, "`db`.`table`", 1)
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
	s.encBuilder.EXPECT().MakeEmptyRows().Return(rows)
	require.Equal(t, rows, s.encBuilder.MakeEmptyRows())
}

func TestNewEncoder(t *testing.T) {
	s := createBackendSuite(t)
	defer s.tearDownTest()

	encoder := mock.NewMockEncoder(s.controller)
	options := &encode.EncodingConfig{
		SessionOptions: encode.SessionOptions{SQLMode: mysql.ModeANSIQuotes, Timestamp: 1234567890},
	}
	s.encBuilder.EXPECT().NewEncoder(nil, options).Return(encoder, nil)

	realEncoder, err := s.encBuilder.NewEncoder(nil, options)
	require.Equal(t, realEncoder, encoder)
	require.NoError(t, err)
}
