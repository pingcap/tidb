// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mockserver

import (
	"context"
	"fmt"
	"io"

	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"google.golang.org/grpc/metadata"
)

type WriteServer struct{}

func (w *WriteServer) SendAndClose(response *import_sstpb.WriteResponse) error {
	//TODO implement me
	panic("implement me")
}

func (w *WriteServer) Recv() (*import_sstpb.WriteRequest, error) {
	//TODO implement me
	panic("implement me")
}

func (w *WriteServer) SetHeader(md metadata.MD) error {
	//TODO implement me
	panic("implement me")
}

func (w *WriteServer) SendHeader(md metadata.MD) error {
	//TODO implement me
	panic("implement me")
}

func (w *WriteServer) SetTrailer(md metadata.MD) {
	//TODO implement me
	panic("implement me")
}

func (w *WriteServer) Context() context.Context {
	//TODO implement me
	panic("implement me")
}

func (w *WriteServer) SendMsg(m interface{}) error {
	//TODO implement me
	panic("implement me")
}

func (w *WriteServer) RecvMsg(m interface{}) error {
	//TODO implement me
	panic("implement me")
}

type MockImportSSTServer struct{}

func (m *MockImportSSTServer) SwitchMode(ctx context.Context, request *import_sstpb.SwitchModeRequest) (*import_sstpb.SwitchModeResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (m *MockImportSSTServer) GetMode(ctx context.Context, request *import_sstpb.GetModeRequest) (*import_sstpb.GetModeResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (m *MockImportSSTServer) Upload(server import_sstpb.ImportSST_UploadServer) error {
	//TODO implement me
	panic("implement me")
}

func (m *MockImportSSTServer) Ingest(ctx context.Context, request *import_sstpb.IngestRequest) (*import_sstpb.IngestResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (m *MockImportSSTServer) Compact(ctx context.Context, request *import_sstpb.CompactRequest) (*import_sstpb.CompactResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (m *MockImportSSTServer) SetDownloadSpeedLimit(ctx context.Context, request *import_sstpb.SetDownloadSpeedLimitRequest) (*import_sstpb.SetDownloadSpeedLimitResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (m *MockImportSSTServer) Download(ctx context.Context, request *import_sstpb.DownloadRequest) (*import_sstpb.DownloadResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (m *MockImportSSTServer) Write(server import_sstpb.ImportSST_WriteServer) error {
	fmt.Println("Write")
	for {
		request, err := server.Recv()
		if err == io.EOF {
			fmt.Println("EOF")
			return nil
		}
		if err != nil {
			fmt.Printf("err: %v\n", err)
			return err
		}
		if meta := request.GetMeta(); meta != nil {
			fmt.Printf("meta: %v\n", meta.String())
		}
		if batch := request.GetBatch(); batch != nil {
			fmt.Printf("chunk: %v\n", batch.String())
		}
		err = server.SendAndClose(&import_sstpb.WriteResponse{
			Metas: []*import_sstpb.SSTMeta{
				{
					Uuid: []byte("got"),
				},
			},
		})
		if err != nil {
			fmt.Printf("err: %v\n", err)
			return err
		}
	}
}

func (m *MockImportSSTServer) RawWrite(server import_sstpb.ImportSST_RawWriteServer) error {
	//TODO implement me
	panic("implement me")
}

func (m *MockImportSSTServer) MultiIngest(ctx context.Context, request *import_sstpb.MultiIngestRequest) (*import_sstpb.IngestResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (m *MockImportSSTServer) DuplicateDetect(request *import_sstpb.DuplicateDetectRequest, server import_sstpb.ImportSST_DuplicateDetectServer) error {
	//TODO implement me
	panic("implement me")
}

func (m *MockImportSSTServer) Apply(ctx context.Context, request *import_sstpb.ApplyRequest) (*import_sstpb.ApplyResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (m *MockImportSSTServer) ClearFiles(ctx context.Context, request *import_sstpb.ClearRequest) (*import_sstpb.ClearResponse, error) {
	//TODO implement me
	panic("implement me")
}
