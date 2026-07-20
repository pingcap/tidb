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

package importinto

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/stretchr/testify/require"
)

func TestImportCleanUpBatchUsesUnredactedStorageCredentials(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("this test is for nextgen kernel only")
	}

	const accessKey = "cleanup-access-key"
	taskIDs := []int64{42, 43}
	backend := s3mem.New()
	require.NoError(t, backend.CreateBucket("cleanup-bucket"))
	fakeS3 := gofakes3.New(backend).Server()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.Header.Get("Authorization"), "Credential="+accessKey+"/") {
			http.Error(w, "unexpected access key", http.StatusForbidden)
			return
		}
		fakeS3.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)

	ctx := context.Background()
	cloudStorageURI := fmt.Sprintf(
		"s3://cleanup-bucket/import?region=us-east-1&endpoint=%s&access-key=%s&secret-access-key=cleanup-secret&force-path-style=true",
		server.URL,
		accessKey,
	)
	store, err := importer.GetSortStore(ctx, cloudStorageURI)
	require.NoError(t, err)
	for _, taskID := range taskIDs {
		require.NoError(t, store.WriteFile(ctx, fmt.Sprintf("%d/data", taskID), []byte("data")))
	}
	store.Close()

	tasks := make([]*proto.Task, 0, len(taskIDs))
	for _, taskID := range taskIDs {
		taskMeta, err := json.Marshal(TaskMeta{Plan: importer.Plan{CloudStorageURI: cloudStorageURI}})
		require.NoError(t, err)
		tasks = append(tasks, &proto.Task{
			TaskBase: proto.TaskBase{ID: taskID, State: proto.TaskStateFailed},
			Meta:     taskMeta,
		})
	}

	require.NoError(t, (&ImportCleanUp{}).CleanUpBatch(ctx, tasks))

	store, err = importer.GetSortStore(ctx, cloudStorageURI)
	require.NoError(t, err)
	t.Cleanup(store.Close)
	for _, task := range tasks {
		exists, err := store.FileExists(ctx, fmt.Sprintf("%d/data", task.ID))
		require.NoError(t, err)
		require.False(t, exists)
		require.NotContains(t, string(task.Meta), accessKey)
		require.Contains(t, string(task.Meta), "access-key=xxxxxx")
	}
}
