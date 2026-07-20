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
	taskIDs := []int64{42, 43, 44}
	buckets := []string{"cleanup-bucket", "cleanup-bucket", "other-cleanup-bucket"}
	backend := s3mem.New()
	require.NoError(t, backend.CreateBucket("cleanup-bucket"))
	require.NoError(t, backend.CreateBucket("other-cleanup-bucket"))
	fakeS3 := gofakes3.New(backend).Server()
	listRequests := make(chan string, len(taskIDs))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.Header.Get("Authorization"), "Credential="+accessKey+"/") {
			http.Error(w, "unexpected access key", http.StatusForbidden)
			return
		}
		if r.Method == http.MethodGet && r.URL.Query().Has("list-type") {
			listRequests <- strings.TrimSuffix(r.URL.Path, "/")
		}
		fakeS3.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)

	ctx := context.Background()
	cloudStorageURIs := make([]string, 0, len(buckets))
	for i, bucket := range buckets {
		cloudStorageURI := fmt.Sprintf(
			"s3://%s/import?region=us-east-1&endpoint=%s&access-key=%s&secret-access-key=cleanup-secret&force-path-style=true",
			bucket,
			server.URL,
			accessKey,
		)
		cloudStorageURIs = append(cloudStorageURIs, cloudStorageURI)
		store, err := importer.GetSortStore(ctx, cloudStorageURI)
		require.NoError(t, err)
		taskID := taskIDs[i]
		require.NoError(t, store.WriteFile(ctx, fmt.Sprintf("%d/data", taskID), []byte("data")))
		if i == 0 || i == 2 {
			require.NoError(t, store.WriteFile(ctx, "kept/data", []byte("data")))
		}
		store.Close()
	}

	tasks := make([]*proto.Task, 0, len(taskIDs))
	for i, taskID := range taskIDs {
		taskMeta, err := json.Marshal(TaskMeta{Plan: importer.Plan{CloudStorageURI: cloudStorageURIs[i]}})
		require.NoError(t, err)
		tasks = append(tasks, &proto.Task{
			TaskBase: proto.TaskBase{ID: taskID, State: proto.TaskStateFailed},
			Meta:     taskMeta,
		})
	}

	require.NoError(t, (&ImportCleanUp{}).CleanUpBatch(ctx, tasks))
	require.Equal(t, 2, len(listRequests))
	listedBuckets := []string{<-listRequests, <-listRequests}
	require.ElementsMatch(t, []string{"/cleanup-bucket", "/other-cleanup-bucket"}, listedBuckets)

	for i, task := range tasks {
		store, err := importer.GetSortStore(ctx, cloudStorageURIs[i])
		require.NoError(t, err)
		exists, err := store.FileExists(ctx, fmt.Sprintf("%d/data", task.ID))
		require.NoError(t, err)
		require.False(t, exists)
		if i == 0 || i == 2 {
			exists, err = store.FileExists(ctx, "kept/data")
			require.NoError(t, err)
			require.True(t, exists)
		}
		store.Close()
		require.NotContains(t, string(task.Meta), accessKey)
		require.Contains(t, string(task.Meta), "access-key=xxxxxx")
	}
}
