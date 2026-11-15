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

package ddl

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/stretchr/testify/require"
)

type spfreshCleanupS3 struct {
	spfreshCleanupS3API
	objects     map[string]int64
	uploads     map[string]string
	failPartial bool
	deletes     int
}

func (s *spfreshCleanupS3) ListObjectsV2(_ context.Context, req *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	keys := make([]string, 0, len(s.objects))
	for key := range s.objects {
		if strings.HasPrefix(key, aws.ToString(req.Prefix)) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	if len(keys) > int(aws.ToInt32(req.MaxKeys)) {
		keys = keys[:aws.ToInt32(req.MaxKeys)]
	}
	out := &s3.ListObjectsV2Output{}
	for _, key := range keys {
		out.Contents = append(out.Contents, s3types.Object{Key: aws.String(key), Size: aws.Int64(s.objects[key])})
	}
	return out, nil
}

func (s *spfreshCleanupS3) DeleteObjects(_ context.Context, req *s3.DeleteObjectsInput, _ ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error) {
	s.deletes++
	if len(req.Delete.Objects) > 1000 {
		return nil, errors.New("unbounded delete batch")
	}
	out := &s3.DeleteObjectsOutput{}
	for i, object := range req.Delete.Objects {
		if s.failPartial && i == 0 {
			out.Errors = append(out.Errors, s3types.Error{Key: object.Key, Code: aws.String("AccessDenied")})
		} else {
			delete(s.objects, aws.ToString(object.Key))
		}
	}
	s.failPartial = false
	return out, nil
}

func (s *spfreshCleanupS3) ListMultipartUploads(_ context.Context, req *s3.ListMultipartUploadsInput, _ ...func(*s3.Options)) (*s3.ListMultipartUploadsOutput, error) {
	out := &s3.ListMultipartUploadsOutput{}
	for key, id := range s.uploads {
		if strings.HasPrefix(key, aws.ToString(req.Prefix)) {
			out.Uploads = append(out.Uploads, s3types.MultipartUpload{Key: aws.String(key), UploadId: aws.String(id)})
		}
	}
	return out, nil
}

func (s *spfreshCleanupS3) AbortMultipartUpload(_ context.Context, req *s3.AbortMultipartUploadInput, _ ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error) {
	delete(s.uploads, aws.ToString(req.Key))
	return &s3.AbortMultipartUploadOutput{}, nil
}

func TestSPFreshBulkCleanupRetriesPartialDeletesAndPreservesOtherRuns(t *testing.T) {
	client := &spfreshCleanupS3{objects: map[string]int64{"run-j10/data": 7}, uploads: map[string]string{"run-j1/uncommitted": "ours", "run-j10/uncommitted": "theirs"}, failPartial: true}
	for i := range 2001 {
		client.objects[fmt.Sprintf("run-j1/%04d", i)] = 10
	}
	allow := func() error { return nil }
	_, _, uploads, err := deleteSPFreshBulkObjects(context.Background(), client, "spfresh-mvp", "run-j1/", allow)
	require.ErrorContains(t, err, "AccessDenied")
	require.EqualValues(t, 1, uploads)
	require.Contains(t, client.objects, "run-j1/0000")
	_, _, _, err = deleteSPFreshBulkObjects(context.Background(), client, "spfresh-mvp", "run-j1/", allow)
	require.NoError(t, err)
	require.Equal(t, map[string]int64{"run-j10/data": 7}, client.objects)
	require.Equal(t, map[string]string{"run-j10/uncommitted": "theirs"}, client.uploads)
	objects, size, uploads, err := deleteSPFreshBulkObjects(context.Background(), client, "spfresh-mvp", "run-j1/", allow)
	require.NoError(t, err)
	require.Zero(t, objects)
	require.Zero(t, size)
	require.Zero(t, uploads)
}

func TestSPFreshBulkCleanupYieldsWithoutDeletingAndRejectsBroadPrefixes(t *testing.T) {
	client := &spfreshCleanupS3{objects: map[string]int64{"run-j1/data": 10}}
	for _, prefix := range []string{"", "run-j1", "/", "run-j1/nested/"} {
		_, _, _, err := deleteSPFreshBulkObjects(context.Background(), client, "spfresh-mvp", prefix, func() error { return nil })
		require.Error(t, err)
	}
	_, _, _, err := deleteSPFreshBulkObjects(context.Background(), client, "spfresh-mvp", "run-j1/", func() error { return errSPFreshBulkCleanupYield })
	require.ErrorIs(t, err, errSPFreshBulkCleanupYield)
	require.Zero(t, client.deletes)
	require.Len(t, client.objects, 1)
}

func TestSPFreshBulkCleanupFollowsJobCompletion(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	job := &model.Job{ID: 17, ReorgMeta: &model.DDLReorgMeta{SPFreshBulkRunID: "run-j17"}}
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMutator(txn)
	require.NoError(t, m.RegisterSPFreshBulkCleanup(meta.SPFreshBulkCleanup{JobID: 17, RunID: "run-j17", StorageURI: "s3://bucket/run-j17", WorkerEndpoints: []string{"http://worker:19000"}}))
	require.NoError(t, txn.Commit(context.Background()))
	txn, err = store.Begin()
	require.NoError(t, err)
	require.NoError(t, finishSPFreshBulkCleanup(&jobContext{metaMut: meta.NewMutator(txn)}, job))
	require.NoError(t, txn.Commit(context.Background()))
	version, err := store.CurrentVersion(kv.GlobalTxnScope)
	require.NoError(t, err)
	active, pending, err := meta.ReadSPFreshBulkCleanup(store.GetSnapshot(version), "", 1)
	require.NoError(t, err)
	require.False(t, active)
	require.Len(t, pending, 1)
}

func TestSPFreshBulkRetirementNotifiesEveryWorkerDespiteBusyReply(t *testing.T) {
	var called atomic.Int32
	busy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called.Add(1)
		http.Error(w, "busy while draining", http.StatusTooManyRequests)
	}))
	defer busy.Close()
	idle := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called.Add(1)
		_, _ = w.Write([]byte(`{"retired":true}`))
	}))
	defer idle.Close()
	err := retireSPFreshBulkWorkers(context.Background(), []string{busy.URL, idle.URL}, "run-j17")
	require.Error(t, err)
	require.Equal(t, int32(2), called.Load())
}

// A request can time out locally while the object store is still writing it.
// Empty LIST plus worker retirement is therefore only one cleanup sweep.
func TestSPFreshBulkCleanupReclaimsLatePutAfterOwnerRestart(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	ctx := context.Background()
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMutator(txn)
	require.NoError(t, m.RegisterSPFreshBulkCleanup(meta.SPFreshBulkCleanup{JobID: 17, RunID: "run-j17", StorageURI: "s3://bucket/run-j17", WorkerEndpoints: []string{"http://worker:19000"}}))
	require.NoError(t, m.FinishSPFreshBulkCleanup(17, "run-j17"))
	require.NoError(t, txn.Commit(ctx))
	read := func() *meta.SPFreshBulkCleanup {
		version, err := store.CurrentVersion(kv.GlobalTxnScope)
		require.NoError(t, err)
		active, records, err := meta.ReadSPFreshBulkCleanup(store.GetSnapshot(version), "", 1)
		require.NoError(t, err)
		require.False(t, active)
		require.Len(t, records, 1)
		return records[0]
	}
	record := read()
	now := time.UnixMilli(record.FinishedAtUnixMilli).Add(spfreshBulkCleanupInterval)
	require.True(t, spfreshBulkCleanupDue(record, now))
	client := &spfreshCleanupS3{objects: map[string]int64{"other-run/data": 9}}
	count, _, _, err := deleteSPFreshBulkObjects(ctx, client, "bucket", "run-j17/", func() error { return nil })
	require.NoError(t, err)
	require.Zero(t, count)
	txn, err = store.Begin()
	require.NoError(t, err)
	require.NoError(t, meta.NewMutator(txn).ScheduleSPFreshBulkCleanup(record.RunID, now.Add(spfreshBulkCleanupResweepInterval).UnixMilli()))
	require.NoError(t, txn.Commit(ctx))
	// The old PUT completes only AFTER the first sweep has seen an empty prefix.
	client.objects["run-j17/late-non-root.spfmvp"] = 123
	// A fresh owner uses only persisted state, without an old in-memory task.
	record = read()
	require.False(t, spfreshBulkCleanupDue(record, now.Add(time.Second)))
	require.True(t, spfreshBulkCleanupDue(record, now.Add(spfreshBulkCleanupResweepInterval)))
	count, size, _, err := deleteSPFreshBulkObjects(ctx, client, "bucket", "run-j17/", func() error { return nil })
	require.NoError(t, err)
	require.EqualValues(t, 1, count)
	require.EqualValues(t, 123, size)
	require.Equal(t, map[string]int64{"other-run/data": 9}, client.objects)
	require.Equal(t, "run-j17", read().RunID)
}
