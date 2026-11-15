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
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend/remote"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/util"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const spfreshBulkLifecycleObject = "lifecycle"
const spfreshBulkCleanupInterval = 5 * time.Second
const spfreshBulkCleanupResweepInterval = time.Hour

var errSPFreshBulkCleanupYield = errors.New("SPFresh bulk cleanup yielded to an active build or new owner")

func finishSPFreshBulkCleanup(jobCtx *jobContext, job *model.Job) error {
	if job.ReorgMeta != nil && job.ReorgMeta.SPFreshBulkRunID != "" {
		return jobCtx.metaMut.FinishSPFreshBulkCleanup(job.ID, job.ReorgMeta.SPFreshBulkRunID)
	}
	return nil
}

func initializeSPFreshBulkLifecycle(ctx context.Context, objectStore storage.ExternalStorage) error {
	s3Store, ok := objectStore.(*storage.S3Storage)
	if !ok {
		return errors.New("SPFresh bulk lifecycle requires S3 storage")
	}
	client, err := spfreshCleanupClient(s3Store)
	if err != nil {
		return err
	}
	if err := checkSPFreshBulkBucketVersioning(ctx, client, s3Store.GetOptions().Bucket); err != nil {
		return err
	}
	exists, err := objectStore.FileExists(ctx, spfreshBulkLifecycleObject)
	if err != nil {
		return err
	}
	if exists {
		return errors.New("SPFresh bulk lifecycle already exists; use a new build job")
	}
	return objectStore.WriteFile(ctx, spfreshBulkLifecycleObject, []byte("active\n"))
}

func checkSPFreshBulkBucketVersioning(ctx context.Context, client spfreshCleanupS3API, bucket string) error {
	result, err := client.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{Bucket: aws.String(bucket)})
	if err != nil {
		return errors.Annotate(err, "read SPFresh bulk bucket versioning")
	}
	if string(result.Status) != "" {
		return errors.New("SPFresh bulk temporary bucket must have versioning disabled")
	}
	return nil
}

func (d *ddl) readSPFreshBulkCleanup(after string) (bool, []*meta.SPFreshBulkCleanup, error) {
	version, err := d.store.CurrentVersion(kv.GlobalTxnScope)
	if err != nil {
		return false, nil, err
	}
	return meta.ReadSPFreshBulkCleanup(d.store.GetSnapshot(version), after, 16)
}

func spfreshBulkCleanupDue(record *meta.SPFreshBulkCleanup, now time.Time) bool {
	return now.Sub(time.UnixMilli(record.FinishedAtUnixMilli)) >= spfreshBulkCleanupInterval && now.UnixMilli() >= record.NextSweepAtUnixMilli
}

func (d *ddl) runSPFreshBulkCleanupLoop() {
	ticker := time.NewTicker(spfreshBulkCleanupInterval)
	defer ticker.Stop()
	var after string
	for {
		select {
		case <-d.ctx.Done():
			return
		case <-ticker.C:
		}
		if !d.ownerManager.IsOwner() {
			continue
		}
		active, pending, err := d.readSPFreshBulkCleanup(after)
		if err != nil {
			logutil.DDLLogger().Warn("read SPFresh bulk cleanup queue", zap.Error(err))
			continue
		}
		if active {
			continue
		}
		if len(pending) == 0 {
			after = ""
		}
		for _, record := range pending {
			after = record.RunID
			// Leave DDL completion/notification off the deletion critical path.
			if !spfreshBulkCleanupDue(record, time.Now()) {
				continue
			}
			ctx, cancel := context.WithTimeout(d.ctx, 30*time.Second)
			err = d.cleanSPFreshBulkRun(ctx, record)
			cancel()
			if errors.Cause(err) == errSPFreshBulkCleanupYield {
				break
			}
			if err != nil {
				metrics.SPFreshBulkCleanupRuns.WithLabelValues("retry").Inc()
				logutil.DDLLogger().Warn("SPFresh bulk cleanup remains pending", zap.Int64("job_id", record.JobID), zap.String("build_id", record.RunID), zap.Error(err))
			}
		}
	}
}

func (d *ddl) cleanSPFreshBulkRun(ctx context.Context, record *meta.SPFreshBulkCleanup) error {
	started := time.Now()
	runID, _, err := parseSPFreshBulkStorageURI(record.StorageURI)
	if err != nil {
		return err
	}
	if runID != record.RunID {
		return errors.New("SPFresh cleanup storage prefix differs from its durable owner")
	}
	backend, err := storage.ParseBackend(record.StorageURI, nil)
	if err != nil {
		return err
	}
	objectStore, err := storage.NewWithDefaultOpt(ctx, backend)
	if err != nil {
		return err
	}
	defer objectStore.Close()
	s3Store, ok := objectStore.(*storage.S3Storage)
	if !ok {
		return errors.New("SPFresh cleanup requires S3 storage")
	}
	client, err := spfreshCleanupClient(s3Store)
	if err != nil {
		return err
	}
	options := s3Store.GetOptions()
	if err := checkSPFreshBulkBucketVersioning(ctx, client, options.Bucket); err != nil {
		return err
	}
	canDelete := func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !d.ownerManager.IsOwner() {
			return errSPFreshBulkCleanupYield
		}
		active, _, err := d.readSPFreshBulkCleanup("")
		if err != nil {
			return err
		}
		if active {
			return errSPFreshBulkCleanupYield
		}
		return nil
	}
	if err := canDelete(); err != nil {
		return err
	}
	// Missing lifecycle is never an active run. Recreating only the closed marker
	// is safe on every sweep. Keep the durable queue row for late remote writes.
	if err := objectStore.WriteFile(ctx, spfreshBulkLifecycleObject, []byte("closed\n")); err != nil {
		return err
	}
	if err := retireSPFreshBulkWorkers(ctx, record.WorkerEndpoints, record.RunID); err != nil {
		return err
	}
	objects, size, uploads, err := deleteSPFreshBulkObjects(ctx, client, options.Bucket, options.Prefix, canDelete)
	if err != nil {
		return err
	}
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnDDL)
	if err := kv.RunInNewTxn(ctx, d.store, true, func(_ context.Context, txn kv.Transaction) error {
		return meta.NewMutator(txn).ScheduleSPFreshBulkCleanup(record.RunID, time.Now().Add(spfreshBulkCleanupResweepInterval).UnixMilli())
	}); err != nil {
		return err
	}
	metrics.SPFreshBulkCleanupRuns.WithLabelValues("success").Inc()
	metrics.SPFreshBulkCleanupDuration.Observe(time.Since(started).Seconds())
	logutil.DDLLogger().Info("SPFresh bulk cleanup sweep completed", zap.Int64("job_id", record.JobID), zap.String("build_id", record.RunID), zap.Int64("objects", objects), zap.Int64("bytes", size), zap.Int64("multipart_uploads", uploads), zap.Duration("duration", time.Since(started)))
	return nil
}

// deleteSPFreshBulkObjects drains one exact, already fenced prefix in bounded
// batches. Every retry lists the remaining objects; no correctness input uses LIST.
func deleteSPFreshBulkObjects(ctx context.Context, client spfreshCleanupS3API, bucket, prefix string, canDelete func() error) (objects, size, uploads int64, err error) {
	if len(prefix) <= 1 || !strings.HasSuffix(prefix, "/") || strings.Count(prefix, "/") != 1 {
		return 0, 0, 0, errors.New("SPFresh cleanup requires one exact run prefix with a trailing slash")
	}
	// Aborted uploads cannot later be completed by a crashed request. Barrier
	// acknowledgement above drains client attempts, not timed-out server writes.
	// The retained queue record covers uploads/objects that appear after this sweep.
	for {
		if err = canDelete(); err != nil {
			return
		}
		var page *s3.ListMultipartUploadsOutput
		page, err = client.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{Bucket: aws.String(bucket), Prefix: aws.String(prefix), MaxUploads: aws.Int32(1000)})
		if err != nil {
			return
		}
		if len(page.Uploads) == 0 {
			break
		}
		for _, upload := range page.Uploads {
			if !strings.HasPrefix(aws.ToString(upload.Key), prefix) || aws.ToString(upload.UploadId) == "" {
				err = errors.New("SPFresh multipart list has an invalid upload identity")
				return
			}
			if err = canDelete(); err != nil {
				return
			}
			_, err = client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: aws.String(bucket), Key: upload.Key, UploadId: upload.UploadId})
			if awsErr, ok := errors.Cause(err).(smithy.APIError); ok && awsErr.ErrorCode() == "NoSuchUpload" {
				err = nil
			}
			if err != nil {
				return
			}
			uploads++
		}
	}
	for {
		if err = canDelete(); err != nil {
			return
		}
		var page *s3.ListObjectsV2Output
		page, err = client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String(prefix), MaxKeys: aws.Int32(1000)})
		if err != nil {
			return
		}
		if len(page.Contents) == 0 {
			return
		}
		batch := make([]s3types.ObjectIdentifier, 0, len(page.Contents))
		for _, object := range page.Contents {
			if !strings.HasPrefix(aws.ToString(object.Key), prefix) {
				err = errors.New("SPFresh object list escaped its run prefix")
				return
			}
			batch = append(batch, s3types.ObjectIdentifier{Key: object.Key})
		}
		if err = canDelete(); err != nil {
			return
		}
		var deleted *s3.DeleteObjectsOutput
		deleted, err = client.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: aws.String(bucket), Delete: &s3types.Delete{Objects: batch, Quiet: aws.Bool(true)}})
		if err != nil {
			return
		}
		// S3 can report individual failures with HTTP 200. Account for the
		// acknowledged subset while preserving the queue record for retry.
		failed := make(map[string]struct{}, len(deleted.Errors))
		for _, failure := range deleted.Errors {
			failed[aws.ToString(failure.Key)] = struct{}{}
		}
		var deletedObjects, deletedBytes int64
		for _, object := range page.Contents {
			if _, ok := failed[aws.ToString(object.Key)]; !ok {
				deletedObjects++
				deletedBytes += aws.ToInt64(object.Size)
			}
		}
		objects += deletedObjects
		size += deletedBytes
		metrics.SPFreshBulkCleanupObjects.Add(float64(deletedObjects))
		metrics.SPFreshBulkCleanupBytes.Add(float64(deletedBytes))
		if len(deleted.Errors) != 0 {
			err = errors.Errorf("SPFresh bulk object deletion failed: %s", aws.ToString(deleted.Errors[0].Code))
			return
		}
		select {
		case <-ctx.Done():
			err = ctx.Err()
			return
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// Notify every endpoint even when an earlier one is busy. The shared caller
// deadline bounds a sweep; pending cleanup durably retries until all writers exit.
func retireSPFreshBulkWorkers(ctx context.Context, endpoints []string, runID string) error {
	var group errgroup.Group
	for _, endpoint := range endpoints {
		group.Go(func() error { return remote.RetireSPFreshBulkBuild(ctx, util.InternalHTTPClient(), endpoint, runID) })
	}
	return group.Wait()
}

// Failure closes admission immediately rather than waiting for terminal DDL
// metadata. This does not delete anything; the durable cleanup queue owns that.
func cancelSPFreshBulkRun(run *spfreshBulkRunContext) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	backend, err := storage.ParseBackend(run.cloudStorageURI, nil)
	if err != nil {
		return err
	}
	objectStore, err := storage.NewWithDefaultOpt(ctx, backend)
	if err != nil {
		return err
	}
	defer objectStore.Close()
	if err := objectStore.WriteFile(ctx, spfreshBulkLifecycleObject, []byte("closed\n")); err != nil {
		return err
	}
	return retireSPFreshBulkWorkers(ctx, run.workerEndpoints, run.runID)
}

// Only the lifecycle operations needed by SPFresh are required of the S3 client.
type spfreshCleanupS3API interface {
	GetBucketVersioning(context.Context, *s3.GetBucketVersioningInput, ...func(*s3.Options)) (*s3.GetBucketVersioningOutput, error)
	ListMultipartUploads(context.Context, *s3.ListMultipartUploadsInput, ...func(*s3.Options)) (*s3.ListMultipartUploadsOutput, error)
	AbortMultipartUpload(context.Context, *s3.AbortMultipartUploadInput, ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error)
	ListObjectsV2(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	DeleteObjects(context.Context, *s3.DeleteObjectsInput, ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error)
}

func spfreshCleanupClient(store *storage.S3Storage) (spfreshCleanupS3API, error) {
	client, ok := store.GetS3APIHandle().(spfreshCleanupS3API)
	if !ok {
		return nil, errors.New("S3 client does not support SPFresh lifecycle operations")
	}
	return client, nil
}
