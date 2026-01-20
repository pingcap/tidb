// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ossstore

import (
	"fmt"
	"io"
	"path"
	"testing"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	"github.com/docker/go-units"
	"github.com/google/uuid"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/dumpling/context"
	"github.com/pingcap/tidb/pkg/objstore/recording"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestStore(t *testing.T) {
	// example: acs:ram::00000000000000:role
	roleARNPrefix := "place-holder"
	accessKey := "place-holder"
	secretKey := "place-holder"
	t.Skip("this test requires real oss credentials, skip for normal ci")

	ctx := context.Background()
	s3Be := &backuppb.S3{
		Bucket: "tidbx-gsort",
		Prefix: "test-prefix",
	}

	t.Run("invalid region", func(t *testing.T) {
		// when get the region of the bucket, it will check the bucket existence
		be := *s3Be
		be.Region = "cn-sss"
		_, err := NewOSSStorage(ctx, &be,
			&storeapi.Options{
				AccessRecording: &recording.AccessStats{},
				CheckPermissions: []storeapi.Permission{
					storeapi.AccessBuckets,
				},
			},
		)
		require.ErrorContains(t, err, "failed to get location of bucket")
	})

	t.Run("region mismatch", func(t *testing.T) {
		// when get the region of the bucket, it will check the bucket existence
		be := *s3Be
		be.Region = "cn-hangzhou"
		_, err := NewOSSStorage(ctx, &be,
			&storeapi.Options{
				AccessRecording: &recording.AccessStats{},
				CheckPermissions: []storeapi.Permission{
					storeapi.AccessBuckets,
				},
			},
		)
		require.ErrorContains(t, err, "bucket and region are not matched")
	})

	t.Run("bucket existence check", func(t *testing.T) {
		// when get the region of the bucket, it will check the bucket existence
		be := *s3Be
		be.Bucket = uuid.New().String()
		_, err := NewOSSStorage(ctx, &be,
			&storeapi.Options{
				AccessRecording: &recording.AccessStats{},
				CheckPermissions: []storeapi.Permission{
					storeapi.AccessBuckets,
				},
			},
		)
		require.ErrorContains(t, err, "NoSuchBucket")
	})

	t.Run("object access permission check fail", func(t *testing.T) {
		for _, c := range []struct {
			perm storeapi.Permission
			role string
		}{
			{storeapi.ListObjects, path.Join(roleARNPrefix, "tidbx-gsort-only-get")},
			{storeapi.GetObject, path.Join(roleARNPrefix, "tidbx-gsort-only-list")},
			{storeapi.PutAndDeleteObject, path.Join(roleARNPrefix, "tidbx-gsort-only-get")},
			{storeapi.PutAndDeleteObject, path.Join(roleARNPrefix, "tidbx-gsort-only-put")},
			{storeapi.PutAndDeleteObject, path.Join(roleARNPrefix, "tidbx-gsort-only-delete")},
		} {
			be := *s3Be
			be.RoleArn = c.role
			_, err := NewOSSStorage(ctx, &be,
				&storeapi.Options{
					AccessRecording: &recording.AccessStats{},
					CheckPermissions: []storeapi.Permission{
						c.perm,
					},
				},
			)
			require.ErrorContains(t, err, fmt.Sprintf("check permission failed due to permission %s", c.perm))
		}
	})

	t.Run("object access permission check success", func(t *testing.T) {
		for _, c := range []struct {
			perm storeapi.Permission
			role string
		}{
			{storeapi.ListObjects, path.Join(roleARNPrefix, "tidbx-gsort-only-list")},
			{storeapi.GetObject, path.Join(roleARNPrefix, "tidbx-gsort-only-get")},
			{storeapi.PutAndDeleteObject, path.Join(roleARNPrefix, "tidbx-gsort-only-put-delete")},
		} {
			be := *s3Be
			be.RoleArn = c.role
			_, err := NewOSSStorage(ctx, &be,
				&storeapi.Options{
					AccessRecording: &recording.AccessStats{},
					CheckPermissions: []storeapi.Permission{
						c.perm,
					},
				},
			)
			require.NoError(t, err)
		}
	})

	t.Run("access through AK/SK", func(t *testing.T) {
		be := *s3Be
		be.AccessKey = accessKey
		be.SecretAccessKey = secretKey
		_, err := NewOSSStorage(ctx, &be,
			&storeapi.Options{
				AccessRecording: &recording.AccessStats{},
				CheckPermissions: []storeapi.Permission{
					storeapi.AccessBuckets,
					storeapi.ListObjects,
					storeapi.GetObject,
					storeapi.PutAndDeleteObject,
				},
			},
		)
		require.NoError(t, err)
	})

	t.Run("access object", func(t *testing.T) {
		be := *s3Be
		be.RoleArn = path.Join(roleARNPrefix, "tidbx-gsort-rw")
		store, err := NewOSSStorage(ctx, &be,
			&storeapi.Options{
				AccessRecording: &recording.AccessStats{},
				CheckPermissions: []storeapi.Permission{
					storeapi.AccessBuckets,
					storeapi.ListObjects,
					storeapi.GetObject,
					storeapi.PutAndDeleteObject,
				},
			},
		)
		require.NoError(t, err)

		key := "test-obj"
		data := make([]byte, 100*units.KiB)
		copy(data[100:], "hello oss store. length 25")
		err = store.WriteFile(ctx, key, data)
		require.NoError(t, err)
		bytes, err := store.ReadFile(ctx, key)
		require.NoError(t, err)
		require.Equal(t, data, bytes)
		exist, err := store.FileExists(ctx, key)
		require.NoError(t, err)
		require.True(t, exist)

		reader, err := store.Open(ctx, key, &storeapi.ReaderOption{
			StartOffset: oss.Ptr(int64(100)),
			EndOffset:   oss.Ptr(int64(100 + 25 + 1)),
		})
		require.NoError(t, err)
		bytes, err = io.ReadAll(reader)
		require.NoError(t, err)
		require.Equal(t, []byte("hello oss store. length 25"), bytes)
		require.NoError(t, reader.Close())

		err = store.DeleteFile(ctx, key)
		require.NoError(t, err)
		exist, err = store.FileExists(ctx, key)
		require.NoError(t, err)
		require.False(t, exist)

		keys := make([]string, 0, 10)
		for i := range 10 {
			key := fmt.Sprintf("create-%d", i)
			writer, err := store.Create(ctx, key, nil)
			require.NoError(t, err)
			n, err := writer.Write(ctx, data)
			require.NoError(t, err)
			require.Equal(t, len(data), n)
			require.NoError(t, writer.Close(ctx))

			keys = append(keys, key)
		}
		err = store.DeleteFiles(ctx, keys)
		require.NoError(t, err)
	})

	t.Run("upload large file", func(t *testing.T) {
		be := *s3Be
		be.RoleArn = path.Join(roleARNPrefix, "tidbx-gsort-rw")
		store, err := NewOSSStorage(ctx, &be, &storeapi.Options{AccessRecording: &recording.AccessStats{}})
		require.NoError(t, err)
		writer, err := store.Create(ctx, "large-file", &storeapi.WriterOption{
			PartSize:    5 * units.MiB,
			Concurrency: 20,
		})
		require.NoError(t, err)
		buf := make([]byte, 3*units.MiB)
		for i := 0; i < 20; i++ {
			_, err := writer.Write(ctx, buf)
			require.NoError(t, err)
		}
		require.NoError(t, writer.Close(ctx))
	})

	t.Run("test copy object", func(t *testing.T) {
		accessStats := &recording.AccessStats{}
		srcStore, err := NewOSSStorage(ctx, &backuppb.S3{
			Bucket: "tidbx-gsort-2",
			Prefix: "p-for-src-files",
		}, &storeapi.Options{AccessRecording: accessStats})
		require.NoError(t, err)
		err = srcStore.WriteFile(ctx, "src-file", []byte("hello oss store copy"))
		require.NoError(t, err)
		require.EqualValues(t, 1, accessStats.Requests.Put.Load())
		be := *s3Be
		be.RoleArn = path.Join(roleARNPrefix, "tidbx-gsort-rw")
		dstAccess := &recording.AccessStats{}
		dstStore, err := NewOSSStorage(ctx, &be,
			&storeapi.Options{
				AccessRecording: dstAccess,
				CheckPermissions: []storeapi.Permission{
					storeapi.AccessBuckets,
					storeapi.ListObjects,
					storeapi.GetObject,
					storeapi.PutAndDeleteObject,
				},
			},
		)
		require.NoError(t, err)
		err = dstStore.CopyFrom(ctx, srcStore, storeapi.CopySpec{
			From: "src-file",
			To:   "dst-file",
		})
		require.NoError(t, err)
		require.EqualValues(t, 1, accessStats.Requests.Put.Load())
		bytes, err := dstStore.ReadFile(ctx, "dst-file")
		require.NoError(t, err)
		require.Equal(t, []byte("hello oss store copy"), bytes)
		require.EqualValues(t, 1, accessStats.Requests.Put.Load())
		require.EqualValues(t, 1, accessStats.Requests.Get.Load())
	})

	t.Run("test walk", func(t *testing.T) {
		be := *s3Be
		be.Prefix = "walk-test"
		be.RoleArn = path.Join(roleARNPrefix, "tidbx-gsort-rw")
		access := &recording.AccessStats{}
		store, err := NewOSSStorage(ctx, &be, &storeapi.Options{AccessRecording: access})
		require.NoError(t, err)

		allFileNames := make([]string, 0, 1500)
		filesInDir := make(map[string][]string)
		for _, sub := range []string{"a", "aa", "aaa", "b", "c"} {
			for i := range 300 {
				name := fmt.Sprintf("%s/file-%03d.txt", sub, i)
				allFileNames = append(allFileNames, name)
				filesInDir[sub] = append(filesInDir[sub], name)
			}
		}
		prepareFn := func() {
			eg, egCtx := errgroup.WithContext(ctx)
			eg.SetLimit(100)
			for _, name := range allFileNames {
				if egCtx.Err() != nil {
					break
				}
				eg.Go(func() error {
					return store.WriteFile(ctx, name, []byte("empty"))
				})
			}
			require.NoError(t, eg.Wait())
		}
		prepareFn()

		walkFn := func(walkOpt *storeapi.WalkOption) []string {
			gotFiles := make([]string, 0, 1500)
			err = store.WalkDir(ctx, walkOpt, func(path string, size int64) error {
				gotFiles = append(gotFiles, path)
				require.EqualValues(t, 5, size)
				return nil
			})
			require.NoError(t, err)
			return gotFiles
		}
		require.EqualValues(t, allFileNames, walkFn(&storeapi.WalkOption{}))
		require.EqualValues(t, allFileNames, walkFn(&storeapi.WalkOption{ListCount: 333}))
		gotFiles := walkFn(&storeapi.WalkOption{SubDir: "a"})
		require.EqualValues(t, filesInDir["a"], gotFiles)
		gotFiles = walkFn(&storeapi.WalkOption{SubDir: "aa"})
		require.EqualValues(t, filesInDir["aa"], gotFiles)
		gotFiles = walkFn(&storeapi.WalkOption{SubDir: "aa", ObjPrefix: "file-1", ListCount: 13})
		require.EqualValues(t, filesInDir["aa"][100:200], gotFiles)
		gotFiles = walkFn(&storeapi.WalkOption{SubDir: "aa", ObjPrefix: "file-11"})
		require.EqualValues(t, filesInDir["aa"][110:120], gotFiles)
		gotFiles = walkFn(&storeapi.WalkOption{SubDir: "b"})
		require.EqualValues(t, filesInDir["b"], gotFiles)
	})
}
