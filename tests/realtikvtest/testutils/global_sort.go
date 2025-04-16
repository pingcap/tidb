// Copyright 2025 PingCAP, Inc.
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

package testutils

import (
	"context"
	goerrors "errors"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/stretchr/testify/require"
)

// RemoveAllObjects removes all objects in the bucket.
func RemoveAllObjects(t *testing.T, server *fakestorage.Server, bucket string) {
	t.Helper()
	objectAttrs, _, err := server.ListObjectsWithOptions(bucket, fakestorage.ListOptions{})
	require.NoError(t, err)
	bucketHandle := server.Client().Bucket(bucket)
	for _, attr := range objectAttrs {
		err := bucketHandle.Object(attr.Name).Delete(context.Background())
		if goerrors.Is(err, storage.ErrObjectNotExist) {
			// DXF cleanup might also delete the object, so we ignore the error.
			continue
		}
		require.NoError(t, err)
	}
}
