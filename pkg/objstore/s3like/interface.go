// Copyright 2025 PingCAP, Inc.
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

package s3like

import (
	"context"
	goerrors "errors"
	"io"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/objstore/objectio"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
)

var (
	ErrNoSuchBucket = goerrors.New("no such bucket")
)

// GetResp is the response of GetObject.
type GetResp struct {
	Body          io.ReadCloser
	IsFullRange   bool
	ContentLength *int64
	ContentRange  *string
}

// Object is the object info.
type Object struct {
	Key  string
	Size int64
}

// ListResp is the response of ListObjects.
type ListResp struct {
	NextMarker  *string
	IsTruncated bool
	Objects     []Object
}

// CopyInput is the input of CopyObject.
type CopyInput struct {
	FromLoc storeapi.BucketPrefix
	// relative to FromLoc
	FromKey string
	// relative to the PrefixClient
	ToKey string
}

// Uploader is used to abstract the concurrent multipart uploader.
// such as the one in S3 SDK manager.Uploader
type Uploader interface {
	// Upload uploads the data from the reader.
	// should be run in a separate goroutine.
	Upload(ctx context.Context, rd io.Reader) error
}

// PrefixClient is the client for a given bucket prefix.
type PrefixClient interface {
	// CheckBucketExistence checks the existence of the bucket.
	CheckBucketExistence(ctx context.Context) error
	// CheckListObjects checks the permission of listObjects
	CheckListObjects(ctx context.Context) error
	// CheckGetObject checks the permission of getObject
	CheckGetObject(ctx context.Context) error
	// CheckPutAndDeleteObject checks the permission of putObject
	CheckPutAndDeleteObject(ctx context.Context) (err error)
	// GetObject gets the object with the given name and range [startOffset, endOffset).
	GetObject(ctx context.Context, name string, startOffset, endOffset int64) (*GetResp, error)
	// PutObject puts the object with the given name and data.
	PutObject(ctx context.Context, name string, data []byte) error
	// DeleteObject deletes the object with the given name.
	DeleteObject(ctx context.Context, name string) error
	// DeleteObjects deletes multiple objects with the given names.
	DeleteObjects(ctx context.Context, names []string) error
	// IsObjectExists checks whether the object with the given name exists.
	IsObjectExists(ctx context.Context, name string) (bool, error)
	// ListObjects lists objects with the given prefix, marker and maxKeys.
	// the marker is the key to start after, if nil, start from the beginning.
	// maxKeys is the maximum number of keys to return.
	ListObjects(ctx context.Context, prefix string, marker *string, maxKeys int) (*ListResp, error)
	// CopyObject copies an object from the source to the destination.
	CopyObject(ctx context.Context, params *CopyInput) error
	// MultipartWriter creates a multipart writer for the object with the given
	// name. each write to the returned writer will be uploaded as a part, so
	// the caller should control the size of each write to fit the part size
	// limit of the underlying S3-like storage.
	MultipartWriter(ctx context.Context, name string) (objectio.Writer, error)
	// MultipartUploader creates a multipart uploader for the object.
	// unlike MultipartWriter, this method allows concurrent uploading of parts.
	MultipartUploader(name string, partSize int64, concurrency int) Uploader
}

// CheckPermissions checks whether the client has the given permissions.
func CheckPermissions(ctx context.Context, cli PrefixClient, perms []storeapi.Permission) error {
	for _, perm := range perms {
		switch perm {
		case storeapi.AccessBuckets:
			if err := cli.CheckBucketExistence(ctx); err != nil {
				return errors.Annotatef(err, "permission %s", perm)
			}
		case storeapi.ListObjects:
			if err := cli.CheckListObjects(ctx); err != nil {
				return errors.Annotatef(err, "permission %s", perm)
			}
		case storeapi.GetObject:
			if err := cli.CheckGetObject(ctx); err != nil {
				return errors.Annotatef(err, "permission %s", perm)
			}
		case storeapi.PutAndDeleteObject:
			if err := cli.CheckPutAndDeleteObject(ctx); err != nil {
				return errors.Annotatef(err, "permission %s", perm)
			}
		default:
			return goerrors.New("unknown permission: " + string(perm))
		}
	}
	return nil
}
