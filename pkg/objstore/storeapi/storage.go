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

package storeapi

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"path"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/google/uuid"
	"github.com/pingcap/tidb/pkg/objstore/objectio"
	"github.com/pingcap/tidb/pkg/objstore/recording"
)

// Permission represents the permission we need to check in create storage.
type Permission string

// StrongConsistency is a marker interface that indicates the storage is strong consistent
// over its `Read`, `Write` and `WalkDir` APIs.
type StrongConsistency interface {
	MarkStrongConsistency()
}

const (
	// AccessBuckets represents bucket access permission
	// it replaces the origin skip-check-path.
	AccessBuckets Permission = "AccessBucket"
	// ListObjects represents listObjects permission
	ListObjects Permission = "ListObjects"
	// GetObject represents GetObject permission
	GetObject Permission = "GetObject"
	// PutObject represents PutObject permission
	PutObject Permission = "PutObject"
	// PutAndDeleteObject represents PutAndDeleteObject permission
	// we cannot check DeleteObject permission alone, so we use PutAndDeleteObject instead.
	PutAndDeleteObject Permission = "PutAndDeleteObject"
)

// WalkOption is the option of storage.WalkDir.
type WalkOption struct {
	// walk on SubDir of base directory, i.e. if the base dir is '/path/to/base'
	// then we're walking '/path/to/base/<SubDir>'
	SubDir string
	// whether subdirectory under the walk dir is skipped, only works for LOCAL storage now.
	// default is false, i.e. we walk recursively.
	SkipSubDir bool
	// ObjPrefix used fo prefix search in storage. Note that only part of storage
	// support it.
	// It can save lots of time when we want find specify prefix objects in storage.
	// For example. we have 10000 <Hash>.sst files and 10 backupmeta.(\d+) files.
	// we can use ObjPrefix = "backupmeta" to retrieve all meta files quickly.
	ObjPrefix string
	// ListCount is the number of entries per page.
	//
	// In cloud storages such as S3 and GCS, the files listed and sent in pages.
	// Typically, a page contains 1000 files, and if a folder has 3000 descendant
	// files, one would need 3 requests to retrieve all of them. This parameter
	// controls this size. Note that both S3, GCS and OSS limits the maximum to
	// 1000.
	//
	// Typically, you want to leave this field unassigned (zero) to use the
	// default value (1000) to minimize the number of requests, unless you want
	// to reduce the possibility of timeout on an extremely slow connection, or
	// perform testing.
	ListCount int64
	// IncludeTombstone will allow `Walk` to emit removed files during walking.
	//
	// In most cases, `Walk` runs over a snapshot, if a file in the snapshot
	// was deleted during walking, the file will be ignored. Set this to `true`
	// will make them be sent to the callback.
	//
	// The size of a deleted file should be `TombstoneSize`.
	IncludeTombstone bool
	// StartAfter is the key to start after. If not empty, the walk will start
	// after the key. Currently only S3-like storage supports this option.
	StartAfter string
}

// ReadSeekCloser is the interface that groups the basic Read, Seek and Close methods.
type ReadSeekCloser interface {
	io.Reader
	io.Seeker
	io.Closer
}

// Uploader upload file with chunks.
type Uploader interface {
	// UploadPart upload part of file data to storage
	UploadPart(ctx context.Context, data []byte) error
	// CompleteUpload make the upload data to a complete file
	CompleteUpload(ctx context.Context) error
}

// WriterOption writer option.
type WriterOption struct {
	Concurrency int
	PartSize    int64
}

// ReaderOption reader option.
type ReaderOption struct {
	// StartOffset is inclusive. And it's incompatible with Seek.
	StartOffset *int64
	// EndOffset is exclusive. And it's incompatible with Seek.
	EndOffset *int64
	// PrefetchSize will switch to NewPrefetchReader if value is positive.
	PrefetchSize int
}

// Copier copier.
type Copier interface {
	// CopyFrom copies a object to the current external storage by the specification.
	CopyFrom(ctx context.Context, e Storage, spec CopySpec) error
}

// CopySpec copy spec.
type CopySpec struct {
	From string
	To   string
}

// Storage represents a kind of file system storage.
type Storage interface {
	// WriteFile writes a complete file to storage, similar to os.WriteFile, but WriteFile should be atomic
	WriteFile(ctx context.Context, name string, data []byte) error
	// ReadFile reads a complete file from storage, similar to os.ReadFile
	ReadFile(ctx context.Context, name string) ([]byte, error)
	// FileExists return true if file exists
	FileExists(ctx context.Context, name string) (bool, error)
	// DeleteFile delete the file in storage
	DeleteFile(ctx context.Context, name string) error
	// Open a Reader by file path. path is relative path to storage base path.
	// Some implementation will use the given ctx as the inner context of the reader.
	Open(ctx context.Context, path string, option *ReaderOption) (objectio.Reader, error)
	// DeleteFiles delete the files in storage
	DeleteFiles(ctx context.Context, names []string) error
	// WalkDir traverse all the files in a dir.
	//
	// fn is the function called for each regular file visited by WalkDir.
	// The argument `path` is the file path that can be used in `Open`
	// function; the argument `size` is the size in byte of the file determined
	// by path.
	WalkDir(ctx context.Context, opt *WalkOption, fn func(path string, size int64) error) error

	// URI returns the base path as a URI
	URI() string

	// Create opens a file writer by path. path is relative path to storage base
	// path. The old file under same path will be overwritten. Currently only s3
	// implemented WriterOption.
	Create(ctx context.Context, path string, option *WriterOption) (objectio.Writer, error)
	// Rename file name from oldFileName to newFileName
	Rename(ctx context.Context, oldFileName, newFileName string) error
	// Close release the resources of the storage.
	Close()
}

// Options are backend-independent options provided to New.
type Options struct {
	// SendCredentials marks whether to send credentials downstream.
	//
	// This field should be set to false if the credentials are provided to
	// downstream via external key managers, e.g. on K8s or cloud provider.
	SendCredentials bool

	// NoCredentials means that no cloud credentials are supplied to BR
	NoCredentials bool

	// HTTPClient to use. The created storage may ignore this field if it is not
	// directly using HTTP (e.g. the local storage).
	// NOTICE: the HTTPClient is only used by s3/azure/gcs.
	// For GCS, we will use this as base client to init a client with credentials.
	HTTPClient *http.Client

	// CheckPermissions check the given permission in New() function.
	// make sure we can access the storage correctly before execute tasks.
	CheckPermissions []Permission

	// S3Retryer is the retryer for create s3 storage, if it is nil,
	// defaultS3Retryer() will be used.
	S3Retryer retry.Standard

	// CheckObjectLockOptions check the s3 bucket has enabled the ObjectLock.
	// if enabled. it will send the options to tikv.
	CheckS3ObjectLockOptions bool
	// AccessRecording records the access statistics of object storage.
	// we use the read/write file size as an estimate of the network traffic,
	// we don't consider the traffic consumed by network protocol, and traffic
	// caused by retry
	AccessRecording *recording.AccessStats
}

// Prefix is like a folder if not empty, we still call it a prefix to match S3
// terminology.
// if not empty, it cannot start with '/' and must end with a '/', such as
// 'a/b/'. the folder name must be valid, we don't check it here.
type Prefix string

// NewPrefix returns a new Prefix instance from the given string.
func NewPrefix(prefix string) Prefix {
	p := strings.Trim(prefix, "/")
	if p != "" {
		p += "/"
	}
	return Prefix(p)
}

func (p Prefix) join(other Prefix) Prefix {
	// due to the definition of Prefix, we can add them directly.
	return p + other
}

// JoinStr returns a new Prefix by joining the given string to the current Prefix.
func (p Prefix) JoinStr(str string) Prefix {
	strPrefix := NewPrefix(str)
	return p.join(strPrefix)
}

// ObjectKey returns the object key by joining the name to the Prefix.
func (p Prefix) ObjectKey(name string) string {
	// if p is not empty, it already ends with '/'.
	// the name better not start with '/', else there will be double '/' in the
	// key.
	// this is existing behavior, we keep it.
	return string(p) + name
}

// String implements fmt.Stringer interface.
func (p Prefix) String() string {
	return string(p)
}

// BucketPrefix represents a prefix in a bucket.
type BucketPrefix struct {
	Bucket string
	Prefix Prefix
}

// NewBucketPrefix returns a new BucketPrefix instance.
func NewBucketPrefix(bucket, prefix string) BucketPrefix {
	return BucketPrefix{
		Bucket: bucket,
		Prefix: NewPrefix(prefix),
	}
}

// ObjectKey returns the object key by joining the name to the Prefix.
func (bp *BucketPrefix) ObjectKey(name string) string {
	return bp.Prefix.ObjectKey(name)
}

// PrefixStr returns the Prefix as a string.
func (bp *BucketPrefix) PrefixStr() string {
	return bp.Prefix.String()
}

// GetHTTPRange returns the HTTP Range header value for the given start and end
// offsets.
// If endOffset is not 0, startOffset must <= endOffset; we don't check the
// validity here.
// If startOffset == 0 and endOffset == 0, `full` is true and `rangeVal` is empty.
// Otherwise, a partial object is requested, `full` is false and `rangeVal`
// contains the Range header value.
func GetHTTPRange(startOffset, endOffset int64) (full bool, rangeVal string) {
	// If we just open part of the object, we set `Range` in the request.
	// If we meant to open the whole object, not just a part of it,
	// we do not pass the range in the request,
	// so that even if the object is empty, we can still get the response without errors.
	// Then this behavior is similar to opening an empty file in local file system.
	switch {
	case endOffset > startOffset:
		// both end of http Range header are inclusive
		rangeVal = fmt.Sprintf("bytes=%d-%d", startOffset, endOffset-1)
	case startOffset == 0:
		// opening the whole object, no need to fill the `Range` field in the request
		full = true
	default:
		rangeVal = fmt.Sprintf("bytes=%d-", startOffset)
	}
	return
}

// GenPermCheckObjectKey generates a unique object key for permission checking.
func GenPermCheckObjectKey() string {
	return path.Join("perm-check", uuid.New().String())
}
