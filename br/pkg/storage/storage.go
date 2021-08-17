// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package storage

import (
	"context"
	"io"
	"net/http"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
)

// Permission represents the permission we need to check in create storage.
type Permission string

const (
	// AccessBuckets represents bucket access permission
	// it replace the origin skip-check-path.
	AccessBuckets Permission = "AccessBucket"

	// ListObjects represents listObjects permission
	ListObjects Permission = "ListObjects"
	// GetObject represents GetObject permission
	GetObject Permission = "GetObject"
	// PutObject represents PutObject permission
	PutObject Permission = "PutObject"
)

// WalkOption is the option of storage.WalkDir.
type WalkOption struct {
	// walk on SubDir of specify directory
	SubDir string
	// ListCount is the number of entries per page.
	//
	// In cloud storages such as S3 and GCS, the files listed and sent in pages.
	// Typically a page contains 1000 files, and if a folder has 3000 descendant
	// files, one would need 3 requests to retrieve all of them. This parameter
	// controls this size. Note that both S3 and GCS limits the maximum to 1000.
	//
	// Typically you want to leave this field unassigned (zero) to use the
	// default value (1000) to minimize the number of requests, unless you want
	// to reduce the possibility of timeout on an extremely slow connection, or
	// perform testing.
	ListCount int64
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

// Writer is like io.Writer but with Context, create a new writer on top of Uploader with NewUploaderWriter.
type Writer interface {
	// Write writes to buffer and if chunk is filled will upload it
	Write(ctx context.Context, p []byte) (int, error)
	// Close writes final chunk and completes the upload
	Close(ctx context.Context) error
}

// ExternalStorage represents a kind of file system storage.
type ExternalStorage interface {
	// WriteFile writes a complete file to storage, similar to os.WriteFile
	WriteFile(ctx context.Context, name string, data []byte) error
	// ReadFile reads a complete file from storage, similar to os.ReadFile
	ReadFile(ctx context.Context, name string) ([]byte, error)
	// FileExists return true if file exists
	FileExists(ctx context.Context, name string) (bool, error)
	// DeleteFile delete the file in storage
	DeleteFile(ctx context.Context, name string) error
	// Open a Reader by file path. path is relative path to storage base path
	Open(ctx context.Context, path string) (ExternalFileReader, error)
	// WalkDir traverse all the files in a dir.
	//
	// fn is the function called for each regular file visited by WalkDir.
	// The argument `path` is the file path that can be used in `Open`
	// function; the argument `size` is the size in byte of the file determined
	// by path.
	WalkDir(ctx context.Context, opt *WalkOption, fn func(path string, size int64) error) error

	// URI returns the base path as a URI
	URI() string

	// Create opens a file writer by path. path is relative path to storage base path
	Create(ctx context.Context, path string) (ExternalFileWriter, error)
}

// ExternalFileReader represents the streaming external file reader.
type ExternalFileReader interface {
	io.ReadCloser
	io.Seeker
}

// ExternalFileWriter represents the streaming external file writer.
type ExternalFileWriter interface {
	// Write writes to buffer and if chunk is filled will upload it
	Write(ctx context.Context, p []byte) (int, error)
	// Close writes final chunk and completes the upload
	Close(ctx context.Context) error
}

// ExternalStorageOptions are backend-independent options provided to New.
type ExternalStorageOptions struct {
	// SendCredentials marks whether to send credentials downstream.
	//
	// This field should be set to false if the credentials are provided to
	// downstream via external key managers, e.g. on K8s or cloud provider.
	SendCredentials bool

	// NoCredentials means that no cloud credentials are supplied to BR
	NoCredentials bool

	// SkipCheckPath marks whether to skip checking path's existence.
	//
	// This should only be set to true in testing, to avoid interacting with the
	// real world.
	// When this field is false (i.e. path checking is enabled), the New()
	// function will ensure the path referred by the backend exists by
	// recursively creating the folders. This will also throw an error if such
	// operation is impossible (e.g. when the bucket storing the path is missing).

	// deprecated: use checkPermissions and specify the checkPermission instead.
	SkipCheckPath bool

	// HTTPClient to use. The created storage may ignore this field if it is not
	// directly using HTTP (e.g. the local storage).
	HTTPClient *http.Client

	// CheckPermissions check the given permission in New() function.
	// make sure we can access the storage correctly before execute tasks.
	CheckPermissions []Permission
}

// Create creates ExternalStorage.
//
// Please consider using `New` in the future.
func Create(ctx context.Context, backend *backuppb.StorageBackend, sendCreds bool) (ExternalStorage, error) {
	return New(ctx, backend, &ExternalStorageOptions{
		SendCredentials: sendCreds,
		SkipCheckPath:   false,
		HTTPClient:      nil,
	})
}

// New creates an ExternalStorage with options.
func New(ctx context.Context, backend *backuppb.StorageBackend, opts *ExternalStorageOptions) (ExternalStorage, error) {
	switch backend := backend.Backend.(type) {
	case *backuppb.StorageBackend_Local:
		if backend.Local == nil {
			return nil, errors.Annotate(berrors.ErrStorageInvalidConfig, "local config not found")
		}
		return NewLocalStorage(backend.Local.Path)
	case *backuppb.StorageBackend_S3:
		if backend.S3 == nil {
			return nil, errors.Annotate(berrors.ErrStorageInvalidConfig, "s3 config not found")
		}
		return newS3Storage(backend.S3, opts)
	case *backuppb.StorageBackend_Noop:
		return newNoopStorage(), nil
	case *backuppb.StorageBackend_Gcs:
		if backend.Gcs == nil {
			return nil, errors.Annotate(berrors.ErrStorageInvalidConfig, "GCS config not found")
		}
		return newGCSStorage(ctx, backend.Gcs, opts)
	default:
		return nil, errors.Annotatef(berrors.ErrStorageInvalidConfig, "storage %T is not supported yet", backend)
	}
}
