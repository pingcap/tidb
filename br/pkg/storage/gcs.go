// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package storage

import (
	"bytes"
	"context"
	goerrors "errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/prefetch"
	"github.com/spf13/pflag"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/net/http2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	htransport "google.golang.org/api/transport/http"
)

const (
	gcsEndpointOption     = "gcs.endpoint"
	gcsStorageClassOption = "gcs.storage-class"
	gcsPredefinedACL      = "gcs.predefined-acl"
	gcsCredentialsFile    = "gcs.credentials-file"
)

// GCSBackendOptions are options for configuration the GCS storage.
type GCSBackendOptions struct {
	Endpoint        string `json:"endpoint" toml:"endpoint"`
	StorageClass    string `json:"storage-class" toml:"storage-class"`
	PredefinedACL   string `json:"predefined-acl" toml:"predefined-acl"`
	CredentialsFile string `json:"credentials-file" toml:"credentials-file"`
}

func (options *GCSBackendOptions) apply(gcs *backuppb.GCS) error {
	gcs.Endpoint = options.Endpoint
	gcs.StorageClass = options.StorageClass
	gcs.PredefinedAcl = options.PredefinedACL

	if options.CredentialsFile != "" {
		b, err := os.ReadFile(options.CredentialsFile)
		if err != nil {
			return errors.Trace(err)
		}
		gcs.CredentialsBlob = string(b)
	}
	return nil
}

func defineGCSFlags(flags *pflag.FlagSet) {
	// TODO: remove experimental tag if it's stable
	flags.String(gcsEndpointOption, "", "(experimental) Set the GCS endpoint URL")
	flags.String(gcsStorageClassOption, "", "(experimental) Specify the GCS storage class for objects")
	flags.String(gcsPredefinedACL, "", "(experimental) Specify the GCS predefined acl for objects")
	flags.String(gcsCredentialsFile, "", "(experimental) Set the GCS credentials file path")
}

func hiddenGCSFlags(flags *pflag.FlagSet) {
	_ = flags.MarkHidden(gcsEndpointOption)
	_ = flags.MarkHidden(gcsStorageClassOption)
	_ = flags.MarkHidden(gcsPredefinedACL)
	_ = flags.MarkHidden(gcsCredentialsFile)
}

func (options *GCSBackendOptions) parseFromFlags(flags *pflag.FlagSet) error {
	var err error
	options.Endpoint, err = flags.GetString(gcsEndpointOption)
	if err != nil {
		return errors.Trace(err)
	}

	options.StorageClass, err = flags.GetString(gcsStorageClassOption)
	if err != nil {
		return errors.Trace(err)
	}

	options.PredefinedACL, err = flags.GetString(gcsPredefinedACL)
	if err != nil {
		return errors.Trace(err)
	}

	options.CredentialsFile, err = flags.GetString(gcsCredentialsFile)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// GCSStorage defines some standard operations for BR/Lightning on the GCS storage.
// It implements the `ExternalStorage` interface.
type GCSStorage struct {
	gcs       *backuppb.GCS
	idx       *atomic.Int64
	clientCnt int64
	clientOps []option.ClientOption

	handles []*storage.BucketHandle
	clients []*storage.Client
}

// GetBucketHandle gets the handle to the GCS API on the bucket.
func (s *GCSStorage) GetBucketHandle() *storage.BucketHandle {
	i := s.idx.Inc() % int64(len(s.handles))
	return s.handles[i]
}

// getClient gets the GCS client.
func (s *GCSStorage) getClient() *storage.Client {
	i := s.idx.Inc() % int64(len(s.clients))
	return s.clients[i]
}

// GetOptions gets the external storage operations for the GCS.
func (s *GCSStorage) GetOptions() *backuppb.GCS {
	return s.gcs
}

// DeleteFile delete the file in storage
func (s *GCSStorage) DeleteFile(ctx context.Context, name string) error {
	object := s.objectName(name)
	err := s.GetBucketHandle().Object(object).Delete(ctx)
	return errors.Trace(err)
}

// DeleteFiles delete the files in storage.
func (s *GCSStorage) DeleteFiles(ctx context.Context, names []string) error {
	for _, name := range names {
		err := s.DeleteFile(ctx, name)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *GCSStorage) objectName(name string) string {
	return path.Join(s.gcs.Prefix, name)
}

// WriteFile writes data to a file to storage.
func (s *GCSStorage) WriteFile(ctx context.Context, name string, data []byte) error {
	object := s.objectName(name)
	wc := s.GetBucketHandle().Object(object).NewWriter(ctx)
	wc.StorageClass = s.gcs.StorageClass
	wc.PredefinedACL = s.gcs.PredefinedAcl
	_, err := wc.Write(data)
	if err != nil {
		return errors.Trace(err)
	}
	return wc.Close()
}

// ReadFile reads the file from the storage and returns the contents.
func (s *GCSStorage) ReadFile(ctx context.Context, name string) ([]byte, error) {
	object := s.objectName(name)
	rc, err := s.GetBucketHandle().Object(object).NewReader(ctx)
	if err != nil {
		return nil, errors.Annotatef(err,
			"failed to read gcs file, file info: input.bucket='%s', input.key='%s'",
			s.gcs.Bucket, object)
	}
	defer rc.Close()

	size := rc.Attrs.Size
	var b []byte
	if size < 0 {
		// happened when using fake-gcs-server in integration test
		b, err = io.ReadAll(rc)
	} else {
		b = make([]byte, size)
		_, err = io.ReadFull(rc, b)
	}
	return b, errors.Trace(err)
}

// FileExists return true if file exists.
func (s *GCSStorage) FileExists(ctx context.Context, name string) (bool, error) {
	object := s.objectName(name)
	_, err := s.GetBucketHandle().Object(object).Attrs(ctx)
	if err != nil {
		if errors.Cause(err) == storage.ErrObjectNotExist { // nolint:errorlint
			return false, nil
		}
		return false, errors.Trace(err)
	}
	return true, nil
}

// Open a Reader by file path.
func (s *GCSStorage) Open(ctx context.Context, path string, o *ReaderOption) (ExternalFileReader, error) {
	object := s.objectName(path)
	handle := s.GetBucketHandle().Object(object)

	attrs, err := handle.Attrs(ctx)
	if err != nil {
		if errors.Cause(err) == storage.ErrObjectNotExist { // nolint:errorlint
			return nil, errors.Annotatef(err,
				"the object doesn't exist, file info: input.bucket='%s', input.key='%s'",
				s.gcs.Bucket, path)
		}
		return nil, errors.Annotatef(err,
			"failed to get gcs file attribute, file info: input.bucket='%s', input.key='%s'",
			s.gcs.Bucket, path)
	}
	pos := int64(0)
	endPos := attrs.Size
	prefetchSize := 0
	if o != nil {
		if o.StartOffset != nil {
			pos = *o.StartOffset
		}
		if o.EndOffset != nil {
			endPos = *o.EndOffset
		}
		prefetchSize = o.PrefetchSize
	}

	return &gcsObjectReader{
		storage:      s,
		name:         path,
		objHandle:    handle,
		reader:       nil, // lazy create
		ctx:          ctx,
		pos:          pos,
		endPos:       endPos,
		prefetchSize: prefetchSize,
		totalSize:    attrs.Size,
	}, nil
}

// WalkDir traverse all the files in a dir.
//
// fn is the function called for each regular file visited by WalkDir.
// The first argument is the file path that can be used in `Open`
// function; the second argument is the size in byte of the file determined
// by path.
func (s *GCSStorage) WalkDir(ctx context.Context, opt *WalkOption, fn func(string, int64) error) error {
	if opt == nil {
		opt = &WalkOption{}
	}
	prefix := path.Join(s.gcs.Prefix, opt.SubDir)
	if len(prefix) > 0 && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	if len(opt.ObjPrefix) != 0 {
		prefix += opt.ObjPrefix
	}

	query := &storage.Query{Prefix: prefix}
	// only need each object's name and size
	err := query.SetAttrSelection([]string{"Name", "Size"})
	if err != nil {
		return errors.Trace(err)
	}
	iter := s.GetBucketHandle().Objects(ctx, query)
	for {
		attrs, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return errors.Trace(err)
		}
		// when walk on specify directory, the result include storage.Prefix,
		// which can not be reuse in other API(Open/Read) directly.
		// so we use TrimPrefix to filter Prefix for next Open/Read.
		path := strings.TrimPrefix(attrs.Name, s.gcs.Prefix)
		// trim the prefix '/' to ensure that the path returned is consistent with the local storage
		path = strings.TrimPrefix(path, "/")
		if err = fn(path, attrs.Size); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (s *GCSStorage) URI() string {
	return "gcs://" + s.gcs.Bucket + "/" + s.gcs.Prefix
}

// Create implements ExternalStorage interface.
func (s *GCSStorage) Create(ctx context.Context, name string, wo *WriterOption) (ExternalFileWriter, error) {
	// NewGCSWriter requires real testing environment on Google Cloud.
	mockGCS := intest.InTest && strings.Contains(s.gcs.GetEndpoint(), "127.0.0.1")
	if wo == nil || wo.Concurrency <= 1 || mockGCS {
		object := s.objectName(name)
		wc := s.GetBucketHandle().Object(object).NewWriter(ctx)
		wc.StorageClass = s.gcs.StorageClass
		wc.PredefinedACL = s.gcs.PredefinedAcl
		return newFlushStorageWriter(wc, &emptyFlusher{}, wc), nil
	}
	uri := s.objectName(name)
	// 5MB is the minimum part size for GCS.
	partSize := int64(gcsMinimumChunkSize)
	if wo.PartSize > partSize {
		partSize = wo.PartSize
	}
	w, err := NewGCSWriter(ctx, s.getClient(), uri, partSize, wo.Concurrency, s.gcs.Bucket)
	if err != nil {
		return nil, errors.Trace(err)
	}
	fw := newFlushStorageWriter(w, &emptyFlusher{}, w)
	bw := newBufferedWriter(fw, int(partSize), NoCompression)
	return bw, nil
}

// Rename file name from oldFileName to newFileName.
func (s *GCSStorage) Rename(ctx context.Context, oldFileName, newFileName string) error {
	data, err := s.ReadFile(ctx, oldFileName)
	if err != nil {
		return errors.Trace(err)
	}
	err = s.WriteFile(ctx, newFileName, data)
	if err != nil {
		return errors.Trace(err)
	}
	return s.DeleteFile(ctx, oldFileName)
}

// Close implements ExternalStorage interface.
func (s *GCSStorage) Close() {
	for _, client := range s.clients {
		if err := client.Close(); err != nil {
			log.Warn("failed to close gcs client", zap.Error(err))
		}
	}
}

// used in tests
var mustReportCredErr = false

const gcsClientCnt = 16

// NewGCSStorage creates a GCS external storage implementation.
func NewGCSStorage(ctx context.Context, gcs *backuppb.GCS, opts *ExternalStorageOptions) (*GCSStorage, error) {
	var clientOps []option.ClientOption
	if opts.NoCredentials {
		clientOps = append(clientOps, option.WithoutAuthentication())
	} else {
		if gcs.CredentialsBlob == "" {
			creds, err := google.FindDefaultCredentials(ctx, storage.ScopeReadWrite)
			if err != nil {
				if intest.InTest && !mustReportCredErr {
					clientOps = append(clientOps, option.WithoutAuthentication())
					goto skipHandleCred
				}
				return nil, errors.Annotatef(berrors.ErrStorageInvalidConfig, "%v Or you should provide '--gcs.credentials_file'", err)
			}
			if opts.SendCredentials {
				if len(creds.JSON) <= 0 {
					return nil, errors.Annotate(berrors.ErrStorageInvalidConfig,
						"You should provide '--gcs.credentials_file' when '--send-credentials-to-tikv' is true")
				}
				gcs.CredentialsBlob = string(creds.JSON)
			}
			if creds != nil {
				clientOps = append(clientOps, option.WithCredentials(creds))
			}
		} else {
			clientOps = append(clientOps, option.WithCredentialsJSON([]byte(gcs.GetCredentialsBlob())))
		}
	}
skipHandleCred:

	if gcs.Endpoint != "" {
		clientOps = append(clientOps, option.WithEndpoint(gcs.Endpoint))
	}

	if opts.HTTPClient != nil {
		// see https://github.com/pingcap/tidb/issues/47022#issuecomment-1722913455
		// https://www.googleapis.com/auth/cloud-platform must be set to use service_account
		// type of credential-file.
		newTransport, err := htransport.NewTransport(ctx, opts.HTTPClient.Transport,
			append(clientOps, option.WithScopes(storage.ScopeFullControl, "https://www.googleapis.com/auth/cloud-platform"))...)
		if err != nil {
			if intest.InTest && !mustReportCredErr {
				goto skipHandleTransport
			}
			return nil, errors.Trace(err)
		}
		opts.HTTPClient.Transport = newTransport
	skipHandleTransport:
		clientOps = append(clientOps, option.WithHTTPClient(opts.HTTPClient))
	}

	if !opts.SendCredentials {
		// Clear the credentials if exists so that they will not be sent to TiKV
		gcs.CredentialsBlob = ""
	}

	ret := &GCSStorage{
		gcs:       gcs,
		idx:       atomic.NewInt64(0),
		clientCnt: gcsClientCnt,
		clientOps: clientOps,
	}
	if err := ret.Reset(ctx); err != nil {
		return nil, errors.Trace(err)
	}
	return ret, nil
}

// Reset resets the GCS storage.
func (s *GCSStorage) Reset(ctx context.Context) error {
	logutil.Logger(ctx).Info("resetting gcs storage")

	for _, client := range s.clients {
		_ = client.Close()
	}

	s.clients = make([]*storage.Client, gcsClientCnt)
	eg, egCtx := util.NewErrorGroupWithRecoverWithCtx(ctx)
	for i := range s.clients {
		i := i
		eg.Go(func() error {
			client, err := storage.NewClient(egCtx, s.clientOps...)
			if err != nil {
				return errors.Trace(err)
			}
			client.SetRetry(storage.WithErrorFunc(shouldRetry))
			s.clients[i] = client
			return nil
		})
	}
	err := eg.Wait()
	if err != nil {
		return errors.Trace(err)
	}

	s.handles = make([]*storage.BucketHandle, gcsClientCnt)
	for i := range s.handles {
		s.handles[i] = s.clients[i].Bucket(s.gcs.Bucket)
	}
	return nil
}

func shouldRetry(err error) bool {
	if storage.ShouldRetry(err) {
		return true
	}

	if err == nil {
		return false
	}

	// workaround for https://github.com/googleapis/google-cloud-go/issues/7440
	if e := (http2.StreamError{}); goerrors.As(err, &e) {
		if e.Code == http2.ErrCodeInternal {
			log.Warn("retrying gcs request due to internal HTTP2 error", zap.Error(err))
			return true
		}
	}

	// workaround for https://github.com/googleapis/google-cloud-go/issues/9262
	if e := (&googleapi.Error{}); goerrors.As(err, &e) {
		if e.Code == 401 {
			log.Warn("retrying gcs request due to internal authentication error", zap.Error(err))
			return true
		}
	}

	errMsg := err.Error()
	// workaround for strange unknown errors
	retryableErrMsg := []string{
		"http2: client connection force closed via ClientConn.Close",
		"broken pipe",
	}

	for _, msg := range retryableErrMsg {
		if strings.Contains(errMsg, msg) {
			log.Warn("retrying gcs request", zap.Error(err))
			return true
		}
	}

	// just log the new unknown error, in case we can add it to this function
	if !goerrors.Is(err, context.Canceled) {
		log.Warn("other error when requesting gcs",
			zap.Error(err),
			zap.String("info", fmt.Sprintf("type: %T, value: %#v", err, err)))
	}

	return false
}

// gcsObjectReader wrap storage.Reader and add the `Seek` method.
type gcsObjectReader struct {
	storage   *GCSStorage
	name      string
	objHandle *storage.ObjectHandle
	reader    io.ReadCloser
	pos       int64
	endPos    int64
	totalSize int64

	prefetchSize int
	// reader context used for implement `io.Seek`
	// currently, lightning depends on package `xitongsys/parquet-go` to read parquet file and it needs `io.Seeker`
	// See: https://github.com/xitongsys/parquet-go/blob/207a3cee75900b2b95213627409b7bac0f190bb3/source/source.go#L9-L10
	ctx context.Context
}

// Read implement the io.Reader interface.
func (r *gcsObjectReader) Read(p []byte) (n int, err error) {
	if r.reader == nil {
		length := int64(-1)
		if r.endPos != r.totalSize {
			length = r.endPos - r.pos
		}
		rc, err := r.objHandle.NewRangeReader(r.ctx, r.pos, length)
		if err != nil {
			return 0, errors.Annotatef(err,
				"failed to read gcs file, file info: input.bucket='%s', input.key='%s'",
				r.storage.gcs.Bucket, r.name)
		}
		r.reader = rc
		if r.prefetchSize > 0 {
			r.reader = prefetch.NewReader(r.reader, r.prefetchSize)
		}
	}
	n, err = r.reader.Read(p)
	r.pos += int64(n)
	return n, err
}

// Close implement the io.Closer interface.
func (r *gcsObjectReader) Close() error {
	if r.reader == nil {
		return nil
	}
	return r.reader.Close()
}

// Seek implement the io.Seeker interface.
//
// Currently, tidb-lightning depends on this method to read parquet file for gcs storage.
func (r *gcsObjectReader) Seek(offset int64, whence int) (int64, error) {
	var realOffset int64
	switch whence {
	case io.SeekStart:
		realOffset = offset
	case io.SeekCurrent:
		realOffset = r.pos + offset
	case io.SeekEnd:
		if offset > 0 {
			return 0, errors.Annotatef(berrors.ErrInvalidArgument, "Seek: offset '%v' should be negative.", offset)
		}
		realOffset = offset + r.totalSize
	default:
		return 0, errors.Annotatef(berrors.ErrStorageUnknown, "Seek: invalid whence '%d'", whence)
	}

	if realOffset < 0 {
		return 0, errors.Annotatef(berrors.ErrInvalidArgument, "Seek: offset '%v' out of range. current pos is '%v'. total size is '%v'", offset, r.pos, r.totalSize)
	}

	if realOffset == r.pos {
		return realOffset, nil
	}

	if r.reader != nil {
		_ = r.reader.Close()
		r.reader = nil
	}
	r.pos = realOffset
	if realOffset >= r.totalSize {
		r.reader = io.NopCloser(bytes.NewReader(nil))
		return realOffset, nil
	}
	rc, err := r.objHandle.NewRangeReader(r.ctx, r.pos, -1)
	if err != nil {
		return 0, errors.Annotatef(err,
			"failed to read gcs file, file info: input.bucket='%s', input.key='%s'",
			r.storage.gcs.Bucket, r.name)
	}
	r.reader = rc
	if r.prefetchSize > 0 {
		r.reader = prefetch.NewReader(r.reader, r.prefetchSize)
	}

	return realOffset, nil
}

func (r *gcsObjectReader) GetFileSize() (int64, error) {
	return r.totalSize, nil
}
