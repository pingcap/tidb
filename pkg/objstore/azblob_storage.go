// Copyright 2021 PingCAP, Inc.
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

package objstore

import (
	"context"
	"fmt"
	"io"
	"path"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/pkg/objstore/compressedio"
	"github.com/pingcap/tidb/pkg/objstore/objectio"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"go.uber.org/zap"
)

// AzureBlobStorage is a storage engine that stores data in Azure Blob Storage.
type AzureBlobStorage struct {
	options *backuppb.AzureBlobStorage

	containerClient *container.Client

	accessTier blob.AccessTier

	cpkScope *blob.CPKScopeInfo
	cpkInfo  *blob.CPKInfo

	// resolvedAccountName is the final account name we are going to use.
	resolvedAccountName     string
	resolvedServiceEndpoint string
}

// CopyFrom implements Copier.
func (s *AzureBlobStorage) CopyFrom(ctx context.Context, e storeapi.Storage, spec storeapi.CopySpec) error {
	es, ok := e.(*AzureBlobStorage)
	if !ok {
		return errors.Annotatef(berrors.ErrStorageInvalidConfig,
			"AzureBlobStorage.CopyFrom supports *AzureBlobStorage only, got %T", e)
	}

	url, err := urlOfObjectByEndpoint(es.resolvedServiceEndpoint, es.options.Bucket, es.withPrefix(spec.From))
	if err != nil {
		return errors.Annotatef(err, "failed to get url of object %s", spec.From)
	}
	dstBlob := s.containerClient.NewBlobClient(s.withPrefix(spec.To))

	// NOTE: `CopyFromURL` supports files up to 256 MiB, which might not be enough for huger regions.
	// Hence we use the asynchronous version and wait for finish.
	// But this might not as effect as the syncrhonous version for small files.
	// It is possible to use syncrhonous version for small files if necessary.
	// REF: https://learn.microsoft.com/en-us/rest/api/storageservices/copy-blob-from-url
	resp, err := dstBlob.StartCopyFromURL(ctx, url, &blob.StartCopyFromURLOptions{})
	if err != nil {
		return errors.Annotatef(err, "failed to copy blob from %s to %s", url, spec.To)
	}
	copyID := resp.CopyID
	deref := aws.ToString

	for {
		prop, err := dstBlob.GetProperties(ctx, &blob.GetPropertiesOptions{})
		if err != nil {
			return errors.Annotate(err, "failed to check asynchronous copy status")
		}
		if prop.CopyID == nil || deref(prop.CopyID) != deref(copyID) {
			return errors.Annotatef(berrors.ErrStorageUnknown,
				"failed to check copy status: copy ID not match; copy id = %v; resp.copyID = %v;", deref(copyID), deref(prop.CopyID))
		}
		cStat := deref((*string)(prop.CopyStatus))
		// REF: https://learn.microsoft.com/en-us/rest/api/storageservices/get-blob-properties?tabs=microsoft-entra-id#response-headers
		switch cStat {
		case "success":
			return nil
		case "failed", "aborted":
			return errors.Annotatef(berrors.ErrStorageUnknown, "asynchronous copy failed or aborted: %s", deref(prop.CopyStatusDescription))
		case "pending":
			finished, total, err := progress(deref(prop.CopyProgress))
			if err != nil {
				return errors.Annotate(err, "failed to parse progress")
			}
			rem := total - finished
			// In practice, most copies finish when the initial request returns.
			// To avoid a busy loop of requesting, we need a minimal sleep duration.
			toSleep := max(time.Duration(rem/azblobPremisedCopySpeedPerMilliSecond)*time.Millisecond, azblobCopyPollPendingMinimalDuration)
			logutil.CL(ctx).Info("AzureBlobStorage: asynchronous copy triggered",
				zap.Int("finished", finished), zap.Int("total", total),
				zap.Stringp("copy-id", prop.CopyID), zap.Duration("to-sleep", toSleep),
				zap.Stringp("copy-desc", prop.CopyStatusDescription),
			)
			time.Sleep(toSleep)
			continue
		default:
			return errors.Annotatef(berrors.ErrStorageUnknown, "unknown copy status: %v", cStat)
		}
	}
}

// progress parses the format "bytes copied/bytes total".
// REF: https://learn.microsoft.com/en-us/rest/api/storageservices/get-blob-properties?tabs=microsoft-entra-id#response-headers
func progress(s string) (finished, total int, err error) {
	n, err := fmt.Sscanf(s, "%d/%d", &finished, &total)
	if n != 2 {
		err = errors.Errorf("failed to parse progress %s", s)
	}
	return
}

// MarkStrongConsistency implements Storage.
func (*AzureBlobStorage) MarkStrongConsistency() {
	// See https://github.com/MicrosoftDocs/azure-docs/issues/105331#issuecomment-1450252384
}

func newAzureBlobStorage(ctx context.Context, options *backuppb.AzureBlobStorage, opts *storeapi.Options) (*AzureBlobStorage, error) {
	clientBuilder, err := getAzureServiceClientBuilder(options, opts)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return newAzureBlobStorageWithClientBuilder(ctx, options, clientBuilder)
}

func newAzureBlobStorageWithClientBuilder(ctx context.Context, options *backuppb.AzureBlobStorage, clientBuilder ClientBuilder) (*AzureBlobStorage, error) {
	serviceClient, err := clientBuilder.GetServiceClient()
	if err != nil {
		return nil, errors.Annotate(err, "Failed to create azure service client")
	}

	containerClient := serviceClient.ServiceClient().NewContainerClient(options.Bucket)
	if _, err = containerClient.GetProperties(ctx, &container.GetPropertiesOptions{}); err != nil {
		return nil, errors.Trace(err)
	}

	if (len(options.EncryptionScope) > 0 || options.EncryptionKey != nil) && len(options.StorageClass) > 0 {
		return nil, errors.Errorf("Set Blob Tier cannot be used with customer-provided key/scope. " +
			"Please don't supply the access-tier when use encryption-key or encryption-scope.")
	} else if len(options.EncryptionScope) > 0 && options.EncryptionKey != nil {
		return nil, errors.Errorf("Undefined input: There are both encryption-scope and customer provided key. " +
			"Please select only one to encrypt blobs.")
	}

	var cpkScope *blob.CPKScopeInfo = nil
	if len(options.EncryptionScope) > 0 {
		cpkScope = &blob.CPKScopeInfo{
			EncryptionScope: &options.EncryptionScope,
		}
	}

	var cpkInfo *blob.CPKInfo = nil
	if options.EncryptionKey != nil {
		defaultAlgorithm := blob.EncryptionAlgorithmTypeAES256
		cpkInfo = &blob.CPKInfo{
			EncryptionAlgorithm: &defaultAlgorithm,
			EncryptionKey:       &options.EncryptionKey.EncryptionKey,
			EncryptionKeySHA256: &options.EncryptionKey.EncryptionKeySha256,
		}
	}

	// parse storage access-tier
	var accessTier blob.AccessTier
	switch options.StorageClass {
	case "Archive", "archive":
		accessTier = blob.AccessTierArchive
	case "Cool", "cool":
		accessTier = blob.AccessTierCool
	case "Hot", "hot":
		accessTier = blob.AccessTierHot
	default:
		accessTier = blob.AccessTier(options.StorageClass)
	}

	log.Debug("select accessTier", zap.String("accessTier", string(accessTier)))

	return &AzureBlobStorage{
		options,
		containerClient,
		accessTier,
		cpkScope,
		cpkInfo,
		clientBuilder.GetAccountName(),
		clientBuilder.GetServiceURL(),
	}, nil
}

func (s *AzureBlobStorage) withPrefix(name string) string {
	return path.Join(s.options.Prefix, name)
}

// WriteFile writes a file to Azure Blob Storage.
func (s *AzureBlobStorage) WriteFile(ctx context.Context, name string, data []byte) error {
	client := s.containerClient.NewBlockBlobClient(s.withPrefix(name))
	// the encryption scope/key and the access tier can not be both in the HTTP headers
	options := &blockblob.UploadBufferOptions{
		CPKScopeInfo: s.cpkScope,
		CPKInfo:      s.cpkInfo,
	}

	if len(s.accessTier) > 0 {
		options.AccessTier = &s.accessTier
	}
	_, err := client.UploadBuffer(ctx, data, options)
	if err != nil {
		return errors.Annotatef(err, "Failed to write azure blob file, file info: bucket(container)='%s', key='%s'", s.options.Bucket, s.withPrefix(name))
	}
	return nil
}

// ReadFile reads a file from Azure Blob Storage.
func (s *AzureBlobStorage) ReadFile(ctx context.Context, name string) ([]byte, error) {
	client := s.containerClient.NewBlockBlobClient(s.withPrefix(name))
	resp, err := client.DownloadStream(ctx, &blob.DownloadStreamOptions{
		CPKInfo: s.cpkInfo,
	})
	if err != nil {
		return nil, errors.Annotatef(err, "Failed to download azure blob file, file info: bucket(container)='%s', key='%s'", s.options.Bucket, s.withPrefix(name))
	}
	body := resp.NewRetryReader(ctx, &blob.RetryReaderOptions{
		MaxRetries: azblobRetryTimes,
	})
	data, err := io.ReadAll(body)
	if err != nil {
		return nil, errors.Annotatef(err, "Failed to read azure blob file, file info: bucket(container)='%s', key='%s'", s.options.Bucket, s.withPrefix(name))
	}
	return data, body.Close()
}

// FileExists checks if a file exists in Azure Blob Storage.
func (s *AzureBlobStorage) FileExists(ctx context.Context, name string) (bool, error) {
	client := s.containerClient.NewBlockBlobClient(s.withPrefix(name))
	_, err := client.GetProperties(ctx, nil)
	if err != nil {
		if bloberror.HasCode(err, bloberror.BlobNotFound) {
			return false, nil
		}
		return false, errors.Trace(err)
	}
	return true, nil
}

// DeleteFile deletes the file with the given name.
func (s *AzureBlobStorage) DeleteFile(ctx context.Context, name string) error {
	client := s.containerClient.NewBlockBlobClient(s.withPrefix(name))
	_, err := client.Delete(ctx, nil)
	if err != nil {
		return errors.Annotatef(err, "Failed to delete azure blob file, file info: bucket(container)='%s', key='%s'", s.options.Bucket, s.withPrefix(name))
	}
	return nil
}

// DeleteFiles deletes the files with the given names.
func (s *AzureBlobStorage) DeleteFiles(ctx context.Context, names []string) error {
	for _, name := range names {
		err := s.DeleteFile(ctx, name)
		if err != nil {
			return err
		}
	}
	return nil
}

// Open implements the StorageReader interface.
func (s *AzureBlobStorage) Open(ctx context.Context, name string, o *storeapi.ReaderOption) (objectio.Reader, error) {
	client := s.containerClient.NewBlockBlobClient(s.withPrefix(name))
	resp, err := client.GetProperties(ctx, nil)
	if err != nil {
		return nil, errors.Annotate(err, "Failed to get properties from the azure blob")
	}

	pos := int64(0)
	totalSize := *resp.ContentLength
	endPos := totalSize
	if o != nil {
		if o.StartOffset != nil {
			pos = *o.StartOffset
		}
		if o.EndOffset != nil {
			endPos = *o.EndOffset
		}
	}

	return &azblobObjectReader{
		blobClient: client,

		pos:       pos,
		endPos:    endPos,
		totalSize: totalSize,

		ctx: ctx,

		cpkInfo: s.cpkInfo,
	}, nil
}

// WalkDir implements the StorageReader interface.
func (s *AzureBlobStorage) WalkDir(ctx context.Context, opt *storeapi.WalkOption, fn func(path string, size int64) error) error {
	if opt == nil {
		opt = &storeapi.WalkOption{}
	}
	prefix := path.Join(s.options.Prefix, opt.SubDir)
	if len(prefix) > 0 && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	if len(opt.ObjPrefix) != 0 {
		prefix += opt.ObjPrefix
	}

	pager := s.containerClient.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{
		Prefix: &prefix,
	})
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return errors.Annotatef(err, "Failed to list azure blobs, bucket(container)='%s'", s.options.Bucket)
		}

		for _, blob := range page.Segment.BlobItems {
			// when walk on specify directory, the result include storage.Prefix,
			// which can not be reuse in other API(Open/Read) directly.
			// so we use TrimPrefix to filter Prefix for next Open/Read.
			path := strings.TrimPrefix((*blob.Name), s.options.Prefix)
			// trim the prefix '/' to ensure that the path returned is consistent with the local storage
			path = strings.TrimPrefix(path, "/")
			if err := fn(path, *blob.Properties.ContentLength); err != nil {
				return errors.Trace(err)
			}
		}
	}

	return nil
}

// URI implements the StorageReader interface.
func (s *AzureBlobStorage) URI() string {
	return "azure://" + s.options.Bucket + "/" + s.options.Prefix
}

const azblobChunkSize = 64 * 1024 * 1024

// Create implements the StorageWriter interface.
func (s *AzureBlobStorage) Create(_ context.Context, name string, _ *storeapi.WriterOption) (objectio.Writer, error) {
	client := s.containerClient.NewBlockBlobClient(s.withPrefix(name))
	uploader := &azblobUploader{
		blobClient: client,

		blockIDList: make([]string, 0, 4),

		accessTier: s.accessTier,

		cpkScope: s.cpkScope,
		cpkInfo:  s.cpkInfo,
	}

	uploaderWriter := objectio.NewBufferedWriter(uploader, azblobChunkSize, compressedio.NoCompression, nil)
	return uploaderWriter, nil
}

// Rename implements the StorageWriter interface.
func (s *AzureBlobStorage) Rename(ctx context.Context, oldFileName, newFileName string) error {
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

// Close implements the Storage interface.
func (*AzureBlobStorage) Close() {}
