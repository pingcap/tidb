// Copyright 2021 PingCAP, Inc. Licensed under Apache-2.0.

package storage

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"path"
	"strings"

	"github.com/google/uuid"
	"github.com/spf13/pflag"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"go.uber.org/zap"
)

const (
	azblobEndpointOption   = "azblob.endpoint"
	azblobAccessTierOption = "azblob.access-tier"
	azblobAccountName      = "azblob.account-name"
	azblobAccountKey       = "azblob.account-key"
)

type AzblobBackendOptions struct {
	Endpoint    string `json:"endpoint" toml:"endpoint"`
	AccountName string `json:"account-name" toml:"account-name"`
	AccountKey  string `json:"account-key" toml:"account-key"`
	AccessTier  string `json:"access-tier" toml:"access-tier"`
}

func (options *AzblobBackendOptions) apply(azblob *backuppb.AzureBlobStorage) error {
	azblob.Endpoint = options.Endpoint
	azblob.StorageClass = options.AccessTier
	azblob.AccountName = options.AccountName
	azblob.SharedKey = options.AccountKey
	return nil
}

func defineAzblobFlags(flags *pflag.FlagSet) {
	flags.String(azblobEndpointOption, "", "(experimental) Set the Azblob endpoint URL")
	flags.String(azblobAccessTierOption, "", "Specify the storage class for azblob")
	flags.String(azblobAccountName, "", "Specify the account name for azblob")
	flags.String(azblobAccountKey, "", "Specify the account key for azblob")
}

func hiddenAzblobFlags(flags *pflag.FlagSet) {
	_ = flags.MarkHidden(azblobEndpointOption)
	_ = flags.MarkHidden(azblobAccessTierOption)
	_ = flags.MarkHidden(azblobAccountName)
	_ = flags.MarkHidden(azblobAccountKey)
}

func (options *AzblobBackendOptions) parseFromFlags(flags *pflag.FlagSet) error {
	var err error
	options.Endpoint, err = flags.GetString(azblobEndpointOption)
	if err != nil {
		return errors.Trace(err)
	}

	options.AccessTier, err = flags.GetString(azblobAccessTierOption)
	if err != nil {
		return errors.Trace(err)
	}

	options.AccountName, err = flags.GetString(azblobAccountName)
	if err != nil {
		return errors.Trace(err)
	}

	options.AccountKey, err = flags.GetString(azblobAccountKey)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

type ClientBuilder interface {
	// Example of serviceURL: https://<your_storage_account>.blob.core.windows.net
	GetServiceClient() (azblob.ServiceClient, error)
	GetAccountName() string
}

// use shared key to access azure blob storage
type sharedKeyClientBuilder struct {
	cred        *azblob.SharedKeyCredential
	accountName string
	serviceURL  string
}

func (b *sharedKeyClientBuilder) GetServiceClient() (azblob.ServiceClient, error) {
	return azblob.NewServiceClientWithSharedKey(b.serviceURL, b.cred, nil)
}

func (b *sharedKeyClientBuilder) GetAccountName() string {
	return b.accountName
}

// use token to access azure blob storage
type tokenClientBuilder struct {
	cred        *azidentity.ClientSecretCredential
	accountName string
	serviceURL  string
}

func (b *tokenClientBuilder) GetServiceClient() (azblob.ServiceClient, error) {
	return azblob.NewServiceClient(b.serviceURL, b.cred, nil)
}

func (b *tokenClientBuilder) GetAccountName() string {
	return b.accountName
}

func getAuthorizerFromEnvironment() (clientId, tenantId, clientSecret string) {
	return os.Getenv("AZURE_CLIENT_ID"),
		os.Getenv("AZURE_TENANT_ID"),
		os.Getenv("AZURE_CLIENT_SECRET")
}

// get azure service client from options and environment
func getAzureServiceClientBuilder(options *backuppb.AzureBlobStorage, opts *ExternalStorageOptions) (ClientBuilder, error) {
	if len(options.Bucket) == 0 {
		return nil, errors.New("bucket(container) cannot be empty to access azure blob storage")
	}

	if len(options.AccountName) > 0 && len(options.SharedKey) > 0 {
		serviceURL := options.Endpoint
		if len(serviceURL) == 0 {
			serviceURL = fmt.Sprintf("https://%s.blob.core.windows.net", options.AccountName)
		}
		cred, err := azblob.NewSharedKeyCredential(options.AccountName, options.SharedKey)
		if err != nil {
			return nil, errors.Annotate(err, "Failed to get azure sharedKey credential")
		}
		return &sharedKeyClientBuilder{
			cred,
			options.AccountName,
			serviceURL,
		}, nil
	}

	accountName := options.AccountName
	if len(accountName) == 0 {
		if val := os.Getenv("AZURE_STORAGE_ACCOUNT"); len(val) > 0 {
			accountName = val
		} else {
			return nil, errors.New("account name cannot be empty to access azure blob storage")
		}
	}

	serviceURL := options.Endpoint
	if len(serviceURL) == 0 {
		serviceURL = fmt.Sprintf("https://%s.blob.core.windows.net", accountName)
	}

	if clientId, tenantId, clientSecret := getAuthorizerFromEnvironment(); len(clientId) > 0 && len(tenantId) > 0 && len(clientSecret) > 0 {
		cred, err := azidentity.NewClientSecretCredential(tenantId, clientId, clientSecret, nil)
		if err != nil {
			log.Warn("Failed to get azure token credential but environment variables exist, try to use shared key.", zap.String("tenantId", tenantId), zap.String("clientId", clientId), zap.String("clientSecret", "?"))
		} else {
			// send account-name to TiKV
			if opts != nil && opts.SendCredentials {
				options.AccountName = accountName
			}
			return &tokenClientBuilder{
				cred,
				accountName,
				serviceURL,
			}, nil
		}
	}

	var sharedKey string
	if val := os.Getenv("AZURE_STORAGE_KEY"); len(val) > 0 {
		log.Info("Get azure sharedKey from environment variable $AZURE_STORAGE_KEY")
		sharedKey = val
	} else {
		return nil, errors.New("cannot find any credential info to access azure blob storage")
	}

	cred, err := azblob.NewSharedKeyCredential(accountName, sharedKey)
	if err != nil {
		return nil, errors.Annotate(err, "Failed to get azure sharedKey credential")
	}
	// if BR can only get credential info from environment variable `sharedKey`,
	// BR will send it to TiKV so that there is no need to set environment variable for TiKV.
	if opts != nil && opts.SendCredentials {
		options.AccountName = accountName
		options.SharedKey = sharedKey
	}
	return &sharedKeyClientBuilder{
		cred,
		accountName,
		serviceURL,
	}, nil
}

type AzureBlobStorage struct {
	options *backuppb.AzureBlobStorage

	containerClient azblob.ContainerClient

	accessTier azblob.AccessTier
}

func newAzureBlobStorage(ctx context.Context, options *backuppb.AzureBlobStorage, opts *ExternalStorageOptions) (*AzureBlobStorage, error) {
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

	containerClient := serviceClient.NewContainerClient(options.Bucket)
	_, err = containerClient.Create(ctx, nil)
	if err != nil {
		var errResp *azblob.StorageError
		if internalErr, ok := err.(*azblob.InternalError); ok && internalErr.As(&errResp) {
			if errResp.ErrorCode != azblob.StorageErrorCodeContainerAlreadyExists {
				return nil, errors.Annotate(err, fmt.Sprintf("Failed to create the container: %s", errResp.ErrorCode))
			}
		} else {
			return nil, errors.Annotate(err, "Failed to create the container: error can not be parsed")
		}
	}

	// parse storage access-tier
	var accessTier azblob.AccessTier
	switch options.StorageClass {
	case "Archive", "archive":
		accessTier = azblob.AccessTierArchive
	case "Cool", "cool":
		accessTier = azblob.AccessTierCool
	case "Hot", "hot":
		accessTier = azblob.AccessTierHot
	default:
		accessTier = azblob.AccessTier(options.StorageClass)
	}

	log.Debug("select accessTier", zap.String("accessTier", string(accessTier)))

	return &AzureBlobStorage{
		options,
		containerClient,
		accessTier,
	}, nil
}

func (s *AzureBlobStorage) withPrefix(name string) string {
	return path.Join(s.options.Prefix, name)
}

func (s *AzureBlobStorage) WriteFile(ctx context.Context, name string, data []byte) error {
	client := s.containerClient.NewBlockBlobClient(s.withPrefix(name))
	resp, err := client.UploadBufferToBlockBlob(ctx, data, azblob.HighLevelUploadToBlockBlobOption{AccessTier: &s.accessTier})
	if err != nil {
		return errors.Annotatef(err, "Failed to write azure blob file, file info: bucket(container)='%s', key='%s'", s.options.Bucket, s.withPrefix(name))
	}
	defer resp.Body.Close()
	return nil
}

func (s *AzureBlobStorage) ReadFile(ctx context.Context, name string) ([]byte, error) {
	client := s.containerClient.NewBlockBlobClient(s.withPrefix(name))
	resp, err := client.Download(ctx, nil)
	if err != nil {
		return nil, errors.Annotatef(err, "Failed to download azure blob file, file info: bucket(container)='%s', key='%s'", s.options.Bucket, s.withPrefix(name))
	}
	defer resp.RawResponse.Body.Close()
	data, err := io.ReadAll(resp.Body(azblob.RetryReaderOptions{}))
	if err != nil {
		return nil, errors.Annotatef(err, "Failed to read azure blob file, file info: bucket(container)='%s', key='%s'", s.options.Bucket, s.withPrefix(name))
	}
	return data, err
}

func (s *AzureBlobStorage) FileExists(ctx context.Context, name string) (bool, error) {
	client := s.containerClient.NewBlockBlobClient(s.withPrefix(name))
	_, err := client.GetProperties(ctx, nil)
	if err != nil {
		var errResp *azblob.StorageError
		if internalErr, ok := err.(*azblob.InternalError); ok && internalErr.As(&errResp) {
			if errResp.ErrorCode == azblob.StorageErrorCodeBlobNotFound {
				return false, nil
			}
		}
		return false, errors.Trace(err)
	}
	return true, nil
}

func (s *AzureBlobStorage) DeleteFile(ctx context.Context, name string) error {
	client := s.containerClient.NewBlockBlobClient(s.withPrefix(name))
	_, err := client.Delete(ctx, nil)
	if err != nil {
		return errors.Annotatef(err, "Failed to delete azure blob file, file info: bucket(container)='%s', key='%s'", s.options.Bucket, s.withPrefix(name))
	}
	return nil
}

func (s *AzureBlobStorage) Open(ctx context.Context, name string) (ExternalFileReader, error) {
	client := s.containerClient.NewBlockBlobClient(s.withPrefix(name))
	return &azblobObjectReader{
		blobClient: client,

		pos: 0,

		ctx: ctx,
	}, nil
}

func (s *AzureBlobStorage) WalkDir(ctx context.Context, opt *WalkOption, fn func(path string, size int64) error) error {
	if opt == nil {
		opt = &WalkOption{}
	}
	if len(opt.ObjPrefix) != 0 {
		return errors.New("azure storage not support ObjPrefix for now")
	}
	prefix := path.Join(s.options.Prefix, opt.SubDir)
	if len(prefix) > 0 && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	prefixLength := len(prefix)

	listOption := &azblob.ContainerListBlobFlatSegmentOptions{Prefix: &prefix}
	for {
		respIter := s.containerClient.ListBlobsFlat(listOption)

		err := respIter.Err()
		if err != nil {
			return errors.Annotatef(err, "Failed to list azure blobs, bucket(container)='%s'", s.options.Bucket)
		}

		if !respIter.NextPage(ctx) {
			err := respIter.Err()
			if err != nil {
				return errors.Annotatef(err, "Failed to list azure blobs, bucket(container)='%s'", s.options.Bucket)
			}
			break
		}

		for _, blob := range respIter.PageResponse().Segment.BlobItems {
			if err := fn((*blob.Name)[prefixLength:], *blob.Properties.ContentLength); err != nil {
				return errors.Trace(err)
			}
		}

		listOption.Marker = respIter.PageResponse().NextMarker
		if len(*listOption.Marker) == 0 {
			break
		}
	}

	return nil
}

func (s *AzureBlobStorage) URI() string {
	return "azure://" + s.options.Bucket + "/" + s.options.Prefix
}

func (s *AzureBlobStorage) Create(ctx context.Context, name string) (ExternalFileWriter, error) {
	client := s.containerClient.NewBlockBlobClient(s.withPrefix(name))
	uploader := &azblobUploader{
		blobClient: client,

		blockIDList: make([]string, 0, 4),

		accessTier: s.accessTier,
	}

	uploaderWriter := newBufferedWriter(uploader, azblob.BlockBlobMaxUploadBlobBytes, NoCompression)
	return uploaderWriter, nil
}

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

type azblobObjectReader struct {
	blobClient azblob.BlockBlobClient

	pos int64

	ctx context.Context
}

// Read implement the io.Reader interface.
func (r *azblobObjectReader) Read(p []byte) (n int, err error) {
	count := int64(len(p))
	resp, err := r.blobClient.Download(r.ctx, &azblob.DownloadBlobOptions{Offset: &r.pos, Count: &count})
	if err != nil {
		return 0, errors.Annotatef(err, "Failed to read data from azure blob, data info: pos='%d', count='%d'", r.pos, count)
	}
	n, err = resp.Body(azblob.RetryReaderOptions{}).Read(p)
	if err != nil && err != io.EOF {
		return 0, errors.Annotatef(err, "Failed to read data from azure blob response, data info: pos='%d', count='%d'", r.pos, count)
	}
	r.pos += int64(n)
	return n, nil
}

// Close implement the io.Closer interface.
func (r *azblobObjectReader) Close() error {
	return nil
}

func (r *azblobObjectReader) Seek(offset int64, whence int) (int64, error) {
	var realOffset int64
	switch whence {
	case io.SeekStart:
		if offset < 0 {
			return 0, errors.Annotatef(berrors.ErrInvalidArgument, "Seek: offset '%v' out of range.", offset)
		}
		realOffset = offset
	case io.SeekCurrent:
		realOffset = r.pos + offset
		if r.pos < 0 && realOffset >= 0 {
			return 0, errors.Annotatef(berrors.ErrInvalidArgument, "Seek: offset '%v' out of range. current pos is '%v'.", offset, r.pos)
		}
	case io.SeekEnd:
		if offset >= 0 {
			return 0, errors.Annotatef(berrors.ErrInvalidArgument, "Seek: offset '%v' should be negative.", offset)
		}
		realOffset = offset
	default:
		return 0, errors.Annotatef(berrors.ErrStorageUnknown, "Seek: invalid whence '%d'", whence)
	}

	if realOffset < 0 {
		resp, err := r.blobClient.GetProperties(r.ctx, nil)
		if err != nil {
			return 0, errors.Annotate(err, "Failed to get properties from the azure blob")
		}

		contentLength := *resp.ContentLength
		r.pos = contentLength + realOffset
		if r.pos < 0 {
			return 0, errors.Annotatef(err, "Seek: offset is %d, but length of content is only %d", realOffset, contentLength)
		}
	} else {
		r.pos = realOffset
	}
	return r.pos, nil
}

type nopCloser struct {
	io.ReadSeeker
}

func newNopCloser(r io.ReadSeeker) nopCloser {
	return nopCloser{r}
}

func (r nopCloser) Close() error {
	return nil
}

type azblobUploader struct {
	blobClient azblob.BlockBlobClient

	blockIDList []string

	accessTier azblob.AccessTier
}

func (u *azblobUploader) Write(ctx context.Context, data []byte) (int, error) {
	generatedUuid, err := uuid.NewUUID()
	if err != nil {
		return 0, errors.Annotate(err, "Fail to generate uuid")
	}
	blockId := base64.StdEncoding.EncodeToString([]byte(generatedUuid.String()))

	_, err = u.blobClient.StageBlock(ctx, blockId, newNopCloser(bytes.NewReader(data)), nil)
	if err != nil {
		return 0, errors.Annotate(err, "Failed to upload block to azure blob")
	}
	u.blockIDList = append(u.blockIDList, blockId)

	return len(data), nil
}

func (u *azblobUploader) Close(ctx context.Context) error {
	_, err := u.blobClient.CommitBlockList(ctx, u.blockIDList, &azblob.CommitBlockListOptions{Tier: &u.accessTier})
	return errors.Trace(err)
}
