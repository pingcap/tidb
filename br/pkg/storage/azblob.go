// Copyright 2021 PingCAP, Inc. Licensed under Apache-2.0.

package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"path"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/google/uuid"
	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/spf13/pflag"
	"go.uber.org/zap"
)

const (
	azblobEndpointOption   = "azblob.endpoint"
	azblobAccessTierOption = "azblob.access-tier"
	azblobAccountName      = "azblob.account-name"
	azblobAccountKey       = "azblob.account-key"
	azblobSASToken         = "azblob.sas-token"
	azblobEncryptionScope  = "azblob.encryption-scope"
	azblobEncryptionKey    = "azblob.encryption-key"
)

const azblobRetryTimes int32 = 5

func getDefaultClientOptions() *azblob.ClientOptions {
	return &azblob.ClientOptions{
		ClientOptions: azcore.ClientOptions{
			Retry: policy.RetryOptions{
				MaxRetries: azblobRetryTimes,
			},
		},
	}
}

// AzblobBackendOptions is the options for Azure Blob storage.
type AzblobBackendOptions struct {
	Endpoint        string `json:"endpoint" toml:"endpoint"`
	AccountName     string `json:"account-name" toml:"account-name"`
	AccountKey      string `json:"account-key" toml:"account-key"`
	AccessTier      string `json:"access-tier" toml:"access-tier"`
	SASToken        string `json:"sas-token" toml:"sas-token"`
	EncryptionScope string `json:"encryption-scope" toml:"encryption-scope"`
	EncryptionKey   string `json:"encryption-key" toml:"encryption-key"`
}

func (options *AzblobBackendOptions) apply(azblob *backuppb.AzureBlobStorage) error {
	azblob.Endpoint = options.Endpoint
	azblob.StorageClass = options.AccessTier
	azblob.AccountName = options.AccountName
	azblob.SharedKey = options.AccountKey
	azblob.AccessSig = options.SASToken
	azblob.EncryptionScope = options.EncryptionScope

	if len(options.EncryptionKey) == 0 {
		options.EncryptionKey = os.Getenv("AZURE_ENCRYPTION_KEY")
	}

	if len(options.EncryptionKey) > 0 {
		keySlice := []byte(options.EncryptionKey)
		keySha256 := sha256.Sum256(keySlice)
		azblob.EncryptionKey = &backuppb.AzureCustomerKey{
			EncryptionKey:       base64.StdEncoding.EncodeToString(keySlice),
			EncryptionKeySha256: base64.StdEncoding.EncodeToString(keySha256[:]),
		}
	}
	return nil
}

func defineAzblobFlags(flags *pflag.FlagSet) {
	flags.String(azblobEndpointOption, "", "(experimental) Set the Azblob endpoint URL")
	flags.String(azblobAccessTierOption, "", "Specify the storage class for azblob")
	flags.String(azblobAccountName, "", "Specify the account name for azblob")
	flags.String(azblobAccountKey, "", "Specify the account key for azblob")
	flags.String(azblobSASToken, "", "Specify the SAS (shared access signatures) for azblob")
	flags.String(azblobEncryptionScope, "", "Specify the server side encryption scope")
	flags.String(azblobEncryptionKey, "", "Specify the server side encryption customer provided key")
}

func hiddenAzblobFlags(flags *pflag.FlagSet) {
	_ = flags.MarkHidden(azblobEndpointOption)
	_ = flags.MarkHidden(azblobAccessTierOption)
	_ = flags.MarkHidden(azblobAccountName)
	_ = flags.MarkHidden(azblobAccountKey)
	_ = flags.MarkHidden(azblobSASToken)
	_ = flags.MarkHidden(azblobEncryptionScope)
	_ = flags.MarkHidden(azblobEncryptionKey)
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

	options.SASToken, err = flags.GetString(azblobSASToken)
	if err != nil {
		return errors.Trace(err)
	}

	options.EncryptionScope, err = flags.GetString(azblobEncryptionScope)
	if err != nil {
		return errors.Trace(err)
	}

	options.EncryptionKey, err = flags.GetString(azblobEncryptionKey)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// ClientBuilder provides common method to build a service client.
type ClientBuilder interface {
	// Example of serviceURL: https://<your_storage_account>.blob.core.windows.net
	GetServiceClient() (*azblob.Client, error)
	GetAccountName() string
}

// use shared key to access azure blob storage
type sharedKeyClientBuilder struct {
	cred        *azblob.SharedKeyCredential
	accountName string
	serviceURL  string

	clientOptions *azblob.ClientOptions
}

func (b *sharedKeyClientBuilder) GetServiceClient() (*azblob.Client, error) {
	return azblob.NewClientWithSharedKeyCredential(b.serviceURL, b.cred, b.clientOptions)
}

func (b *sharedKeyClientBuilder) GetAccountName() string {
	return b.accountName
}

// use SAS to access azure blob storage
type sasClientBuilder struct {
	accountName string
	// Example of serviceURL: https://<account>.blob.core.windows.net/?<sas token>
	serviceURL string

	clientOptions *azblob.ClientOptions
}

func (b *sasClientBuilder) GetServiceClient() (*azblob.Client, error) {
	return azblob.NewClientWithNoCredential(b.serviceURL, b.clientOptions)
}

func (b *sasClientBuilder) GetAccountName() string {
	return b.accountName
}

// use token to access azure blob storage
type tokenClientBuilder struct {
	cred        *azidentity.ClientSecretCredential
	accountName string
	serviceURL  string

	clientOptions *azblob.ClientOptions
}

func (b *tokenClientBuilder) GetServiceClient() (*azblob.Client, error) {
	return azblob.NewClient(b.serviceURL, b.cred, b.clientOptions)
}

func (b *tokenClientBuilder) GetAccountName() string {
	return b.accountName
}

func getAuthorizerFromEnvironment() (clientID, tenantID, clientSecret string) {
	return os.Getenv("AZURE_CLIENT_ID"),
		os.Getenv("AZURE_TENANT_ID"),
		os.Getenv("AZURE_CLIENT_SECRET")
}

// get azure service client from options and environment
func getAzureServiceClientBuilder(options *backuppb.AzureBlobStorage, opts *ExternalStorageOptions) (ClientBuilder, error) {
	if len(options.Bucket) == 0 {
		return nil, errors.New("bucket(container) cannot be empty to access azure blob storage")
	}

	clientOptions := getDefaultClientOptions()
	if opts != nil && opts.HTTPClient != nil {
		clientOptions.Transport = opts.HTTPClient
	}

	if len(options.AccountName) > 0 && len(options.AccessSig) > 0 {
		serviceURL := options.Endpoint
		if len(serviceURL) == 0 {
			if strings.HasPrefix(options.AccessSig, "?") {
				serviceURL = fmt.Sprintf("https://%s.blob.core.windows.net/%s", options.AccountName, options.AccessSig)
			} else {
				serviceURL = fmt.Sprintf("https://%s.blob.core.windows.net/?%s", options.AccountName, options.AccessSig)
			}
		}
		return &sasClientBuilder{
			options.AccountName,
			serviceURL,

			clientOptions,
		}, nil
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

			clientOptions,
		}, nil
	}

	accountName := options.AccountName
	if len(accountName) == 0 {
		val := os.Getenv("AZURE_STORAGE_ACCOUNT")
		if len(val) <= 0 {
			return nil, errors.New("account name cannot be empty to access azure blob storage")
		}
		accountName = val
	}

	serviceURL := options.Endpoint
	if len(serviceURL) == 0 {
		serviceURL = fmt.Sprintf("https://%s.blob.core.windows.net", accountName)
	}

	if clientID, tenantID, clientSecret := getAuthorizerFromEnvironment(); len(clientID) > 0 && len(tenantID) > 0 && len(clientSecret) > 0 {
		cred, err := azidentity.NewClientSecretCredential(tenantID, clientID, clientSecret, nil)
		if err == nil {
			// send account-name to TiKV
			if opts != nil && opts.SendCredentials {
				options.AccountName = accountName
			}
			return &tokenClientBuilder{
				cred,
				accountName,
				serviceURL,

				clientOptions,
			}, nil
		}
		log.Warn("Failed to get azure token credential but environment variables exist, try to use shared key.", zap.String("tenantId", tenantID), zap.String("clientId", clientID), zap.String("clientSecret", "?"))
	}

	var sharedKey string
	val := os.Getenv("AZURE_STORAGE_KEY")
	if len(val) <= 0 {
		return nil, errors.New("cannot find any credential info to access azure blob storage")
	}
	log.Info("Get azure sharedKey from environment variable $AZURE_STORAGE_KEY")
	sharedKey = val

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

		clientOptions,
	}, nil
}

// AzureBlobStorage is a storage engine that stores data in Azure Blob Storage.
type AzureBlobStorage struct {
	options *backuppb.AzureBlobStorage

	containerClient *container.Client

	accessTier blob.AccessTier

	cpkScope *blob.CPKScopeInfo
	cpkInfo  *blob.CPKInfo
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

// DeleteFile deletes the files with the given names.
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
func (s *AzureBlobStorage) Open(ctx context.Context, name string, o *ReaderOption) (ExternalFileReader, error) {
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
func (s *AzureBlobStorage) WalkDir(ctx context.Context, opt *WalkOption, fn func(path string, size int64) error) error {
	if opt == nil {
		opt = &WalkOption{}
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
func (s *AzureBlobStorage) Create(_ context.Context, name string, _ *WriterOption) (ExternalFileWriter, error) {
	client := s.containerClient.NewBlockBlobClient(s.withPrefix(name))
	uploader := &azblobUploader{
		blobClient: client,

		blockIDList: make([]string, 0, 4),

		accessTier: s.accessTier,

		cpkScope: s.cpkScope,
		cpkInfo:  s.cpkInfo,
	}

	uploaderWriter := newBufferedWriter(uploader, azblobChunkSize, NoCompression)
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

// Close implements the ExternalStorage interface.
func (*AzureBlobStorage) Close() {}

type azblobObjectReader struct {
	blobClient *blockblob.Client

	pos       int64
	endPos    int64
	totalSize int64

	ctx context.Context

	cpkInfo *blob.CPKInfo
}

// Read implement the io.Reader interface.
func (r *azblobObjectReader) Read(p []byte) (n int, err error) {
	maxCnt := r.endPos - r.pos
	if maxCnt > int64(len(p)) {
		maxCnt = int64(len(p))
	}
	if maxCnt == 0 {
		return 0, io.EOF
	}
	resp, err := r.blobClient.DownloadStream(r.ctx, &blob.DownloadStreamOptions{
		Range: blob.HTTPRange{
			Offset: r.pos,
			Count:  maxCnt,
		},

		CPKInfo: r.cpkInfo,
	})
	if err != nil {
		return 0, errors.Annotatef(err, "Failed to read data from azure blob, data info: pos='%d', count='%d'", r.pos, maxCnt)
	}
	body := resp.NewRetryReader(r.ctx, &blob.RetryReaderOptions{
		MaxRetries: azblobRetryTimes,
	})
	n, err = body.Read(p)
	if err != nil && err != io.EOF {
		return 0, errors.Annotatef(err, "Failed to read data from azure blob response, data info: pos='%d', count='%d'", r.pos, maxCnt)
	}
	r.pos += int64(n)
	return n, body.Close()
}

// Close implement the io.Closer interface.
func (*azblobObjectReader) Close() error {
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
		if offset > 0 {
			return 0, errors.Annotatef(berrors.ErrInvalidArgument, "Seek: offset '%v' should be negative.", offset)
		}
		realOffset = offset + r.totalSize
	default:
		return 0, errors.Annotatef(berrors.ErrStorageUnknown, "Seek: invalid whence '%d'", whence)
	}

	if realOffset < 0 {
		return 0, errors.Annotatef(berrors.ErrInvalidArgument, "Seek: offset is %d, but length of content is only %d", realOffset, r.totalSize)
	}
	r.pos = realOffset
	return r.pos, nil
}

func (r *azblobObjectReader) GetFileSize() (int64, error) {
	return r.totalSize, nil
}

type nopCloser struct {
	io.ReadSeeker
}

func newNopCloser(r io.ReadSeeker) nopCloser {
	return nopCloser{r}
}

func (nopCloser) Close() error {
	return nil
}

type azblobUploader struct {
	blobClient *blockblob.Client

	blockIDList []string

	accessTier blob.AccessTier

	cpkScope *blob.CPKScopeInfo
	cpkInfo  *blob.CPKInfo
}

func (u *azblobUploader) Write(ctx context.Context, data []byte) (int, error) {
	generatedUUID, err := uuid.NewUUID()
	if err != nil {
		return 0, errors.Annotate(err, "Fail to generate uuid")
	}
	blockID := base64.StdEncoding.EncodeToString([]byte(generatedUUID.String()))

	_, err = u.blobClient.StageBlock(ctx, blockID, newNopCloser(bytes.NewReader(data)), &blockblob.StageBlockOptions{
		CPKScopeInfo: u.cpkScope,
		CPKInfo:      u.cpkInfo,
	})
	if err != nil {
		return 0, errors.Annotate(err, "Failed to upload block to azure blob")
	}
	u.blockIDList = append(u.blockIDList, blockID)

	return len(data), nil
}

func (u *azblobUploader) Close(ctx context.Context) error {
	// the encryption scope and the access tier can not be both in the HTTP headers
	options := &blockblob.CommitBlockListOptions{
		CPKScopeInfo: u.cpkScope,
		CPKInfo:      u.cpkInfo,
	}

	if len(u.accessTier) > 0 {
		options.Tier = &u.accessTier
	}
	_, err := u.blobClient.CommitBlockList(ctx, u.blockIDList, options)
	return errors.Trace(err)
}
