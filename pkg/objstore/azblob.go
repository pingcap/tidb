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
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
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

	azblobPremisedCopySpeedPerSecond      = 100 * units.MiB
	azblobPremisedCopySpeedPerMilliSecond = azblobPremisedCopySpeedPerSecond / 1000
	azblobCopyPollPendingMinimalDuration  = 2 * time.Second
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
	GetServiceURL() string
}

func urlOfObjectByEndpoint(endpoint, container, object string) (string, error) {
	u, err := url.Parse(endpoint)
	if err != nil {
		return "", errors.Annotatef(err, "%s isn't a valid url", endpoint)
	}
	u.Path = path.Join(u.Path, container, object)
	return u.String(), nil
}

type defaultClientBuilder struct {
	defaultCred *azidentity.DefaultAzureCredential

	accountName string
	serviceURL  string
	clientOpts  *azblob.ClientOptions
}

// GetAccountName implements ClientBuilder.
func (d *defaultClientBuilder) GetAccountName() string {
	return d.accountName
}

// GetServiceClient implements ClientBuilder.
func (d *defaultClientBuilder) GetServiceClient() (*azblob.Client, error) {
	return azblob.NewClient(d.serviceURL, d.defaultCred, d.clientOpts)
}

// GetServiceURL implements ClientBuilder.
func (d *defaultClientBuilder) GetServiceURL() string {
	return d.serviceURL
}

// use shared key to access azure blob storage
type sharedKeyClientBuilder struct {
	cred        *azblob.SharedKeyCredential
	accountName string
	serviceURL  string

	clientOptions *azblob.ClientOptions
}

// GetServiceURL implements ClientBuilder.
func (b *sharedKeyClientBuilder) GetServiceURL() string {
	return b.serviceURL
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

// GetServiceURL implements ClientBuilder.
func (b *sasClientBuilder) GetServiceURL() string {
	return b.serviceURL
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

// GetServiceURL implements ClientBuilder.
func (b *tokenClientBuilder) GetServiceURL() string {
	return b.serviceURL
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
func getAzureServiceClientBuilder(options *backuppb.AzureBlobStorage, opts *storeapi.Options) (ClientBuilder, error) {
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
	if len(val) > 0 {
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

	defaultCred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, err
	}
	return &defaultClientBuilder{
		defaultCred: defaultCred,
		accountName: accountName,
		serviceURL:  serviceURL,
		clientOpts:  clientOptions,
	}, nil
}
