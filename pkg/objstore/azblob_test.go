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
	"crypto/rand"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/pkg/objstore/objectio"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/stretchr/testify/require"
)

// use shared key to access azurite
type sharedKeyAzuriteClientBuilder struct {
}

// GetServiceURL implements ClientBuilder.
func (b *sharedKeyAzuriteClientBuilder) GetServiceURL() string {
	return "http://127.0.0.1:10000/devstoreaccount1"
}

func (b *sharedKeyAzuriteClientBuilder) GetServiceClient() (*azblob.Client, error) {
	connStr := "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
	return azblob.NewClientFromConnectionString(connStr, nil)
}

func (b *sharedKeyAzuriteClientBuilder) GetAccountName() string {
	return "devstoreaccount1"
}

func createContainer(
	ctx context.Context,
	clientBuilder *sharedKeyAzuriteClientBuilder,
	container string,
) (bool, error) {
	serviceClient, err := clientBuilder.GetServiceClient()
	if err != nil {
		return false, errors.Annotate(err, "Failed to create azure service client")
	}
	containerClient := serviceClient.ServiceClient().NewContainerClient(container)
	_, err = containerClient.Create(ctx, nil)
	if err != nil && !bloberror.HasCode(err, bloberror.ContainerAlreadyExists) {
		if strings.Contains(err.Error(), "connect: connection refused") {
			return true, nil
		}
		return false, errors.Annotate(err, "Failed to create container")
	}
	return false, nil
}

func TestAzblob(t *testing.T) {
	ctx := context.Background()
	options := &backuppb.AzureBlobStorage{
		Bucket: "test",
		Prefix: "a/b/",
	}
	builder := &sharedKeyAzuriteClientBuilder{}
	skip, err := createContainer(ctx, builder, options.Bucket)
	if skip || err != nil {
		t.Skip("azurite is not running, skip test")
		return
	}
	require.NoError(t, err)

	azblobStorage, err := newAzureBlobStorageWithClientBuilder(ctx, options, builder)
	require.NoError(t, err)

	err = azblobStorage.WriteFile(ctx, "key", []byte("data"))
	require.NoError(t, err)

	err = azblobStorage.WriteFile(ctx, "key1", []byte("data1"))
	require.NoError(t, err)

	err = azblobStorage.WriteFile(ctx, "key2", []byte("data22223346757222222222289722222"))
	require.NoError(t, err)

	d, err := azblobStorage.ReadFile(ctx, "key")
	require.NoError(t, err)
	require.Equal(t, []byte("data"), d)

	exist, err := azblobStorage.FileExists(ctx, "key")
	require.NoError(t, err)
	require.True(t, exist)

	exist, err = azblobStorage.FileExists(ctx, "key_not_exist")
	require.NoError(t, err)
	require.False(t, exist)

	keyDelete := "key_delete"
	exist, err = azblobStorage.FileExists(ctx, keyDelete)
	require.NoError(t, err)
	require.False(t, exist)

	err = azblobStorage.WriteFile(ctx, keyDelete, []byte("data"))
	require.NoError(t, err)

	exist, err = azblobStorage.FileExists(ctx, keyDelete)
	require.NoError(t, err)
	require.True(t, exist)

	err = azblobStorage.DeleteFile(ctx, keyDelete)
	require.NoError(t, err)

	exist, err = azblobStorage.FileExists(ctx, keyDelete)
	require.NoError(t, err)
	require.False(t, exist)

	list := ""
	var totalSize int64 = 0
	err = azblobStorage.WalkDir(ctx, nil, func(name string, size int64) error {
		list += name
		totalSize += size
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, "keykey1key2", list)
	require.Equal(t, int64(42), totalSize)

	efr, err := azblobStorage.Open(ctx, "key2", nil)
	require.NoError(t, err)
	size, err := efr.GetFileSize()
	require.NoError(t, err)
	require.EqualValues(t, 33, size)

	realReader := efr.(*azblobObjectReader)
	require.Nil(t, realReader.reader)

	p := make([]byte, 10)
	n, err := efr.Read(p)
	require.NoError(t, err)
	require.Equal(t, 10, n)
	require.Equal(t, "data222233", string(p))
	require.NotNil(t, realReader.reader)
	oldInnerReader := realReader.reader

	p = make([]byte, 40)
	n, err = efr.Read(p)
	require.NoError(t, err)
	require.Equal(t, 23, n)
	require.Equal(t, "46757222222222289722222", string(p[:23]))
	require.Same(t, oldInnerReader, realReader.reader)

	p = make([]byte, 5)
	offs, err := efr.Seek(3, io.SeekStart)
	require.NoError(t, err)
	require.Equal(t, int64(3), offs)
	// reader reopened
	require.NotSame(t, oldInnerReader, realReader.reader)

	n, err = efr.Read(p)
	require.NoError(t, err)
	require.Equal(t, 5, n)
	require.Equal(t, "a2222", string(p))

	p = make([]byte, 5)
	offs, err = efr.Seek(3, io.SeekCurrent)
	require.NoError(t, err)
	require.Equal(t, int64(11), offs)

	n, err = efr.Read(p)
	require.NoError(t, err)
	require.Equal(t, 5, n)
	require.Equal(t, "67572", string(p))

	p = make([]byte, 5)
	offs, err = efr.Seek(int64(-7), io.SeekEnd)
	require.NoError(t, err)
	// Note, change it to maxCnt - offs
	require.Equal(t, int64(26), offs)

	n, err = efr.Read(p)
	require.NoError(t, err)
	require.Equal(t, 5, n)
	require.Equal(t, "97222", string(p))

	err = efr.Close()
	require.NoError(t, err)

	require.Equal(t, "azure://test/a/b/", azblobStorage.URI())
}

func TestNewAzblobStorage(t *testing.T) {
	t.Setenv("AZURE_FEDERATED_TOKEN_FILE", "")
	{
		options := &backuppb.AzureBlobStorage{
			Endpoint:    "http://127.0.0.1:1000",
			Bucket:      "test",
			Prefix:      "a/b",
			AccountName: "user",
			SharedKey:   "cGFzc3dk",
		}
		builder, err := getAzureServiceClientBuilder(options, nil)
		require.NoError(t, err)
		b, ok := builder.(*sharedKeyClientBuilder)
		require.True(t, ok)
		require.Equal(t, "user", b.GetAccountName())
		require.Equal(t, "http://127.0.0.1:1000", b.serviceURL)
	}

	{
		options := &backuppb.AzureBlobStorage{
			Bucket:      "test",
			Prefix:      "a/b",
			AccountName: "user",
			SharedKey:   "cGFzc3dk",
		}
		builder, err := getAzureServiceClientBuilder(options, nil)
		require.NoError(t, err)
		b, ok := builder.(*sharedKeyClientBuilder)
		require.True(t, ok)
		require.Equal(t, "user", b.GetAccountName())
		require.Equal(t, "https://user.blob.core.windows.net", b.serviceURL)
	}

	err := os.Setenv("AZURE_STORAGE_ACCOUNT", "env_user")
	require.NoError(t, err)
	defer os.Unsetenv("AZURE_STORAGE_ACCOUNT")

	{
		options := &backuppb.AzureBlobStorage{
			Bucket:      "test",
			Prefix:      "a/b",
			AccountName: "user",
			SharedKey:   "cGFzc3dk",
		}
		builder, err := getAzureServiceClientBuilder(options, nil)
		require.NoError(t, err)
		b, ok := builder.(*sharedKeyClientBuilder)
		require.True(t, ok)
		require.Equal(t, "user", b.GetAccountName())
		require.Equal(t, "https://user.blob.core.windows.net", b.serviceURL)
	}

	{
		options := &backuppb.AzureBlobStorage{
			Bucket:    "test",
			Prefix:    "a/b",
			SharedKey: "cGFzc3dk",
		}
		builder, err := getAzureServiceClientBuilder(options, nil)
		require.NoError(t, err)
		_, ok := builder.(*defaultClientBuilder)
		require.True(t, ok, "it is %T", builder)
	}

	err = os.Setenv("AZURE_STORAGE_KEY", "cGFzc3dk")
	require.NoError(t, err)
	defer os.Unsetenv("AZURE_STORAGE_KEY")

	{
		options := &backuppb.AzureBlobStorage{
			Endpoint:  "http://127.0.0.1:1000",
			Bucket:    "test",
			Prefix:    "a/b",
			SharedKey: "cGFzc2dk",
		}
		builder, err := getAzureServiceClientBuilder(options, nil)
		require.NoError(t, err)
		b, ok := builder.(*sharedKeyClientBuilder)
		require.True(t, ok)
		require.Equal(t, "env_user", b.GetAccountName())
		require.Equal(t, "http://127.0.0.1:1000", b.serviceURL)
	}

	err = os.Setenv("AZURE_CLIENT_ID", "321")
	require.NoError(t, err)
	defer os.Unsetenv("AZURE_CLIENT_ID")

	err = os.Setenv("AZURE_TENANT_ID", "321")
	require.NoError(t, err)
	defer os.Unsetenv("AZURE_TENANT_ID")

	err = os.Setenv("AZURE_CLIENT_SECRET", "321")
	require.NoError(t, err)
	defer os.Unsetenv("AZURE_CLIENT_SECRET")

	{
		options := &backuppb.AzureBlobStorage{
			Endpoint: "http://127.0.0.1:1000",
			Bucket:   "test",
			Prefix:   "a/b",
		}
		builder, err := getAzureServiceClientBuilder(options, nil)
		require.NoError(t, err)
		b, ok := builder.(*tokenClientBuilder)
		require.True(t, ok)
		require.Equal(t, "env_user", b.GetAccountName())
		require.Equal(t, "http://127.0.0.1:1000", b.serviceURL)
	}

	{
		options := &backuppb.AzureBlobStorage{
			Endpoint:  "http://127.0.0.1:1000",
			Bucket:    "test",
			Prefix:    "a/b",
			SharedKey: "cGFzc2dk",
		}
		builder, err := getAzureServiceClientBuilder(options, nil)
		require.NoError(t, err)
		b, ok := builder.(*tokenClientBuilder)
		require.True(t, ok)
		require.Equal(t, "env_user", b.GetAccountName())
		require.Equal(t, "http://127.0.0.1:1000", b.serviceURL)
	}

	{
		options := &backuppb.AzureBlobStorage{
			Endpoint:    "http://127.0.0.1:1000",
			Bucket:      "test",
			Prefix:      "a/b",
			AccountName: "user",
			SharedKey:   "cGFzc3dk",
		}
		builder, err := getAzureServiceClientBuilder(options, nil)
		require.NoError(t, err)
		b, ok := builder.(*sharedKeyClientBuilder)
		require.True(t, ok)
		require.Equal(t, "user", b.GetAccountName())
		require.Equal(t, "http://127.0.0.1:1000", b.serviceURL)
	}

	{
		options := &backuppb.AzureBlobStorage{
			Endpoint:    "http://127.0.0.1:1000",
			Bucket:      "test",
			Prefix:      "a/b",
			AccountName: "user",
		}
		builder, err := getAzureServiceClientBuilder(options, nil)
		require.NoError(t, err)
		b, ok := builder.(*tokenClientBuilder)
		require.True(t, ok)
		require.Equal(t, "user", b.GetAccountName())
		require.Equal(t, "http://127.0.0.1:1000", b.serviceURL)
	}
}

func TestAzureWorkloadIdentity(t *testing.T) {
	t.Setenv("AZURE_CLIENT_ID", "workload-client")
	t.Setenv("AZURE_TENANT_ID", "workload-tenant")
	t.Setenv("AZURE_CLIENT_SECRET", "")
	t.Setenv("AZURE_FEDERATED_TOKEN_FILE", filepath.Join(t.TempDir(), "token"))
	t.Setenv("AZURE_STORAGE_ACCOUNT", "envaccount")
	t.Setenv("AZURE_STORAGE_KEY", "")
	t.Setenv("AZURE_AUTHORITY_HOST", "")

	for _, sendCredentials := range []bool{false, true} {
		t.Run(fmt.Sprintf("send credentials %t", sendCredentials), func(t *testing.T) {
			options := &backuppb.AzureBlobStorage{Bucket: "test"}
			builder, err := getAzureServiceClientBuilder(options, &storeapi.Options{SendCredentials: sendCredentials})
			require.NoError(t, err)
			require.IsType(t, &tokenClientBuilder{}, builder)
			require.IsType(t, &azidentity.WorkloadIdentityCredential{}, builder.(*tokenClientBuilder).cred)
			require.Equal(t, "envaccount", builder.GetAccountName())
			require.Equal(t, "https://envaccount.blob.core.windows.net", builder.GetServiceURL())
			if sendCredentials {
				require.Equal(t, "envaccount", options.AccountName)
			} else {
				require.Empty(t, options.AccountName)
			}
			require.Empty(t, options.SharedKey)
			require.Empty(t, options.AccessSig)
			_, err = builder.GetServiceClient()
			require.NoError(t, err)
		})
	}

	t.Run("account and endpoint options", func(t *testing.T) {
		options := &backuppb.AzureBlobStorage{
			Bucket: "test", AccountName: "account", Endpoint: "https://account.example.com",
		}
		builder, err := getAzureServiceClientBuilder(options, nil)
		require.NoError(t, err)
		require.IsType(t, &tokenClientBuilder{}, builder)
		require.Equal(t, options.AccountName, builder.GetAccountName())
		require.Equal(t, options.Endpoint, builder.GetServiceURL())
	})

	t.Run("credential precedence", func(t *testing.T) {
		t.Setenv("AZURE_STORAGE_KEY", "cGFzc3dk")
		options := &backuppb.AzureBlobStorage{Bucket: "test", AccountName: "account"}
		builder, err := getAzureServiceClientBuilder(options, nil)
		require.NoError(t, err)
		require.IsType(t, &tokenClientBuilder{}, builder)
		require.IsType(t, &azidentity.WorkloadIdentityCredential{}, builder.(*tokenClientBuilder).cred)

		t.Setenv("AZURE_CLIENT_SECRET", "secret")
		builder, err = getAzureServiceClientBuilder(options, nil)
		require.NoError(t, err)
		require.IsType(t, &tokenClientBuilder{}, builder)
		require.IsType(t, &azidentity.ClientSecretCredential{}, builder.(*tokenClientBuilder).cred)

		options.SharedKey = "cGFzc3dk"
		builder, err = getAzureServiceClientBuilder(options, nil)
		require.NoError(t, err)
		require.IsType(t, &sharedKeyClientBuilder{}, builder)

		options.AccessSig = "sig=test"
		builder, err = getAzureServiceClientBuilder(options, nil)
		require.NoError(t, err)
		require.IsType(t, &sasClientBuilder{}, builder)
	})

	t.Run("invalid tenant", func(t *testing.T) {
		t.Setenv("AZURE_TENANT_ID", "invalid tenant")
		// A configured workload identity must not silently fall back to a storage key.
		t.Setenv("AZURE_STORAGE_KEY", "cGFzc3dk")
		builder, err := getAzureServiceClientBuilder(&backuppb.AzureBlobStorage{Bucket: "test"}, nil)
		require.ErrorContains(t, err, "Failed to get azure workload identity credential")
		require.Nil(t, builder)
	})

	for _, name := range []string{"AZURE_CLIENT_ID", "AZURE_TENANT_ID"} {
		t.Run("missing "+name, func(t *testing.T) {
			t.Setenv(name, "")
			require.NoError(t, os.Unsetenv(name))
			builder, err := getAzureServiceClientBuilder(&backuppb.AzureBlobStorage{Bucket: "test"}, nil)
			require.ErrorContains(t, err, "Failed to get azure workload identity credential")
			require.Nil(t, builder)
		})
	}
}

type azureWorkloadIdentityTransport func(*http.Request) (*http.Response, error)

func (f azureWorkloadIdentityTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestAzureWorkloadIdentityAuthentication(t *testing.T) {
	t.Setenv("AZURE_CLIENT_ID", "workload-client")
	t.Setenv("AZURE_TENANT_ID", "workload-tenant")
	t.Setenv("AZURE_CLIENT_SECRET", "")
	t.Setenv("AZURE_STORAGE_KEY", "")
	t.Setenv("AZURE_AUTHORITY_HOST", "https://login.microsoftonline.com/")
	tokenFile := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(tokenFile, []byte("federated-token"), 0600))
	t.Setenv("AZURE_FEDERATED_TOKEN_FILE", tokenFile)

	tokenRequests, storageRequests := 0, 0
	client := &http.Client{Transport: azureWorkloadIdentityTransport(func(req *http.Request) (*http.Response, error) {
		body := ""
		switch req.URL.Path {
		case "/common/discovery/instance":
			body = `{"tenant_discovery_endpoint":"https://login.microsoftonline.com/workload-tenant/v2.0/.well-known/openid-configuration","metadata":[{"preferred_network":"login.microsoftonline.com","preferred_cache":"login.microsoftonline.com","aliases":["login.microsoftonline.com"]}]}`
		case "/workload-tenant/v2.0/.well-known/openid-configuration":
			body = `{"authorization_endpoint":"https://login.microsoftonline.com/workload-tenant/oauth2/v2.0/authorize","token_endpoint":"https://login.microsoftonline.com/workload-tenant/oauth2/v2.0/token","issuer":"https://login.microsoftonline.com/workload-tenant/v2.0"}`
		case "/workload-tenant/oauth2/v2.0/token":
			require.NoError(t, req.ParseForm())
			require.Equal(t, "workload-client", req.Form.Get("client_id"))
			require.Equal(t, "federated-token", req.Form.Get("client_assertion"))
			require.Equal(t, "urn:ietf:params:oauth:client-assertion-type:jwt-bearer", req.Form.Get("client_assertion_type"))
			require.Equal(t, "client_credentials", req.Form.Get("grant_type"))
			require.Contains(t, strings.Fields(req.Form.Get("scope")), "https://storage.azure.com/.default")
			body = `{"access_token":"access-token","expires_in":3600,"token_type":"Bearer"}`
			tokenRequests++
		case "/test":
			require.Equal(t, http.MethodGet, req.Method)
			require.Equal(t, "container", req.URL.Query().Get("restype"))
			require.Equal(t, "Bearer access-token", req.Header.Get("Authorization"))
			storageRequests++
		default:
			t.Fatalf("unexpected request: %s", req.URL.Redacted())
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(body)),
			Request:    req,
		}, nil
	})}
	_, err := newAzureBlobStorage(context.Background(), &backuppb.AzureBlobStorage{
		Bucket: "test", AccountName: "account",
	}, &storeapi.Options{HTTPClient: client})
	require.NoError(t, err)
	require.Equal(t, 1, tokenRequests)
	require.Equal(t, 1, storageRequests)
}

type fakeClientBuilder struct {
	Endpoint string
}

// GetServiceURL implements ClientBuilder.
func (b *fakeClientBuilder) GetServiceURL() string {
	return b.Endpoint
}

func (b *fakeClientBuilder) GetServiceClient() (*azblob.Client, error) {
	connStr := fmt.Sprintf("DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=%s/devstoreaccount1;", b.Endpoint)
	return azblob.NewClientFromConnectionString(connStr, getDefaultClientOptions())
}

func (b *fakeClientBuilder) GetAccountName() string {
	return "devstoreaccount1"
}

func TestDownloadRetry(t *testing.T) {
	var count int32 = 0
	var lock sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Log(r.URL)
		if strings.Contains(r.URL.String(), "restype=container") {
			w.WriteHeader(200)
			return
		}
		lock.Lock()
		count += 1
		lock.Unlock()
		header := w.Header()
		header.Add("Etag", "0x1")
		header.Add("Content-Length", "5")
		w.WriteHeader(200)
		w.Write([]byte("1234567"))
	}))

	defer server.Close()
	t.Log(server.URL)

	options := &backuppb.AzureBlobStorage{
		Bucket:       "test",
		Prefix:       "a/b/",
		StorageClass: "Hot",
	}

	ctx := context.Background()
	builder := &fakeClientBuilder{Endpoint: server.URL}
	s, err := newAzureBlobStorageWithClientBuilder(ctx, options, builder)
	require.NoError(t, err)
	_, err = s.ReadFile(ctx, "c")
	require.Error(t, err)
	require.Less(t, azblobRetryTimes, count)
}

func TestAzblobSeekToEndShouldNotError(t *testing.T) {
	const fileSize int32 = 16

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Log(r.URL)
		switch r.Method {
		case http.MethodHead:
			// Open file request, return file size
			header := w.Header()
			header.Add("Content-Length", fmt.Sprintf("%d", fileSize))
			w.WriteHeader(200)
		case http.MethodGet:
			if r.Header.Get("Range") != "" || r.Header.Get("x-ms-range") != "" {
				// Seek request, return an error
				w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			} else {
				w.WriteHeader(200)
			}
		}
	}))

	defer server.Close()
	t.Log(server.URL)

	options := &backuppb.AzureBlobStorage{
		Bucket: "test",
		Prefix: "a/b/",
	}

	ctx := context.Background()
	builder := &fakeClientBuilder{Endpoint: server.URL}
	s, err := newAzureBlobStorageWithClientBuilder(ctx, options, builder)
	require.NoError(t, err)

	r, err := s.Open(ctx, "c", nil)
	require.NoError(t, err)

	// Seek to end
	offset, err := r.Seek(0, io.SeekEnd)
	require.NoError(t, err)
	require.EqualValues(t, fileSize, offset)

	// Read after seek to end
	buf := make([]byte, 1)
	n, err := r.Read(buf)
	require.Equal(t, 0, n)
	require.Equal(t, io.EOF, err)

	require.NoError(t, r.Close())
}

type wr struct {
	w   objectio.Writer
	ctx context.Context
}

func (w wr) Write(bs []byte) (int, error) {
	return w.w.Write(w.ctx, bs)
}

func TestCopyObject(t *testing.T) {
	mkTestStrg := func(bucket, prefix string) *AzureBlobStorage {
		ctx := context.Background()
		options := &backuppb.AzureBlobStorage{
			Bucket: bucket,
			Prefix: prefix,
		}
		builder := &sharedKeyAzuriteClientBuilder{}
		skip, err := createContainer(ctx, builder, options.Bucket)
		if skip || err != nil {
			t.Skipf("azurite is not running, skip test (err = %s)", err)
			panic("just a note, should never reach here")
		}
		require.NoError(t, err)

		azblobStorage, err := newAzureBlobStorageWithClientBuilder(ctx, options, builder)
		require.NoError(t, err)
		return azblobStorage
	}

	strg1 := mkTestStrg("alice", "somewhat/")
	strg2 := mkTestStrg("bob", "complex/prefix/")

	ctx := context.Background()

	w, err := strg1.Create(ctx, "test.bin", &storeapi.WriterOption{})
	require.NoError(t, err)
	_, err = io.CopyN(wr{w, ctx}, rand.Reader, 300*1024*1024)
	require.NoError(t, err)
	require.NoError(t, strg2.CopyFrom(ctx, strg1, storeapi.CopySpec{
		From: "test.bin",
		To:   "somewhere/test.bin",
	}))
	srcReader, err := strg1.ReadFile(ctx, "test.bin")
	require.NoError(t, err)
	reader, err := strg2.ReadFile(ctx, "somewhere/test.bin")
	require.NoError(t, err)
	require.Equal(t, srcReader, reader)
}

func TestAzblobConcurrentUpload(t *testing.T) {
	ctx := context.Background()
	options := &backuppb.AzureBlobStorage{Bucket: "test", Prefix: "concurrent/"}
	builder := &sharedKeyAzuriteClientBuilder{}
	skip, err := createContainer(ctx, builder, options.Bucket)
	if skip || err != nil {
		t.Skip("azurite is not running, skip test")
	}

	azblobStorage, err := newAzureBlobStorageWithClientBuilder(ctx, options, builder)
	require.NoError(t, err)

	data := make([]byte, 20*1024*1024)
	_, err = rand.Read(data)
	require.NoError(t, err)

	w, err := azblobStorage.Create(ctx, "concurrent.bin", &storeapi.WriterOption{
		Concurrency: 4,
		PartSize:    4 * 1024 * 1024,
	})
	require.NoError(t, err)
	_, err = w.Write(ctx, data)
	require.NoError(t, err)
	require.NoError(t, w.Close(ctx))

	got, err := azblobStorage.ReadFile(ctx, "concurrent.bin")
	require.NoError(t, err)
	require.Equal(t, data, got)
}
