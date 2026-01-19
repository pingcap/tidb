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
	"crypto/sha256"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"

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

	type blockList struct {
		XMLName xml.Name `xml:"BlockList"`
		Latest  []string `xml:"Latest"`
	}

	var mu sync.Mutex
	blobs := make(map[string][]byte)
	blocks := make(map[string]map[string][]byte)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.RawQuery, "restype=container") {
			w.WriteHeader(200)
			return
		}

		blobPath := r.URL.Path
		switch r.Method {
		case http.MethodPut:
			body, readErr := io.ReadAll(r.Body)
			if readErr != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}

			comp := r.URL.Query().Get("comp")
			switch comp {
			case "block":
				blockID := r.URL.Query().Get("blockid")
				if blockID == "" {
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				mu.Lock()
				if _, ok := blocks[blobPath]; !ok {
					blocks[blobPath] = make(map[string][]byte)
				}
				blocks[blobPath][blockID] = body
				mu.Unlock()
				w.WriteHeader(201)
				return
			case "blocklist":
				var bl blockList
				if err := xml.Unmarshal(body, &bl); err != nil || len(bl.Latest) == 0 {
					w.WriteHeader(http.StatusBadRequest)
					return
				}

				var assembled []byte
				mu.Lock()
				blockMap := blocks[blobPath]
				if blockMap == nil {
					mu.Unlock()
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				for _, blockID := range bl.Latest {
					data, ok := blockMap[blockID]
					if !ok {
						mu.Unlock()
						w.WriteHeader(http.StatusBadRequest)
						return
					}
					assembled = append(assembled, data...)
				}
				blobs[blobPath] = assembled
				mu.Unlock()

				w.WriteHeader(201)
				return
			default:
				mu.Lock()
				blobs[blobPath] = body
				mu.Unlock()
				w.WriteHeader(201)
				return
			}
		case http.MethodGet, http.MethodHead:
			mu.Lock()
			data, ok := blobs[blobPath]
			mu.Unlock()
			if !ok {
				w.WriteHeader(404)
				return
			}
			w.Header().Set("Etag", "0x1")
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(data)))
			w.WriteHeader(200)
			if r.Method == http.MethodGet {
				_, _ = w.Write(data)
			}
			return
		default:
			w.WriteHeader(200)
			return
		}
	}))
	defer server.Close()

	options := &backuppb.AzureBlobStorage{
		Bucket: "test",
		Prefix: "a/b/",
	}
	builder := &fakeClientBuilder{Endpoint: server.URL}
	azblobStorage, err := newAzureBlobStorageWithClientBuilder(ctx, options, builder)
	require.NoError(t, err)

	key := "key"
	data := []byte("data")
	err = azblobStorage.WriteFile(ctx, key, data)
	require.NoError(t, err)

	d, err := azblobStorage.ReadFile(ctx, key)
	require.NoError(t, err)
	require.Equal(t, data, d)

	require.Equal(t, "azure://test/a/b/", azblobStorage.URI())
}

func TestNewAzblobStorage(t *testing.T) {
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
	hasher := sha256.New()
	const testFileSize = 128 * 1024 * 1024
	_, err = io.Copy(wr{w, ctx}, io.TeeReader(io.LimitReader(rand.Reader, testFileSize), hasher))
	require.NoError(t, err)
	require.NoError(t, w.Close(ctx))
	checksum := hasher.Sum(nil)

	require.NoError(t, strg2.CopyFrom(ctx, strg1, storeapi.CopySpec{
		From: "test.bin",
		To:   "somewhere/test.bin",
	}))

	reader, err := strg2.Open(ctx, "somewhere/test.bin", nil)
	require.NoError(t, err)
	defer reader.Close()
	gotHasher := sha256.New()
	_, err = io.CopyBuffer(gotHasher, reader, make([]byte, 4*1024*1024))
	require.NoError(t, err)
	require.Equal(t, checksum, gotHasher.Sum(nil))
}
