// Copyright 2021 PingCAP, Inc. Licensed under Apache-2.0.

package storage

import (
	"context"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/stretchr/testify/require"
)

// use shared key to access azurite
type sharedKeyAzuriteClientBuilder struct {
}

func (b *sharedKeyAzuriteClientBuilder) GetServiceClient() (azblob.ServiceClient, error) {
	connStr := "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
	return azblob.NewServiceClientFromConnectionString(connStr, nil)
}

func (b *sharedKeyAzuriteClientBuilder) GetAccountName() string {
	return "devstoreaccount1"
}

func TestAzblob(t *testing.T) {
	ctx := context.Background()
	options := &backuppb.AzureBlobStorage{
		Bucket: "test",
		Prefix: "a/b/",
	}

	azblobStorage, err := newAzureBlobStorageWithClientBuilder(ctx, options, &sharedKeyAzuriteClientBuilder{})
	if err != nil {
		if strings.Contains(err.Error(), "connect: connection refused") {
			t.Log("azurite is not running, skip test")
			return
		}
	}
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

	efr, err := azblobStorage.Open(ctx, "key2")
	require.NoError(t, err)

	p := make([]byte, 10)
	n, err := efr.Read(p)
	require.NoError(t, err)
	require.Equal(t, 10, n)
	require.Equal(t, "data222233", string(p))

	p = make([]byte, 40)
	n, err = efr.Read(p)
	require.NoError(t, err)
	require.Equal(t, 23, n)
	require.Equal(t, "46757222222222289722222", string(p[:23]))

	p = make([]byte, 5)
	offs, err := efr.Seek(3, io.SeekStart)
	require.NoError(t, err)
	require.Equal(t, int64(3), offs)

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
		_, err := getAzureServiceClientBuilder(options, nil)
		require.Error(t, err)
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
