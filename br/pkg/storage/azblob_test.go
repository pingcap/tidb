package storage

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	. "github.com/pingcap/check"
	backup "github.com/pingcap/kvproto/pkg/brpb"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
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

func checkAzuriteRunning() bool {
	checkStatement := fmt.Sprintf("lsof -i:%d ", 10000)
	output, _ := exec.Command("sh", "-c", checkStatement).CombinedOutput()
	return len(output) > 0
}

func (r *testStorageSuite) TestAzBlob(c *C) {
	if !checkAzuriteRunning() {
		c.Log("azurite is not running, skip test")
		return
	} else {
		c.Log("found port 10000 is occupied, run test")
	}

	ctx := context.Background()
	options := &backup.AzureBlobStorage{
		Bucket: "test",
		Prefix: "a/b/",
	}
	azblobStorage, err := newAzureBlobStorageWithClientBuilder(ctx, options, &sharedKeyAzuriteClientBuilder{})
	c.Assert(err, IsNil)

	err = azblobStorage.WriteFile(ctx, "key", []byte("data"))
	c.Assert(err, IsNil)

	err = azblobStorage.WriteFile(ctx, "key1", []byte("data1"))
	c.Assert(err, IsNil)

	err = azblobStorage.WriteFile(ctx, "key2", []byte("data22223346757222222222289722222"))
	c.Assert(err, IsNil)

	d, err := azblobStorage.ReadFile(ctx, "key")
	c.Assert(err, IsNil)
	c.Assert(d, DeepEquals, []byte("data"))

	exist, err := azblobStorage.FileExists(ctx, "key")
	c.Assert(err, IsNil)
	c.Assert(exist, IsTrue)

	exist, err = azblobStorage.FileExists(ctx, "key_not_exist")
	c.Assert(err, IsNil)
	c.Assert(exist, IsFalse)

	keyDelete := "key_delete"
	exist, err = azblobStorage.FileExists(ctx, keyDelete)
	c.Assert(err, IsNil)
	c.Assert(exist, IsFalse)

	err = azblobStorage.WriteFile(ctx, keyDelete, []byte("data"))
	c.Assert(err, IsNil)

	exist, err = azblobStorage.FileExists(ctx, keyDelete)
	c.Assert(err, IsNil)
	c.Assert(exist, IsTrue)

	err = azblobStorage.DeleteFile(ctx, keyDelete)
	c.Assert(err, IsNil)

	exist, err = azblobStorage.FileExists(ctx, keyDelete)
	c.Assert(err, IsNil)
	c.Assert(exist, IsFalse)

	list := ""
	var totalSize int64 = 0
	err = azblobStorage.WalkDir(ctx, nil, func(name string, size int64) error {
		list += name
		totalSize += size
		return nil
	})
	c.Assert(err, IsNil)
	c.Assert(list, Equals, "keykey1key2")
	c.Assert(totalSize, Equals, int64(42))

	efr, err := azblobStorage.Open(ctx, "key2")
	c.Assert(err, IsNil)

	p := make([]byte, 10)
	n, err := efr.Read(p)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 10)
	c.Assert(string(p), Equals, "data222233")

	p = make([]byte, 40)
	n, err = efr.Read(p)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 23)
	c.Assert(string(p[:23]), Equals, "46757222222222289722222")

	p = make([]byte, 5)
	offs, err := efr.Seek(3, io.SeekStart)
	c.Assert(err, IsNil)
	c.Assert(offs, Equals, int64(3))

	n, err = efr.Read(p)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 5)
	c.Assert(string(p), Equals, "a2222")

	p = make([]byte, 5)
	offs, err = efr.Seek(3, io.SeekCurrent)
	c.Assert(err, IsNil)
	c.Assert(offs, Equals, int64(11))

	n, err = efr.Read(p)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 5)
	c.Assert(string(p), Equals, "67572")

	p = make([]byte, 5)
	offs, err = efr.Seek(int64(-7), io.SeekEnd)
	c.Assert(err, IsNil)
	// Note, change it to maxCnt - offs
	c.Assert(offs, Equals, int64(26))

	n, err = efr.Read(p)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 5)
	c.Assert(string(p), Equals, "97222")

	err = efr.Close()
	c.Assert(err, IsNil)

	c.Assert(azblobStorage.URI(), Equals, "azure://test/a/b/")
}

func (r *testStorageSuite) TestAzBlobBuilder(c *C) {
	{
		options := &backuppb.AzureBlobStorage{
			Endpoint:    "http://127.0.0.1:1000",
			Bucket:      "test",
			Prefix:      "a/b",
			AccountName: "user",
			SharedKey:   "cGFzc3dk",
		}
		builder, err := getAzureServiceClientBuilder(options, nil)
		c.Assert(err, IsNil)
		b, ok := builder.(*sharedKeyClientBuilder)
		c.Assert(ok, IsTrue)
		c.Assert(b.GetAccountName(), Equals, "user")
		c.Assert(b.serviceURL, Equals, "http://127.0.0.1:1000")
	}

	{
		options := &backuppb.AzureBlobStorage{
			Bucket:      "test",
			Prefix:      "a/b",
			AccountName: "user",
			SharedKey:   "cGFzc3dk",
		}
		builder, err := getAzureServiceClientBuilder(options, nil)
		c.Assert(err, IsNil)
		b, ok := builder.(*sharedKeyClientBuilder)
		c.Assert(ok, IsTrue)
		c.Assert(b.GetAccountName(), Equals, "user")
		c.Assert(b.serviceURL, Equals, "https://user.blob.core.windows.net")
	}

	err := os.Setenv("AZURE_STORAGE_ACCOUNT", "env_user")
	c.Assert(err, IsNil)
	defer os.Unsetenv("AZURE_STORAGE_ACCOUNT")

	{
		options := &backuppb.AzureBlobStorage{
			Bucket:      "test",
			Prefix:      "a/b",
			AccountName: "user",
			SharedKey:   "cGFzc3dk",
		}
		builder, err := getAzureServiceClientBuilder(options, nil)
		c.Assert(err, IsNil)
		b, ok := builder.(*sharedKeyClientBuilder)
		c.Assert(ok, IsTrue)
		c.Assert(b.GetAccountName(), Equals, "user")
		c.Assert(b.serviceURL, Equals, "https://user.blob.core.windows.net")
	}

	{
		options := &backuppb.AzureBlobStorage{
			Bucket:    "test",
			Prefix:    "a/b",
			SharedKey: "cGFzc3dk",
		}
		_, err := getAzureServiceClientBuilder(options, nil)
		c.Assert(err, NotNil)
	}

	err = os.Setenv("AZURE_STORAGE_KEY", "cGFzc3dk")
	c.Assert(err, IsNil)
	defer os.Unsetenv("AZURE_STORAGE_KEY")

	{
		options := &backuppb.AzureBlobStorage{
			Endpoint:  "http://127.0.0.1:1000",
			Bucket:    "test",
			Prefix:    "a/b",
			SharedKey: "cGFzc2dk",
		}
		builder, err := getAzureServiceClientBuilder(options, nil)
		c.Assert(err, IsNil)
		b, ok := builder.(*sharedKeyClientBuilder)
		c.Assert(ok, IsTrue)
		c.Assert(b.GetAccountName(), Equals, "env_user")
		c.Assert(b.serviceURL, Equals, "http://127.0.0.1:1000")
	}

	err = os.Setenv("AZURE_CLIENT_ID", "321")
	c.Assert(err, IsNil)
	defer os.Unsetenv("AZURE_CLIENT_ID")

	err = os.Setenv("AZURE_TENANT_ID", "321")
	c.Assert(err, IsNil)
	defer os.Unsetenv("AZURE_TENANT_ID")

	err = os.Setenv("AZURE_CLIENT_SECRET", "321")
	c.Assert(err, IsNil)
	defer os.Unsetenv("AZURE_CLIENT_SECRET")

	{
		options := &backuppb.AzureBlobStorage{
			Endpoint: "http://127.0.0.1:1000",
			Bucket:   "test",
			Prefix:   "a/b",
		}
		builder, err := getAzureServiceClientBuilder(options, nil)
		c.Assert(err, IsNil)
		b, ok := builder.(*tokenClientBuilder)
		c.Assert(ok, IsTrue)
		c.Assert(b.GetAccountName(), Equals, "env_user")
		c.Assert(b.serviceURL, Equals, "http://127.0.0.1:1000")
	}

	{
		options := &backuppb.AzureBlobStorage{
			Endpoint:  "http://127.0.0.1:1000",
			Bucket:    "test",
			Prefix:    "a/b",
			SharedKey: "cGFzc2dk",
		}
		builder, err := getAzureServiceClientBuilder(options, nil)
		c.Assert(err, IsNil)
		b, ok := builder.(*tokenClientBuilder)
		c.Assert(ok, IsTrue)
		c.Assert(b.GetAccountName(), Equals, "env_user")
		c.Assert(b.serviceURL, Equals, "http://127.0.0.1:1000")
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
		c.Assert(err, IsNil)
		b, ok := builder.(*sharedKeyClientBuilder)
		c.Assert(ok, IsTrue)
		c.Assert(b.GetAccountName(), Equals, "user")
		c.Assert(b.serviceURL, Equals, "http://127.0.0.1:1000")
	}

	{
		options := &backuppb.AzureBlobStorage{
			Endpoint:    "http://127.0.0.1:1000",
			Bucket:      "test",
			Prefix:      "a/b",
			AccountName: "user",
		}
		builder, err := getAzureServiceClientBuilder(options, nil)
		c.Assert(err, IsNil)
		b, ok := builder.(*tokenClientBuilder)
		c.Assert(ok, IsTrue)
		c.Assert(b.GetAccountName(), Equals, "user")
		c.Assert(b.serviceURL, Equals, "http://127.0.0.1:1000")
	}

}
