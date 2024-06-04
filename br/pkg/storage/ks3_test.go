package storage

import (
	"context"
	"testing"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/stretchr/testify/require"
)

func TestKS3(t *testing.T) {
	ctx := context.Background()
	{
		// force path style is off
		path := "ks3://bucket/prefix?access-key=xxx&secret-access-key=xxxxxx&endpoint=http%3a%2f%2fxxx.ksyuncs.com&force-path-style=false&region=xxx"
		backend, err := ParseBackend(path, nil)
		require.NoError(t, err)
		stg, err := New(ctx, backend, nil)
		require.NoError(t, err)
		ks3stg := stg.(*KS3Storage)
		require.Equal(t, "bucket", ks3stg.options.Bucket)
		require.Equal(t, "prefix/", ks3stg.options.Prefix)
		require.Equal(t, "bucket", backend.Backend.(*backuppb.StorageBackend_S3).S3.Bucket)
		require.Equal(t, "prefix", backend.Backend.(*backuppb.StorageBackend_S3).S3.Prefix)
	}

	{
		// force path style is on
		path := "ks3://prefix?access-key=xxx&secret-access-key=xxxxxx&endpoint=http%3a%2f%2fbucket.xxx.ksyuncs.com&force-path-style=true&region=xxx"
		backend, err := ParseBackend(path, nil)
		require.NoError(t, err)
		stg, err := New(ctx, backend, nil)
		require.NoError(t, err)
		ks3stg := stg.(*KS3Storage)
		require.Equal(t, "bucket", ks3stg.options.Bucket)
		require.Equal(t, "prefix/", ks3stg.options.Prefix)
		require.Equal(t, "prefix", backend.Backend.(*backuppb.StorageBackend_S3).S3.Bucket)
		require.Equal(t, "", backend.Backend.(*backuppb.StorageBackend_S3).S3.Prefix)
	}

	{
		// force path style is on
		path := "ks3://prefix/prefix2?access-key=xxx&secret-access-key=xxxxxx&endpoint=http%3a%2f%2fbucket.xxx.ksyuncs.com&force-path-style=true&region=xxx"
		backend, err := ParseBackend(path, nil)
		require.NoError(t, err)
		stg, err := New(ctx, backend, nil)
		require.NoError(t, err)
		ks3stg := stg.(*KS3Storage)
		require.Equal(t, "bucket", ks3stg.options.Bucket)
		require.Equal(t, "prefix/prefix2/", ks3stg.options.Prefix)
		require.Equal(t, "prefix", backend.Backend.(*backuppb.StorageBackend_S3).S3.Bucket)
		require.Equal(t, "prefix2", backend.Backend.(*backuppb.StorageBackend_S3).S3.Prefix)
	}
}
