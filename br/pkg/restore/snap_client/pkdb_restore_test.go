package snapclient

import (
	"testing"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestPkdbGetSSTMetaFromFileCarriesPackageInfo(t *testing.T) {
	file := &backuppb.File{
		Name:              "logical_write.sst",
		StartKey:          []byte("t1a"),
		EndKey:            []byte("t1ccc"),
		PackageFileName:   "backup-000001.pack",
		PackageFileOffset: 4096,
	}
	rule := &import_sstpb.RewriteRule{
		OldKeyPrefix: []byte("t1"),
		NewKeyPrefix: []byte("t2"),
	}
	region := &metapb.Region{
		StartKey: []byte("t2abc"),
		EndKey:   []byte("t3a"),
	}

	sstMeta, err := getSSTMetaFromFile(file, region, rule, RewriteModeLegacy)
	require.NoError(t, err)
	require.Equal(t, "backup-000001.pack", sstMeta.GetPackageFileName())
	require.Equal(t, uint64(4096), sstMeta.GetPackageFileOffset())
}

func TestPkdbDownloadErrorDetectsPackedSSTReadDirectly(t *testing.T) {
	resp := downloadResponseWithError("Cannot read s3://bucket/prefix/logical_write.sst into /tmp/tikv/import/logical_write.sst")
	req := &import_sstpb.DownloadRequest{
		Name: "logical_write.sst",
		Sst: import_sstpb.SSTMeta{
			PackageFileName: "backup-000001.pack",
		},
	}

	err := annotateDownloadResponseError(resp, req)
	require.ErrorIs(t, errors.Cause(err), berrors.ErrVersionMismatch)
	require.Contains(t, err.Error(), "logical_write.sst")
	require.Contains(t, err.Error(), "backup-000001.pack")
}

func TestPkdbBatchDownloadErrorDetectsPackedSSTReadDirectly(t *testing.T) {
	resp := downloadResponseWithError("Cannot read local:///tmp/backup/logical_write.sst into /tmp/tikv/import/logical_write.sst")
	req := &import_sstpb.DownloadRequest{
		Ssts: map[string]*import_sstpb.SSTMeta{
			"logical_write.sst": {
				PackageFileName: "backup-000001.pack",
			},
		},
	}

	err := annotateDownloadResponseError(resp, req)
	require.ErrorIs(t, errors.Cause(err), berrors.ErrVersionMismatch)
	require.Contains(t, err.Error(), "logical_write.sst")
	require.Contains(t, err.Error(), "backup-000001.pack")
}

func downloadResponseWithError(msg string) *import_sstpb.DownloadResponse {
	return &import_sstpb.DownloadResponse{
		Error: &import_sstpb.Error{
			Message: msg,
		},
	}
}
