package backup

import (
	"testing"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestPkdbPackedBackupResponseCompatibility(t *testing.T) {
	cases := []struct {
		name    string
		files   []*backuppb.File
		wantErr error
	}{
		{
			name: "direct SST is rejected when packed backup is requested",
			files: []*backuppb.File{
				{Name: "logical_write.sst"},
			},
			wantErr: berrors.ErrVersionMismatch,
		},
		{
			name: "logical SST is accepted when package metadata exists",
			files: []*backuppb.File{
				{
					Name:            "logical_write.sst",
					PackageFileName: "backup-000001.pack",
				},
			},
			wantErr: nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client := &Client{}
			client.SetExpectPackedFiles(true)

			err := client.checkPackedBackupResponse(1, &backuppb.BackupResponse{
				Files: tc.files,
			})
			if tc.wantErr == nil {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, errors.Cause(err), tc.wantErr)
		})
	}
}
