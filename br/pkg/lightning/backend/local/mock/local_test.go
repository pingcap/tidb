package mock

import (
	"context"
	"testing"

	"github.com/coreos/go-semver/semver"
	"github.com/golang/mock/gomock"
	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/local"
	"github.com/pingcap/tidb/br/pkg/lightning/mydump"
	"github.com/pingcap/tidb/br/pkg/mock"
	"github.com/stretchr/testify/require"
)

var tiFlashReplicaQuery = "SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.TIFLASH_REPLICA WHERE REPLICA_COUNT > 0;"

func TestCheckRequirementsTiFlash(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()
	glue := mock.NewMockGlue(controller)
	exec := mock.NewMockSQLExecutor(controller)
	ctx := context.Background()

	dbMetas := []*mydump.MDDatabaseMeta{
		{
			Name: "test",
			Tables: []*mydump.MDTableMeta{
				{
					DB:        "test",
					Name:      "t1",
					DataFiles: []mydump.FileInfo{{}},
				},
				{
					DB:        "test",
					Name:      "tbl",
					DataFiles: []mydump.FileInfo{{}},
				},
			},
		},
		{
			Name: "test1",
			Tables: []*mydump.MDTableMeta{
				{
					DB:        "test1",
					Name:      "t",
					DataFiles: []mydump.FileInfo{{}},
				},
				{
					DB:        "test1",
					Name:      "tbl",
					DataFiles: []mydump.FileInfo{{}},
				},
			},
		},
	}
	checkCtx := &backend.CheckCtx{DBMetas: dbMetas}

	glue.EXPECT().GetSQLExecutor().Return(exec)
	exec.EXPECT().QueryStringsWithLog(ctx, tiFlashReplicaQuery, gomock.Any(), gomock.Any()).
		Return([][]string{{"db", "tbl"}, {"test", "t1"}, {"test1", "tbl"}}, nil)

	err := local.CheckTiFlashVersion4test(ctx, glue, checkCtx, *semver.New("4.0.2"))
	require.Regexp(t, "^lightning local backend doesn't support TiFlash in this TiDB version. conflict tables: \\[`test`.`t1`, `test1`.`tbl`\\]", err.Error())
}
