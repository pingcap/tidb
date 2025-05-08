package infosync

import (
	"context"
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/indexer"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	_ tiCIManager = &TiCIManagerCtx{}
)

// tiCIManager manages fulltext index for TiCI.
type tiCIManager interface {
	// CreateFulltextIndex create fulltext index on TiCI
	CreateFulltextIndex(ctx context.Context, tblInfo *model.TableInfo, indexInfo *model.IndexInfo, schemaName string) error
}

// TiCIManagerCtx manages fulltext index for TiCI.
type TiCIManagerCtx struct {
	indexServiceClient indexer.IndexerServiceClient
}

// newTiCIManager creates a new TiCI manager.
func newTiCIManager(ticiHost string, ticiPort string) (*TiCIManagerCtx, error) {
	conn, err := grpc.NewClient(fmt.Sprintf("%s:%s", ticiHost, ticiPort), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	indexServiceClient := indexer.NewIndexerServiceClient(conn)
	return &TiCIManagerCtx{
		indexServiceClient: indexServiceClient,
	}, nil
}

func (t *TiCIManagerCtx) CreateFulltextIndex(ctx context.Context, tblInfo *model.TableInfo, indexInfo *model.IndexInfo, schemaName string) error {
	columns := make([]*indexer.ColumnInfo, 0)
	for i := range indexInfo.Columns {
		columns = append(columns, &indexer.ColumnInfo{
			ColumnId:     indexInfo.ID,
			ColumnName:   indexInfo.Name.L,
			Type:         int32(indexInfo.Tp),
			ColumnLength: int32(indexInfo.Columns[i].Length),
			Decimal:      int32(tblInfo.Columns[i].GetDecimal()),
			DefaultVal:   tblInfo.Columns[i].DefaultValueBit,
			IsPrimaryKey: indexInfo.Primary,
			IsArray:      false,
		})
	}
	req := &indexer.CreateIndexRequest{
		IndexInfo: &indexer.IndexInfo{
			TableId:   tblInfo.ID,
			IndexId:   indexInfo.ID,
			IndexName: indexInfo.Name.L,
			IndexType: indexer.IndexType_FULL_TEXT,
			Columns:   columns,
			IsUnique:  indexInfo.Unique,
			ParserInfo: &indexer.ParserInfo{
				ParserType: indexer.ParserType_DEFAULT_PARSER,
			},
		},
		TableInfo: &indexer.TableInfo{
			TableId:      tblInfo.ID,
			TableName:    tblInfo.Name.L,
			DatabaseName: schemaName,
			Version:      int64(tblInfo.Version),
			Columns:      columns,
		},
	}
	resp, err := t.indexServiceClient.CreateIndex(ctx, req)
	if err != nil {
		return errors.Trace(err)
	}
	if resp.Status != 0 {
		logutil.BgLogger().Error("create fulltext index failed", zap.String("indexID", resp.IndexId), zap.String("errorMessage", resp.ErrorMessage))
		return errors.New(resp.ErrorMessage)
	}
	logutil.BgLogger().Info("create fulltext index success", zap.String("indexID", resp.IndexId))

	return nil
}
