package infosync

import (
	"context"
	"fmt"

	"github.com/tici/proto/indexer"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	_ TiCIManager = &TiCIManagerCtx{}
)

// TiCIManager manages fulltext index for TiCI.
type TiCIManager interface {
	// CreateIndex create fulltext index on TiCI
	CreateIndex(ctx context.Context) error
}

// TiCIManagerCtx manages fulltext index for TiCI.
type TiCIManagerCtx struct {
	indexServiceClient indexer.IndexerServiceClient
}

// NewTiCIClient creates a new TiCI client connection.
func NewTiCIManager(ticiHost string, ticiPort string) (*TiCIManagerCtx, error) {
	conn, err := grpc.NewClient(fmt.Sprintf("%s:%s", ticiHost, ticiPort), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	indexServiceClient := indexer.NewIndexerServiceClient(conn)
	return &TiCIManagerCtx{
		indexServiceClient: indexServiceClient,
	}, nil
}

func (t *TiCIManagerCtx) CreateIndex(ctx context.Context) error {

	return nil
}
