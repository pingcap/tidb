package main

import (
	"context"
	"fmt"

	logclient "github.com/pingcap/tidb/br/pkg/restore/log_client"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/utils/iter"
)

func run() error {
	ctx := context.Background()
	storagePath := "s3://astro/tpcc-1000-incr-with-crc64?endpoint=http://10.2.7.193:9000"
	s, err := storage.NewFromURL(ctx, storagePath)
	if err != nil {
		return err
	}
	fmt.Printf("%#v\n", iter.CollectAll(ctx, logclient.LoadMigrations(ctx, s)))
	return nil
}

func main() {
	if err := run(); err != nil {
		panic(err)
	}
}
