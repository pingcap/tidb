package main

import (
	"context"
	"fmt"
	"time"

	logclient "github.com/pingcap/tidb/br/pkg/restore/log_client"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/br/pkg/utils/iter"
)

func run() error {
	ctx := context.Background()
	storagePath := "s3://astro/tpcc-1000-incr-with-crc64?endpoint=http://10.2.7.193:9000&access-key=minioadmin&secret-access-key=minioadmin"
	s, err := storage.NewFromURL(ctx, storagePath)
	if err != nil {
		return err
	}
	fmt.Println("storage created")
	time.AfterFunc(10*time.Second, func() {
		fmt.Println(string(utils.AllStackInfo()))
	})
	res := iter.CollectAll(ctx, logclient.LoadMigrations(ctx, s))
	if res.Err != nil {
		return res.Err
	}
	chosenOne := res.Item[0].Compactions[0]
	fmt.Printf("%#v\n", chosenOne.Artifactes)
	fmt.Printf("%#v\n", chosenOne)
	res2 := iter.CollectAll(ctx, logclient.Subcompactions(ctx, chosenOne.Artifactes, s))
	if res2.Err != nil {
		return res2.Err
	}
	fmt.Printf("%#v\n", res2.Item[0])
	return nil
}

func main() {
	if err := run(); err != nil {
		panic(err)
	}
}
