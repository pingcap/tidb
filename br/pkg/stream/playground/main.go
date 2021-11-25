// WARNING: this file is just for debugging before the `br stream` command get ready.
// feel free to edit it and do things you want.
// this file should be removed after things get ready.

package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"runtime"
	"strconv"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/stream"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/spf13/pflag"
	"go.etcd.io/etcd/clientv3"
)

func simpleRanges(tableCount int) stream.Ranges {
	ranges := stream.Ranges{}
	for i := 0; i < tableCount; i++ {
		base := int64(i*2 + 1)
		ranges = append(ranges, stream.Range{
			StartKey: tablecodec.EncodeTablePrefix(base),
			EndKey:   tablecodec.EncodeTablePrefix(base + 1),
		})
	}
	return ranges
}

func simpleTask(name string, tableCount int) stream.TaskInfo {
	backend, _ := storage.ParseBackend("noop://", nil)
	return stream.TaskInfo{
		StreamBackupTaskInfo: backuppb.StreamBackupTaskInfo{
			Storage:     backend,
			StartTs:     0,
			EndTs:       1000,
			Name:        name,
			TableFilter: []string{"*.*"},
		},
		Ranges:  simpleRanges(tableCount),
		Pausing: false,
	}
}

func caller(skip int) string {
	pc, file, line, ok := runtime.Caller(skip + 1)
	if !ok {
		return "<unknown>"
	}
	f := runtime.FuncForPC(pc)
	return f.Name() + "(" + file + ":" + strconv.Itoa(line) + ")"
}

func must(err error) {
	if err != nil {
		log.Fatalf("%s: %s\n", caller(1), err)
	}
}

var (
	commad = pflag.String("command", "insert", "The command to execute(insert | remove)")
	name   = pflag.String("name", "", "The target")
	tables = pflag.Int("table", 4, "The table count(only avaliable in 'insert')")
	etcd   = pflag.StringSlice("etcd", []string{"127.0.0.1:2379"}, "The etcd address")
)

func main() {
	pflag.Parse()
	ctx := context.Background()
	if *name == "" {
		must(errors.New("must specify name"))
	}
	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints: *etcd,
	})
	must(err)
	cli := stream.MetaDataClient{
		Client: etcdCli,
	}
	switch *commad {
	case "insert":
		task := simpleTask(*name, *tables)
		must(cli.PutTask(ctx, task))
	case "delete":
		must(cli.DeleteTask(ctx, *name))
	case "get":
		task, err := cli.GetTask(ctx, *name)
		must(err)
		fmt.Printf("%v\n", task)
	default:
		must(fmt.Errorf("command %s not supported", *commad))
	}
}
