package main

import (
	"context"
	"os"
	"os/signal"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/glue"
)

func main() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	_, _, err := log.InitLogger(&log.Config{})
	if err != nil {
		panic(err)
	}
	log.Info("hello, world")

	g := glue.ConsoleOperations{glue.StdIOGlue{}}
	total := 10000

	pb := g.StartProgressBar("Test", total, glue.WithTimeCost(), glue.WithConstExtraField("hello", "world"))
	for i := 0; i < total; i++ {
		select {
		case <-c:
			pb.Close()
			time.Sleep(1 * time.Second)
			return
		default:
		}
		pb.Inc()
		time.Sleep(10 * time.Millisecond)
	}

	pb.Wait(context.TODO())
}
