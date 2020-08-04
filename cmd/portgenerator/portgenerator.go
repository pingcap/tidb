package main

import (
	"flag"
	"fmt"

	"github.com/phayes/freeport"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

var (
	count uint
)

func init() {
	flag.UintVar(&count, "count", 1, "number of generated ports")
}

func generatePorts(count int) []int {
	var (
		err   error
		ports []int
	)
	if ports, err = freeport.GetFreePorts(count); err != nil {
		log.Fatal("no more free ports", zap.Error(err))
	}
	return ports
}

func main() {
	flag.Parse()
	for _, port := range generatePorts(int(count)) {
		fmt.Println(port)
	}
}
