package main

import (
	"flag"
	"fmt"

	"github.com/phayes/freeport"
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
	for ports, err = freeport.GetFreePorts(count); err != nil; ports, err = freeport.GetFreePorts(count) {
	}
	return ports
}

func main() {
	flag.Parse()
	for _, port := range generatePorts(int(count)) {
		fmt.Println(port)
	}
}
