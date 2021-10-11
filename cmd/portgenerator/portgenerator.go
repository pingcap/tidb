// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
