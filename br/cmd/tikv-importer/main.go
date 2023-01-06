// Copyright 2022 PingCAP, Inc.
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
	"log"

	"github.com/pingcap/tidb/br/pkg/lightning/importer"
)

func main() {
	var (
		addr    = flag.String("addr", ":8287", "importer server address")
		pdAddr  = flag.String("pd-addr", "127.0.0.1:2379", "pd address")
		dataDir = flag.String("data-dir", "/tmp/importer", "data directory")
	)
	flag.Parse()

	server, err := importer.NewServer(
		importer.WithServerAddr(*addr),
		importer.WithPDAddr(*pdAddr),
		importer.WithDataDir(*dataDir),
	)
	if err != nil {
		log.Fatalf("fail to create server: %v", err)
	}
	if err := server.Run(); err != nil {
		log.Fatalf("server exit with error: %v", err)
	}
}
