// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"flag"

	"github.com/Shopify/sarama"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/tidb-binlog/driver/reader"
	"go.uber.org/zap"
)

var (
	offset    = flag.Int64("offset", sarama.OffsetNewest, "offset")
	commitTS  = flag.Int64("commitTS", 0, "commitTS")
	clusterID = flag.String("clusterID", "", "clusterID")
	topic     = flag.String("topic", "", "topic name to consume binlog, one of topic or clusterID must be set")
)

func main() {
	flag.Parse()

	cfg := &reader.Config{
		KafkaAddr: []string{"127.0.0.1:9092"},
		Offset:    *offset,
		CommitTS:  *commitTS,
		ClusterID: *clusterID,
		Topic:     *topic,
	}

	breader, err := reader.NewReader(cfg)
	if err != nil {
		panic(err)
	}

	for msg := range breader.Messages() {
		log.Info("recv", zap.Stringer("message", msg.Binlog))
	}
}
