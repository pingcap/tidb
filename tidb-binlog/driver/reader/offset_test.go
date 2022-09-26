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

package reader

import (
	"os"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	pb "github.com/pingcap/tidb/tidb-binlog/proto/go-binlog"
	"github.com/stretchr/testify/require"
)

type testOffset struct {
	producer  sarama.SyncProducer
	config    *sarama.Config
	addr      string
	available bool
	topic     string
}

var to testOffset

func Setup() {
	to.topic = "test"
	to.addr = "192.168.198.61"
	if os.Getenv("HOSTIP") != "" {
		to.addr = os.Getenv("HOSTIP")
	}
	to.config = sarama.NewConfig()
	to.config.Producer.Partitioner = sarama.NewManualPartitioner
	to.config.Producer.Return.Successes = true
	to.config.Net.DialTimeout = time.Second * 3
	to.config.Net.ReadTimeout = time.Second * 3
	to.config.Net.WriteTimeout = time.Second * 3
	// need at least version to delete topic
	to.config.Version = sarama.V0_10_1_0
	var err error
	to.producer, err = sarama.NewSyncProducer([]string{to.addr + ":9092"}, to.config)
	if err == nil {
		to.available = true
	}
}

func deleteTopic(t *testing.T) {
	broker := sarama.NewBroker(to.addr + ":9092")
	err := broker.Open(to.config)
	require.NoError(t, err)
	defer broker.Close()
	broker.DeleteTopics(&sarama.DeleteTopicsRequest{Topics: []string{to.topic}, Timeout: 10 * time.Second})
}

func TestOffset(t *testing.T) {
	Setup()

	if !to.available {
		t.Skip("no kafka available")
	}

	deleteTopic(t)
	defer deleteTopic(t)

	topic := to.topic

	sk, err := NewKafkaSeeker([]string{to.addr + ":9092"}, to.config)
	require.NoError(t, err)

	to.producer, err = sarama.NewSyncProducer([]string{to.addr + ":9092"}, to.config)
	require.NoError(t, err)
	defer to.producer.Close()

	var testPoss = map[int64]int64{
		10: 0,
		20: 0,
		30: 0,
	}
	for ts := range testPoss {
		testPoss[ts], err = procudeMessage(ts, topic)
		require.NoError(t, err)
		// c.Log("produce ", ts, " at ", testPoss[ts])
	}

	var testCases = map[int64]int64{
		1:  testPoss[10],
		10: testPoss[20],
		15: testPoss[20],
		20: testPoss[30],
		35: testPoss[30] + 1,
	}
	for ts, offset := range testCases {
		offsetFounds, err := sk.Seek(topic, ts, []int32{0})
		t.Log("check: ", ts)
		require.NoError(t, err)
		require.Len(t, offsetFounds, 1)
		require.Equal(t, offset, offsetFounds[0])
	}
}

func procudeMessage(ts int64, topic string) (offset int64, err error) {
	binlog := new(pb.Binlog)
	binlog.CommitTs = ts
	var data []byte
	data, err = binlog.Marshal()
	if err != nil {
		return
	}

	msg := &sarama.ProducerMessage{
		Topic:     topic,
		Partition: int32(0),
		Key:       sarama.StringEncoder("key"),
		Value:     sarama.ByteEncoder(data),
	}
	_, offset, err = to.producer.SendMessage(msg)
	if err == nil {
		return
	}

	return offset, err
}
