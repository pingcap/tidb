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
	. "github.com/pingcap/check"
	pb "github.com/pingcap/tidb/tidb-binlog/proto/go-binlog"
)

func TestClient(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testOffsetSuite{})

type testOffsetSuite struct {
	producer  sarama.SyncProducer
	config    *sarama.Config
	addr      string
	available bool
	topic     string
}

func (to *testOffsetSuite) SetUpSuite(c *C) {
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

func (to *testOffsetSuite) deleteTopic(c *C) {
	broker := sarama.NewBroker(to.addr + ":9092")
	err := broker.Open(to.config)
	c.Assert(err, IsNil)
	defer broker.Close()
	broker.DeleteTopics(&sarama.DeleteTopicsRequest{Topics: []string{to.topic}, Timeout: 10 * time.Second})
}

func (to *testOffsetSuite) TestOffset(c *C) {
	if !to.available {
		c.Skip("no kafka available")
	}

	to.deleteTopic(c)
	defer to.deleteTopic(c)

	topic := to.topic

	sk, err := NewKafkaSeeker([]string{to.addr + ":9092"}, to.config)
	c.Assert(err, IsNil)

	to.producer, err = sarama.NewSyncProducer([]string{to.addr + ":9092"}, to.config)
	c.Assert(err, IsNil)
	defer to.producer.Close()

	var testPoss = map[int64]int64{
		10: 0,
		20: 0,
		30: 0,
	}
	for ts := range testPoss {
		testPoss[ts], err = to.procudeMessage(ts, topic)
		c.Assert(err, IsNil)
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
		c.Log("check: ", ts)
		c.Assert(err, IsNil)
		c.Assert(offsetFounds, HasLen, 1)
		c.Assert(offsetFounds[0], Equals, offset)
	}
}

func (to *testOffsetSuite) procudeMessage(ts int64, topic string) (offset int64, err error) {
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
