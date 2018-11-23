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

package utils

import (
	. "github.com/pingcap/check"
	"testing"
)

func TestClient(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testUrlsSuite{})

type testUrlsSuite struct{}

func (t *testUrlsSuite) TestParseHostPortAddr(c *C) {
	urls := []string{
		"127.0.0.1:2379",
		"127.0.0.1:2379,127.0.0.2:2379",
		"localhost:2379",
		"pump-1:8250,pump-2:8250",
		"http://127.0.0.1:2379",
		"https://127.0.0.1:2379",
		"http://127.0.0.1:2379,http://127.0.0.2:2379",
		"https://127.0.0.1:2379,https://127.0.0.2:2379",
		"unix:///home/tidb/tidb.sock",
	}

	expectUrls := [][]string{
		{"127.0.0.1:2379"},
		{"127.0.0.1:2379", "127.0.0.2:2379"},
		{"localhost:2379"},
		{"pump-1:8250", "pump-2:8250"},
		{"http://127.0.0.1:2379"},
		{"https://127.0.0.1:2379"},
		{"http://127.0.0.1:2379", "http://127.0.0.2:2379"},
		{"https://127.0.0.1:2379", "https://127.0.0.2:2379"},
		{"unix:///home/tidb/tidb.sock"},
	}

	for i, url := range urls {
		urlList, err := ParseHostPortAddr(url)
		c.Assert(err, Equals, nil)
		c.Assert(len(urlList), Equals, len(expectUrls[i]))
		for j, u := range urlList {
			c.Assert(u, Equals, expectUrls[i][j])
		}
	}

	inValidUrls := []string{
		"127.0.0.1",
		"http:///127.0.0.1:2379",
		"htt://127.0.0.1:2379",
	}

	for _, url := range inValidUrls {
		_, err := ParseHostPortAddr(url)
		c.Assert(err, NotNil)
	}
}
