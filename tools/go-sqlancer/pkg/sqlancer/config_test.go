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

package sqlancer

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSetDSN(t *testing.T) {
	conf := NewConfig()

	assert.Nil(t, conf.SetDSN("root:@tcp(127.0.0.1:4000)/"), "parse dsn no error")
	assert.Equal(t, conf.DSN, "root:@tcp(127.0.0.1:4000)/", "parse dsn")
	assert.Equal(t, conf.DBName, "test", "parse dbname")

	assert.Nil(t, conf.SetDSN("root:passwd@tcp(127.0.0.1:4000)/bank"), "parse dsn no error")
	assert.Equal(t, conf.DSN, "root:passwd@tcp(127.0.0.1:4000)/", "parse dsn")
	assert.Equal(t, conf.DBName, "bank", "parse dbname")
}
