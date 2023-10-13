// Copyright 2023 PingCAP, Inc.
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

package importer

import (
	"math/rand"
	"testing"
	"time"

	"github.com/pingcap/tidb/br/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestIndexRouteWriter(t *testing.T) {
	logger := zap.NewExample()
	routeWriter := NewIndexRouteWriter(logger, func(i int64) *external.Writer {
		return external.NewWriterBuilder().Build(nil, "", "")
	})
	wg := util.WaitGroupWrapper{}
	for i := 0; i < 10; i++ {
		idx := i
		wg.Run(func() {
			seed := time.Now().Unix()
			logger.Info("seed", zap.Int("idx", idx), zap.Int64("seed", seed))
			r := rand.New(rand.NewSource(seed))
			gotWriters := make(map[int64]*wrappedWriter)
			for i := 0; i < 3000; i++ {
				indexID := int64(r.Int()) % 100
				writer := routeWriter.getWriter(indexID)
				require.NotNil(t, writer)
				if got, ok := gotWriters[indexID]; ok {
					require.Equal(t, got, writer)
				} else {
					gotWriters[indexID] = writer
				}
			}
		})
	}
	wg.Wait()
}
