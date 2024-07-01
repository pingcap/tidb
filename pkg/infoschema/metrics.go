// Copyright 2024 PingCAP, Inc.
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

package infoschema

import (
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

type sieveStatusHookImpl struct {
	evict prometheus.Counter
	hit   prometheus.Counter
	miss  prometheus.Counter
}

func newSieveStatusHookImpl() *sieveStatusHookImpl {
	return &sieveStatusHookImpl{
		evict: metrics.InfoSchemaV2CacheCounter.WithLabelValues("evict"),
		hit:   metrics.InfoSchemaV2CacheCounter.WithLabelValues("hit"),
		miss:  metrics.InfoSchemaV2CacheCounter.WithLabelValues("miss"),
	}
}

func (s *sieveStatusHookImpl) onEvict() {
	s.evict.Inc()
}

func (s *sieveStatusHookImpl) onHit() {
	s.hit.Inc()
}

func (s *sieveStatusHookImpl) onMiss() {
	s.miss.Inc()
}
