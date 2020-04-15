// Copyright 2019 PingCAP, Inc.
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

package infoschema_test

import (
	"strings"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/v4/infoschema"
	"github.com/pingcap/tidb/v4/util/set"
)

type metricSchemaSuite struct{}

var _ = Suite(&metricSchemaSuite{})

func (s *metricSchemaSuite) SetUpSuite(c *C) {
}

func (s *metricSchemaSuite) TearDownSuite(c *C) {
}

func (s *metricSchemaSuite) TestMetricSchemaDef(c *C) {
	for name, def := range infoschema.MetricTableMap {
		if strings.Contains(def.PromQL, "$QUANTILE") || strings.Contains(def.PromQL, "histogram_quantile") {
			c.Assert(def.Quantile > 0, IsTrue, Commentf("the quantile of metric table %v should > 0", name))
		} else {
			c.Assert(def.Quantile == 0, IsTrue, Commentf("metric table %v has quantile, but doesn't contain $QUANTILE in promQL ", name))
		}
		if strings.Contains(def.PromQL, "$LABEL_CONDITIONS") {
			c.Assert(len(def.Labels) > 0, IsTrue, Commentf("the labels of metric table %v should not be nil", name))
		} else {
			li := strings.Index(def.PromQL, "{")
			ri := strings.Index(def.PromQL, "}")
			// ri - li > 1 means already has label conditions, so no need $LABEL_CONDITIONS any more.
			if !(ri-li > 1) {
				c.Assert(len(def.Labels) == 0, IsTrue, Commentf("metric table %v has labels, but doesn't contain $LABEL_CONDITIONS in promQL", name))
			}
		}

		if strings.Contains(def.PromQL, " by (") {
			for _, label := range def.Labels {
				c.Assert(strings.Contains(def.PromQL, label), IsTrue, Commentf("metric table %v has labels, but doesn't contain label %v in promQL", name, label))
			}
		}
		if name != strings.ToLower(name) {
			c.Assert(name, Equals, strings.ToLower(name), Commentf("metric table name %v should be lower case", name))
		}
		// INSTANCE must be the first label
		if set.NewStringSet(def.Labels...).Exist("instance") {
			c.Assert(def.Labels[0], Equals, "instance", Commentf("metrics table %v: expect `instance`is the first label but got %v", name, def.Labels))
		}
	}
}
