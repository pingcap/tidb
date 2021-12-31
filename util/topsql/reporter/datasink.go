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

package reporter

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/errors"
	topsqlstate "github.com/pingcap/tidb/util/topsql/state"
	"github.com/pingcap/tipb/go-tipb"
)

// DataSink collects and sends data to a target.
type DataSink interface {
	// TrySend pushes a report data into the sink, which will later be sent to a target by the sink. A deadline can be
	// specified to control how late it should be sent. If the sink is kept full and cannot schedule a send within
	// the specified deadline, or the sink is closed, an error will be returned.
	TrySend(data *ReportData, deadline time.Time) error

	// OnReporterClosing notifies DataSink that the reporter is closing.
	OnReporterClosing()
}

// DataSinkRegisterer is for registering DataSink
type DataSinkRegisterer interface {
	Register(dataSink DataSink) error
	Deregister(dataSink DataSink)
}

// ReportData contains data that reporter sends to the agent.
type ReportData struct {
	// DataRecords contains the topN records of each second and the `others`
	// record which aggregation all []tipb.TopSQLRecord that is out of Top N.
	DataRecords []tipb.TopSQLRecord
	SQLMetas    []tipb.SQLMeta
	PlanMetas   []tipb.PlanMeta
}

func (d *ReportData) hasData() bool {
	return len(d.DataRecords) != 0 || len(d.SQLMetas) != 0 || len(d.PlanMetas) != 0
}

var _ DataSinkRegisterer = &DefaultDataSinkRegisterer{}

// DefaultDataSinkRegisterer implements DataSinkRegisterer.
type DefaultDataSinkRegisterer struct {
	sync.Mutex
	ctx       context.Context
	dataSinks map[DataSink]struct{}
}

// NewDefaultDataSinkRegisterer creates a new DefaultDataSinkRegisterer which implements DataSinkRegisterer.
func NewDefaultDataSinkRegisterer(ctx context.Context) DefaultDataSinkRegisterer {
	return DefaultDataSinkRegisterer{
		ctx:       ctx,
		dataSinks: make(map[DataSink]struct{}, 10),
	}
}

// Register implements DataSinkRegisterer.
func (r *DefaultDataSinkRegisterer) Register(dataSink DataSink) error {
	r.Lock()
	defer r.Unlock()

	select {
	case <-r.ctx.Done():
		return errors.New("DefaultDataSinkRegisterer closed")
	default:
		if len(r.dataSinks) >= 10 {
			return errors.New("too many datasinks")
		}
		r.dataSinks[dataSink] = struct{}{}
		if len(r.dataSinks) > 0 {
			topsqlstate.EnableTopSQL()
		}
		return nil
	}
}

// Deregister implements DataSinkRegisterer.
func (r *DefaultDataSinkRegisterer) Deregister(dataSink DataSink) {
	r.Lock()
	defer r.Unlock()

	select {
	case <-r.ctx.Done():
	default:
		delete(r.dataSinks, dataSink)
		if len(r.dataSinks) == 0 {
			topsqlstate.DisableTopSQL()
		}
	}
}
