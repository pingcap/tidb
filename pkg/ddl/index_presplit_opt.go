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

package ddl

import (
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
)

// IndexArgSplitOpt is the index pre-split option stored in DDL job args.
type IndexArgSplitOpt struct {
	Lower      []string   `json:"lower,omitempty"`
	Upper      []string   `json:"upper,omitempty"`
	Num        int64      `json:"num,omitempty"`
	ValueLists [][]string `json:"value_lists,omitempty"`
}

func buildIndexPresplitOpt(indexOpt *ast.IndexOption) (*IndexArgSplitOpt, error) {
	if indexOpt == nil {
		return nil, nil
	}
	opt := indexOpt.SplitOpt
	if opt == nil {
		return nil, nil
	}
	if len(opt.ValueLists) > 0 {
		valLists := make([][]string, 0, len(opt.ValueLists))
		for _, lst := range opt.ValueLists {
			values := make([]string, 0, len(lst))
			for _, exp := range lst {
				var sb strings.Builder
				rCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)
				if err := exp.Restore(rCtx); err != nil {
					return nil, errors.Trace(err)
				}
				values = append(values, sb.String())
			}
			valLists = append(valLists, values)
		}
		return &IndexArgSplitOpt{
			Num:        opt.Num,
			ValueLists: valLists,
		}, nil
	}

	lowers := make([]string, 0, len(opt.Lower))
	for _, expL := range opt.Lower {
		var sb strings.Builder
		rCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)
		if err := expL.Restore(rCtx); err != nil {
			return nil, errors.Trace(err)
		}
		lowers = append(lowers, sb.String())
	}
	uppers := make([]string, 0, len(opt.Upper))
	for _, expU := range opt.Upper {
		var sb strings.Builder
		rCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)
		if err := expU.Restore(rCtx); err != nil {
			return nil, errors.Trace(err)
		}
		uppers = append(uppers, sb.String())
	}
	maxSplitRegionNum := int64(config.GetGlobalConfig().SplitRegionMaxNum)
	if opt.Num > maxSplitRegionNum {
		return nil, errors.Errorf("Split index region num exceeded the limit %v", maxSplitRegionNum)
	} else if opt.Num < 1 {
		return nil, errors.Errorf("Split index region num should be greater than 0")
	}
	return &IndexArgSplitOpt{
		Lower: lowers,
		Upper: uppers,
		Num:   opt.Num,
	}, nil
}
