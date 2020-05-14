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

package profile

import (
	"bytes"
	"io"
	"io/ioutil"
	"runtime/pprof"
	"strconv"
	"strings"
	"time"

	"github.com/google/pprof/profile"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/texttree"
)

// CPUProfileInterval represents the duration of sampling CPU
var CPUProfileInterval = 30 * time.Second

// Collector is used to collect the profile results
type Collector struct{}

// ProfileReaderToDatums reads data from reader and returns the flamegraph which is organized by tree form.
func (c *Collector) ProfileReaderToDatums(f io.Reader) ([][]types.Datum, error) {
	p, err := profile.Parse(f)
	if err != nil {
		return nil, err
	}
	return c.profileToDatums(p)
}

func (c *Collector) profileToFlamegraphNode(p *profile.Profile) (*flamegraphNode, error) {
	err := p.CheckValid()
	if err != nil {
		return nil, err
	}

	root := newFlamegraphNode()
	for _, sample := range p.Sample {
		root.add(sample)
	}
	return root, nil
}

func (c *Collector) profileToDatums(p *profile.Profile) ([][]types.Datum, error) {
	root, err := c.profileToFlamegraphNode(p)
	if err != nil {
		return nil, err
	}
	col := newFlamegraphCollector(p)
	col.collect(root)
	return col.rows, nil
}

// cpuProfileGraph returns the CPU profile flamegraph which is organized by tree form
func (c *Collector) cpuProfileGraph() ([][]types.Datum, error) {
	buffer := &bytes.Buffer{}
	if err := pprof.StartCPUProfile(buffer); err != nil {
		panic(err)
	}
	time.Sleep(CPUProfileInterval)
	pprof.StopCPUProfile()
	return c.ProfileReaderToDatums(buffer)
}

// ProfileGraph returns the CPU/memory/mutex/allocs/block profile flamegraph which is organized by tree form
func (c *Collector) ProfileGraph(name string) ([][]types.Datum, error) {
	if strings.ToLower(strings.TrimSpace(name)) == "cpu" {
		return c.cpuProfileGraph()
	}

	p := pprof.Lookup(name)
	if p == nil {
		return nil, errors.Errorf("cannot retrieve %s profile", name)
	}
	debug := 0
	if name == "goroutine" {
		debug = 2
	}
	buffer := &bytes.Buffer{}
	if err := p.WriteTo(buffer, debug); err != nil {
		return nil, err
	}
	if name == "goroutine" {
		return c.ParseGoroutines(buffer)
	}
	return c.ProfileReaderToDatums(buffer)
}

// ParseGoroutines returns the groutine list for given string representation
func (c *Collector) ParseGoroutines(reader io.Reader) ([][]types.Datum, error) {
	content, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	goroutines := strings.Split(string(content), "\n\n")
	var rows [][]types.Datum
	for _, goroutine := range goroutines {
		colIndex := strings.Index(goroutine, ":")
		if colIndex < 0 {
			return nil, errors.New("goroutine incompatible with current go version")
		}

		headers := strings.SplitN(strings.TrimSpace(goroutine[len("goroutine")+1:colIndex]), " ", 2)
		if len(headers) != 2 {
			return nil, errors.Errorf("incompatible goroutine headers: %s", goroutine[len("goroutine")+1:colIndex])
		}
		id, err := strconv.Atoi(strings.TrimSpace(headers[0]))
		if err != nil {
			return nil, errors.Annotatef(err, "invalid goroutine id: %s", headers[0])
		}
		state := strings.Trim(headers[1], "[]")
		stack := strings.Split(strings.TrimSpace(goroutine[colIndex+1:]), "\n")
		for i := 0; i < len(stack)/2; i++ {
			fn := stack[i*2]
			loc := stack[i*2+1]
			var identifier string
			if i == 0 {
				identifier = fn
			} else if i == len(stack)/2-1 {
				identifier = string(texttree.TreeLastNode) + string(texttree.TreeNodeIdentifier) + fn
			} else {
				identifier = string(texttree.TreeMiddleNode) + string(texttree.TreeNodeIdentifier) + fn
			}
			rows = append(rows, types.MakeDatums(
				identifier,
				id,
				state,
				strings.TrimSpace(loc),
			))
		}
	}
	return rows, nil
}

// getFuncMemUsage get function memory usage from heap profile
func (c *Collector) getFuncMemUsage(name string) (int64, error) {
	prof := pprof.Lookup("heap")
	if prof == nil {
		return 0, errors.Errorf("cannot retrieve %s profile", name)
	}
	debug := 0
	buffer := &bytes.Buffer{}
	if err := prof.WriteTo(buffer, debug); err != nil {
		return 0, err
	}
	p, err := profile.Parse(buffer)
	if err != nil {
		return 0, err
	}
	root, err := c.profileToFlamegraphNode(p)
	if err != nil {
		return 0, err
	}
	return root.collectFuncUsage(name), nil
}
