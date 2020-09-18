// Copyright 2020 PingCAP, Inc.
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

package placement

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"path"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/util/pdapi"
)

func checkLabelConstraint(label string) (LabelConstraint, error) {
	r := LabelConstraint{}

	if len(label) < 4 {
		return r, errors.Errorf("label constraint should be in format '{+|-}key=value', but got '%s'", label)
	}

	var op LabelConstraintOp
	switch label[0] {
	case '+':
		op = In
	case '-':
		op = NotIn
	default:
		return r, errors.Errorf("label constraint should be in format '{+|-}key=value', but got '%s'", label)
	}

	kv := strings.Split(label[1:], "=")
	if len(kv) != 2 {
		return r, errors.Errorf("label constraint should be in format '{+|-}key=value', but got '%s'", label)
	}

	key := strings.TrimSpace(kv[0])
	if key == "" {
		return r, errors.Errorf("label constraint should be in format '{+|-}key=value', but got '%s'", label)
	}

	val := strings.TrimSpace(kv[1])
	if val == "" {
		return r, errors.Errorf("label constraint should be in format '{+|-}key=value', but got '%s'", label)
	}

	r.Key = key
	r.Op = op
	r.Values = []string{val}
	return r, nil
}

// CheckLabelConstraints will check labels, and build LabelConstraints for rule.
func CheckLabelConstraints(labels []string) ([]LabelConstraint, error) {
	constraints := make([]LabelConstraint, 0, len(labels))
	for _, str := range labels {
		label, err := checkLabelConstraint(strings.TrimSpace(str))
		if err != nil {
			return constraints, err
		}
		constraints = append(constraints, label)
	}
	return constraints, nil
}

func GroupID(id int64) string {
	return fmt.Sprintf("TIDB_DDL_%d", id)
}

func doRequest(ctx context.Context, addrs []string, route, method string, body io.Reader) ([]byte, error) {
	var err error
	var req *http.Request
	var res *http.Response
	for _, addr := range addrs {
		var url string
		if strings.HasPrefix(addr, "http://") {
			url = fmt.Sprintf("%s%s", addr, route)
		} else {
			url = fmt.Sprintf("http://%s%s", addr, route)
		}

		if ctx != nil {
			req, err = http.NewRequestWithContext(ctx, method, url, body)
		} else {
			req, err = http.NewRequest(method, url, body)
		}
		if err != nil {
			return nil, err
		}
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}

		res, err = http.DefaultClient.Do(req)
		if err == nil {
			bodyBytes, err := ioutil.ReadAll(res.Body)
			terror.Log(err)
			if res.StatusCode != http.StatusOK {
				err = errors.Errorf("%s", bodyBytes)
			}
			terror.Log(res.Body.Close())
			return bodyBytes, err
		}
	}
	return nil, err
}

func GetAllBundles(ctx context.Context, addrs []string) (*Bundles, error) {
	bundles := &Bundles{}
	if len(addrs) == 0 {
		return bundles, nil
	}

	res, err := doRequest(ctx, addrs, path.Join(pdapi.Config, "placement-rule"), "GET", nil)
	if err == nil {
		err = json.Unmarshal(res, bundles)
	}
	return bundles, err
}
