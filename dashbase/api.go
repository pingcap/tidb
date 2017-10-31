// Copyright 2017 PingCAP, Inc.
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

package dashbase

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/juju/errors"
)

type ApiClient struct {
	URL string
}

type ApiSQLResponse struct {
	Request struct {
		Aggregations map[string]struct {
			RequestType string
			Col         string
			Type        string
		}
	}
	Hits []struct {
		TimeInSeconds int64
		Payload       struct {
			Fields map[string][]string
		}
	}
	Aggregations map[string]struct {
		ResponseType string
		Value        float64
	}
	ErrorMessage string `json:"message"`
}

const (
	apiTimeout time.Duration = 30 * time.Second
)

// Query sends a SQL query to remote Dashbase API client
func (client *ApiClient) Query(SQLStatement string) (*ApiSQLResponse, error) {
	param := url.Values{}
	param.Add("sql", SQLStatement)
	param.Add("timezone", "GMT")

	httpClient := http.Client{Timeout: apiTimeout}
	resp, err := httpClient.Get(fmt.Sprintf("%s/v1/sql?%s", client.URL, param.Encode()))
	if err != nil {
		return nil, errors.Trace(fmt.Errorf("Failed to connect Dashbase API service at %s", client.URL))
	}
	defer resp.Body.Close()

	var ret ApiSQLResponse
	err = json.NewDecoder(resp.Body).Decode(&ret)
	if err != nil {
		return nil, errors.Trace(fmt.Errorf("Failed to decode Dashbase API response data"))
	}

	if len(ret.ErrorMessage) > 0 {
		return nil, errors.Trace(fmt.Errorf("Dashbase error: %s", ret.ErrorMessage))
	}

	return &ret, nil
}
