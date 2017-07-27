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

type FirehoseClient struct {
	Host    string
	Port    string
	Columns *[]Column
}

type ApiClient struct {
	Host string
	Port int
}

type ApiSqlResponse struct {
	Hits []struct {
		Payload struct {
			Stored string
		}
	}
}

const (
	apiTimeout time.Duration = 30 * time.Second
)

// Query sends a SQL query to remote Dashbase API client
func (client *ApiClient) Query(SqlStatement string) (*ApiSqlResponse, error) {
	param := url.Values{}
	param.Add("sql", SqlStatement)
	param.Add("timezone", "GMT")

	httpClient := http.Client{Timeout: apiTimeout}
	resp, err := httpClient.Get(fmt.Sprintf("http://%s:%d/v1/sql?%s", client.Host, client.Port, param.Encode()))
	if err != nil {
		return nil, errors.Trace(fmt.Errorf("Failed to connect Dashbase API service at %s:%d", client.Host, client.Port))
	}
	defer resp.Body.Close()

	var ret ApiSqlResponse
	err = json.NewDecoder(resp.Body).Decode(&ret)
	if err != nil {
		return nil, errors.Trace(fmt.Errorf("Failed to decode Dashbase API response data"))
	}

	return &ret, nil
}
