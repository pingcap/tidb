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
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/juju/errors"
)

type FirehoseClient struct {
	Host string
	Port int
}

type firehoseSchemaItem struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type FirehoseInsertResponse struct {
	IsError         bool
	EventsProcessed int
	ErrorMessage    string `json:"message"`
}

const (
	firehoseTimeout time.Duration = 30 * time.Second
)

// Insert inserts multiple rows into Dashbase
func (client *FirehoseClient) Insert(payload []map[string]interface{}, columns []*Column) (*FirehoseInsertResponse, error) {
	schema := make([]firehoseSchemaItem, len(columns))
	for i, column := range columns {
		schema[i] = firehoseSchemaItem{
			Name: column.Name,
			Type: string(column.LowType),
		}
	}

	events := make([]map[string]interface{}, len(payload))
	for i, row := range payload {
		event := make(map[string]interface{})
		event["_schema"] = schema
		for k, v := range row {
			event[k] = v
		}
		events[i] = event
	}

	postJSONBody, err := json.Marshal(map[string]interface{}{"eventList": events})
	if err != nil {
		panic("Unexpected Json serialize error")
	}

	fmt.Printf("Insert JSON: %s\n", string(postJSONBody))

	req, err := http.NewRequest(
		"POST",
		fmt.Sprintf("http://%s:%d/v1/firehose/http/insert", client.Host, client.Port),
		bytes.NewBuffer(postJSONBody))
	req.Header.Set("Content-Type", "application/json")
	if err != nil {
		return nil, errors.Trace(fmt.Errorf("Failed to connect Dashbase Firehose service at %s:%d", client.Host, client.Port))
	}

	httpClient := http.Client{Timeout: firehoseTimeout}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, errors.Trace(fmt.Errorf("Failed to connect Dashbase Firehose service at %s:%d", client.Host, client.Port))
	}
	defer resp.Body.Close()

	var ret FirehoseInsertResponse
	err = json.NewDecoder(resp.Body).Decode(&ret)
	if err != nil {
		return nil, errors.Trace(fmt.Errorf("Failed to decode Dashbase Firehose response data"))
	}

	if ret.IsError {
		return nil, errors.Trace(fmt.Errorf("Dashbase error: Unknown"))
	}

	if len(ret.ErrorMessage) > 0 {
		return nil, errors.Trace(fmt.Errorf("Dashbase error: %s", ret.ErrorMessage))
	}

	return &ret, nil
}

type ApiClient struct {
	Host string
	Port int
}

type ApiSQLResponse struct {
	Hits []struct {
		Payload struct {
			Stored string
		}
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
	resp, err := httpClient.Get(fmt.Sprintf("http://%s:%d/v1/sql?%s", client.Host, client.Port, param.Encode()))
	if err != nil {
		return nil, errors.Trace(fmt.Errorf("Failed to connect Dashbase API service at %s:%d", client.Host, client.Port))
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
