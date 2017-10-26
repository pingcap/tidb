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

package command

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
)

type tableInfo struct {
	Name          string       `json:"name"`
	ID            int64        `json:"id"`
	RecordRegions []regionInfo `json:"record_regions"`
}

func (t tableInfo) String() string {
	return fmt.Sprintf("tableInfo{name: %s, id: %d}", t.Name, t.ID)
}

type regionInfo struct {
	RegionID int64      `json:"region_id"`
	Leader   peerInfo   `json:"leader"`
	Peers    []peerInfo `json:"peers"`
}

type peerInfo struct {
	ID      int64 `json:"id"`
	StoreID int64 `json:"store_id"`
}

// Filter is the higher-order function to manipulate (string) collections
// TODO move to public function package
func Filter(vs []string, f func(string) bool) []string {
	vsf := make([]string, 0)
	for _, v := range vs {
		if f(v) {
			vsf = append(vsf, v)
		}
	}
	return vsf
}

// getTableInfo returns the TiDB table info given the TiDB host, the dbName and the tableName
func getTableInfo(host, dbName, tableName string, verbose bool) (*tableInfo, error) {

	if host == "" || dbName == "" || tableName == "" {
		errMsg := fmt.Sprintf("host, dbName and tableName are all required, but now\n "+
			"host: %s\n dbName: %s\n tableName: %s\n", host, dbName, tableName)
		return nil, errors.New(errMsg)
	}

	urlString := fmt.Sprintf("%s/tables/%s/%s/regions", host, dbName, tableName)

	if verbose {
		fmt.Println("the url is", urlString)
	}

	resp, err := http.Get(urlString)

	if err != nil {
		return nil, err
	}

	defer func() {
		err = resp.Body.Close()
		if err != nil {
			fmt.Println("Error when close the http response body. ", err)
		}
	}()

	content, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var ti tableInfo
	err = json.Unmarshal(content, &ti)
	if err != nil {
		errMsg := fmt.Sprintf("Error when unmarshaling the response body %s, the error is %s",
			string(content), err)
		return nil, errors.New(errMsg)
	}
	return &ti, nil

}
