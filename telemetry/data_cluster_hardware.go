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

package telemetry

import (
	"regexp"
	"sort"
	"strings"

	"github.com/iancoleman/strcase"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/sqlexec"
)

var (
	// Explicitly list all allowed field names to avoid potential leaking sensitive info when new fields are added later.
	sortedCPUAllowedFieldNames    = []string{"cpu-logical-cores", "cpu-physical-cores", "cpu-frequency", "cache", "cpu-vendor-id", "l1-cache-size", "l1-cache-line-size", "l2-cache-size", "l2-cache-line-size", "l3-cache-size", "l3-cache-line-size"}
	sortedDiskAllowedFieldNames   = []string{"fstype", "opts", "total", "free", "used", "free-percent", "used-percent"} // path is not included
	regexDiskAllowedNames         = regexp.MustCompile(`^(disk\d+s\d+|rootfs|devtmpfs|sd[a-z]\d*|vd[a-z]\d*|hd[a-z]\d*|nvme\d+(n\d+(p\d)?)?|md[\da-z]+)$`)
	sortedDiskAllowedPaths        = []string{"/", "/boot", "/dev", "/private/var/vm", "/System/Volumes/Data"}
	sortedMemoryAllowedFieldNames = []string{"capacity"}
)

func init() {
	sort.Strings(sortedCPUAllowedFieldNames)
	sort.Strings(sortedDiskAllowedFieldNames)
	sort.Strings(sortedDiskAllowedPaths)
	sort.Strings(sortedMemoryAllowedFieldNames)
}

type clusterHardwareItem struct {
	InstanceType   string                       `json:"instanceType"`
	ListenHostHash string                       `json:"listenHostHash"`
	ListenPort     string                       `json:"listenPort"`
	CPU            map[string]string            `json:"cpu,omitempty"`
	Memory         map[string]string            `json:"memory,omitempty"`
	Disk           map[string]map[string]string `json:"disk,omitempty"`
}

func normalizeDiskName(name string) string {
	const prefix = "/dev/"
	if strings.HasPrefix(name, prefix) {
		return name[len(prefix):]
	}
	return name
}

func isNormalizedDiskNameAllowed(name string) bool {
	return regexDiskAllowedNames.MatchString(name)
}

func normalizeFieldName(name string) string {
	return strcase.ToLowerCamel(name)
}

func getClusterHardware(ctx sessionctx.Context) ([]*clusterHardwareItem, error) {
	rows, _, err := ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(`SELECT TYPE, INSTANCE, DEVICE_TYPE, DEVICE_NAME, NAME, VALUE FROM information_schema.cluster_hardware`)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// WARNING: The key of this variable is not hashed, it must not be returned directly.
	itemsByInstance := make(map[string]*clusterHardwareItem)
L:
	for _, row := range rows {
		if row.Len() < 6 {
			continue L
		}
		instance := row.GetString(1)
		activeItem, ok := itemsByInstance[instance]
		if !ok {
			hostHash, port := parseAddressAndHash(instance)
			activeItem = &clusterHardwareItem{
				InstanceType:   row.GetString(0),
				ListenHostHash: hostHash,
				ListenPort:     port,
				CPU:            make(map[string]string),
				Memory:         make(map[string]string),
				Disk:           make(map[string]map[string]string),
			}
			itemsByInstance[instance] = activeItem
		}

		deviceType := row.GetString(2)
		deviceName := row.GetString(3)
		fieldName := row.GetString(4)
		fieldValue := row.GetString(5)

		switch deviceType {
		case "cpu":
			if deviceName != "cpu" {
				continue L
			}
			if !sortedStringContains(sortedCPUAllowedFieldNames, fieldName) {
				continue L
			}
			activeItem.CPU[normalizeFieldName(fieldName)] = fieldValue
		case "memory":
			if deviceName != "memory" {
				continue L
			}
			if !sortedStringContains(sortedMemoryAllowedFieldNames, fieldName) {
				continue L
			}
			activeItem.Memory[normalizeFieldName(fieldName)] = fieldValue
		case "disk":
			var hashedDeviceName string
			normalizedDiskName := normalizeDiskName(deviceName)
			if isNormalizedDiskNameAllowed(normalizedDiskName) {
				// Use plain text only when it is in a list that we know safe.
				hashedDeviceName = normalizedDiskName
			} else {
				hashedDeviceName = hashString(normalizedDiskName)
			}
			activeDiskItem, ok := activeItem.Disk[hashedDeviceName]
			if !ok {
				activeDiskItem = make(map[string]string)
				activeDiskItem[normalizeFieldName("deviceName")] = hashedDeviceName
				activeItem.Disk[hashedDeviceName] = activeDiskItem
			}
			if fieldName == "path" {
				var path string
				if sortedStringContains(sortedDiskAllowedPaths, fieldValue) {
					// Use plain text only when it is in a list that we know safe.
					path = fieldValue
				} else {
					path = hashString(fieldValue)
				}
				activeDiskItem[normalizeFieldName("path")] = path
			} else if sortedStringContains(sortedDiskAllowedFieldNames, fieldName) {
				activeDiskItem[normalizeFieldName(fieldName)] = fieldValue
			}
		}
	}

	r := make([]*clusterHardwareItem, 0, len(itemsByInstance))
	for _, item := range itemsByInstance {
		r = append(r, item)
	}

	// TODO: Go's map has random order. Better to generate a fixed-order result.

	return r, nil
}
