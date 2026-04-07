// Copyright 2026 PingCAP, Inc.
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

package config

import (
	"github.com/pingcap/tidb/br/pkg/stream/crr/internal/checkpoint"
	"github.com/pingcap/tidb/br/pkg/stream/crr/service"
	"github.com/spf13/pflag"
)

const (
	flagTaskName                = "task-name"
	flagStateStorageSubDir      = "state-storage-sub-dir"
	flagRetryInterval           = "retry-interval"
	flagCalcPollInterval        = "calc.poll-interval"
	flagCalcMetaReadConcurrency = "calc.meta-read-concurrency"
)

type Config struct {
	service.Config
	StateStorageSubDir string
}

func DefaultConfig() Config {
	return Config{
		Config: service.Config{
			CalculatorConfig: service.CalculatorConfig{
				PollInterval:        checkpoint.DefaultPollInterval,
				MetaReadConcurrency: checkpoint.DefaultMetaReadConcurrency,
			},
			RetryInterval: service.DefaultRetryInterval,
		},
	}
}

func DefineFlags(flags *pflag.FlagSet) {
	defaults := DefaultConfig()
	flags.String(flagTaskName, "", "The name of the upstream log backup task.")
	flags.String(
		flagStateStorageSubDir,
		defaults.StateStorageSubDir,
		"The relative subdirectory under upstream storage used to persist CRR resume state.",
	)
	flags.Duration(
		flagRetryInterval,
		defaults.RetryInterval,
		"The retry interval after crr-checkpoint service errors or watch failures.",
	)
	flags.Duration(
		flagCalcPollInterval,
		defaults.PollInterval,
		"The calculator polling interval for downstream sync checks.",
	)
	flags.Int(
		flagCalcMetaReadConcurrency,
		defaults.MetaReadConcurrency,
		"The calculator concurrency for reading backupmeta files.",
	)
}

func (cfg *Config) Parse(flags *pflag.FlagSet) error {
	*cfg = DefaultConfig()

	var err error
	cfg.TaskName, err = flags.GetString(flagTaskName)
	if err != nil {
		return err
	}
	cfg.StateStorageSubDir, err = flags.GetString(flagStateStorageSubDir)
	if err != nil {
		return err
	}
	cfg.RetryInterval, err = flags.GetDuration(flagRetryInterval)
	if err != nil {
		return err
	}
	cfg.PollInterval, err = flags.GetDuration(flagCalcPollInterval)
	if err != nil {
		return err
	}
	cfg.MetaReadConcurrency, err = flags.GetInt(flagCalcMetaReadConcurrency)
	if err != nil {
		return err
	}
	return nil
}
