// Copyright 2022 PingCAP, Inc.
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

package task

import (
	"context"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/restore"
	"github.com/pingcap/tidb/br/pkg/summary"
	"github.com/spf13/pflag"
	"go.uber.org/zap"
)

const (
	flagResolvedTS  = "resolved-ts"
	flagNumofStores = "num-of-stores"
	flagServAddr    = "service-addr"
)

var (
	RecoveryStart  = "recovery start"
	RecoveryStatus = "recovery status"
)

var RecoveryFuncMap = map[string]func(c context.Context, cmdName string, cfg *RecoveryConfig) error{
	RecoveryStart:  RunRecoveryStart,
	RecoveryStatus: RunRecoveryStatus,
}

// RecoveryConfig specifies the configure about recovery service
type RecoveryConfig struct {
	Config

	// resolved-ts is tso all TiKVs shall aligned
	ResolvedTs string `json:"resolved-ts" toml:"resolved-ts"`

	// Spec for num of tikv to recovery
	NumOfStores         int    `json:"num-of-stores" toml:"num-of-stores"`
	RecoveryServiceAddr string `json:"service-addr" toml:"service-addr"`
}

// DefineRecoveryStartFlags defines flags used for `recovery start`
func DefineRecoveryStartFlags(flags *pflag.FlagSet) {
	flags.String(flagServAddr, "",
		"a host service for recoverying the store.")
	flags.Int(flagNumofStores, 3, "num of stores to be recoveried by BR")
	flags.String(flagResolvedTS, "",
		"a resolved ts for data consistency amoung tikvs, used for recovery tikv, bring all tikv in same data consistency.\n"+
			"this is TSO, e.g. '400036290571534337'.")
}

func DefineRecoveryStatusCommonFlags(flags *pflag.FlagSet) {
	//ToDo
}

func (cfg *RecoveryConfig) ParseRecoveryStartFromFlags(flags *pflag.FlagSet) error {
	var err error
	cfg.ResolvedTs, err = flags.GetString(flagResolvedTS)
	if err != nil {
		return errors.Trace(err)
	}

	cfg.NumOfStores, err = flags.GetInt(flagNumofStores)
	if err != nil {
		return errors.Trace(err)
	}

	cfg.RecoveryServiceAddr, err = flags.GetString(flagServAddr)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (cfg *RecoveryConfig) ParseRecoveryStatusFromFlags(flags *pflag.FlagSet) error {

	return nil
}

func (cfg *RecoveryConfig) ParseRecoveryCommonFromFlags(flags *pflag.FlagSet) error {
	return nil
}

// RunRecoveryCommand run a recovery services
func RunRecoveryCommand(
	ctx context.Context,
	cmdName string,
	cfg *RecoveryConfig,
) error {
	defer func() {
		if _, ok := skipSummaryCommandList[cmdName]; !ok {
			summary.Summary(cmdName)
		}
	}()
	commandFn, exist := RecoveryFuncMap[cmdName]
	if !exist {
		return errors.Annotatef(berrors.ErrInvalidArgument, "invalid command %s", cmdName)
	}

	if err := commandFn(ctx, cmdName, cfg); err != nil {
		log.Error("failed to recovery", zap.String("command", cmdName), zap.Error(err))
		summary.SetSuccessStatus(false)
		summary.CollectFailureUnit(cmdName, err)
		return err
	}
	summary.SetSuccessStatus(true)
	return nil
}

// RunRecoveryStart specifies starting a recovery task
func RunRecoveryStart(
	c context.Context,
	cmdName string,
	cfg *RecoveryConfig,
) error {
	ctx, cancelFn := context.WithCancel(c)
	defer cancelFn()
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("task.RunRecoveryStart", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}
	// read pd info to get tikv address

	// read each tikv region meta info

	// command tikv to recovery the regions

	// send resolved data to each tikv, require them to delete data from specific tso

	err := restore.StartService(c, cfg.NumOfStores, cfg.RecoveryServiceAddr)
	if err != nil {
		log.Error("failed to recovery", zap.String("command", cmdName), zap.Error(err))
		summary.SetSuccessStatus(false)
		summary.CollectFailureUnit(cmdName, err)
		return err
	}

	return nil
}

// RunRecoveryStatus get status for a specific recovery task
func RunRecoveryStatus(
	c context.Context,
	cmdName string,
	cfg *RecoveryConfig,
) error {
	ctx, cancelFn := context.WithCancel(c)
	defer cancelFn()

	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan(
			"task.RunRecoveryStatus",
			opentracing.ChildOf(span.Context()),
		)
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	// check current service status
	return nil
}
