package task

import (
	"context"
	"fmt"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/pkg/kv"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

func setConfig(ctx context.Context, g glue.Glue, storage kv.Storage, key string, value any) error {
	se, err := g.CreateSession(storage)
	if err != nil {
		return errors.Trace(err)
	}
	defer se.Close()

	execCtx := se.GetSessionCtx().GetRestrictedSQLExecutor()

	_, _, err = execCtx.ExecRestrictedSQL(
		kv.WithInternalSourceType(ctx, kv.InternalTxnBR),
		nil,
		fmt.Sprintf("set config tikv `%s`=%%?", key),
		value,
	)
	if err != nil {
		return errors.Annotatef(err, "failed to set config `%s`=%v", key, value)
	}
	log.Info("Set config value in all tikv nodes", zap.String(key, fmt.Sprintf("%v", value)))
	return nil
}

func setConfigs(ctx context.Context, g glue.Glue, storage kv.Storage, values map[string]any) error {
	var group errgroup.Group
	for key, value := range values {
		keySafe := key
		valueSafe := value
		group.Go(func() error {
			return setConfig(ctx, g, storage, keySafe, valueSafe)
		})
	}
	return group.Wait()
}

func ensureTiKVConfigFromFile(ctx context.Context, g glue.Glue, storage kv.Storage, path string) error {
	if len(path) <= 0 {
		return nil
	}
	log.Info("About to dynamically configure TiKV with config values from file.", zap.String("path", path))
	configValues := make(map[string]any)
	if _, err := toml.DecodeFile(path, &configValues); err != nil {
		return errors.Trace(err)
	}
	return setConfigs(ctx, g, storage, convertToPlainKeys(configValues))
}

func convertToPlainKeys(values map[string]any) map[string]any {
	result := make(map[string]any, len(values))
	for key, value := range values {
		if innerMap, ok := value.(map[string]any); ok {
			convertedInnerMap := convertToPlainKeys(innerMap)
			for innerKey, innerValue := range convertedInnerMap {
				result[key+"."+innerKey] = innerValue
			}
		} else {
			result[key] = value
		}
	}
	return result
}
