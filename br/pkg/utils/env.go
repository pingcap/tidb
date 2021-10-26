// Copyright 2021 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"golang.org/x/net/http/httpproxy"
)

// LogEnvVariables logs related environment variables.
func LogEnvVariables() {
	// log http proxy settings, it will be used in gRPC connection by default
	fields := proxyFields()
	if len(fields) > 0 {
		log.Info("using proxy config", fields...)
	}
}

func proxyFields() []zap.Field {
	proxyCfg := httpproxy.FromEnvironment()
	fields := make([]zap.Field, 0, 3)
	if proxyCfg.HTTPProxy != "" {
		fields = append(fields, zap.String("http_proxy", proxyCfg.HTTPProxy))
	}
	if proxyCfg.HTTPSProxy != "" {
		fields = append(fields, zap.String("https_proxy", proxyCfg.HTTPSProxy))
	}
	if proxyCfg.NoProxy != "" {
		fields = append(fields, zap.String("no_proxy", proxyCfg.NoProxy))
	}
	return fields
}
