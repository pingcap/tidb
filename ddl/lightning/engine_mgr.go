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
package lightning

import (
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"go.uber.org/zap"
)

type EngineManager struct {
	enginePool map[string]*engineInfo
}

func (em *EngineManager) init() {
	em.enginePool = make(map[string]*engineInfo, 10)
}

func (em *EngineManager) StoreEngineInfo(key string, ei *engineInfo) {
	em.enginePool[key] = ei
}

func (em *EngineManager) LoadEngineInfo(key string) (*engineInfo, bool) {
	ei, exist := em.enginePool[key]
	if !exist {
		log.L().Error(LERR_GET_ENGINE_FAILED, zap.String("Engine_Manager:", "Not found"),
		zap.Stack("stack trace"))
		return nil, exist
	}
	return ei, exist
}

func (em *EngineManager) ReleaseEngine(key string) {
	log.L().Info(LINFO_ENGINE_DELETE, zap.String("Engine info key:", key),
				zap.Stack("stack"))
	delete(em.enginePool, key)
	return
}

// Caculate all memory used by all active engine.
func (em *EngineManager) totalSize() int64 {
	var memUsed int64
	for _, en := range em.enginePool {
		memUsed += en.openedEngine.TotalMemoryConsume()
	}
	return memUsed
}
