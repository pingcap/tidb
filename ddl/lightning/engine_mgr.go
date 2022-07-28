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

// EngineManager is a manager of engine.
type EngineManager struct {
	enginePool map[string]*engineInfo
}

func (em *EngineManager) init() {
	em.enginePool = make(map[string]*engineInfo, 10)
}

// StoreEngineInfo store one engineInfo into Manager.
func (em *EngineManager) StoreEngineInfo(key string, ei *engineInfo) {
	em.enginePool[key] = ei
}

// LoadEngineInfo load one engineInfo from Manager.
func (em *EngineManager) LoadEngineInfo(key string) (*engineInfo, bool) {
	ei, exist := em.enginePool[key]
	if !exist {
		log.L().Error(LitErrGetEngineFail, zap.String("Engine_Manager:", "Not found"))
		return nil, exist
	}
	return ei, exist
}

// ReleaseEngine delete an engineInfo from Mangager.
func (em *EngineManager) ReleaseEngine(key string) {
	log.L().Info(LitInfoEngineDelete, zap.String("Engine info key:", key))
	delete(em.enginePool, key)
}
