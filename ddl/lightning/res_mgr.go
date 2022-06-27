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
	"context"
	"errors"
	"strconv"
	"sync"
	"unsafe"

	"github.com/docker/go-units"
	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	lcom "github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
	"go.uber.org/zap"
)

type defaultType string

const (
	// Default struct need to be count.
	ALLOC_BACKEND_CONTEXT defaultType = "AllocBackendContext"
	ALLOC_ENGINE_INFO     defaultType = "AllocEngineInfo"
	ALLOC_WORKER_CONTEXT  defaultType = "AllocWorkerCONTEXT"

	// Used to mark the object size did not stored in map
	firstAlloc  int64 = -1
	allocFailed int64 = 0
)

// MemoryRoot is used to trace the memory usage of all light DDL environment.
type LightningMemoryRoot struct {
	maxLimit     int64
	currUsage    int64
	engineUsage  int64
	writeBuffer  int64
	backendCache map[string]*BackendContext
	EngineMgr    EngineManager
	// This map is use to store all object memory allocated size.
	structSize map[string]int64
	mLock      sync.Mutex
}

func (m *LightningMemoryRoot) init(maxMemUsage int64) {
	// Set lightning memory quota to 2 times flush_size
	if maxMemUsage < flush_size {
		m.maxLimit = flush_size
	} else {
		m.maxLimit = maxMemUsage
	}

	m.currUsage = 0
	m.engineUsage = 0
	m.writeBuffer = 0

	m.backendCache = make(map[string]*BackendContext, 10)
	m.EngineMgr.init()
	m.structSize = make(map[string]int64, 10)
	m.initDefaultStruceMemSize()
}

// Caculate memory struct size and save it into map.
func (m *LightningMemoryRoot) initDefaultStruceMemSize() {
	var (
		bc   BackendContext
		ei   engineInfo
		wCtx WorkerContext
	)

	m.structSize[string(ALLOC_BACKEND_CONTEXT)] = int64(unsafe.Sizeof(bc))
	m.structSize[string(ALLOC_ENGINE_INFO)] = int64(unsafe.Sizeof(ei))
	m.structSize[string(ALLOC_WORKER_CONTEXT)] = int64(unsafe.Sizeof(wCtx))
}

// Reset memory quota. but not less than flush_size(1 MB)
func (m *LightningMemoryRoot) Reset(maxMemUsage int64) {
	m.mLock.Lock()
	defer func() {
		m.mLock.Unlock()
	}()
	// Set lightning memory quota to flush_size
	if maxMemUsage < flush_size {
		m.maxLimit = flush_size
	} else {
		m.maxLimit = maxMemUsage
	}
}

// Check Memory allocated for lightning.
func (m *LightningMemoryRoot) checkMemoryUsage(t defaultType) error {
	var (
		requiredMem int64 = 0
	)

	switch t {
	case ALLOC_BACKEND_CONTEXT:
		requiredMem, _ = m.structSize[string(ALLOC_BACKEND_CONTEXT)]
	case ALLOC_ENGINE_INFO:
		requiredMem, _ = m.structSize[string(ALLOC_ENGINE_INFO)]
	case ALLOC_WORKER_CONTEXT:
		requiredMem, _ = m.structSize[string(ALLOC_WORKER_CONTEXT)]
	default:
		return errors.New(LERR_UNKNOW_MEM_TYPE)
	}

	if m.currUsage + requiredMem > m.maxLimit {
		return errors.New(LERR_OUT_OF_MAX_MEM)
	}
	return nil
}

// check and create one backend
func (m *LightningMemoryRoot) RegistBackendContext(ctx context.Context, unique bool, key string, sqlMode mysql.SQLMode) error {
	var (
		err   error = nil
		bd    backend.Backend
		exist bool = false
		cfg   *config.Config
	)
	m.mLock.Lock()
	defer func() {
		m.mLock.Unlock()
	}()
	// Firstly, get backend Context from backend cache.
	_, exist = m.backendCache[key]
	// If bc not exist, build new backend for reorg task, otherwise reuse exist backend
	// to continue the task.
	if exist == false {
		// First to check the memory usage
		m.totalMemoryConsume()
		err = m.checkMemoryUsage(ALLOC_BACKEND_CONTEXT)
		if err != nil {
			log.L().Warn(LERR_ALLOC_MEM_FAILED, zap.String("backend key", key),
				zap.String("Current Memory Usage:", strconv.FormatInt(m.currUsage, 10)),
				zap.String("Memory limitation:", strconv.FormatInt(m.maxLimit, 10)))
			return err
		}
		cfg, err = generateLightningConfig(ctx, unique, key)
		if err != nil {
			log.L().Warn(LERR_ALLOC_MEM_FAILED, zap.String("backend key", key),
				zap.String("Generate config for lightning error:", err.Error()))
			return err
		}
		glue := glue_lit{}
		bd, err = createLocalBackend(ctx, cfg, glue)
		if err != nil {
			log.L().Error(LERR_CREATE_BACKEND_FAILED, zap.String("backend key", key),
		                zap.String("Error", err.Error()), zap.Stack("stack trace"))
			return err
		}

		// Init important variables
		sysVars := obtainImportantVariables()

		m.backendCache[key] = newBackendContext(key, &bd, ctx, cfg, sysVars)

		// Count memory usage.
		m.currUsage += m.structSize[string(ALLOC_BACKEND_CONTEXT)]
		log.L().Info(LINFO_CREATE_BACKEND, zap.String("backend key", key),
			zap.String("Current Memory Usage:", strconv.FormatInt(m.currUsage, 10)),
			zap.String("Memory limitation:", strconv.FormatInt(m.maxLimit, 10)),
			zap.String("Unique Index:", strconv.FormatBool(unique)))
	}
	return err
}

// Uniform entry to close backend and release related memory allocated
func (m *LightningMemoryRoot) DeleteBackendContext(bcKey string) {
	// Only acquire/release lock here.
	m.mLock.Lock()
	defer func() {
		delete(m.backendCache, bcKey)
		m.mLock.Unlock()
	}()
	// Close key specific backend
	bc, exist := m.backendCache[bcKey]
	if !exist {
		log.L().Error(LERR_GET_BACKEND_FAILED, zap.String("backend key", bcKey))
		return
	}

	// Close and delete backend by key
	m.deleteBackendEngines(bcKey)
	bc.Backend.Close()

	m.currUsage -= m.structSize[bc.Key]
	delete(m.structSize, bcKey)
	m.currUsage -= m.structSize[string(ALLOC_BACKEND_CONTEXT)]
	log.L().Info(LINFO_CLOSE_BACKEND, zap.String("backend key", bcKey),
		zap.String("Current Memory Usage:", strconv.FormatInt(m.currUsage, 10)),
		zap.String("Memory limitation:", strconv.FormatInt(m.maxLimit, 10)))
	return
}

// In exception case, clear intermediate files that lightning engine generated for index.
func (m *LightningMemoryRoot) ClearEngines(jobId int64, indexIds ...int64) {
	for _, indexId := range indexIds {
		eiKey := GenEngineInfoKey(jobId, indexId)
		ei, exist := m.EngineMgr.enginePool[eiKey]
		if exist {
			indexEngine := ei.openedEngine
			closedEngine, err := indexEngine.Close(ei.backCtx.Ctx, ei.cfg)
			if err != nil {
				log.L().Error(LERR_CLOSE_ENGINE_ERR, zap.String("Engine key", eiKey))
			}
			// Here the local intermediate file will be removed.
			err = closedEngine.Cleanup(ei.backCtx.Ctx)
			if err != nil {
				log.L().Error(LERR_CLOSE_ENGINE_ERR, zap.String("Engine key", eiKey))
			}
		}
	}
}

// Check and allocate one EngineInfo, delete engineInfo are put into delete backend
// The worker count means this time the engine need pre-check memory for workers
func (m *LightningMemoryRoot) RegistEngineInfo(job *model.Job, bcKey string, engineKey string, indexId int32, workerCount int) (int, error) {
	var err error = nil
	m.mLock.Lock()
	defer func() {
		m.mLock.Unlock()
	}()
	bc, exist := m.backendCache[bcKey]
	if !exist {
		log.L().Warn(LWAR_BACKEND_NOT_EXIST, zap.String("Backend key", bcKey))
		return 0, err
	}

	// Caculate lightning concurrecy degree and set memory usage.
	// and pre-allocate memory usage for worker
	newWorkerCount := m.workerDegree(workerCount, engineKey)
	en, exist1 := bc.EngineCache[engineKey]
	if !exist1 {
		// When return workerCount is 0, means there is no memory available for lightning worker.
		if workerCount == int(allocFailed) {
			log.L().Warn(LERR_ALLOC_MEM_FAILED, zap.String("Backend key", bcKey),
				zap.String("Engine key", engineKey),
				zap.String("Expected worker count:", strconv.Itoa(workerCount)),
				zap.String("Currnt alloc wroker count:", strconv.Itoa(newWorkerCount)))
			return 0, errors.New(LERR_CREATE_ENGINE_FAILED)
		}
		// Firstly, update and check the memory usage
		m.totalMemoryConsume()
		err = m.checkMemoryUsage(ALLOC_ENGINE_INFO)
		if err != nil {
			log.L().Warn(LERR_ALLOC_MEM_FAILED, zap.String("Backend key", bcKey),
				zap.String("Engine key", engineKey),
				zap.String("Current Memory Usage:", strconv.FormatInt(m.currUsage, 10)),
				zap.String("Memory limitation:", strconv.FormatInt(m.maxLimit, 10)))
			return 0, err
		}
		// Create one slice for one backend on one stmt, current we share one engine
		err = CreateEngine(bc.Ctx, job, bcKey, engineKey, indexId, workerCount)
		if err != nil {
			return 0, errors.New(LERR_CREATE_ENGINE_FAILED)
		}

		// Count memory usage.
		m.currUsage += m.structSize[string(ALLOC_ENGINE_INFO)]
		m.engineUsage += m.structSize[string(ALLOC_ENGINE_INFO)]
	} else {
		// If engine exist, then add newWorkerCount.
		en.WriterCount += newWorkerCount
	}
	log.L().Info(LINFO_OPEN_ENGINE, zap.String("backend key", bcKey),
		zap.String("Engine key", engineKey),
		zap.String("Current Memory Usage:", strconv.FormatInt(m.currUsage, 10)),
		zap.String("Memory limitation:", strconv.FormatInt(m.maxLimit, 10)),
	    zap.String("Expected Worker Count", strconv.Itoa(workerCount)),
		zap.String("Allocated worker count", strconv.Itoa(newWorkerCount)))
	return newWorkerCount, nil
}

// Create one
func (m *LightningMemoryRoot) RegistWorkerContext(engineInfoKey string, id int) (*WorkerContext, error) {
	var (
		err        error = nil
		wCtx       *WorkerContext
		memRequire int64 = m.structSize[string(ALLOC_WORKER_CONTEXT)]
	)
	m.mLock.Lock()
	defer func() {
		m.mLock.Unlock()
	}()
	// First to check the memory usage
	m.totalMemoryConsume()
	err = m.checkMemoryUsage(ALLOC_WORKER_CONTEXT)
	if err != nil {
		log.L().Error(LERR_ALLOC_MEM_FAILED, zap.String("Engine key", engineInfoKey),
			zap.String("worer Id:", strconv.Itoa(id)),
			zap.String("Memory allocate:", strconv.FormatInt(memRequire, 10)),
			zap.String("Current Memory Usage:", strconv.FormatInt(m.currUsage, 10)),
			zap.String("Memory limitation:", strconv.FormatInt(m.maxLimit, 10)))
		return nil, err
	}

	wCtx = &WorkerContext{}
	err = wCtx.InitWorkerContext(engineInfoKey, id)
	if err != nil {
		log.L().Error(LERR_CREATE_CONTEX_FAILED, zap.String("Engine key", engineInfoKey),
			zap.String("worer Id:", strconv.Itoa(id)),
			zap.String("Memory allocate:", strconv.FormatInt(memRequire, 10)),
			zap.String("Current Memory Usage:", strconv.FormatInt(m.currUsage, 10)),
			zap.String("Memory limitation:", strconv.FormatInt(m.maxLimit, 10)))
		return nil, err
	}

	// Count memory usage.
	m.currUsage += memRequire 
	log.L().Info(LINFO_CREATE_WRITER, zap.String("Engine key", engineInfoKey),
		zap.String("worer Id:", strconv.Itoa(id)),
		zap.String("Memory allocate:", strconv.FormatInt(memRequire, 10)),
		zap.String("Current Memory Usage:", strconv.FormatInt(m.currUsage, 10)),
		zap.String("Memory limitation:", strconv.FormatInt(m.maxLimit, 10)))
	return wCtx, err
}

// Uniform entry to release Engine info.
func (m *LightningMemoryRoot) deleteBackendEngines(bcKey string) error {
	var err error = nil
	var count int = 0
	bc, exist := m.getBackendContext(bcKey, true)
	if !exist {
		log.L().Error(LERR_GET_BACKEND_FAILED, zap.String("backend key", bcKey))
		return err
	}
	count = 0
	// Delete EngienInfo registed in m.engineManager.engineCache
	for _, ei := range bc.EngineCache {
		eiKey := ei.key
		wCnt := ei.WriterCount
		m.currUsage -= m.structSize[eiKey]
		delete(m.structSize, eiKey)
		delete(m.EngineMgr.enginePool, eiKey)
		m.currUsage -= m.structSize[string(ALLOC_WORKER_CONTEXT)] * int64(wCnt)
		count++
		log.L().Info(LINFO_DEL_ENGINE, zap.String("backend key", bcKey),
			zap.String("engine id", eiKey),
			zap.String("Current Memory Usage:", strconv.FormatInt(m.currUsage, 10)),
			zap.String("Memory limitation:", strconv.FormatInt(m.maxLimit, 10)))
	}

	bc.EngineCache = make(map[string]*engineInfo, 10)
	m.currUsage -= m.structSize[string(ALLOC_ENGINE_INFO)] * int64(count)
	m.engineUsage -= m.structSize[string(ALLOC_ENGINE_INFO)] * int64(count)
	log.L().Info(LINFO_CLOSE_BACKEND, zap.String("backend key", bcKey),
		zap.String("Current Memory Usage:", strconv.FormatInt(m.currUsage, 10)),
		zap.String("Memory limitation:", strconv.FormatInt(m.maxLimit, 10)))
	return err
}

func (m *LightningMemoryRoot) getBackendContext(bcKey string, needLog bool) (*BackendContext, bool) {
	bc, exist := m.backendCache[bcKey]
	if !exist {
		if needLog {
			log.L().Warn(LWAR_BACKEND_NOT_EXIST, zap.String("backend key:", bcKey))
		}
		return nil, false
	}
	return bc, exist
}

func (m *LightningMemoryRoot) totalMemoryConsume() {
	var diffSize int64 = 0
	for _, bc := range m.backendCache {
		curSize := bc.Backend.TotalMemoryConsume()
		bcSize, exist := m.structSize[bc.Key]
		if !exist {
			diffSize += curSize
			m.structSize[bc.Key] = curSize
		} else {
			diffSize += curSize - bcSize
			m.structSize[bc.Key] += curSize - bcSize
		}
		m.structSize[bc.Key] = curSize
	}
	m.currUsage += diffSize
	return
}

func (m *LightningMemoryRoot) workerDegree(workerCnt int, engineKey string) int {
	var kvp common.KvPair
	size := unsafe.Sizeof(kvp)
	// If only one worker's memory init requirement still bigger than mem limitation.
	if int64(size*units.MiB)+m.currUsage > m.maxLimit {
		return int(allocFailed)
	}

	for int64(size*units.MiB*uintptr(workerCnt))+m.currUsage > m.maxLimit && workerCnt > 1 {
		workerCnt /= 2
	}

	m.currUsage += int64(size * units.MiB * uintptr(workerCnt))
	_, exist := m.structSize[engineKey]
	if !exist {
		m.structSize[engineKey] = int64(size * units.MiB * uintptr(workerCnt))
	} else {
		m.structSize[engineKey] += int64(size * units.MiB * uintptr(workerCnt))
	}
	return workerCnt
}

func (m *LightningMemoryRoot) TotalDiskAvailable() uint64 {
    sz, err := lcom.GetStorageSize(GlobalLightningEnv.SortPath)
	if err != nil {
		log.L().Error(LERR_GET_STORAGE_QUOTA,
			zap.String("OS error:", err.Error()),
			zap.String("default disk quota", strconv.FormatInt(GlobalLightningEnv.diskQuota, 10)))
		return uint64(GlobalLightningEnv.diskQuota)
	}
	return sz.Available
}

// defaultImportantVariables is used in ObtainImportantVariables to retrieve the system
// variables from downstream which may affect KV encode result. The values record the default
// values if missing.
var defaultImportantVariables = map[string]string{
	"max_allowed_packet":      "67108864",
	"div_precision_increment": "4",
	"time_zone":               "SYSTEM",
	"lc_time_names":           "en_US",
	"default_week_format":     "0",
	"block_encryption_mode":   "aes-128-ecb",
	"group_concat_max_len":    "1024",
}

// defaultImportVariablesTiDB is used in ObtainImportantVariables to retrieve the system
// variables from downstream in local/importer backend. The values record the default
// values if missing.
var defaultImportVariablesTiDB = map[string]string{
	"tidb_row_format_version": "1",
}

func obtainImportantVariables() map[string]string {
	// convert result into a map. fill in any missing variables with default values.
	result := make(map[string]string, len(defaultImportantVariables)+len(defaultImportVariablesTiDB))
	for key, value := range defaultImportantVariables {
		result[key] = value
		v := variable.GetSysVar(key)
		if v.Value != value {
			result[key] = value
		}
	}

	for key, value := range defaultImportVariablesTiDB {
		result[key] = value
		v := variable.GetSysVar(key)
		if v.Value != value {
			result[key] = value
		}
	}
	return result
}
