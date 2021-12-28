package sst

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	tidbcfg "github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/twmb/murmur3"
	"sync"
	"sync/atomic"
)

func LogDebug(format string, a ...interface{}) {
	fmt.Printf("debug] %s", fmt.Sprintf(format, a...))
}

// pdaddr; tidb-host/status
type ClusterInfo struct {
	PdAddr string
	// TidbHost string - 127.0.0.1
	Port   uint
	Status uint
}

type DDLInfo struct {
	Schema  string
	Table   *model.TableInfo
	StartTs uint64
}

const (
	indexEngineID = -1 // same to restore.table_restore.go indexEngineID
)

type engineInfo struct {
	*backend.OpenedEngine
	writer *backend.LocalEngineWriter
	cfg    *backend.EngineConfig
	ref    int32
}

func (ec *engineCache) put(startTs uint64, cfg *backend.EngineConfig, en *backend.OpenedEngine) {
	ec.mtx.Lock()
	ec.cache[startTs] = &engineInfo{
		en,
		nil,
		cfg,
		0,
	}
	ec.mtx.Unlock()
	LogDebug("put %d", startTs)
}

var (
	ErrNotFound       = errors.New("not object in this cache")
	ErrWasInUse       = errors.New("this object was in used")
	ec                = engineCache{cache: map[uint64]*engineInfo{}}
	cluster           ClusterInfo
	IndexDDLLightning = flag.Bool("ddl-mode", true, "index ddl use sst mode")
)

func (ec *engineCache) getEngineInfo(startTs uint64) (*engineInfo, error) {
	LogDebug("getEngineInfo by %d", startTs)
	ec.mtx.RUnlock()
	ei := ec.cache[startTs]
	// `ref` or run by atomic ?
	// if ei.ref {
	// 	ei = nil
	// } else {
	// 	ei.ref = true
	// }
	ec.mtx.Unlock()
	if false == atomic.CompareAndSwapInt32(&ei.ref, 0, 1) {
		return nil, ErrWasInUse
	}
	return ei, nil
}

func (ec *engineCache) releaseRef(startTs uint64) {
	LogDebug("releaseRef by %d", startTs)
	ec.mtx.RUnlock()
	ei := ec.cache[startTs]
	ec.mtx.Unlock()
	atomic.CompareAndSwapInt32(&ei.ref, 1, 0)
}

func (ec *engineCache) getWriter(startTs uint64) (*backend.LocalEngineWriter, error) {
	LogDebug("getWriter by %d", startTs)
	ei, err := ec.getEngineInfo(startTs)
	if err != nil {
		return nil, err
	}
	if ei.writer != nil {
		return ei.writer, nil
	}
	ei.writer, err = ei.OpenedEngine.LocalWriter(context.TODO(), &backend.LocalWriterConfig{})
	if err != nil {
		return nil, err
	}
	return ei.writer, nil
}

type engineCache struct {
	cache map[uint64]*engineInfo
	mtx   sync.RWMutex
}

func init() {
	cfg := tidbcfg.GetGlobalConfig()
	cluster.PdAddr = cfg.AdvertiseAddress
	cluster.Port = cfg.Port
	cluster.Status = cfg.Status.StatusPort
	LogDebug("InitOnce %+v", cluster)
}

// TODO: 1. checkpoint??
// TODO: 2. EngineID can use startTs for only.
func PrepareIndexOp(ctx context.Context, ddl DDLInfo) error {
	LogDebug("PrepareIndexOp %+v", ddl)
	info := cluster
	be, err := createLocalBackend(ctx, info)
	if err != nil {
		return fmt.Errorf("PrepareIndexOp.createLocalBackend err:%w", err)
	}
	cpt := checkpoints.TidbTableInfo{
		genNextTblId(),
		ddl.Schema,
		ddl.Table.Name.String(),
		ddl.Table,
	}
	var cfg backend.EngineConfig
	cfg.TableInfo = &cpt
	//
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], ddl.StartTs)
	h := murmur3.New32()
	h.Write(b[:])
	en, err := be.OpenEngine(ctx, &cfg, ddl.Table.Name.String(), int32(h.Sum32()))
	if err != nil {
		return fmt.Errorf("PrepareIndexOp.OpenEngine err:%w", err)
	}
	ec.put(ddl.StartTs, &cfg, en)
	return nil
}

func IndexOperator(ctx context.Context, startTs uint64, kvp kv.KvPairs) error {
	if kvp.Size() <= 0 {
		return nil
	}
	lw, err := ec.getWriter(startTs)
	if err != nil {
		return fmt.Errorf("IndexOperator.getWriter err:%w", err)
	}
	defer ec.releaseRef(startTs)
	err = lw.WriteRows(ctx, nil, &kvp)
	if err != nil {
		return fmt.Errorf("IndexOperator.WriteRows err:%w", err)
	}
	return nil
}

// stop this routine by close(kvs) or some context error.
func RunIndexOpRoutine(ctx context.Context, engine *backend.OpenedEngine, kvs <-chan kv.KvPairs) error {
	logutil.BgLogger().Info("createIndex-routine on dbname.tbl")

	running := true
	for running {
		select {
		case <-ctx.Done():
			fmt.Errorf("RunIndexOpRoutine was exit by Context.Done")
		case kvp, close := <-kvs:
			if close {
				running = false
				break
			}
			err := process(ctx, engine, kvp)
			if err != nil {
				return fmt.Errorf("process err:%s.clean data.", err.Error())
			}
		}
	}
	logutil.BgLogger().Info("createIndex-routine on dbname.tbl exit...")
	return nil
}

func FinishIndexOp(ctx context.Context, startTs uint64) error {
	LogDebug("FinishIndexOp %d", startTs)
	ei,err := ec.getEngineInfo(startTs)
	if err != nil {
		return err
	}
	defer ec.releaseRef(startTs)
	indexEngine := ei.OpenedEngine
	cfg := ei.cfg
	//
	closeEngine, err1 := indexEngine.Close(ctx, cfg)
	if err1 != nil {
		return fmt.Errorf("engine.Close err:%w", err1)
	}
	// use default value first;
	err = closeEngine.Import(ctx, int64(config.SplitRegionSize))
	if err != nil {
		return fmt.Errorf("engine.Import err:%w", err)
	}
	err = closeEngine.Cleanup(ctx)
	if err != nil {
		return fmt.Errorf("engine.Cleanup err:%w", err)
	}
	return nil
}

func process(ctx context.Context, indexEngine *backend.OpenedEngine, kvp kv.KvPairs) error {
	indexWriter, err := indexEngine.LocalWriter(ctx, &backend.LocalWriterConfig{})
	if err != nil {
		return fmt.Errorf("LocalWriter err:%s", err.Error())
	}
	// columnNames 可以不需要，因为我们肯定是 非 sorted 的数据.
	err = indexWriter.WriteRows(ctx, nil, &kvp)
	if err != nil {
		indexWriter.Close(ctx)
		return fmt.Errorf("WriteRows err:%s", err.Error())
	}
	return nil
}
