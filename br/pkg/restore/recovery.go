package restore

import (
	"context"
	"fmt"
	"io"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/emirpasic/gods/maps/treemap"
	"github.com/pingcap/errors"
	recovpb "github.com/pingcap/kvproto/pkg/recoverypb"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

type Regions struct {
	*recovpb.RegionMeta
	Instance int
}

type RecoveryService struct {
	instances      int
	serviceAddr    string
	instanceId     *uint32
	received       *sync.WaitGroup // Whether all reports are collected or not.
	generated      *sync.WaitGroup // Whether commands are generated or not.
	finished       *sync.WaitGroup // Whether all commands are sent out or not.
	regionMetas    [][]*recovpb.RegionMeta
	regionRecovers [][]*recovpb.RegionRecover
	resolvedTs     *uint64
}

func NewRecoveryService(instances int, serviceAddr string) RecoveryService {
	var instanceId = new(uint32)
	var received = new(sync.WaitGroup)
	received.Add(instances)
	var generated = new(sync.WaitGroup)
	generated.Add(1)
	var finished = new(sync.WaitGroup)
	finished.Add(instances)
	var regionMetas = make([][]*recovpb.RegionMeta, instances)
	var regionRecovers = make([][]*recovpb.RegionRecover, instances)
	var resolvedTs = new(uint64)
	return RecoveryService{instances, serviceAddr, instanceId, received, generated, finished, regionMetas, regionRecovers, resolvedTs}
}

func (recoveryService RecoveryService) RecoverRegions(stream recovpb.Recovery_RecoverRegionsServer) (err error) {
	var offset = atomic.AddUint32(recoveryService.instanceId, 1) - 1
	for {
		var meta *recovpb.RegionMeta
		if meta, err = stream.Recv(); err == nil {
			recoveryService.regionMetas[offset] = append(recoveryService.regionMetas[offset], meta)
		} else if err == io.EOF {
			break
		} else {
			log.Error("peer info receieved failed", zap.Error(err))
			return errors.Trace(err)
		}
	}
	log.Info("received reports: ", zap.Uint32("Instance", offset))
	recoveryService.received.Done()

	recoveryService.generated.Wait()
	if err = stream.SendHeader(nil); err != nil {
		log.Error("send header failed.", zap.Error(err))
		return errors.Trace(err)
	}
	for _, command := range recoveryService.regionRecovers[offset] {
		if err = stream.Send(command); err != nil {
			log.Error("send region recovery command failed", zap.Error(err))
			return errors.Trace(err)
		}
	}

	log.Info("all recovery command sent to instance", zap.Uint32("instance", offset))

	recoveryService.finished.Done()
	return
}

func (recoveryService RecoveryService) Close(ctx context.Context, in *recovpb.CloseRequest) (*recovpb.CloseReply, error) {
	return &recovpb.CloseReply{Ok: "Close"}, nil
}

func (recoveryService RecoveryService) ResolveData(ctx context.Context, in *recovpb.ResolvedRequest) (*recovpb.ResolvedReply, error) {
	return &recovpb.ResolvedReply{ResolvedTs: 431656330251468803}, nil
}

func StartService(ctx context.Context, numOfStores int, serviceAddr string) error {
	// start recovery service
	log.Info("region recovery service start")

	// start a gorountine to read all region info
	var recoveryService = NewRecoveryService(numOfStores, serviceAddr)
	listener, err := net.Listen("tcp", serviceAddr)
	if err != nil {
		log.Info("listen service failed.", zap.String("service address", serviceAddr), zap.Error(err))
		return errors.Trace(err)
	}
	fmt.Printf("listen %s success, waiting for %d instances\n", serviceAddr, numOfStores)

	s := grpc.NewServer(
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    time.Duration(10) * time.Second,
			Timeout: time.Duration(3) * time.Second,
		}),
	)

	recovpb.RegisterRecoveryServer(s, recoveryService)
	go s.Serve(listener)

	// Wait for all TiKV instances reporting peer info.
	recoveryService.received.Wait()
	// TODO: here there is issue when tikv report without tombstone, but service has tombstone in meta
	// the printRegionInfo make the service work fine without tombstone, it looks like a concurrency issue.
	printRegionInfo(recoveryService.regionMetas)
	makeRecoveryPlan(recoveryService.regionMetas, recoveryService.regionRecovers, numOfStores)
	log.Info("region recovery commands are generated")
	recoveryService.generated.Done()

	recoveryService.finished.Wait()
	// TODO send message to clone the streams
	time.Sleep(time.Second * 3) // Sleep to wait grpc streams get closed.
	log.Info("region recovery phase completed.")
	s.Stop()
	return nil
}

// function provide for dump server received peer info into a file
// function shall be enalbed by debug enabled
func printRegionInfo(region_info [][]*recovpb.RegionMeta) {
	log.Info("dump service log information.")
	// Group region peer info by region id.
	var regions = make(map[uint64][]Regions, 0)
	for index, v := range region_info {
		for _, m := range v {
			if regions[m.RegionId] == nil {
				regions[m.RegionId] = make([]Regions, 0, 3)
			}
			regions[m.RegionId] = append(regions[m.RegionId], Regions{m, index})
		}
	}

	for region_id, meta := range regions {
		log.Info("Region", zap.Uint64("RegionID", region_id))
		for offset, m := range meta {
			log.Info("tikv", zap.Int("InstID", offset))
			log.Info("meta:", zap.Uint64("applied_index", m.GetAppliedIndex()))
			log.Info("meta:", zap.Uint64("last_index", m.GetLastIndex()))
			log.Info("meta:", zap.Uint64("term", m.GetTerm()))
			log.Info("meta:", zap.Uint64("version", m.GetVersion()))
			log.Info("meta:", zap.Bool("tombstone", m.GetTombstone()))
			log.Info("meta:", zap.ByteString("start_key", m.GetStartKey()))
			log.Info("meta:", zap.ByteString("end_key", m.GetEndKey()))
		}
	}

}

// generate the related the recovery plan to tikvs:
// 1. check overlap the region, make a recovery decision
// 2. assign a leader for region during the tikv startup
// 3. set region as tombstone
func makeRecoveryPlan(regionMetas [][]*recovpb.RegionMeta, commands [][]*recovpb.RegionRecover, numStore int) {
	type peer struct {
		rid uint64
		ver uint64
	}

	type regionEndKey struct {
		end_key []byte
		rid     uint64
	}

	// Group region peer info by region id.
	// region [id] [peer[0-n]]
	var regions = make(map[uint64][]Regions, 0)
	for index, v := range regionMetas {
		for _, m := range v {
			if regions[m.RegionId] == nil {
				regions[m.RegionId] = make([]Regions, 0, numStore)
			}
			regions[m.RegionId] = append(regions[m.RegionId], Regions{m, index})
		}
	}

	// Reverse sort replicas by last index, and collect all regions' version.
	var versions = make([]peer, 0, len(regions))
	for r, x := range regions {
		sort.Slice(x, func(i, j int) bool { return x[i].LastIndex > x[j].LastIndex })
		var v = x[0].Version
		versions = append(versions, peer{r, v})
	}

	sort.Slice(versions, func(i, j int) bool { return versions[i].ver > versions[j].ver })

	// split and merge in progressing during the backup, there may some overlap region, we have to handle it
	// Resolve version conflicts.
	var topo = treemap.NewWith(keyCmpInterface)
	for _, p := range versions {
		var sk = prefixStartKey(regions[p.rid][0].StartKey)
		var ek = prefixEndKey(regions[p.rid][0].EndKey)
		var fk, fv interface{}
		fk, _ = topo.Ceiling(sk)
		// keysapce overlap sk within ceiling - fk
		if fk != nil && (keyEq(fk.([]byte), sk) || keyCmp(fk.([]byte), ek) < 0) {
			continue
		}

		// keysapce overlap sk within floor - fk.end_key
		fk, fv = topo.Floor(sk)
		if fk != nil && keyCmp(fv.(regionEndKey).end_key, sk) > 0 {
			continue
		}
		topo.Put(sk, regionEndKey{ek, p.rid})
	}

	//TODO: there is corner case, in a region, the peer with max last index may have lower term (network quarantine)

	// After resolved, all validPeer regions shouldn't be tombstone.
	// do some sanity check
	var validPeer = make(map[uint64]struct{}, 0)
	var iter = topo.Iterator()
	var prevEndKey = prefixStartKey([]byte{})
	var prevR uint64 = 0
	for iter.Next() {
		v := iter.Value().(regionEndKey)
		if regions[v.rid][0].Tombstone {
			log.Error("validPeer shouldn't be tombstone", zap.Uint64("regionID", v.rid))
			// TODO, panic or something we have to do, a PoC or test may need for decision
			panic("validPeer shouldn't be tombstone")
		}
		if !keyEq(prevEndKey, iter.Key().([]byte)) {
			log.Error("region doesn't conject to region", zap.Uint64("preRegion", prevR), zap.Uint64("curRegion", v.rid))
			// TODO, panic or something we have to do, a PoC or test may need for decision
			panic("regions should conject to each other")
		}
		prevEndKey = v.end_key
		prevR = v.rid
		validPeer[v.rid] = struct{}{}
	}

	// Generate recover commands.
	for r, x := range regions {
		if _, ok := validPeer[r]; !ok {
			// Generate a tombstone command.
			// 1, peer is tomebstone
			// 2, split region in progressing, old one can be a tomebstone
			for _, m := range x {
				cmd := &recovpb.RegionRecover{RegionId: m.RegionId, Tombstone: true}
				if cmd.Tombstone != m.Tombstone {
					commands[m.Instance] = append(commands[m.Instance], cmd)
				}
			}
		} else {
			// Generate normal commands.
			var maxTerm uint64 = 0
			for _, m := range x {
				if m.Term > maxTerm {
					maxTerm = m.Term
				}
			}
			for i, m := range x {
				cmd := &recovpb.RegionRecover{RegionId: m.RegionId, Term: maxTerm}
				// max last index as a leader
				cmd.AsLeader = (i == 0)
				if cmd.Term != m.Term || cmd.AsLeader {
					commands[m.Instance] = append(commands[m.Instance], cmd)
				}
			}
		}
	}
}

func prefixStartKey(key []byte) []byte {
	var sk = make([]byte, 0, len(key)+1)
	sk = append(sk, 'z')
	sk = append(sk, key...)
	return sk
}

func prefixEndKey(key []byte) []byte {
	if len(key) == 0 {
		return []byte{'z' + 1}
	}
	return prefixStartKey(key)
}

func keyEq(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func keyCmp(a, b []byte) int {
	var length = 0
	var chosen = 0
	if len(a) < len(b) {
		length = len(a)
		chosen = -1
	} else if len(a) == len(b) {
		length = len(a)
		chosen = 0
	} else {
		length = len(b)
		chosen = 1
	}
	for i := 0; i < length; i++ {
		if a[i] < b[i] {
			return -1
		} else if a[i] > b[i] {
			return 1
		}
	}
	return chosen
}

func keyCmpInterface(a, b interface{}) int {
	return keyCmp(a.([]byte), b.([]byte))
}
