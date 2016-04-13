package localstore

import "github.com/pingcap/tidb/kv"

type localPD struct {
	regions []*regionInfo
}

type regionInfo struct {
	startKey kv.Key
	endKey   kv.Key
	rs       *localRegion
}

func (pd *localPD) GetRegionInfo() []*regionInfo {
	return pd.regions
}

func (pd *localPD) SetRegionInfo(regions []*regionInfo) {
	pd.regions = regions
}

// ChangeRegionInfo used for test handling region info change.
func ChangeRegionInfo(store kv.Storage, regionID int, startKey, endKey []byte) {
	s := store.(*dbStore)
	for i, region := range s.pd.regions {
		if region.rs.id == regionID {
			newRegionInfo := &regionInfo{
				startKey: startKey,
				endKey:   endKey,
				rs:       region.rs,
			}
			region.rs.startKey = startKey
			region.rs.endKey = endKey
			s.pd.regions[i] = newRegionInfo
			break
		}
	}
}
