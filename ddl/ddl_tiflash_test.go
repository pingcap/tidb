package ddl_test
import (
	"fmt"
	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/ddl/placement"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/helper"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/mockstore/unistore"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/tikv/client-go/v2/testutils"
	"time"
)

type tiflashDDLTestSuite struct {
	store kv.Storage
	dom   *domain.Domain
}


var _ = Suite(&tiflashDDLTestSuite{})

func (s *tiflashDDLTestSuite) SetUpSuite(c *C) {
	fmt.Printf("HAAAHAHHAAA\n")
	var err error
	s.store, err = mockstore.NewMockStore(
		mockstore.WithClusterInspector(func(c testutils.Cluster) {
			mockCluster := c.(*unistore.Cluster)
			_, _, region1 := mockstore.BootstrapWithSingleStore(c)
			tiflashIdx := 0
			for tiflashIdx < 2 {
				store2 := c.AllocID()
				peer2 := c.AllocID()
				addr2 := fmt.Sprintf("tiflash%d", tiflashIdx)
				mockCluster.AddStore(store2, addr2, &metapb.StoreLabel{Key: "engine", Value: "tiflash"})
				mockCluster.AddPeer(region1, store2, peer2)
				tiflashIdx++
			}
		}),
		mockstore.WithStoreType(mockstore.EmbedUnistore),
	)
	fmt.Printf("HAAAHAHHAAA2\n")

	c.Assert(err, IsNil)

	session.SetSchemaLease(0)
	fmt.Printf("HAAAHAHHAAA3\n")
	session.DisableStats4Test()
	fmt.Printf("HAAAHAHHAAA4\n")

	s.dom, err = session.BootstrapSession(s.store)
	fmt.Printf("HAAAHAHHAAA5\n")
	c.Assert(err, IsNil)
	s.dom.SetStatsUpdating(true)
	fmt.Printf("HAAAHAHHAAA6\n")
}

func (s *tiflashDDLTestSuite) TearDownSuite(c *C) {
	s.dom.Close()
	c.Assert(s.store.Close(), IsNil)
}

func (s *tiflashDDLTestSuite) CheckPlacementRule(rule placement.Rule) (bool, error) {
	tikvStore, ok := s.dom.Store().(helper.Storage)
	if !ok {
		return false, errors.New("Can not get Helper")
	}
	tikvHelper := &helper.Helper{
		Store:       tikvStore,
		RegionCache: tikvStore.GetRegionCache(),
	}

	allRulesArr, err := tikvHelper.GetGroupRules("tiflash")
	if err != nil {
		return false, errors.Trace(err)
	}

	matched := false
	for _, r := range allRulesArr {
		if r.StartKeyHex == rule.StartKeyHex && r.EndKeyHex == rule.EndKeyHex && r.Count == rule.Count && len(r.LocationLabels) == len(rule.LocationLabels){
			matched = true
			for i, l := range r.LocationLabels {
				if l != rule.LocationLabels[i]{
					matched = false
					break
				}
			}
			if matched {
				break
			}
		}
	}

	if matched{
		return true, nil
	}
	return false, nil
}

func (s *tiflashDDLTestSuite) TestSetPlacementRule(c *C) {
	fmt.Printf("ZZZZZZZZZZ\n")
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(z int)")
	tk.MustExec("alter table t set tiflash replica 1")

	time.Sleep(time.Second * 3)

	tb, err := s.dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	expectRule := ddl.MakeNewRule(tb.Meta().ID, 1, []string{})
	s.CheckPlacementRule(*expectRule)

}


