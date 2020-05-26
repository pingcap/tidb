package expression_test

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/server"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testkit"
	"go.uber.org/zap"
)

var _ = SerialSuites(&testServerSuite{})

type testServerSuite struct {
	testIntegrationSuiteBase

	port       uint
	statusPort uint

	tidbdrv *server.TiDBDriver
	tidbsrv *server.Server
	tk      *testkit.TestKit
}

// SetUpSuite create a mocked server mocked store
func (s *testServerSuite) SetUpSuite(c *C) {
	var err error
	s.store, s.dom, err = newStoreWithBootstrap()
	c.Assert(err, IsNil)
	s.ctx = mock.NewContext()
	s.tidbdrv = server.NewTiDBDriver(s.store)
	globalConfig := config.GetGlobalConfig()
	s.port = globalConfig.Port
	s.statusPort = globalConfig.Status.StatusPort

	cfg := config.NewConfig()
	cfg.Port = s.port
	cfg.Store = "tikv"
	cfg.Status.StatusPort = s.statusPort
	cfg.Status.ReportStatus = true

	srv, err := server.NewServer(cfg, s.tidbdrv)
	c.Assert(err, IsNil)
	go srv.Run()
	s.waitUntilServerOnline()

	s.tk = testkit.NewTestKit(c, s.store)
	s.tk.MustExec("create database tidb;")
}

// TearDownSuite will close mocked store and server
func (s *testServerSuite) TearDownSuite(c *C) {
	if s.dom != nil {
		s.dom.Close()
	}
	if s.store != nil {
		_ = s.store.Close()
	}
	if s.tidbsrv != nil {
		s.tidbsrv.Close()
	}
}

func (s *testServerSuite) prepare(c *C) {
	s.tk.MustExec("use tidb;")
}

// TearDownTest will drop all table created by testing
func (s *testServerSuite) clean(c *C) {
	tk := s.tk
	tk.MustExec("use tidb")
	r := tk.MustQuery("show tables")
	for _, tb := range r.Rows() {
		tableName := tb[0]
		tk.MustExec(fmt.Sprintf("drop table %v", tableName))
	}
}

// statusURL return the full URL of a status path
func (s *testServerSuite) statusURL(path string) string {
	return fmt.Sprintf("http://localhost:%d%s", s.statusPort, path)
}

// fetchStatus exec http.Get to server status port
func (s *testServerSuite) fetchStatus(path string) (*http.Response, error) {
	return http.Get(s.statusURL(path))
}

const retryTime = 100

func (s *testServerSuite) waitUntilServerOnline() {
	var retry int
	for retry = 0; retry < retryTime; retry++ {
		// fetch http status
		resp, err := s.fetchStatus("/status")
		if err == nil {
			ioutil.ReadAll(resp.Body)
			resp.Body.Close()
			break
		}
		time.Sleep(time.Millisecond * 10)
	}
	if retry == retryTime {
		log.Fatal("failed to connect HTTP status in every 10 ms", zap.Int("retryTime", retryTime))
	}
}

type mvccInfo struct {
	Key      string                        `json:"key"`
	RegionID uint64                        `json:"region_id"`
	Value    *kvrpcpb.MvccGetByKeyResponse `json:"value"`
}

func (s *testServerSuite) getMVCCInfo(path string) (*mvccInfo, error) {
	// retrieveMVCC information from http api
	resp, err := s.fetchStatus(path)
	if err != nil {
		return nil, err
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	mvcc := &mvccInfo{}
	if err := json.Unmarshal(body, &mvcc); err != nil {
		return nil, err
	}

	// check rpc error
	if mvcc.Value.Error != "" {
		return nil, fmt.Errorf(mvcc.Value.Error)
	}
	if mvcc.Value.RegionError != nil {
		return nil, fmt.Errorf(mvcc.Value.RegionError.Message)
	}
	return mvcc, nil
}
