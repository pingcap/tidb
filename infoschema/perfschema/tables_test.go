// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package perfschema_test

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strings"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/infoschema/perfschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testTableSuite{})

type testTableSuite struct {
	store kv.Storage
	dom   *domain.Domain
}

func (s *testTableSuite) SetUpSuite(c *C) {
	testleak.BeforeTest()

	var err error
	s.store, err = mockstore.NewMockTikvStore()
	c.Assert(err, IsNil)
	session.DisableStats4Test()
	s.dom, err = session.BootstrapSession(s.store)
	c.Assert(err, IsNil)
}

func (s *testTableSuite) TearDownSuite(c *C) {
	defer testleak.AfterTest(c)()
	s.dom.Close()
	s.store.Close()
}

func (s *testTableSuite) TestPredefinedTables(c *C) {
	c.Assert(perfschema.IsPredefinedTable("EVENTS_statements_summary_by_digest"), IsTrue)
	c.Assert(perfschema.IsPredefinedTable("statements"), IsFalse)
}

func (s *testTableSuite) TestPerfSchemaTables(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("use performance_schema")
	tk.MustQuery("select * from global_status where variable_name = 'Ssl_verify_mode'").Check(testkit.Rows())
	tk.MustQuery("select * from session_status where variable_name = 'Ssl_verify_mode'").Check(testkit.Rows())
	tk.MustQuery("select * from setup_actors").Check(testkit.Rows())
	tk.MustQuery("select * from events_stages_history_long").Check(testkit.Rows())
}

func currentSourceDir() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Dir(file)
}

func (s *testTableSuite) TestTiKVProfileCPU(c *C) {
	router := http.NewServeMux()
	mockServer := httptest.NewServer(router)
	mockAddr := strings.TrimPrefix(mockServer.URL, "http://")
	defer mockServer.Close()

	copyHandler := func(filename string) http.HandlerFunc {
		return func(w http.ResponseWriter, _ *http.Request) {
			file, err := os.Open(filepath.Join(currentSourceDir(), filename))
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			defer func() { terror.Log(file.Close()) }()
			_, err = io.Copy(w, file)
			terror.Log(err)
		}
	}
	// mock tikv profile
	router.HandleFunc("/debug/pprof/profile", copyHandler("testdata/tikv.cpu.profile"))

	// failpoint setting
	servers := []string{
		strings.Join([]string{"tikv", mockAddr, mockAddr}, ","),
		strings.Join([]string{"pd", mockAddr, mockAddr}, ","),
	}
	fpExpr := strings.Join(servers, ";")
	fpName := "github.com/pingcap/tidb/infoschema/perfschema/mockRemoteNodeStatusAddress"
	c.Assert(failpoint.Enable(fpName, fmt.Sprintf(`return("%s")`, fpExpr)), IsNil)
	defer func() { c.Assert(failpoint.Disable(fpName), IsNil) }()

	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("use performance_schema")
	result := tk.MustQuery("select function, percent_abs, percent_rel from tikv_profile_cpu where depth < 3")

	warnings := tk.Se.GetSessionVars().StmtCtx.GetWarnings()
	c.Assert(len(warnings), Equals, 0, Commentf("expect no warnings, but found: %+v", warnings))

	result.Check(testkit.Rows(
		"root 100% 100%",
		"├─tikv::server::load_statistics::linux::ThreadLoadStatistics::record::h59facb8d680e7794 75.00% 75.00%",
		"│ └─procinfo::pid::stat::stat_task::h69e1aa2c331aebb6 75.00% 100%",
		"├─nom::nom::digit::h905aaaeff7d8ec8e 16.07% 16.07%",
		"│ ├─<core::iter::adapters::Enumerate<I> as core::iter::traits::iterator::Iterator>::next::h16936f9061bb75e4 6.25% 38.89%",
		"│ ├─Unknown 3.57% 22.22%",
		"│ ├─<&u8 as nom::traits::AsChar>::is_dec_digit::he9eacc3fad26ab81 2.68% 16.67%",
		"│ ├─<&[u8] as nom::traits::InputIter>::iter_indices::h6192338433683bff 1.79% 11.11%",
		"│ └─<&[T] as nom::traits::Slice<core::ops::range::RangeFrom<usize>>>::slice::h38d31f11f84aa302 1.79% 11.11%",
		"├─<jemallocator::Jemalloc as core::alloc::GlobalAlloc>::realloc::h5199c50710ab6f9d 1.79% 1.79%",
		"│ └─rallocx 1.79% 100%",
		"├─<jemallocator::Jemalloc as core::alloc::GlobalAlloc>::dealloc::hea83459aa98dd2dc 1.79% 1.79%",
		"│ └─sdallocx 1.79% 100%",
		"├─<jemallocator::Jemalloc as core::alloc::GlobalAlloc>::alloc::hc7962e02169a5c56 0.89% 0.89%",
		"│ └─mallocx 0.89% 100%",
		"├─engine::rocks::util::engine_metrics::flush_engine_iostall_properties::h64a7661c95aa1db7 0.89% 0.89%",
		"│ └─rocksdb::rocksdb::DB::get_map_property_cf::h9722f9040411af44 0.89% 100%",
		"├─core::ptr::real_drop_in_place::h8def0d99e7136f33 0.89% 0.89%",
		"│ └─<alloc::raw_vec::RawVec<T,A> as core::ops::drop::Drop>::drop::h9b59b303bffde02c 0.89% 100%",
		"├─tikv_util::metrics::threads_linux::ThreadInfoStatistics::record::ha8cc290b3f46af88 0.89% 0.89%",
		"│ └─procinfo::pid::stat::stat_task::h69e1aa2c331aebb6 0.89% 100%",
		"├─crossbeam_utils::backoff::Backoff::snooze::h5c121ef4ce616a3c 0.89% 0.89%",
		"│ └─core::iter::range::<impl core::iter::traits::iterator::Iterator for core::ops::range::Range<A>>::next::hdb23ceb766e7a91f 0.89% 100%",
		"└─<hashbrown::raw::bitmask::BitMaskIter as core::iter::traits::iterator::Iterator>::next::he129c78b3deb639d 0.89% 0.89%",
		"  └─Unknown 0.89% 100%"))

	// We can use current processe profile to mock profile of PD because the PD has the
	// same way of retrieving profile with TiDB. And the purpose of this test case is used
	// to make sure all profile HTTP API have been accessed.
	accessed := map[string]struct{}{}
	handlerFactory := func(name string, debug ...int) func(w http.ResponseWriter, _ *http.Request) {
		debugLevel := 0
		if len(debug) > 0 {
			debugLevel = debug[0]
		}
		return func(w http.ResponseWriter, _ *http.Request) {
			profile := pprof.Lookup(name)
			if profile == nil {
				http.Error(w, fmt.Sprintf("profile %s not found", name), http.StatusBadRequest)
				return
			}
			if err := profile.WriteTo(w, debugLevel); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			accessed[name] = struct{}{}
		}
	}

	// mock PD profile
	router.HandleFunc("/pd/api/v1/debug/pprof/profile", copyHandler("../../util/profile/testdata/test.pprof"))
	router.HandleFunc("/pd/api/v1/debug/pprof/heap", handlerFactory("heap"))
	router.HandleFunc("/pd/api/v1/debug/pprof/mutex", handlerFactory("mutex"))
	router.HandleFunc("/pd/api/v1/debug/pprof/allocs", handlerFactory("allocs"))
	router.HandleFunc("/pd/api/v1/debug/pprof/block", handlerFactory("block"))
	router.HandleFunc("/pd/api/v1/debug/pprof/goroutine", handlerFactory("goroutine", 2))

	tk.MustQuery("select * from pd_profile_cpu where depth < 3")
	warnings = tk.Se.GetSessionVars().StmtCtx.GetWarnings()
	c.Assert(len(warnings), Equals, 0, Commentf("expect no warnings, but found: %+v", warnings))

	tk.MustQuery("select * from pd_profile_memory where depth < 3")
	warnings = tk.Se.GetSessionVars().StmtCtx.GetWarnings()
	c.Assert(len(warnings), Equals, 0, Commentf("expect no warnings, but found: %+v", warnings))

	tk.MustQuery("select * from pd_profile_mutex where depth < 3")
	warnings = tk.Se.GetSessionVars().StmtCtx.GetWarnings()
	c.Assert(len(warnings), Equals, 0, Commentf("expect no warnings, but found: %+v", warnings))

	tk.MustQuery("select * from pd_profile_allocs where depth < 3")
	warnings = tk.Se.GetSessionVars().StmtCtx.GetWarnings()
	c.Assert(len(warnings), Equals, 0, Commentf("expect no warnings, but found: %+v", warnings))

	tk.MustQuery("select * from pd_profile_block where depth < 3")
	warnings = tk.Se.GetSessionVars().StmtCtx.GetWarnings()
	c.Assert(len(warnings), Equals, 0, Commentf("expect no warnings, but found: %+v", warnings))

	tk.MustQuery("select * from pd_profile_goroutines")
	warnings = tk.Se.GetSessionVars().StmtCtx.GetWarnings()
	c.Assert(len(warnings), Equals, 0, Commentf("expect no warnings, but found: %+v", warnings))

	c.Assert(len(accessed), Equals, 5, Commentf("expect all HTTP API had been accessed, but found: %v", accessed))
}
