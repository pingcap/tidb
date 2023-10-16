// Copyright 2019 PingCAP, Inc.
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

package ddl_test

import (
	"testing"
	"time"

	mysql "github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/external"
	"github.com/stretchr/testify/require"
)

func TestCreateSequence(t *testing.T) {
	store := testkit.CreateMockStore(t)
	session.SetSchemaLease(600 * time.Millisecond)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop sequence if exists seq")
	tk.MustGetErrCode("create sequence `seq  `", mysql.ErrWrongTableName)

	// increment should not be set as 0.
	tk.MustGetErrCode("create sequence seq increment 0", mysql.ErrSequenceInvalidData)

	// maxvalue should be larger than minvalue.
	tk.MustGetErrCode("create sequence seq maxvalue 1 minvalue 2", mysql.ErrSequenceInvalidData)

	// maxvalue should be larger than minvalue.
	tk.MustGetErrCode("create sequence seq maxvalue 1 minvalue 1", mysql.ErrSequenceInvalidData)

	// maxvalue shouldn't be equal to MaxInt64.
	tk.MustGetErrCode("create sequence seq maxvalue 9223372036854775807 minvalue 1", mysql.ErrSequenceInvalidData)

	// TODO : minvalue shouldn't be equal to MinInt64.

	// maxvalue should be larger than start.
	tk.MustGetErrCode("create sequence seq maxvalue 1 start with 2", mysql.ErrSequenceInvalidData)

	// cacheVal should be less than (math.MaxInt64-maxIncrement)/maxIncrement.
	tk.MustGetErrCode("create sequence seq increment 100000 cache 922337203685477", mysql.ErrSequenceInvalidData)

	// test unsupported table option in sequence.
	tk.MustGetErrCode("create sequence seq CHARSET=utf8", mysql.ErrSequenceUnsupportedTableOption)

	tk.MustExec("create sequence seq comment=\"test\"")

	sequenceTable := external.GetTableByName(t, tk, "test", "seq")

	require.Equal(t, true, sequenceTable.Meta().IsSequence())
	require.Equal(t, model.DefaultSequenceIncrementValue, sequenceTable.Meta().Sequence.Increment)
	require.Equal(t, model.DefaultPositiveSequenceStartValue, sequenceTable.Meta().Sequence.Start)
	require.Equal(t, model.DefaultPositiveSequenceMinValue, sequenceTable.Meta().Sequence.MinValue)
	require.Equal(t, model.DefaultPositiveSequenceMaxValue, sequenceTable.Meta().Sequence.MaxValue)
	require.Equal(t, true, sequenceTable.Meta().Sequence.Cache)
	require.Equal(t, model.DefaultSequenceCacheValue, sequenceTable.Meta().Sequence.CacheValue)
	require.Equal(t, false, sequenceTable.Meta().Sequence.Cycle)

	// Test create privilege.
	tk.MustExec("drop user if exists myuser@localhost")
	tk.MustExec("create user myuser@localhost")

	tk1 := testkit.NewTestKit(t, store)
	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	require.NoError(t, se.Auth(&auth.UserIdentity{Username: "myuser", Hostname: "localhost"}, nil, nil, nil))
	tk1.SetSession(se)

	// grant the myuser the access to database test.
	tk.MustExec("grant select on test.* to 'myuser'@'localhost'")

	tk1.MustExec("use test")
	_, err = tk1.Exec("create sequence my_seq")
	require.Error(t, err)
	require.EqualError(t, err, "[planner:1142]CREATE command denied to user 'myuser'@'localhost' for table 'my_seq'")
}

func TestSequenceFunction(t *testing.T) {
	store := testkit.CreateMockStore(t)
	session.SetSchemaLease(600 * time.Millisecond)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("drop sequence if exists seq1")
	tk.MustExec("create sequence seq")

	// test normal sequence function.
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("1"))
	tk.MustQuery("select nextval(test.seq)").Check(testkit.Rows("2"))
	tk.MustQuery("select next value for seq").Check(testkit.Rows("3"))
	tk.MustQuery("select next value for test.seq").Check(testkit.Rows("4"))

	// test sequence function error.
	tk.MustGetErrMsg("select nextval(seq1)", "[schema:1146]Table 'test.seq1' doesn't exist")
	tk.MustExec("create database test2")
	tk.MustExec("use test2")
	tk.MustQuery("select nextval(test.seq)").Check(testkit.Rows("5"))
	tk.MustQuery("select next value for test.seq").Check(testkit.Rows("6"))
	tk.MustGetErrMsg("select nextval(seq)", "[schema:1146]Table 'test2.seq' doesn't exist")
	tk.MustGetErrMsg("select next value for seq", "[schema:1146]Table 'test2.seq' doesn't exist")
	tk.MustExec("use test")

	// test sequence nocache.
	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("create sequence seq nocache")
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("1"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("2"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("3"))

	// test sequence option logic.
	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("create sequence seq increment = 5")
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("1"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("6"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("11"))

	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("create sequence seq increment = 5 start = 3")
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("3"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("8"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("13"))

	// minvalue should be specified lower than start (negative here), default 1 when increment > 0.
	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("create sequence seq minvalue -5 start = -2 increment = 5")
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("-2"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("3"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("8"))

	// test sequence cycle.
	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("create sequence seq increment = 5 start = 3 maxvalue = 12 cycle")
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("3"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("8"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("1"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("6"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("11"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("1"))

	// test sequence maxvalue allocation.
	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("create sequence seq increment = 4 start = 2 maxvalue = 10 cycle")
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("2"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("6"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("10"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("1"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("5"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("9"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("1"))

	// test sequence has run out.
	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("create sequence seq increment = 5 start = 3 maxvalue = 12 nocycle")
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("3"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("8"))
	err := tk.QueryToErr("select nextval(seq)")
	require.EqualError(t, err, "[table:4135]Sequence 'test.seq' has run out")

	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("create sequence seq increment = 3 start = 3 maxvalue = 9 nocycle")
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("3"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("6"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("9"))
	err = tk.QueryToErr("select nextval(seq)")
	require.EqualError(t, err, "[table:4135]Sequence 'test.seq' has run out")

	// test negative-growth sequence
	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("create sequence seq increment = -2 start = 3 minvalue -5 maxvalue = 12 cycle")
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("3"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("1"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("-1"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("-3"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("-5"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("12"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("10"))

	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("create sequence seq increment = -3 start = 2 minvalue -6 maxvalue = 11 cycle")
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("2"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("-1"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("-4"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("11"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("8"))

	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("create sequence seq increment = -4 start = 6 minvalue -6 maxvalue = 11")
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("6"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("2"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("-2"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("-6"))
	err = tk.QueryToErr("select nextval(seq)")
	require.EqualError(t, err, "[table:4135]Sequence 'test.seq' has run out")

	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("create sequence seq increment = -3 start = 2 minvalue -2 maxvalue 10")
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("2"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("-1"))
	err = tk.QueryToErr("select nextval(seq)")
	require.EqualError(t, err, "[table:4135]Sequence 'test.seq' has run out")

	// test sequence setval function.
	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("create sequence seq")
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("1"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("2"))
	// set value to a used value, will get NULL.
	tk.MustQuery("select setval(seq, 2)").Check(testkit.Rows("<nil>"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("3"))
	// set value to a unused value, will get itself.
	tk.MustQuery("select setval(seq, 5)").Check(testkit.Rows("5"))
	// the next value will not be base on next value.
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("6"))

	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("create sequence seq increment 3 maxvalue 11")
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("1"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("4"))
	tk.MustQuery("select setval(seq, 3)").Check(testkit.Rows("<nil>"))
	tk.MustQuery("select setval(seq, 4)").Check(testkit.Rows("<nil>"))
	tk.MustQuery("select setval(seq, 5)").Check(testkit.Rows("5"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("7"))
	tk.MustQuery("select setval(seq, 8)").Check(testkit.Rows("8"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("10"))
	err = tk.QueryToErr("select nextval(seq)")
	require.EqualError(t, err, "[table:4135]Sequence 'test.seq' has run out")
	tk.MustQuery("select setval(seq, 11)").Check(testkit.Rows("11"))
	err = tk.QueryToErr("select nextval(seq)")
	require.EqualError(t, err, "[table:4135]Sequence 'test.seq' has run out")
	// set value can be bigger than maxvalue.
	tk.MustQuery("select setval(seq, 100)").Check(testkit.Rows("100"))
	err = tk.QueryToErr("select nextval(seq)")
	require.EqualError(t, err, "[table:4135]Sequence 'test.seq' has run out")

	// test setval in second cache round.
	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("create sequence seq increment 10 start 5 maxvalue 100 cache 10 cycle")
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("5"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("15"))
	tk.MustQuery("select setval(seq, 20)").Check(testkit.Rows("20"))
	// the next value will not be base on next value.
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("25"))
	sequenceTable := external.GetTableByName(t, tk, "test", "seq")
	tc, ok := sequenceTable.(*tables.TableCommon)
	require.Equal(t, true, ok)
	_, end, round := tc.GetSequenceCommon().GetSequenceBaseEndRound()
	require.Equal(t, int64(95), end)
	require.Equal(t, int64(0), round)
	// exhausted the sequence first round in cycle.
	tk.MustQuery("select setval(seq, 95)").Check(testkit.Rows("95"))
	// make sequence alloc the next batch.
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("1"))
	sequenceTable = external.GetTableByName(t, tk, "test", "seq")
	tc, ok = sequenceTable.(*tables.TableCommon)
	require.Equal(t, true, ok)
	_, end, round = tc.GetSequenceCommon().GetSequenceBaseEndRound()
	require.Equal(t, int64(91), end)
	require.Equal(t, int64(1), round)
	tk.MustQuery("select setval(seq, 15)").Check(testkit.Rows("15"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("21"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("31"))

	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("create sequence seq increment 2 start 0 maxvalue 10 minvalue -10 cache 3 cycle")
	tk.MustQuery("select setval(seq, -20)").Check(testkit.Rows("<nil>"))
	tk.MustQuery("select setval(seq, 20)").Check(testkit.Rows("20"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("-10"))
	sequenceTable = external.GetTableByName(t, tk, "test", "seq")
	tc, ok = sequenceTable.(*tables.TableCommon)
	require.Equal(t, true, ok)
	_, end, round = tc.GetSequenceCommon().GetSequenceBaseEndRound()
	require.Equal(t, int64(-6), end)
	require.Equal(t, int64(1), round)

	// test setval in negative-growth sequence.
	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("create sequence seq increment -3 start 5 maxvalue 10 minvalue -10 cache 3 cycle")
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("5"))
	sequenceTable = external.GetTableByName(t, tk, "test", "seq")
	tc, ok = sequenceTable.(*tables.TableCommon)
	require.Equal(t, true, ok)
	_, end, round = tc.GetSequenceCommon().GetSequenceBaseEndRound()
	require.Equal(t, int64(-1), end)
	require.Equal(t, int64(0), round)
	// exhausted the sequence first cache batch.
	tk.MustQuery("select setval(seq, -2)").Check(testkit.Rows("-2"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("-4"))
	sequenceTable = external.GetTableByName(t, tk, "test", "seq")
	tc, ok = sequenceTable.(*tables.TableCommon)
	require.Equal(t, true, ok)
	_, end, round = tc.GetSequenceCommon().GetSequenceBaseEndRound()
	require.Equal(t, int64(-10), end)
	require.Equal(t, int64(0), round)
	// exhausted the sequence second cache batch.
	tk.MustQuery("select setval(seq, -10)").Check(testkit.Rows("-10"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("10"))
	sequenceTable = external.GetTableByName(t, tk, "test", "seq")
	tc, ok = sequenceTable.(*tables.TableCommon)
	require.Equal(t, true, ok)
	_, end, round = tc.GetSequenceCommon().GetSequenceBaseEndRound()
	require.Equal(t, int64(4), end)
	require.Equal(t, int64(1), round)
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("7"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("4"))
	// test the sequence negative rebase.
	tk.MustQuery("select setval(seq, 0)").Check(testkit.Rows("0"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("-2"))

	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("create sequence seq increment -2 start 0 maxvalue 10 minvalue -10 cache 3 cycle")
	tk.MustQuery("select setval(seq, 20)").Check(testkit.Rows("<nil>"))
	tk.MustQuery("select setval(seq, -20)").Check(testkit.Rows("-20"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("10"))
	sequenceTable = external.GetTableByName(t, tk, "test", "seq")
	tc, ok = sequenceTable.(*tables.TableCommon)
	require.Equal(t, true, ok)
	_, end, round = tc.GetSequenceCommon().GetSequenceBaseEndRound()
	require.Equal(t, int64(6), end)
	require.Equal(t, int64(1), round)

	// test sequence lastval function.
	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("create sequence seq")
	tk.MustQuery("select lastval(seq)").Check(testkit.Rows("<nil>"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("1"))
	tk.MustQuery("select lastval(seq)").Check(testkit.Rows("1"))
	tk.MustQuery("select next value for seq").Check(testkit.Rows("2"))
	tk.MustQuery("select lastval(seq)").Check(testkit.Rows("2"))
	// setval won't change the last value.
	tk.MustQuery("select setval(seq, -1)").Check(testkit.Rows("<nil>"))
	tk.MustQuery("select lastval(seq)").Check(testkit.Rows("2"))
	tk.MustQuery("select setval(seq, 5)").Check(testkit.Rows("5"))
	tk.MustQuery("select lastval(seq)").Check(testkit.Rows("2"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("6"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("7"))

	// test lastval in positive-growth sequence cycle and cache.
	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("create sequence seq increment 3 start 3 maxvalue 14 cache 3 cycle")
	tk.MustQuery("select lastval(seq)").Check(testkit.Rows("<nil>"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("3"))
	sequenceTable = external.GetTableByName(t, tk, "test", "seq")
	tc, ok = sequenceTable.(*tables.TableCommon)
	require.Equal(t, true, ok)
	_, end, round = tc.GetSequenceCommon().GetSequenceBaseEndRound()
	require.Equal(t, int64(9), end)
	require.Equal(t, int64(0), round)
	// invalidate the current sequence cache.
	tk.MustQuery("select setval(seq, 10)").Check(testkit.Rows("10"))
	tk.MustQuery("select lastval(seq)").Check(testkit.Rows("3"))
	// trigger the next sequence cache.
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("12"))
	sequenceTable = external.GetTableByName(t, tk, "test", "seq")
	tc, ok = sequenceTable.(*tables.TableCommon)
	require.Equal(t, true, ok)
	_, end, round = tc.GetSequenceCommon().GetSequenceBaseEndRound()
	require.Equal(t, int64(14), end)
	require.Equal(t, int64(0), round)
	// invalidate the current sequence cache.
	tk.MustQuery("select setval(seq, 13)").Check(testkit.Rows("13"))
	tk.MustQuery("select lastval(seq)").Check(testkit.Rows("12"))
	// trigger the next sequence cache.
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("1"))
	sequenceTable = external.GetTableByName(t, tk, "test", "seq")
	tc, ok = sequenceTable.(*tables.TableCommon)
	require.Equal(t, true, ok)
	_, end, round = tc.GetSequenceCommon().GetSequenceBaseEndRound()
	require.Equal(t, int64(7), end)
	require.Equal(t, int64(1), round)
	tk.MustQuery("select lastval(seq)").Check(testkit.Rows("1"))

	// test lastval in negative-growth sequence cycle and cache.
	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("create sequence seq increment -3 start -2 maxvalue 10 minvalue -10 cache 3 cycle")
	tk.MustQuery("select lastval(seq)").Check(testkit.Rows("<nil>"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("-2"))
	sequenceTable = external.GetTableByName(t, tk, "test", "seq")
	tc, ok = sequenceTable.(*tables.TableCommon)
	require.Equal(t, true, ok)
	_, end, round = tc.GetSequenceCommon().GetSequenceBaseEndRound()
	require.Equal(t, int64(-8), end)
	require.Equal(t, int64(0), round)
	// invalidate the current sequence cache.
	tk.MustQuery("select setval(seq, -8)").Check(testkit.Rows("-8"))
	tk.MustQuery("select lastval(seq)").Check(testkit.Rows("-2"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("10"))
	sequenceTable = external.GetTableByName(t, tk, "test", "seq")
	tc, ok = sequenceTable.(*tables.TableCommon)
	require.Equal(t, true, ok)
	_, end, round = tc.GetSequenceCommon().GetSequenceBaseEndRound()
	require.Equal(t, int64(4), end)
	require.Equal(t, int64(1), round)
	tk.MustQuery("select lastval(seq)").Check(testkit.Rows("10"))

	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("create sequence seq increment -1 start 1 maxvalue 10 minvalue -10 cache 3 cycle")
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("1"))
	tk.MustQuery("select setval(seq, -8)").Check(testkit.Rows("-8"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("-9"))
	sequenceTable = external.GetTableByName(t, tk, "test", "seq")
	tc, ok = sequenceTable.(*tables.TableCommon)
	require.Equal(t, true, ok)
	_, end, round = tc.GetSequenceCommon().GetSequenceBaseEndRound()
	require.Equal(t, int64(-10), end)
	require.Equal(t, int64(0), round)

	// Test the sequence seek formula will overflow Int64.
	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("create sequence seq increment 2 start -9223372036854775807 maxvalue 9223372036854775806 minvalue -9223372036854775807 cache 2 cycle")
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("-9223372036854775807"))
	tk.MustQuery("select setval(seq, 9223372036854775800)").Check(testkit.Rows("9223372036854775800"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("9223372036854775801"))

	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("create sequence seq increment -2 start 9223372036854775806 maxvalue 9223372036854775806 minvalue -9223372036854775807 cache 2 cycle")
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("9223372036854775806"))
	tk.MustQuery("select setval(seq, -9223372036854775800)").Check(testkit.Rows("-9223372036854775800"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("-9223372036854775802"))

	// Test sequence function with wrong object name.
	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("drop table if exists seq")
	tk.MustExec("drop view if exists seq")
	tk.MustExec("drop sequence if exists seq1")
	tk.MustExec("drop table if exists seq1")
	tk.MustExec("drop view if exists seq1")
	tk.MustExec("create table seq(a int)")
	err = tk.ExecToErr("select nextval(seq)")
	require.Error(t, err)
	require.EqualError(t, err, "[schema:1347]'test.seq' is not SEQUENCE")
	err = tk.ExecToErr("select lastval(seq)")
	require.Error(t, err)
	require.EqualError(t, err, "[schema:1347]'test.seq' is not SEQUENCE")
	err = tk.ExecToErr("select setval(seq, 10)")
	require.Error(t, err)
	require.EqualError(t, err, "[schema:1347]'test.seq' is not SEQUENCE")

	tk.MustExec("create view seq1 as select * from seq")
	err = tk.ExecToErr("select nextval(seq1)")
	require.Error(t, err)
	require.EqualError(t, err, "[schema:1347]'test.seq1' is not SEQUENCE")
	err = tk.ExecToErr("select lastval(seq1)")
	require.Error(t, err)
	require.EqualError(t, err, "[schema:1347]'test.seq1' is not SEQUENCE")
	err = tk.ExecToErr("select setval(seq1, 10)")
	require.Error(t, err)
	require.EqualError(t, err, "[schema:1347]'test.seq1' is not SEQUENCE")
	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("drop table if exists seq")
	tk.MustExec("drop view if exists seq")
	tk.MustExec("drop sequence if exists seq1")
	tk.MustExec("drop table if exists seq1")
	tk.MustExec("drop view if exists seq1")

	// test a bug found in ticase.
	tk.MustExec("create sequence seq")
	tk.MustQuery("select setval(seq, 10)").Check(testkit.Rows("10"))
	tk.MustQuery("select setval(seq, 5)").Check(testkit.Rows("<nil>"))
	tk.MustExec("drop sequence seq")
	tk.MustExec("create sequence seq increment=-1")
	tk.MustQuery("select setval(seq, -10)").Check(testkit.Rows("-10"))
	tk.MustQuery("select setval(seq, -5)").Check(testkit.Rows("<nil>"))
	tk.MustExec("drop sequence seq")

	// test the current value already satisfied setval in other session.
	tk.MustExec("create sequence seq")
	tk.MustQuery("select setval(seq, 100)").Check(testkit.Rows("100"))
	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	tk1 := testkit.NewTestKit(t, store)
	tk1.SetSession(se)
	tk1.MustExec("use test")
	tk1.MustQuery("select setval(seq, 50)").Check(testkit.Rows("<nil>"))
	tk1.MustQuery("select nextval(seq)").Check(testkit.Rows("101"))
	tk1.MustQuery("select setval(seq, 100)").Check(testkit.Rows("<nil>"))
	tk1.MustQuery("select setval(seq, 101)").Check(testkit.Rows("<nil>"))
	tk1.MustQuery("select setval(seq, 102)").Check(testkit.Rows("102"))
	tk.MustExec("drop sequence seq")

	tk.MustExec("create sequence seq increment=-1")
	tk.MustQuery("select setval(seq, -100)").Check(testkit.Rows("-100"))
	tk1.MustQuery("select setval(seq, -50)").Check(testkit.Rows("<nil>"))
	tk1.MustQuery("select nextval(seq)").Check(testkit.Rows("-101"))
	tk1.MustQuery("select setval(seq, -100)").Check(testkit.Rows("<nil>"))
	tk1.MustQuery("select setval(seq, -101)").Check(testkit.Rows("<nil>"))
	tk1.MustQuery("select setval(seq, -102)").Check(testkit.Rows("-102"))
	tk.MustExec("drop sequence seq")

	// test the sequence name preprocess.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create sequence seq")
	tk.MustExec("create table t(a int)")
	tk.MustExec("insert into t values(1),(2)")
	tk.MustQuery("select nextval(seq), t.a from t").Check(testkit.Rows("1 1", "2 2"))
	err = tk.ExecToErr("select nextval(t), t.a from t")
	require.Error(t, err)
	require.EqualError(t, err, "[schema:1347]'test.t' is not SEQUENCE")
	err = tk.ExecToErr("select nextval(seq), nextval(t), t.a from t")
	require.Error(t, err)
	require.EqualError(t, err, "[schema:1347]'test.t' is not SEQUENCE")
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("3"))
	tk.MustExec("drop sequence seq")
	tk.MustExec("drop table t")
}

// before this PR:
// single insert consume: 50.498672ms
// after this PR:
// single insert consume: 33.213615ms
func BenchmarkInsertCacheDefaultExpr(b *testing.B) {
	store := testkit.CreateMockStore(b)
	session.SetSchemaLease(600 * time.Millisecond)
	tk := testkit.NewTestKit(b, store)
	tk.MustExec("use test")
	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create sequence seq")
	tk.MustExec("create table t(a int default next value for seq)")
	sql := "insert into t values "
	for i := 0; i < 1000; i++ {
		if i == 0 {
			sql += "()"
		} else {
			sql += ",()"
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tk.MustExec(sql)
	}
}
