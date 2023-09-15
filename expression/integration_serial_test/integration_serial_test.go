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

package integration_serial_test

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/parser/terror"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/types"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

func TestTimeBuiltin(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	originSQLMode := tk.Session().GetSessionVars().StrictSQLMode
	tk.Session().GetSessionVars().StrictSQLMode = true
	defer func() {
		tk.Session().GetSessionVars().StrictSQLMode = originSQLMode
	}()
	tk.MustExec("use test")

	// for makeDate
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b double, c datetime, d time, e char(20), f bit(10))")
	tk.MustExec(`insert into t values(1, 1.1, "2017-01-01 12:01:01", "12:01:01", "abcdef", 0b10101)`)
	result := tk.MustQuery("select makedate(a,a), makedate(b,b), makedate(c,c), makedate(d,d), makedate(e,e), makedate(f,f), makedate(null,null), makedate(a,b) from t")
	result.Check(testkit.Rows("2001-01-01 2001-01-01 <nil> <nil> <nil> 2021-01-21 <nil> 2001-01-01"))

	// for date
	result = tk.MustQuery(`select date("2019-09-12"), date("2019-09-12 12:12:09"), date("2019-09-12 12:12:09.121212");`)
	result.Check(testkit.Rows("2019-09-12 2019-09-12 2019-09-12"))
	result = tk.MustQuery(`select date("0000-00-00"), date("0000-00-00 12:12:09"), date("0000-00-00 00:00:00.121212"), date("0000-00-00 00:00:00.000000");`)
	result.Check(testkit.Rows("<nil> 0000-00-00 0000-00-00 <nil>"))
	result = tk.MustQuery(`select date("aa"), date(12.1), date("");`)
	result.Check(testkit.Rows("<nil> <nil> <nil>"))

	// for year
	result = tk.MustQuery(`select year("2013-01-09"), year("2013-00-09"), year("000-01-09"), year("1-01-09"), year("20131-01-09"), year(null);`)
	result.Check(testkit.Rows("2013 2013 0 2001 <nil> <nil>"))
	result = tk.MustQuery(`select year("2013-00-00"), year("2013-00-00 00:00:00"), year("0000-00-00 12:12:12"), year("2017-00-00 12:12:12");`)
	result.Check(testkit.Rows("2013 2013 0 2017"))
	result = tk.MustQuery(`select year("aa"), year(2013), year(2012.09), year("1-01"), year("-09");`)
	result.Check(testkit.Rows("<nil> <nil> <nil> <nil> <nil>"))
	tk.MustExec(`drop table if exists t`)
	tk.MustExec(`create table t(a bigint)`)
	_, err := tk.Exec(`insert into t select year("aa")`)
	require.Error(t, err)
	require.True(t, terror.ErrorEqual(err, types.ErrWrongValue), "err %v", err)
	tk.MustExec(`set sql_mode='STRICT_TRANS_TABLES'`) // without zero date
	tk.MustExec(`insert into t select year("0000-00-00 00:00:00")`)
	tk.MustExec(`set sql_mode="NO_ZERO_DATE";`) // with zero date
	tk.MustExec(`insert into t select year("0000-00-00 00:00:00")`)
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1292|Incorrect datetime value: '0000-00-00 00:00:00.000000'"))
	tk.MustExec(`set sql_mode="NO_ZERO_DATE,STRICT_TRANS_TABLES";`)
	_, err = tk.Exec(`insert into t select year("0000-00-00 00:00:00");`)
	require.Error(t, err)
	require.True(t, types.ErrWrongValue.Equal(err), "err %v", err)

	tk.MustExec(`insert into t select 1`)
	tk.MustExec(`set sql_mode="STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION";`)
	_, err = tk.Exec(`update t set a = year("aa")`)
	require.True(t, terror.ErrorEqual(err, types.ErrWrongValue), "err %v", err)
	_, err = tk.Exec(`delete from t where a = year("aa")`)
	// Only `code` can be used to compare because the error `class` information
	// will be lost after expression push-down
	require.Equal(t, types.ErrWrongValue.Code(), errors.Cause(err).(*terror.Error).Code(), "err %v", err)

	// for month
	result = tk.MustQuery(`select month("2013-01-09"), month("2013-00-09"), month("000-01-09"), month("1-01-09"), month("20131-01-09"), month(null);`)
	result.Check(testkit.Rows("1 0 1 1 <nil> <nil>"))
	result = tk.MustQuery(`select month("2013-00-00"), month("2013-00-00 00:00:00"), month("0000-00-00 12:12:12"), month("2017-00-00 12:12:12");`)
	result.Check(testkit.Rows("0 0 0 0"))
	result = tk.MustQuery(`select month("aa"), month(2013), month(2012.09), month("1-01"), month("-09");`)
	result.Check(testkit.Rows("<nil> <nil> <nil> <nil> <nil>"))
	result = tk.MustQuery(`select month("2013-012-09"), month("2013-0000000012-09"), month("2013-30-09"), month("000-41-09");`)
	result.Check(testkit.Rows("12 12 <nil> <nil>"))
	tk.MustExec(`drop table if exists t`)
	tk.MustExec(`create table t(a bigint)`)
	_, err = tk.Exec(`insert into t select month("aa")`)
	require.Error(t, err)
	require.True(t, terror.ErrorEqual(err, types.ErrWrongValue), "err: %v", err)
	tk.MustExec(`insert into t select month("0000-00-00 00:00:00")`)
	tk.MustExec(`set sql_mode="NO_ZERO_DATE";`)
	tk.MustExec(`insert into t select month("0000-00-00 00:00:00")`)
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1292|Incorrect datetime value: '0000-00-00 00:00:00.000000'"))
	tk.MustExec(`set sql_mode="NO_ZERO_DATE,STRICT_TRANS_TABLES";`)
	_, err = tk.Exec(`insert into t select month("0000-00-00 00:00:00");`)
	require.Error(t, err)
	require.True(t, types.ErrWrongValue.Equal(err), "err: %v", err)
	tk.MustExec(`insert into t select 1`)
	tk.MustExec(`set sql_mode="STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION";`)
	tk.MustExec(`insert into t select 1`)
	_, err = tk.Exec(`update t set a = month("aa")`)
	require.True(t, terror.ErrorEqual(err, types.ErrWrongValue))
	_, err = tk.Exec(`delete from t where a = month("aa")`)
	require.Equal(t, types.ErrWrongValue.Code(), errors.Cause(err).(*terror.Error).Code(), "err %v", err)

	// for week
	result = tk.MustQuery(`select week("2012-12-22"), week("2012-12-22", -2), week("2012-12-22", 0), week("2012-12-22", 1), week("2012-12-22", 2), week("2012-12-22", 200);`)
	result.Check(testkit.Rows("51 51 51 51 51 51"))
	result = tk.MustQuery(`select week("2008-02-20"), week("2008-02-20", 0), week("2008-02-20", 1), week("2009-02-20", 2), week("2008-02-20", 3), week("2008-02-20", 4);`)
	result.Check(testkit.Rows("7 7 8 7 8 8"))
	result = tk.MustQuery(`select week("2008-02-20", 5), week("2008-02-20", 6), week("2009-02-20", 7), week("2008-02-20", 8), week("2008-02-20", 9);`)
	result.Check(testkit.Rows("7 8 7 7 8"))
	result = tk.MustQuery(`select week("aa", 1), week(null, 2), week(11, 2), week(12.99, 2);`)
	result.Check(testkit.Rows("<nil> <nil> <nil> <nil>"))
	result = tk.MustQuery(`select week("aa"), week(null), week(11), week(12.99);`)
	result.Check(testkit.Rows("<nil> <nil> <nil> <nil>"))
	tk.MustExec(`drop table if exists t`)
	tk.MustExec(`create table t(a datetime)`)
	_, err = tk.Exec(`insert into t select week("aa", 1)`)
	require.Error(t, err)
	require.True(t, terror.ErrorEqual(err, types.ErrWrongValue))
	tk.MustExec(`insert into t select now()`)
	_, err = tk.Exec(`update t set a = week("aa", 1)`)
	require.True(t, terror.ErrorEqual(err, types.ErrWrongValue))
	_, err = tk.Exec(`delete from t where a = week("aa", 1)`)
	require.Equal(t, types.ErrWrongValue.Code(), errors.Cause(err).(*terror.Error).Code(), "err %v", err)

	// for weekofyear
	result = tk.MustQuery(`select weekofyear("2012-12-22"), weekofyear("2008-02-20"), weekofyear("aa"), weekofyear(null), weekofyear(11), weekofyear(12.99);`)
	result.Check(testkit.Rows("51 8 <nil> <nil> <nil> <nil>"))
	tk.MustExec(`drop table if exists t`)
	tk.MustExec(`create table t(a bigint)`)
	_, err = tk.Exec(`insert into t select weekofyear("aa")`)
	require.Error(t, err)
	require.True(t, terror.ErrorEqual(err, types.ErrWrongValue))

	tk.MustExec(`insert into t select 1`)
	_, err = tk.Exec(`update t set a = weekofyear("aa")`)
	require.True(t, terror.ErrorEqual(err, types.ErrWrongValue))
	_, err = tk.Exec(`delete from t where a = weekofyear("aa")`)
	require.Equal(t, types.ErrWrongValue.Code(), errors.Cause(err).(*terror.Error).Code(), "err %v", err)

	// for weekday
	result = tk.MustQuery(`select weekday("2012-12-20"), weekday("2012-12-21"), weekday("2012-12-22"), weekday("2012-12-23"), weekday("2012-12-24"), weekday("2012-12-25"), weekday("2012-12-26"), weekday("2012-12-27");`)
	result.Check(testkit.Rows("3 4 5 6 0 1 2 3"))
	result = tk.MustQuery(`select weekday("2012-12-90"), weekday("0000-00-00"), weekday("aa"), weekday(null), weekday(11), weekday(12.99);`)
	result.Check(testkit.Rows("<nil> <nil> <nil> <nil> <nil> <nil>"))

	// for quarter
	result = tk.MustQuery(`select quarter("2012-00-20"), quarter("2012-01-21"), quarter("2012-03-22"), quarter("2012-05-23"), quarter("2012-08-24"), quarter("2012-09-25"), quarter("2012-11-26"), quarter("2012-12-27");`)
	result.Check(testkit.Rows("0 1 1 2 3 3 4 4"))
	result = tk.MustQuery(`select quarter("2012-14-20"), quarter("aa"), quarter(null), quarter(11), quarter(12.99);`)
	result.Check(testkit.Rows("<nil> <nil> <nil> <nil> <nil>"))
	result = tk.MustQuery(`select quarter("0000-00-00"), quarter("0000-00-00 00:00:00");`)
	result.Check(testkit.Rows("0 0"))
	tk.MustQuery("show warnings").Check(testkit.Rows())
	result = tk.MustQuery(`select quarter(0), quarter(0.0), quarter(0e1), quarter(0.00);`)
	result.Check(testkit.Rows("0 0 0 0"))
	tk.MustQuery("show warnings").Check(testkit.Rows())

	// for from_days
	result = tk.MustQuery(`select from_days(0), from_days(-199), from_days(1111), from_days(120), from_days(1), from_days(1111111), from_days(9999999), from_days(22222);`)
	result.Check(testkit.Rows("0000-00-00 0000-00-00 0003-01-16 0000-00-00 0000-00-00 3042-02-13 0000-00-00 0060-11-03"))
	result = tk.MustQuery(`select from_days("2012-14-20"), from_days("111a"), from_days("aa"), from_days(null), from_days("123asf"), from_days(12.99);`)
	result.Check(testkit.Rows("0005-07-05 0000-00-00 0000-00-00 <nil> 0000-00-00 0000-00-00"))

	// Fix issue #3923
	result = tk.MustQuery("select timediff(cast('2004-12-30 12:00:00' as time), '12:00:00');")
	result.Check(testkit.Rows("00:00:00"))
	result = tk.MustQuery("select timediff('12:00:00', cast('2004-12-30 12:00:00' as time));")
	result.Check(testkit.Rows("00:00:00"))
	result = tk.MustQuery("select timediff(cast('2004-12-30 12:00:00' as time), '2004-12-30 12:00:00');")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select timediff('2004-12-30 12:00:00', cast('2004-12-30 12:00:00' as time));")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select timediff(cast('2004-12-30 12:00:01' as datetime), '2004-12-30 12:00:00');")
	result.Check(testkit.Rows("00:00:01"))
	result = tk.MustQuery("select timediff('2004-12-30 12:00:00', cast('2004-12-30 12:00:01' as datetime));")
	result.Check(testkit.Rows("-00:00:01"))
	result = tk.MustQuery("select timediff(cast('2004-12-30 12:00:01' as time), '-34 00:00:00');")
	result.Check(testkit.Rows("828:00:01"))
	result = tk.MustQuery("select timediff('-34 00:00:00', cast('2004-12-30 12:00:01' as time));")
	result.Check(testkit.Rows("-828:00:01"))
	result = tk.MustQuery("select timediff(cast('2004-12-30 12:00:01' as datetime), cast('2004-12-30 11:00:01' as datetime));")
	result.Check(testkit.Rows("01:00:00"))
	result = tk.MustQuery("select timediff(cast('2004-12-30 12:00:01' as datetime), '2004-12-30 12:00:00.1');")
	result.Check(testkit.Rows("00:00:00.9"))
	result = tk.MustQuery("select timediff('2004-12-30 12:00:00.1', cast('2004-12-30 12:00:01' as datetime));")
	result.Check(testkit.Rows("-00:00:00.9"))
	result = tk.MustQuery("select timediff(cast('2004-12-30 12:00:01' as datetime), '-34 124:00:00');")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select timediff('-34 124:00:00', cast('2004-12-30 12:00:01' as datetime));")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select timediff(cast('2004-12-30 12:00:01' as time), '-34 124:00:00');")
	result.Check(testkit.Rows("838:59:59"))
	result = tk.MustQuery("select timediff('-34 124:00:00', cast('2004-12-30 12:00:01' as time));")
	result.Check(testkit.Rows("-838:59:59"))
	result = tk.MustQuery("select timediff(cast('2004-12-30' as datetime), '12:00:00');")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select timediff('12:00:00', cast('2004-12-30' as datetime));")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select timediff('12:00:00', '-34 12:00:00');")
	result.Check(testkit.Rows("838:59:59"))
	result = tk.MustQuery("select timediff('12:00:00', '34 12:00:00');")
	result.Check(testkit.Rows("-816:00:00"))
	result = tk.MustQuery("select timediff('2014-1-2 12:00:00', '-34 12:00:00');")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select timediff('-34 12:00:00', '2014-1-2 12:00:00');")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select timediff('2014-1-2 12:00:00', '12:00:00');")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select timediff('12:00:00', '2014-1-2 12:00:00');")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select timediff('2014-1-2 12:00:00', '2014-1-1 12:00:00');")
	result.Check(testkit.Rows("24:00:00"))
	tk.MustQuery("select timediff(cast('10:10:10' as time), cast('10:10:11' as time))").Check(testkit.Rows("-00:00:01"))

	result = tk.MustQuery("select timestampadd(MINUTE, 1, '2003-01-02'), timestampadd(WEEK, 1, '2003-01-02 23:59:59')" +
		", timestampadd(MICROSECOND, 1, 950501);")
	result.Check(testkit.Rows("2003-01-02 00:01:00 2003-01-09 23:59:59 1995-05-01 00:00:00.000001"))
	result = tk.MustQuery("select timestampadd(day, 2, 950501), timestampadd(MINUTE, 37.5,'2003-01-02'), timestampadd(MINUTE, 37.49,'2003-01-02')," +
		" timestampadd(YeAr, 1, '2003-01-02');")
	result.Check(testkit.Rows("1995-05-03 00:00:00 2003-01-02 00:38:00 2003-01-02 00:37:00 2004-01-02 00:00:00"))
	result = tk.MustQuery("select to_seconds(950501), to_seconds('2009-11-29'), to_seconds('2009-11-29 13:43:32'), to_seconds('09-11-29 13:43:32');")
	result.Check(testkit.Rows("62966505600 63426672000 63426721412 63426721412"))
	result = tk.MustQuery("select to_days(950501), to_days('2007-10-07'), to_days('2007-10-07 00:00:59'), to_days('0000-01-01')")
	result.Check(testkit.Rows("728779 733321 733321 1"))

	result = tk.MustQuery("select last_day('2003-02-05'), last_day('2004-02-05'), last_day('2004-01-01 01:01:01'), last_day(950501);")
	result.Check(testkit.Rows("2003-02-28 2004-02-29 2004-01-31 1995-05-31"))

	tk.MustExec("SET SQL_MODE='';")
	result = tk.MustQuery("select last_day('0000-00-00');")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select to_days('0000-00-00');")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select to_seconds('0000-00-00');")
	result.Check(testkit.Rows("<nil>"))

	result = tk.MustQuery("select timestamp('2003-12-31'), timestamp('2003-12-31 12:00:00','12:00:00');")
	result.Check(testkit.Rows("2003-12-31 00:00:00 2004-01-01 00:00:00"))
	result = tk.MustQuery("select timestamp(20170118123950.123), timestamp(20170118123950.999);")
	result.Check(testkit.Rows("2017-01-18 12:39:50.123 2017-01-18 12:39:50.999"))
	// Issue https://github.com/pingcap/tidb/issues/20003
	result = tk.MustQuery("select timestamp(0.0001, 0.00001);")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select timestamp('2003-12-31', '01:01:01.01'), timestamp('2003-12-31 12:34', '01:01:01.01')," +
		" timestamp('2008-12-31','00:00:00.0'), timestamp('2008-12-31 00:00:00.000');")

	tk.MustQuery(`select timestampadd(second, 1, cast("2001-01-01" as date))`).Check(testkit.Rows("2001-01-01 00:00:01"))
	tk.MustQuery(`select timestampadd(hour, 1, cast("2001-01-01" as date))`).Check(testkit.Rows("2001-01-01 01:00:00"))
	tk.MustQuery(`select timestampadd(day, 1, cast("2001-01-01" as date))`).Check(testkit.Rows("2001-01-02"))
	tk.MustQuery(`select timestampadd(month, 1, cast("2001-01-01" as date))`).Check(testkit.Rows("2001-02-01"))
	tk.MustQuery(`select timestampadd(year, 1, cast("2001-01-01" as date))`).Check(testkit.Rows("2002-01-01"))
	tk.MustQuery(`select timestampadd(second, 1, cast("2001-01-01" as datetime))`).Check(testkit.Rows("2001-01-01 00:00:01"))
	tk.MustQuery(`select timestampadd(hour, 1, cast("2001-01-01" as datetime))`).Check(testkit.Rows("2001-01-01 01:00:00"))
	tk.MustQuery(`select timestampadd(day, 1, cast("2001-01-01" as datetime))`).Check(testkit.Rows("2001-01-02 00:00:00"))
	tk.MustQuery(`select timestampadd(month, 1, cast("2001-01-01" as datetime))`).Check(testkit.Rows("2001-02-01 00:00:00"))
	tk.MustQuery(`select timestampadd(year, 1, cast("2001-01-01" as datetime))`).Check(testkit.Rows("2002-01-01 00:00:00"))

	result.Check(testkit.Rows("2003-12-31 01:01:01.01 2003-12-31 13:35:01.01 2008-12-31 00:00:00.0 2008-12-31 00:00:00.000"))
	result = tk.MustQuery("select timestamp('2003-12-31', 1), timestamp('2003-12-31', -1);")
	result.Check(testkit.Rows("2003-12-31 00:00:01 2003-12-30 23:59:59"))
	result = tk.MustQuery("select timestamp('2003-12-31', '2000-12-12 01:01:01.01'), timestamp('2003-14-31','01:01:01.01');")
	result.Check(testkit.Rows("<nil> <nil>"))

	result = tk.MustQuery("select TIMESTAMPDIFF(MONTH,'2003-02-01','2003-05-01'), TIMESTAMPDIFF(yEaR,'2002-05-01', " +
		"'2001-01-01'), TIMESTAMPDIFF(minute,binary('2003-02-01'),'2003-05-01 12:05:55'), TIMESTAMPDIFF(day," +
		"'1995-05-02', 950501);")
	result.Check(testkit.Rows("3 -1 128885 -1"))

	result = tk.MustQuery("select datediff('2007-12-31 23:59:59','2007-12-30'), datediff('2010-11-30 23:59:59', " +
		"'2010-12-31'), datediff(950501,'2016-01-13'), datediff(950501.9,'2016-01-13'), datediff(binary(950501), '2016-01-13');")
	result.Check(testkit.Rows("1 -31 -7562 -7562 -7562"))
	result = tk.MustQuery("select datediff('0000-01-01','0001-01-01'), datediff('0001-00-01', '0001-00-01'), datediff('0001-01-00','0001-01-00'), datediff('2017-01-01','2017-01-01');")
	result.Check(testkit.Rows("-365 <nil> <nil> 0"))

	// for ADDTIME
	result = tk.MustQuery("select addtime('01:01:11', '00:00:01.013'), addtime('01:01:11.00', '00:00:01'), addtime" +
		"('2017-01-01 01:01:11.12', '00:00:01'), addtime('2017-01-01 01:01:11.12', '00:00:01.88');")
	result.Check(testkit.Rows("01:01:12.013000 01:01:12 2017-01-01 01:01:12.120000 2017-01-01 01:01:13"))
	result = tk.MustQuery("select addtime(cast('01:01:11' as time(4)), '00:00:01.013'), addtime(cast('01:01:11.00' " +
		"as datetime(3)), '00:00:01')," + " addtime(cast('2017-01-01 01:01:11.12' as date), '00:00:01'), addtime(cast" +
		"(cast('2017-01-01 01:01:11.12' as date) as datetime(2)), '00:00:01.88');")
	result.Check(testkit.Rows("01:01:12.0130 2001-01-11 00:00:01.000 00:00:01 2017-01-01 00:00:01.88"))
	result = tk.MustQuery("select addtime('2017-01-01 01:01:01', 5), addtime('2017-01-01 01:01:01', -5), addtime('2017-01-01 01:01:01', 0.0), addtime('2017-01-01 01:01:01', 1.34);")
	result.Check(testkit.Rows("2017-01-01 01:01:06 2017-01-01 01:00:56 2017-01-01 01:01:01 2017-01-01 01:01:02.340000"))
	result = tk.MustQuery("select addtime(cast('01:01:11.00' as datetime(3)), cast('00:00:01' as time)), addtime(cast('01:01:11.00' as datetime(3)), cast('00:00:01' as time(5)))")
	result.Check(testkit.Rows("2001-01-11 00:00:01.000 2001-01-11 00:00:01.00000"))
	result = tk.MustQuery("select addtime(cast('01:01:11.00' as date), cast('00:00:01' as time));")
	result.Check(testkit.Rows("00:00:01"))
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a datetime, b timestamp, c time)")
	tk.MustExec(`insert into t values("2017-01-01 12:30:31", "2017-01-01 12:30:31", "01:01:01")`)
	result = tk.MustQuery("select addtime(a, b), addtime(cast(a as date), b), addtime(b,a), addtime(a,c), addtime(b," +
		"c), addtime(c,a), addtime(c,b)" +
		" from t;")
	result.Check(testkit.Rows("<nil> <nil> <nil> 2017-01-01 13:31:32 2017-01-01 13:31:32 <nil> <nil>"))
	result = tk.MustQuery("select addtime('01:01:11', cast('1' as time))")
	result.Check(testkit.Rows("01:01:12"))
	tk.MustQuery("select addtime(cast(null as char(20)), cast('1' as time))").Check(testkit.Rows("<nil>"))
	require.NoError(t, tk.QueryToErr(`select addtime("01:01:11", cast('sdf' as time))`))
	tk.MustQuery(`select addtime("01:01:11", cast(null as char(20)))`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select addtime(cast(1 as time), cast(1 as time))`).Check(testkit.Rows("00:00:02"))
	tk.MustQuery(`select addtime(cast(null as time), cast(1 as time))`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select addtime(cast(1 as time), cast(null as time))`).Check(testkit.Rows("<nil>"))

	// for SUBTIME
	result = tk.MustQuery("select subtime('01:01:11', '00:00:01.013'), subtime('01:01:11.00', '00:00:01'), subtime" +
		"('2017-01-01 01:01:11.12', '00:00:01'), subtime('2017-01-01 01:01:11.12', '00:00:01.88');")
	result.Check(testkit.Rows("01:01:09.987000 01:01:10 2017-01-01 01:01:10.120000 2017-01-01 01:01:09.240000"))
	result = tk.MustQuery("select subtime(cast('01:01:11' as time(4)), '00:00:01.013'), subtime(cast('01:01:11.00' " +
		"as datetime(3)), '00:00:01')," + " subtime(cast('2017-01-01 01:01:11.12' as date), '00:00:01'), subtime(cast" +
		"(cast('2017-01-01 01:01:11.12' as date) as datetime(2)), '00:00:01.88');")
	result.Check(testkit.Rows("01:01:09.9870 2001-01-10 23:59:59.000 -00:00:01 2016-12-31 23:59:58.12"))
	result = tk.MustQuery("select subtime('2017-01-01 01:01:01', 5), subtime('2017-01-01 01:01:01', -5), subtime('2017-01-01 01:01:01', 0.0), subtime('2017-01-01 01:01:01', 1.34);")
	result.Check(testkit.Rows("2017-01-01 01:00:56 2017-01-01 01:01:06 2017-01-01 01:01:01 2017-01-01 01:00:59.660000"))
	result = tk.MustQuery("select subtime('01:01:11', '0:0:1.013'), subtime('01:01:11.00', '0:0:1'), subtime('2017-01-01 01:01:11.12', '0:0:1'), subtime('2017-01-01 01:01:11.12', '0:0:1.120000');")
	result.Check(testkit.Rows("01:01:09.987000 01:01:10 2017-01-01 01:01:10.120000 2017-01-01 01:01:10"))
	result = tk.MustQuery("select subtime(cast('01:01:11.00' as datetime(3)), cast('00:00:01' as time)), subtime(cast('01:01:11.00' as datetime(3)), cast('00:00:01' as time(5)))")
	result.Check(testkit.Rows("2001-01-10 23:59:59.000 2001-01-10 23:59:59.00000"))
	result = tk.MustQuery("select subtime(cast('01:01:11.00' as date), cast('00:00:01' as time));")
	result.Check(testkit.Rows("-00:00:01"))
	result = tk.MustQuery("select subtime(a, b), subtime(cast(a as date), b), subtime(b,a), subtime(a,c), subtime(b," +
		"c), subtime(c,a), subtime(c,b) from t;")
	result.Check(testkit.Rows("<nil> <nil> <nil> 2017-01-01 11:29:30 2017-01-01 11:29:30 <nil> <nil>"))
	tk.MustQuery("select subtime(cast('10:10:10' as time), cast('9:10:10' as time))").Check(testkit.Rows("01:00:00"))
	tk.MustQuery("select subtime('10:10:10', cast('9:10:10' as time))").Check(testkit.Rows("01:00:00"))

	// SUBTIME issue #31868
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a DATETIME(6))")
	tk.MustExec(`insert into t values ("1000-01-01 01:00:00.000000"), ("1000-01-01 01:00:00.000001")`)
	tk.MustQuery(`SELECT SUBTIME(a, '00:00:00.000001') FROM t ORDER BY a;`).Check(testkit.Rows("1000-01-01 00:59:59.999999", "1000-01-01 01:00:00.000000"))
	tk.MustQuery(`SELECT SUBTIME(a, '10:00:00.000001') FROM t ORDER BY a;`).Check(testkit.Rows("0999-12-31 14:59:59.999999", "0999-12-31 15:00:00.000000"))

	// ADDTIME & SUBTIME issue #5966
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a datetime, b timestamp, c time, d date, e bit(1))")
	tk.MustExec(`insert into t values("2017-01-01 12:30:31", "2017-01-01 12:30:31", "01:01:01", "2017-01-01", 0b1)`)

	result = tk.MustQuery("select addtime(a, e), addtime(b, e), addtime(c, e), addtime(d, e) from t")
	result.Check(testkit.Rows("<nil> <nil> <nil> <nil>"))
	result = tk.MustQuery("select addtime('2017-01-01 01:01:01', 0b1), addtime('2017-01-01', b'1'), addtime('01:01:01', 0b1011)")
	result.Check(testkit.Rows("<nil> <nil> <nil>"))
	result = tk.MustQuery("select addtime('2017-01-01', 1), addtime('2017-01-01 01:01:01', 1), addtime(cast('2017-01-01' as date), 1)")
	result.Check(testkit.Rows("2017-01-01 00:00:01 2017-01-01 01:01:02 00:00:01"))
	result = tk.MustQuery("select subtime(a, e), subtime(b, e), subtime(c, e), subtime(d, e) from t")
	result.Check(testkit.Rows("<nil> <nil> <nil> <nil>"))
	result = tk.MustQuery("select subtime('2017-01-01 01:01:01', 0b1), subtime('2017-01-01', b'1'), subtime('01:01:01', 0b1011)")
	result.Check(testkit.Rows("<nil> <nil> <nil>"))
	result = tk.MustQuery("select subtime('2017-01-01', 1), subtime('2017-01-01 01:01:01', 1), subtime(cast('2017-01-01' as date), 1)")
	result.Check(testkit.Rows("2016-12-31 23:59:59 2017-01-01 01:01:00 -00:00:01"))

	result = tk.MustQuery("select addtime(-32073, 0), addtime(0, -32073);")
	result.Check(testkit.Rows("<nil> <nil>"))
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|",
		"Warning|1292|Truncated incorrect time value: '-32073'",
		"Warning|1292|Truncated incorrect time value: '-32073'"))
	result = tk.MustQuery("select addtime(-32073, c), addtime(c, -32073) from t;")
	result.Check(testkit.Rows("<nil> <nil>"))
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|",
		"Warning|1292|Truncated incorrect time value: '-32073'",
		"Warning|1292|Truncated incorrect time value: '-32073'"))
	result = tk.MustQuery("select addtime(a, -32073), addtime(b, -32073), addtime(d, -32073) from t;")
	result.Check(testkit.Rows("<nil> <nil> <nil>"))
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|",
		"Warning|1292|Truncated incorrect time value: '-32073'",
		"Warning|1292|Truncated incorrect time value: '-32073'",
		"Warning|1292|Truncated incorrect time value: '-32073'"))

	result = tk.MustQuery("select subtime(-32073, 0), subtime(0, -32073);")
	result.Check(testkit.Rows("<nil> <nil>"))
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|",
		"Warning|1292|Truncated incorrect time value: '-32073'",
		"Warning|1292|Truncated incorrect time value: '-32073'"))
	result = tk.MustQuery("select subtime(-32073, c), subtime(c, -32073) from t;")
	result.Check(testkit.Rows("<nil> <nil>"))
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|",
		"Warning|1292|Truncated incorrect time value: '-32073'",
		"Warning|1292|Truncated incorrect time value: '-32073'"))
	result = tk.MustQuery("select subtime(a, -32073), subtime(b, -32073), subtime(d, -32073) from t;")
	result.Check(testkit.Rows("<nil> <nil> <nil>"))
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|",
		"Warning|1292|Truncated incorrect time value: '-32073'",
		"Warning|1292|Truncated incorrect time value: '-32073'",
		"Warning|1292|Truncated incorrect time value: '-32073'"))

	// fixed issue #3986
	tk.MustExec("SET SQL_MODE='NO_ENGINE_SUBSTITUTION';")
	tk.MustExec("SET TIME_ZONE='+03:00';")
	tk.MustExec("DROP TABLE IF EXISTS t;")
	tk.MustExec("CREATE TABLE t (ix TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP);")
	tk.MustExec("INSERT INTO t VALUES (0), (20030101010160), (20030101016001), (20030101240101), (20030132010101), (20031301010101), (20031200000000), (20030000000000);")
	result = tk.MustQuery("SELECT CAST(ix AS SIGNED) FROM t;")
	result.Check(testkit.Rows("0", "0", "0", "0", "0", "0", "0", "0"))

	// test time
	result = tk.MustQuery("select time('2003-12-31 01:02:03')")
	result.Check(testkit.Rows("01:02:03"))
	result = tk.MustQuery("select time('2003-12-31 01:02:03.000123')")
	result.Check(testkit.Rows("01:02:03.000123"))
	result = tk.MustQuery("select time('01:02:03.000123')")
	result.Check(testkit.Rows("01:02:03.000123"))
	result = tk.MustQuery("select time('01:02:03')")
	result.Check(testkit.Rows("01:02:03"))
	result = tk.MustQuery("select time('-838:59:59.000000')")
	result.Check(testkit.Rows("-838:59:59.000000"))
	result = tk.MustQuery("select time('-838:59:59.000001')")
	result.Check(testkit.Rows("-838:59:59.000000"))
	result = tk.MustQuery("select time('-839:59:59.000000')")
	result.Check(testkit.Rows("-838:59:59.000000"))
	result = tk.MustQuery("select time('840:59:59.000000')")
	result.Check(testkit.Rows("838:59:59.000000"))
	// FIXME: #issue 4193
	// result = tk.MustQuery("select time('840:59:60.000000')")
	// result.Check(testkit.Rows("<nil>"))
	// result = tk.MustQuery("select time('800:59:59.9999999')")
	// result.Check(testkit.Rows("801:00:00.000000"))
	// result = tk.MustQuery("select time('12003-12-10 01:02:03.000123')")
	// result.Check(testkit.Rows("<nil>")
	// result = tk.MustQuery("select time('')")
	// result.Check(testkit.Rows("<nil>")
	// result = tk.MustQuery("select time('2003-12-10-10 01:02:03.000123')")
	// result.Check(testkit.Rows("00:20:03")

	// Issue 20995
	result = tk.MustQuery("select time('0.1234567')")
	result.Check(testkit.Rows("00:00:00.123457"))

	// for hour
	result = tk.MustQuery(`SELECT hour("12:13:14.123456"), hour("12:13:14.000010"), hour("272:59:55"), hour(020005), hour(null), hour("27aaaa2:59:55");`)
	result.Check(testkit.Rows("12 12 272 2 <nil> <nil>"))

	// for hour, issue #4340
	result = tk.MustQuery(`SELECT HOUR(20171222020005);`)
	result.Check(testkit.Rows("2"))
	result = tk.MustQuery(`SELECT HOUR(20171222020005.1);`)
	result.Check(testkit.Rows("2"))
	result = tk.MustQuery(`SELECT HOUR(20171222020005.1e0);`)
	result.Check(testkit.Rows("2"))
	result = tk.MustQuery(`SELECT HOUR("20171222020005");`)
	result.Check(testkit.Rows("2"))
	result = tk.MustQuery(`SELECT HOUR("20171222020005.1");`)
	result.Check(testkit.Rows("2"))
	result = tk.MustQuery(`select hour(20171222);`)
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery(`select hour(8381222);`)
	result.Check(testkit.Rows("838"))
	result = tk.MustQuery(`select hour(10000000000);`)
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery(`select hour(10100000000);`)
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery(`select hour(10001000000);`)
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery(`select hour(10101000000);`)
	result.Check(testkit.Rows("0"))

	// for minute
	result = tk.MustQuery(`SELECT minute("12:13:14.123456"), minute("12:13:14.000010"), minute("272:59:55"), minute(null), minute("27aaaa2:59:55");`)
	result.Check(testkit.Rows("13 13 59 <nil> <nil>"))

	// for second
	result = tk.MustQuery(`SELECT second("12:13:14.123456"), second("12:13:14.000010"), second("272:59:55"), second(null), second("27aaaa2:59:55");`)
	result.Check(testkit.Rows("14 14 55 <nil> <nil>"))

	// for microsecond
	result = tk.MustQuery(`SELECT microsecond("12:00:00.123456"), microsecond("12:00:00.000010"), microsecond(null), microsecond("27aaaa2:59:55");`)
	result.Check(testkit.Rows("123456 10 <nil> <nil>"))

	// for period_add
	result = tk.MustQuery(`SELECT period_add(200807, 2), period_add(200807, -2);`)
	result.Check(testkit.Rows("200809 200805"))
	result = tk.MustQuery(`SELECT period_add(NULL, 2), period_add(-191, NULL), period_add(NULL, NULL), period_add(12.09, -2), period_add("200207aa", "1aa");`)
	result.Check(testkit.Rows("<nil> <nil> <nil> 200010 200208"))
	for _, errPeriod := range []string{
		"period_add(0, 20)", "period_add(0, 0)", "period_add(-1, 1)", "period_add(200013, 1)", "period_add(-200012, 1)", "period_add('', '')",
	} {
		err := tk.QueryToErr(fmt.Sprintf("SELECT %v;", errPeriod))
		require.Error(t, err, "[expression:1210]Incorrect arguments to period_add")
	}

	// for period_diff
	result = tk.MustQuery(`SELECT period_diff(200807, 200705), period_diff(200807, 200908);`)
	result.Check(testkit.Rows("14 -13"))
	result = tk.MustQuery(`SELECT period_diff(NULL, 2), period_diff(-191, NULL), period_diff(NULL, NULL), period_diff(12.09, 2), period_diff("12aa", "11aa");`)
	result.Check(testkit.Rows("<nil> <nil> <nil> 10 1"))
	for _, errPeriod := range []string{
		"period_diff(-00013,1)", "period_diff(00013,1)", "period_diff(0, 0)", "period_diff(200013, 1)", "period_diff(5612, 4513)", "period_diff('', '')",
	} {
		err := tk.QueryToErr(fmt.Sprintf("SELECT %v;", errPeriod))
		require.Error(t, err, "[expression:1210]Incorrect arguments to period_diff")
	}

	// TODO: fix `CAST(xx as duration)` and release the test below:
	// result = tk.MustQuery(`SELECT hour("aaa"), hour(123456), hour(1234567);`)
	// result = tk.MustQuery(`SELECT minute("aaa"), minute(123456), minute(1234567);`)
	// result = tk.MustQuery(`SELECT second("aaa"), second(123456), second(1234567);`)
	// result = tk.MustQuery(`SELECT microsecond("aaa"), microsecond(123456), microsecond(1234567);`)

	// for time_format
	result = tk.MustQuery("SELECT TIME_FORMAT('150:02:28', '%H:%i:%s %p');")
	result.Check(testkit.Rows("150:02:28 AM"))
	result = tk.MustQuery("SELECT TIME_FORMAT('bad string', '%H:%i:%s %p');")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("SELECT TIME_FORMAT(null, '%H:%i:%s %p');")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("SELECT TIME_FORMAT(123, '%H:%i:%s %p');")
	result.Check(testkit.Rows("00:01:23 AM"))
	result = tk.MustQuery("SELECT TIME_FORMAT('24:00:00', '%r');")
	result.Check(testkit.Rows("12:00:00 AM"))
	result = tk.MustQuery("SELECT TIME_FORMAT('25:00:00', '%r');")
	result.Check(testkit.Rows("01:00:00 AM"))
	result = tk.MustQuery("SELECT TIME_FORMAT('24:00:00', '%l %p');")
	result.Check(testkit.Rows("12 AM"))

	// for date_format
	result = tk.MustQuery(`SELECT DATE_FORMAT('2017-06-15', '%W %M %e %Y %r %y');`)
	result.Check(testkit.Rows("Thursday June 15 2017 12:00:00 AM 17"))
	result = tk.MustQuery(`SELECT DATE_FORMAT(151113102019.12, '%W %M %e %Y %r %y');`)
	result.Check(testkit.Rows("Friday November 13 2015 10:20:19 AM 15"))
	result = tk.MustQuery(`SELECT DATE_FORMAT('0000-00-00', '%W %M %e %Y %r %y');`)
	result.Check(testkit.Rows("<nil>"))
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|",
		"Warning|1292|Incorrect datetime value: '0000-00-00 00:00:00.000000'"))
	result = tk.MustQuery(`SELECT DATE_FORMAT('0', '%W %M %e %Y %r %y'), DATE_FORMAT('0.0', '%W %M %e %Y %r %y'), DATE_FORMAT(0, 0);`)
	result.Check(testkit.Rows("<nil> <nil> 0"))
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|",
		"Warning|1292|Incorrect time value: '0'",
		"Warning|1292|Incorrect datetime value: '0.0'"))
	result = tk.MustQuery(`SELECT DATE_FORMAT(0, '%W %M %e %Y %r %y'), DATE_FORMAT(0.0, '%W %M %e %Y %r %y');`)
	result.Check(testkit.Rows("<nil> <nil>"))
	tk.MustQuery("show warnings").Check(testkit.Rows())

	// for yearweek
	result = tk.MustQuery(`select yearweek("2014-12-27"), yearweek("2014-29-27"), yearweek("2014-00-27"), yearweek("2014-12-27 12:38:32"), yearweek("2014-12-27 12:38:32.1111111"), yearweek("2014-12-27 12:90:32"), yearweek("2014-12-27 89:38:32.1111111");`)
	result.Check(testkit.Rows("201451 <nil> <nil> 201451 201451 <nil> <nil>"))
	result = tk.MustQuery(`select yearweek(12121), yearweek(1.00009), yearweek("aaaaa"), yearweek(""), yearweek(NULL);`)
	result.Check(testkit.Rows("<nil> <nil> <nil> <nil> <nil>"))
	result = tk.MustQuery(`select yearweek("0000-00-00"), yearweek("2019-01-29", "aa"), yearweek("2011-01-01", null);`)
	result.Check(testkit.Rows("<nil> 201904 201052"))

	// for dayOfWeek, dayOfMonth, dayOfYear
	result = tk.MustQuery(`select dayOfWeek(null), dayOfWeek("2017-08-12"), dayOfWeek("0000-00-00"), dayOfWeek("2017-00-00"), dayOfWeek("0000-00-00 12:12:12"), dayOfWeek("2017-00-00 12:12:12")`)
	result.Check(testkit.Rows("<nil> 7 <nil> <nil> <nil> <nil>"))
	result = tk.MustQuery(`select dayOfYear(null), dayOfYear("2017-08-12"), dayOfYear("0000-00-00"), dayOfYear("2017-00-00"), dayOfYear("0000-00-00 12:12:12"), dayOfYear("2017-00-00 12:12:12")`)
	result.Check(testkit.Rows("<nil> 224 <nil> <nil> <nil> <nil>"))
	result = tk.MustQuery(`select dayOfMonth(null), dayOfMonth("2017-08-12"), dayOfMonth("0000-00-00"), dayOfMonth("2017-00-00"), dayOfMonth("0000-00-00 12:12:12"), dayOfMonth("2017-00-00 12:12:12")`)
	result.Check(testkit.Rows("<nil> 12 0 0 0 0"))

	tk.MustExec("set sql_mode = 'NO_ZERO_DATE'")
	result = tk.MustQuery(`select dayOfWeek(null), dayOfWeek("2017-08-12"), dayOfWeek("0000-00-00"), dayOfWeek("2017-00-00"), dayOfWeek("0000-00-00 12:12:12"), dayOfWeek("2017-00-00 12:12:12")`)
	result.Check(testkit.Rows("<nil> 7 <nil> <nil> <nil> <nil>"))
	result = tk.MustQuery(`select dayOfYear(null), dayOfYear("2017-08-12"), dayOfYear("0000-00-00"), dayOfYear("2017-00-00"), dayOfYear("0000-00-00 12:12:12"), dayOfYear("2017-00-00 12:12:12")`)
	result.Check(testkit.Rows("<nil> 224 <nil> <nil> <nil> <nil>"))
	result = tk.MustQuery(`select dayOfMonth(null), dayOfMonth("2017-08-12"), dayOfMonth("0000-00-00"), dayOfMonth("2017-00-00"), dayOfMonth("0000-00-00 12:12:12"), dayOfMonth("2017-00-00 12:12:12")`)
	result.Check(testkit.Rows("<nil> 12 <nil> 0 0 0"))

	tk.MustExec(`drop table if exists t`)
	tk.MustExec(`create table t(a bigint)`)
	tk.MustExec(`insert into t value(1)`)
	tk.MustExec("set sql_mode = 'STRICT_TRANS_TABLES'")

	_, err = tk.Exec("insert into t value(dayOfWeek('0000-00-00'))")
	require.True(t, types.ErrWrongValue.Equal(err), "%v", err)
	_, err = tk.Exec(`update t set a = dayOfWeek("0000-00-00")`)
	require.True(t, types.ErrWrongValue.Equal(err))
	_, err = tk.Exec(`delete from t where a = dayOfWeek(123)`)
	require.NoError(t, err)

	tk.MustExec("insert into t value(dayOfMonth('2017-00-00'))")
	tk.MustExec("insert into t value(dayOfMonth('0000-00-00'))")
	tk.MustExec(`update t set a = dayOfMonth("0000-00-00")`)
	tk.MustExec("set sql_mode = 'NO_ZERO_DATE';")
	tk.MustExec("insert into t value(dayOfMonth('0000-00-00'))")
	tk.MustQuery("show warnings").CheckContain("Incorrect datetime value: '0000-00-00 00:00:00.000000'")
	tk.MustExec(`update t set a = dayOfMonth("0000-00-00")`)
	tk.MustExec("set sql_mode = 'NO_ZERO_DATE,STRICT_TRANS_TABLES';")
	_, err = tk.Exec("insert into t value(dayOfMonth('0000-00-00'))")
	require.True(t, types.ErrWrongValue.Equal(err))
	tk.MustExec("insert into t value(0)")
	_, err = tk.Exec(`update t set a = dayOfMonth("0000-00-00")`)
	require.True(t, types.ErrWrongValue.Equal(err))
	_, err = tk.Exec(`delete from t where a = dayOfMonth(123)`)
	require.NoError(t, err)

	_, err = tk.Exec("insert into t value(dayOfYear('0000-00-00'))")
	require.True(t, types.ErrWrongValue.Equal(err))
	_, err = tk.Exec(`update t set a = dayOfYear("0000-00-00")`)
	require.True(t, types.ErrWrongValue.Equal(err))
	_, err = tk.Exec(`delete from t where a = dayOfYear(123)`)
	require.NoError(t, err)

	tk.MustExec("set sql_mode = ''")

	// for unix_timestamp
	tk.MustExec("SET time_zone = '+00:00';")
	tk.MustQuery("SELECT UNIX_TIMESTAMP('1970-01-01 00:00:00.000001');").Check(testkit.Rows("0.000000"))
	tk.MustQuery("SELECT UNIX_TIMESTAMP('1970-01-01 00:00:00.999999');").Check(testkit.Rows("0.000000"))
	tk.MustQuery("SELECT UNIX_TIMESTAMP('1970-01-01 00:00:01.000000');").Check(testkit.Rows("1.000000"))
	tk.MustQuery("SELECT UNIX_TIMESTAMP('2038-01-19 03:14:07.999999');").Check(testkit.Rows("2147483647.999999"))
	tk.MustQuery("SELECT UNIX_TIMESTAMP('2038-01-19 03:14:08.000000');").Check(testkit.Rows("2147483648.000000"))
	tk.MustQuery("SELECT UNIX_TIMESTAMP('3001-01-18 23:59:59.999999');").Check(testkit.Rows("32536771199.999999"))
	tk.MustQuery("SELECT UNIX_TIMESTAMP('3001-01-19 00:00:00.000000');").Check(testkit.Rows("0.000000"))

	result = tk.MustQuery("SELECT UNIX_TIMESTAMP(151113);")
	result.Check(testkit.Rows("1447372800"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP(20151113);")
	result.Check(testkit.Rows("1447372800"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP(151113102019);")
	result.Check(testkit.Rows("1447410019"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP(151113102019e0);")
	result.Check(testkit.Rows("1447410019.000000"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP(15111310201912e-2);")
	result.Check(testkit.Rows("1447410019.120000"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP(151113102019.12);")
	result.Check(testkit.Rows("1447410019.12"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP(151113102019.1234567);")
	result.Check(testkit.Rows("1447410019.123457"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP(20151113102019);")
	result.Check(testkit.Rows("1447410019"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP('2015-11-13 10:20:19');")
	result.Check(testkit.Rows("1447410019"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP('2015-11-13 10:20:19.012');")
	result.Check(testkit.Rows("1447410019.012"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP('1970-01-01 00:00:00');")
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP('1969-12-31 23:59:59');")
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP('1970-13-01 00:00:00');")
	// FIXME: MySQL returns 0 here.
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP('2038-01-19 03:14:07.999999');")
	result.Check(testkit.Rows("2147483647.999999"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP('3001-01-18 23:59:59.999999');")
	result.Check(testkit.Rows("32536771199.999999"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP('3001-01-19 00:00:00');")
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP(0);")
	result.Check(testkit.Rows("0"))
	// result = tk.MustQuery("SELECT UNIX_TIMESTAMP(-1);")
	// result.Check(testkit.Rows("0"))
	// result = tk.MustQuery("SELECT UNIX_TIMESTAMP(12345);")
	// result.Check(testkit.Rows("0"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP('2017-01-01')")
	result.Check(testkit.Rows("1483228800"))
	// Test different time zone.
	tk.MustExec("SET time_zone = '+08:00';")
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP('1970-01-01 00:00:00');")
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP('1970-01-01 08:00:00');")
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP('2015-11-13 18:20:19.012'), UNIX_TIMESTAMP('2015-11-13 18:20:19.0123');")
	result.Check(testkit.Rows("1447410019.012 1447410019.0123"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP('2038-01-19 11:14:07.999999');")
	result.Check(testkit.Rows("2147483647.999999"))

	result = tk.MustQuery("SELECT TIME_FORMAT('bad string', '%H:%i:%s %p');")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("SELECT TIME_FORMAT(null, '%H:%i:%s %p');")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("SELECT TIME_FORMAT(123, '%H:%i:%s %p');")
	result.Check(testkit.Rows("00:01:23 AM"))

	// for monthname
	tk.MustExec(`drop table if exists t`)
	tk.MustExec(`create table t(a varchar(10))`)
	tk.MustExec(`insert into t value("abc")`)
	tk.MustExec("set sql_mode = 'STRICT_TRANS_TABLES'")

	tk.MustExec("insert into t value(monthname('0000-00-00'))")
	tk.MustExec(`update t set a = monthname("0000-00-00")`)
	tk.MustExec("set sql_mode = 'NO_ZERO_DATE'")
	tk.MustExec("insert into t value(monthname('0000-00-00'))")
	tk.MustQuery("show warnings").CheckContain("Incorrect datetime value: '0000-00-00 00:00:00.000000'")
	tk.MustExec(`update t set a = monthname("0000-00-00")`)
	tk.MustExec("set sql_mode = ''")
	tk.MustExec("insert into t value(monthname('0000-00-00'))")
	tk.MustExec("set sql_mode = 'STRICT_TRANS_TABLES,NO_ZERO_DATE'")
	_, err = tk.Exec(`update t set a = monthname("0000-00-00")`)
	require.True(t, types.ErrWrongValue.Equal(err))
	_, err = tk.Exec(`delete from t where a = monthname(123)`)
	require.NoError(t, err)
	result = tk.MustQuery(`select monthname("2017-12-01"), monthname("0000-00-00"), monthname("0000-01-00"), monthname("0000-01-00 00:00:00")`)
	result.Check(testkit.Rows("December <nil> January January"))
	tk.MustQuery("show warnings").CheckContain("Incorrect datetime value: '0000-00-00 00:00:00.000000'")

	// for dayname
	tk.MustExec(`drop table if exists t`)
	tk.MustExec(`create table t(a varchar(10))`)
	tk.MustExec(`insert into t value("abc")`)
	tk.MustExec("set sql_mode = 'STRICT_TRANS_TABLES'")

	_, err = tk.Exec("insert into t value(dayname('0000-00-00'))")
	require.True(t, types.ErrWrongValue.Equal(err))
	_, err = tk.Exec(`update t set a = dayname("0000-00-00")`)
	require.True(t, types.ErrWrongValue.Equal(err))
	_, err = tk.Exec(`delete from t where a = dayname(123)`)
	require.NoError(t, err)
	result = tk.MustQuery(`select dayname("2017-12-01"), dayname("0000-00-00"), dayname("0000-01-00"), dayname("0000-01-00 00:00:00")`)
	result.Check(testkit.Rows("Friday <nil> <nil> <nil>"))
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|",
		"Warning|1292|Incorrect datetime value: '0000-00-00 00:00:00.000000'",
		"Warning|1292|Incorrect datetime value: '0000-01-00 00:00:00.000000'",
		"Warning|1292|Incorrect datetime value: '0000-01-00 00:00:00.000000'"))
	// for dayname implicit cast to boolean and real
	result = tk.MustQuery(`select 1 from dual where dayname('2016-03-07')`)
	result.Check(testkit.Rows())
	result = tk.MustQuery(`select 1 from dual where dayname('2016-03-07') is true`)
	result.Check(testkit.Rows())
	result = tk.MustQuery(`select 1 from dual where dayname('2016-03-07') is false`)
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery(`select 1 from dual where dayname('2016-03-08')`)
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery(`select 1 from dual where dayname('2016-03-08') is true`)
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery(`select 1 from dual where dayname('2016-03-08') is false`)
	result.Check(testkit.Rows())
	result = tk.MustQuery(`select cast(dayname("2016-03-07") as double), cast(dayname("2016-03-08") as double)`)
	result.Check(testkit.Rows("0 1"))

	// for sec_to_time
	result = tk.MustQuery("select sec_to_time(NULL)")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select sec_to_time(2378), sec_to_time(3864000), sec_to_time(-3864000)")
	result.Check(testkit.Rows("00:39:38 838:59:59 -838:59:59"))
	result = tk.MustQuery("select sec_to_time(86401.4), sec_to_time(-86401.4), sec_to_time(864014e-1), sec_to_time(-864014e-1), sec_to_time('86401.4'), sec_to_time('-86401.4')")
	result.Check(testkit.Rows("24:00:01.4 -24:00:01.4 24:00:01.400000 -24:00:01.400000 24:00:01.400000 -24:00:01.400000"))
	result = tk.MustQuery("select sec_to_time(86401.54321), sec_to_time(86401.543212345)")
	result.Check(testkit.Rows("24:00:01.54321 24:00:01.543212"))
	result = tk.MustQuery("select sec_to_time('123.4'), sec_to_time('123.4567891'), sec_to_time('123')")
	result.Check(testkit.Rows("00:02:03.400000 00:02:03.456789 00:02:03.000000"))

	// for time_to_sec
	result = tk.MustQuery("select time_to_sec(NULL)")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select time_to_sec('22:23:00'), time_to_sec('00:39:38'), time_to_sec('23:00'), time_to_sec('00:00'), time_to_sec('00:00:00'), time_to_sec('23:59:59')")
	result.Check(testkit.Rows("80580 2378 82800 0 0 86399"))
	result = tk.MustQuery("select time_to_sec('1:0'), time_to_sec('1:00'), time_to_sec('1:0:0'), time_to_sec('-02:00'), time_to_sec('-02:00:05'), time_to_sec('020005')")
	result.Check(testkit.Rows("3600 3600 3600 -7200 -7205 7205"))
	result = tk.MustQuery("select time_to_sec('20171222020005'), time_to_sec(020005), time_to_sec(20171222020005), time_to_sec(171222020005)")
	result.Check(testkit.Rows("7205 7205 7205 7205"))

	// for str_to_date
	result = tk.MustQuery("select str_to_date('01-01-2017', '%d-%m-%Y'), str_to_date('59:20:12 01-01-2017', '%s:%i:%H %d-%m-%Y'), str_to_date('59:20:12', '%s:%i:%H')")
	result.Check(testkit.Rows("2017-01-01 2017-01-01 12:20:59 12:20:59"))
	result = tk.MustQuery("select str_to_date('aaa01-01-2017', 'aaa%d-%m-%Y'), str_to_date('59:20:12 aaa01-01-2017', '%s:%i:%H aaa%d-%m-%Y'), str_to_date('59:20:12aaa', '%s:%i:%Haaa')")
	result.Check(testkit.Rows("2017-01-01 2017-01-01 12:20:59 12:20:59"))

	result = tk.MustQuery("select str_to_date('01-01-2017', '%d'), str_to_date('59', '%d-%Y')")
	// TODO: MySQL returns "<nil> <nil>".
	result.Check(testkit.Rows("0000-00-01 <nil>"))
	result = tk.MustQuery("show warnings")
	result.Sort().Check(testkit.RowsWithSep("|",
		"Warning|1292|Incorrect datetime value: '0000-00-00 00:00:00'",
		"Warning|1292|Truncated incorrect datetime value: '01-01-2017'"))

	result = tk.MustQuery("select str_to_date('2018-6-1', '%Y-%m-%d'), str_to_date('2018-6-1', '%Y-%c-%d'), str_to_date('59:20:1', '%s:%i:%k'), str_to_date('59:20:1', '%s:%i:%l')")
	result.Check(testkit.Rows("2018-06-01 2018-06-01 01:20:59 01:20:59"))

	result = tk.MustQuery("select str_to_date('2020-07-04 11:22:33 PM c', '%Y-%m-%d %r')")
	result.Check(testkit.Rows("2020-07-04 23:22:33"))
	result = tk.MustQuery("show warnings")
	result.Check(testkit.RowsWithSep("|", "Warning|1292|Truncated incorrect datetime value: '2020-07-04 11:22:33 PM c'"))

	result = tk.MustQuery("select str_to_date('11:22:33 PM', ' %r')")
	result.Check(testkit.Rows("23:22:33"))
	result = tk.MustQuery("show warnings")
	result.Check(testkit.Rows())

	// for maketime
	tk.MustExec(`drop table if exists t`)
	tk.MustExec(`create table t(a double, b float, c decimal(10,4));`)
	tk.MustExec(`insert into t value(1.23, 2.34, 3.1415)`)
	result = tk.MustQuery("select maketime(1,1,a), maketime(2,2,b), maketime(3,3,c) from t;")
	result.Check(testkit.Rows("01:01:01.230000 02:02:02.340000 03:03:03.1415"))
	result = tk.MustQuery("select maketime(12, 13, 14), maketime('12', '15', 30.1), maketime(0, 1, 59.1), maketime(0, 1, '59.1'), maketime(0, 1, 59.5)")
	result.Check(testkit.Rows("12:13:14 12:15:30.1 00:01:59.1 00:01:59.100000 00:01:59.5"))
	result = tk.MustQuery("select maketime(12, 15, 60), maketime(12, 15, '60'), maketime(12, 60, 0), maketime(12, 15, null)")
	result.Check(testkit.Rows("<nil> <nil> <nil> <nil>"))
	result = tk.MustQuery("select maketime('', '', ''), maketime('h', 'm', 's');")
	result.Check(testkit.Rows("00:00:00.000000 00:00:00.000000"))

	// for get_format
	result = tk.MustQuery(`select GET_FORMAT(DATE,'USA'), GET_FORMAT(DATE,'JIS'), GET_FORMAT(DATE,'ISO'), GET_FORMAT(DATE,'EUR'),
	GET_FORMAT(DATE,'INTERNAL'), GET_FORMAT(DATETIME,'USA') , GET_FORMAT(DATETIME,'JIS'), GET_FORMAT(DATETIME,'ISO'),
	GET_FORMAT(DATETIME,'EUR') , GET_FORMAT(DATETIME,'INTERNAL'), GET_FORMAT(TIME,'USA') , GET_FORMAT(TIME,'JIS'),
	GET_FORMAT(TIME,'ISO'), GET_FORMAT(TIME,'EUR'), GET_FORMAT(TIME,'INTERNAL')`)
	result.Check(testkit.Rows("%m.%d.%Y %Y-%m-%d %Y-%m-%d %d.%m.%Y %Y%m%d %Y-%m-%d %H.%i.%s %Y-%m-%d %H:%i:%s %Y-%m-%d %H:%i:%s %Y-%m-%d %H.%i.%s %Y%m%d%H%i%s %h:%i:%s %p %H:%i:%s %H:%i:%s %H.%i.%s %H%i%s"))

	// for convert_tz
	result = tk.MustQuery(`select convert_tz("2004-01-01 12:00:00", "+00:00", "+10:32"), convert_tz("2004-01-01 12:00:00.01", "+00:00", "+10:32"), convert_tz("2004-01-01 12:00:00.01234567", "+00:00", "+10:32");`)
	result.Check(testkit.Rows("2004-01-01 22:32:00 2004-01-01 22:32:00.01 2004-01-01 22:32:00.012346"))
	result = tk.MustQuery(`select convert_tz(20040101, "+00:00", "+10:32"), convert_tz(20040101.01, "+00:00", "+10:32"), convert_tz(20040101.01234567, "+00:00", "+10:32");`)
	result.Check(testkit.Rows("2004-01-01 10:32:00 2004-01-01 10:32:00.00 2004-01-01 10:32:00.000000"))
	result = tk.MustQuery(`select convert_tz(NULL, "+00:00", "+10:32"), convert_tz("2004-01-01 12:00:00", NULL, "+10:32"), convert_tz("2004-01-01 12:00:00", "+00:00", NULL);`)
	result.Check(testkit.Rows("<nil> <nil> <nil>"))
	result = tk.MustQuery(`select convert_tz("a", "+00:00", "+10:32"), convert_tz("2004-01-01 12:00:00", "a", "+10:32"), convert_tz("2004-01-01 12:00:00", "+00:00", "a");`)
	result.Check(testkit.Rows("<nil> <nil> <nil>"))
	result = tk.MustQuery(`select convert_tz("", "+00:00", "+10:32"), convert_tz("2004-01-01 12:00:00", "", "+10:32"), convert_tz("2004-01-01 12:00:00", "+00:00", "");`)
	result.Check(testkit.Rows("<nil> <nil> <nil>"))
	result = tk.MustQuery(`select convert_tz("0", "+00:00", "+10:32"), convert_tz("2004-01-01 12:00:00", "0", "+10:32"), convert_tz("2004-01-01 12:00:00", "+00:00", "0");`)
	result.Check(testkit.Rows("<nil> <nil> <nil>"))

	// for from_unixtime
	tk.MustExec(`set @@session.time_zone = "+08:00"`)
	result = tk.MustQuery(`select from_unixtime(20170101), from_unixtime(20170101.9999999), from_unixtime(20170101.999), from_unixtime(20170101.999, "%Y %D %M %h:%i:%s %x"), from_unixtime(20170101.999, "%Y %D %M %h:%i:%s %x")`)
	result.Check(testkit.Rows("1970-08-22 18:48:21 1970-08-22 18:48:22.000000 1970-08-22 18:48:21.999 1970 22nd August 06:48:21 1970 1970 22nd August 06:48:21 1970"))
	tk.MustExec(`set @@session.time_zone = "+00:00"`)
	result = tk.MustQuery(`select from_unixtime(20170101), from_unixtime(20170101.9999999), from_unixtime(20170101.999), from_unixtime(20170101.999, "%Y %D %M %h:%i:%s %x"), from_unixtime(20170101.999, "%Y %D %M %h:%i:%s %x")`)
	result.Check(testkit.Rows("1970-08-22 10:48:21 1970-08-22 10:48:22.000000 1970-08-22 10:48:21.999 1970 22nd August 10:48:21 1970 1970 22nd August 10:48:21 1970"))
	tk.MustExec(`set @@session.time_zone = @@global.time_zone`)

	// for extract
	result = tk.MustQuery(`select extract(day from '800:12:12'), extract(hour from '800:12:12'), extract(month from 20170101), extract(day_second from '2017-01-01 12:12:12')`)
	result.Check(testkit.Rows("12 800 1 1121212"))
	result = tk.MustQuery("select extract(day_microsecond from '2017-01-01 12:12:12'), extract(day_microsecond from '01 12:12:12'), extract(day_microsecond from '12:12:12'), extract(day_microsecond from '01 00:00:00.89')")
	result.Check(testkit.Rows("1121212000000 361212000000 121212000000 240000890000"))
	result = tk.MustQuery("select extract(day_second from '2017-01-01 12:12:12'), extract(day_second from '01 12:12:12'), extract(day_second from '12:12:12'), extract(day_second from '01 00:00:00.89')")
	result.Check(testkit.Rows("1121212 361212 121212 240000"))
	result = tk.MustQuery("select extract(day_minute from '2017-01-01 12:12:12'), extract(day_minute from '01 12:12:12'), extract(day_minute from '12:12:12'), extract(day_minute from '01 00:00:00.89')")
	result.Check(testkit.Rows("11212 3612 1212 2400"))
	result = tk.MustQuery("select extract(day_hour from '2017-01-01 12:12:12'), extract(day_hour from '01 12:12:12'), extract(day_hour from '12:12:12'), extract(day_hour from '01 00:00:00.89')")
	result.Check(testkit.Rows("112 36 12 24"))
	result = tk.MustQuery("select extract(day_microsecond from cast('2017-01-01 12:12:12' as datetime)), extract(day_second from cast('2017-01-01 12:12:12' as datetime)), extract(day_minute from cast('2017-01-01 12:12:12' as datetime)), extract(day_hour from cast('2017-01-01 12:12:12' as datetime))")
	result.Check(testkit.Rows("1121212000000 1121212 11212 112"))
	result = tk.MustQuery("select extract(day_microsecond from cast(20010101020304.050607 as decimal(20,6))), extract(day_second from cast(20010101020304.050607 as decimal(20,6))), extract(day_minute from cast(20010101020304.050607 as decimal(20,6))), extract(day_hour from cast(20010101020304.050607 as decimal(20,6))), extract(day from cast(20010101020304.050607 as decimal(20,6)))")
	result.Check(testkit.Rows("1020304050607 1020304 10203 102 1"))
	result = tk.MustQuery("select extract(day_microsecond from cast(1020304.050607 as decimal(20,6))), extract(day_second from cast(1020304.050607 as decimal(20,6))), extract(day_minute from cast(1020304.050607 as decimal(20,6))), extract(day_hour from cast(1020304.050607 as decimal(20,6))), extract(day from cast(1020304.050607 as decimal(20,6)))")
	result.Check(testkit.Rows("1020304050607 1020304 10203 102 4"))

	// for adddate, subdate
	dateArithmeticalTests := []struct {
		Date      string
		Interval  string
		Unit      string
		AddResult string
		SubResult string
	}{
		{"\"2011-11-11\"", "1", "DAY", "2011-11-12", "2011-11-10"},
		{"NULL", "1", "DAY", "<nil>", "<nil>"},
		{"\"2011-11-11\"", "NULL", "DAY", "<nil>", "<nil>"},
		{"\"2011-11-11 10:10:10\"", "1000", "MICROSECOND", "2011-11-11 10:10:10.001000", "2011-11-11 10:10:09.999000"},
		{"\"2011-11-11 10:10:10\"", "\"10\"", "SECOND", "2011-11-11 10:10:20", "2011-11-11 10:10:00"},
		{"\"2011-11-11 10:10:10\"", "\"10\"", "MINUTE", "2011-11-11 10:20:10", "2011-11-11 10:00:10"},
		{"\"2011-11-11 10:10:10\"", "\"10\"", "HOUR", "2011-11-11 20:10:10", "2011-11-11 00:10:10"},
		{"\"2011-11-11 10:10:10\"", "\"11\"", "DAY", "2011-11-22 10:10:10", "2011-10-31 10:10:10"},
		{"\"2011-11-11 10:10:10\"", "\"2\"", "WEEK", "2011-11-25 10:10:10", "2011-10-28 10:10:10"},
		{"\"2011-11-11 10:10:10\"", "\"2\"", "MONTH", "2012-01-11 10:10:10", "2011-09-11 10:10:10"},
		{"\"2011-11-11 10:10:10\"", "\"4\"", "QUARTER", "2012-11-11 10:10:10", "2010-11-11 10:10:10"},
		{"\"2011-11-11 10:10:10\"", "\"2\"", "YEAR", "2013-11-11 10:10:10", "2009-11-11 10:10:10"},
		{"\"2011-11-11 10:10:10\"", "\"10.00100000\"", "SECOND_MICROSECOND", "2011-11-11 10:10:20.100000", "2011-11-11 10:09:59.900000"},
		{"\"2011-11-11 10:10:10\"", "\"10.0010000000\"", "SECOND_MICROSECOND", "2011-11-11 10:10:30", "2011-11-11 10:09:50"},
		{"\"2011-11-11 10:10:10\"", "\"10.0010000010\"", "SECOND_MICROSECOND", "2011-11-11 10:10:30.000010", "2011-11-11 10:09:49.999990"},
		{"\"2011-11-11 10:10:10\"", "\"10:10.100\"", "MINUTE_MICROSECOND", "2011-11-11 10:20:20.100000", "2011-11-11 09:59:59.900000"},
		{"\"2011-11-11 10:10:10\"", "\"10:10\"", "MINUTE_SECOND", "2011-11-11 10:20:20", "2011-11-11 10:00:00"},
		{"\"2011-11-11 10:10:10\"", "\"10:10:10.100\"", "HOUR_MICROSECOND", "2011-11-11 20:20:20.100000", "2011-11-10 23:59:59.900000"},
		{"\"2011-11-11 10:10:10\"", "\"10:10:10\"", "HOUR_SECOND", "2011-11-11 20:20:20", "2011-11-11 00:00:00"},
		{"\"2011-11-11 10:10:10\"", "\"10:10\"", "HOUR_MINUTE", "2011-11-11 20:20:10", "2011-11-11 00:00:10"},
		{"\"2011-11-11 10:10:10\"", "\"11 10:10:10.100\"", "DAY_MICROSECOND", "2011-11-22 20:20:20.100000", "2011-10-30 23:59:59.900000"},
		{"\"2011-11-11 10:10:10\"", "\"11 10:10:10\"", "DAY_SECOND", "2011-11-22 20:20:20", "2011-10-31 00:00:00"},
		{"\"2011-11-11 10:10:10\"", "\"11 10:10\"", "DAY_MINUTE", "2011-11-22 20:20:10", "2011-10-31 00:00:10"},
		{"\"2011-11-11 10:10:10\"", "\"11 10\"", "DAY_HOUR", "2011-11-22 20:10:10", "2011-10-31 00:10:10"},
		{"\"2011-11-11 10:10:10\"", "\"11-1\"", "YEAR_MONTH", "2022-12-11 10:10:10", "2000-10-11 10:10:10"},
		{"\"2011-11-11 10:10:10\"", "\"11-11\"", "YEAR_MONTH", "2023-10-11 10:10:10", "1999-12-11 10:10:10"},
		{"\"2011-11-11 10:10:10\"", "\"20\"", "DAY", "2011-12-01 10:10:10", "2011-10-22 10:10:10"},
		{"\"2011-11-11 10:10:10\"", "19.88", "DAY", "2011-12-01 10:10:10", "2011-10-22 10:10:10"},
		{"\"2011-11-11 10:10:10\"", "\"19.88\"", "DAY", "2011-11-30 10:10:10", "2011-10-23 10:10:10"},
		{"\"2011-11-11 10:10:10\"", "\"prefix19suffix\"", "DAY", "2011-11-30 10:10:10", "2011-10-23 10:10:10"},
		{"\"2011-11-11 10:10:10\"", "\"20-11\"", "DAY", "2011-12-01 10:10:10", "2011-10-22 10:10:10"},
		{"\"2011-11-11 10:10:10\"", "\"20,11\"", "daY", "2011-12-01 10:10:10", "2011-10-22 10:10:10"},
		{"\"2011-11-11 10:10:10\"", "\"1000\"", "dAy", "2014-08-07 10:10:10", "2009-02-14 10:10:10"},
		{"\"2011-11-11 10:10:10\"", "\"true\"", "Day", "2011-11-12 10:10:10", "2011-11-10 10:10:10"},
		{"\"2011-11-11 10:10:10\"", "true", "Day", "2011-11-12 10:10:10", "2011-11-10 10:10:10"},
		{"\"2011-11-11\"", "1", "DAY", "2011-11-12", "2011-11-10"},
		{"\"2011-11-11\"", "10", "HOUR", "2011-11-11 10:00:00", "2011-11-10 14:00:00"},
		{"\"2011-11-11\"", "10", "MINUTE", "2011-11-11 00:10:00", "2011-11-10 23:50:00"},
		{"\"2011-11-11\"", "10", "SECOND", "2011-11-11 00:00:10", "2011-11-10 23:59:50"},
		{"\"2011-11-11\"", "\"10:10\"", "HOUR_MINUTE", "2011-11-11 10:10:00", "2011-11-10 13:50:00"},
		{"\"2011-11-11\"", "\"10:10:10\"", "HOUR_SECOND", "2011-11-11 10:10:10", "2011-11-10 13:49:50"},
		{"\"2011-11-11\"", "\"10:10:10.101010\"", "HOUR_MICROSECOND", "2011-11-11 10:10:10.101010", "2011-11-10 13:49:49.898990"},
		{"\"2011-11-11\"", "\"10:10\"", "MINUTE_SECOND", "2011-11-11 00:10:10", "2011-11-10 23:49:50"},
		{"\"2011-11-11\"", "\"10:10.101010\"", "MINUTE_MICROSECOND", "2011-11-11 00:10:10.101010", "2011-11-10 23:49:49.898990"},
		{"\"2011-11-11\"", "\"10.101010\"", "SECOND_MICROSECOND", "2011-11-11 00:00:10.101010", "2011-11-10 23:59:49.898990"},
		{"\"2011-11-11 00:00:00\"", "1", "DAY", "2011-11-12 00:00:00", "2011-11-10 00:00:00"},
		{"\"2011-11-11 00:00:00\"", "10", "HOUR", "2011-11-11 10:00:00", "2011-11-10 14:00:00"},
		{"\"2011-11-11 00:00:00\"", "10", "MINUTE", "2011-11-11 00:10:00", "2011-11-10 23:50:00"},
		{"\"2011-11-11 00:00:00\"", "10", "SECOND", "2011-11-11 00:00:10", "2011-11-10 23:59:50"},
		{"\"2011-11-11 00:00:00.500\"", "500000", "MICROSECOND", "2011-11-11 00:00:01", "2011-11-11 00:00:00"},

		{"\"2011-11-11\"", "\"abc1000\"", "MICROSECOND", "2011-11-11 00:00:00", "2011-11-11 00:00:00"},
		{"\"20111111 10:10:10\"", "\"1\"", "DAY", "<nil>", "<nil>"},
		{"\"2011-11-11\"", "\"10\"", "SECOND_MICROSECOND", "2011-11-11 00:00:00.100000", "2011-11-10 23:59:59.900000"},
		{"\"2011-11-11\"", "\"10.0000\"", "MINUTE_MICROSECOND", "2011-11-11 00:00:10", "2011-11-10 23:59:50"},
		{"\"2011-11-11\"", "\"10:10:10\"", "MINUTE_MICROSECOND", "2011-11-11 00:10:10.100000", "2011-11-10 23:49:49.900000"},

		{"cast(\"2011-11-11\" as datetime)", "\"10:10:10\"", "MINUTE_MICROSECOND", "2011-11-11 00:10:10.100000", "2011-11-10 23:49:49.900000"},
		{"cast(\"2011-11-11\" as datetime)", "\"1000000\"", "MICROSECOND", "2011-11-11 00:00:01.000000", "2011-11-10 23:59:59.000000"},
		{"cast(\"2011-11-11 00:00:00\" as datetime)", "1", "DAY", "2011-11-12 00:00:00", "2011-11-10 00:00:00"},
		{"cast(\"2011-11-11 00:00:00\" as datetime)", "10", "HOUR", "2011-11-11 10:00:00", "2011-11-10 14:00:00"},
		{"cast(\"2011-11-11 00:00:00\" as datetime)", "10", "MINUTE", "2011-11-11 00:10:00", "2011-11-10 23:50:00"},
		{"cast(\"2011-11-11 00:00:00\" as datetime)", "10", "SECOND", "2011-11-11 00:00:10", "2011-11-10 23:59:50"},

		{"cast(\"2011-11-11 00:00:00\" as datetime)", "\"1\"", "DAY", "2011-11-12 00:00:00", "2011-11-10 00:00:00"},
		{"cast(\"2011-11-11 00:00:00\" as datetime)", "\"10\"", "HOUR", "2011-11-11 10:00:00", "2011-11-10 14:00:00"},
		{"cast(\"2011-11-11 00:00:00\" as datetime)", "\"10\"", "MINUTE", "2011-11-11 00:10:00", "2011-11-10 23:50:00"},
		{"cast(\"2011-11-11 00:00:00\" as datetime)", "\"10\"", "SECOND", "2011-11-11 00:00:10.000000", "2011-11-10 23:59:50.000000"},

		{"cast(\"2011-11-11\" as date)", "\"10:10:10\"", "MINUTE_MICROSECOND", "2011-11-11 00:10:10.100000", "2011-11-10 23:49:49.900000"},
		{"cast(\"2011-11-11\" as date)", "\"1000000\"", "MINUTE_MICROSECOND", "2011-11-11 00:00:01.000000", "2011-11-10 23:59:59.000000"},
		{"cast(\"2011-11-11 00:00:00\" as date)", "1", "DAY", "2011-11-12", "2011-11-10"},
		{"cast(\"2011-11-11 00:00:00\" as date)", "10", "HOUR", "2011-11-11 10:00:00", "2011-11-10 14:00:00"},
		{"cast(\"2011-11-11 00:00:00\" as date)", "10", "MINUTE", "2011-11-11 00:10:00", "2011-11-10 23:50:00"},
		{"cast(\"2011-11-11 00:00:00\" as date)", "10", "SECOND", "2011-11-11 00:00:10", "2011-11-10 23:59:50"},

		{"cast(\"2011-11-11 00:00:00\" as date)", "\"1\"", "DAY", "2011-11-12", "2011-11-10"},
		{"cast(\"2011-11-11 00:00:00\" as date)", "\"10\"", "HOUR", "2011-11-11 10:00:00", "2011-11-10 14:00:00"},
		{"cast(\"2011-11-11 00:00:00\" as date)", "\"10\"", "MINUTE", "2011-11-11 00:10:00", "2011-11-10 23:50:00"},
		{"cast(\"2011-11-11 00:00:00\" as date)", "\"10\"", "SECOND", "2011-11-11 00:00:10.000000", "2011-11-10 23:59:50.000000"},

		// interval decimal support
		{"\"2011-01-01 00:00:00\"", "10.10", "YEAR_MONTH", "2021-11-01 00:00:00", "2000-03-01 00:00:00"},
		{"\"2011-01-01 00:00:00\"", "10.10", "DAY_HOUR", "2011-01-11 10:00:00", "2010-12-21 14:00:00"},
		{"\"2011-01-01 00:00:00\"", "10.10", "HOUR_MINUTE", "2011-01-01 10:10:00", "2010-12-31 13:50:00"},
		{"\"2011-01-01 00:00:00\"", "10.10", "DAY_MINUTE", "2011-01-01 10:10:00", "2010-12-31 13:50:00"},
		{"\"2011-01-01 00:00:00\"", "10.10", "DAY_SECOND", "2011-01-01 00:10:10", "2010-12-31 23:49:50"},
		{"\"2011-01-01 00:00:00\"", "10.10", "HOUR_SECOND", "2011-01-01 00:10:10", "2010-12-31 23:49:50"},
		{"\"2011-01-01 00:00:00\"", "10.10", "MINUTE_SECOND", "2011-01-01 00:10:10", "2010-12-31 23:49:50"},
		{"\"2011-01-01 00:00:00\"", "10.10", "DAY_MICROSECOND", "2011-01-01 00:00:10.100000", "2010-12-31 23:59:49.900000"},
		{"\"2011-01-01 00:00:00\"", "10.10", "HOUR_MICROSECOND", "2011-01-01 00:00:10.100000", "2010-12-31 23:59:49.900000"},
		{"\"2011-01-01 00:00:00\"", "10.10", "MINUTE_MICROSECOND", "2011-01-01 00:00:10.100000", "2010-12-31 23:59:49.900000"},
		{"\"2011-01-01 00:00:00\"", "10.10", "SECOND_MICROSECOND", "2011-01-01 00:00:10.100000", "2010-12-31 23:59:49.900000"},
		{"\"2011-01-01 00:00:00\"", "10.10", "YEAR", "2021-01-01 00:00:00", "2001-01-01 00:00:00"},
		{"\"2011-01-01 00:00:00\"", "10.10", "QUARTER", "2013-07-01 00:00:00", "2008-07-01 00:00:00"},
		{"\"2011-01-01 00:00:00\"", "10.10", "MONTH", "2011-11-01 00:00:00", "2010-03-01 00:00:00"},
		{"\"2011-01-01 00:00:00\"", "10.10", "WEEK", "2011-03-12 00:00:00", "2010-10-23 00:00:00"},
		{"\"2011-01-01 00:00:00\"", "10.10", "DAY", "2011-01-11 00:00:00", "2010-12-22 00:00:00"},
		{"\"2011-01-01 00:00:00\"", "10.10", "HOUR", "2011-01-01 10:00:00", "2010-12-31 14:00:00"},
		{"\"2011-01-01 00:00:00\"", "10.10", "MINUTE", "2011-01-01 00:10:00", "2010-12-31 23:50:00"},
		{"\"2011-01-01 00:00:00\"", "10.10", "SECOND", "2011-01-01 00:00:10.100000", "2010-12-31 23:59:49.900000"},
		{"\"2011-01-01 00:00:00\"", "10.10", "MICROSECOND", "2011-01-01 00:00:00.000010", "2010-12-31 23:59:59.999990"},
		{"\"2011-01-01 00:00:00\"", "10.90", "MICROSECOND", "2011-01-01 00:00:00.000011", "2010-12-31 23:59:59.999989"},
		{"cast(\"2011-01-01\" as date)", "1.1", "SECOND", "2011-01-01 00:00:01.1", "2010-12-31 23:59:58.9"},
		{"cast(\"2011-01-01\" as datetime)", "1.1", "SECOND", "2011-01-01 00:00:01.1", "2010-12-31 23:59:58.9"},
		{"cast(\"2011-01-01\" as datetime(3))", "1.1", "SECOND", "2011-01-01 00:00:01.100", "2010-12-31 23:59:58.900"},

		{"\"2009-01-01\"", "6/4", "HOUR_MINUTE", "2009-01-04 12:20:00", "2008-12-28 11:40:00"},
		{"\"2009-01-01\"", "6/0", "HOUR_MINUTE", "<nil>", "<nil>"},
		{"\"1970-01-01 12:00:00\"", "CAST(6/4 AS DECIMAL(3,1))", "HOUR_MINUTE", "1970-01-01 13:05:00", "1970-01-01 10:55:00"},
		// for issue #8077
		{"\"2012-01-02\"", "\"prefix8\"", "HOUR", "2012-01-02 08:00:00", "2012-01-01 16:00:00"},
		{"\"2012-01-02\"", "\"prefix8prefix\"", "HOUR", "2012-01-02 08:00:00", "2012-01-01 16:00:00"},
		{"\"2012-01-02\"", "\"8:00\"", "HOUR", "2012-01-02 08:00:00", "2012-01-01 16:00:00"},
		{"\"2012-01-02\"", "\"8:00:00\"", "HOUR", "2012-01-02 08:00:00", "2012-01-01 16:00:00"},
	}
	for _, tc := range dateArithmeticalTests {
		addDate := fmt.Sprintf("select adddate(%s, interval %s %s);", tc.Date, tc.Interval, tc.Unit)
		subDate := fmt.Sprintf("select subdate(%s, interval %s %s);", tc.Date, tc.Interval, tc.Unit)
		result = tk.MustQuery(addDate)
		result.Check(testkit.Rows(tc.AddResult))
		result = tk.MustQuery(subDate)
		result.Check(testkit.Rows(tc.SubResult))
	}

	// Customized check for the cases of adddate(time, ...) - it returns datetime with current date padded.
	// 1. Check if the result contains space, that is, it must contain YMD part.
	// 2. Check if the result's suffix matches expected, that is, the HMS part is an exact match.
	checkHmsMatch := func(actual []string, expected []interface{}) bool {
		return strings.Contains(actual[0], " ") && strings.HasSuffix(actual[0], expected[0].(string))
	}

	// for date_add/sub(duration, ...)
	dateAddSubDurationAnyTests := []struct {
		Date         string
		Interval     string
		Unit         string
		AddResult    string
		SubResult    string
		checkHmsOnly bool // Duration + day returns datetime with current date padded, only check HMS part for them.
	}{
		{"cast('01:02:03' as time)", "'1000'", "MICROSECOND", "01:02:03.001000", "01:02:02.999000", false},
		{"cast('01:02:03' as time)", "1000", "MICROSECOND", "01:02:03.001000", "01:02:02.999000", false},
		{"cast('01:02:03' as time)", "'1'", "SECOND", "01:02:04.000000", "01:02:02.000000", false},
		{"cast('01:02:03' as time)", "1", "SECOND", "01:02:04", "01:02:02", false},
		{"cast('01:02:03' as time)", "'1.1'", "SECOND", "01:02:04.100000", "01:02:01.900000", false},
		{"cast('01:02:03' as time)", "1.1", "SECOND", "01:02:04.1", "01:02:01.9", false},
		{"cast('01:02:03' as time(3))", "1.1", "SECOND", "01:02:04.100", "01:02:01.900", false},
		{"cast('01:02:03' as time)", "cast(1.1 as decimal(10, 3))", "SECOND", "01:02:04.100", "01:02:01.900", false},
		{"cast('01:02:03' as time)", "cast('1.5' as double)", "SECOND", "01:02:04.500000", "01:02:01.500000", false},
		{"cast('01:02:03' as time)", "1", "DAY_MICROSECOND", "01:02:03.100000", "01:02:02.900000", false},
		{"cast('01:02:03' as time)", "1.1", "DAY_MICROSECOND", "01:02:04.100000", "01:02:01.900000", false},
		{"cast('01:02:03' as time)", "100", "DAY_MICROSECOND", "01:02:03.100000", "01:02:02.900000", false},
		{"cast('01:02:03' as time)", "1000000", "DAY_MICROSECOND", "01:02:04.000000", "01:02:02.000000", false},
		{"cast('01:02:03' as time)", "1", "DAY_SECOND", "01:02:04", "01:02:02", true},
		{"cast('01:02:03' as time)", "1.1", "DAY_SECOND", "01:03:04", "01:01:02", true},
		{"cast('01:02:03' as time)", "1", "DAY_MINUTE", "01:03:03", "01:01:03", true},
		{"cast('01:02:03' as time)", "1.1", "DAY_MINUTE", "02:03:03", "00:01:03", true},
		{"cast('01:02:03' as time)", "1", "DAY_HOUR", "02:02:03", "00:02:03", true},
		{"cast('01:02:03' as time)", "1.1", "DAY_HOUR", "02:02:03", "00:02:03", true},
		{"cast('01:02:03' as time)", "1", "DAY", "01:02:03", "01:02:03", true},
		{"cast('01:02:03' as time)", "1", "WEEK", "01:02:03", "01:02:03", true},
		{"cast('01:02:03' as time)", "1", "MONTH", "01:02:03", "01:02:03", true},
		{"cast('01:02:03' as time)", "1", "QUARTER", "01:02:03", "01:02:03", true},
		{"cast('01:02:03' as time)", "1", "YEAR", "01:02:03", "01:02:03", true},
		{"cast('01:02:03' as time)", "1", "YEAR_MONTH", "01:02:03", "01:02:03", true},
	}
	for _, tc := range dateAddSubDurationAnyTests {
		addDate := fmt.Sprintf("select date_add(%s, interval %s %s);", tc.Date, tc.Interval, tc.Unit)
		subDate := fmt.Sprintf("select date_sub(%s, interval %s %s);", tc.Date, tc.Interval, tc.Unit)
		if tc.checkHmsOnly {
			result = tk.MustQuery(addDate)
			result.CheckWithFunc(testkit.Rows(tc.AddResult), checkHmsMatch)
			result = tk.MustQuery(subDate)
			result.CheckWithFunc(testkit.Rows(tc.SubResult), checkHmsMatch)
		} else {
			result = tk.MustQuery(addDate)
			result.Check(testkit.Rows(tc.AddResult))
			result = tk.MustQuery(subDate)
			result.Check(testkit.Rows(tc.SubResult))
		}
	}

	tk.MustQuery(`select subdate(cast("2000-02-01" as datetime), cast(1 as decimal))`).Check(testkit.Rows("2000-01-31 00:00:00"))
	tk.MustQuery(`select subdate(cast("2000-02-01" as datetime), cast(null as decimal))`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select subdate(cast(null as datetime), cast(1 as decimal))`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select subdate(cast("2000-02-01" as datetime), cast("xxx" as decimal))`).Check(testkit.Rows("2000-02-01 00:00:00"))
	tk.MustQuery(`select subdate(cast("xxx" as datetime), cast(1 as decimal))`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select subdate(cast(20000101 as SIGNED), cast("1" as decimal))`).Check(testkit.Rows("1999-12-31"))
	tk.MustQuery(`select subdate(cast(20000101 as SIGNED), cast("xxx" as decimal))`).Check(testkit.Rows("2000-01-01"))
	tk.MustQuery(`select subdate(cast("abc" as SIGNED), cast("1" as decimal))`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select subdate(cast(null as SIGNED), cast("1" as decimal))`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select subdate(cast(20000101 as SIGNED), cast(null as decimal))`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select adddate(cast("2000-02-01" as datetime), cast(1 as decimal))`).Check(testkit.Rows("2000-02-02 00:00:00"))
	tk.MustQuery(`select adddate(cast("2000-02-01" as datetime), cast(null as decimal))`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select adddate(cast(null as datetime), cast(1 as decimal))`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select adddate(cast("2000-02-01" as datetime), cast("xxx" as decimal))`).Check(testkit.Rows("2000-02-01 00:00:00"))
	tk.MustQuery(`select adddate(cast("xxx" as datetime), cast(1 as decimal))`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select adddate(cast("2000-02-01" as datetime), cast(1 as SIGNED))`).Check(testkit.Rows("2000-02-02 00:00:00"))
	tk.MustQuery(`select adddate(cast("2000-02-01" as datetime), cast(null as SIGNED))`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select adddate(cast(null as datetime), cast(1 as SIGNED))`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select adddate(cast("2000-02-01" as datetime), cast("xxx" as SIGNED))`).Check(testkit.Rows("2000-02-01 00:00:00"))
	tk.MustQuery(`select adddate(cast("xxx" as datetime), cast(1 as SIGNED))`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select adddate(20100101, cast(1 as decimal))`).Check(testkit.Rows("2010-01-02"))
	tk.MustQuery(`select adddate(cast('10:10:10' as time), 1)`).CheckWithFunc(testkit.Rows("10:10:10"), checkHmsMatch)
	tk.MustQuery(`select adddate(cast('10:10:10' as time), cast(1 as decimal))`).CheckWithFunc(testkit.Rows("10:10:10"), checkHmsMatch)

	// for localtime, localtimestamp
	result = tk.MustQuery(`select localtime() = now(), localtime = now(), localtimestamp() = now(), localtimestamp = now()`)
	result.Check(testkit.Rows("1 1 1 1"))

	// for current_timestamp, current_timestamp()
	result = tk.MustQuery(`select current_timestamp() = now(), current_timestamp = now()`)
	result.Check(testkit.Rows("1 1"))

	// for tidb_parse_tso
	tk.MustExec("SET time_zone = '+00:00';")
	result = tk.MustQuery(`select tidb_parse_tso(404411537129996288)`)
	result.Check(testkit.Rows("2018-11-20 09:53:04.877000"))
	result = tk.MustQuery(`select tidb_parse_tso("404411537129996288")`)
	result.Check(testkit.Rows("2018-11-20 09:53:04.877000"))
	result = tk.MustQuery(`select tidb_parse_tso(1)`)
	result.Check(testkit.Rows("1970-01-01 00:00:00.000000"))
	result = tk.MustQuery(`select tidb_parse_tso(0)`)
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery(`select tidb_parse_tso(-1)`)
	result.Check(testkit.Rows("<nil>"))

	// for tidb_parse_tso_logical
	result = tk.MustQuery(`SELECT TIDB_PARSE_TSO_LOGICAL(404411537129996288)`)
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery(`SELECT TIDB_PARSE_TSO_LOGICAL(404411537129996289)`)
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery(`SELECT TIDB_PARSE_TSO_LOGICAL(404411537129996290)`)
	result.Check(testkit.Rows("2"))
	result = tk.MustQuery(`SELECT TIDB_PARSE_TSO_LOGICAL(-1)`)
	result.Check(testkit.Rows("<nil>"))

	// for tidb_bounded_staleness
	tk.MustExec("SET time_zone = '+00:00';")
	tt := time.Now().UTC()
	ts := oracle.GoTimeToTS(tt)
	tidbBoundedStalenessTests := []struct {
		sql          string
		injectSafeTS uint64
		expect       string
	}{
		{
			sql:          `select tidb_bounded_staleness(DATE_SUB(NOW(), INTERVAL 600 SECOND), DATE_ADD(NOW(), INTERVAL 600 SECOND))`,
			injectSafeTS: ts,
			expect:       tt.Format(types.TimeFSPFormat[:len(types.TimeFSPFormat)-3]),
		},
		{
			sql: `select tidb_bounded_staleness("2021-04-27 12:00:00.000", "2021-04-27 13:00:00.000")`,
			injectSafeTS: func() uint64 {
				tt, err := time.Parse("2006-01-02 15:04:05.000", "2021-04-27 13:30:04.877")
				require.NoError(t, err)
				return oracle.GoTimeToTS(tt)
			}(),
			expect: "2021-04-27 13:00:00.000",
		},
		{
			sql: `select tidb_bounded_staleness("2021-04-27 12:00:00.000", "2021-04-27 13:00:00.000")`,
			injectSafeTS: func() uint64 {
				tt, err := time.Parse("2006-01-02 15:04:05.000", "2021-04-27 11:30:04.877")
				require.NoError(t, err)
				return oracle.GoTimeToTS(tt)
			}(),
			expect: "2021-04-27 12:00:00.000",
		},
		{
			sql:          `select tidb_bounded_staleness("2021-04-27 12:00:00.000", "2021-04-27 11:00:00.000")`,
			injectSafeTS: 0,
			expect:       "<nil>",
		},
		// Time is too small.
		{
			sql:          `select tidb_bounded_staleness("0020-04-27 12:00:00.000", "2021-04-27 11:00:00.000")`,
			injectSafeTS: 0,
			expect:       "1970-01-01 00:00:00.000",
		},
		// Wrong value.
		{
			sql:          `select tidb_bounded_staleness(1, 2)`,
			injectSafeTS: 0,
			expect:       "<nil>",
		},
		{
			sql:          `select tidb_bounded_staleness("invalid_time_1", "invalid_time_2")`,
			injectSafeTS: 0,
			expect:       "<nil>",
		},
	}
	for _, test := range tidbBoundedStalenessTests {
		require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/expression/injectSafeTS",
			fmt.Sprintf("return(%v)", test.injectSafeTS)))
		tk.MustQuery(test.sql).Check(testkit.Rows(test.expect))
	}
	failpoint.Disable("github.com/pingcap/tidb/expression/injectSafeTS")
	// test whether tidb_bounded_staleness is deterministic
	result = tk.MustQuery(`select tidb_bounded_staleness(NOW(), DATE_ADD(NOW(), INTERVAL 600 SECOND)), tidb_bounded_staleness(NOW(), DATE_ADD(NOW(), INTERVAL 600 SECOND))`)
	require.Len(t, result.Rows()[0], 2)
	require.Equal(t, result.Rows()[0][0], result.Rows()[0][1])
	preResult := result.Rows()[0][0]
	time.Sleep(time.Second)
	result = tk.MustQuery(`select tidb_bounded_staleness(NOW(), DATE_ADD(NOW(), INTERVAL 600 SECOND)), tidb_bounded_staleness(NOW(), DATE_ADD(NOW(), INTERVAL 600 SECOND))`)
	require.Len(t, result.Rows()[0], 2)
	require.Equal(t, result.Rows()[0][0], result.Rows()[0][1])
	require.NotEqual(t, preResult, result.Rows()[0][0])

	// fix issue 10308
	result = tk.MustQuery("select time(\"- -\");")
	result.Check(testkit.Rows("00:00:00"))
	tk.MustQuery("show warnings;").Check(testkit.Rows("Warning 1292 Truncated incorrect time value: '- -'"))
	result = tk.MustQuery("select time(\"---1\");")
	result.Check(testkit.Rows("00:00:00"))
	tk.MustQuery("show warnings;").Check(testkit.Rows("Warning 1292 Truncated incorrect time value: '---1'"))
	result = tk.MustQuery("select time(\"-- --1\");")
	result.Check(testkit.Rows("00:00:00"))
	tk.MustQuery("show warnings;").Check(testkit.Rows("Warning 1292 Truncated incorrect time value: '-- --1'"))

	// fix issue #15185
	result = tk.MustQuery(`select timestamp(11111.1111)`)
	result.Check(testkit.Rows("2001-11-11 00:00:00.0000"))
	result = tk.MustQuery(`select timestamp(cast(11111.1111 as decimal(60, 5)))`)
	result.Check(testkit.Rows("2001-11-11 00:00:00.00000"))
	result = tk.MustQuery(`select timestamp(1021121141105.4324)`)
	result.Check(testkit.Rows("0102-11-21 14:11:05.4324"))
	result = tk.MustQuery(`select timestamp(cast(1021121141105.4324 as decimal(60, 5)))`)
	result.Check(testkit.Rows("0102-11-21 14:11:05.43240"))
	result = tk.MustQuery(`select timestamp(21121141105.101)`)
	result.Check(testkit.Rows("2002-11-21 14:11:05.101"))
	result = tk.MustQuery(`select timestamp(cast(21121141105.101 as decimal(60, 5)))`)
	result.Check(testkit.Rows("2002-11-21 14:11:05.10100"))
	result = tk.MustQuery(`select timestamp(1121141105.799055)`)
	result.Check(testkit.Rows("2000-11-21 14:11:05.799055"))
	result = tk.MustQuery(`select timestamp(cast(1121141105.799055 as decimal(60, 5)))`)
	result.Check(testkit.Rows("2000-11-21 14:11:05.79906"))
	result = tk.MustQuery(`select timestamp(121141105.123)`)
	result.Check(testkit.Rows("2000-01-21 14:11:05.123"))
	result = tk.MustQuery(`select timestamp(cast(121141105.123 as decimal(60, 5)))`)
	result.Check(testkit.Rows("2000-01-21 14:11:05.12300"))
	result = tk.MustQuery(`select timestamp(1141105)`)
	result.Check(testkit.Rows("0114-11-05 00:00:00"))
	result = tk.MustQuery(`select timestamp(cast(1141105 as decimal(60, 5)))`)
	result.Check(testkit.Rows("0114-11-05 00:00:00.00000"))
	result = tk.MustQuery(`select timestamp(41105.11)`)
	result.Check(testkit.Rows("2004-11-05 00:00:00.00"))
	result = tk.MustQuery(`select timestamp(cast(41105.11 as decimal(60, 5)))`)
	result.Check(testkit.Rows("2004-11-05 00:00:00.00000"))
	result = tk.MustQuery(`select timestamp(1105.3)`)
	result.Check(testkit.Rows("2000-11-05 00:00:00.0"))
	result = tk.MustQuery(`select timestamp(cast(1105.3 as decimal(60, 5)))`)
	result.Check(testkit.Rows("2000-11-05 00:00:00.00000"))
	result = tk.MustQuery(`select timestamp(105)`)
	result.Check(testkit.Rows("2000-01-05 00:00:00"))
	result = tk.MustQuery(`select timestamp(cast(105 as decimal(60, 5)))`)
	result.Check(testkit.Rows("2000-01-05 00:00:00.00000"))
}

func TestSetVariables(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	_, err := tk.Exec("set sql_mode='adfasdfadsfdasd';")
	require.Error(t, err)
	_, err = tk.Exec("set @@sql_mode='adfasdfadsfdasd';")
	require.Error(t, err)
	_, err = tk.Exec("set @@global.sql_mode='adfasdfadsfdasd';")
	require.Error(t, err)
	_, err = tk.Exec("set @@session.sql_mode='adfasdfadsfdasd';")
	require.Error(t, err)

	var r *testkit.Result
	_, err = tk.Exec("set @@session.sql_mode=',NO_ZERO_DATE,ANSI,ANSI_QUOTES';")
	require.NoError(t, err)
	r = tk.MustQuery(`select @@session.sql_mode`)
	r.Check(testkit.Rows("NO_ZERO_DATE,REAL_AS_FLOAT,PIPES_AS_CONCAT,ANSI_QUOTES,IGNORE_SPACE,ONLY_FULL_GROUP_BY,ANSI"))
	r = tk.MustQuery(`show variables like 'sql_mode'`)
	r.Check(testkit.Rows("sql_mode NO_ZERO_DATE,REAL_AS_FLOAT,PIPES_AS_CONCAT,ANSI_QUOTES,IGNORE_SPACE,ONLY_FULL_GROUP_BY,ANSI"))

	// for invalid SQL mode.
	tk.MustExec("use test")
	tk.MustExec("drop table if exists tab0")
	tk.MustExec("CREATE TABLE tab0(col1 time)")
	_, err = tk.Exec("set sql_mode='STRICT_TRANS_TABLES';")
	require.NoError(t, err)
	_, err = tk.Exec("INSERT INTO tab0 select cast('999:44:33' as time);")
	require.Error(t, err)
	require.Error(t, err, "[types:1292]Truncated incorrect time value: '999:44:33'")
	_, err = tk.Exec("set sql_mode=' ,';")
	require.Error(t, err)
	_, err = tk.Exec("INSERT INTO tab0 select cast('999:44:33' as time);")
	require.Error(t, err)
	require.Error(t, err, "[types:1292]Truncated incorrect time value: '999:44:33'")

	// issue #5478
	_, err = tk.Exec("set session transaction read write;")
	require.NoError(t, err)
	_, err = tk.Exec("set global transaction read write;")
	require.NoError(t, err)
	r = tk.MustQuery(`select @@session.tx_read_only, @@global.tx_read_only, @@session.transaction_read_only, @@global.transaction_read_only;`)
	r.Check(testkit.Rows("0 0 0 0"))

	_, err = tk.Exec("set session transaction read only;")
	require.Error(t, err)

	_, err = tk.Exec("start transaction read only;")
	require.Error(t, err)

	_, err = tk.Exec("set tidb_enable_noop_functions=1")
	require.NoError(t, err)

	tk.MustExec("set session transaction read only;")
	tk.MustExec("start transaction read only;")

	r = tk.MustQuery(`select @@session.tx_read_only, @@global.tx_read_only, @@session.transaction_read_only, @@global.transaction_read_only;`)
	r.Check(testkit.Rows("1 0 1 0"))
	_, err = tk.Exec("set global transaction read only;")
	require.Error(t, err)
	tk.MustExec("set global tidb_enable_noop_functions=1;")
	tk.MustExec("set global transaction read only;")
	r = tk.MustQuery(`select @@session.tx_read_only, @@global.tx_read_only, @@session.transaction_read_only, @@global.transaction_read_only;`)
	r.Check(testkit.Rows("1 1 1 1"))

	_, err = tk.Exec("set session transaction read write;")
	require.NoError(t, err)
	_, err = tk.Exec("set global transaction read write;")
	require.NoError(t, err)
	r = tk.MustQuery(`select @@session.tx_read_only, @@global.tx_read_only, @@session.transaction_read_only, @@global.transaction_read_only;`)
	r.Check(testkit.Rows("0 0 0 0"))

	// reset
	tk.MustExec("set tidb_enable_noop_functions=0")
	tk.MustExec("set global tidb_enable_noop_functions=1")

	_, err = tk.Exec("set @@global.max_user_connections='';")
	require.Error(t, err)
	require.Error(t, err, variable.ErrWrongTypeForVar.GenWithStackByArgs("max_user_connections").Error())
	_, err = tk.Exec("set @@global.max_prepared_stmt_count='';")
	require.Error(t, err)
	require.Error(t, err, variable.ErrWrongTypeForVar.GenWithStackByArgs("max_prepared_stmt_count").Error())
}

func TestPreparePlanCache(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set tidb_enable_prepared_plan_cache=ON")
	// Use the example from the docs https://docs.pingcap.com/tidb/stable/sql-prepare-plan-cache
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int);")
	tk.MustExec("prepare stmt from 'select * from t where a = ?';")
	tk.MustExec("set @a = 1;")
	tk.MustExec("execute stmt using @a;")
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("0"))
	tk.MustExec("execute stmt using @a;")
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("1"))
}

func TestPreparePlanCacheOnCachedTable(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set tidb_enable_prepared_plan_cache=ON")
	tk.Session()

	var err error
	se, err := session.CreateSession4TestWithOpt(store, &session.Opt{
		PreparedPlanCache: plannercore.NewLRUPlanCache(100, 0.1, math.MaxUint64, tk.Session(), false),
	})
	require.NoError(t, err)
	tk.SetSession(se)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int);")
	tk.MustExec("alter table t cache")

	var readFromTableCache bool
	for i := 0; i < 50; i++ {
		tk.MustQuery("select * from t where a = 1")
		if tk.Session().GetSessionVars().StmtCtx.ReadFromTableCache {
			readFromTableCache = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	require.True(t, readFromTableCache)
	// already read cache after reading first time
	tk.MustExec("prepare stmt from 'select * from t where a = ?';")
	tk.MustExec("set @a = 1;")
	tk.MustExec("execute stmt using @a;")
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("0"))
	tk.MustExec("execute stmt using @a;")
	readFromTableCache = tk.Session().GetSessionVars().StmtCtx.ReadFromTableCache
	require.True(t, readFromTableCache)
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("1"))
}

func TestIssue16205(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set tidb_enable_prepared_plan_cache=ON")
	tk.MustExec("use test")
	tk.MustExec("prepare stmt from 'select random_bytes(3)'")
	rows1 := tk.MustQuery("execute stmt").Rows()
	require.Len(t, rows1, 1)
	rows2 := tk.MustQuery("execute stmt").Rows()
	require.Len(t, rows2, 1)
	require.NotEqual(t, rows1[0][0].(string), rows2[0][0].(string))
}

func TestRowCountPlanCache(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set tidb_enable_prepared_plan_cache=ON")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int auto_increment primary key)")
	tk.MustExec("prepare stmt from 'select row_count()';")
	tk.MustExec("insert into t values()")
	res := tk.MustQuery("execute stmt").Rows()
	require.Len(t, res, 1)
	require.Equal(t, "1", res[0][0])
	tk.MustExec("insert into t values(),(),()")
	res = tk.MustQuery("execute stmt").Rows()
	require.Len(t, res, 1)
	require.Equal(t, "3", res[0][0])
}

func TestCacheRegexpr(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set tidb_enable_prepared_plan_cache=ON")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a varchar(40))")
	tk.MustExec("insert into t1 values ('C1'),('R1')")
	tk.MustExec("prepare stmt1 from 'select a from t1 where a rlike ?'")
	tk.MustExec("set @a='^C.*'")
	tk.MustQuery("execute stmt1 using @a").Check(testkit.Rows("C1"))
	tk.MustExec("set @a='^R.*'")
	tk.MustQuery("execute stmt1 using @a").Check(testkit.Rows("R1"))
}

func TestCacheRefineArgs(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set tidb_enable_prepared_plan_cache=ON")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(col_int int)")
	tk.MustExec("insert into t values(null)")
	tk.MustExec("prepare stmt from 'SELECT ((col_int is true) = ?) AS res FROM t'")
	tk.MustExec("set @p0='0.8'")
	tk.MustQuery("execute stmt using @p0").Check(testkit.Rows("0"))
	tk.MustExec("set @p0='0'")
	tk.MustQuery("execute stmt using @p0").Check(testkit.Rows("1"))

	tk.MustExec("prepare stmt from 'SELECT UCASE(?) < col_int from t;';")
	tk.MustExec("set @a1 = 'xayh7vrWVNqZtzlJmdJQUwAHnkI8Ec';")
	tk.MustQuery("execute stmt using @a1;").Check(testkit.Rows("<nil>"))

	tk.MustExec("delete from t")
	tk.MustExec("insert into t values(1)")
	tk.MustExec("prepare stmt from 'SELECT col_int < ? FROM t'")
	tk.MustExec("set @p0='-184467440737095516167.1'")
	tk.MustQuery("execute stmt using @p0").Check(testkit.Rows("0"))
}

func TestCacheConstEval(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set tidb_enable_prepared_plan_cache=ON")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(col_double double)")
	tk.MustExec("insert into t values (1)")
	tk.Session().GetSessionVars().EnableVectorizedExpression = false
	tk.MustExec("insert into mysql.expr_pushdown_blacklist values('cast', 'tikv,tiflash,tidb', 'for test')")
	tk.MustExec("admin reload expr_pushdown_blacklist")
	tk.MustExec("prepare stmt from 'SELECT * FROM (SELECT col_double AS c0 FROM t) t WHERE (ABS((REPEAT(?, ?) OR 5617780767323292672)) < LN(EXP(c0)) + (? ^ ?))'")
	tk.MustExec("set @a1 = 'JuvkBX7ykVux20zQlkwDK2DFelgn7'")
	tk.MustExec("set @a2 = 1")
	tk.MustExec("set @a3 = -112990.35179796701")
	tk.MustExec("set @a4 = 87997.92704840179")
	// Main purpose here is checking no error is reported. 1 is the result when plan cache is disabled, it is
	// incompatible with MySQL actually, update the result after fixing it.
	tk.MustQuery("execute stmt using @a1, @a2, @a3, @a4").Check(testkit.Rows("1"))
	tk.Session().GetSessionVars().EnableVectorizedExpression = true
	tk.MustExec("delete from mysql.expr_pushdown_blacklist where name = 'cast' and store_type = 'tikv,tiflash,tidb' and reason = 'for test'")
	tk.MustExec("admin reload expr_pushdown_blacklist")
}

// issues 14448, 19383, 17734
func TestNoopFunctions(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`set tidb_enable_non_prepared_plan_cache=0`) // variable changes in the test will not affect the plan cache
	tk.MustExec("DROP TABLE IF EXISTS t1")
	tk.MustExec("CREATE TABLE t1 (a INT NOT NULL PRIMARY KEY)")
	tk.MustExec("INSERT INTO t1 VALUES (1),(2),(3)")

	message := `.* has only noop implementation in tidb now, use tidb_enable_noop_functions to enable these functions`
	stmts := []string{
		"SELECT SQL_CALC_FOUND_ROWS * FROM t1 LIMIT 1",
		"SELECT * FROM t1 LOCK IN SHARE MODE",
		"SELECT * FROM t1 GROUP BY a DESC",
		"SELECT * FROM t1 GROUP BY a ASC",
	}

	for _, stmt := range stmts {
		// test on
		tk.MustExec("SET tidb_enable_noop_functions='ON'")
		tk.MustExec(stmt)
		// test warning
		tk.MustExec("SET tidb_enable_noop_functions='WARN'")
		tk.MustExec(stmt)
		warn := tk.Session().GetSessionVars().StmtCtx.GetWarnings()
		require.Regexp(t, message, warn[0].Err.Error())
		// test off
		tk.MustExec("SET tidb_enable_noop_functions='OFF'")
		_, err := tk.Exec(stmt)
		require.Regexp(t, message, err.Error())
	}

	// These statements return a different error message
	// to the above. Test for error, not specifically the message.
	// After they execute, we need to reset the values because
	// otherwise tidb_enable_noop_functions can't be changed.

	stmts = []string{
		"START TRANSACTION READ ONLY",
		"SET TRANSACTION READ ONLY",
		"SET tx_read_only = 1",
		"SET transaction_read_only = 1",
	}

	for _, stmt := range stmts {
		// test off
		tk.MustExec("SET tidb_enable_noop_functions='OFF'")
		_, err := tk.Exec(stmt)
		require.Error(t, err)
		// test warning
		tk.MustExec("SET tidb_enable_noop_functions='WARN'")
		tk.MustExec(stmt)
		warn := tk.Session().GetSessionVars().StmtCtx.GetWarnings()
		require.Len(t, warn, 1)
		// test on
		tk.MustExec("SET tidb_enable_noop_functions='ON'")
		tk.MustExec(stmt)

		// Reset (required for future loop iterations and future tests)
		tk.MustExec("SET tx_read_only = 0")
		tk.MustExec("SET transaction_read_only = 0")
	}
}

func TestCrossDCQuery(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop placement policy if exists p1")
	tk.MustExec("drop placement policy if exists p2")
	tk.MustExec("create placement policy p1 leader_constraints='[+zone=sh]'")
	tk.MustExec("create placement policy p2 leader_constraints='[+zone=bj]'")
	tk.MustExec(`create table t1 (c int primary key, d int,e int,index idx_d(d),index idx_e(e))
PARTITION BY RANGE (c) (
	PARTITION p0 VALUES LESS THAN (6) placement policy p1,
	PARTITION p1 VALUES LESS THAN (11) placement policy p2
);`)
	defer func() {
		tk.MustExec("drop table if exists t1")
		tk.MustExec("drop placement policy if exists p1")
		tk.MustExec("drop placement policy if exists p2")
	}()

	tk.MustExec(`insert into t1 (c,d,e) values (1,1,1);`)
	tk.MustExec(`insert into t1 (c,d,e) values (2,3,5);`)
	tk.MustExec(`insert into t1 (c,d,e) values (3,5,7);`)

	testcases := []struct {
		name      string
		txnScope  string
		zone      string
		sql       string
		expectErr error
	}{
		// FIXME: block by https://github.com/pingcap/tidb/issues/21872
		//{
		//	name:      "cross dc read to sh by holding bj, IndexReader",
		//	txnScope:  "bj",
		//	sql:       "select /*+ USE_INDEX(t1, idx_d) */ d from t1 where c < 5 and d < 1;",
		//	expectErr: fmt.Errorf(".*can not be read by.*"),
		//},
		// FIXME: block by https://github.com/pingcap/tidb/issues/21847
		//{
		//	name:      "cross dc read to sh by holding bj, BatchPointGet",
		//	txnScope:  "bj",
		//	sql:       "select * from t1 where c in (1,2,3,4);",
		//	expectErr: fmt.Errorf(".*can not be read by.*"),
		//},
		{
			name:      "cross dc read to sh by holding bj, PointGet",
			txnScope:  "local",
			zone:      "bj",
			sql:       "select * from t1 where c = 1",
			expectErr: fmt.Errorf(".*can not be read by.*"),
		},
		{
			name:      "cross dc read to sh by holding bj, IndexLookUp",
			txnScope:  "local",
			zone:      "bj",
			sql:       "select * from t1 use index (idx_d) where c < 5 and d < 5;",
			expectErr: fmt.Errorf(".*can not be read by.*"),
		},
		{
			name:      "cross dc read to sh by holding bj, IndexMerge",
			txnScope:  "local",
			zone:      "bj",
			sql:       "select /*+ USE_INDEX_MERGE(t1, idx_d, idx_e) */ * from t1 where c <5 and (d =5 or e=5);",
			expectErr: fmt.Errorf(".*can not be read by.*"),
		},
		{
			name:      "cross dc read to sh by holding bj, TableReader",
			txnScope:  "local",
			zone:      "bj",
			sql:       "select * from t1 where c < 6",
			expectErr: fmt.Errorf(".*can not be read by.*"),
		},
		{
			name:      "cross dc read to global by holding bj",
			txnScope:  "local",
			zone:      "bj",
			sql:       "select * from t1",
			expectErr: fmt.Errorf(".*can not be read by.*"),
		},
		{
			name:      "read sh dc by holding sh",
			txnScope:  "local",
			zone:      "sh",
			sql:       "select * from t1 where c < 6",
			expectErr: nil,
		},
		{
			name:      "read sh dc by holding global",
			txnScope:  "global",
			zone:      "",
			sql:       "select * from t1 where c < 6",
			expectErr: nil,
		},
	}
	tk.MustExec("set global tidb_enable_local_txn = on;")
	for _, testcase := range testcases {
		t.Log(testcase.name)
		require.NoError(t, failpoint.Enable("tikvclient/injectTxnScope",
			fmt.Sprintf(`return("%v")`, testcase.zone)))
		tk.MustExec(fmt.Sprintf("set @@txn_scope='%v'", testcase.txnScope))
		tk.Exec("begin")
		res, err := tk.Exec(testcase.sql)
		_, resErr := session.GetRows4Test(context.Background(), tk.Session(), res)
		var checkErr error
		if err != nil {
			checkErr = err
		} else {
			checkErr = resErr
		}
		if testcase.expectErr != nil {
			require.Error(t, checkErr)
			require.Regexp(t, ".*can not be read by.*", checkErr.Error())
		} else {
			require.NoError(t, checkErr)
		}
		if res != nil {
			res.Close()
		}
		tk.Exec("commit")
	}
	require.NoError(t, failpoint.Disable("tikvclient/injectTxnScope"))
	tk.MustExec("set global tidb_enable_local_txn = off;")
}

func TestTiDBRowChecksumBuiltin(t *testing.T) {
	store := testkit.CreateMockStore(t)

	checksum := func(cols ...interface{}) uint32 {
		buf := make([]byte, 0, 64)
		for _, col := range cols {
			switch x := col.(type) {
			case int:
				buf = binary.LittleEndian.AppendUint64(buf, uint64(x))
			case string:
				buf = binary.LittleEndian.AppendUint32(buf, uint32(len(x)))
				buf = append(buf, []byte(x)...)
			}
		}
		return crc32.ChecksumIEEE(buf)
	}

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set global tidb_enable_row_level_checksum = 1")
	tk.MustExec("use test")
	tk.MustExec("create table t (id int primary key, c int)")

	// row with 2 checksums
	tk.MustExec("insert into t values (1, 10)")
	tk.MustExec("alter table t change column c c varchar(10)")
	checksum1 := fmt.Sprintf("%d,%d", checksum(1, 10), checksum(1, "10"))
	// row with 1 checksum
	tk.Session().GetSessionVars().EnableRowLevelChecksum = true
	tk.MustExec("insert into t values (2, '20')")
	checksum2 := fmt.Sprintf("%d", checksum(2, "20"))
	// row without checksum
	tk.Session().GetSessionVars().EnableRowLevelChecksum = false
	tk.MustExec("insert into t values (3, '30')")
	checksum3 := "<nil>"

	// fast point-get
	tk.MustQuery("select tidb_row_checksum() from t where id = 1").Check(testkit.Rows(checksum1))
	tk.MustQuery("select tidb_row_checksum() from t where id = 2").Check(testkit.Rows(checksum2))
	tk.MustQuery("select tidb_row_checksum() from t where id = 3").Check(testkit.Rows(checksum3))
	// fast batch-point-get
	tk.MustQuery("select tidb_row_checksum() from t where id in (1, 2, 3)").Check(testkit.Rows(checksum1, checksum2, checksum3))

	// non-fast point-get
	tk.MustGetDBError("select length(tidb_row_checksum()) from t where id = 1", expression.ErrNotSupportedYet)
	tk.MustGetDBError("select c from t where id = 1 and tidb_row_checksum() is not null", expression.ErrNotSupportedYet)
	// non-fast batch-point-get
	tk.MustGetDBError("select length(tidb_row_checksum()) from t where id in (1, 2, 3)", expression.ErrNotSupportedYet)
	tk.MustGetDBError("select c from t where id in (1, 2, 3) and tidb_row_checksum() is not null", expression.ErrNotSupportedYet)

	// other plans
	tk.MustGetDBError("select tidb_row_checksum() from t", expression.ErrNotSupportedYet)
	tk.MustGetDBError("select tidb_row_checksum() from t where id > 0", expression.ErrNotSupportedYet)
}
