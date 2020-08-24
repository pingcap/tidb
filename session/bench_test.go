// Copyright 2016 PingCAP, Inc.
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

package session

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var smallCount = 100
var bigCount = 10000

func prepareBenchSession() (Session, *domain.Domain, kv.Storage) {
	store, err := mockstore.NewMockStore()
	if err != nil {
		logutil.BgLogger().Fatal(err.Error())
	}
	domain, err := BootstrapSession(store)
	if err != nil {
		logutil.BgLogger().Fatal(err.Error())
	}
	log.SetLevel(zapcore.ErrorLevel)
	se, err := CreateSession4Test(store)
	if err != nil {
		logutil.BgLogger().Fatal(err.Error())
	}
	mustExecute(se, "use test")
	return se, domain, store
}

func prepareBenchData(se Session, colType string, valueFormat string, valueCount int) {
	mustExecute(se, "drop table if exists t")
	mustExecute(se, fmt.Sprintf("create table t (pk int primary key auto_increment, col %s, index idx (col))", colType))
	mustExecute(se, "begin")
	for i := 0; i < valueCount; i++ {
		mustExecute(se, "insert t (col) values ("+fmt.Sprintf(valueFormat, i)+")")
	}
	mustExecute(se, "commit")
}

func prepareSortBenchData(se Session, colType string, valueFormat string, valueCount int) {
	mustExecute(se, "drop table if exists t")
	mustExecute(se, fmt.Sprintf("create table t (pk int primary key auto_increment, col %s)", colType))
	mustExecute(se, "begin")
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < valueCount; i++ {
		if i%1000 == 0 {
			mustExecute(se, "commit")
			mustExecute(se, "begin")
		}
		mustExecute(se, "insert t (col) values ("+fmt.Sprintf(valueFormat, r.Intn(valueCount))+")")
	}
	mustExecute(se, "commit")
}

func prepareJoinBenchData(se Session, colType string, valueFormat string, valueCount int) {
	mustExecute(se, "drop table if exists t")
	mustExecute(se, fmt.Sprintf("create table t (pk int primary key auto_increment, col %s)", colType))
	mustExecute(se, "begin")
	for i := 0; i < valueCount; i++ {
		mustExecute(se, "insert t (col) values ("+fmt.Sprintf(valueFormat, i)+")")
	}
	mustExecute(se, "commit")
}

func readResult(ctx context.Context, rs sqlexec.RecordSet, count int) {
	req := rs.NewChunk()
	for count > 0 {
		err := rs.Next(ctx, req)
		if err != nil {
			logutil.Logger(ctx).Fatal("read result failed", zap.Error(err))
		}
		if req.NumRows() == 0 {
			logutil.Logger(ctx).Fatal(strconv.Itoa(count))
		}
		count -= req.NumRows()
	}
	rs.Close()
}

func BenchmarkBasic(b *testing.B) {
	ctx := context.Background()
	se, do, st := prepareBenchSession()
	defer func() {
		se.Close()
		st.Close()
		do.Close()
	}()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rs, err := se.Execute(ctx, "select 1")
		if err != nil {
			b.Fatal(err)
		}
		readResult(ctx, rs[0], 1)
	}
	b.StopTimer()
}

func BenchmarkTableScan(b *testing.B) {
	ctx := context.Background()
	se, do, st := prepareBenchSession()
	defer func() {
		se.Close()
		st.Close()
		do.Close()
	}()
	prepareBenchData(se, "int", "%v", smallCount)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rs, err := se.Execute(ctx, "select * from t")
		if err != nil {
			b.Fatal(err)
		}
		readResult(ctx, rs[0], smallCount)
	}
	b.StopTimer()
}

func BenchmarkExplainTableScan(b *testing.B) {
	ctx := context.Background()
	se, do, st := prepareBenchSession()
	defer func() {
		se.Close()
		st.Close()
		do.Close()
	}()
	prepareBenchData(se, "int", "%v", 0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rs, err := se.Execute(ctx, "explain select * from t")
		if err != nil {
			b.Fatal(err)
		}
		readResult(ctx, rs[0], 1)
	}
	b.StopTimer()
}

func BenchmarkTableLookup(b *testing.B) {
	ctx := context.Background()
	se, do, st := prepareBenchSession()
	defer func() {
		se.Close()
		st.Close()
		do.Close()
	}()
	prepareBenchData(se, "int", "%d", smallCount)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rs, err := se.Execute(ctx, "select * from t where pk = 64")
		if err != nil {
			b.Fatal(err)
		}
		readResult(ctx, rs[0], 1)
	}
	b.StopTimer()
}

func BenchmarkExplainTableLookup(b *testing.B) {
	ctx := context.Background()
	se, do, st := prepareBenchSession()
	defer func() {
		se.Close()
		st.Close()
		do.Close()
	}()
	prepareBenchData(se, "int", "%d", 0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rs, err := se.Execute(ctx, "explain select * from t where pk = 64")
		if err != nil {
			b.Fatal(err)
		}
		readResult(ctx, rs[0], 1)
	}
	b.StopTimer()
}

func BenchmarkStringIndexScan(b *testing.B) {
	ctx := context.Background()
	se, do, st := prepareBenchSession()
	defer func() {
		se.Close()
		st.Close()
		do.Close()
	}()
	prepareBenchData(se, "varchar(255)", "'hello %d'", smallCount)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rs, err := se.Execute(ctx, "select * from t where col > 'hello'")
		if err != nil {
			b.Fatal(err)
		}
		readResult(ctx, rs[0], smallCount)
	}
	b.StopTimer()
}

func BenchmarkExplainStringIndexScan(b *testing.B) {
	ctx := context.Background()
	se, do, st := prepareBenchSession()
	defer func() {
		se.Close()
		st.Close()
		do.Close()
	}()
	prepareBenchData(se, "varchar(255)", "'hello %d'", 0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rs, err := se.Execute(ctx, "explain select * from t where col > 'hello'")
		if err != nil {
			b.Fatal(err)
		}
		readResult(ctx, rs[0], 1)
	}
	b.StopTimer()
}

func BenchmarkStringIndexLookup(b *testing.B) {
	ctx := context.Background()
	se, do, st := prepareBenchSession()
	defer func() {
		se.Close()
		st.Close()
		do.Close()
	}()
	prepareBenchData(se, "varchar(255)", "'hello %d'", smallCount)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rs, err := se.Execute(ctx, "select * from t where col = 'hello 64'")
		if err != nil {
			b.Fatal(err)
		}
		readResult(ctx, rs[0], 1)
	}
	b.StopTimer()
}

func BenchmarkIntegerIndexScan(b *testing.B) {
	ctx := context.Background()
	se, do, st := prepareBenchSession()
	defer func() {
		se.Close()
		st.Close()
		do.Close()
	}()
	prepareBenchData(se, "int", "%v", smallCount)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rs, err := se.Execute(ctx, "select * from t where col >= 0")
		if err != nil {
			b.Fatal(err)
		}
		readResult(ctx, rs[0], smallCount)
	}
	b.StopTimer()
}

func BenchmarkIntegerIndexLookup(b *testing.B) {
	ctx := context.Background()
	se, do, st := prepareBenchSession()
	defer func() {
		se.Close()
		st.Close()
		do.Close()
	}()
	prepareBenchData(se, "int", "%v", smallCount)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rs, err := se.Execute(ctx, "select * from t where col = 64")
		if err != nil {
			b.Fatal(err)
		}
		readResult(ctx, rs[0], 1)
	}
	b.StopTimer()
}

func BenchmarkDecimalIndexScan(b *testing.B) {
	ctx := context.Background()
	se, do, st := prepareBenchSession()
	defer func() {
		se.Close()
		st.Close()
		do.Close()
	}()
	prepareBenchData(se, "decimal(32,6)", "%v.1234", smallCount)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rs, err := se.Execute(ctx, "select * from t where col >= 0")
		if err != nil {
			b.Fatal(err)
		}
		readResult(ctx, rs[0], smallCount)
	}
	b.StopTimer()
}

func BenchmarkDecimalIndexLookup(b *testing.B) {
	ctx := context.Background()
	se, do, st := prepareBenchSession()
	defer func() {
		se.Close()
		st.Close()
		do.Close()
	}()
	prepareBenchData(se, "decimal(32,6)", "%v.1234", smallCount)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rs, err := se.Execute(ctx, "select * from t where col = 64.1234")
		if err != nil {
			b.Fatal(err)
		}
		readResult(ctx, rs[0], 1)
	}
	b.StopTimer()
}

func BenchmarkInsertWithIndex(b *testing.B) {
	se, do, st := prepareBenchSession()
	defer func() {
		se.Close()
		st.Close()
		do.Close()
	}()
	mustExecute(se, "drop table if exists t")
	mustExecute(se, "create table t (pk int primary key, col int, index idx (col))")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustExecute(se, fmt.Sprintf("insert t values (%d, %d)", i, i))
	}
	b.StopTimer()
}

func BenchmarkInsertNoIndex(b *testing.B) {
	se, do, st := prepareBenchSession()
	defer func() {
		se.Close()
		st.Close()
		do.Close()
	}()
	mustExecute(se, "drop table if exists t")
	mustExecute(se, "create table t (pk int primary key, col int)")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustExecute(se, fmt.Sprintf("insert t values (%d, %d)", i, i))
	}
	b.StopTimer()
}

func BenchmarkSort(b *testing.B) {
	ctx := context.Background()
	se, do, st := prepareBenchSession()
	defer func() {
		se.Close()
		st.Close()
		do.Close()
	}()
	prepareSortBenchData(se, "int", "%v", bigCount)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rs, err := se.Execute(ctx, "select * from t order by col limit 50")
		if err != nil {
			b.Fatal(err)
		}
		readResult(ctx, rs[0], 50)
	}
	b.StopTimer()
}

func BenchmarkJoin(b *testing.B) {
	ctx := context.Background()
	se, do, st := prepareBenchSession()
	defer func() {
		se.Close()
		st.Close()
		do.Close()
	}()
	prepareJoinBenchData(se, "int", "%v", smallCount)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rs, err := se.Execute(ctx, "select * from t a join t b on a.col = b.col")
		if err != nil {
			b.Fatal(err)
		}
		readResult(ctx, rs[0], smallCount)
	}
	b.StopTimer()
}

func BenchmarkJoinLimit(b *testing.B) {
	ctx := context.Background()
	se, do, st := prepareBenchSession()
	defer func() {
		se.Close()
		st.Close()
		do.Close()
	}()
	prepareJoinBenchData(se, "int", "%v", smallCount)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rs, err := se.Execute(ctx, "select * from t a join t b on a.col = b.col limit 1")
		if err != nil {
			b.Fatal(err)
		}
		readResult(ctx, rs[0], 1)
	}
	b.StopTimer()
}

func BenchmarkPartitionPruning(b *testing.B) {
	ctx := context.Background()
	se, do, st := prepareBenchSession()
	defer func() {
		se.Close()
		do.Close()
		st.Close()
	}()

	mustExecute(se, `create table t (id int, dt datetime)
partition by range (to_days(dt)) (
partition p0 values less than (737515),
partition p1 values less than (737516),
partition p2 values less than (737517),
partition p3 values less than (737518),
partition p4 values less than (737519),
partition p5 values less than (737520),
partition p6 values less than (737521),
partition p7 values less than (737522),
partition p8 values less than (737523),
partition p9 values less than (737524),
partition p10 values less than (737525),
partition p11 values less than (737526),
partition p12 values less than (737527),
partition p13 values less than (737528),
partition p14 values less than (737529),
partition p15 values less than (737530),
partition p16 values less than (737531),
partition p17 values less than (737532),
partition p18 values less than (737533),
partition p19 values less than (737534),
partition p20 values less than (737535),
partition p21 values less than (737536),
partition p22 values less than (737537),
partition p23 values less than (737538),
partition p24 values less than (737539),
partition p25 values less than (737540),
partition p26 values less than (737541),
partition p27 values less than (737542),
partition p28 values less than (737543),
partition p29 values less than (737544),
partition p30 values less than (737545),
partition p31 values less than (737546),
partition p32 values less than (737547),
partition p33 values less than (737548),
partition p34 values less than (737549),
partition p35 values less than (737550),
partition p36 values less than (737551),
partition p37 values less than (737552),
partition p38 values less than (737553),
partition p39 values less than (737554),
partition p40 values less than (737555),
partition p41 values less than (737556),
partition p42 values less than (737557),
partition p43 values less than (737558),
partition p44 values less than (737559),
partition p45 values less than (737560),
partition p46 values less than (737561),
partition p47 values less than (737562),
partition p48 values less than (737563),
partition p49 values less than (737564),
partition p50 values less than (737565),
partition p51 values less than (737566),
partition p52 values less than (737567),
partition p53 values less than (737568),
partition p54 values less than (737569),
partition p55 values less than (737570),
partition p56 values less than (737571),
partition p57 values less than (737572),
partition p58 values less than (737573),
partition p59 values less than (737574),
partition p60 values less than (737575),
partition p61 values less than (737576),
partition p62 values less than (737577),
partition p63 values less than (737578),
partition p64 values less than (737579),
partition p65 values less than (737580),
partition p66 values less than (737581),
partition p67 values less than (737582),
partition p68 values less than (737583),
partition p69 values less than (737584),
partition p70 values less than (737585),
partition p71 values less than (737586),
partition p72 values less than (737587),
partition p73 values less than (737588),
partition p74 values less than (737589),
partition p75 values less than (737590),
partition p76 values less than (737591),
partition p77 values less than (737592),
partition p78 values less than (737593),
partition p79 values less than (737594),
partition p80 values less than (737595),
partition p81 values less than (737596),
partition p82 values less than (737597),
partition p83 values less than (737598),
partition p84 values less than (737599),
partition p85 values less than (737600),
partition p86 values less than (737601),
partition p87 values less than (737602),
partition p88 values less than (737603),
partition p89 values less than (737604),
partition p90 values less than (737605),
partition p91 values less than (737606),
partition p92 values less than (737607),
partition p93 values less than (737608),
partition p94 values less than (737609),
partition p95 values less than (737610),
partition p96 values less than (737611),
partition p97 values less than (737612),
partition p98 values less than (737613),
partition p99 values less than (737614),
partition p100 values less than (737615),
partition p101 values less than (737616),
partition p102 values less than (737617),
partition p103 values less than (737618),
partition p104 values less than (737619),
partition p105 values less than (737620),
partition p106 values less than (737621),
partition p107 values less than (737622),
partition p108 values less than (737623),
partition p109 values less than (737624),
partition p110 values less than (737625),
partition p111 values less than (737626),
partition p112 values less than (737627),
partition p113 values less than (737628),
partition p114 values less than (737629),
partition p115 values less than (737630),
partition p116 values less than (737631),
partition p117 values less than (737632),
partition p118 values less than (737633),
partition p119 values less than (737634),
partition p120 values less than (737635),
partition p121 values less than (737636),
partition p122 values less than (737637),
partition p123 values less than (737638),
partition p124 values less than (737639),
partition p125 values less than (737640),
partition p126 values less than (737641),
partition p127 values less than (737642),
partition p128 values less than (737643),
partition p129 values less than (737644),
partition p130 values less than (737645),
partition p131 values less than (737646),
partition p132 values less than (737647),
partition p133 values less than (737648),
partition p134 values less than (737649),
partition p135 values less than (737650),
partition p136 values less than (737651),
partition p137 values less than (737652),
partition p138 values less than (737653),
partition p139 values less than (737654),
partition p140 values less than (737655),
partition p141 values less than (737656),
partition p142 values less than (737657),
partition p143 values less than (737658),
partition p144 values less than (737659),
partition p145 values less than (737660),
partition p146 values less than (737661),
partition p147 values less than (737662),
partition p148 values less than (737663),
partition p149 values less than (737664),
partition p150 values less than (737665),
partition p151 values less than (737666),
partition p152 values less than (737667),
partition p153 values less than (737668),
partition p154 values less than (737669),
partition p155 values less than (737670),
partition p156 values less than (737671),
partition p157 values less than (737672),
partition p158 values less than (737673),
partition p159 values less than (737674),
partition p160 values less than (737675),
partition p161 values less than (737676),
partition p162 values less than (737677),
partition p163 values less than (737678),
partition p164 values less than (737679),
partition p165 values less than (737680),
partition p166 values less than (737681),
partition p167 values less than (737682),
partition p168 values less than (737683),
partition p169 values less than (737684),
partition p170 values less than (737685),
partition p171 values less than (737686),
partition p172 values less than (737687),
partition p173 values less than (737688),
partition p174 values less than (737689),
partition p175 values less than (737690),
partition p176 values less than (737691),
partition p177 values less than (737692),
partition p178 values less than (737693),
partition p179 values less than (737694),
partition p180 values less than (737695),
partition p181 values less than (737696),
partition p182 values less than (737697),
partition p183 values less than (737698),
partition p184 values less than (737699),
partition p185 values less than (737700),
partition p186 values less than (737701),
partition p187 values less than (737702),
partition p188 values less than (737703),
partition p189 values less than (737704),
partition p190 values less than (737705),
partition p191 values less than (737706),
partition p192 values less than (737707),
partition p193 values less than (737708),
partition p194 values less than (737709),
partition p195 values less than (737710),
partition p196 values less than (737711),
partition p197 values less than (737712),
partition p198 values less than (737713),
partition p199 values less than (737714),
partition p200 values less than (737715),
partition p201 values less than (737716),
partition p202 values less than (737717),
partition p203 values less than (737718),
partition p204 values less than (737719),
partition p205 values less than (737720),
partition p206 values less than (737721),
partition p207 values less than (737722),
partition p208 values less than (737723),
partition p209 values less than (737724),
partition p210 values less than (737725),
partition p211 values less than (737726),
partition p212 values less than (737727),
partition p213 values less than (737728),
partition p214 values less than (737729),
partition p215 values less than (737730),
partition p216 values less than (737731),
partition p217 values less than (737732),
partition p218 values less than (737733),
partition p219 values less than (737734),
partition p220 values less than (737735),
partition p221 values less than (737736),
partition p222 values less than (737737),
partition p223 values less than (737738),
partition p224 values less than (737739),
partition p225 values less than (737740),
partition p226 values less than (737741),
partition p227 values less than (737742),
partition p228 values less than (737743),
partition p229 values less than (737744),
partition p230 values less than (737745),
partition p231 values less than (737746),
partition p232 values less than (737747),
partition p233 values less than (737748),
partition p234 values less than (737749),
partition p235 values less than (737750),
partition p236 values less than (737751),
partition p237 values less than (737752),
partition p238 values less than (737753),
partition p239 values less than (737754),
partition p240 values less than (737755),
partition p241 values less than (737756),
partition p242 values less than (737757),
partition p243 values less than (737758),
partition p244 values less than (737759),
partition p245 values less than (737760),
partition p246 values less than (737761),
partition p247 values less than (737762),
partition p248 values less than (737763),
partition p249 values less than (737764),
partition p250 values less than (737765),
partition p251 values less than (737766),
partition p252 values less than (737767),
partition p253 values less than (737768),
partition p254 values less than (737769),
partition p255 values less than (737770),
partition p256 values less than (737771),
partition p257 values less than (737772),
partition p258 values less than (737773),
partition p259 values less than (737774),
partition p260 values less than (737775),
partition p261 values less than (737776),
partition p262 values less than (737777),
partition p263 values less than (737778),
partition p264 values less than (737779),
partition p265 values less than (737780),
partition p266 values less than (737781),
partition p267 values less than (737782),
partition p268 values less than (737783),
partition p269 values less than (737784),
partition p270 values less than (737785),
partition p271 values less than (737786),
partition p272 values less than (737787),
partition p273 values less than (737788),
partition p274 values less than (737789),
partition p275 values less than (737790),
partition p276 values less than (737791),
partition p277 values less than (737792),
partition p278 values less than (737793),
partition p279 values less than (737794),
partition p280 values less than (737795),
partition p281 values less than (737796),
partition p282 values less than (737797),
partition p283 values less than (737798),
partition p284 values less than (737799),
partition p285 values less than (737800),
partition p286 values less than (737801),
partition p287 values less than (737802),
partition p288 values less than (737803),
partition p289 values less than (737804),
partition p290 values less than (737805),
partition p291 values less than (737806),
partition p292 values less than (737807),
partition p293 values less than (737808),
partition p294 values less than (737809),
partition p295 values less than (737810),
partition p296 values less than (737811),
partition p297 values less than (737812),
partition p298 values less than (737813),
partition p299 values less than (737814),
partition p300 values less than (737815),
partition p301 values less than (737816),
partition p302 values less than (737817),
partition p303 values less than (737818),
partition p304 values less than (737819),
partition p305 values less than (737820),
partition p306 values less than (737821),
partition p307 values less than (737822),
partition p308 values less than (737823),
partition p309 values less than (737824),
partition p310 values less than (737825),
partition p311 values less than (737826),
partition p312 values less than (737827),
partition p313 values less than (737828),
partition p314 values less than (737829),
partition p315 values less than (737830),
partition p316 values less than (737831),
partition p317 values less than (737832),
partition p318 values less than (737833),
partition p319 values less than (737834),
partition p320 values less than (737835),
partition p321 values less than (737836),
partition p322 values less than (737837),
partition p323 values less than (737838),
partition p324 values less than (737839),
partition p325 values less than (737840),
partition p326 values less than (737841),
partition p327 values less than (737842),
partition p328 values less than (737843),
partition p329 values less than (737844),
partition p330 values less than (737845),
partition p331 values less than (737846),
partition p332 values less than (737847),
partition p333 values less than (737848),
partition p334 values less than (737849),
partition p335 values less than (737850),
partition p336 values less than (737851),
partition p337 values less than (737852),
partition p338 values less than (737853),
partition p339 values less than (737854),
partition p340 values less than (737855),
partition p341 values less than (737856),
partition p342 values less than (737857),
partition p343 values less than (737858),
partition p344 values less than (737859),
partition p345 values less than (737860),
partition p346 values less than (737861),
partition p347 values less than (737862),
partition p348 values less than (737863),
partition p349 values less than (737864),
partition p350 values less than (737865),
partition p351 values less than (737866),
partition p352 values less than (737867),
partition p353 values less than (737868),
partition p354 values less than (737869),
partition p355 values less than (737870),
partition p356 values less than (737871),
partition p357 values less than (737872),
partition p358 values less than (737873),
partition p359 values less than (737874),
partition p360 values less than (737875),
partition p361 values less than (737876),
partition p362 values less than (737877),
partition p363 values less than (737878),
partition p364 values less than (737879),
partition p365 values less than (737880),
partition p366 values less than (737881),
partition p367 values less than (737882),
partition p368 values less than (737883),
partition p369 values less than (737884),
partition p370 values less than (737885),
partition p371 values less than (737886),
partition p372 values less than (737887),
partition p373 values less than (737888),
partition p374 values less than (737889),
partition p375 values less than (737890),
partition p376 values less than (737891),
partition p377 values less than (737892),
partition p378 values less than (737893),
partition p379 values less than (737894),
partition p380 values less than (737895),
partition p381 values less than (737896),
partition p382 values less than (737897),
partition p383 values less than (737898),
partition p384 values less than (737899),
partition p385 values less than (737900),
partition p386 values less than (737901),
partition p387 values less than (737902),
partition p388 values less than (737903),
partition p389 values less than (737904),
partition p390 values less than (737905),
partition p391 values less than (737906),
partition p392 values less than (737907),
partition p393 values less than (737908),
partition p394 values less than (737909),
partition p395 values less than (737910),
partition p396 values less than (737911),
partition p397 values less than (737912),
partition p398 values less than (737913),
partition p399 values less than (737914),
partition p400 values less than (737915),
partition p401 values less than (737916),
partition p402 values less than (737917),
partition p403 values less than (737918),
partition p404 values less than (737919),
partition p405 values less than (737920),
partition p406 values less than (737921),
partition p407 values less than (737922),
partition p408 values less than (737923),
partition p409 values less than (737924),
partition p410 values less than (737925),
partition p411 values less than (737926),
partition p412 values less than (737927),
partition p413 values less than (737928),
partition p414 values less than (737929),
partition p415 values less than (737930),
partition p416 values less than (737931),
partition p417 values less than (737932),
partition p418 values less than (737933),
partition p419 values less than (737934),
partition p420 values less than (737935),
partition p421 values less than (737936),
partition p422 values less than (737937),
partition p423 values less than (737938),
partition p424 values less than (737939),
partition p425 values less than (737940),
partition p426 values less than (737941),
partition p427 values less than (737942),
partition p428 values less than (737943),
partition p429 values less than (737944),
partition p430 values less than (737945),
partition p431 values less than (737946),
partition p432 values less than (737947),
partition p433 values less than (737948),
partition p434 values less than (737949),
partition p435 values less than (737950),
partition p436 values less than (737951),
partition p437 values less than (737952),
partition p438 values less than (737953),
partition p439 values less than (737954),
partition p440 values less than (737955),
partition p441 values less than (737956),
partition p442 values less than (737957),
partition p443 values less than (737958),
partition p444 values less than (737959),
partition p445 values less than (737960),
partition p446 values less than (737961),
partition p447 values less than (737962),
partition p448 values less than (737963),
partition p449 values less than (737964),
partition p450 values less than (737965),
partition p451 values less than (737966),
partition p452 values less than (737967),
partition p453 values less than (737968),
partition p454 values less than (737969),
partition p455 values less than (737970),
partition p456 values less than (737971),
partition p457 values less than (737972),
partition p458 values less than (737973),
partition p459 values less than (737974),
partition p460 values less than (737975),
partition p461 values less than (737976),
partition p462 values less than (737977),
partition p463 values less than (737978),
partition p464 values less than (737979),
partition p465 values less than (737980),
partition p466 values less than (737981),
partition p467 values less than (737982),
partition p468 values less than (737983),
partition p469 values less than (737984),
partition p470 values less than (737985),
partition p471 values less than (737986),
partition p472 values less than (737987),
partition p473 values less than (737988),
partition p474 values less than (737989),
partition p475 values less than (737990),
partition p476 values less than (737991),
partition p477 values less than (737992),
partition p478 values less than (737993),
partition p479 values less than (737994),
partition p480 values less than (737995),
partition p481 values less than (737996),
partition p482 values less than (737997),
partition p483 values less than (737998),
partition p484 values less than (737999),
partition p485 values less than (738000),
partition p486 values less than (738001),
partition p487 values less than (738002),
partition p488 values less than (738003),
partition p489 values less than (738004),
partition p490 values less than (738005),
partition p491 values less than (738006),
partition p492 values less than (738007),
partition p493 values less than (738008),
partition p494 values less than (738009),
partition p495 values less than (738010),
partition p496 values less than (738011),
partition p497 values less than (738012),
partition p498 values less than (738013),
partition p499 values less than (738014),
partition p500 values less than (738015),
partition p501 values less than (738016),
partition p502 values less than (738017),
partition p503 values less than (738018),
partition p504 values less than (738019),
partition p505 values less than (738020),
partition p506 values less than (738021),
partition p507 values less than (738022),
partition p508 values less than (738023),
partition p509 values less than (738024),
partition p510 values less than (738025),
partition p511 values less than (738026),
partition p512 values less than (738027),
partition p513 values less than (738028),
partition p514 values less than (738029),
partition p515 values less than (738030),
partition p516 values less than (738031),
partition p517 values less than (738032),
partition p518 values less than (738033),
partition p519 values less than (738034),
partition p520 values less than (738035),
partition p521 values less than (738036),
partition p522 values less than (738037),
partition p523 values less than (738038),
partition p524 values less than (738039),
partition p525 values less than (738040),
partition p526 values less than (738041),
partition p527 values less than (738042),
partition p528 values less than (738043),
partition p529 values less than (738044),
partition p530 values less than (738045),
partition p531 values less than (738046),
partition p532 values less than (738047),
partition p533 values less than (738048),
partition p534 values less than (738049),
partition p535 values less than (738050),
partition p536 values less than (738051),
partition p537 values less than (738052),
partition p538 values less than (738053),
partition p539 values less than (738054),
partition p540 values less than (738055),
partition p541 values less than (738056),
partition p542 values less than (738057),
partition p543 values less than (738058),
partition p544 values less than (738059),
partition p545 values less than (738060),
partition p546 values less than (738061),
partition p547 values less than (738062),
partition p548 values less than (738063),
partition p549 values less than (738064),
partition p550 values less than (738065),
partition p551 values less than (738066),
partition p552 values less than (738067),
partition p553 values less than (738068),
partition p554 values less than (738069),
partition p555 values less than (738070),
partition p556 values less than (738071),
partition p557 values less than (738072),
partition p558 values less than (738073),
partition p559 values less than (738074),
partition p560 values less than (738075),
partition p561 values less than (738076),
partition p562 values less than (738077),
partition p563 values less than (738078),
partition p564 values less than (738079),
partition p565 values less than (738080),
partition p566 values less than (738081),
partition p567 values less than (738082),
partition p568 values less than (738083),
partition p569 values less than (738084),
partition p570 values less than (738085),
partition p571 values less than (738086),
partition p572 values less than (738087),
partition p573 values less than (738088),
partition p574 values less than (738089),
partition p575 values less than (738090),
partition p576 values less than (738091),
partition p577 values less than (738092),
partition p578 values less than (738093),
partition p579 values less than (738094),
partition p580 values less than (738095),
partition p581 values less than (738096),
partition p582 values less than (738097),
partition p583 values less than (738098),
partition p584 values less than (738099),
partition p585 values less than (738100),
partition p586 values less than (738101),
partition p587 values less than (738102),
partition p588 values less than (738103),
partition p589 values less than (738104),
partition p590 values less than (738105),
partition p591 values less than (738106),
partition p592 values less than (738107),
partition p593 values less than (738108),
partition p594 values less than (738109),
partition p595 values less than (738110),
partition p596 values less than (738111),
partition p597 values less than (738112),
partition p598 values less than (738113),
partition p599 values less than (738114),
partition p600 values less than (738115),
partition p601 values less than (738116),
partition p602 values less than (738117),
partition p603 values less than (738118),
partition p604 values less than (738119),
partition p605 values less than (738120),
partition p606 values less than (738121),
partition p607 values less than (738122),
partition p608 values less than (738123),
partition p609 values less than (738124),
partition p610 values less than (738125),
partition p611 values less than (738126),
partition p612 values less than (738127),
partition p613 values less than (738128),
partition p614 values less than (738129),
partition p615 values less than (738130),
partition p616 values less than (738131),
partition p617 values less than (738132),
partition p618 values less than (738133),
partition p619 values less than (738134),
partition p620 values less than (738135),
partition p621 values less than (738136),
partition p622 values less than (738137),
partition p623 values less than (738138),
partition p624 values less than (738139),
partition p625 values less than (738140),
partition p626 values less than (738141),
partition p627 values less than (738142),
partition p628 values less than (738143),
partition p629 values less than (738144),
partition p630 values less than (738145),
partition p631 values less than (738146),
partition p632 values less than (738147),
partition p633 values less than (738148),
partition p634 values less than (738149),
partition p635 values less than (738150),
partition p636 values less than (738151),
partition p637 values less than (738152),
partition p638 values less than (738153),
partition p639 values less than (738154),
partition p640 values less than (738155),
partition p641 values less than (738156),
partition p642 values less than (738157),
partition p643 values less than (738158),
partition p644 values less than (738159),
partition p645 values less than (738160),
partition p646 values less than (738161),
partition p647 values less than (738162),
partition p648 values less than (738163),
partition p649 values less than (738164),
partition p650 values less than (738165),
partition p651 values less than (738166),
partition p652 values less than (738167),
partition p653 values less than (738168),
partition p654 values less than (738169),
partition p655 values less than (738170),
partition p656 values less than (738171),
partition p657 values less than (738172),
partition p658 values less than (738173),
partition p659 values less than (738174),
partition p660 values less than (738175),
partition p661 values less than (738176),
partition p662 values less than (738177),
partition p663 values less than (738178),
partition p664 values less than (738179),
partition p665 values less than (738180),
partition p666 values less than (738181),
partition p667 values less than (738182),
partition p668 values less than (738183),
partition p669 values less than (738184),
partition p670 values less than (738185),
partition p671 values less than (738186),
partition p672 values less than (738187),
partition p673 values less than (738188),
partition p674 values less than (738189),
partition p675 values less than (738190),
partition p676 values less than (738191),
partition p677 values less than (738192),
partition p678 values less than (738193),
partition p679 values less than (738194),
partition p680 values less than (738195),
partition p681 values less than (738196),
partition p682 values less than (738197),
partition p683 values less than (738198),
partition p684 values less than (738199),
partition p685 values less than (738200),
partition p686 values less than (738201),
partition p687 values less than (738202),
partition p688 values less than (738203),
partition p689 values less than (738204),
partition p690 values less than (738205),
partition p691 values less than (738206),
partition p692 values less than (738207),
partition p693 values less than (738208),
partition p694 values less than (738209),
partition p695 values less than (738210),
partition p696 values less than (738211),
partition p697 values less than (738212),
partition p698 values less than (738213),
partition p699 values less than (738214),
partition p700 values less than (738215),
partition p701 values less than (738216),
partition p702 values less than (738217),
partition p703 values less than (738218),
partition p704 values less than (738219),
partition p705 values less than (738220),
partition p706 values less than (738221),
partition p707 values less than (738222),
partition p708 values less than (738223),
partition p709 values less than (738224),
partition p710 values less than (738225),
partition p711 values less than (738226),
partition p712 values less than (738227),
partition p713 values less than (738228),
partition p714 values less than (738229),
partition p715 values less than (738230),
partition p716 values less than (738231),
partition p717 values less than (738232),
partition p718 values less than (738233),
partition p719 values less than (738234),
partition p720 values less than (738235),
partition p721 values less than (738236),
partition p722 values less than (738237),
partition p723 values less than (738238),
partition p724 values less than (738239),
partition p725 values less than (738240),
partition p726 values less than (738241),
partition p727 values less than (738242),
partition p728 values less than (738243),
partition p729 values less than (738244),
partition p730 values less than (738245),
partition p731 values less than (738246),
partition p732 values less than (738247),
partition p733 values less than (738248),
partition p734 values less than (738249),
partition p735 values less than (738250),
partition p736 values less than (738251),
partition p737 values less than (738252),
partition p738 values less than (738253),
partition p739 values less than (738254),
partition p740 values less than (738255),
partition p741 values less than (738256),
partition p742 values less than (738257),
partition p743 values less than (738258),
partition p744 values less than (738259),
partition p745 values less than (738260),
partition p746 values less than (738261),
partition p747 values less than (738262),
partition p748 values less than (738263),
partition p749 values less than (738264),
partition p750 values less than (738265),
partition p751 values less than (738266),
partition p752 values less than (738267),
partition p753 values less than (738268),
partition p754 values less than (738269),
partition p755 values less than (738270),
partition p756 values less than (738271),
partition p757 values less than (738272),
partition p758 values less than (738273),
partition p759 values less than (738274),
partition p760 values less than (738275),
partition p761 values less than (738276),
partition p762 values less than (738277),
partition p763 values less than (738278),
partition p764 values less than (738279),
partition p765 values less than (738280),
partition p766 values less than (738281),
partition p767 values less than (738282),
partition p768 values less than (738283),
partition p769 values less than (738284),
partition p770 values less than (738285),
partition p771 values less than (738286),
partition p772 values less than (738287),
partition p773 values less than (738288),
partition p774 values less than (738289),
partition p775 values less than (738290),
partition p776 values less than (738291),
partition p777 values less than (738292),
partition p778 values less than (738293),
partition p779 values less than (738294),
partition p780 values less than (738295),
partition p781 values less than (738296),
partition p782 values less than (738297),
partition p783 values less than (738298),
partition p784 values less than (738299),
partition p785 values less than (738300),
partition p786 values less than (738301),
partition p787 values less than (738302),
partition p788 values less than (738303),
partition p789 values less than (738304),
partition p790 values less than (738305),
partition p791 values less than (738306),
partition p792 values less than (738307),
partition p793 values less than (738308),
partition p794 values less than (738309),
partition p795 values less than (738310),
partition p796 values less than (738311),
partition p797 values less than (738312),
partition p798 values less than (738313),
partition p799 values less than (738314),
partition p800 values less than (738315),
partition p801 values less than (738316),
partition p802 values less than (738317),
partition p803 values less than (738318),
partition p804 values less than (738319),
partition p805 values less than (738320),
partition p806 values less than (738321),
partition p807 values less than (738322),
partition p808 values less than (738323),
partition p809 values less than (738324),
partition p810 values less than (738325),
partition p811 values less than (738326),
partition p812 values less than (738327),
partition p813 values less than (738328),
partition p814 values less than (738329),
partition p815 values less than (738330),
partition p816 values less than (738331),
partition p817 values less than (738332),
partition p818 values less than (738333),
partition p819 values less than (738334),
partition p820 values less than (738335),
partition p821 values less than (738336),
partition p822 values less than (738337),
partition p823 values less than (738338),
partition p824 values less than (738339),
partition p825 values less than (738340),
partition p826 values less than (738341),
partition p827 values less than (738342),
partition p828 values less than (738343),
partition p829 values less than (738344),
partition p830 values less than (738345),
partition p831 values less than (738346),
partition p832 values less than (738347),
partition p833 values less than (738348),
partition p834 values less than (738349),
partition p835 values less than (738350),
partition p836 values less than (738351),
partition p837 values less than (738352),
partition p838 values less than (738353),
partition p839 values less than (738354),
partition p840 values less than (738355),
partition p841 values less than (738356),
partition p842 values less than (738357),
partition p843 values less than (738358),
partition p844 values less than (738359),
partition p845 values less than (738360),
partition p846 values less than (738361),
partition p847 values less than (738362),
partition p848 values less than (738363),
partition p849 values less than (738364),
partition p850 values less than (738365),
partition p851 values less than (738366),
partition p852 values less than (738367),
partition p853 values less than (738368),
partition p854 values less than (738369),
partition p855 values less than (738370),
partition p856 values less than (738371),
partition p857 values less than (738372),
partition p858 values less than (738373),
partition p859 values less than (738374),
partition p860 values less than (738375),
partition p861 values less than (738376),
partition p862 values less than (738377),
partition p863 values less than (738378),
partition p864 values less than (738379),
partition p865 values less than (738380),
partition p866 values less than (738381),
partition p867 values less than (738382),
partition p868 values less than (738383),
partition p869 values less than (738384),
partition p870 values less than (738385),
partition p871 values less than (738386),
partition p872 values less than (738387),
partition p873 values less than (738388),
partition p874 values less than (738389),
partition p875 values less than (738390),
partition p876 values less than (738391),
partition p877 values less than (738392),
partition p878 values less than (738393),
partition p879 values less than (738394),
partition p880 values less than (738395),
partition p881 values less than (738396),
partition p882 values less than (738397),
partition p883 values less than (738398),
partition p884 values less than (738399),
partition p885 values less than (738400),
partition p886 values less than (738401),
partition p887 values less than (738402),
partition p888 values less than (738403),
partition p889 values less than (738404),
partition p890 values less than (738405),
partition p891 values less than (738406),
partition p892 values less than (738407),
partition p893 values less than (738408),
partition p894 values less than (738409),
partition p895 values less than (738410),
partition p896 values less than (738411),
partition p897 values less than (738412),
partition p898 values less than (738413),
partition p899 values less than (738414),
partition p900 values less than (738415),
partition p901 values less than (738416),
partition p902 values less than (738417),
partition p903 values less than (738418),
partition p904 values less than (738419),
partition p905 values less than (738420),
partition p906 values less than (738421),
partition p907 values less than (738422),
partition p908 values less than (738423),
partition p909 values less than (738424),
partition p910 values less than (738425),
partition p911 values less than (738426),
partition p912 values less than (738427),
partition p913 values less than (738428),
partition p914 values less than (738429),
partition p915 values less than (738430),
partition p916 values less than (738431),
partition p917 values less than (738432),
partition p918 values less than (738433),
partition p919 values less than (738434),
partition p920 values less than (738435),
partition p921 values less than (738436),
partition p922 values less than (738437),
partition p923 values less than (738438),
partition p924 values less than (738439),
partition p925 values less than (738440),
partition p926 values less than (738441),
partition p927 values less than (738442),
partition p928 values less than (738443),
partition p929 values less than (738444),
partition p930 values less than (738445),
partition p931 values less than (738446),
partition p932 values less than (738447),
partition p933 values less than (738448),
partition p934 values less than (738449),
partition p935 values less than (738450),
partition p936 values less than (738451),
partition p937 values less than (738452),
partition p938 values less than (738453),
partition p939 values less than (738454),
partition p940 values less than (738455),
partition p941 values less than (738456),
partition p942 values less than (738457),
partition p943 values less than (738458),
partition p944 values less than (738459),
partition p945 values less than (738460),
partition p946 values less than (738461),
partition p947 values less than (738462),
partition p948 values less than (738463),
partition p949 values less than (738464),
partition p950 values less than (738465),
partition p951 values less than (738466),
partition p952 values less than (738467),
partition p953 values less than (738468),
partition p954 values less than (738469),
partition p955 values less than (738470),
partition p956 values less than (738471),
partition p957 values less than (738472),
partition p958 values less than (738473),
partition p959 values less than (738474),
partition p960 values less than (738475),
partition p961 values less than (738476),
partition p962 values less than (738477),
partition p963 values less than (738478),
partition p964 values less than (738479),
partition p965 values less than (738480),
partition p966 values less than (738481),
partition p967 values less than (738482),
partition p968 values less than (738483),
partition p969 values less than (738484),
partition p970 values less than (738485),
partition p971 values less than (738486),
partition p972 values less than (738487),
partition p973 values less than (738488),
partition p974 values less than (738489),
partition p975 values less than (738490),
partition p976 values less than (738491),
partition p977 values less than (738492),
partition p978 values less than (738493),
partition p979 values less than (738494),
partition p980 values less than (738495),
partition p981 values less than (738496),
partition p982 values less than (738497),
partition p983 values less than (738498),
partition p984 values less than (738499),
partition p985 values less than (738500),
partition p986 values less than (738501),
partition p987 values less than (738502),
partition p988 values less than (738503),
partition p989 values less than (738504),
partition p990 values less than (738505),
partition p991 values less than (738506),
partition p992 values less than (738507),
partition p993 values less than (738508),
partition p994 values less than (738509),
partition p995 values less than (738510),
partition p996 values less than (738511),
partition p997 values less than (738512),
partition p998 values less than (738513),
partition p999 values less than (738514),
partition p1000 values less than (738515),
partition p1001 values less than (738516),
partition p1002 values less than (738517),
partition p1003 values less than (738518),
partition p1004 values less than (738519),
partition p1005 values less than (738520),
partition p1006 values less than (738521),
partition p1007 values less than (738522),
partition p1008 values less than (738523),
partition p1009 values less than (738524),
partition p1010 values less than (738525),
partition p1011 values less than (738526),
partition p1012 values less than (738527),
partition p1013 values less than (738528),
partition p1014 values less than (738529),
partition p1015 values less than (738530),
partition p1016 values less than (738531),
partition p1017 values less than (738532),
partition p1018 values less than (738533),
partition p1019 values less than (738534),
partition p1020 values less than (738535),
partition p1021 values less than (738536),
partition p1022 values less than (738537),
partition p1023 values less than (738538)
)`)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rs, err := se.Execute(ctx, "select * from t where dt > to_days('2019-04-01 21:00:00') and dt < to_days('2019-04-07 23:59:59')")
		if err != nil {
			b.Fatal(err)
		}
		_, err = drainRecordSet(ctx, se.(*session), rs[0])
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}

func BenchmarkRangeColumnPartitionPruning(b *testing.B) {
	ctx := context.Background()
	se, do, st := prepareBenchSession()
	defer func() {
		se.Close()
		do.Close()
		st.Close()
	}()

	var build strings.Builder
	build.WriteString(`create table t (id int, dt date) partition by range columns (dt) (`)
	start := time.Date(2020, 5, 15, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 1023; i++ {
		start = start.Add(24 * time.Hour)
		fmt.Fprintf(&build, "partition p%d values less than ('%s'),\n", i, start.Format("2006-01-02"))
	}
	build.WriteString("partition p1023 values less than maxvalue)")
	mustExecute(se, build.String())
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rs, err := se.Execute(ctx, "select * from t where dt > '2020-05-01' and dt < '2020-06-07'")
		if err != nil {
			b.Fatal(err)
		}
		_, err = drainRecordSet(ctx, se.(*session), rs[0])
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}
