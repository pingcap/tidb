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

package executor

import (
	"testing"

	"github.com/pingcap/tidb/testkit"
)

func TestIssues40463(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test;")
	tk.MustExec("CREATE TABLE `4f380f26-9af6-4df8-959d-ad6296eff914` (`f7a9a4be-3728-449b-a5ea-df9b957eec67` enum('bkdv0','9rqy','lw','neud','ym','4nbv','9a7','bpkfo','xtfl','59','6vjj') NOT NULL DEFAULT 'neud', `43ca0135-1650-429b-8887-9eabcae2a234` set('8','5x47','xc','o31','lnz','gs5s','6yam','1','20ea','i','e') NOT NULL DEFAULT 'e', PRIMARY KEY (`f7a9a4be-3728-449b-a5ea-df9b957eec67`,`43ca0135-1650-429b-8887-9eabcae2a234`) /*T![clustered_index] CLUSTERED */) ENGINE=InnoDB DEFAULT CHARSET=ascii COLLATE=ascii_bin;")
	tk.MustExec("INSERT INTO `4f380f26-9af6-4df8-959d-ad6296eff914` VALUES ('bkdv0','gs5s'),('lw','20ea'),('neud','8'),('ym','o31'),('4nbv','o31'),('xtfl','e');")

	tk.MustExec("CREATE TABLE `ba35a09f-76f4-40aa-9b48-13154a24bdd2` (`9b2a7138-14a3-4e8f-b29a-720392aad22c` set('zgn','if8yo','e','k7','bav','xj6','lkag','m5','as','ia','l3') DEFAULT 'zgn,if8yo,e,k7,ia,l3',`a60d6b5c-08bd-4a6d-b951-716162d004a5` set('6li6','05jlu','w','l','m','e9r','5q','d0ol','i6ajr','csf','d32') DEFAULT '6li6,05jlu,w,l,m,d0ol,i6ajr,csf,d32',`fb753d37-6252-4bd3-9bd1-0059640e7861` year(4) DEFAULT '2065', UNIQUE KEY `51816c39-27df-4bbe-a0e7-d6b6f54be2a2` (`fb753d37-6252-4bd3-9bd1-0059640e7861`), KEY `b0dfda0a-ffed-4c5b-9a72-4113bc1cbc8e` (`9b2a7138-14a3-4e8f-b29a-720392aad22c`,`fb753d37-6252-4bd3-9bd1-0059640e7861`)) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin /*T! SHARD_ROW_ID_BITS=5 */;")
	tk.MustExec("insert into `ba35a09f-76f4-40aa-9b48-13154a24bdd2` values ('if8yo', '6li6,05jlu,w,l,m,d0ol,i6ajr,csf,d32', 2065);")

	tk.MustExec("CREATE TABLE `07ccc74e-14c3-4685-bb41-c78a069b1a6d` (`8a93bdc5-2214-4f96-b5a7-1ba4c0d396ae` bigint(20) NOT NULL DEFAULT '-4604789462044748682',`30b19ecf-679f-4ca3-813f-d3c3b8f7da7e` date NOT NULL DEFAULT '5030-11-23',`1c52eaf2-1ebb-4486-9410-dfd00c7c835c` decimal(7,5) DEFAULT '-81.91307',`4b09dfdc-e688-41cb-9ffa-d03071a43077` float DEFAULT '1.7989023',PRIMARY KEY (`30b19ecf-679f-4ca3-813f-d3c3b8f7da7e`,`8a93bdc5-2214-4f96-b5a7-1ba4c0d396ae`) /*T![clustered_index] CLUSTERED */,KEY `ae7a7637-ca52-443b-8a3f-69694f730cc4` (`8a93bdc5-2214-4f96-b5a7-1ba4c0d396ae`),KEY `42640042-8a17-4145-9510-5bb419f83ed9` (`8a93bdc5-2214-4f96-b5a7-1ba4c0d396ae`),KEY `839f4f5a-83f3-449b-a7dd-c7d2974d351a` (`30b19ecf-679f-4ca3-813f-d3c3b8f7da7e`),KEY `c474cde1-6fe4-45df-9067-b4e479f84149` (`8a93bdc5-2214-4f96-b5a7-1ba4c0d396ae`),KEY `f834d0a9-709e-4ca8-925d-73f48322b70d` (`8a93bdc5-2214-4f96-b5a7-1ba4c0d396ae`)) ENGINE=InnoDB DEFAULT CHARSET=gbk COLLATE=gbk_chinese_ci;")
	tk.MustExec("set sql_mode=``;")
	tk.MustExec("INSERT INTO `07ccc74e-14c3-4685-bb41-c78a069b1a6d` VALUES (616295989348159438,'0000-00-00',1.00000,1.7989023),(2215857492573998768,'1970-02-02',0.00000,1.7989023),(2215857492573998768,'1983-05-13',0.00000,1.7989023),(-2840083604831267906,'1984-01-30',1.00000,1.7989023),(599388718360890339,'1986-09-09',1.00000,1.7989023),(3506764933630033073,'1987-11-22',1.00000,1.7989023),(3506764933630033073,'2002-02-26',1.00000,1.7989023),(3506764933630033073,'2003-05-14',1.00000,1.7989023),(3506764933630033073,'2007-05-16',1.00000,1.7989023),(3506764933630033073,'2017-02-20',1.00000,1.7989023),(3506764933630033073,'2017-08-06',1.00000,1.7989023),(2215857492573998768,'2019-02-18',1.00000,1.7989023),(3506764933630033073,'2020-08-11',1.00000,1.7989023),(3506764933630033073,'2028-06-07',1.00000,1.7989023),(3506764933630033073,'2036-08-16',1.00000,1.7989023);")

	tk.MustQuery("select  /*+ use_index_merge( `4f380f26-9af6-4df8-959d-ad6296eff914` ) */ /*+  stream_agg() */  approx_percentile( `4f380f26-9af6-4df8-959d-ad6296eff914`.`f7a9a4be-3728-449b-a5ea-df9b957eec67` , 77 ) as r0 , `4f380f26-9af6-4df8-959d-ad6296eff914`.`f7a9a4be-3728-449b-a5ea-df9b957eec67` as r1 from `4f380f26-9af6-4df8-959d-ad6296eff914` where not( IsNull( `4f380f26-9af6-4df8-959d-ad6296eff914`.`f7a9a4be-3728-449b-a5ea-df9b957eec67` ) ) and not( `4f380f26-9af6-4df8-959d-ad6296eff914`.`f7a9a4be-3728-449b-a5ea-df9b957eec67` in ( select `8a93bdc5-2214-4f96-b5a7-1ba4c0d396ae` from `07ccc74e-14c3-4685-bb41-c78a069b1a6d` where `4f380f26-9af6-4df8-959d-ad6296eff914`.`f7a9a4be-3728-449b-a5ea-df9b957eec67` in ( select `a60d6b5c-08bd-4a6d-b951-716162d004a5` from `ba35a09f-76f4-40aa-9b48-13154a24bdd2` where not( `4f380f26-9af6-4df8-959d-ad6296eff914`.`f7a9a4be-3728-449b-a5ea-df9b957eec67` between 'bpkfo' and '59' ) and not( `4f380f26-9af6-4df8-959d-ad6296eff914`.`f7a9a4be-3728-449b-a5ea-df9b957eec67` in ( select `fb753d37-6252-4bd3-9bd1-0059640e7861` from `ba35a09f-76f4-40aa-9b48-13154a24bdd2` where IsNull( `4f380f26-9af6-4df8-959d-ad6296eff914`.`f7a9a4be-3728-449b-a5ea-df9b957eec67` ) or not( `4f380f26-9af6-4df8-959d-ad6296eff914`.`43ca0135-1650-429b-8887-9eabcae2a234` in ( select `8a93bdc5-2214-4f96-b5a7-1ba4c0d396ae` from `07ccc74e-14c3-4685-bb41-c78a069b1a6d` where IsNull( `4f380f26-9af6-4df8-959d-ad6296eff914`.`43ca0135-1650-429b-8887-9eabcae2a234` ) and not( `4f380f26-9af6-4df8-959d-ad6296eff914`.`f7a9a4be-3728-449b-a5ea-df9b957eec67` between 'neud' and 'bpkfo' ) ) ) ) ) ) ) ) group by `4f380f26-9af6-4df8-959d-ad6296eff914`.`f7a9a4be-3728-449b-a5ea-df9b957eec67`;")
}
