// Copyright 2023 PingCAP, Inc.
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

package correlated

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
)

func TestCorrelatedSubquery(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`CREATE TABLE tlc07c2a51 (
  col_1 date DEFAULT NULL,
  col_2 json NOT NULL,
  col_3 varbinary(345) DEFAULT 'vE5ARCSlc%iI$Q',
  col_4 json NOT NULL,
  col_5 varchar(247) COLLATE utf8_general_ci NOT NULL,
  col_6 bit(21) NOT NULL DEFAULT b'110000110101111111000',
  col_7 bigint(20) NOT NULL DEFAULT '8151770874925830095',
  PRIMARY KEY (col_7,col_5) /*T![clustered_index] CLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci
PARTITION BY HASH (col_7) PARTITIONS 6;`)
	tk.MustExec(`CREATE TABLE tc4cf4a6b (
  col_1 date DEFAULT NULL,
  col_2 json NOT NULL,
  col_3 varbinary(345) DEFAULT 'vE5ARCSlc%iI$Q',
  col_4 json NOT NULL,
  col_5 varchar(247) COLLATE utf8_general_ci NOT NULL,
  col_6 bit(21) NOT NULL DEFAULT b'110000110101111111000',
  col_7 bigint(20) NOT NULL DEFAULT '8151770874925830095',
  PRIMARY KEY (col_7,col_5) /*T![clustered_index] CLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci
PARTITION BY HASH (col_7) PARTITIONS 6;`)
	tk.MustExec("INSERT INTO `tlc07c2a51` VALUES('2025-02-16','[\"xyJVLmVKsRn2ReGAfFkByZOztvVD8Xk5LGeLZo1WxYdG8LrA4WqO67szs9dvpLwK\", \"XQlzkZj81WXYJzMeVJkM1J2BVXNLyVlGc5g2TXUnh92o2NQp9vkAwsl8szXzNtmv\", \"CgcdqXY0HtrPc06XPeVPA6EcbqKnxVMS6TxkZObriQzKtlKv3HOEavEoyBdIFNac\"]',x'46245164543872483543','[\"yzegaiBdfb7svUI0JPXPzpoT8bkqQFexLr33ptDUtHWG3YLmrAvPU6hDP6h8YMOW\", \"gO9YIayc2kdwvnZ9362i2U9vQlM78COelLXxE9dGmvhBexkhvbEXXFEOyTATET6n\", \"TB3coCHSmOLXvzXt2KS9rRkotZ9fZk2JKwBRHoVR45EH8hW5gbrWbzBEwsTKKO41\"]','J%GY9U+&1#peaZ&Lg',x'04fd7c',1425801567592606408),('2025-02-16','[\"QEcM0XVHagvAoGbWAPkeAN26zG5WYnn2VPrVnDkAu71BGXQgkcTh8YOHPoAl6Plt\", \"Ba2NxxuOAXB40XzAowXIQlm0V2Pm2v65mY6j3w2tPxOeKwWopSgoUMmcNpYjlTlR\", \"HXry0elnp7ACh5tcenWOnh9sUeEjlTxquZLTfVfs5DuYzThBEqNtot7Fx3moiogI\"]',x'46245164543872483543','[\"E0yDwpJcR9qD4yDpZHw6DubUo2iG33MS3n8LKHCpbfMrCdOGMlzCCX8UYayiEi2c\", \"hL4tNhJwACwWuCvMRPkGRuA33cxrTZ8sNE4Fhh8LPpp5cboNPfm0EmOZBYzhwHbZ\", \"EfUYPQcEiHnlMN1LQa7vNMaGnEZ4k7epGKOxIbPsBQWtWe2fnOr7ZsZUSsp14Rqg\"]','FhP6pCikhV@_3*fH_',x'1e3ee7',2351169536288823809),('2001-04-19','[\"sWbccKqdpkgbdT2V94wPXU2pRT9UfngWMh0XClnCsvF2e3f3bnxJBlVrNXsT0VAh\", \"d4DpAYeD9lIl8ElznHJZX4iMisZFJlvguO0epZN02s8eMqsMJa8jUtOXkZeUlwYL\"]',x'7e665e6d76346d433724505e63516938','[\"NP5pVB7KUOaEa6NwD6oCIs8jCUE8d6RiExRkJwxt52rkuvGqxYV8wqdLV6mGVH2Z\", \"jLuDxB3x7Wr7enKMnW62OFcHJenidyCoPVmomerdTmmLycuqsWlb4SE2bx41gwjj\", \"tcrn9AuYmnwrvdHSdrD8KvJLOSW9gKGxzEukgAZlWK8cGVjjYTZO6YJcNcdmzCay\"]','Y^$',x'0b11a2',3652259273996203228),('2033-02-27','[\"osJeD8MsHuzSH0xdSQablscHNffpStRRrgdnhyDxrDq8eul7XjG36GpNZfgGnFci\", \"5Z8yHsJcLTWnP2yqK67te0WIAyMxFmPCW7EswNfNPAhXRxR7wOkYShUsqHxQLjbg\"]',x'54','[\"1PwgCYrA4KVwdCdV7E483ZpaPEjMtTv5MNrHQDkKlAEfS8FFY44G5olC3WPHhxZk\"]','%CZLL',x'12bce2',3717145498561167222),('2012-06-02','[\"V54Y14kWulMThN1zS9fW1kN5O74Q7WEh4I5aBSLTzTfXm3wRyLfb0Iq4psea6HZf\"]',x'614b594855416c513164','[\"Tw5f9qrnS5OleyctGGsB5eNFeM4TsCx7fOIt1y4y3v1TFldmvTbMmZccPP6gG2nf\", \"mNCgtgW12foWs3dQiUcCVnbRjkZyiiqXfGf3X3xItp4l40psarAXQgYl31gwSWwX\", \"SQxPqFHQHhilYzCCAjvpOVTXXhsRg0nBzKqdsvBD4xYsbNEmfE73ErKVYaMoD0qB\", \"bXPrB8c8va8Q5fPNReZUs8IBJKF0LDBzD2Tuwi2P4J8HRGElg7ftrbajtuKBSlpW\"]','+ix8V+ft8~uVf',x'069f5c',6189906848193069044),('2008-08-10','[\"V0vn67z0oCJdhvnr0riW4gITlMHpv5YanbNNzVXkWLTkEUDoGUY55FN2ii7C5r4h\", \"bp0N4YFYOzkDB2rDSzKNmJpfGmwykzpwb7vRykXPwEpXWui8rNWRTPynBsNnLpMg\"]',x'5e5236575a3974592441','[\"v8GdeCQYeGF7Om3SrunPEOd20VElZdDilpeMSKrUFxgS3TjbeakTqrocJTFz6BNZ\", \"lGmK6aGmdSh0oPg0cs9QWuvq2uKEuLZb6EruoenXEFuS5EHUyPYNXT2tsI5FSPJz\", \"cLSbqVhAVVez4Vksx84SLdjRTPZGrPBoKgvpn3y80ro5DEkg8BRubhJETzRaSKVK\"]','b#',x'0cdcc0',6546519514658297081),('2009-01-22','[\"IbPY5E7yenD30IaHAUL3qC59dtPYxwERoqfWp6rqpL7hsOaBbUm2vek5NZ0itZHQ\"]',x'2166256c662d6c30334e38454748','[\"8FF6cqAisQx1L6xtScBjYNXVf9Lql9bHaGNpXuVt7AWQaGAGXW2xiPAyzapGHiGG\"]','X~9~N9jHba#GV8tB4Vi',x'164e11',6607813116980509751),('2002-11-04','[\"Tf9RAVEBF9PHTSosZh5lWsc7LPrS9B24SG9yCqruuInClfYuUNtXY0MwZmrLCX8k\", \"e4TWSCiURA4V0agGj4id8Q6L0wLkCEbb0mtpMKTjtzeq527RvS1fCJvLtUZMTUXZ\", \"7mLipNkFUVvdEbfcPfsCu1b5bD4kILowxXtzXXwm81NwItdQwsdocpmzTszP1cAP\"]',x'7a2576','[\"tnoSerc2aAakQ3P9QDsg66lGTWkZ9Gh9M4vop9gJdMpqRJwemyWQJpFOTrOSZipj\", \"D1YFyRHunNdMKh17fVZoGMWylXkSBwrbReamEw7wXQBaQDq2Wa3OjWKylEZ29zM7\", \"T6jtYPW9FgcAfRWfcP8b9pNyxykO6GGIHgsCcmWUCmla8vOVhjhWrUJadQhFPitY\", \"ut7hb4CC5bH9KQmC0Zo8OtWrFPAfFkvcopQeLAcYkselVIN5au3DjFu6Q1Wh8Qjs\", \"tlNxKzCNxsjxgTpsRAyEXSbTvWdAFNZdbP5KRm0eD6sI7mZIegsZtFrXHtUIjJKz\"]','+m(=E-7BD_PTO4xAlN',x'1d22e5',7517561678520304673),('2025-02-16','[\"ih1dfdPoKD9NQqkguZJtRDLhiBz7YX0EUbxWaKoOj6Jcdfu6Z0rVvXOdYsefYoih\"]',x'46245164543872483543','[\"sN2f5pJoSizXgV7B2PgaNdtQFGdJnnmEFR1xslNzkGeoYI79JvYvQ2JYCBqvRGAm\", \"ZeY4KZAr9Ofkre3hoKRSbQJPf5ewPIkndMCZ0dRUqnwPRUjZBN5aaiXI3vefD8fY\"]','4FY*GRDPu#x',x'032849',7726971008804453429),('1994-09-29','[\"yuVOcYYUdna4b3mQqmuBfR3C03Nf7LLshm2zeWUSUgV5pXfpCDr5eKZh2KZwvBk7\"]',x'2a682a674964595f757240684837575e65','[\"UekvSaOwHY7q4cY7WKDQsIMl6JFI1NFMWXjAVYO2xazXcBommPNg23Xzdy9tLVca\", \"1R9FT97KQeeXAhjwnt7lydI74Yhwhs73w4JVUHPWeCA6VTKDhSxNgE29NncuJljX\", \"1J6C6axIsgZTarapkIQEc4cJjCRPaMGnPqmyQz4Fpp8yS0oK8yt91y0ryxWZnxcz\", \"dsKflwi46pGNv13HIkXKr34QqzfQ5xfdgEARdOceitD6vPgCp63D2AzQVVdhXLF2\"]','-hTu_Tv6rT@lv',x'18bae9',8059165531753386662);")
	tk.MustExec("INSERT INTO `tc4cf4a6b` VALUES('1997-09-05','[\"eHmlQuuQLz6Cr6RJbYMupozlc9H4C4y9iALlRW3K3idQXqAZqmHzzv1AvN0Z6kLC\"]',x'6321553645395e4631652b','[\"KbyfwnLn5f5XskAkAopGODRErHa2g9M0tKWf1hiGfn3ermF9A3wqpGUVgBrP2Iux\", \"vmlfVpV4shEqeSjelAyQYEXolbiGyBfD1KTYnneiKPNbk2YmsIkysXQJBdjNA14L\"]','QyzHu~fb&u^V!',x'000000',-9223372036854775808),('1997-09-05','[\"hfiExxjCGXpidyDlzCBUwDksJH0yB7IKMrx95R1N2ZvoompbxobSORfJWu4NdZES\", \"WLBPyTfd7GxOKIlccTI2kwXyb4UFyBwvt4X3NbADJkpkefpZP1VB6sjO2y73vJwh\", \"V73KjNUyfJHLQ2oFaOcK721AA8QWwaPz7VTsQ5aevwG7lewlW4Y1evLpVMz2LIbn\"]',x'742873664b4a256b57613823346128643830','[\"KbyfwnLn5f5XskAkAopGODRErHa2g9M0tKWf1hiGfn3ermF9A3wqpGUVgBrP2Iux\", \"vmlfVpV4shEqeSjelAyQYEXolbiGyBfD1KTYnneiKPNbk2YmsIkysXQJBdjNA14L\"]','XSnyU0E07X2',x'000000',-9223372036854775808);")
	tk.MustExec("analyze table tlc07c2a51;")
	tk.MustExec("analyze table tc4cf4a6b;")
	tk.MustQuery(`SELECT 1
FROM tlc07c2a51
WHERE NOT (tlc07c2a51.col_1>=
             (SELECT GROUP_CONCAT(tc4cf4a6b.col_7
                                  ORDER BY tc4cf4a6b.col_7 SEPARATOR ',') AS r0
              FROM (tlc07c2a51)
              JOIN tc4cf4a6b
              WHERE ISNULL(tc4cf4a6b.col_3)
              HAVING tlc07c2a51.col_6>1951988)) ;`).Check(testkit.Rows())
	tk.MustQuery(`SELECT 1
FROM tlc07c2a51
WHERE NOT (tlc07c2a51.col_1>=
            any (SELECT GROUP_CONCAT(tc4cf4a6b.col_7
                                  ORDER BY tc4cf4a6b.col_7 SEPARATOR ',') AS r0
              FROM tlc07c2a51
              JOIN tc4cf4a6b
              WHERE ISNULL(tc4cf4a6b.col_3)
              group by tlc07c2a51.col_6
              HAVING tlc07c2a51.col_6>0)) ;`).Check(testkit.Rows("1", "1", "1", "1", "1", "1", "1", "1", "1", "1"))
}
