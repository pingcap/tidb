package executor_test

import (
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

type TestSuite struct {
	store kv.Storage
	dom   *domain.Domain
}

func TestCopGen(t *testing.T) {
	store, clean, testGen := testkit.CreateMockStoreWithTestGen(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int auto_increment primary key, b int, c int)")
	tk.MustExec("insert into t values (1, 1, 1), (2, 2, 2), (3, 3, 3)")
	tk.MustExec("insert into t(b, c) (select b, c from t)")
	tk.MustExec("insert into t(b, c) (select b, c from t)")
	tk.MustExec("create table t2(a int auto_increment primary key, b int, c int)")
	tk.MustExec("insert into t2(b, c) (select b, c from t)")

	err := testGen.AddTable("test", "t")
	require.NoError(t, err)
	err = testGen.AddTable("test", "t2")
	require.NoError(t, err)

	require.NoError(t, testGen.Prepare())
	tk.MustQuery("select * from t where a * 2 - 1 = 1").Check(testkit.Rows("1 1 1"))
	require.NoError(t, testGen.Dump("/tmp/copgen_test_data.json"))
}

func TestProjectionPushdown(t *testing.T) {
	store, clean, testGen := testkit.CreateMockStoreWithTestGen(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, d varchar(20))")
	tk.MustExec("insert into t values (1, '2022-03-03'), (2, '2022-01-02'), (3, '2022-02-02')")
	require.NoError(t, testGen.AddTable("test", "t"))

	require.NoError(t, testGen.Prepare())
	tk.MustExec("set @@tidb_opt_projection_push_down=1")
	tk.MustQuery("select DATEDIFF(d, '2020-01-01') from t")
	require.NoError(t, testGen.Dump("/tmp/datediff_data.json"))
}

func parseSQLText(data string) (res []ast.StmtNode, warns []error, err error) {
	p := parser.New()
	p.EnableWindowFunc(true)
	// TODO, is there any problem in sqlMode?
	p.SetSQLMode(mysql.ModeNone)
	// FIXME, should change the collation and charset according to user's sql
	statements, warns, err := p.Parse(data, "utf8mb4", "utf8mb4_bin")
	return statements, warns, err
}

func TestCoprTest(t *testing.T) {
	store, dom, clean, testGen := testkit.CreateMockStoreAndDomainWithTestGen(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	// read file as data
	data, err := ioutil.ReadFile("../../copr-test/push-down-test/prepare/0_data.sql")
	require.NoError(t, err)
	tk.MustExec(string(data))
	//tk.MustQuery("show tables").Check(testkit.Rows(""))
	//tk.MustQuery("desc table1000_int_autoinc").Check(testkit.Rows(""))
	// add all tables to test
	db, ok := dom.InfoSchema().SchemaByName(model.NewCIStr("test"))
	require.True(t, ok)
	for _, tblInfo := range db.Tables {
		err = testGen.AddTable("test", tblInfo.Name.L)
		require.NoError(t, err)
	}

	require.NoError(t, testGen.Prepare())
	tk.MustExec("set @@sql_mode=''")
	tk.MustExec("set @@tidb_opt_projection_push_down=1")
	dir := "../../copr-test/push-down-test/sql"
	var files []string
	err = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}
		if !strings.HasSuffix(info.Name(), ".sql") || strings.HasSuffix(info.Name(), "10_agg.sql") {
			return nil
		}
		files = append(files, path)
		return nil
	})
	require.NoError(t, err)
	for _, file := range files {
		data, err := ioutil.ReadFile(file)
		require.NoError(t, err)
		statements, warns, err := parseSQLText(string(data))
		if warns != nil {
			log.Printf("Parse warning: %v\n", warns)
		}
		require.NoError(t, err)
		for _, stmt := range statements {
			tk.MustExec(stmt.Text())
		}
	}
	require.NoError(t, testGen.Dump("/tmp/copr-test.json"))
}
