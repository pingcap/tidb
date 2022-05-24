// Copyright 2021 PingCAP, Inc.
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

package temptable

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/testkit/testsetup"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	opts := []goleak.Option{
		goleak.IgnoreTopFunction("go.etcd.io/etcd/client/pkg/v3/logutil.(*MergeLogger).outputLoop"),
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
		goleak.IgnoreTopFunction("github.com/golang/glog.(*loggingT).flushDaemon"),
	}
	testsetup.SetupForCommonTest()
	goleak.VerifyTestMain(m, opts...)
}

type mockedInfoSchema struct {
	t *testing.T
	infoschema.InfoSchema
	tables map[int64]model.TempTableType
}

func newMockedInfoSchema(t *testing.T) *mockedInfoSchema {
	return &mockedInfoSchema{
		t:      t,
		tables: make(map[int64]model.TempTableType),
	}
}

func (is *mockedInfoSchema) AddTable(tempType model.TempTableType, id ...int64) *mockedInfoSchema {
	for _, tblID := range id {
		is.tables[tblID] = tempType
	}

	return is
}

func (is *mockedInfoSchema) TableByID(tblID int64) (table.Table, bool) {
	tempType, ok := is.tables[tblID]
	if !ok {
		return nil, false
	}

	tblInfo := &model.TableInfo{
		ID:   tblID,
		Name: model.NewCIStr(fmt.Sprintf("tb%d", tblID)),
		Columns: []*model.ColumnInfo{{
			ID:        1,
			Name:      model.NewCIStr("col1"),
			Offset:    0,
			FieldType: *types.NewFieldType(mysql.TypeLonglong),
			State:     model.StatePublic,
		}},
		Indices:       []*model.IndexInfo{},
		TempTableType: tempType,
		State:         model.StatePublic,
	}

	tbl, err := table.TableFromMeta(nil, tblInfo)
	require.NoError(is.t, err)

	return tbl, true
}

type mockedSnapshot struct {
	*mockedRetriever
}

func newMockedSnapshot(retriever *mockedRetriever) *mockedSnapshot {
	return &mockedSnapshot{mockedRetriever: retriever}
}

func (s *mockedSnapshot) SetOption(_ int, _ interface{}) {
	require.FailNow(s.t, "SetOption not supported")
}

type methodInvoke struct {
	Method string
	Args   []interface{}
	Ret    []interface{}
}

type mockedRetriever struct {
	t       *testing.T
	data    []*kv.Entry
	dataMap map[string][]byte
	invokes []*methodInvoke

	allowInvokes map[string]interface{}
	errorMap     map[string]error
}

func newMockedRetriever(t *testing.T) *mockedRetriever {
	return &mockedRetriever{t: t}
}

func (r *mockedRetriever) SetData(data []*kv.Entry) *mockedRetriever {
	lessFunc := func(i, j int) bool { return bytes.Compare(data[i].Key, data[j].Key) < 0 }
	if !sort.SliceIsSorted(data, lessFunc) {
		data = append([]*kv.Entry{}, data...)
		sort.Slice(data, lessFunc)
	}

	r.data = data
	r.dataMap = make(map[string][]byte)
	for _, item := range r.data {
		r.dataMap[string(item.Key)] = item.Value
	}
	return r
}

func (r *mockedRetriever) InjectMethodError(method string, err error) *mockedRetriever {
	if r.errorMap == nil {
		r.errorMap = make(map[string]error)
	}
	r.errorMap[method] = err
	return r
}

func (r *mockedRetriever) SetAllowedMethod(methods ...string) *mockedRetriever {
	r.allowInvokes = make(map[string]interface{})
	for _, m := range methods {
		r.allowInvokes[m] = struct{}{}
	}
	return r
}

func (r *mockedRetriever) ResetInvokes() {
	r.invokes = nil
}

func (r *mockedRetriever) GetInvokes() []*methodInvoke {
	return r.invokes
}

func (r *mockedRetriever) Get(ctx context.Context, k kv.Key) (val []byte, err error) {
	r.checkMethodInvokeAllowed("Get")
	if err = r.getMethodErr("Get"); err == nil {
		var ok bool
		val, ok = r.dataMap[string(k)]
		if !ok {
			err = kv.ErrNotExist
		}
	}
	r.appendInvoke("Get", []interface{}{ctx, k}, []interface{}{val, err})
	return
}

func (r *mockedRetriever) BatchGet(ctx context.Context, keys []kv.Key) (data map[string][]byte, err error) {
	r.checkMethodInvokeAllowed("BatchGet")
	if err = r.getMethodErr("BatchGet"); err == nil {
		data = make(map[string][]byte)
		for _, k := range keys {
			val, ok := r.dataMap[string(k)]
			if ok {
				data[string(k)] = val
			}
		}
	}

	r.appendInvoke("BatchGet", []interface{}{ctx, keys}, []interface{}{data, err})
	return
}

func (r *mockedRetriever) checkMethodInvokeAllowed(method string) {
	require.NotNil(r.t, r.allowInvokes, fmt.Sprintf("Invoke for '%s' is not allowed, should allow it first", method))
	require.Contains(r.t, r.allowInvokes, method, fmt.Sprintf("Invoke for '%s' is not allowed, should allow it first", method))
}

func (r *mockedRetriever) Iter(k kv.Key, upperBound kv.Key) (iter kv.Iterator, err error) {
	r.checkMethodInvokeAllowed("Iter")
	if err = r.getMethodErr("Iter"); err == nil {
		data := make([]*kv.Entry, 0)
		for _, item := range r.data {
			if bytes.Compare(item.Key, k) >= 0 && (len(upperBound) == 0 || bytes.Compare(item.Key, upperBound) < 0) {
				data = append(data, item)
			}
		}
		mockIter := mock.NewMockIterFromRecords(r.t, data, true)
		if nextErr := r.getMethodErr("IterNext"); nextErr != nil {
			mockIter.InjectNextError(nextErr)
		}
		iter = mockIter
	}
	r.appendInvoke("Iter", []interface{}{k, upperBound}, []interface{}{iter, err})
	return
}

func (r *mockedRetriever) IterReverse(k kv.Key) (iter kv.Iterator, err error) {
	r.checkMethodInvokeAllowed("IterReverse")
	if err = r.getMethodErr("IterReverse"); err == nil {
		data := make([]*kv.Entry, 0)
		for i := 0; i < len(r.data); i++ {
			item := r.data[len(r.data)-i-1]
			if len(k) == 0 || bytes.Compare(item.Key, k) < 0 {
				data = append(data, item)
			}
		}
		mockIter := mock.NewMockIterFromRecords(r.t, data, true)
		if nextErr := r.getMethodErr("IterReverseNext"); nextErr != nil {
			mockIter.InjectNextError(nextErr)
		}
		iter = mockIter
	}
	r.appendInvoke("IterReverse", []interface{}{k}, []interface{}{iter, err})
	return
}

func (r *mockedRetriever) appendInvoke(method string, args []interface{}, ret []interface{}) {
	r.invokes = append(r.invokes, &methodInvoke{
		Method: method,
		Args:   args,
		Ret:    ret,
	})
}

func (r *mockedRetriever) getMethodErr(method string) error {
	if r.errorMap == nil {
		return nil
	}

	if err, ok := r.errorMap[method]; ok && err != nil {
		return err
	}

	return nil
}
