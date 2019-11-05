package failpoint_test

import (
	"errors"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
)

func TestHttp(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&httpSuite{})

type httpSuite struct{}

type contains struct {
	*CheckerInfo
}

var Contains Checker = &contains{
	&CheckerInfo{Name: "Contains", Params: []string{"obtained", "expected"}},
}

func (checker *contains) Check(params []interface{}, names []string) (result bool, error string) {
	param1, ok1 := params[0].(string)
	param2, ok2 := params[1].(string)
	if !ok1 || !ok2 {
		return false, "Argument to " + checker.Name + " must be string"
	}

	return strings.Contains(param1, param2), ""
}

type badReader struct{}

func (badReader) Read([]byte) (int, error) {
	return 0, errors.New("mock bad read")
}

func (s *httpSuite) TestServeHTTP(c *C) {
	handler := &failpoint.HttpHandler{}

	// PUT
	req, err := http.NewRequest(http.MethodPut, "http://127.0.0.1/failpoint-name", strings.NewReader("return(1)"))
	c.Assert(err, IsNil)
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, req)
	c.Assert(res.Code, Equals, http.StatusNoContent)

	req, err = http.NewRequest(http.MethodPut, "http://127.0.0.1", strings.NewReader("return(1)"))
	c.Assert(err, IsNil)
	res = httptest.NewRecorder()
	handler.ServeHTTP(res, req)
	c.Assert(res.Code, Equals, http.StatusBadRequest)
	c.Assert(res.Body.String(), Contains, "malformed request URI")

	req, err = http.NewRequest(http.MethodPut, "http://127.0.0.1/failpoint-name", badReader{})
	c.Assert(err, IsNil)
	res = httptest.NewRecorder()
	handler.ServeHTTP(res, req)
	c.Assert(res.Code, Equals, http.StatusBadRequest)
	c.Assert(res.Body.String(), Contains, "failed ReadAll in PUT")

	req, err = http.NewRequest(http.MethodPut, "http://127.0.0.1/failpoint-name", strings.NewReader("invalid"))
	c.Assert(err, IsNil)
	res = httptest.NewRecorder()
	handler.ServeHTTP(res, req)
	c.Assert(res.Code, Equals, http.StatusBadRequest)
	c.Assert(res.Body.String(), Contains, "failed to set failpoint")

	// GET
	req, err = http.NewRequest(http.MethodGet, "http://127.0.0.1/failpoint-name", strings.NewReader(""))
	c.Assert(err, IsNil)
	res = httptest.NewRecorder()
	handler.ServeHTTP(res, req)
	c.Assert(res.Code, Equals, http.StatusOK)
	c.Assert(res.Body.String(), Contains, "return(1)")

	req, err = http.NewRequest(http.MethodGet, "http://127.0.0.1/failpoint-name-not-exists", strings.NewReader(""))
	c.Assert(err, IsNil)
	res = httptest.NewRecorder()
	handler.ServeHTTP(res, req)
	c.Assert(res.Code, Equals, http.StatusNotFound)
	c.Assert(res.Body.String(), Contains, "failed to GET")

	req, err = http.NewRequest(http.MethodGet, "http://127.0.0.1/", strings.NewReader(""))
	c.Assert(err, IsNil)
	res = httptest.NewRecorder()
	handler.ServeHTTP(res, req)
	c.Assert(res.Code, Equals, http.StatusOK)
	c.Assert(res.Body.String(), Contains, "failpoint-name=return(1)")

	// DELETE
	req, err = http.NewRequest(http.MethodDelete, "http://127.0.0.1/failpoint-name", strings.NewReader(""))
	c.Assert(err, IsNil)
	res = httptest.NewRecorder()
	handler.ServeHTTP(res, req)
	c.Assert(res.Code, Equals, http.StatusNoContent)

	req, err = http.NewRequest(http.MethodDelete, "http://127.0.0.1/failpoint-name-not-exists", strings.NewReader(""))
	c.Assert(err, IsNil)
	res = httptest.NewRecorder()
	handler.ServeHTTP(res, req)
	c.Assert(res.Code, Equals, http.StatusBadRequest)
	c.Assert(res.Body.String(), Contains, "failed to delete failpoint")

	// DEFAULT
	req, err = http.NewRequest(http.MethodPost, "http://127.0.0.1/failpoint-name", strings.NewReader(""))
	c.Assert(err, IsNil)
	res = httptest.NewRecorder()
	handler.ServeHTTP(res, req)
	c.Assert(res.Code, Equals, http.StatusMethodNotAllowed)
	c.Assert(res.Body.String(), Contains, "Method not allowed")

	// Test environment variable injection
	resp, err := http.Get("http://127.0.0.1:23389/failpoint-env1")
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	body, err := ioutil.ReadAll(resp.Body)
	c.Assert(err, IsNil)
	c.Assert(string(body), Contains, "return(10)")

	resp, err = http.Get("http://127.0.0.1:23389/failpoint-env2")
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	body, err = ioutil.ReadAll(resp.Body)
	c.Assert(err, IsNil)
	c.Assert(string(body), Contains, "return(true)")
}
