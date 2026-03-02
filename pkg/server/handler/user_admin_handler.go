// Copyright 2026 PingCAP, Inc.
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

package handler

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"io"
	"math/big"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/extension"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/session"
	sessiontypes "github.com/pingcap/tidb/pkg/session/types"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlescape"
	"go.uber.org/zap"
)

const (
	internalUserParam     = "username"
	defaultUserHost       = "%"
	auditUnknownValue     = "<unknown>"
	auditRedactedPassword = "******"
	userAdminSessionAlias = "status-api"
)

type resetPasswordRequest struct {
	Reason    string `json:"reason"`
	ExpireNow bool   `json:"expire_now"`
}

type resetPasswordResponse struct {
	Status      string `json:"status"`
	NewPassword string `json:"new_password"`
}

type statusResponse struct {
	Status  string `json:"status"`
	Message string `json:"message"`
}

type createUserRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type createUserResponse struct {
	Status    string `json:"status"`
	Username  string `json:"username"`
	CreatedAt string `json:"created_at"`
}

// UserResetPasswordHandler handles internal user password reset.
type UserResetPasswordHandler struct {
	store kv.Storage
	cfg   *config.Config
}

// NewUserResetPasswordHandler creates a new UserResetPasswordHandler.
func NewUserResetPasswordHandler(store kv.Storage, cfg *config.Config) *UserResetPasswordHandler {
	return &UserResetPasswordHandler{store: store, cfg: cfg}
}

// ServeHTTP implements http.Handler.
func (h UserResetPasswordHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	username := strings.TrimSpace(mux.Vars(req)[internalUserParam])
	auditUser := auditValue(username)
	if req.Method != http.MethodPost {
		err := errors.New("only POST is supported")
		auditUserAdminStmt(req, buildAlterUserPasswordSQL(auditUser, defaultUserHost), err)
		WriteErrorWithCode(w, http.StatusMethodNotAllowed, err)
		return
	}
	if err := requireMTLS(req, h.cfg); err != nil {
		auditUserAdminStmt(req, buildAlterUserPasswordSQL(auditUser, defaultUserHost), err)
		WriteErrorWithCode(w, http.StatusForbidden, err)
		return
	}
	if username == "" {
		err := errors.New("missing username")
		auditUserAdminStmt(req, buildAlterUserPasswordSQL(auditUser, defaultUserHost), err)
		WriteErrorWithCode(w, http.StatusBadRequest, err)
		return
	}

	var payload resetPasswordRequest
	if err := json.NewDecoder(req.Body).Decode(&payload); err != nil && err != io.EOF {
		wrapped := errors.Wrap(err, "invalid request body")
		auditUserAdminStmt(req, buildAlterUserPasswordSQL(auditUser, defaultUserHost), wrapped)
		WriteErrorWithCode(w, http.StatusBadRequest, wrapped)
		return
	}

	newPassword, err := generateSecurePassword()
	if err != nil {
		auditUserAdminStmt(req, buildAlterUserPasswordSQL(auditUser, defaultUserHost), err)
		WriteErrorWithCode(w, http.StatusInternalServerError, err)
		return
	}

	ctx := kv.WithInternalSourceType(req.Context(), kv.InternalTxnOthers)
	se, err := session.CreateSession(h.store)
	if err != nil {
		auditUserAdminStmt(req, buildAlterUserPasswordSQL(auditUser, defaultUserHost), err)
		WriteErrorWithCode(w, http.StatusInternalServerError, err)
		return
	}
	defer se.Close()

	hosts, err := listUserHosts(ctx, se, username)
	if err != nil {
		auditUserAdminStmt(req, buildAlterUserPasswordSQL(auditUser, defaultUserHost), err)
		WriteErrorWithCode(w, http.StatusInternalServerError, err)
		return
	}
	if len(hosts) == 0 {
		err := errors.New("user not found")
		auditUserAdminStmt(req, buildAlterUserPasswordSQL(auditUser, defaultUserHost), err)
		WriteErrorWithCode(w, http.StatusNotFound, err)
		return
	}

	for _, host := range hosts {
		stmtText := buildAlterUserPasswordSQL(username, host)
		if _, err := se.ExecuteInternal(ctx, "ALTER USER %?@%? IDENTIFIED BY %?", username, host, newPassword); err != nil {
			auditUserAdminStmt(req, stmtText, err)
			WriteErrorWithCode(w, http.StatusInternalServerError, err)
			return
		}
		auditUserAdminStmt(req, stmtText, nil)
	}

	logutil.Logger(req.Context()).Info("internal user password reset",
		zap.String("user", username),
		zap.Bool("expire_now", payload.ExpireNow),
		zap.String("reason", payload.Reason))

	WriteData(w, resetPasswordResponse{
		Status:      "success",
		NewPassword: newPassword,
	})
}

// UserDeleteHandler handles internal user deletion.
type UserDeleteHandler struct {
	store kv.Storage
	cfg   *config.Config
}

// NewUserDeleteHandler creates a new UserDeleteHandler.
func NewUserDeleteHandler(store kv.Storage, cfg *config.Config) *UserDeleteHandler {
	return &UserDeleteHandler{store: store, cfg: cfg}
}

// ServeHTTP implements http.Handler.
func (h UserDeleteHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	username := strings.TrimSpace(mux.Vars(req)[internalUserParam])
	auditUser := auditValue(username)
	if req.Method != http.MethodDelete {
		err := errors.New("only DELETE is supported")
		auditUserAdminStmt(req, buildDropUserSQL(auditUser, defaultUserHost), err)
		WriteErrorWithCode(w, http.StatusMethodNotAllowed, err)
		return
	}
	if err := requireMTLS(req, h.cfg); err != nil {
		auditUserAdminStmt(req, buildDropUserSQL(auditUser, defaultUserHost), err)
		WriteErrorWithCode(w, http.StatusForbidden, err)
		return
	}
	if username == "" {
		err := errors.New("missing username")
		auditUserAdminStmt(req, buildDropUserSQL(auditUser, defaultUserHost), err)
		WriteErrorWithCode(w, http.StatusBadRequest, err)
		return
	}

	ctx := kv.WithInternalSourceType(req.Context(), kv.InternalTxnOthers)
	se, err := session.CreateSession(h.store)
	if err != nil {
		auditUserAdminStmt(req, buildDropUserSQL(auditUser, defaultUserHost), err)
		WriteErrorWithCode(w, http.StatusInternalServerError, err)
		return
	}
	defer se.Close()

	hosts, err := listUserHosts(ctx, se, username)
	if err != nil {
		auditUserAdminStmt(req, buildDropUserSQL(auditUser, defaultUserHost), err)
		WriteErrorWithCode(w, http.StatusInternalServerError, err)
		return
	}
	if len(hosts) == 0 {
		auditUserAdminStmt(req, buildDropUserSQL(auditUser, defaultUserHost), nil)
		w.WriteHeader(http.StatusNoContent)
		return
	}
	totalUsers, err := countUserEntries(ctx, se)
	if err != nil {
		auditUserAdminStmt(req, buildDropUserSQL(auditUser, defaultUserHost), err)
		WriteErrorWithCode(w, http.StatusInternalServerError, err)
		return
	}
	if totalUsers <= len(hosts) {
		err := errors.New("user cannot be deleted: last user")
		auditUserAdminStmt(req, buildDropUserSQL(auditUser, defaultUserHost), err)
		WriteErrorWithCode(w, http.StatusConflict, err)
		return
	}

	for _, host := range hosts {
		stmtText := buildDropUserSQL(username, host)
		if _, err := se.ExecuteInternal(ctx, "DROP USER %?@%?", username, host); err != nil {
			auditUserAdminStmt(req, stmtText, err)
			WriteErrorWithCode(w, http.StatusConflict, err)
			return
		}
		auditUserAdminStmt(req, stmtText, nil)
	}

	logutil.Logger(req.Context()).Info("internal user deleted", zap.String("user", username))
	w.WriteHeader(http.StatusNoContent)
}

// UserCreateHandler handles internal user creation.
type UserCreateHandler struct {
	store kv.Storage
	cfg   *config.Config
}

// NewUserCreateHandler creates a new UserCreateHandler.
func NewUserCreateHandler(store kv.Storage, cfg *config.Config) *UserCreateHandler {
	return &UserCreateHandler{store: store, cfg: cfg}
}

// ServeHTTP implements http.Handler.
func (h UserCreateHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	placeholderStmt := buildCreateUserSQL(auditUnknownValue, defaultUserHost)
	if req.Method != http.MethodPost {
		err := errors.New("only POST is supported")
		auditUserAdminStmt(req, placeholderStmt, err)
		WriteErrorWithCode(w, http.StatusMethodNotAllowed, err)
		return
	}
	if err := requireMTLS(req, h.cfg); err != nil {
		auditUserAdminStmt(req, placeholderStmt, err)
		WriteErrorWithCode(w, http.StatusForbidden, err)
		return
	}

	var payload createUserRequest
	if err := json.NewDecoder(req.Body).Decode(&payload); err != nil && err != io.EOF {
		wrapped := errors.Wrap(err, "invalid request body")
		auditUserAdminStmt(req, placeholderStmt, wrapped)
		WriteErrorWithCode(w, http.StatusBadRequest, wrapped)
		return
	}

	username := strings.TrimSpace(payload.Username)
	password := payload.Password
	stmtText := buildCreateUserSQL(auditValue(username), defaultUserHost)
	if username == "" || strings.TrimSpace(password) == "" {
		err := errors.New("missing username or password")
		auditUserAdminStmt(req, stmtText, err)
		WriteErrorWithCode(w, http.StatusBadRequest, err)
		return
	}

	ctx := kv.WithInternalSourceType(req.Context(), kv.InternalTxnOthers)
	se, err := session.CreateSession(h.store)
	if err != nil {
		auditUserAdminStmt(req, stmtText, err)
		WriteErrorWithCode(w, http.StatusInternalServerError, err)
		return
	}
	defer se.Close()

	hosts, err := listUserHosts(ctx, se, username)
	if err != nil {
		auditUserAdminStmt(req, stmtText, err)
		WriteErrorWithCode(w, http.StatusInternalServerError, err)
		return
	}
	if len(hosts) > 0 {
		err := errors.New("user already exists")
		auditUserAdminStmt(req, stmtText, err)
		WriteErrorWithCode(w, http.StatusConflict, err)
		return
	}

	if _, err := se.ExecuteInternal(ctx, "CREATE USER %?@%? IDENTIFIED BY %?", username, defaultUserHost, password); err != nil {
		auditUserAdminStmt(req, stmtText, err)
		if terror.ErrorEqual(err, variable.ErrNotValidPassword) {
			WriteErrorWithCode(w, http.StatusBadRequest, err)
			return
		}
		if terror.ErrorEqual(err, exeerrors.ErrCannotUser) {
			WriteErrorWithCode(w, http.StatusConflict, err)
			return
		}
		WriteErrorWithCode(w, http.StatusInternalServerError, err)
		return
	}
	auditUserAdminStmt(req, stmtText, nil)

	logutil.Logger(req.Context()).Info("internal user created", zap.String("user", username))

	resp := createUserResponse{
		Status:    "success",
		Username:  username,
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
	}
	js, err := json.MarshalIndent(resp, "", " ")
	if err != nil {
		WriteError(w, err)
		return
	}
	w.Header().Set(HeaderContentType, ContentTypeJSON)
	w.WriteHeader(http.StatusCreated)
	_, err = w.Write(js)
	terror.Log(errors.Trace(err))
}

type bindRolesRequest struct {
	Roles []string `json:"roles"`
	Role  string   `json:"role"`
}

// UserRolesHandler handles internal role bindings.
type UserRolesHandler struct {
	store kv.Storage
	cfg   *config.Config
}

// NewUserRolesHandler creates a new UserRolesHandler.
func NewUserRolesHandler(store kv.Storage, cfg *config.Config) *UserRolesHandler {
	return &UserRolesHandler{store: store, cfg: cfg}
}

// ServeHTTP implements http.Handler.
func (h UserRolesHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	username := strings.TrimSpace(mux.Vars(req)[internalUserParam])
	auditUser := auditValue(username)
	placeholderStmt := buildGrantRoleSQL(auditUnknownValue, defaultUserHost, auditUser, defaultUserHost)
	if req.Method != http.MethodPost {
		err := errors.New("only POST is supported")
		auditUserAdminStmt(req, placeholderStmt, err)
		WriteErrorWithCode(w, http.StatusMethodNotAllowed, err)
		return
	}
	if err := requireMTLS(req, h.cfg); err != nil {
		auditUserAdminStmt(req, placeholderStmt, err)
		WriteErrorWithCode(w, http.StatusForbidden, err)
		return
	}
	if username == "" {
		err := errors.New("missing username")
		auditUserAdminStmt(req, placeholderStmt, err)
		WriteErrorWithCode(w, http.StatusBadRequest, err)
		return
	}

	var payload bindRolesRequest
	if err := json.NewDecoder(req.Body).Decode(&payload); err != nil && err != io.EOF {
		wrapped := errors.Wrap(err, "invalid request body")
		auditUserAdminStmt(req, placeholderStmt, wrapped)
		WriteErrorWithCode(w, http.StatusBadRequest, wrapped)
		return
	}
	if payload.Role != "" {
		payload.Roles = append(payload.Roles, payload.Role)
	}
	roles := normalizeRoleList(payload.Roles)
	if len(roles) == 0 {
		err := errors.New("roles must not be empty")
		auditUserAdminStmt(req, placeholderStmt, err)
		WriteErrorWithCode(w, http.StatusBadRequest, err)
		return
	}
	auditRoleName, auditRoleHost := splitRole(roles[0])

	ctx := kv.WithInternalSourceType(req.Context(), kv.InternalTxnOthers)
	se, err := session.CreateSession(h.store)
	if err != nil {
		auditUserAdminStmt(req, buildGrantRoleSQL(auditRoleName, auditRoleHost, auditUser, defaultUserHost), err)
		WriteErrorWithCode(w, http.StatusInternalServerError, err)
		return
	}
	defer se.Close()

	hosts, err := listUserHosts(ctx, se, username)
	if err != nil {
		auditUserAdminStmt(req, buildGrantRoleSQL(auditRoleName, auditRoleHost, auditUser, defaultUserHost), err)
		WriteErrorWithCode(w, http.StatusInternalServerError, err)
		return
	}
	if len(hosts) == 0 {
		err := errors.New("user not found")
		auditUserAdminStmt(req, buildGrantRoleSQL(auditRoleName, auditRoleHost, auditUser, defaultUserHost), err)
		WriteErrorWithCode(w, http.StatusNotFound, err)
		return
	}

	for _, host := range hosts {
		for _, role := range roles {
			roleName, roleHost := splitRole(role)
			stmtText := buildGrantRoleSQL(roleName, roleHost, username, host)
			if _, err := se.ExecuteInternal(ctx, "GRANT %?@%? TO %?@%?", roleName, roleHost, username, host); err != nil {
				auditUserAdminStmt(req, stmtText, err)
				WriteErrorWithCode(w, http.StatusInternalServerError, err)
				return
			}
			auditUserAdminStmt(req, stmtText, nil)
		}
	}

	logutil.Logger(req.Context()).Info("internal roles granted",
		zap.String("user", username),
		zap.Strings("roles", roles))
	WriteData(w, statusResponse{
		Status:  "success",
		Message: "Roles granted to user.",
	})
}

func listUserHosts(ctx context.Context, se sessiontypes.Session, username string) ([]string, error) {
	rs, err := se.ExecuteInternal(ctx, "SELECT Host FROM mysql.user WHERE User = %?", username)
	if err != nil {
		return nil, err
	}
	if rs == nil {
		return nil, nil
	}
	defer terror.Call(rs.Close)

	hosts := make([]string, 0)
	for {
		chk := rs.NewChunk(nil)
		if err := rs.Next(ctx, chk); err != nil {
			return nil, err
		}
		if chk.NumRows() == 0 {
			break
		}
		for i := 0; i < chk.NumRows(); i++ {
			hosts = append(hosts, chk.GetRow(i).GetString(0))
		}
	}
	return hosts, nil
}

func countUserEntries(ctx context.Context, se sessiontypes.Session) (int, error) {
	rs, err := se.ExecuteInternal(ctx, "SELECT COUNT(*) FROM mysql.user")
	if err != nil {
		return 0, err
	}
	if rs == nil {
		return 0, nil
	}
	defer terror.Call(rs.Close)

	chk := rs.NewChunk(nil)
	if err := rs.Next(ctx, chk); err != nil {
		return 0, err
	}
	if chk.NumRows() == 0 {
		return 0, nil
	}
	return int(chk.GetRow(0).GetInt64(0)), nil
}

func normalizeRoleList(roles []string) []string {
	seen := make(map[string]struct{}, len(roles))
	out := make([]string, 0, len(roles))
	for _, role := range roles {
		role = strings.TrimSpace(role)
		if role == "" {
			continue
		}
		if _, ok := seen[role]; ok {
			continue
		}
		seen[role] = struct{}{}
		out = append(out, role)
	}
	return out
}

func splitRole(role string) (string, string) {
	at := strings.LastIndex(role, "@")
	if at <= 0 || at == len(role)-1 {
		return role, defaultUserHost
	}
	return role[:at], role[at+1:]
}

func requireMTLS(_ *http.Request, cfg *config.Config) error {
	// cmux wraps TLS connections and may hide the TLS state from net/http, so we rely on
	// the status server being configured to require client certificates.
	if cfg != nil && cfg.Security.ClusterSSLCA != "" && len(cfg.Security.ClusterVerifyCN) > 0 {
		return nil
	}
	return errors.New("client certificate invalid or missing (mTLS failure)")
}

func auditValue(value string) string {
	if strings.TrimSpace(value) == "" {
		return auditUnknownValue
	}
	return value
}

func buildAlterUserExpireSQL(username, host string) string {
	return sqlescape.MustEscapeSQL("ALTER USER %?@%? PASSWORD EXPIRE", username, host)
}

func buildAlterUserPasswordSQL(username, host string) string {
	return sqlescape.MustEscapeSQL("ALTER USER %?@%? IDENTIFIED BY %?", username, host, auditRedactedPassword)
}

func buildCreateUserSQL(username, host string) string {
	return sqlescape.MustEscapeSQL("CREATE USER %?@%? IDENTIFIED BY %?", username, host, auditRedactedPassword)
}

func buildDropUserSQL(username, host string) string {
	return sqlescape.MustEscapeSQL("DROP USER %?@%?", username, host)
}

func buildGrantRoleSQL(roleName, roleHost, username, userHost string) string {
	return sqlescape.MustEscapeSQL("GRANT %?@%? TO %?@%?", roleName, roleHost, username, userHost)
}

func generateSecurePassword() (string, error) {
	const (
		lowerChars   = "abcdefghijklmnopqrstuvwxyz"
		upperChars   = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
		digitChars   = "0123456789"
		specialChars = "!#$*-_=+"
		passwordLen  = 24
	)

	allChars := lowerChars + upperChars + digitChars + specialChars
	password := make([]byte, passwordLen)
	requiredSets := []string{lowerChars, upperChars, digitChars, specialChars}
	for i, charset := range requiredSets {
		ch, err := randomPasswordChar(charset)
		if err != nil {
			return "", err
		}
		password[i] = ch
	}
	for i := len(requiredSets); i < passwordLen; i++ {
		ch, err := randomPasswordChar(allChars)
		if err != nil {
			return "", err
		}
		password[i] = ch
	}
	for i := len(password) - 1; i > 0; i-- {
		j, err := secureRandomInt(i + 1)
		if err != nil {
			return "", err
		}
		password[i], password[j] = password[j], password[i]
	}
	return string(password), nil
}

func randomPasswordChar(charset string) (byte, error) {
	idx, err := secureRandomInt(len(charset))
	if err != nil {
		return 0, err
	}
	return charset[idx], nil
}

func secureRandomInt(max int) (int, error) {
	n, err := rand.Int(rand.Reader, big.NewInt(int64(max)))
	if err != nil {
		return 0, err
	}
	return int(n.Int64()), nil
}

func auditUserAdminStmt(req *http.Request, stmtText string, err error) {
	extensions, extErr := extension.GetExtensions()
	if extErr != nil {
		logutil.Logger(req.Context()).Debug("failed to get extensions for audit log", zap.Error(extErr))
		return
	}
	sessExtensions := extensions.NewSessionExtensions()
	if sessExtensions == nil || !sessExtensions.HasStmtEventListeners() {
		return
	}

	stmtNode := parseStmtNode(stmtText)
	normalized, digest := parser.NormalizeDigest(stmtText)
	user, connInfo := auditUserFromRequest(req)
	info := &userAdminStmtEventInfo{
		user:          user,
		connInfo:      connInfo,
		sessionAlias:  userAdminSessionAlias,
		stmtNode:      stmtNode,
		originalText:  stmtText,
		normalizedSQL: normalized,
		digest:        digest,
		err:           err,
	}
	tp := extension.StmtSuccess
	if err != nil {
		tp = extension.StmtError
	}
	sessExtensions.OnStmtEvent(tp, info)
}

func parseStmtNode(sql string) ast.StmtNode {
	if strings.TrimSpace(sql) == "" {
		return nil
	}
	stmt, err := parser.New().ParseOneStmt(sql, "", "")
	if err != nil {
		return nil
	}
	return stmt
}

func auditUserFromRequest(req *http.Request) (*auth.UserIdentity, *variable.ConnectionInfo) {
	if req == nil {
		return nil, nil
	}
	host, port := splitHostPort(req.RemoteAddr)
	user := clientCertCommonName(req)
	connType := variable.ConnTypeSocket
	if req.TLS != nil {
		connType = variable.ConnTypeTLS
	}
	connInfo := &variable.ConnectionInfo{
		User:           user,
		Host:           host,
		ClientIP:       host,
		ClientPort:     port,
		ConnectionType: connType,
	}
	userIdentity := &auth.UserIdentity{
		Username:     user,
		Hostname:     host,
		AuthUsername: user,
		AuthHostname: host,
	}
	return userIdentity, connInfo
}

func clientCertCommonName(req *http.Request) string {
	if req == nil || req.TLS == nil || len(req.TLS.PeerCertificates) == 0 {
		return ""
	}
	return req.TLS.PeerCertificates[0].Subject.CommonName
}

func splitHostPort(addr string) (string, string) {
	if addr == "" {
		return "", ""
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return addr, ""
	}
	return host, port
}

type userAdminStmtEventInfo struct {
	user          *auth.UserIdentity
	activeRoles   []*auth.RoleIdentity
	currentDB     string
	connInfo      *variable.ConnectionInfo
	sessionAlias  string
	stmtNode      ast.StmtNode
	originalText  string
	normalizedSQL string
	digest        *parser.Digest
	err           error
}

func (i *userAdminStmtEventInfo) User() *auth.UserIdentity {
	return i.user
}

func (i *userAdminStmtEventInfo) ActiveRoles() []*auth.RoleIdentity {
	return i.activeRoles
}

func (i *userAdminStmtEventInfo) CurrentDB() string {
	return i.currentDB
}

func (i *userAdminStmtEventInfo) ConnectionInfo() *variable.ConnectionInfo {
	return i.connInfo
}

func (i *userAdminStmtEventInfo) SessionAlias() string {
	return i.sessionAlias
}

func (i *userAdminStmtEventInfo) StmtNode() ast.StmtNode {
	return i.stmtNode
}

func (i *userAdminStmtEventInfo) ExecuteStmtNode() *ast.ExecuteStmt {
	return nil
}

func (i *userAdminStmtEventInfo) ExecutePreparedStmt() ast.StmtNode {
	return nil
}

func (i *userAdminStmtEventInfo) PreparedParams() []types.Datum {
	return nil
}

func (i *userAdminStmtEventInfo) OriginalText() string {
	return i.originalText
}

func (i *userAdminStmtEventInfo) SQLDigest() (normalized string, digest *parser.Digest) {
	return i.normalizedSQL, i.digest
}

func (i *userAdminStmtEventInfo) AffectedRows() uint64 {
	return 0
}

func (i *userAdminStmtEventInfo) RelatedTables() []stmtctx.TableEntry {
	return nil
}

func (i *userAdminStmtEventInfo) GetError() error {
	return i.err
}
