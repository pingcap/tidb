package server

import (
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"strings"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/sessionstates"
	"github.com/pingcap/tidb/pkg/util/fastrand"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	sessionTokenPrefix  = "session:"
	authorizationHeader = "authorization"
	basicAuthPrefix     = "Basic"
	bearerPrefix        = "Bearer"
	authTokenBinHeader  = "auth-token-bin"
)

type authCtxKey struct{}

// AuthFromContext returns the authenticated user identity from the context.
func AuthFromContext(ctx context.Context) interface{} {
	return ctx.Value(authCtxKey{})
}

// Authenticate implements the ServerAuthHandler interface for handshake-based authentication.
// This is called during the Handshake RPC when a client wants to exchange credentials for a token.
func (s *FlightSQLServer) Authenticate(c flight.AuthConn) error {
	// Read the credentials from the client
	in, err := c.Read()
	if err != nil {
		return err
	}

	// Parse Basic auth: "username:password"
	credentials := string(in)
	colonIndex := strings.IndexByte(credentials, ':')
	if colonIndex < 0 {
		return errors.New("invalid credentials format, expected username:password")
	}

	username := credentials[:colonIndex]
	password := credentials[colonIndex+1:]

	// Authenticate and generate session token
	token, err := s.authenticateUser(username, password)
	if err != nil {
		return err
	}

	// Send the token back to the client
	return c.Send([]byte(token))
}

// IsValid implements the ServerAuthHandler interface for validating tokens on each request.
// The token parameter can be:
// 1. Raw "username:password" (from Basic auth, extracted by our middleware)
// 2. Session token with "session:" prefix
// 3. Raw bearer token (future)
func (s *FlightSQLServer) IsValid(token string) (interface{}, error) {
	if len(token) == 0 {
		return nil, errors.New("no authentication token provided")
	}

	// Check if it's raw credentials "username:password" (from Basic auth)
	if colonIndex := strings.IndexByte(token, ':'); colonIndex > 0 && !strings.HasPrefix(token, sessionTokenPrefix) {
		username := token[:colonIndex]
		password := token[colonIndex+1:]

		// Validate credentials
		_, err := s.authenticateUser(username, password)
		if err != nil {
			return nil, err
		}

		return username, nil
	}

	// Check if it's a session token (prefixed with "session:")
	if strings.HasPrefix(token, sessionTokenPrefix) {
		tokenStr := token[len(sessionTokenPrefix):]
		tokenBytes, err := base64.StdEncoding.DecodeString(tokenStr)
		if err != nil {
			return nil, errors.Wrap(err, "failed to decode session token")
		}

		// Parse the token to extract username
		var sessionToken sessionstates.SessionToken
		if err := json.Unmarshal(tokenBytes, &sessionToken); err != nil {
			return nil, errors.Wrap(err, "failed to unmarshal session token")
		}

		// Validate the session token
		if err := sessionstates.ValidateSessionToken(tokenBytes, sessionToken.Username); err != nil {
			return nil, errors.Wrap(err, "invalid session token")
		}

		// Return username as the user context
		return sessionToken.Username, nil
	}

	// If no prefix, treat as raw bearer token (not yet implemented)
	return nil, errors.New("raw bearer token authentication not yet implemented")
}

// authenticateUser validates username/password and returns a session token.
// This shared logic is used by Authenticate (handshake).
func (s *FlightSQLServer) authenticateUser(username, password string) (string, error) {
	// Create a temporary context for authentication
	ctx, err := s.server.driver.OpenCtx(uint64(0), 0, uint8(mysql.DefaultCollationID), "", nil, nil)
	if err != nil {
		return "", errors.Wrap(err, "failed to open context for authentication")
	}
	defer ctx.Close()

	// For Flight SQL, we receive plaintext passwords (over TLS)
	// We need to scramble them server-side since the client doesn't do it

	// Generate a salt for password scrambling
	salt := fastrand.Buf(20)

	// Scramble the plaintext password using mysql_native_password algorithm
	// This matches what a MySQL client would do
	var authData []byte
	if len(password) > 0 {
		authData = scrambleNativePassword(password, salt)
	}

	// Create user identity with native password auth
	userIdentity := &auth.UserIdentity{
		Username:   username,
		Hostname:   "%",
		AuthPlugin: mysql.AuthNativePassword,
	}

	// Authenticate with the scrambled password
	if err = ctx.Auth(userIdentity, authData, salt, nil); err != nil {
		return "", errors.Wrap(err, "authentication failed")
	}

	// Authentication successful, create a session token
	token, err := sessionstates.CreateSessionToken(username)
	if err != nil {
		return "", errors.Wrap(err, "failed to create session token")
	}

	// Encode token as JSON
	tokenBytes, err := json.Marshal(token)
	if err != nil {
		return "", errors.Wrap(err, "failed to marshal session token")
	}

	// Return base64-encoded token with prefix
	tokenStr := base64.StdEncoding.EncodeToString(tokenBytes)
	return sessionTokenPrefix + tokenStr, nil
}

// scrambleNativePassword implements the mysql_native_password scrambling algorithm.
// This is what a MySQL client does when authenticating with a plaintext password.
// Algorithm: token = SHA1(password) XOR SHA1(salt || SHA1(SHA1(password)))
func scrambleNativePassword(password string, salt []byte) []byte {
	if len(password) == 0 {
		return nil
	}

	// s1 = SHA1(password)
	h := sha1.New()
	h.Write([]byte(password))
	s1 := h.Sum(nil)

	// s2 = SHA1(s1)
	h.Reset()
	h.Write(s1)
	s2 := h.Sum(nil)

	// s3 = SHA1(salt || s2)
	h.Reset()
	h.Write(salt)
	h.Write(s2)
	s3 := h.Sum(nil)

	// token = s1 XOR s3
	token := make([]byte, len(s1))
	for i := range s1 {
		token[i] = s1[i] ^ s3[i]
	}

	return token
}

// createFlightSQLAuthMiddleware creates a custom auth middleware that:
// 1. Handles Handshake RPC with Basic auth
// 2. Handles other RPCs with bearer tokens
// 3. Supports username/password on every request (for FlightSQL driver compatibility)
func createFlightSQLAuthMiddleware(handler flight.ServerAuthHandler) flight.ServerMiddleware {
	return flight.ServerMiddleware{
		Unary:  createAuthUnaryInterceptor(handler),
		Stream: createAuthStreamInterceptor(handler),
	}
}

func createAuthUnaryInterceptor(handler flight.ServerAuthHandler) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, unary grpc.UnaryHandler) (interface{}, error) {
		// Extract auth token from metadata
		token, err := extractAuthToken(ctx)
		if err != nil {
			return nil, status.Errorf(codes.Unauthenticated, "auth error: %s", err)
		}

		// Validate the token
		identity, err := handler.IsValid(token)
		if err != nil {
			return nil, status.Errorf(codes.Unauthenticated, "auth error: %s", err)
		}

		// Add identity to context
		ctx = context.WithValue(ctx, authCtxKey{}, identity)
		return unary(ctx, req)
	}
}

type wrappedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedStream) Context() context.Context {
	return w.ctx
}

func createAuthStreamInterceptor(handler flight.ServerAuthHandler) grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, streamHandler grpc.StreamHandler) error {
		// Skip auth for Handshake RPC - it has its own auth flow
		if strings.HasSuffix(info.FullMethod, "/Handshake") {
			return streamHandler(srv, stream)
		}

		// Extract auth token from metadata
		token, err := extractAuthToken(stream.Context())
		if err != nil {
			return status.Errorf(codes.Unauthenticated, "auth error: %s", err)
		}

		// Validate the token
		identity, err := handler.IsValid(token)
		if err != nil {
			return status.Errorf(codes.Unauthenticated, "auth error: %s", err)
		}

		// Wrap stream with auth context
		wrapped := &wrappedStream{
			ServerStream: stream,
			ctx:          context.WithValue(stream.Context(), authCtxKey{}, identity),
		}

		return streamHandler(srv, wrapped)
	}
}

// extractAuthToken extracts the authentication token from the request metadata.
// It checks both "authorization" header (for Basic/Bearer auth) and "auth-token-bin" header.
func extractAuthToken(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", nil
	}

	// Check for authorization header first (Basic or Bearer)
	if authHeaders := md.Get(authorizationHeader); len(authHeaders) > 0 {
		authHeader := authHeaders[0]

		// Handle Basic auth: extract username/password and use as-is
		// This allows FlightSQL driver to send username/password on every request
		if strings.HasPrefix(authHeader, basicAuthPrefix+" ") {
			// Decode base64-encoded "username:password"
			encoded := authHeader[len(basicAuthPrefix)+1:]
			decoded, err := base64.StdEncoding.DecodeString(encoded)
			if err != nil {
				return "", err
			}
			// Return the raw "username:password" string
			// The auth handler will need to validate this
			return string(decoded), nil
		}

		// Handle Bearer token
		if strings.HasPrefix(authHeader, bearerPrefix+" ") {
			return authHeader[len(bearerPrefix)+1:], nil
		}
	}

	// Check for auth-token-bin header (used after handshake)
	if authTokens := md.Get(authTokenBinHeader); len(authTokens) > 0 {
		return authTokens[0], nil
	}

	return "", nil
}

