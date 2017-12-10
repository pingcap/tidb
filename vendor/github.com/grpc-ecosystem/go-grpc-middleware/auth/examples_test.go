// Copyright 2016 Michal Witkowski. All Rights Reserved.
// See LICENSE for licensing terms.

package grpc_auth_test

import (
	"github.com/grpc-ecosystem/go-grpc-middleware/auth"
	"github.com/grpc-ecosystem/go-grpc-middleware/tags"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

var cc *grpc.ClientConn

func parseToken(token string) (struct{}, error) {
	return struct{}{}, nil
}

func userClaimFromToken(struct{}) string {
	return "foobar"
}

// Simple example of an `AuthFunc` that extracts, verifies the token and sets it in the handler
// contexts.
func Example_authfunc(ctx context.Context) (context.Context, error) {
	token, err := grpc_auth.AuthFromMD(ctx, "bearer")
	if err != nil {
		return nil, err
	}
	tokenInfo, err := parseToken(token)
	if err != nil {
		return nil, grpc.Errorf(codes.Unauthenticated, "invalid auth token: %v", err)
	}
	grpc_ctxtags.Extract(ctx).Set("auth.sub", userClaimFromToken(tokenInfo))
	newCtx := context.WithValue(ctx, "tokenInfo", tokenInfo)
	return newCtx, nil
}

// Simple example of server initialization code.
func Example_serverconfig() *grpc.Server {
	server := grpc.NewServer(
		grpc.StreamInterceptor(grpc_auth.StreamServerInterceptor(Example_authfunc)),
		grpc.UnaryInterceptor(grpc_auth.UnaryServerInterceptor(Example_authfunc)),
	)
	return server
}
