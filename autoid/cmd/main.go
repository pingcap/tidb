package main

import (
	"flag"
	"fmt"
	"log"
	"net"

	pb "github.com/pingcap/kvproto/pkg/autoid"
	autoid "github.com/tiancaiamao/autoid"
	"google.golang.org/grpc"
)

var (
	port = flag.Int("port", 50051, "The server port")
)

// server is used to implement helloworld.GreeterServer.
type server struct {
	*autoid.Service
}

func main() {
	flag.Parse()
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterAutoIDAllocServer(s, &server{
		Service: autoid.New(),
	})
	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
