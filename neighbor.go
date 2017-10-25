package grpc_mesh

import (
	"fmt"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"log"
	"sync"
)

type GRPCNeighbor struct {
	mu sync.Mutex
	// Host - discovered service neighbor hostname or ip addr
	Host string
	// Port - discovered service neighbor MygRPCClient port
	Port uint16
	// Client - interface to get your MygRPCClient Client
	// use client, _ := GRPCNeighbor.Client.(MyGRPCClient)
	Client interface{}
	conn   *grpc.ClientConn
}

// newGRPCNeighbor returns prepares
func newGRPCNeighbor(host string, port uint16) *GRPCNeighbor {
	return &GRPCNeighbor{
		Host: host,
		Port: port,
	}
}

// String returns hostname of the neighbor
func (gn *GRPCNeighbor) String() string {
	return gn.Host
}

// ConnectionString returns string in format host:port
func (gn *GRPCNeighbor) ConnectionString() string {
	return fmt.Sprintf("%v:%v", gn.Host, gn.Port)
}

func (gn *GRPCNeighbor) connector(f func(conn *grpc.ClientConn) (rpc_client interface{})) error {
	gn.mu.Lock()
	defer gn.mu.Unlock()
	var err error
	gn.conn, err = grpc.DialContext(context.Background(), gn.ConnectionString(), grpc.WithInsecure())
	if err != nil {
		log.Printf("Error dialing neighbor: %v", err)
		return err
	}
	gn.Client = f(gn.conn)
	return err
}
