package grpcmesh

import (
	"fmt"
	"testing"
)

const name string = "nb_hostname"
const port uint16 = 54321

func TestGRPCNeighbor_String(t *testing.T) {
	nb := newGRPCNeighbor(name, port)
	str := nb.String()
	if str != name {
		t.Fatalf("Neighbor String() should return hostname")
	}
}

func TestGRPCNeighbor_ConnectionString(t *testing.T) {
	nb := newGRPCNeighbor(name, port)
	str := nb.ConnectionString()
	if str != fmt.Sprintf("%v:%v", name, port) {
		t.Fatalf("Neighbor String() should return hostname")
	}
}
