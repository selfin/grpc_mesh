package grpc_mesh

import (
	"fmt"
	consul "github.com/hashicorp/consul/api"
	"google.golang.org/grpc"
	"log"
	"reflect"
	"testing"
	"time"
)

type MygRPCClient struct {
}

func (g *MygRPCClient) DoSomething() bool {
	return true
}

func AbstractgRPCClient(conn *grpc.ClientConn) (rpc_client interface{}) {
	return MygRPCClient{}

}

func ExampleNewGRPCNeighbors() {
	const service_name string = "RPCService"
	const hostname string = "hostname"
	gRPCNeighborhood := NewGRPCNeighbors()
	gRPCNeighborhood.WithConnector(AbstractgRPCClient)
	// Port and Address is obligatory for proper work
	RPCService := &consul.AgentServiceRegistration{
		ID:      service_name,
		Name:    service_name,
		Port:    4000,
		Tags:    []string{"datacenter1"},
		Address: hostname,
	}
	if err := gRPCNeighborhood.Announce(RPCService); err != nil {
		log.Fatalln(err)
	}
	go gRPCNeighborhood.Locator(service_name, "datacenter1", time.Minute)

	// Using discovery
	neighbor, _ := gRPCNeighborhood.GetNeighbor(hostname)
	gRPCClient, _ := neighbor.Client.(MygRPCClient)
	gRPCClient.DoSomething()

}

func initNeighborhood(t *testing.T) Neighborhood {
	neighborhood := NewGRPCNeighbors()
	neighborhood.WithConnector(AbstractgRPCClient)
	if len(neighborhood.neighbors) != 0 {
		t.Fatalf("Not nil neighbor list")
	}
	n1 := newGRPCNeighbor("host1", 1)
	n2 := newGRPCNeighbor("host2", 2)
	n3 := newGRPCNeighbor("host3", 3)
	n4 := newGRPCNeighbor("host4", 4)
	// some update validation
	neighborhood.updateNeighbors([]*GRPCNeighbor{n1, n3})
	neighborhood.updateNeighbors([]*GRPCNeighbor{n2, n4})
	return neighborhood
}

func TestGRPCNeighbors_GetNeighbor(t *testing.T) {
	n := initNeighborhood(t)
	nb, ok := n.GetNeighbor("host2")
	if !ok {
		t.Fatalf("GetNeighbor do not return neighbor")
	}
	if nb.Port != 2 {
		t.Fatalf("GetNeighbor do not return needed neighbor")
	}
}

func TestGRPCNeighbors_DisconnectNeighbor(t *testing.T) {
	n := initNeighborhood(t)
	_, ok := n.GetNeighbor("host2")
	if !ok {
		t.Fatalf("GetNeighbor do not return neighbor")
	}
	n.DisconnectNeighbor("host2")
	// Deletion async
	time.Sleep(time.Millisecond * 300)
	_, ok = n.GetNeighbor("host2")
	if ok {
		t.Fatalf("DisconnectNeighbor do not remove neighbor")
	}
}

func TestGRPCNeighbors_WithConnector(t *testing.T) {
	n := initNeighborhood(t)
	nb, ok := n.GetNeighbor("host4")
	if !ok {
		t.Fatalf("GetNeighbor do not return neighbor")
	}
	gRPCClient, ok := nb.Client.(MygRPCClient)
	if !ok {

		t.Fatalf("Problem with reflecting client needed `MygRPCClient` got %v", reflect.TypeOf(nb.Client))
	}
	if !gRPCClient.DoSomething() {
		t.Fatalf("Problem with gRPCClient after reflecting")
	}
}

func TestGRPCNeighbors_GetNeighbors(t *testing.T) {
	n := initNeighborhood(t)
	lst := n.GetNeighbors()
	if len(lst) != 2 {
		t.Fatalf("Wrong neigbors count after updates")
	}
	for _, nb := range lst {
		if nb.Port != 2 {
			if nb.Port != 4 {
				t.Fatalf("Wrong neigbors after updates, got %v", nb)
			}
		}
	}
}
