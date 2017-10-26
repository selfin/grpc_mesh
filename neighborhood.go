// Package grpc_mesh
// provides api to build full mesh gRPC connections between service instances,
// consul service discovery api used as backend for neighbors lookup
package grpc_mesh

import (
	"fmt"
	consul "github.com/hashicorp/consul/api"
	"google.golang.org/grpc"
	"log"
	"os"
	"sync"
	"time"
)

// Neighborhood
// Server example:

type Neighborhood interface {
	// ConfigureConsul set consul config, if default config not suitable, use it before calling any other func
	ConfigureConsul(*consul.Config) error
	// Set MygRPCClient clent connector
	// If connector didn't set, neighbors would not be added
	WithConnector(f func(conn *grpc.ClientConn) (rpc_client interface{}))
	// Announce registers services with consul
	Announce(services ...*consul.AgentServiceRegistration) error
	// StopAnnounce deregisters all announced services and stops Locator
	StopAnnounce()
	// Locator is long living goroutine updating internal neighbors with discovered
	Locator(service string, tag string, interval time.Duration)
	// Search once updates internal neighbors with discovered
	Search(service string, tag string)
	// GetNeighbor by its host
	GetNeighbor(string) (neighbor *GRPCNeighbor, ok bool)
	// GetNeighbors returns all discovered neighbors
	GetNeighbors() []*GRPCNeighbor
	// DisconnectNeighbor notifies Neighborhood to remove neighbor by its hostname
	DisconnectNeighbor(string)
}

type gRPCNeighbors struct {
	mu            sync.RWMutex
	messages      *log.Logger
	errors        *log.Logger
	announcer     string
	connector     func(conn *grpc.ClientConn) (rpc_client interface{})
	services      []*consul.AgentServiceRegistration
	client        *consul.Client
	client_conf   *consul.Config
	updater_close chan bool
	neighbors     map[string]*GRPCNeighbor
}

// NewNeighborhood returns prepared consul based discovery
func NewNeighborhood() Neighborhood {
	return &gRPCNeighbors{
		neighbors:     make(map[string]*GRPCNeighbor),
		updater_close: make(chan bool),
		messages:      log.New(os.Stdout, "", log.LstdFlags),
		errors:        log.New(os.Stderr, "ERROR: ", log.LstdFlags|log.Lshortfile|log.Lmicroseconds),
	}
}

// ConfigureConsul provides ability to set custom consul configuration
func (gns *gRPCNeighbors) ConfigureConsul(config *consul.Config) error {
	gns.mu.Lock()
	defer gns.mu.Unlock()
	if gns.client_conf == nil {
		gns.client_conf = config
	} else {
		return fmt.Errorf("configuration already created, use ConfigureConsul before running Announce and Search")
	}
	return nil
}

// WithConnector set MygRPCClient client connection realization
func (gns *gRPCNeighbors) WithConnector(f func(conn *grpc.ClientConn) (rpc_client interface{})) {
	gns.mu.Lock()
	gns.connector = f
	gns.mu.Unlock()
}

// Announce registers provided services with consul api
func (gns *gRPCNeighbors) Announce(services ...*consul.AgentServiceRegistration) error {
	gns.initConsul()
	if name, err := gns.client.Agent().NodeName(); err != nil {
		return err
	} else {
		gns.mu.Lock()
		gns.announcer = name
		gns.mu.Unlock()
	}
	for _, service := range services {
		if err := gns.client.Agent().ServiceRegister(service); err != nil {
			return err
		}
		gns.services = append(gns.services, service)
	}
	return nil
}

// StopAnnounce stops Locator and deregister all registered with consul services
func (gns *gRPCNeighbors) StopAnnounce() {
	gns.mu.Lock()
	close(gns.updater_close)
	for _, service := range gns.services {
		if err := gns.client.Agent().ServiceDeregister(service.Name); err != nil {
			gns.errors.Printf("Error deregistering service %v", service.Name)
		}
	}
	gns.mu.Unlock()
	return
}

// GetNeighbors returns list of all currently active neighbors
func (gns *gRPCNeighbors) GetNeighbors() []*GRPCNeighbor {
	gns.mu.RLock()
	neighbors := make([]*GRPCNeighbor, 0, len(gns.neighbors))
	for _, gn := range gns.neighbors {
		neighbors = append(neighbors, gn)
	}
	gns.mu.RUnlock()
	return neighbors
}

// GetNeighbor returns GRPCNeighbor struct by hostname
func (gns *gRPCNeighbors) GetNeighbor(host string) (*GRPCNeighbor, bool) {
	gns.mu.RLock()
	neighbor, ok := gns.neighbors[host]
	gns.mu.RUnlock()
	return neighbor, ok
}
func (gns *gRPCNeighbors) DisconnectNeighbor(host string) {
	go gns.removeClient(host)
}

// Locator infinitive loop running Search every minute
// use it with Announce to update neighbors in background
func (gns *gRPCNeighbors) Locator(service string, tag string, interval time.Duration) {
	gns.messages.Printf("Consul neighbors updater started")
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			gns.Search(service, tag)

		case <-gns.updater_close:
			gns.messages.Printf("Consul neighbors updater stopped")
			return
		}
	}
}

// Search makes once discover of all Healthy service neighbors
// filtered by the tag and store it in internal map
func (gns *gRPCNeighbors) Search(service string, tag string) {
	gns.initConsul()
	entries, _, err := gns.client.Health().Service(service, tag, true, nil)
	if err != nil {
		gns.errors.Printf("Error getting service nodes: %v", err)
	}
	if len(entries) == 0 {
		return
	}
	var self string
	gns.mu.RLock()
	if gns.announcer != "" {
		self = gns.announcer
	}
	gns.mu.RUnlock()
	lst := make([]*GRPCNeighbor, 0, len(entries))
	for _, entry := range entries {
		if self != "" {
			if entry.Node.Node == self {
				// filter self node for announcer
				continue
			}
		}
		if entry.Service.Address == "" || entry.Service.Port == 0 {
			gns.errors.Printf("Bad service %v definition, address and port is obligatory", entry.Service.ID)
		}
		lst = append(lst, newGRPCNeighbor(entry.Service.Address, uint16(entry.Service.Port)))
	}
	gns.updateNeighbors(lst)
}

// updateNeighbors validates state of local map
// new neighbors list should contain all discovered neighbors
// all connections not presented in in will be closed
// for all new neighbors will be created new GRPC connection
func (gns *gRPCNeighbors) updateNeighbors(neighbors []*GRPCNeighbor) {
	tmp := make(map[string]bool)
	gns.mu.Lock()
	for _, gn := range neighbors {
		tmp[gn.Host] = true
		if _, ok := gns.neighbors[gn.Host]; !ok {
			gns.messages.Printf("New gRPCNeighbor: %v", gn)
			go gns.createClient(gn)
		}
	}
	for host := range gns.neighbors {
		if _, ok := tmp[host]; !ok {
			gns.messages.Printf("Closing connection with %v ", host)
			go gns.removeClient(host)
		}
	}
	gns.mu.Unlock()
	// Here we need to give chance to client to connect
	time.Sleep(time.Millisecond * 100)
}

// initConsul creates consul api struct if not present
func (gns *gRPCNeighbors) initConsul() (err error) {
	gns.mu.Lock()
	defer gns.mu.Unlock()
	if gns.client != nil {
		return nil
	}
	if gns.client_conf == nil {
		gns.client_conf = consul.DefaultConfig()
	}
	if gns.client, err = consul.NewClient(gns.client_conf); err != nil {
		return err
	}
	return nil

}

// createClient initialize new MygRPCClient connection and add neighbor to the internal map
func (gns *gRPCNeighbors) createClient(gn *GRPCNeighbor) {
	gns.mu.Lock()
	defer gns.mu.Unlock()
	if gns.connector == nil {
		gns.errors.Printf("gRPC NeighborhoodConnector not set, ignoring neighbor %v", gn)
		return
	}
	if err := gn.connector(gns.connector); err != nil {
		gns.errors.Printf("Error dialing to %v: %v", gn, err)
		return
	}
	gns.neighbors[gn.Host] = gn
	gns.messages.Printf("Connected with neighbor %v", gn)
	return
}

// removeClient closes neighbor's connection and remove it from internal map
func (gns *gRPCNeighbors) removeClient(host string) {
	gns.mu.Lock()
	if gn, ok := gns.neighbors[host]; ok {
		gn.conn.Close()
		delete(gns.neighbors, host)
	}
	gns.mu.Unlock()
}
