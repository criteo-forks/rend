package consul

import (
	"fmt"
	"github.com/hashicorp/consul/api"
	"log"
)

// GetNodes return a list of node addresses from a given Consul service
func GetNodes(service string, consulAddr string, dc string) ([]string, error) {
	defaultConfig := api.DefaultConfig()
	// We use default config because it can be fine configured through ENV variables
	// (such as auth), in most case we only need to change the address
	defaultConfig.Address = consulAddr
	client, err := api.NewClient(defaultConfig)
	if err != nil {
		return nil, err
	}

	// Get a handle to the KV API
	health := client.Health()
	entries, _, err := health.Service(service, "", true, &api.QueryOptions{Datacenter: dc})
	if err != nil {
		log.Println("Failed to get service from Consul:", err)
		return nil, err
	}
	return extractNodesAdresses(entries), nil
}

func extractNodesAdresses(entries []*api.ServiceEntry) (nodesAddr []string) {
	for i := range entries {
		nodesAddr = append(nodesAddr, fmt.Sprintf("%s:%d", entries[i].Service.Address, entries[i].Service.Port))
	}
	return nodesAddr
}
