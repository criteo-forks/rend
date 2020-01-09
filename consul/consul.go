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

	health := client.Health()

	return getServiceFromConsul(health.Service, service, dc)
}

func getServiceFromConsul(getService func(string, string, bool, *api.QueryOptions) ([]*api.ServiceEntry, *api.QueryMeta, error), service string, dc string) ([]string, error) {
	entries, _, err := getService(service, "", true, &api.QueryOptions{Datacenter: dc})
	if err != nil {
		log.Println("Failed to get service from Consul:", err)
		return nil, err
	}
	return extractNodesAddresses(entries), nil
}

func extractNodesAddresses(entries []*api.ServiceEntry) (nodesAddr []string) {
	for i := range entries {
		nodesAddr = append(nodesAddr, fmt.Sprintf("%s:%d", entries[i].Service.Address, entries[i].Service.Port))
	}
	return nodesAddr
}
