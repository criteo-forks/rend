package consul

import (
	"reflect"
	"testing"

	"github.com/hashicorp/consul/api"
)

func TestExtractNodesAddressesCorrectlyExtractData(t *testing.T) {
	service1 := api.AgentService{Address: "foo", Port: 80}
	service2 := api.AgentService{Address: "bar", Port: 80}
	entries := []*api.ServiceEntry{&api.ServiceEntry{Service: &service1}, &api.ServiceEntry{Service: &service2}}
	nodes := extractNodesAddresses(entries)
	expectedNodes := []string{"foo:80", "bar:80"}
	if !reflect.DeepEqual(expectedNodes, nodes) {
		t.Error("extractNodesAddresses doesn't extract properly")
	}
}

func TestGetNodesFromConsulPropelyHandleWrongConsulAddr(t *testing.T) {
	_, err := GetNodes("barservice", "foo://foohost:8500", "")
	if err == nil {
		t.Error("Errors when creating the api client are not properly reported")
	}
}

func TestGetNodesFromConsulProperlyFailIfConsulIsUnreachable(t *testing.T) {
	_, err := GetNodes("barservice", "foohost:8500", "")
	if err == nil {
		t.Error("Errors when querying are not properly reported")
	}
}

func TestGetServiceFromConsulProperlyReturnAddresses(t *testing.T) {
	service1 := api.AgentService{Address: "foo", Port: 80}
	service2 := api.AgentService{Address: "bar", Port: 80}
	entries := []*api.ServiceEntry{&api.ServiceEntry{Service: &service1}, &api.ServiceEntry{Service: &service2}}

	fakeGetService := func(service string, tag string, passingOnly bool, q *api.QueryOptions) ([]*api.ServiceEntry, *api.QueryMeta, error) {
		return entries, nil, nil
	}
	nodes, err := getServiceFromConsul(fakeGetService, "", "")
	expectedNodes := []string{"foo:80", "bar:80"}
	if (!reflect.DeepEqual(expectedNodes, nodes)) || (err != nil) {
		t.Error("extractNodesAddresses doesn't extract properly")
	}
}
