package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"sync"

	"github.com/netflix/rend/consul"
	"github.com/netflix/rend/handlers"
	"github.com/netflix/rend/handlers/memcached"
	"github.com/netflix/rend/metrics"
	"github.com/netflix/rend/orcas"
	"github.com/netflix/rend/protocol"
	"github.com/netflix/rend/protocol/binprot"
	"github.com/netflix/rend/server"
)

// Flags
var (
	srcClusterName string
	dstClusterName string
	srcHostnames   string
	dstHostnames   string
	srcClusterDC   string
	dstClusterDC   string
	consulAddr     string
	listenPort     int
	adminPort      int
)

func init() {
	flag.IntVar(&listenPort, "p", 11211, "External port to listen on")
	flag.IntVar(&adminPort, "admin-port", 8080, "Admin port for metrics and debug")
	flag.StringVar(&consulAddr, "consul-addr", "localhost:8500", "Consul addr for service resolution (set --hostnames to no use)")

	flag.StringVar(&srcHostnames, "source-hostnames", "", "List of instances of for the source cluster (override Consul service)")
	flag.StringVar(&srcClusterName, "source-cluster-name", "memcached-cluster", "The consul service name of the source cluster")
	flag.StringVar(&srcClusterDC, "source-datacenter", "", "The datacenter used for destination cluster (empty for local)")

	flag.StringVar(&dstHostnames, "destination-hostnames", "", "List of instances of for the destination cluster (override Consul service)")
	flag.StringVar(&dstClusterName, "destination-cluster-name", "memcached-cluster", "The consul service name of the destination cluster")
	flag.StringVar(&dstClusterDC, "destination-datacenter", "", "The datacenter used for destination cluster (empty for local)")

	flag.Parse()

	// Setting up signal handlers
	sigs := make(chan os.Signal)
	signal.Notify(sigs, os.Interrupt)

	go func() {
		<-sigs
		log.Println("Keyboard Interrupt")
		os.Exit(0)
	}()

	// http debug and metrics endpoint
	log.Printf("starting admin endpoint on port %d", adminPort)
	go http.ListenAndServe(fmt.Sprintf("localhost:%d", adminPort), nil)

	// metrics output prefix
	metrics.SetPrefix("rend_")
}

func newHandlerFromConfig(hostnames string, clusterName string, dc string) handlers.HandlerConst {
	var memcachedInstances []string

	if hostnames != "" {
		memcachedInstances = strings.Split(hostnames, ",")
	} else {
		var err error
		memcachedInstances, err = consul.GetNodes(clusterName, consulAddr, dc)
		if err != nil {
			log.Fatalf("Error: couldn't fetch service from Consul: %s", err)
		}
	}

	if len(memcachedInstances) <= 0 || len(memcachedInstances[0]) <= 0 {
		log.Fatalf("Error: Cannot create a cluster (cluster: %s) of 0 nodes", clusterName)
	}

	return memcached.Cluster(memcachedInstances, clusterName)
}

// And away we go
func main() {
	l := server.TCPListener(listenPort)
	protocols := []protocol.Components{binprot.Components}

	sourceCluster := newHandlerFromConfig(srcHostnames, srcClusterName, srcClusterDC)
	backfillCluster := newHandlerFromConfig(dstHostnames, dstClusterName, dstClusterDC)

	log.Printf("Starting Rend (backfill mode) on port %d", listenPort)
	go server.ListenAndServe(l, protocols, server.Default, orcas.Backfill, sourceCluster, backfillCluster)

	// Block forever
	wg := sync.WaitGroup{}
	wg.Add(1)
	wg.Wait()
}
