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
	"github.com/netflix/rend/handlers/couchbase"
	"github.com/netflix/rend/handlers/memcached"
	"github.com/netflix/rend/metrics"
	"github.com/netflix/rend/orcas"
	"github.com/netflix/rend/protocol"
	"github.com/netflix/rend/protocol/binprot"
	"github.com/netflix/rend/server"
)

// Flags
var (
	srcType        string
	dstType        string
	srcClusterName string
	dstClusterName string
	srcHostnames   string
	dstHostnames   string
	srcClusterDC   string
	dstClusterDC   string
	srcBucketName  string
	dstBucketName  string
	consulAddr     string
	listenPort     int
	adminPort      int
)

func init() {
	flag.IntVar(&listenPort, "p", 11211, "External port to listen on")
	flag.IntVar(&adminPort, "admin-port", 8080, "Admin port for metrics and debug")
	flag.StringVar(&consulAddr, "consul-addr", "localhost:8500", "Consul addr for service resolution (set --hostnames to no use)")

	// TODO: make a real configuration file
	flag.StringVar(&srcType, "source-cluster-type", "memcached", "(memcached or couchbase) type of cluster to configure")
	flag.StringVar(&srcHostnames, "source-hostnames", "", "List of instances of for the source cluster (override Consul service)")
	flag.StringVar(&srcClusterName, "source-cluster-name", "memcached-cluster", "The consul service name of the source cluster")
	flag.StringVar(&srcClusterDC, "source-datacenter", "", "The datacenter used for source cluster (empty for local)")
	flag.StringVar(&srcBucketName, "source-bucket", "", "The bucket to use for source couchbase cluster configuration")

	flag.StringVar(&dstType, "destination-cluster-type", "memcached", "(memcached or couchbase) type of cluster to configure")
	flag.StringVar(&dstHostnames, "destination-hostnames", "", "List of instances of for the destination cluster (override Consul service)")
	flag.StringVar(&dstClusterName, "destination-cluster-name", "memcached-cluster", "The consul service name of the destination cluster")
	flag.StringVar(&dstClusterDC, "destination-datacenter", "", "The datacenter used for destination cluster (empty for local)")
	flag.StringVar(&dstBucketName, "destination-bucket", "", "The bucket to use for destination couchbase cluster configuration")

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

func newHandlerFromConfig(clusterType string, hostnames string, clusterName string, dc string, bucket string) handlers.HandlerConst {
	var instances []string

	if hostnames != "" {
		instances = strings.Split(hostnames, ",")
	} else {
		var err error
		instances, err = consul.GetNodes(clusterName, consulAddr, dc)
		if err != nil {
			log.Fatalf("Error: couldn't fetch service from Consul: %s", err)
		}
	}

	if len(instances) <= 0 || len(instances[0]) <= 0 {
		log.Fatalf("Error: Cannot create a cluster (cluster: %s) of 0 nodes", clusterName)
	}

	switch clusterType {
	case "memcached":
		return memcached.Cluster(instances, clusterName)
	case "couchbase":
		return couchbase.NewHandlerConst(instances[0], bucket)
	default:
		log.Fatalf("Cluster type unsupported: %s", clusterType)
	}
	return nil
}

// And away we go
func main() {
	l := server.TCPListener(listenPort)
	protocols := []protocol.Components{binprot.Components}

	sourceCluster := newHandlerFromConfig(srcType, srcHostnames, srcClusterName, srcClusterDC, srcBucketName)
	backfillCluster := newHandlerFromConfig(dstType, dstHostnames, dstClusterName, dstClusterDC, dstBucketName)

	log.Printf("Starting Rend (backfill mode) on port %d", listenPort)
	go server.ListenAndServe(l, protocols, server.Default, orcas.Backfill, sourceCluster, backfillCluster)

	// Block forever
	wg := sync.WaitGroup{}
	wg.Add(1)
	wg.Wait()
}
