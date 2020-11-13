package main

import (
	"flag"
	"fmt"
	"github.com/netflix/rend/consul"
	"github.com/netflix/rend/handlers/aerospike"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"syscall"
	"time"

	"github.com/netflix/rend/handlers"
	"github.com/netflix/rend/orcas"
	"github.com/netflix/rend/protocol"
	"github.com/netflix/rend/protocol/binprot"
	"github.com/netflix/rend/server"
	"github.com/spf13/viper"
)

func initAeroDefaultConfig() {
	log.Println("Initializing configuration")
	viper.SetDefault("ListenPort", 11221)
	viper.SetDefault("InternalMetricsListenAddress", ":11299")
	viper.SetDefault("ConsulAddr", "localhost:8500")
	viper.SetDefault("ClusterName", "aerospike-nvme-bench-3-nodes")
	viper.SetDefault("Bucket", "persisted")
	viper.SetDefault("NbConcurrentRequests", 500)
	viper.SetDefault("MemcachedMaxBufferedSetRequests", 80*1000)
	viper.SetDefault("GracefulShutdownWaitTimeInSec", 10*60)
}

func main() {
	if _, set := os.LookupEnv("GOGC"); !set {
		debug.SetGCPercent(100)
	}

	var configPath = flag.String("configFilePath", "", "File path where to find the config.yaml of the application")
	flag.Parse()

	initAeroDefaultConfig()
	if *configPath != "" {
		dir, fileName := filepath.Split(*configPath)
		viper.SetConfigName(fileName)
		viper.SetConfigType("yaml")
		viper.AddConfigPath(dir)
	} else {
		viper.SetConfigName("config")
		viper.SetConfigType("yaml")
		viper.AddConfigPath(".")
		viper.AddConfigPath("/etc/memaerospike/")
	}

	err := viper.ReadInConfig() // Find and read the config file
	if err != nil {             // Handle errors reading the config file
		panic(fmt.Errorf("Fatal error config file: %s \n", err))
	}

	// http debug and metrics endpoint
	go http.ListenAndServe(viper.GetString("InternalMetricsListenAddress"), nil)

	var h1 handlers.HandlerConst
	var h2 handlers.HandlerConst

	// get nodes from consul
	consulAddr := viper.GetString("ConsulAddr")
	clusterName := viper.GetString("ClusterName")
	instances, err := consul.GetNodes(clusterName, consulAddr, "")
	log.Printf("Discovered for %s: %s", clusterName, instances)
	if err != nil {
		log.Fatalf("Error: couldn't fetch service from Consul: %s", err)
	}

	// Init aerospike connection
	aerospikeHandler, err := aerospike.NewHandler(instances[0], viper.GetString("Bucket"))
	if err != nil {
		log.Fatal(err)
	}

	// L1Only MODE
	h1 = func() (handlers.Handler, error) {
		return aerospikeHandler, nil
	}
	h2 = handlers.NilHandler

	l := server.TCPListener(viper.GetInt("ListenPort"))
	protocols := []protocol.Components{binprot.Components}

	// Graceful stop
	var gracefulStop = make(chan os.Signal)
	signal.Notify(gracefulStop, syscall.SIGTERM)
	signal.Notify(gracefulStop, syscall.SIGKILL)
	signal.Notify(gracefulStop, syscall.SIGINT)
	go func() {
		_ = <-gracefulStop
		log.Println("[INFO] Gracefully stopping Memandra server")
		go func() {
			waitTime := viper.GetDuration("GracefulShutdownWaitTimeInSec") * time.Second
			log.Println("[INFO] Waiting", waitTime, "for aerospike executors to shutdown")
			time.Sleep(waitTime)
			log.Println("[INFO] Forcing exit as", waitTime, "passed")
			os.Exit(0)
		}()

		aerospikeHandler.Shutdown()
		os.Exit(0)
	}()

	server.ListenAndServe(l, protocols, server.Default, orcas.L1Only, h1, h2)
}
