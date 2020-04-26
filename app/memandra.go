package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"
	"time"

	"github.com/netflix/rend/handlers"
	"github.com/netflix/rend/handlers/cassandra"
	"github.com/netflix/rend/orcas"
	"github.com/netflix/rend/protocol"
	"github.com/netflix/rend/protocol/binprot"
	"github.com/netflix/rend/protocol/textprot"
	"github.com/netflix/rend/server"
	"github.com/spf13/viper"
)

func initDefaultConfig() {
	log.Println("Initializing configuration")
	viper.SetDefault("ListenPort", 11221)
	viper.SetDefault("InternalMetricsListenAddress", ":11299")
	viper.SetDefault("CassandraHostname", "127.0.0.1")
	viper.SetDefault("CassandraKeyspace", "kvstore")
	viper.SetDefault("CassandraBucket", "bucket")
	viper.SetDefault("CassandraTimeoutMs", 10000*time.Millisecond)
	viper.SetDefault("CassandraConnectTimeoutMs", 1000*time.Millisecond)
	viper.SetDefault("CassandraNbConcurrentRequests", 500)
	viper.SetDefault("MemcachedMaxBufferedSetRequests", 80*1000)
	viper.SetDefault("GracefulShutdownWaitTimeInSec", 10*60)
}

func main() {
	if _, set := os.LookupEnv("GOGC"); !set {
		debug.SetGCPercent(100)
	}

	initDefaultConfig()
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")
	viper.AddConfigPath("/etc/memandra/")

	err := viper.ReadInConfig() // Find and read the config file
	if err != nil {             // Handle errors reading the config file
		panic(fmt.Errorf("Fatal error config file: %s \n", err))
	}

	// http debug and metrics endpoint
	go http.ListenAndServe(viper.GetString("InternalMetricsListenAddress"), nil)

	// metrics output prefix
	// metrics.SetPrefix("memandra_")

	var h1 handlers.HandlerConst
	var h2 handlers.HandlerConst

	// Init Cassandra connection
	cassandraHandler, err := cassandra.New()
	if err != nil {
		log.Fatal(err)
	}

	// L1Only MODE
	h1 = func() (handlers.Handler, error) {
		return cassandraHandler, nil
	}
	h2 = handlers.NilHandler

	l := server.TCPListener(viper.GetInt("ListenPort"))
	ps := []protocol.Components{binprot.Components, textprot.Components}

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
			log.Println("[INFO] Waiting", waitTime, "for Cassandra executors to shutdown")
			time.Sleep(waitTime)
			log.Println("[INFO] Forcing exit as", waitTime, "passed")
			os.Exit(0)
		}()

		cassandraHandler.Shutdown()
		os.Exit(0)
	}()

	server.ListenAndServe(l, ps, server.Default, orcas.L1OnlyCassandra, h1, h2)
}
