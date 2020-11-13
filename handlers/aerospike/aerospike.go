package aerospike

import (
	"errors"
	"github.com/netflix/rend/metrics"
	"github.com/netflix/rend/timer"
	"github.com/spf13/viper"
	"log"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	aero "github.com/aerospike/aerospike-client-go"
	"github.com/netflix/rend/common"
	"github.com/netflix/rend/handlers"
)

var (
	errNotFound = "Key not found"
)

// TODO: implement Replace

type Handler struct {
	client        *aero.Client
	namespace     string
	set           string
	setbuffer     chan AerospikeGetSet
	executors     sync.WaitGroup
	isShutingDown uint32
}

type AerospikeGetSet struct {
	Get      bool
	cmdGet   *common.GetRequest
	cmdSet   *common.SetRequest
	dataOut  chan common.GetResponse
	errorOut chan error
}

// Aerospike metrics
var (
	HistSetEnqueueLatencies = metrics.AddHistogram("set_enqueue_latencies", false, nil)
	HistGetEnqueueLatencies = metrics.AddHistogram("get_enqueue_latencies", false, nil)
	HistSetLatencies        = metrics.AddHistogram("set_aerospike_latencies", false, nil)
	HistGetLatencies        = metrics.AddHistogram("get_aerospike_latencies", false, nil)
	HistReplaceLatencies    = metrics.AddHistogram("replace_aerospike_latencies", false, nil)
	HistDeleteLatencies     = metrics.AddHistogram("delete_aerospike_latencies", false, nil)
)

func NewHandler(clusterAddr string, bucketName string) (Handler, error) {
	addr := strings.Split(clusterAddr, ":")
	port, err := strconv.Atoi(addr[1])
	if err != nil {
		return Handler{}, err
	}
	clientPolicy := aero.NewClientPolicy()
	clientPolicy.ConnectionQueueSize = 1024
	client, err := aero.NewClientWithPolicy(clientPolicy, addr[0], port)
	//client, err := aero.NewClient(addr[0], port)
	if err != nil {
		return Handler{}, err
	}

	aerospikeHandler := Handler{
		client:    client,
		namespace: bucketName,
		set:       "rend",

		// Statements
		setbuffer: make(chan AerospikeGetSet, viper.GetInt("MemcachedMaxBufferedSetRequests")),

		// Synchronization
		isShutingDown: 0,
		executors:     sync.WaitGroup{},
	}

	// Spawn go-routines that will fan out memcached requests to Aerospike
	for i := 1; i <= viper.GetInt("NbConcurrentRequests"); i++ {
		go aerospikeHandler.spawnExecutor()
	}

	return aerospikeHandler, nil
}

func NewHandlerConst(clusterAddr string, bucketName string) handlers.HandlerConst {
	return func() (handlers.Handler, error) {
		return NewHandler(clusterAddr, bucketName)
	}
}

// We read from the channel from N executor/goroutine in order to propagate the writes
func (h Handler) spawnExecutor() {
	h.executors.Add(1)

	for item := range h.setbuffer {
		if item.Get {
			h.HandleGet(item)
		} else {
			h.HandleSet(item)
		}
	}

	h.executors.Done()
}

func (h Handler) HandleSet(item AerospikeGetSet) {
	start := timer.Now()
	key, err := aero.NewKey(h.namespace, h.set, item.cmdSet.Key)
	if err != nil {
		log.Println("[ERROR] NewKey get creation failed", err)
		return
	}

	bin := aero.NewBin("value", item.cmdSet.Data)
	err = h.client.PutBins(aero.NewWritePolicy(0, item.cmdSet.Exptime), key, bin)
	if err != nil {
		log.Println("[ERROR] Aerospike put returned an error. ", item.cmdSet.Key, err)
	}
	metrics.ObserveHist(HistSetLatencies, timer.Since(start))
}

func (h Handler) HandleGet(item AerospikeGetSet) {
	defer close(item.errorOut)
	defer close(item.dataOut)

	start := timer.Now()
	if len(item.cmdGet.Opaques) != len(item.cmdGet.Keys) || len(item.cmdGet.Quiet) != len(item.cmdGet.Keys) {
		item.errorOut <- errors.New("Received different number of Keys, Opaques and Quiet")
		return
	}
	for i, key := range item.cmdGet.Keys {
		miss := false
		aeroKey, err := aero.NewKey(h.namespace, h.set, key)
		if err != nil {
			miss = true
			log.Println("[ERROR] NewKey get creation failed", err)
			item.errorOut <- err
			break
		}
		record, err := h.client.Get(nil, aeroKey, "value")
		data := []byte{}
		if err != nil {
			if err.Error() == errNotFound {
				miss = true
			} else {
				miss = true
				log.Println("[ERROR] Aerospike get returned an error. ", err)
				item.errorOut <- err
				break
			}
		} else {
			rawData, ok := record.Bins["value"]
			if !ok {
				miss = true
				log.Println("[ERROR] Bin retrieved from Aerospike was not found")
				item.errorOut <- errors.New("Bin retrieved from Aerospike was not found")
				break
			}
			data, ok = rawData.([]byte)
			if !ok {
				miss = true
				log.Println("[ERROR] Data retrieved from AeroSpike was corrupted")
				item.errorOut <- errors.New("Data retrieved from AeroSpike was corrupted")
				break
			}
		}
		item.dataOut <- common.GetResponse{Key: key, Data: data, Opaque: item.cmdGet.Opaques[i], Flags: 0, Miss: miss, Quiet: item.cmdGet.Quiet[i]}
		metrics.ObserveHist(HistGetLatencies, timer.Since(start))
	}
}

func (h Handler) Close() error {
	return nil
}

func (h Handler) Shutdown() error {
	atomic.AddUint32(&h.isShutingDown, 1)
	h.client.Close()
	return nil
}

func (h *Handler) IsShutingDown() bool {
	return atomic.LoadUint32(&h.isShutingDown) > 0
}

func (h Handler) Set(cmd common.SetRequest) error {
	if h.IsShutingDown() {
		return common.ErrItemNotStored
	}

	start := timer.Now()
	h.setbuffer <- AerospikeGetSet{
		Get:    false,
		cmdSet: &cmd,
	}
	metrics.ObserveHist(HistSetEnqueueLatencies, timer.Since(start))

	return nil
}

func (h Handler) Get(cmd common.GetRequest) (<-chan common.GetResponse, <-chan error) {
	dataOut := make(chan common.GetResponse)
	errorOut := make(chan error)

	if h.IsShutingDown() {
		errorOut <- common.ErrItemNotStored
	} else {
		start := timer.Now()
		h.setbuffer <- AerospikeGetSet{
			Get:      true,
			cmdGet:   &cmd,
			dataOut:  dataOut,
			errorOut: errorOut,
		}
		metrics.ObserveHist(HistGetEnqueueLatencies, timer.Since(start))
	}

	return dataOut, errorOut
}

func (h Handler) GetE(cmd common.GetRequest) (<-chan common.GetEResponse, <-chan error) {
	return nil, nil
}
func (h Handler) GAT(cmd common.GATRequest) (common.GetResponse, error) {
	return common.GetResponse{
		Miss:   true,
		Opaque: cmd.Opaque,
		Key:    cmd.Key,
	}, nil
}

func (h Handler) Delete(cmd common.DeleteRequest) error {
	start := timer.Now()
	aeroKey, err := aero.NewKey(h.namespace, h.set, cmd.Key)
	if err != nil {
		log.Println("[ERROR] NewKey get creation failed", err)
		return err
	}
	_, err = h.client.Delete(nil, aeroKey)
	metrics.ObserveHist(HistDeleteLatencies, timer.Since(start))
	if err != nil {
		return err
	}
	return nil
}

func (h Handler) Touch(cmd common.TouchRequest) error {
	return nil
}
func (h Handler) Add(cmd common.SetRequest) error {
	return nil
}
func (h Handler) Replace(cmd common.SetRequest) error {
	return nil
}
func (h Handler) Append(cmd common.SetRequest) error {
	return nil
}
func (h Handler) Prepend(cmd common.SetRequest) error {
	return nil
}
