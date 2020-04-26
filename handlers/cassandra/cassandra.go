package cassandra

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gocql/gocql"
	"github.com/netflix/rend/common"
	"github.com/netflix/rend/metrics"
	"github.com/netflix/rend/timer"
	"github.com/spf13/viper"
)

type Handler struct {
	session       *gocql.Session
	setbuffer     chan CassandraSet
	keyspace      string
	bucket        string
	setStmt       string
	getStmt       string
	getEStmt      string
	deleteStmt    string
	replaceStmt   string
	isShutingDown uint32
	executors     sync.WaitGroup
}

type CassandraSet struct {
	Key     []byte
	Data    []byte
	Flags   uint32
	Exptime uint32
}

// Dirty https://github.com/gocql/gocql/issues/915
type SimpleConvictionPolicy struct{}

func (e *SimpleConvictionPolicy) Reset(host *gocql.HostInfo) {}
func (e *SimpleConvictionPolicy) AddFailure(error error, host *gocql.HostInfo) bool {
	return false
}

// Cassandra Batching metrics
var (
	HistSetBufferWait = metrics.AddHistogram("set_batch_buffer_timewait", false, nil)
)

// SetReadonlyMode switch Cassandra handler to readonly mode for graceful exit
func (h *Handler) Shutdown() {
	atomic.AddUint32(&h.isShutingDown, 1)
	close(h.setbuffer)
	h.executors.Wait()
}

func (h *Handler) IsShutingDown() bool {
	return atomic.LoadUint32(&h.isShutingDown) > 0
}

// We read from the channel from N executor/goroutine in order to propagate the writes
func (h *Handler) spawnSetExecutor() {
	h.executors.Add(1)

	for item := range h.setbuffer {
		query := h.session.Query(h.setStmt, item.Key, item.Data, item.Exptime)

		if err := query.Exec(); err != nil {
			log.Println("[ERROR] Cassandra SET returned an error. ", err)
		}
	}

	h.executors.Done()
}

func New() (*Handler, error) {
	cluster := gocql.NewCluster(viper.GetString("CassandraHostname"))
	cluster.Keyspace = viper.GetString("CassandraKeyspace")
	cluster.Consistency = gocql.LocalOne
	cluster.Timeout = viper.GetDuration("CassandraTimeoutMs")
	cluster.ConnectTimeout = viper.GetDuration("CassandraConnectTimeoutMs")
	cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())
	cluster.ConvictionPolicy = &SimpleConvictionPolicy{}
	cluster.NumConns = 10

	sess, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}

	cassandraHandler := &Handler{
		session:  sess,
		keyspace: viper.GetString("CassandraKeyspace"),
		bucket:   viper.GetString("CassandraBucket"),

		// Statements
		setbuffer:   make(chan CassandraSet, viper.GetInt("MemcachedMaxBufferedSetRequests")),
		setStmt:     fmt.Sprintf("INSERT INTO %s (key,value) VALUES (?, ?) USING TTL ?", viper.GetString("CassandraBucket")),
		getStmt:     fmt.Sprintf("SELECT key,value FROM %s WHERE key=? LIMIT 1", viper.GetString("CassandraBucket")),
		getEStmt:    fmt.Sprintf("SELECT key,value,TTL(value) FROM %s where key=?", viper.GetString("CassandraBucket")),
		deleteStmt:  fmt.Sprintf("DELETE FROM %s WHERE keycol=?", viper.GetString("CassandraBucket")),
		replaceStmt: fmt.Sprintf("SELECT writetime(value) FROM %s WHERE key=? LIMIT 1", viper.GetString("CassandraBucket")),

		// Synchronization
		isShutingDown: 0,
		executors:     sync.WaitGroup{},
	}

	// Spawn go-routines that will fan out memcached requests to Cassandra
	for i := 1; i <= viper.GetInt("CassandraNbConcurrentRequests"); i++ {
		go cassandraHandler.spawnSetExecutor()
	}

	return cassandraHandler, nil
}

func (h *Handler) Close() error {
	return nil
}

func computeExpTime(Exptime uint32) uint32 {
	// Maximum allowed relative TTL in memcached protocol
	maxTtl := uint32(60 * 60 * 24 * 30) // number of seconds in 30 days
	if Exptime > maxTtl {
		return Exptime - uint32(time.Now().Unix())
	}
	return Exptime
}

func (h *Handler) Set(cmd common.SetRequest) error {
	if h.IsShutingDown() {
		return common.ErrItemNotStored
	}

	start := timer.Now()
	h.setbuffer <- CassandraSet{
		Key:     cmd.Key,
		Data:    cmd.Data,
		Flags:   cmd.Flags,
		Exptime: computeExpTime(cmd.Exptime),
	}
	metrics.ObserveHist(HistSetBufferWait, timer.Since(start))

	//// TODO : maybe add a set timeout that return "not_stored" in case of buffer error ?
	return nil
}

func (h *Handler) Add(cmd common.SetRequest) error {
	return nil
}

func (h *Handler) Replace(cmd common.SetRequest) error {
	if h.IsShutingDown() {
		return common.ErrItemNotStored
	}

	/* TODO: better use "UPDATE ... IF EXISTS" pattern because it make use of
	"Lightweight transactions" and it's more consistent. */
	var wtime uint
	start := timer.Now()
	if err := h.session.Query(h.replaceStmt, cmd.Key).Scan(&wtime); err == nil {
		h.setbuffer <- CassandraSet{
			Key:     cmd.Key,
			Data:    cmd.Data,
			Flags:   cmd.Flags,
			Exptime: computeExpTime(cmd.Exptime),
		}
		metrics.ObserveHist(HistSetBufferWait, timer.Since(start))
		return nil
	} else {
		if err.Error() == "not found" {
			return common.ErrKeyNotFound
		} else {
			return common.ErrInternal
		}
	}
}

func (h *Handler) Append(cmd common.SetRequest) error {
	return nil
}

func (h *Handler) Prepend(cmd common.SetRequest) error {
	return nil
}

func (h *Handler) Get(cmd common.GetRequest) (<-chan common.GetResponse, <-chan error) {
	dataOut := make(chan common.GetResponse, len(cmd.Keys))
	errorOut := make(chan error)

	go func() {
		defer close(dataOut)
		defer close(errorOut)

		for idx, key := range cmd.Keys {
			var val []byte
			if err := h.session.Query(h.getStmt, key).Scan(&key, &val); err == nil {
				dataOut <- common.GetResponse{
					Miss:   false,
					Quiet:  cmd.Quiet[idx],
					Opaque: cmd.Opaques[idx],
					Flags:  0,
					Key:    key,
					Data:   val,
				}
			} else {
				dataOut <- common.GetResponse{
					Miss:   true,
					Quiet:  cmd.Quiet[idx],
					Opaque: cmd.Opaques[idx],
					Key:    key,
					Data:   nil,
				}
			}
		}
	}()

	return dataOut, errorOut
}

func (h *Handler) GetE(cmd common.GetRequest) (<-chan common.GetEResponse, <-chan error) {
	dataOut := make(chan common.GetEResponse, len(cmd.Keys))
	errorOut := make(chan error)

	go func() {
		defer close(dataOut)
		defer close(errorOut)

		for idx, key := range cmd.Keys {
			var val []byte
			var ttl uint32

			if err := h.session.Query(h.getEStmt, key).Scan(&key, &val, &ttl); err == nil {
				dataOut <- common.GetEResponse{
					Miss:    false,
					Quiet:   cmd.Quiet[idx],
					Opaque:  cmd.Opaques[idx],
					Flags:   0,
					Key:     key,
					Data:    val,
					Exptime: ttl,
				}
			} else {
				dataOut <- common.GetEResponse{
					Miss:   true,
					Quiet:  cmd.Quiet[idx],
					Opaque: cmd.Opaques[idx],
					Key:    key,
					Data:   nil,
				}
			}
		}
	}()

	return dataOut, errorOut
}

func (h *Handler) GAT(cmd common.GATRequest) (common.GetResponse, error) {
	return common.GetResponse{
		Miss:   true,
		Opaque: cmd.Opaque,
		Key:    cmd.Key,
	}, nil
}

func (h *Handler) Delete(cmd common.DeleteRequest) error {
	if err := h.session.Query(h.deleteStmt, cmd.Key).Exec(); err != nil {
		return err
	}
	return nil
}

func (h *Handler) Touch(cmd common.TouchRequest) error {
	return nil
}
