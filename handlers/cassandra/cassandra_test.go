package cassandra

import (
	"fmt"
	"github.com/spf13/viper"
	"sync"
	"testing"
)

func NewTestHandler(t *testing.T) *Handler {
	cassandraHandler := &Handler{
		session:  nil,
		keyspace: viper.GetString("CassandraKeyspace"),
		bucket:   viper.GetString("CassandraBucket"),

		// Statements
		setbuffer:   make(chan CassandraSet, 1000),
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
	for i := 1; i <= 2; i++ {
		go cassandraHandler.spawnSetExecutor()
	}

	return cassandraHandler
}

func TestCassandraShutdown(t *testing.T) {
	cassHandler := NewTestHandler(t)
	cassHandler.Shutdown()
}

func TestExpTime(t *testing.T) {

	ttl := computeExpTime(60 * 60)
	if ttl != 60*60 {
		t.Errorf("ttl <= 30 days should be expressed as a ttl time. got %d", ttl)
	}

	ttl = computeExpTime(60 * 60 * 24 * 30)
	if ttl != 60*60*24*30 {
		t.Errorf("ttl <= 30 days should be expressed as a ttl time. got %d", ttl)
	}

	ttl = computeExpTime(60*60*24*30 + 1)
	if ttl == 60*60*24*30+1 {
		t.Errorf("ttl above 30 days should be expressed as an Unix timestamp. got %d", ttl)
	}
}
