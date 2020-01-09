package cluster

import (
	"github.com/netflix/rend/common"
	"github.com/netflix/rend/handlers/memcached/std"
	"github.com/netflix/rend/protocol/binprot"
	"net"
)

type Node struct {
	handler std.Handler
	conn    net.Conn
}

func (n Node) Label() string {
	return n.conn.RemoteAddr().String()
}

func (n Node) Weight() uint32 {
	return 1
}

type Handler struct {
	nodes     []Node
	Continuum *Continuum
	name      string
}

func emptyClusterHandler() Handler {
	return Handler{[]Node{}, nil, "EmptyCluster"}
}

func NewHandler(nodesAddresses []string, clusterName string) (Handler, error) {
	nodes := make([]Node, len(nodesAddresses))
	buckets := make([]Bucket, len(nodesAddresses))

	for ix, nodeAddr := range nodesAddresses {
		conn, err := net.Dial("tcp", nodeAddr)
		if err != nil {
			if conn != nil {
				conn.Close()
			}
			return emptyClusterHandler(), err
		}
		nodes[ix] = Node{std.NewHandler(conn), conn}
		buckets[ix] = nodes[ix]
	}
	return Handler{nodes, New(buckets), clusterName}, nil
}

func (h Handler) Close() error {
	var err error

	for _, node := range h.nodes {
		ret := node.handler.Close()
		if ret != nil {
			err = ret
		}
	}
	return err
}

func (h Handler) Set(cmd common.SetRequest) error {
	return h.Continuum.Hash(cmd.Key).(Node).handler.Set(cmd)
}

func (h Handler) Get(cmd common.GetRequest) (<-chan common.GetResponse, <-chan error) {
	dataOut := make(chan common.GetResponse)
	errorOut := make(chan error)
	go h.realHandleGet(cmd, dataOut, errorOut)

	return dataOut, errorOut
}

func (h *Handler) realHandleGet(cmd common.GetRequest, dataOut chan common.GetResponse, errorOut chan error) {
	defer close(errorOut)
	defer close(dataOut)

	for idx, key := range cmd.Keys {
		handle := h.Continuum.Hash(key).(Node).handler
		if err := binprot.WriteGetCmd(handle.Rw.Writer, key, 0); err != nil {
			errorOut <- err
			return
		}

		data, flags, _, err := std.GetLocal(handle.Rw, false)
		if err != nil {
			if err == common.ErrKeyNotFound {
				dataOut <- common.GetResponse{
					Miss:   true,
					Quiet:  cmd.Quiet[idx],
					Opaque: cmd.Opaques[idx],
					Flags:  flags,
					Key:    key,
					Data:   nil,
				}

				continue
			}

			errorOut <- err
			return
		}

		dataOut <- common.GetResponse{
			Miss:   false,
			Quiet:  cmd.Quiet[idx],
			Opaque: cmd.Opaques[idx],
			Flags:  flags,
			Key:    key,
			Data:   data,
		}
	}
}

func (h Handler) GetE(cmd common.GetRequest) (<-chan common.GetEResponse, <-chan error) {
	return nil, nil
}
func (h Handler) GAT(cmd common.GATRequest) (common.GetResponse, error) {
	return common.GetResponse{}, nil
}
func (h Handler) Delete(cmd common.DeleteRequest) error {
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
