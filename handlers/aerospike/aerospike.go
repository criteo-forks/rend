package aerospike

import (
	"errors"
	"strconv"
	"strings"

	aero "github.com/aerospike/aerospike-client-go"
	"github.com/netflix/rend/common"
	"github.com/netflix/rend/handlers"
)

var (
	errNotFound = "Key not found"
)

type Handler struct {
	client    *aero.Client
	namespace string
	set       string
}

func NewHandler(clusterAddr string, bucketName string) (Handler, error) {
	addr := strings.Split(clusterAddr, ":")
	port, err := strconv.Atoi(addr[1])
	if err != nil {
		return Handler{}, err
	}
	client, err := aero.NewClient(addr[0], port)
	if err != nil {
		return Handler{}, err
	}

	return Handler{client: client, namespace: bucketName, set: "rend"}, nil
}

func NewHandlerConst(clusterAddr string, bucketName string) handlers.HandlerConst {
	return func() (handlers.Handler, error) {
		return NewHandler(clusterAddr, bucketName)
	}
}

func (h Handler) Close() error {
	h.client.Close()
	return nil
}

func (h Handler) Set(cmd common.SetRequest) error {
	key, err := aero.NewKey(h.namespace, h.set, cmd.Key)
	if err != nil {
		return err
	}
	data := aero.BinMap{"value": cmd.Data}
	policy := aero.NewWritePolicy(0, cmd.Exptime)
	return h.client.Put(policy, key, data)
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

	if len(cmd.Opaques) != len(cmd.Keys) || len(cmd.Quiet) != len(cmd.Keys) {
		errorOut <- errors.New("Received different number of Keys, Opaques and Quiet")
		return
	}
	for i, key := range cmd.Keys {
		aeroKey, err := aero.NewKey(h.namespace, h.set, key)
		if err != nil {
			errorOut <- err
			break
		}
		record, err := h.client.Get(nil, aeroKey, "value")
		miss := false
		data := []byte{}
		if err != nil {
			if err.Error() == errNotFound {
				miss = true
			} else {
				errorOut <- err
				break
			}
		} else {
			// TODO: find a way to make it safer
			data = record.Bins["value"].([]byte)
		}
		dataOut <- common.GetResponse{Key: key, Data: data, Opaque: cmd.Opaques[i], Flags: 0, Miss: miss, Quiet: cmd.Quiet[i]}
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
