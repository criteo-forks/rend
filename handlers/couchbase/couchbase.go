package couchbase

import (
	"errors"

	"github.com/netflix/rend/common"
	"github.com/netflix/rend/handlers"
	gocb "gopkg.in/couchbase/gocb.v1"
)

var (
	errNotFound = "key not found"
)

type Handler struct {
	client clientWrapper
}

type clientWrapper interface {
	set([]byte, []byte, uint32) error
	get([]byte, *[]byte) error
	close() error
}

type couchbaseClient struct {
	client *gocb.Bucket
}

const customRawBinaryTypecode = 0x4352544f

type CustomRawBinaryTranscoder struct {
}

func (t CustomRawBinaryTranscoder) Decode(bytes []byte, flags uint32, out interface{}) error {
	switch typedOut := out.(type) {
	case *[]byte:
		*typedOut = bytes
		return nil
	case *string:
		*typedOut = string(bytes)
		return nil
	default:
		return errors.New("Custom raw binary format must be encoded in a byte array or interface")
	}
}

func (t CustomRawBinaryTranscoder) Encode(value interface{}) ([]byte, uint32, error) {
	var bytes []byte

	switch typeValue := value.(type) {
	case []byte:
		bytes = typeValue
	case *[]byte:
		bytes = *typeValue
	case string:
		bytes = []byte(typeValue)
	case *string:
		bytes = []byte(*typeValue)
	default:
		return nil, customRawBinaryTypecode,
			errors.New("raw binary custom format must be encoded from a byte array or interface")
	}

	return bytes, customRawBinaryTypecode, nil
}

func (c *couchbaseClient) close() error {
	return c.client.Close()
}

func (c *couchbaseClient) get(key []byte, data *[]byte) error {
	// TODO: may have to check https://github.com/golang/go/issues/25484
	// if string conversion is a perf bottleneck
	_, err := c.client.Get(string(key), data)
	return err
}

func (c *couchbaseClient) set(key []byte, data []byte, exptime uint32) error {
	_, err := c.client.Upsert(string(key), data, exptime)
	return err
}

func NewHandler(clusterAddr string, bucketName string) (Handler, error) {
	cluster, err := gocb.Connect("http://" + clusterAddr)
	if err != nil {
		return Handler{}, err
	}

	bucket, err := cluster.OpenBucket(bucketName, "")
	if err != nil {
		return Handler{}, err
	}
	bucket.SetTranscoder(&CustomRawBinaryTranscoder{})

	client := couchbaseClient{client: bucket}

	return Handler{client: &client}, nil
}

func NewHandlerConst(clusterAddr string, bucketName string) handlers.HandlerConst {
	return func() (handlers.Handler, error) {
		return NewHandler(clusterAddr, bucketName)
	}
}

func (h Handler) Close() error {
	return h.client.close()
}

func (h Handler) Set(cmd common.SetRequest) error {
	return h.client.set(cmd.Key, cmd.Data, cmd.Exptime)
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
		errorOut <- errors.New("Received different number ofKeys, Opaques and Quiet")
		return
	}
	var data []byte
	for i, key := range cmd.Keys {
		err := h.client.get(key, &data)
		miss := false
		if err != nil {
			if err.Error() == errNotFound {
				miss = true
			} else {
				errorOut <- err
				break
			}
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
