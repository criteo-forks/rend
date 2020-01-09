package orcas

import (
	"log"

	"github.com/netflix/rend/common"
	"github.com/netflix/rend/handlers"
	"github.com/netflix/rend/protocol"
)

type BackfillOrca struct {
	remoteCluster handlers.Handler
	localCluster  handlers.Handler
	res           protocol.Responder
}

func Backfill(l1, l2 handlers.Handler, res protocol.Responder) Orca {
	return &BackfillOrca{
		remoteCluster: l1,
		localCluster:  l2,
		res:           res,
	}
}

func (l *BackfillOrca) Set(req common.SetRequest) error {
	// We don't of SET, we just swallow them and return a success
	l.res.Set(req.Opaque, req.Quiet)
	return nil
}

func (l *BackfillOrca) Get(req common.GetRequest) error {
	// We return miss upon get in order to not block clients with remote DC call
	var emptyData []byte
	for ix, key := range req.Keys {
		l.res.Get(common.GetResponse{Key: key, Quiet: req.Quiet[ix], Opaque: req.Opaques[ix], Flags: 0, Data: emptyData, Miss: true})
	}
	l.res.GetEnd(req.NoopOpaque, req.NoopEnd)

	// And now we start async get to remote cluster in order to backfill the local one
	responseChan, errChan := l.remoteCluster.Get(req)
	func() {
		var opaque uint32
		for {
			select {
			case res, ok := <-responseChan:
				if !ok {
					responseChan = nil
				} else if !res.Miss {
					l.localCluster.Set(common.SetRequest{Key: res.Key, Data: res.Data, Exptime: 1500, Flags: 0, Opaque: opaque, Quiet: true})
				}

			case err, ok := <-errChan:
				if !ok {
					errChan = nil
				} else {
					log.Println("Error during get from source cluster:", err)
				}
			}
			if responseChan == nil && errChan == nil {
				break
			}
			opaque++
		}
	}()

	return nil
}

func (l *BackfillOrca) Add(req common.SetRequest) error                                 { return common.ErrNoError }
func (l *BackfillOrca) Replace(req common.SetRequest) error                             { return common.ErrNoError }
func (l *BackfillOrca) Append(req common.SetRequest) error                              { return common.ErrNoError }
func (l *BackfillOrca) Prepend(req common.SetRequest) error                             { return common.ErrNoError }
func (l *BackfillOrca) Delete(req common.DeleteRequest) error                           { return common.ErrNoError }
func (l *BackfillOrca) Touch(req common.TouchRequest) error                             { return common.ErrNoError }
func (l *BackfillOrca) GetE(req common.GetRequest) error                                { return common.ErrNoError }
func (l *BackfillOrca) Gat(req common.GATRequest) error                                 { return common.ErrNoError }
func (l *BackfillOrca) Noop(req common.NoopRequest) error                               { return common.ErrNoError }
func (l *BackfillOrca) Quit(req common.QuitRequest) error                               { return common.ErrNoError }
func (l *BackfillOrca) Version(req common.VersionRequest) error                         { return common.ErrNoError }
func (l *BackfillOrca) Unknown(req common.Request) error                                { return common.ErrNoError }
func (l *BackfillOrca) Error(req common.Request, reqType common.RequestType, err error) {}
