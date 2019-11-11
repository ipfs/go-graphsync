package loadattemptqueue

import (
	"errors"

	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/requestmanager/types"
	"github.com/ipld/go-ipld-prime"
)

// LoadRequest is a request to load the given link for the given request id,
// with results returned to the given channel
type LoadRequest struct {
	requestID  graphsync.RequestID
	link       ipld.Link
	resultChan chan types.AsyncLoadResult
}

// NewLoadRequest returns a new LoadRequest for the given request id, link,
// and results channel
func NewLoadRequest(requestID graphsync.RequestID,
	link ipld.Link,
	resultChan chan types.AsyncLoadResult) LoadRequest {
	return LoadRequest{requestID, link, resultChan}
}

// LoadAttempter attempts to load a link to an array of bytes
// it has three results:
// bytes present, error nil = success
// bytes nil, error present = error
// bytes nil, error nil = did not load, but try again later
type LoadAttempter func(graphsync.RequestID, ipld.Link) ([]byte, error)

// LoadAttemptQueue attempts to load using the load attempter, and then can
// place requests on a retry queue
type LoadAttemptQueue struct {
	loadAttempter  LoadAttempter
	pausedRequests []LoadRequest
}

// New initializes a new AsyncLoader from loadAttempter function
func New(loadAttempter LoadAttempter) *LoadAttemptQueue {
	return &LoadAttemptQueue{
		loadAttempter: loadAttempter,
	}
}

// AttemptLoad attempts to loads the given load request, and if retry is true
// it saves the loadrequest for retrying later
func (laq *LoadAttemptQueue) AttemptLoad(lr LoadRequest, retry bool) {
	response, err := laq.loadAttempter(lr.requestID, lr.link)
	if err != nil {
		lr.resultChan <- types.AsyncLoadResult{Data: nil, Err: err}
		close(lr.resultChan)
		return
	}
	if response != nil {
		lr.resultChan <- types.AsyncLoadResult{Data: response, Err: nil}
		close(lr.resultChan)
		return
	}
	if !retry {
		laq.terminateWithError("No active request", lr.resultChan)
		return
	}
	laq.pausedRequests = append(laq.pausedRequests, lr)
}

// ClearRequest purges the given request from the queue of load requests
// to retry
func (laq *LoadAttemptQueue) ClearRequest(requestID graphsync.RequestID) {
	pausedRequests := laq.pausedRequests
	laq.pausedRequests = nil
	for _, lr := range pausedRequests {
		if lr.requestID == requestID {
			laq.terminateWithError("No active request", lr.resultChan)
		} else {
			laq.pausedRequests = append(laq.pausedRequests, lr)
		}
	}
}

// RetryLoads attempts loads on all saved load requests that were loaded with
// retry = true
func (laq *LoadAttemptQueue) RetryLoads() {
	// drain buffered
	pausedRequests := laq.pausedRequests
	laq.pausedRequests = nil
	for _, lr := range pausedRequests {
		laq.AttemptLoad(lr, true)
	}
}

func (laq *LoadAttemptQueue) terminateWithError(errMsg string, resultChan chan<- types.AsyncLoadResult) {
	resultChan <- types.AsyncLoadResult{Data: nil, Err: errors.New(errMsg)}
	close(resultChan)
}
