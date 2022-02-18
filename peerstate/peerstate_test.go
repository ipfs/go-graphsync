package peerstate_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/peerstate"
)

func TestDiagnostics(t *testing.T) {
	requestIDs := make([]graphsync.RequestID, 0, 5)
	for i := 0; i < 5; i++ {
		requestIDs = append(requestIDs, graphsync.NewRequestID())
	}
	testCases := map[string]struct {
		requestStates       graphsync.RequestStates
		queueStats          peerstate.TaskQueueState
		expectedDiagnostics map[graphsync.RequestID][]string
	}{
		"all requests and queue match": {
			requestStates: graphsync.RequestStates{
				requestIDs[0]: graphsync.Running,
				requestIDs[1]: graphsync.Running,
				requestIDs[2]: graphsync.Queued,
				requestIDs[3]: graphsync.Queued,
				requestIDs[4]: graphsync.Paused,
			},
			queueStats: peerstate.TaskQueueState{
				Active:  []graphsync.RequestID{requestIDs[0], requestIDs[1]},
				Pending: []graphsync.RequestID{requestIDs[2], requestIDs[3]},
			},
			expectedDiagnostics: map[graphsync.RequestID][]string{},
		},
		"active task with with incorrect state": {
			requestStates: graphsync.RequestStates{
				requestIDs[0]: graphsync.Running,
				requestIDs[1]: graphsync.Queued,
				requestIDs[2]: graphsync.Queued,
				requestIDs[3]: graphsync.Queued,
				requestIDs[4]: graphsync.Paused,
			},
			queueStats: peerstate.TaskQueueState{
				Active:  []graphsync.RequestID{requestIDs[0], requestIDs[1], requestIDs[4]},
				Pending: []graphsync.RequestID{requestIDs[2], requestIDs[3]},
			},
			expectedDiagnostics: map[graphsync.RequestID][]string{
				requestIDs[1]: {fmt.Sprintf("expected request with id %s in active task queue to be in running state, but was queued", requestIDs[1].String()), fmt.Sprintf("request with id %s in queued state is not in the pending task queue", requestIDs[1].String())},
				requestIDs[4]: {fmt.Sprintf("expected request with id %s in active task queue to be in running state, but was paused", requestIDs[4].String())},
			},
		},
		"active task with no state": {
			requestStates: graphsync.RequestStates{
				requestIDs[0]: graphsync.Running,
				requestIDs[2]: graphsync.Queued,
				requestIDs[3]: graphsync.Queued,
				requestIDs[4]: graphsync.Paused,
			},
			queueStats: peerstate.TaskQueueState{
				Active:  []graphsync.RequestID{requestIDs[0], requestIDs[1]},
				Pending: []graphsync.RequestID{requestIDs[2], requestIDs[3]},
			},
			expectedDiagnostics: map[graphsync.RequestID][]string{
				requestIDs[1]: {fmt.Sprintf("request with id %s in active task queue but appears to have no tracked state", requestIDs[1].String())},
			},
		},
		"pending task with with incorrect state": {
			requestStates: graphsync.RequestStates{
				requestIDs[0]: graphsync.Running,
				requestIDs[1]: graphsync.Running,
				requestIDs[2]: graphsync.Queued,
				requestIDs[3]: graphsync.Running,
				requestIDs[4]: graphsync.Paused,
			},
			queueStats: peerstate.TaskQueueState{
				Active:  []graphsync.RequestID{requestIDs[0], requestIDs[1]},
				Pending: []graphsync.RequestID{requestIDs[2], requestIDs[3], requestIDs[4]},
			},
			expectedDiagnostics: map[graphsync.RequestID][]string{
				requestIDs[3]: {fmt.Sprintf("expected request with id %s in pending task queue to be in queued state, but was running", requestIDs[3].String()), fmt.Sprintf("request with id %s in running state is not in the active task queue", requestIDs[3].String())},
				requestIDs[4]: {fmt.Sprintf("expected request with id %s in pending task queue to be in queued state, but was paused", requestIDs[4].String())},
			},
		},
		"pending task with no state": {
			requestStates: graphsync.RequestStates{
				requestIDs[0]: graphsync.Running,
				requestIDs[1]: graphsync.Running,
				requestIDs[2]: graphsync.Queued,
				requestIDs[4]: graphsync.Paused,
			},
			queueStats: peerstate.TaskQueueState{
				Active:  []graphsync.RequestID{requestIDs[0], requestIDs[1]},
				Pending: []graphsync.RequestID{requestIDs[2], requestIDs[3]},
			},
			expectedDiagnostics: map[graphsync.RequestID][]string{
				requestIDs[3]: {fmt.Sprintf("request with id %s in pending task queue but appears to have no tracked state", requestIDs[3].String())},
			},
		},
		"request state running with no active task": {
			requestStates: graphsync.RequestStates{
				requestIDs[0]: graphsync.Running,
				requestIDs[1]: graphsync.Running,
				requestIDs[2]: graphsync.Queued,
				requestIDs[3]: graphsync.Queued,
				requestIDs[4]: graphsync.Paused,
			},
			queueStats: peerstate.TaskQueueState{
				Active:  []graphsync.RequestID{requestIDs[0]},
				Pending: []graphsync.RequestID{requestIDs[2], requestIDs[3]},
			},
			expectedDiagnostics: map[graphsync.RequestID][]string{
				requestIDs[1]: {fmt.Sprintf("request with id %s in running state is not in the active task queue", requestIDs[1].String())},
			},
		},
		"request state queued with no pending task": {
			requestStates: graphsync.RequestStates{
				requestIDs[0]: graphsync.Running,
				requestIDs[1]: graphsync.Running,
				requestIDs[2]: graphsync.Queued,
				requestIDs[3]: graphsync.Queued,
				requestIDs[4]: graphsync.Paused,
			},
			queueStats: peerstate.TaskQueueState{
				Active:  []graphsync.RequestID{requestIDs[0], requestIDs[1]},
				Pending: []graphsync.RequestID{requestIDs[2]},
			},
			expectedDiagnostics: map[graphsync.RequestID][]string{
				requestIDs[3]: {fmt.Sprintf("request with id %s in queued state is not in the pending task queue", requestIDs[3].String())},
			},
		},
	}
	for testCase, data := range testCases {
		t.Run(testCase, func(t *testing.T) {
			require.Equal(t, data.expectedDiagnostics, peerstate.PeerState{data.requestStates, data.queueStats}.Diagnostics())
		})
	}
}
