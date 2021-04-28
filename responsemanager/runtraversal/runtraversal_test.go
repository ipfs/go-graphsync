package runtraversal

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	ipld "github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/traversal"
	"github.com/stretchr/testify/require"

	"github.com/ipfs/go-graphsync/testutil"
)

type fakeResponseKey struct {
	Link ipld.Link
	Data string
}

type fakeResponseSender struct {
	stubbedResponses  map[fakeResponseKey]error
	expectedResponses map[fakeResponseKey]struct{}
	receivedResponses map[fakeResponseKey]struct{}
}

func newFakeResponseSender() *fakeResponseSender {
	return &fakeResponseSender{
		stubbedResponses:  make(map[fakeResponseKey]error),
		expectedResponses: make(map[fakeResponseKey]struct{}),
		receivedResponses: make(map[fakeResponseKey]struct{}),
	}
}

func (frs *fakeResponseSender) SendResponse(
	link ipld.Link,
	data []byte,
) error {
	frs.receivedResponses[fakeResponseKey{link, string(data)}] = struct{}{}
	return frs.stubbedResponses[fakeResponseKey{link, string(data)}]
}

func (frs *fakeResponseSender) expectResponse(link ipld.Link, data []byte, returnVal error) {
	frs.expectedResponses[fakeResponseKey{link, string(data)}] = struct{}{}
	frs.stubbedResponses[fakeResponseKey{link, string(data)}] = returnVal
}

func (frs *fakeResponseSender) verifyExpectations(t *testing.T) {
	require.Equal(t, frs.expectedResponses, frs.receivedResponses)
}

type loadedLink struct {
	link    ipld.Link
	linkCtx ipld.LinkContext
}

type traverseOutcome struct {
	isError bool
	err     error
	data    []byte
}
type fakeTraverser struct {
	finalError       error
	currentLink      int
	loadedLinks      []loadedLink
	receivedOutcomes []traverseOutcome
	expectedOutcomes []traverseOutcome
}

func (ft *fakeTraverser) NBlocksTraversed() int {
	return 0
}

// IsComplete returns the completion state (boolean) and if so, the final error result from IPLD
func (ft *fakeTraverser) IsComplete() (bool, error) {
	if ft.currentLink >= len(ft.loadedLinks) {
		return true, ft.finalError
	}
	return false, nil
}

// Current request returns the current link waiting to be loaded
func (ft *fakeTraverser) CurrentRequest() (ipld.Link, ipld.LinkContext) {
	ll := ft.loadedLinks[ft.currentLink]
	ft.currentLink++
	return ll.link, ll.linkCtx
}

// Advance advances the traversal successfully by supplying the given reader as the result of the next IPLD load
func (ft *fakeTraverser) Advance(reader io.Reader) error {
	buf := new(bytes.Buffer)
	_, _ = io.Copy(buf, reader)
	ft.receivedOutcomes = append(ft.receivedOutcomes, traverseOutcome{false, nil, buf.Bytes()})
	return nil
}

// Error errors the traversal by returning the given error as the result of the next IPLD load
func (ft *fakeTraverser) Error(err error) {
	ft.receivedOutcomes = append(ft.receivedOutcomes, traverseOutcome{true, err, nil})
}

// Shutdown cancels the traversal if still in progress
func (ft *fakeTraverser) Shutdown(ctx context.Context) {}

func (ft *fakeTraverser) verifyExpectations(t *testing.T) {
	require.Equal(t, ft.expectedOutcomes, ft.receivedOutcomes)
}

type fakeLoader struct {
	expectedLoads []loadedLink
	receivedLoads []loadedLink
	loadReturns   []traverseOutcome
	currentLoader int
}

func (fl *fakeLoader) Load(link ipld.Link, linkCtx ipld.LinkContext) (io.Reader, error) {
	fl.receivedLoads = append(fl.receivedLoads, loadedLink{link, linkCtx})
	outcome := fl.loadReturns[fl.currentLoader]
	fl.currentLoader++
	if outcome.isError {
		return nil, outcome.err
	}
	return bytes.NewBuffer(outcome.data), nil
}

func (fl *fakeLoader) verifyExpectations(t *testing.T) {
	require.Equal(t, fl.expectedLoads, fl.receivedLoads)
}

func TestRunTraversal(t *testing.T) {
	blks := testutil.GenerateBlocksOfSize(5, 100)
	links := make([]loadedLink, 0, 5)
	for _, blk := range blks {
		links = append(links, loadedLink{link: cidlink.Link{Cid: blk.Cid()}, linkCtx: ipld.LinkContext{}})
	}
	testCases := map[string]struct {
		linksToLoad          []loadedLink
		linkLoadsExpected    int
		loadOutcomes         []traverseOutcome
		loadOutcomesExpected int
		errorsOnSend         []error
		finalError           error
		expectedError        error
	}{
		"normal operation": {
			linksToLoad:       links,
			linkLoadsExpected: 5,
			loadOutcomes: []traverseOutcome{
				{false, nil, blks[0].RawData()},
				{false, nil, blks[1].RawData()},
				{false, nil, blks[2].RawData()},
				{false, nil, blks[3].RawData()},
				{false, nil, blks[4].RawData()},
			},
			errorsOnSend: []error{
				nil, nil, nil, nil, nil,
			},
		},
		"error on complete": {
			linksToLoad:       links,
			linkLoadsExpected: 5,
			loadOutcomes: []traverseOutcome{
				{false, nil, blks[0].RawData()},
				{false, nil, blks[1].RawData()},
				{false, nil, blks[2].RawData()},
				{false, nil, blks[3].RawData()},
				{false, nil, blks[4].RawData()},
			},
			errorsOnSend: []error{
				nil, nil, nil, nil, nil,
			},
			finalError:    errors.New("traverse failed"),
			expectedError: errors.New("traverse failed"),
		},
		"error on load": {
			linksToLoad:       links[:3],
			linkLoadsExpected: 3,
			loadOutcomes: []traverseOutcome{
				{false, nil, blks[0].RawData()},
				{false, nil, blks[1].RawData()},
				{true, traversal.SkipMe{}, nil},
			},
			errorsOnSend: []error{
				nil, nil, nil,
			},
		},
		"error on send": {
			linksToLoad:       links,
			linkLoadsExpected: 3,
			loadOutcomes: []traverseOutcome{
				{false, nil, blks[0].RawData()},
				{false, nil, blks[1].RawData()},
				{false, nil, blks[2].RawData()},
			},
			errorsOnSend: []error{
				nil, nil, errors.New("something went wrong"),
			},
			expectedError: errors.New("something went wrong"),
		},
	}
	for testCase, data := range testCases {
		t.Run(testCase, func(t *testing.T) {
			fl := &fakeLoader{
				expectedLoads: data.linksToLoad[:data.linkLoadsExpected],
				loadReturns:   data.loadOutcomes,
			}
			ft := &fakeTraverser{
				finalError:       data.finalError,
				loadedLinks:      data.linksToLoad,
				expectedOutcomes: data.loadOutcomes,
			}
			frs := newFakeResponseSender()
			for i, err := range data.errorsOnSend {
				frs.expectResponse(data.linksToLoad[i].link, data.loadOutcomes[i].data, err)
			}
			err := RunTraversal(fl.Load, ft, frs.SendResponse)
			require.Equal(t, data.expectedError, err)
			fl.verifyExpectations(t)
			frs.verifyExpectations(t)
			ft.verifyExpectations(t)
		})
	}
}
