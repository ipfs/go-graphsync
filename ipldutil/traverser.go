package ipldutil

import (
	"context"
	"errors"
	"io"

	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/traversal"
	"github.com/ipld/go-ipld-prime/traversal/selector"
)

var defaultVisitor traversal.AdvVisitFn = func(traversal.Progress, ipld.Node, traversal.VisitReason) error { return nil }

// ContextCancelError is a sentinel that indicates the passed in context
// was cancelled
type ContextCancelError struct{}

func (cp ContextCancelError) Error() string {
	return "Context cancelled"
}

// TraversalBuilder defines parameters for an iterative traversal
type TraversalBuilder struct {
	Root     ipld.Link
	Selector ipld.Node
	Visitor  traversal.AdvVisitFn
	Chooser  traversal.LinkTargetNodePrototypeChooser
}

// Traverser is an interface for performing a selector traversal that operates iteratively --
// it stops and waits for a manual load every time a block boundary is encountered
type Traverser interface {
	// IsComplete returns the completion state (boolean) and if so, the final error result from IPLD
	IsComplete() (bool, error)
	// Current request returns the current link waiting to be loaded
	CurrentRequest() (ipld.Link, ipld.LinkContext)
	// Advance advances the traversal successfully by supplying the given reader as the result of the next IPLD load
	Advance(reader io.Reader) error
	// Error errors the traversal by returning the given error as the result of the next IPLD load
	Error(err error)
	// Shutdown cancels the traversal
	Shutdown(ctx context.Context)
}

type state struct {
	isDone         bool
	completionErr  error
	currentLink    ipld.Link
	currentContext ipld.LinkContext
}

type nextResponse struct {
	input io.Reader
	err   error
}

// Start initiates the traversal (run in a go routine because the regular
// selector traversal expects a call back)
func (tb TraversalBuilder) Start(parentCtx context.Context) Traverser {
	ctx, cancel := context.WithCancel(parentCtx)
	t := &traverser{
		parentCtx:    parentCtx,
		ctx:          ctx,
		cancel:       cancel,
		root:         tb.Root,
		selector:     tb.Selector,
		visitor:      defaultVisitor,
		chooser:      defaultChooser,
		awaitRequest: make(chan struct{}, 1),
		stateChan:    make(chan state, 1),
		responses:    make(chan nextResponse),
		stopped:      make(chan struct{}),
	}
	if tb.Visitor != nil {
		t.visitor = tb.Visitor
	}
	if tb.Chooser != nil {
		t.chooser = tb.Chooser
	}
	t.start()
	return t
}

// traverser is a class to perform a selector traversal that stops every time a new block is loaded
// and waits for manual input (in the form of advance or error)
type traverser struct {
	parentCtx      context.Context
	ctx            context.Context
	cancel         func()
	root           ipld.Link
	selector       ipld.Node
	visitor        traversal.AdvVisitFn
	chooser        traversal.LinkTargetNodePrototypeChooser
	currentLink    ipld.Link
	currentContext ipld.LinkContext
	isDone         bool
	completionErr  error
	awaitRequest   chan struct{}
	stateChan      chan state
	responses      chan nextResponse
	stopped        chan struct{}
}

func (t *traverser) checkState() {
	select {
	case <-t.awaitRequest:
		select {
		case <-t.ctx.Done():
			t.isDone = true
			t.completionErr = ContextCancelError{}
		case newState := <-t.stateChan:
			t.isDone = newState.isDone
			t.completionErr = newState.completionErr
			t.currentLink = newState.currentLink
			t.currentContext = newState.currentContext
		}
	default:
	}
}

func (t *traverser) writeDone(err error) {
	select {
	case <-t.ctx.Done():
	case t.stateChan <- state{true, err, nil, ipld.LinkContext{}}:
	}
}

func (t *traverser) start() {
	select {
	case <-t.ctx.Done():
		return
	case t.awaitRequest <- struct{}{}:
	}
	go func() {
		defer close(t.stopped)
		loader := func(lnk ipld.Link, lnkCtx ipld.LinkContext) (io.Reader, error) {
			select {
			case <-t.ctx.Done():
				return nil, ContextCancelError{}
			case t.stateChan <- state{false, nil, lnk, lnkCtx}:
			}
			select {
			case <-t.ctx.Done():
				return nil, ContextCancelError{}
			case response := <-t.responses:
				return response.input, response.err
			}
		}
		ns, err := t.chooser(t.root, ipld.LinkContext{})
		if err != nil {
			t.writeDone(err)
			return
		}
		nb := ns.NewBuilder()
		err = t.root.Load(t.ctx, ipld.LinkContext{}, nb, loader)
		if err != nil {
			t.writeDone(err)
			return
		}
		nd := nb.Build()

		sel, err := selector.ParseSelector(t.selector)
		if err != nil {
			t.writeDone(err)
			return
		}
		err = traversal.Progress{
			Cfg: &traversal.Config{
				Ctx:                        t.ctx,
				LinkLoader:                 loader,
				LinkTargetNodePrototypeChooser: t.chooser,
			},
		}.WalkAdv(nd, sel, t.visitor)
		t.writeDone(err)
	}()
}

func (t *traverser) Shutdown(ctx context.Context) {
	t.cancel()
	select {
	case <-ctx.Done():
	case <-t.stopped:
	}
}

// IsComplete returns true if a traversal is complete
func (t *traverser) IsComplete() (bool, error) {
	t.checkState()
	return t.isDone, t.completionErr
}

// CurrentRequest returns the current block load waiting to be fulfilled in order
// to advance further
func (t *traverser) CurrentRequest() (ipld.Link, ipld.LinkContext) {
	t.checkState()
	return t.currentLink, t.currentContext
}

// Advance advances the traversal with an io.Reader for the next requested block
func (t *traverser) Advance(reader io.Reader) error {
	isComplete, _ := t.IsComplete()
	if isComplete {
		return errors.New("cannot advance when done")
	}
	select {
	case <-t.ctx.Done():
		return ContextCancelError{}
	case t.awaitRequest <- struct{}{}:
	}
	select {
	case <-t.ctx.Done():
		return ContextCancelError{}
	case t.responses <- nextResponse{reader, nil}:
	}
	return nil
}

// Error aborts the traversal with an error
func (t *traverser) Error(err error) {
	isComplete, _ := t.IsComplete()
	if isComplete {
		return
	}
	select {
	case <-t.ctx.Done():
		return
	case t.awaitRequest <- struct{}{}:
	}
	select {
	case <-t.ctx.Done():
	case t.responses <- nextResponse{nil, err}:
	}
}
