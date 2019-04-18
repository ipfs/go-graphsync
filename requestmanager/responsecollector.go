package requestmanager

import (
	"context"
)

type responseCollector struct {
	ctx context.Context
}

func newResponseCollector(ctx context.Context) *responseCollector {
	return &responseCollector{ctx}
}

func (rc *responseCollector) collectResponses(
	requestCtx context.Context,
	incomingResponses <-chan ResponseProgress,
	incomingErrors <-chan error,
	cancelRequest func()) (<-chan ResponseProgress, <-chan error) {

	returnedResponses := make(chan ResponseProgress)
	returnedErrors := make(chan error)

	go func() {
		var receivedResponses []ResponseProgress
		defer close(returnedResponses)
		outgoingResponses := func() chan<- ResponseProgress {
			if len(receivedResponses) == 0 {
				return nil
			}
			return returnedResponses
		}
		nextResponse := func() ResponseProgress {
			if len(receivedResponses) == 0 {
				return ResponseProgress{}
			}
			return receivedResponses[0]
		}
		for len(receivedResponses) > 0 || incomingResponses != nil {
			select {
			case <-rc.ctx.Done():
				return
			case <-requestCtx.Done():
				if incomingResponses != nil {
					cancelRequest()
				}
				return
			case response, ok := <-incomingResponses:
				if !ok {
					incomingResponses = nil
				} else {
					receivedResponses = append(receivedResponses, response)
				}
			case outgoingResponses() <- nextResponse():
				receivedResponses = receivedResponses[1:]
			}
		}
	}()
	go func() {
		var receivedErrors []error
		defer close(returnedErrors)

		outgoingErrors := func() chan<- error {
			if len(receivedErrors) == 0 {
				return nil
			}
			return returnedErrors
		}
		nextError := func() error {
			if len(receivedErrors) == 0 {
				return nil
			}
			return receivedErrors[0]
		}

		for len(receivedErrors) > 0 || incomingErrors != nil {
			select {
			case <-rc.ctx.Done():
				return
			case <-requestCtx.Done():
				return
			case err, ok := <-incomingErrors:
				if !ok {
					incomingErrors = nil
				} else {
					receivedErrors = append(receivedErrors, err)
				}
			case outgoingErrors() <- nextError():
				receivedErrors = receivedErrors[1:]
			}
		}
	}()
	return returnedResponses, returnedErrors
}
