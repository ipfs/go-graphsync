package requestmanager

import "context"

type responseCollector struct {
	ctx context.Context
}

func newResponseCollector(ctx context.Context) *responseCollector {
	return &responseCollector{ctx}
}

func (rc *responseCollector) collectResponses(
	requestCtx context.Context,
	incomingResponses <-chan ResponseProgress, cancelRequest func()) <-chan ResponseProgress {

	returnedResponses := make(chan ResponseProgress)
	var receivedResponses []ResponseProgress

	go func() {
		defer close(returnedResponses)
		outgoingResponses := func() chan<- ResponseProgress {
			if len(receivedResponses) == 0 {
				return nil
			}
			return returnedResponses
		}
		nextResponse := func() ResponseProgress {
			if len(receivedResponses) == 0 {
				return nil
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
	return returnedResponses
}
