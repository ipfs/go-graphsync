package responsemanager

import (
	"context"
	"errors"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/cidset"
	"github.com/ipfs/go-graphsync/dedupkey"
	"github.com/ipfs/go-graphsync/ipldutil"
	gsmsg "github.com/ipfs/go-graphsync/message"
	"github.com/ipfs/go-graphsync/notifications"
	"github.com/ipfs/go-graphsync/responsemanager/responseassembler"
	ipld "github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/libp2p/go-libp2p-core/peer"
)

type queryPreparer struct {
	requestHooks      RequestHooks
	responseAssembler ResponseAssembler
	loader            ipld.Loader
}

func (qe *queryPreparer) prepareQuery(ctx context.Context,
	p peer.ID,
	request gsmsg.GraphSyncRequest, signals signals, sub *notifications.TopicDataSubscriber) (ipld.Loader, ipldutil.Traverser, bool, error) {
	log.Infof("Processing request hooks for request: %d", request.ID())
	result := qe.requestHooks.ProcessRequestHooks(p, request)
	var transactionError error
	var isPaused bool
	failNotifee := notifications.Notifee{Data: graphsync.RequestFailedUnknown, Subscriber: sub}
	rejectNotifee := notifications.Notifee{Data: graphsync.RequestRejected, Subscriber: sub}
	err := qe.responseAssembler.Transaction(p, request.ID(), func(rb responseassembler.ResponseBuilder) error {
		for _, extension := range result.Extensions {
			rb.SendExtensionData(extension)
		}
		if result.Err != nil {
			rb.FinishWithError(graphsync.RequestFailedUnknown)
			rb.AddNotifee(failNotifee)
			transactionError = result.Err
		} else if !result.IsValidated {
			rb.FinishWithError(graphsync.RequestRejected)
			rb.AddNotifee(rejectNotifee)
			transactionError = errors.New("request not valid")
		} else if result.IsPaused {
			rb.PauseRequest()
			isPaused = true
		}
		return nil
	})
	if err != nil {
		return nil, nil, false, err
	}
	if transactionError != nil {
		return nil, nil, false, transactionError
	}
	if err := qe.processDedupByKey(request, p, failNotifee); err != nil {
		return nil, nil, false, err
	}
	if err := qe.processDoNoSendCids(request, p, failNotifee); err != nil {
		return nil, nil, false, err
	}
	rootLink := cidlink.Link{Cid: request.Root()}
	traverser := ipldutil.TraversalBuilder{
		Root:     rootLink,
		Selector: request.Selector(),
		Chooser:  result.CustomChooser,
	}.Start(ctx)
	loader := result.CustomLoader
	if loader == nil {
		loader = qe.loader
	}
	return loader, traverser, isPaused, nil
}

func (qe *queryPreparer) processDedupByKey(request gsmsg.GraphSyncRequest, p peer.ID, failNotifee notifications.Notifee) error {
	dedupData, has := request.Extension(graphsync.ExtensionDeDupByKey)
	if !has {
		return nil
	}
	key, err := dedupkey.DecodeDedupKey(dedupData)
	if err != nil {
		_ = qe.responseAssembler.Transaction(p, request.ID(), func(rb responseassembler.ResponseBuilder) error {
			rb.FinishWithError(graphsync.RequestFailedUnknown)
			rb.AddNotifee(failNotifee)
			return nil
		})
		return err
	}
	qe.responseAssembler.DedupKey(p, request.ID(), key)
	return nil
}

func (qe *queryPreparer) processDoNoSendCids(request gsmsg.GraphSyncRequest, p peer.ID, failNotifee notifications.Notifee) error {
	doNotSendCidsData, has := request.Extension(graphsync.ExtensionDoNotSendCIDs)
	if !has {
		return nil
	}
	cidSet, err := cidset.DecodeCidSet(doNotSendCidsData)
	if err != nil {
		_ = qe.responseAssembler.Transaction(p, request.ID(), func(rb responseassembler.ResponseBuilder) error {
			rb.FinishWithError(graphsync.RequestFailedUnknown)
			rb.AddNotifee(failNotifee)
			return nil
		})
		return err
	}
	links := make([]ipld.Link, 0, cidSet.Len())
	err = cidSet.ForEach(func(c cid.Cid) error {
		links = append(links, cidlink.Link{Cid: c})
		return nil
	})
	if err != nil {
		return err
	}
	qe.responseAssembler.IgnoreBlocks(p, request.ID(), links)
	return nil
}
