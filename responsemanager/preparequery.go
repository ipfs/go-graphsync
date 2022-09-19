package responsemanager

import (
	"context"

	"github.com/ipfs/go-cid"
	ipld "github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/cidset"
	"github.com/ipfs/go-graphsync/dedupkey"
	"github.com/ipfs/go-graphsync/donotsendfirstblocks"
	gsmsg "github.com/ipfs/go-graphsync/message"
	"github.com/ipfs/go-graphsync/responsemanager/hooks"
	"github.com/ipfs/go-graphsync/responsemanager/responseassembler"
)

type errorString string

func (e errorString) Error() string {
	return string(e)
}

const errInvalidRequest = errorString("request not valid")

func prepareQuery(
	ctx context.Context,
	p peer.ID,
	request gsmsg.GraphSyncRequest,
	result hooks.RequestResult,
	responseStream responseassembler.ResponseStream) error {
	err := responseStream.Transaction(func(rb responseassembler.ResponseBuilder) error {
		for _, extension := range result.Extensions {
			rb.SendExtensionData(extension)
		}
		if result.Err != nil {
			rb.FinishWithError(graphsync.RequestFailedUnknown)
			return result.Err
		} else if !result.IsValidated {
			rb.FinishWithError(graphsync.RequestRejected)
			return errInvalidRequest
		} else if result.IsPaused {
			rb.PauseRequest()
		}
		return nil
	})
	if err != nil {
		return err
	}
	if err := processDedupByKey(request, responseStream); err != nil {
		return err
	}
	if err := processDoNoSendCids(request, responseStream); err != nil {
		return err
	}
	if err := processDoNotSendFirstBlocks(request, responseStream); err != nil {
		return err
	}
	return nil
}

func processDedupByKey(request gsmsg.GraphSyncRequest, responseStream responseassembler.ResponseStream) error {
	dedupData, has := request.Extension(graphsync.ExtensionDeDupByKey)
	if !has {
		return nil
	}
	key, err := dedupkey.DecodeDedupKey(dedupData)
	if err != nil {
		_ = responseStream.Transaction(func(rb responseassembler.ResponseBuilder) error {
			rb.FinishWithError(graphsync.RequestFailedUnknown)
			return nil
		})
		return err
	}
	responseStream.DedupKey(key)
	return nil
}

func processDoNoSendCids(request gsmsg.GraphSyncRequest, responseStream responseassembler.ResponseStream) error {
	doNotSendCidsData, has := request.Extension(graphsync.ExtensionDoNotSendCIDs)
	if !has {
		return nil
	}
	cidSet, err := cidset.DecodeCidSet(doNotSendCidsData)
	if err != nil {
		_ = responseStream.Transaction(func(rb responseassembler.ResponseBuilder) error {
			rb.FinishWithError(graphsync.RequestFailedUnknown)
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
	responseStream.IgnoreBlocks(links)
	return nil
}

func processDoNotSendFirstBlocks(request gsmsg.GraphSyncRequest, responseStream responseassembler.ResponseStream) error {
	doNotSendFirstBlocksData, has := request.Extension(graphsync.ExtensionsDoNotSendFirstBlocks)
	if !has {
		return nil
	}
	skipCount, err := donotsendfirstblocks.DecodeDoNotSendFirstBlocks(doNotSendFirstBlocksData)
	if err != nil {
		_ = responseStream.Transaction(func(rb responseassembler.ResponseBuilder) error {
			rb.FinishWithError(graphsync.RequestFailedUnknown)
			return nil
		})
		return err
	}
	responseStream.SkipFirstBlocks(skipCount)
	return nil
}
