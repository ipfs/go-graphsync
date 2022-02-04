package v2

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-msgio"

	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/message"
	"github.com/ipfs/go-graphsync/message/ipldbind"
)

// MessageHandler is used to hold per-peer state for each connection. There is
// no state to hold for the v2 protocol, so this exists to provide a consistent
// interface between the protocol versions.
type MessageHandler struct{}

// NewMessageHandler creates a new MessageHandler
func NewMessageHandler() *MessageHandler {
	return &MessageHandler{}
}

// FromNet can read a network stream to deserialized a GraphSyncMessage
func (mh *MessageHandler) FromNet(p peer.ID, r io.Reader) (message.GraphSyncMessage, error) {
	reader := msgio.NewVarintReaderSize(r, network.MessageSizeMax)
	return mh.FromMsgReader(p, reader)
}

// FromMsgReader can deserialize a DAG-CBOR message into a GraphySyncMessage
func (mh *MessageHandler) FromMsgReader(_ peer.ID, r msgio.Reader) (message.GraphSyncMessage, error) {
	msg, err := r.ReadMsg()
	if err != nil {
		return message.GraphSyncMessage{}, err
	}

	builder := ipldbind.Prototype.Message.Representation().NewBuilder()
	err = dagcbor.Decode(builder, bytes.NewReader(msg))
	if err != nil {
		return message.GraphSyncMessage{}, err
	}
	node := builder.Build()
	ipldGSM := bindnode.Unwrap(node).(*ipldbind.GraphSyncMessage)
	return mh.fromIPLD(ipldGSM)
}

// ToProto converts a GraphSyncMessage to its ipldbind.GraphSyncMessage equivalent
func (mh *MessageHandler) toIPLD(gsm message.GraphSyncMessage) (*ipldbind.GraphSyncMessage, error) {
	ibm := new(ipldbind.GraphSyncMessage)
	requests := gsm.Requests()
	ibm.Requests = make([]ipldbind.GraphSyncRequest, 0, len(requests))
	for _, request := range requests {
		selector := request.Selector()
		selPtr := &selector
		if selector == nil {
			selPtr = nil
		}
		root := request.Root()
		rootPtr := &root
		if root == cid.Undef {
			rootPtr = nil
		}
		ibm.Requests = append(ibm.Requests, ipldbind.GraphSyncRequest{
			Id:          request.ID().Bytes(),
			Root:        rootPtr,
			Selector:    selPtr,
			Priority:    request.Priority(),
			RequestType: request.Type(),
			Extensions:  ipldbind.NewGraphSyncExtensions(request),
		})
	}

	responses := gsm.Responses()
	ibm.Responses = make([]ipldbind.GraphSyncResponse, 0, len(responses))
	for _, response := range responses {
		glsm, ok := response.Metadata().(message.GraphSyncLinkMetadata)
		if !ok {
			return nil, fmt.Errorf("unexpected metadata type")
		}
		ibm.Responses = append(ibm.Responses, ipldbind.GraphSyncResponse{
			Id:         response.RequestID().Bytes(),
			Status:     response.Status(),
			Metadata:   glsm.RawMetadata(),
			Extensions: ipldbind.NewGraphSyncExtensions(response),
		})
	}

	blocks := gsm.Blocks()
	ibm.Blocks = make([]ipldbind.GraphSyncBlock, 0, len(blocks))
	for _, b := range blocks {
		ibm.Blocks = append(ibm.Blocks, ipldbind.GraphSyncBlock{
			Data:   b.RawData(),
			Prefix: b.Cid().Prefix().Bytes(),
		})
	}

	return ibm, nil
}

// ToNet writes a GraphSyncMessage in its DAG-CBOR format to a writer,
// prefixed with a length uvar
func (mh *MessageHandler) ToNet(_ peer.ID, gsm message.GraphSyncMessage, w io.Writer) error {
	msg, err := mh.toIPLD(gsm)
	if err != nil {
		return err
	}

	lbuf := make([]byte, binary.MaxVarintLen64)
	buf := new(bytes.Buffer)
	buf.Write(lbuf)

	node := bindnode.Wrap(msg, ipldbind.Prototype.Message.Type())
	err = dagcbor.Encode(node.Representation(), buf)
	if err != nil {
		return err
	}

	lbuflen := binary.PutUvarint(lbuf, uint64(buf.Len()-binary.MaxVarintLen64))
	out := buf.Bytes()
	copy(out[binary.MaxVarintLen64-lbuflen:], lbuf[:lbuflen])
	_, err = w.Write(out[binary.MaxVarintLen64-lbuflen:])

	return err
}

// Mapping from a ipldbind.GraphSyncMessage object to a GraphSyncMessage object
func (mh *MessageHandler) fromIPLD(ibm *ipldbind.GraphSyncMessage) (message.GraphSyncMessage, error) {
	requests := make(map[graphsync.RequestID]message.GraphSyncRequest, len(ibm.Requests))
	for _, req := range ibm.Requests {
		id, err := graphsync.ParseRequestID(req.Id)
		if err != nil {
			return message.GraphSyncMessage{}, err
		}

		if req.RequestType == graphsync.RequestTypeCancel {
			requests[id] = message.NewCancelRequest(id)
			continue
		}

		if req.RequestType == graphsync.RequestTypeUpdate {
			requests[id] = message.NewUpdateRequest(id, req.Extensions.ToExtensionsList()...)
			continue
		}

		root := cid.Undef
		if req.Root != nil {
			root = *req.Root
		}

		var selector datamodel.Node
		if req.Selector != nil {
			selector = *req.Selector
		}

		requests[id] = message.NewRequest(id, root, selector, graphsync.Priority(req.Priority), req.Extensions.ToExtensionsList()...)
	}

	responses := make(map[graphsync.RequestID]message.GraphSyncResponse, len(ibm.Responses))
	for _, res := range ibm.Responses {
		id, err := graphsync.ParseRequestID(res.Id)
		if err != nil {
			return message.GraphSyncMessage{}, err
		}
		responses[id] = message.NewResponse(id,
			graphsync.ResponseStatusCode(res.Status),
			res.Metadata,
			res.Extensions.ToExtensionsList()...)
	}

	blks := make(map[cid.Cid]blocks.Block, len(ibm.Blocks))
	for _, b := range ibm.Blocks {
		pref, err := cid.PrefixFromBytes(b.Prefix)
		if err != nil {
			return message.GraphSyncMessage{}, err
		}

		c, err := pref.Sum(b.Data)
		if err != nil {
			return message.GraphSyncMessage{}, err
		}

		blk, err := blocks.NewBlockWithCid(b.Data, c)
		if err != nil {
			return message.GraphSyncMessage{}, err
		}

		blks[blk.Cid()] = blk
	}

	return message.NewMessage(requests, responses, blks), nil
}
