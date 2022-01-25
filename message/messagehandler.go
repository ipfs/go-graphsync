package message

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"sync"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/ipldutil"
	"github.com/ipfs/go-graphsync/message/ipldbind"
	pb "github.com/ipfs/go-graphsync/message/pb"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	pool "github.com/libp2p/go-buffer-pool"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-msgio"
	"google.golang.org/protobuf/proto"
)

type v1RequestKey struct {
	p  peer.ID
	id int32
}

type MessageHandler struct {
	mapLock sync.Mutex
	// each host can have multiple peerIDs, so our integer requestID mapping for
	// protocol v1.0.0 needs to be a combo of peerID and requestID
	fromV1Map map[v1RequestKey]graphsync.RequestID
	toV1Map   map[graphsync.RequestID]int32
	nextIntId int32
}

// NewMessageHandler instantiates a new MessageHandler instance
func NewMessageHandler() *MessageHandler {
	return &MessageHandler{
		fromV1Map: make(map[v1RequestKey]graphsync.RequestID),
		toV1Map:   make(map[graphsync.RequestID]int32),
	}
}

// FromNet can read a network stream to deserialized a GraphSyncMessage
func (mh *MessageHandler) FromNet(r io.Reader) (GraphSyncMessage, error) {
	reader := msgio.NewVarintReaderSize(r, network.MessageSizeMax)
	return mh.FromMsgReader(reader)
}

// FromNetV1 can read a v1.0.0 network stream to deserialized a GraphSyncMessage
func (mh *MessageHandler) FromNetV1(p peer.ID, r io.Reader) (GraphSyncMessage, error) {
	reader := msgio.NewVarintReaderSize(r, network.MessageSizeMax)
	return mh.FromMsgReaderV1(p, reader)
}

// FromMsgReader can deserialize a DAG-CBOR message into a GraphySyncMessage
func (mh *MessageHandler) FromMsgReader(r msgio.Reader) (GraphSyncMessage, error) {
	msg, err := r.ReadMsg()
	if err != nil {
		return GraphSyncMessage{}, err
	}

	builder := ipldbind.Prototype.Message.Representation().NewBuilder()
	err = dagcbor.Decode(builder, bytes.NewReader(msg))
	if err != nil {
		return GraphSyncMessage{}, err
	}

	node := builder.Build()
	ipldGSM := bindnode.Unwrap(node).(*ipldbind.GraphSyncMessage)
	return mh.fromIPLD(ipldGSM)
}

// FromMsgReaderV11 can deserialize a v1.1.0 protobuf message into a GraphySyncMessage
func (mh *MessageHandler) FromMsgReaderV11(r msgio.Reader) (GraphSyncMessage, error) {
	msg, err := r.ReadMsg()
	if err != nil {
		return GraphSyncMessage{}, err
	}

	var pb pb.Message
	err = proto.Unmarshal(msg, &pb)
	r.ReleaseMsg(msg)
	if err != nil {
		return GraphSyncMessage{}, err
	}

	return mh.newMessageFromProtoV11(&pb)
}

// FromMsgReaderV1 can deserialize a v1.0.0 protobuf message into a GraphySyncMessage
func (mh *MessageHandler) FromMsgReaderV1(p peer.ID, r msgio.Reader) (GraphSyncMessage, error) {
	msg, err := r.ReadMsg()
	if err != nil {
		return GraphSyncMessage{}, err
	}

	var pb pb.Message_V1_0_0
	err = proto.Unmarshal(msg, &pb)
	r.ReleaseMsg(msg)
	if err != nil {
		return GraphSyncMessage{}, err
	}

	return mh.newMessageFromProtoV1(p, &pb)
}

// ToProto converts a GraphSyncMessage to its ipldbind.GraphSyncMessage equivalent
func (mh *MessageHandler) toIPLD(gsm GraphSyncMessage) (*ipldbind.GraphSyncMessage, error) {
	ibm := new(ipldbind.GraphSyncMessage)
	ibm.Requests = make([]ipldbind.GraphSyncRequest, 0, len(gsm.requests))
	for _, request := range gsm.requests {
		sel := &request.selector
		if request.selector == nil {
			sel = nil
		}
		root := &request.root
		if request.root == cid.Undef {
			root = nil
		}
		ibm.Requests = append(ibm.Requests, ipldbind.GraphSyncRequest{
			Id:         request.id.Bytes(),
			Root:       root,
			Selector:   sel,
			Priority:   request.priority,
			Cancel:     request.isCancel,
			Update:     request.isUpdate,
			Extensions: ipldbind.NewGraphSyncExtensions(request.extensions),
		})
	}

	ibm.Responses = make([]ipldbind.GraphSyncResponse, 0, len(gsm.responses))
	for _, response := range gsm.responses {
		ibm.Responses = append(ibm.Responses, ipldbind.GraphSyncResponse{
			Id:         response.requestID.Bytes(),
			Status:     response.status,
			Extensions: ipldbind.NewGraphSyncExtensions(response.extensions),
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

// ToProto converts a GraphSyncMessage to its pb.Message equivalent
func (mh *MessageHandler) ToProtoV11(gsm GraphSyncMessage) (*pb.Message, error) {
	pbm := new(pb.Message)
	pbm.Requests = make([]*pb.Message_Request, 0, len(gsm.requests))
	for _, request := range gsm.requests {
		var selector []byte
		var err error
		if request.selector != nil {
			selector, err = ipldutil.EncodeNode(request.selector)
			if err != nil {
				return nil, err
			}
		}
		ext, err := toEncodedExtensions(request.extensions)
		if err != nil {
			return nil, err
		}
		pbm.Requests = append(pbm.Requests, &pb.Message_Request{
			Id:         request.id.Bytes(),
			Root:       request.root.Bytes(),
			Selector:   selector,
			Priority:   int32(request.priority),
			Cancel:     request.isCancel,
			Update:     request.isUpdate,
			Extensions: ext,
		})
	}

	pbm.Responses = make([]*pb.Message_Response, 0, len(gsm.responses))
	for _, response := range gsm.responses {
		ext, err := toEncodedExtensions(response.extensions)
		if err != nil {
			return nil, err
		}

		pbm.Responses = append(pbm.Responses, &pb.Message_Response{
			Id:         response.requestID.Bytes(),
			Status:     int32(response.status),
			Extensions: ext,
		})
	}

	pbm.Data = make([]*pb.Message_Block, 0, len(gsm.blocks))
	for _, b := range gsm.blocks {
		pbm.Data = append(pbm.Data, &pb.Message_Block{
			Prefix: b.Cid().Prefix().Bytes(),
			Data:   b.RawData(),
		})
	}

	return pbm, nil
}

// ToProtoV1 converts a GraphSyncMessage to its pb.Message_V1_0_0 equivalent
func (mh *MessageHandler) ToProtoV1(p peer.ID, gsm GraphSyncMessage) (*pb.Message_V1_0_0, error) {
	mh.mapLock.Lock()
	defer mh.mapLock.Unlock()

	pbm := new(pb.Message_V1_0_0)
	pbm.Requests = make([]*pb.Message_V1_0_0_Request, 0, len(gsm.requests))
	for _, request := range gsm.requests {
		var selector []byte
		var err error
		if request.selector != nil {
			selector, err = ipldutil.EncodeNode(request.selector)
			if err != nil {
				return nil, err
			}
		}
		rid, err := bytesIdToInt(p, mh.fromV1Map, mh.toV1Map, &mh.nextIntId, request.id.Bytes())
		if err != nil {
			return nil, err
		}
		ext, err := toEncodedExtensions(request.extensions)
		if err != nil {
			return nil, err
		}
		pbm.Requests = append(pbm.Requests, &pb.Message_V1_0_0_Request{
			Id:         rid,
			Root:       request.root.Bytes(),
			Selector:   selector,
			Priority:   int32(request.priority),
			Cancel:     request.isCancel,
			Update:     request.isUpdate,
			Extensions: ext,
		})
	}

	pbm.Responses = make([]*pb.Message_V1_0_0_Response, 0, len(gsm.responses))
	for _, response := range gsm.responses {
		rid, err := bytesIdToInt(p, mh.fromV1Map, mh.toV1Map, &mh.nextIntId, response.requestID.Bytes())
		if err != nil {
			return nil, err
		}
		ext, err := toEncodedExtensions(response.extensions)
		if err != nil {
			return nil, err
		}
		pbm.Responses = append(pbm.Responses, &pb.Message_V1_0_0_Response{
			Id:         rid,
			Status:     int32(response.status),
			Extensions: ext,
		})
	}

	pbm.Data = make([]*pb.Message_V1_0_0_Block, 0, len(gsm.blocks))
	for _, b := range gsm.blocks {
		pbm.Data = append(pbm.Data, &pb.Message_V1_0_0_Block{
			Prefix: b.Cid().Prefix().Bytes(),
			Data:   b.RawData(),
		})
	}
	return pbm, nil
}

// ToNet writes a GraphSyncMessage in its DAG-CBOR format to a writer,
// prefixed with a length uvar
func (mh *MessageHandler) ToNet(gsm GraphSyncMessage, w io.Writer) error {
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
	//_, err = buf.WriteTo(w)

	lbuflen := binary.PutUvarint(lbuf, uint64(buf.Len()-binary.MaxVarintLen64))
	out := buf.Bytes()
	copy(out[binary.MaxVarintLen64-lbuflen:], lbuf[:lbuflen])
	_, err = w.Write(out[binary.MaxVarintLen64-lbuflen:])

	return err
}

// ToNetV11 writes a GraphSyncMessage in its v1.1.0 protobuf format to a writer
func (mh *MessageHandler) ToNetV11(gsm GraphSyncMessage, w io.Writer) error {
	msg, err := mh.ToProtoV11(gsm)
	if err != nil {
		return err
	}
	size := proto.Size(msg)
	buf := pool.Get(size + binary.MaxVarintLen64)
	defer pool.Put(buf)

	n := binary.PutUvarint(buf, uint64(size))

	out, err := proto.MarshalOptions{}.MarshalAppend(buf[:n], msg)
	if err != nil {
		return err
	}
	_, err = w.Write(out)
	return err
}

// ToNetV1 writes a GraphSyncMessage in its v1.0.0 protobuf format to a writer
func (mh *MessageHandler) ToNetV1(p peer.ID, gsm GraphSyncMessage, w io.Writer) error {
	msg, err := mh.ToProtoV1(p, gsm)
	if err != nil {
		return err
	}
	size := proto.Size(msg)
	buf := pool.Get(size + binary.MaxVarintLen64)
	defer pool.Put(buf)

	n := binary.PutUvarint(buf, uint64(size))

	out, err := proto.MarshalOptions{}.MarshalAppend(buf[:n], msg)
	if err != nil {
		return err
	}
	_, err = w.Write(out)
	return err
}

// Maps a []byte slice form of a RequestID (uuid) to an integer format as used
// by a v1 peer. Inverse of intIdToRequestId()
func bytesIdToInt(p peer.ID, fromV1Map map[v1RequestKey]graphsync.RequestID, toV1Map map[graphsync.RequestID]int32, nextIntId *int32, id []byte) (int32, error) {
	rid, err := graphsync.ParseRequestID(id)
	if err != nil {
		return 0, err
	}
	iid, ok := toV1Map[rid]
	if !ok {
		iid = *nextIntId
		*nextIntId++
		toV1Map[rid] = iid
		fromV1Map[v1RequestKey{p, iid}] = rid
	}
	return iid, nil
}

// Maps an integer form of a RequestID as used by a v1 peer to a native (uuid) form.
// Inverse of bytesIdToInt().
func intIdToRequestId(p peer.ID, fromV1Map map[v1RequestKey]graphsync.RequestID, toV1Map map[graphsync.RequestID]int32, iid int32) (graphsync.RequestID, error) {
	key := v1RequestKey{p, iid}
	rid, ok := fromV1Map[key]
	if !ok {
		rid = graphsync.NewRequestID()
		fromV1Map[key] = rid
		toV1Map[rid] = iid
	}
	return rid, nil
}

// Mapping from a ipldbind.GraphSyncMessage object to a GraphSyncMessage object
func (mh *MessageHandler) fromIPLD(ibm *ipldbind.GraphSyncMessage) (GraphSyncMessage, error) {
	requests := make(map[graphsync.RequestID]GraphSyncRequest, len(ibm.Requests))
	for _, req := range ibm.Requests {
		id, err := graphsync.ParseRequestID(req.Id)
		if err != nil {
			return GraphSyncMessage{}, err
		}
		root := cid.Undef
		if req.Root != nil {
			root = *req.Root
		}
		var selector datamodel.Node
		if req.Selector != nil {
			selector = *req.Selector
		}
		requests[id] = newRequest(id, root, selector, graphsync.Priority(req.Priority), req.Cancel, req.Update, req.Extensions.Values)
	}

	responses := make(map[graphsync.RequestID]GraphSyncResponse, len(ibm.Responses))
	for _, res := range ibm.Responses {
		// exts := res.Extensions
		id, err := graphsync.ParseRequestID(res.Id)
		if err != nil {
			return GraphSyncMessage{}, err
		}
		responses[id] = newResponse(id, graphsync.ResponseStatusCode(res.Status), res.Extensions.Values)
	}

	blks := make(map[cid.Cid]blocks.Block, len(ibm.Blocks))
	for _, b := range ibm.Blocks {
		pref, err := cid.PrefixFromBytes(b.Prefix)
		if err != nil {
			return GraphSyncMessage{}, err
		}

		c, err := pref.Sum(b.Data)
		if err != nil {
			return GraphSyncMessage{}, err
		}

		blk, err := blocks.NewBlockWithCid(b.Data, c)
		if err != nil {
			return GraphSyncMessage{}, err
		}

		blks[blk.Cid()] = blk
	}

	return GraphSyncMessage{
		requests, responses, blks,
	}, nil
}

// Mapping from a pb.Message object to a GraphSyncMessage object
func (mh *MessageHandler) newMessageFromProtoV11(pbm *pb.Message) (GraphSyncMessage, error) {
	requests := make(map[graphsync.RequestID]GraphSyncRequest, len(pbm.GetRequests()))
	for _, req := range pbm.Requests {
		if req == nil {
			return GraphSyncMessage{}, errors.New("request is nil")
		}
		var root cid.Cid
		var err error
		if !req.Cancel && !req.Update {
			root, err = cid.Cast(req.Root)
			if err != nil {
				return GraphSyncMessage{}, err
			}
		}

		var selector datamodel.Node
		if !req.Cancel && !req.Update {
			selector, err = ipldutil.DecodeNode(req.Selector)
			if err != nil {
				return GraphSyncMessage{}, err
			}
		}
		id, err := graphsync.ParseRequestID(req.Id)
		if err != nil {
			return GraphSyncMessage{}, err
		}
		exts, err := fromEncodedExtensions(req.GetExtensions())
		if err != nil {
			return GraphSyncMessage{}, err
		}
		requests[id] = newRequest(id, root, selector, graphsync.Priority(req.Priority), req.Cancel, req.Update, exts)
	}
	responses := make(map[graphsync.RequestID]GraphSyncResponse, len(pbm.GetResponses()))
	for _, res := range pbm.Responses {
		if res == nil {
			return GraphSyncMessage{}, errors.New("response is nil")
		}
		id, err := graphsync.ParseRequestID(res.Id)
		if err != nil {
			return GraphSyncMessage{}, err
		}
		exts, err := fromEncodedExtensions(res.GetExtensions())
		if err != nil {
			return GraphSyncMessage{}, err
		}
		responses[id] = newResponse(id, graphsync.ResponseStatusCode(res.Status), exts)
	}

	blks := make(map[cid.Cid]blocks.Block, len(pbm.GetData()))
	for _, b := range pbm.GetData() {
		if b == nil {
			return GraphSyncMessage{}, errors.New("block is nil")
		}

		pref, err := cid.PrefixFromBytes(b.GetPrefix())
		if err != nil {
			return GraphSyncMessage{}, err
		}

		c, err := pref.Sum(b.GetData())
		if err != nil {
			return GraphSyncMessage{}, err
		}

		blk, err := blocks.NewBlockWithCid(b.GetData(), c)
		if err != nil {
			return GraphSyncMessage{}, err
		}

		blks[blk.Cid()] = blk
	}

	return GraphSyncMessage{
		requests, responses, blks,
	}, nil
}

// Mapping from a pb.Message_V1_0_0 object to a GraphSyncMessage object, including
// RequestID (int / uuid) mapping.
func (mh *MessageHandler) newMessageFromProtoV1(p peer.ID, pbm *pb.Message_V1_0_0) (GraphSyncMessage, error) {
	mh.mapLock.Lock()
	defer mh.mapLock.Unlock()

	requests := make(map[graphsync.RequestID]GraphSyncRequest, len(pbm.GetRequests()))
	for _, req := range pbm.Requests {
		if req == nil {
			return GraphSyncMessage{}, errors.New("request is nil")
		}
		var root cid.Cid
		var err error
		if !req.Cancel && !req.Update {
			root, err = cid.Cast(req.Root)
			if err != nil {
				return GraphSyncMessage{}, err
			}
		}

		var selector datamodel.Node
		if !req.Cancel && !req.Update {
			selector, err = ipldutil.DecodeNode(req.Selector)
			if err != nil {
				return GraphSyncMessage{}, err
			}
		}
		id, err := intIdToRequestId(p, mh.fromV1Map, mh.toV1Map, req.Id)
		if err != nil {
			return GraphSyncMessage{}, err
		}
		exts, err := fromEncodedExtensions(req.GetExtensions())
		if err != nil {
			return GraphSyncMessage{}, err
		}
		requests[id] = newRequest(id, root, selector, graphsync.Priority(req.Priority), req.Cancel, req.Update, exts)
	}

	responses := make(map[graphsync.RequestID]GraphSyncResponse, len(pbm.GetResponses()))
	for _, res := range pbm.Responses {
		if res == nil {
			return GraphSyncMessage{}, errors.New("response is nil")
		}
		id, err := intIdToRequestId(p, mh.fromV1Map, mh.toV1Map, res.Id)
		if err != nil {
			return GraphSyncMessage{}, err
		}
		exts, err := fromEncodedExtensions(res.GetExtensions())
		if err != nil {
			return GraphSyncMessage{}, err
		}
		responses[id] = newResponse(id, graphsync.ResponseStatusCode(res.Status), exts)
	}

	blks := make(map[cid.Cid]blocks.Block, len(pbm.GetData()))
	for _, b := range pbm.GetData() {
		if b == nil {
			return GraphSyncMessage{}, errors.New("block is nil")
		}

		pref, err := cid.PrefixFromBytes(b.GetPrefix())
		if err != nil {
			return GraphSyncMessage{}, err
		}

		c, err := pref.Sum(b.GetData())
		if err != nil {
			return GraphSyncMessage{}, err
		}

		blk, err := blocks.NewBlockWithCid(b.GetData(), c)
		if err != nil {
			return GraphSyncMessage{}, err
		}

		blks[blk.Cid()] = blk
	}

	return GraphSyncMessage{
		requests, responses, blks,
	}, nil
}

func toEncodedExtensions(in map[string]datamodel.Node) (map[string][]byte, error) {
	out := make(map[string][]byte, len(in))
	for name, data := range in {
		if data == nil {
			out[name] = nil
		} else {
			var buf bytes.Buffer
			err := dagcbor.Encode(data, &buf)
			if err != nil {
				return nil, err
			}
			out[name] = buf.Bytes()
		}
	}
	return out, nil
}

func fromEncodedExtensions(in map[string][]byte) (map[string]datamodel.Node, error) {
	if in == nil {
		return make(map[string]datamodel.Node), nil
	}
	out := make(map[string]datamodel.Node, len(in))
	for name, data := range in {
		if len(data) == 0 {
			out[name] = nil
		} else {
			nb := basicnode.Prototype.Any.NewBuilder()
			err := dagcbor.Decode(nb, bytes.NewReader(data))
			if err != nil {
				return nil, err
			}
			out[name] = nb.Build()
		}
	}
	return out, nil
}
