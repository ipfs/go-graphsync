package message

import (
	"encoding/binary"
	"errors"
	"io"
	"sync"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/ipldutil"
	pb "github.com/ipfs/go-graphsync/message/pb"
	"github.com/ipld/go-ipld-prime"
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

// FromMsgReader can deserialize a protobuf message into a GraphySyncMessage.
func (mh *MessageHandler) FromMsgReader(r msgio.Reader) (GraphSyncMessage, error) {
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

	return mh.newMessageFromProto(&pb)
}

// FromMsgReaderV1 can deserialize a v1.0.0 protobuf message into a GraphySyncMessage.
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

// ToProto converts a GraphSyncMessage to its pb.Message equivalent
func (mh *MessageHandler) ToProto(gsm GraphSyncMessage) (*pb.Message, error) {
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
		pbm.Requests = append(pbm.Requests, &pb.Message_Request{
			Id:         request.id.Bytes(),
			Root:       request.root.Bytes(),
			Selector:   selector,
			Priority:   int32(request.priority),
			Cancel:     request.isCancel,
			Update:     request.isUpdate,
			Extensions: toProtoExtensions(request.Extensions),
		})
	}

	pbm.Responses = make([]*pb.Message_Response, 0, len(gsm.responses))
	for _, response := range gsm.responses {
		pbm.Responses = append(pbm.Responses, &pb.Message_Response{
			Id:         response.requestID.Bytes(),
			Status:     int32(response.status),
			Extensions: toProtoExtensions(request.Extensions),
		})
	}

	blocks := gsm.Blocks()
	pbm.Data = make([]*pb.Message_Block, 0, len(blocks))
	for _, b := range blocks {
		pbm.Data = append(pbm.Data, &pb.Message_Block{
			Data:   b.RawData(),
			Prefix: b.Cid().Prefix().Bytes(),
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
		pbm.Requests = append(pbm.Requests, &pb.Message_V1_0_0_Request{
			Id:         rid,
			Root:       request.root.Bytes(),
			Selector:   selector,
			Priority:   int32(request.priority),
			Cancel:     request.isCancel,
			Update:     request.isUpdate,
			Extensions: request.extensions,
		})
	}

	pbm.Responses = make([]*pb.Message_V1_0_0_Response, 0, len(gsm.responses))
	for _, response := range gsm.responses {
		rid, err := bytesIdToInt(p, mh.fromV1Map, mh.toV1Map, &mh.nextIntId, response.requestID.Bytes())
		if err != nil {
			return nil, err
		}
		pbm.Responses = append(pbm.Responses, &pb.Message_V1_0_0_Response{
			Id:         rid,
			Status:     int32(response.status),
			Extensions: response.extensions,
		})
	}

	blocks := gsm.Blocks()
	pbm.Data = make([]*pb.Message_V1_0_0_Block, 0, len(blocks))
	for _, b := range blocks {
		pbm.Data = append(pbm.Data, &pb.Message_V1_0_0_Block{
			Data:   b.RawData(),
			Prefix: b.Cid().Prefix().Bytes(),
		})
	}
	return pbm, nil
}

// ToNet writes a GraphSyncMessage in its protobuf format to a writer
func (mh *MessageHandler) ToNet(gsm GraphSyncMessage, w io.Writer) error {
	msg, err := mh.ToProto(gsm)
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

// ToNet writes a GraphSyncMessage in its v1.0.0 protobuf format to a writer
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

func toProtoExtensions(m GraphSyncExtensions) map[string][]byte {
	protoExts := make(map[string][]byte, len(m.Values))
	for name, node := range m.Values {
		// Only keep those which are plain bytes,
		// as those are the only ones that the older protocol clients understand.
		if node.Kind() != ipld.Kind_Bytes {
			continue
		}
		raw, err := node.AsBytes()
		if err != nil {
			panic(err) // shouldn't happen
		}
		protoExts[name] = raw
	}
	return protoExts
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

// Mapping from a pb.Message object to a GraphSyncMessage object
func (mh *MessageHandler) newMessageFromProto(pbm *pb.Message) (GraphSyncMessage, error) {
	requests := make(map[graphsync.RequestID]GraphSyncRequest, len(pbm.Requests))
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

		var selector ipld.Node
		if !req.Cancel && !req.Update {
			selector, err = ipldutil.DecodeNode(req.Selector)
			if err != nil {
				return GraphSyncMessage{}, err
			}
		}
		exts := req.Extensions
		if exts == nil {
			exts = make(map[string][]byte)
		}
		id, err := graphsync.ParseRequestID(req.Id)
		if err != nil {
			return GraphSyncMessage{}, err
		}
		requests[id] = newRequest(id, root, selector, graphsync.Priority(req.Priority), req.Cancel, req.Update, exts)
	}

	responses := make(map[graphsync.RequestID]GraphSyncResponse, len(pbm.Responses))
	for _, res := range pbm.Responses {
		if res == nil {
			return GraphSyncMessage{}, errors.New("response is nil")
		}
		exts := res.Extensions
		if exts == nil {
			exts = make(map[string][]byte)
		}
		id, err := graphsync.ParseRequestID(res.Id)
		if err != nil {
			return GraphSyncMessage{}, err
		}
		responses[id] = newResponse(id, graphsync.ResponseStatusCode(res.Status), exts)
	}

	blks := make(map[cid.Cid]blocks.Block, len(pbm.Data))
	for _, b := range pbm.Data {
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

	requests := make(map[graphsync.RequestID]GraphSyncRequest, len(pbm.Requests))
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

		var selector ipld.Node
		if !req.Cancel && !req.Update {
			selector, err = ipldutil.DecodeNode(req.Selector)
			if err != nil {
				return GraphSyncMessage{}, err
			}
		}
		exts := req.Extensions
		if exts == nil {
			exts = make(map[string][]byte)
		}
		id, err := intIdToRequestId(p, mh.fromV1Map, mh.toV1Map, req.Id)
		if err != nil {
			return GraphSyncMessage{}, err
		}
		requests[id] = newRequest(id, root, selector, graphsync.Priority(req.Priority), req.Cancel, req.Update, exts)
	}

	responses := make(map[graphsync.RequestID]GraphSyncResponse, len(pbm.Responses))
	for _, res := range pbm.Responses {
		if res == nil {
			return GraphSyncMessage{}, errors.New("response is nil")
		}
		exts := res.Extensions
		if exts == nil {
			exts = make(map[string][]byte)
		}
		id, err := intIdToRequestId(p, mh.fromV1Map, mh.toV1Map, res.Id)
		if err != nil {
			return GraphSyncMessage{}, err
		}
		responses[id] = newResponse(id, graphsync.ResponseStatusCode(res.Status), exts)
	}

	blks := make(map[cid.Cid]blocks.Block, len(pbm.Data))
	for _, b := range pbm.Data {
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
