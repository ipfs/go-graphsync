package testutil

import (
	"context"

	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/ipld/go-ipld-prime"
	"github.com/libp2p/go-libp2p-core/peer"
)

// OpenedChannel records a call to open a channel
type OpenedChannel struct {
	DataSender peer.ID
	ChannelID  datatransfer.ChannelID
	Root       ipld.Link
	Selector   ipld.Node
	Message    datatransfer.Message
}

// ResumedChannel records a call to resume a channel
type ResumedChannel struct {
	ChannelID datatransfer.ChannelID
	Message   datatransfer.Message
}

// CustomizedTransfer is just a way to record calls made to transport configurer
type CustomizedTransfer struct {
	ChannelID datatransfer.ChannelID
	Voucher   datatransfer.Voucher
}

// FakeTransport is a fake transport with mocked results
type FakeTransport struct {
	OpenedChannels      []OpenedChannel
	OpenChannelErr      error
	ClosedChannels      []datatransfer.ChannelID
	CloseChannelErr     error
	PausedChannels      []datatransfer.ChannelID
	PauseChannelErr     error
	ResumedChannels     []ResumedChannel
	ResumeChannelErr    error
	CleanedUpChannels   []datatransfer.ChannelID
	CustomizedTransfers []CustomizedTransfer
	EventHandler        datatransfer.EventsHandler
	SetEventHandlerErr  error
}

// NewFakeTransport returns a new instance of FakeTransport
func NewFakeTransport() *FakeTransport {
	return &FakeTransport{}
}

// OpenChannel initiates an outgoing request for the other peer to send data
// to us on this channel
// Note: from a data transfer symantic standpoint, it doesn't matter if the
// request is push or pull -- OpenChannel is called by the party that is
// intending to receive data
func (ft *FakeTransport) OpenChannel(ctx context.Context, dataSender peer.ID, channelID datatransfer.ChannelID, root ipld.Link, stor ipld.Node, msg datatransfer.Message) error {
	ft.OpenedChannels = append(ft.OpenedChannels, OpenedChannel{dataSender, channelID, root, stor, msg})
	return ft.OpenChannelErr
}

// CloseChannel closes the given channel
func (ft *FakeTransport) CloseChannel(ctx context.Context, chid datatransfer.ChannelID) error {
	ft.ClosedChannels = append(ft.ClosedChannels, chid)
	return ft.CloseChannelErr
}

// SetEventHandler sets the handler for events on channels
func (ft *FakeTransport) SetEventHandler(events datatransfer.EventsHandler) error {
	ft.EventHandler = events
	return ft.SetEventHandlerErr
}

// PauseChannel paused the given channel ID
func (ft *FakeTransport) PauseChannel(ctx context.Context, chid datatransfer.ChannelID) error {
	ft.PausedChannels = append(ft.PausedChannels, chid)
	return ft.PauseChannelErr
}

// ResumeChannel resumes the given channel
func (ft *FakeTransport) ResumeChannel(ctx context.Context, msg datatransfer.Message, chid datatransfer.ChannelID) error {
	ft.ResumedChannels = append(ft.ResumedChannels, ResumedChannel{chid, msg})
	return ft.ResumeChannelErr
}

// CleanupChannel cleans up the given channel
func (ft *FakeTransport) CleanupChannel(chid datatransfer.ChannelID) {
	ft.CleanedUpChannels = append(ft.CleanedUpChannels, chid)
}

func (ft *FakeTransport) RecordCustomizedTransfer(chid datatransfer.ChannelID, voucher datatransfer.Voucher) {
	ft.CustomizedTransfers = append(ft.CustomizedTransfers, CustomizedTransfer{chid, voucher})
}
