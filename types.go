package datatransfer

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/filecoin-project/go-data-transfer/encoding"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/libp2p/go-libp2p-core/peer"
)

type errorString string

func (es errorString) Error() string {
	return string(es)
}

//go:generate cbor-gen-for ChannelID

// ErrChannelNotFound indicates the given channel does not exist
const ErrChannelNotFound = errorString("channel not found")

// ErrPause can be returned from validators to pause a request
const ErrPause = errorString("data transfer pause request")

// TypeIdentifier is a unique string identifier for a type of encodable object in a
// registry
type TypeIdentifier string

// EmptyTypeIdentifier means there is no voucher present
const EmptyTypeIdentifier = TypeIdentifier("")

// Registerable is a type of object in a registry. It must be encodable and must
// have a single method that uniquely identifies its type
type Registerable interface {
	encoding.Encodable
	// Type is a unique string identifier for this voucher type
	Type() TypeIdentifier
}

// Voucher is used to validate
// a data transfer request against the underlying storage or retrieval deal
// that precipitated it. The only requirement is a voucher can read and write
// from bytes, and has a string identifier type
type Voucher Registerable

// VoucherResult is used to provide option additional information about a
// voucher being rejected or accepted
type VoucherResult Registerable

// Status is the status of transfer for a given channel
type Status uint64

const (
	// Requested means a data transfer was requested by has not yet been approved
	Requested Status = iota

	// Ongoing means the data transfer is in progress
	Ongoing

	// TransferFinished indicates the initiator is done sending/receiving
	// data but is awaiting confirmation from the responder
	TransferFinished

	// ResponderCompleted indicates the initiator received a message from the
	// responder that it's completed
	ResponderCompleted

	// Finalizing means the responder is awaiting a final message from the initator to
	// consider the transfer done
	Finalizing

	// Completing just means we have some final cleanup for a completed request
	Completing

	// Completed means the data transfer is completed successfully
	Completed

	// Failing just means we have some final cleanup for a failed request
	Failing

	// Failed means the data transfer failed
	Failed

	// Cancelling just means we have some final cleanup for a cancelled request
	Cancelling

	// Cancelled means the data transfer ended prematurely
	Cancelled

	// InitiatorPaused means the data sender has paused the channel (only the sender can unpause this)
	InitiatorPaused

	// ResponderPaused means the data receiver has paused the channel (only the receiver can unpause this)
	ResponderPaused

	// BothPaused means both sender and receiver have paused the channel seperately (both must unpause)
	BothPaused

	// ResponderFinalizing is a unique state where the responder is awaiting a final voucher
	ResponderFinalizing

	// ResponderFinalizingTransferFinished is a unique state where the responder is awaiting a final voucher
	// and we have received all data
	ResponderFinalizingTransferFinished

	// ChannelNotFoundError means the searched for data transfer does not exist
	ChannelNotFoundError
)

// Statuses are human readable names for data transfer states
var Statuses = map[Status]string{
	// Requested means a data transfer was requested by has not yet been approved
	Requested:                           "Requested",
	Ongoing:                             "Ongoing",
	TransferFinished:                    "TransferFinished",
	ResponderCompleted:                  "ResponderCompleted",
	Finalizing:                          "Finalizing",
	Completing:                          "Completing",
	Completed:                           "Completed",
	Failing:                             "Failing",
	Failed:                              "Failed",
	Cancelling:                          "Cancelling",
	Cancelled:                           "Cancelled",
	InitiatorPaused:                     "InitiatorPaused",
	ResponderPaused:                     "ResponderPaused",
	BothPaused:                          "BothPaused",
	ResponderFinalizing:                 "ResponderFinalizing",
	ResponderFinalizingTransferFinished: "ResponderFinalizingTransferFinished",
	ChannelNotFoundError:                "ChannelNotFoundError",
}

// TransferID is an identifier for a data transfer, shared between
// request/responder and unique to the requester
type TransferID uint64

// ChannelID is a unique identifier for a channel, distinct by both the other
// party's peer ID + the transfer ID
type ChannelID struct {
	Initiator peer.ID
	Responder peer.ID
	ID        TransferID
}

func (c ChannelID) String() string {
	return fmt.Sprintf("%s-%s-%d", c.Initiator, c.Responder, c.ID)
}

// OtherParty returns the peer on the other side of the request, depending
// on whether this peer is the initiator or responder
func (c ChannelID) OtherParty(thisPeer peer.ID) peer.ID {
	if thisPeer == c.Initiator {
		return c.Responder
	}
	return c.Initiator
}

// Channel represents all the parameters for a single data transfer
type Channel interface {
	// TransferID returns the transfer id for this channel
	TransferID() TransferID

	// BaseCID returns the CID that is at the root of this data transfer
	BaseCID() cid.Cid

	// Selector returns the IPLD selector for this data transfer (represented as
	// an IPLD node)
	Selector() ipld.Node

	// Voucher returns the voucher for this data transfer
	Voucher() Voucher

	// Sender returns the peer id for the node that is sending data
	Sender() peer.ID

	// Recipient returns the peer id for the node that is receiving data
	Recipient() peer.ID

	// TotalSize returns the total size for the data being transferred
	TotalSize() uint64

	// IsPull returns whether this is a pull request
	IsPull() bool

	// ChannelID returns the ChannelID for this request
	ChannelID() ChannelID

	// OtherParty returns the opposite party in the channel to the passed in party
	OtherParty(thisParty peer.ID) peer.ID
}

// ChannelState is channel parameters plus it's current state
type ChannelState interface {
	Channel

	// Status is the current status of this channel
	Status() Status

	// Sent returns the number of bytes sent
	Sent() uint64

	// Received returns the number of bytes received
	Received() uint64

	// Message offers additional information about the current status
	Message() string

	// Vouchers returns all vouchers sent on this channel
	Vouchers() []Voucher

	// VoucherResults are results of vouchers sent on the channel
	VoucherResults() []VoucherResult

	// LastVoucher returns the last voucher sent on the channel
	LastVoucher() Voucher

	// LastVoucherResult returns the last voucher result sent on the channel
	LastVoucherResult() VoucherResult
}

// EventCode is a name for an event that occurs on a data transfer channel
type EventCode int

const (
	// Open is an event occurs when a channel is first opened
	Open EventCode = iota

	// Accept is an event that emits when the data transfer is first accepted
	Accept

	// Progress is an event that gets emitted every time more data is transferred
	Progress

	// Cancel indicates one side has cancelled the transfer
	Cancel

	// Error is an event that emits when an error occurs in a data transfer
	Error

	// CleanupComplete emits when a request is cleaned up
	CleanupComplete

	// NewVoucher means we have a new voucher on this channel
	NewVoucher

	// NewVoucherResult means we have a new voucher result on this channel
	NewVoucherResult

	// PauseInitiator emits when the data sender pauses transfer
	PauseInitiator

	// ResumeInitiator emits when the data sender resumes transfer
	ResumeInitiator

	// PauseResponder emits when the data receiver pauses transfer
	PauseResponder

	// ResumeResponder emits when the data receiver resumes transfer
	ResumeResponder

	// FinishTransfer emits when the initiator has completed sending/receiving data
	FinishTransfer

	// ResponderCompletes emits when the initiator receives a message that the responder is finished
	ResponderCompletes

	// ResponderBeginsFinalization emits when the initiator receives a message that the responder is finilizing
	ResponderBeginsFinalization

	// BeginFinalizing emits when the responder completes its operations but awaits a response from the
	// initiator
	BeginFinalizing

	// Complete is emitted when a data transfer is complete
	Complete
)

// Events are human readable names for data transfer events
var Events = map[EventCode]string{
	Open:                        "Open",
	Accept:                      "Accept",
	Progress:                    "Progress",
	Cancel:                      "Cancel",
	Error:                       "Error",
	CleanupComplete:             "CleanupComplete",
	NewVoucher:                  "NewVoucher",
	NewVoucherResult:            "NewVoucherResult",
	PauseInitiator:              "PauseInitiator",
	ResumeInitiator:             "ResumeInitiator",
	PauseResponder:              "PauseResponder",
	ResumeResponder:             "ResumeResponder",
	FinishTransfer:              "FinishTransfer",
	ResponderBeginsFinalization: "ResponderBeginsFinalization",
	ResponderCompletes:          "ResponderCompletes",
	BeginFinalizing:             "BeginFinalizing",
	Complete:                    "Complete",
}

// Event is a struct containing information about a data transfer event
type Event struct {
	Code      EventCode // What type of event it is
	Message   string    // Any clarifying information about the event
	Timestamp time.Time // when the event happened
}

// Subscriber is a callback that is called when events are emitted
type Subscriber func(event Event, channelState ChannelState)

// Unsubscribe is a function that gets called to unsubscribe from data transfer events
type Unsubscribe func()

// RequestValidator is an interface implemented by the client of the
// data transfer module to validate requests
type RequestValidator interface {
	// ValidatePush validates a push request received from the peer that will send data
	ValidatePush(
		sender peer.ID,
		voucher Voucher,
		baseCid cid.Cid,
		selector ipld.Node) (VoucherResult, error)
	// ValidatePull validates a pull request received from the peer that will receive data
	ValidatePull(
		receiver peer.ID,
		voucher Voucher,
		baseCid cid.Cid,
		selector ipld.Node) (VoucherResult, error)
}

// ErrRetryValidation is a special error that the a revalidator can return
// for ValidatePush/ValidatePull that will not fail the request
// but send the voucher result back and await another attempt
var ErrRetryValidation = errors.New("Retry Revalidation")

// Revalidator is a request validator revalidates in progress requests
// by requesting request additional vouchers, and resuming when it receives them
type Revalidator interface {
	// Revalidate revalidates a request with a new voucher
	Revalidate(channelID ChannelID, voucher Voucher) (VoucherResult, error)
	// OnPullDataSent is called on the responder side when more bytes are sent
	// for a given pull request. It should return a VoucherResult + ErrPause to
	// request revalidation or nil to continue uninterrupted,
	// other errors will terminate the request
	OnPullDataSent(chid ChannelID, additionalBytesSent uint64) (VoucherResult, error)
	// OnPushDataReceived is called on the responder side when more bytes are received
	// for a given push request.  It should return a VoucherResult + ErrPause to
	// request revalidation or nil to continue uninterrupted,
	// other errors will terminate the request
	OnPushDataReceived(chid ChannelID, additionalBytesReceived uint64) (VoucherResult, error)
	// OnComplete is called to make a final request for revalidation -- often for the
	// purpose of settlement.
	// if VoucherResult is non nil, the request will enter a settlement phase awaiting
	// a final update
	OnComplete(chid ChannelID) (VoucherResult, error)
}

// Manager is the core interface presented by all implementations of
// of the data transfer sub system
type Manager interface {

	// Start initializes data transfer processing
	Start(ctx context.Context) error

	// Stop terminates all data transfers and ends processing
	Stop() error

	// RegisterVoucherType registers a validator for the given voucher type
	// will error if voucher type does not implement voucher
	// or if there is a voucher type registered with an identical identifier
	RegisterVoucherType(voucherType Voucher, validator RequestValidator) error

	// RegisterRevalidator registers a revalidator for the given voucher type
	// Note: this is the voucher type used to revalidate. It can share a name
	// with the initial validator type and CAN be the same type, or a different type.
	// The revalidator can simply be the sampe as the original request validator,
	// or a different validator that satisfies the revalidator interface.
	RegisterRevalidator(voucherType Voucher, revalidator Revalidator) error

	// RegisterVoucherResultType allows deserialization of a voucher result,
	// so that a listener can read the metadata
	RegisterVoucherResultType(resultType VoucherResult) error

	// open a data transfer that will send data to the recipient peer and
	// transfer parts of the piece that match the selector
	OpenPushDataChannel(ctx context.Context, to peer.ID, voucher Voucher, baseCid cid.Cid, selector ipld.Node) (ChannelID, error)

	// open a data transfer that will request data from the sending peer and
	// transfer parts of the piece that match the selector
	OpenPullDataChannel(ctx context.Context, to peer.ID, voucher Voucher, baseCid cid.Cid, selector ipld.Node) (ChannelID, error)

	// send an intermediate voucher as needed when the receiver sends a request for revalidation
	SendVoucher(ctx context.Context, chid ChannelID, voucher Voucher) error

	// close an open channel (effectively a cancel)
	CloseDataTransferChannel(ctx context.Context, chid ChannelID) error

	// pause a data transfer channel (only allowed if transport supports it)
	PauseDataTransferChannel(ctx context.Context, chid ChannelID) error

	// resume a data transfer channel (only allowed if transport supports it)
	ResumeDataTransferChannel(ctx context.Context, chid ChannelID) error

	// get status of a transfer
	TransferChannelStatus(ctx context.Context, x ChannelID) Status

	// get notified when certain types of events happen
	SubscribeToEvents(subscriber Subscriber) Unsubscribe

	// get all in progress transfers
	InProgressChannels(ctx context.Context) (map[ChannelID]ChannelState, error)
}
