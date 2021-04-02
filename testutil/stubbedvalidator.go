package testutil

import (
	"errors"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/stretchr/testify/require"

	datatransfer "github.com/filecoin-project/go-data-transfer"
)

// NewStubbedValidator returns a new instance of a stubbed validator
func NewStubbedValidator() *StubbedValidator {
	return &StubbedValidator{}
}

// ValidatePush returns a stubbed result for a push validation
func (sv *StubbedValidator) ValidatePush(
	sender peer.ID,
	voucher datatransfer.Voucher,
	baseCid cid.Cid,
	selector ipld.Node) (datatransfer.VoucherResult, error) {
	sv.didPush = true
	sv.ValidationsReceived = append(sv.ValidationsReceived, ReceivedValidation{false, sender, voucher, baseCid, selector})
	return sv.result, sv.pushError
}

// ValidatePull returns a stubbed result for a pull validation
func (sv *StubbedValidator) ValidatePull(
	receiver peer.ID,
	voucher datatransfer.Voucher,
	baseCid cid.Cid,
	selector ipld.Node) (datatransfer.VoucherResult, error) {
	sv.didPull = true
	sv.ValidationsReceived = append(sv.ValidationsReceived, ReceivedValidation{true, receiver, voucher, baseCid, selector})
	return sv.result, sv.pullError
}

// StubResult returns thes given voucher result when a validate call is made
func (sv *StubbedValidator) StubResult(voucherResult datatransfer.VoucherResult) {
	sv.result = voucherResult
}

// StubErrorPush sets ValidatePush to error
func (sv *StubbedValidator) StubErrorPush() {
	sv.pushError = errors.New("something went wrong")
}

// StubSuccessPush sets ValidatePush to succeed
func (sv *StubbedValidator) StubSuccessPush() {
	sv.pushError = nil
}

// StubPausePush sets ValidatePush to pause
func (sv *StubbedValidator) StubPausePush() {
	sv.pushError = datatransfer.ErrPause
}

// ExpectErrorPush expects ValidatePush to error
func (sv *StubbedValidator) ExpectErrorPush() {
	sv.expectPush = true
	sv.StubErrorPush()
}

// ExpectSuccessPush expects ValidatePush to error
func (sv *StubbedValidator) ExpectSuccessPush() {
	sv.expectPush = true
	sv.StubSuccessPush()
}

// ExpectPausePush expects ValidatePush to pause
func (sv *StubbedValidator) ExpectPausePush() {
	sv.expectPush = true
	sv.StubPausePush()
}

// StubErrorPull sets ValidatePull to error
func (sv *StubbedValidator) StubErrorPull() {
	sv.pullError = errors.New("something went wrong")
}

// StubSuccessPull sets ValidatePull to succeed
func (sv *StubbedValidator) StubSuccessPull() {
	sv.pullError = nil
}

// StubPausePull sets ValidatePull to pause
func (sv *StubbedValidator) StubPausePull() {
	sv.pullError = datatransfer.ErrPause
}

// ExpectErrorPull expects ValidatePull to error
func (sv *StubbedValidator) ExpectErrorPull() {
	sv.expectPull = true
	sv.StubErrorPull()
}

// ExpectSuccessPull expects ValidatePull to error
func (sv *StubbedValidator) ExpectSuccessPull() {
	sv.expectPull = true
	sv.StubSuccessPull()
}

// ExpectPausePull expects ValidatePull to pause
func (sv *StubbedValidator) ExpectPausePull() {
	sv.expectPull = true
	sv.StubPausePull()
}

// VerifyExpectations verifies the specified calls were made
func (sv *StubbedValidator) VerifyExpectations(t *testing.T) {
	if sv.expectPush {
		require.True(t, sv.didPush)
	}
	if sv.expectPull {
		require.True(t, sv.didPull)
	}
}

// ReceivedValidation records a call to either ValidatePush or ValidatePull
type ReceivedValidation struct {
	IsPull   bool
	Other    peer.ID
	Voucher  datatransfer.Voucher
	BaseCid  cid.Cid
	Selector ipld.Node
}

// StubbedValidator is a validator that returns predictable results
type StubbedValidator struct {
	result              datatransfer.VoucherResult
	didPush             bool
	didPull             bool
	expectPush          bool
	expectPull          bool
	pushError           error
	pullError           error
	ValidationsReceived []ReceivedValidation
}

// StubbedRevalidator is a revalidator that returns predictable results
type StubbedRevalidator struct {
	revalidationResult datatransfer.VoucherResult
	checkResult        datatransfer.VoucherResult
	didRevalidate      bool
	didPushCheck       bool
	didPullCheck       bool
	didComplete        bool
	expectRevalidate   bool
	expectPushCheck    bool
	expectPullCheck    bool
	expectComplete     bool
	revalidationError  error
	pushCheckError     error
	pullCheckError     error
	completeError      error
}

// NewStubbedRevalidator returns a new instance of a stubbed revalidator
func NewStubbedRevalidator() *StubbedRevalidator {
	return &StubbedRevalidator{}
}

// OnPullDataSent returns a stubbed result for checking when pull data is sent
func (srv *StubbedRevalidator) OnPullDataSent(chid datatransfer.ChannelID, additionalBytesSent uint64) (bool, datatransfer.VoucherResult, error) {
	srv.didPullCheck = true
	return srv.expectPullCheck, srv.revalidationResult, srv.pullCheckError
}

// OnPushDataReceived returns a stubbed result for checking when push data is received
func (srv *StubbedRevalidator) OnPushDataReceived(chid datatransfer.ChannelID, additionalBytesReceived uint64) (bool, datatransfer.VoucherResult, error) {
	srv.didPushCheck = true
	return srv.expectPushCheck, srv.revalidationResult, srv.pushCheckError
}

// OnComplete returns a stubbed result for checking when the requests completes
func (srv *StubbedRevalidator) OnComplete(chid datatransfer.ChannelID) (bool, datatransfer.VoucherResult, error) {
	srv.didComplete = true
	return srv.expectComplete, srv.revalidationResult, srv.completeError
}

// Revalidate returns a stubbed result for revalidating a request
func (srv *StubbedRevalidator) Revalidate(chid datatransfer.ChannelID, voucher datatransfer.Voucher) (datatransfer.VoucherResult, error) {
	srv.didRevalidate = true
	return srv.checkResult, srv.revalidationError
}

// StubRevalidationResult returns the given voucher result when a call is made to Revalidate
func (srv *StubbedRevalidator) StubRevalidationResult(voucherResult datatransfer.VoucherResult) {
	srv.revalidationResult = voucherResult
}

// StubCheckResult returns the given voucher result when a call is made to
// OnPullDataSent, OnPushDataReceived, or OnComplete
func (srv *StubbedRevalidator) StubCheckResult(voucherResult datatransfer.VoucherResult) {
	srv.checkResult = voucherResult
}

// StubErrorPushCheck sets OnPushDataReceived to error
func (srv *StubbedRevalidator) StubErrorPushCheck() {
	srv.pushCheckError = errors.New("something went wrong")
}

// StubSuccessPushCheck sets OnPushDataReceived to succeed
func (srv *StubbedRevalidator) StubSuccessPushCheck() {
	srv.pushCheckError = nil
}

// StubPausePushCheck sets OnPushDataReceived to pause
func (srv *StubbedRevalidator) StubPausePushCheck() {
	srv.pushCheckError = datatransfer.ErrPause
}

// ExpectErrorPushCheck expects OnPushDataReceived to error
func (srv *StubbedRevalidator) ExpectErrorPushCheck() {
	srv.expectPushCheck = true
	srv.StubErrorPushCheck()
}

// ExpectSuccessPushCheck expects OnPushDataReceived to succeed
func (srv *StubbedRevalidator) ExpectSuccessPushCheck() {
	srv.expectPushCheck = true
	srv.StubSuccessPushCheck()
}

// ExpectPausePushCheck expects OnPushDataReceived to pause
func (srv *StubbedRevalidator) ExpectPausePushCheck() {
	srv.expectPushCheck = true
	srv.StubPausePushCheck()
}

// StubErrorPullCheck sets OnPullDataSent to error
func (srv *StubbedRevalidator) StubErrorPullCheck() {
	srv.pullCheckError = errors.New("something went wrong")
}

// StubSuccessPullCheck sets OnPullDataSent to succeed
func (srv *StubbedRevalidator) StubSuccessPullCheck() {
	srv.pullCheckError = nil
}

// StubPausePullCheck sets OnPullDataSent to pause
func (srv *StubbedRevalidator) StubPausePullCheck() {
	srv.pullCheckError = datatransfer.ErrPause
}

// ExpectErrorPullCheck expects OnPullDataSent to error
func (srv *StubbedRevalidator) ExpectErrorPullCheck() {
	srv.expectPullCheck = true
	srv.StubErrorPullCheck()
}

// ExpectSuccessPullCheck expects OnPullDataSent to succeed
func (srv *StubbedRevalidator) ExpectSuccessPullCheck() {
	srv.expectPullCheck = true
	srv.StubSuccessPullCheck()
}

// ExpectPausePullCheck expects OnPullDataSent to pause
func (srv *StubbedRevalidator) ExpectPausePullCheck() {
	srv.expectPullCheck = true
	srv.StubPausePullCheck()
}

// StubErrorComplete sets OnComplete to error
func (srv *StubbedRevalidator) StubErrorComplete() {
	srv.completeError = errors.New("something went wrong")
}

// StubSuccessComplete sets OnComplete to succeed
func (srv *StubbedRevalidator) StubSuccessComplete() {
	srv.completeError = nil
}

// StubPauseComplete sets OnComplete to pause
func (srv *StubbedRevalidator) StubPauseComplete() {
	srv.completeError = datatransfer.ErrPause
}

// ExpectErrorComplete expects OnComplete to error
func (srv *StubbedRevalidator) ExpectErrorComplete() {
	srv.expectComplete = true
	srv.StubErrorComplete()
}

// ExpectSuccessComplete expects OnComplete to succeed
func (srv *StubbedRevalidator) ExpectSuccessComplete() {
	srv.expectComplete = true
	srv.StubSuccessComplete()
}

// ExpectPauseComplete expects OnComplete to pause
func (srv *StubbedRevalidator) ExpectPauseComplete() {
	srv.expectComplete = true
	srv.StubPauseComplete()
}

// StubErrorRevalidation sets Revalidate to error
func (srv *StubbedRevalidator) StubErrorRevalidation() {
	srv.revalidationError = errors.New("something went wrong")
}

// StubSuccessRevalidation sets Revalidate to succeed
func (srv *StubbedRevalidator) StubSuccessRevalidation() {
	srv.revalidationError = nil
}

// StubPauseRevalidation sets Revalidate to pause
func (srv *StubbedRevalidator) StubPauseRevalidation() {
	srv.revalidationError = datatransfer.ErrPause
}

// ExpectSuccessErrResume configures Revalidate to return an ErrResume
// and expect a Revalidate call.
func (srv *StubbedRevalidator) ExpectSuccessErrResume() {
	srv.expectRevalidate = true
	srv.revalidationError = datatransfer.ErrResume
}

// ExpectErrorRevalidation expects Revalidate to error
func (srv *StubbedRevalidator) ExpectErrorRevalidation() {
	srv.expectRevalidate = true
	srv.StubErrorRevalidation()
}

// ExpectSuccessRevalidation expects Revalidate to succeed
func (srv *StubbedRevalidator) ExpectSuccessRevalidation() {
	srv.expectRevalidate = true
	srv.StubSuccessRevalidation()
}

// ExpectPauseRevalidation expects Revalidate to pause
func (srv *StubbedRevalidator) ExpectPauseRevalidation() {
	srv.expectRevalidate = true
	srv.StubPauseRevalidation()
}

// VerifyExpectations verifies the specified calls were made
func (srv *StubbedRevalidator) VerifyExpectations(t *testing.T) {
	if srv.expectRevalidate {
		require.True(t, srv.didRevalidate)
	}
	if srv.expectPushCheck {
		require.True(t, srv.didPushCheck)
	}
	if srv.expectPullCheck {
		require.True(t, srv.didPullCheck)
	}
	if srv.expectComplete {
		require.True(t, srv.didComplete)
	}
}
