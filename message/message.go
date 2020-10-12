package message

import (
	"github.com/filecoin-project/go-data-transfer/message/message1_1"
)

var NewRequest = message1_1.NewRequest
var RestartExistingChannelRequest = message1_1.RestartExistingChannelRequest
var UpdateRequest = message1_1.UpdateRequest
var VoucherRequest = message1_1.VoucherRequest
var RestartResponse = message1_1.RestartResponse
var NewResponse = message1_1.NewResponse
var VoucherResultResponse = message1_1.VoucherResultResponse
var CancelResponse = message1_1.CancelResponse
var UpdateResponse = message1_1.UpdateResponse
var FromNet = message1_1.FromNet
var CompleteResponse = message1_1.CompleteResponse
var CancelRequest = message1_1.CancelRequest
