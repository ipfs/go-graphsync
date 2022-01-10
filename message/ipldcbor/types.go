package ipldcbor

type Map struct {
	Keys   []string
	Values map[string]datamodel.Node
}
type List []datamodel.Node
type GraphSyncExtensions struct {
	Keys   []string
	Values map[string]datamodel.Node
}
type GraphSyncMetadatum struct {
	Link         datamodel.Link
	BlockPresent bool
}
type GraphSyncMetadata []GraphSyncMetadatum
type GraphSyncResponseCode string
type GraphSyncRequest struct {
	Id         int
	Root       datamodel.Link
	Extensions GraphSyncExtensions
	Priority   int
	Cancel     bool
	Update     bool
}
type GraphSyncResponse struct {
	Id         int
	Metadata   GraphSyncMetadata
	Extensions GraphSyncExtensions
}
type GraphSyncBlock struct {
	Prefix []uint8
	Data   []uint8
}
type List__GraphSyncRequest []GraphSyncRequest
type List__GraphSyncResponse []GraphSyncResponse
type List__GraphSyncBlock []GraphSyncBlock
type GraphSyncMessage struct {
	Requests  List__GraphSyncRequest
	Responses List__GraphSyncResponse
	Blocks    List__GraphSyncBlock
}
