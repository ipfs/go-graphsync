package types

// AsyncLoadResult is sent once over the channel returned by an async load.
type AsyncLoadResult struct {
	Data []byte
	Err  error
}
