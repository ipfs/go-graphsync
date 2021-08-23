# go-graphsync test-plan

### What This Does

This test plan measures a series of transfers between two nodes with graphsync, optionally comparing them to HTTP. It offers a wide variety of configurable parameters, which are documented here.

### File Parameters

These parameters configure the nature of the file that is transfered:

- `size` - size of file to transfer, in human-friendly form 
   - **Default**: 1MiB
- `chunk_size` - unixfs chunk size (power of 2), controls the size of the leaves in file
   - **Default**: 20 *(or 1MB chunks)*
- `links_per_level` - unixfs links per level, controlles UnixFS DAG shape (wide vs deep) 
   - **Default**: 1024
- `raw_leaves` - should unixfs leaves be raw bytes (true), or wrapped as protonodes (false)
   - **Default**: true
- `disk_store` - when we ingest the file to unix fs, should the blockstore where it lives be stored on disk (true) or memory (false)
   - **Default**: default - false
- `concurrency` - number of files to construct and attempt to transfer *simultaneously*
   - **Default**: 1

Why you might want to change these:
- obviously large file sizes more closely mirror use cases in a typical filecoin data transfer work load
- the links per level, chunk size, and raw leaves allow you to expriment with different dag structures and see how graphsync performs in different conditions
- the disk store allows you to measure the impact of datastore performance
- concurrency allows you to test how graphsync performs under heavy loads of attempting transfer many files simultaneously

### Networking Parameters

These parameters control the parameters for the network layer
- `secure_channel` - type secure encoding for the libp2p channel
   - **Default**: "noise"
- `latencies` - list of non-zero latencies to run the test under. 
   - **Default**: 100ms, 200ms, 300ms
- `no_latency_case` - also run a test case with no latency 
   - **Default**: true
- `bandwidths` - list limited bandwidths (egress bytes/s) to run the test under (written as humanized sizes). 
   - **Default**: 10M, 1M, 512kb
- `unlimited_bandwidth_case` - also run a test case with unlimited latency
   - **Default**: true

Why you might want to change these:
- we may pay a penalty for the cost of transfering over secure io
- bandwidth and latency parameters allow you to test graphsync under different network conditions. Importantly, these parameters generate a new test case for each instance, in a combinatorial form. So, if you you do two latencies and two bandwidths, you will get 4 rounds. And if concurrency is >1, each round with have more than one transfer

### Graphsync Options

The parameters control values passed constructing graphsync that may affect overall performance. Their default values are the same default values is no value is passed to the graphsync constructor

- `max_memory_per_peer` - the maximum amount of data a responder can buffer in memory for a single peer while it waits for it to be sent out over the wire
   - **Default**: 16MB
- `max_memory_total` - the maximum amount of data a responder can buffer in memory for *all peers* while it waits for it to be sent out over the wire
   - **Default**: 256MB
- `max_in_progress_requests` - The maximum number of requests Graphsync will respond to at once. When graphsync receives more than this number of simultaneous in bound requests, those after the first six (with a priotization that distributes evenly among peers) will wait for other requests to finish before they beginnin responding.
   - **Default**: 6

These performance configuration parameters in GraphSync may cause bottlenecks with their default values. For example if the `concurrency` parameter is greater than 6, the remaining files will block until graphsync finishes some of the first 6. The buffering parameters may artificially lower performance on a fast connection. In a production context, they can be adjusted upwards based on the resources and goals of the graphsync node operator

### HTTP Comparison Parameters

The parameters allow you to compare graphsync performance against transfer of the same data under similar conditions over HTTP

- `compare_http` - run the HTTP comparison test
   - **Default**: true
- `use_libp2p_http` - if we run the HTTP comparison test, should we use HTTP over libp2p (true) or just use standard HTTP on top of normal TCP/IP (false)
   - **Default**: false

### Diagnostic Parameters

These parameters control what kind of additional diagnostic data the test will generate

- `memory_snapshots` - specifies whether we should take memory snapshots as we run. Has three potention values: *none* (no snapshots), *simple* (take snapshots at the end of each request) and *detailed* (take snap shots every 10 blocks when requests are executing). Note: snapshoting will take a snapshot, then run GC, then take a snapshot again. *detailed* should not be used in any scenario where you are measuring timings
   - **Default**: none
- `block_diagnostics` - should we output detailed timings for block operations - blocks queued on the responder, blocks sent out on the network from the responder, responses received on the requestor, and blocks processed on the requestor
   - **Default**: false
