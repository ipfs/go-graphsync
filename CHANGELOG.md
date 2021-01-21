# go-graphsync changelog

# go-graphsync 0.6.0

Major code refactor for simplicity, ease of understanding

### Changelog

- github.com/ipfs/go-graphsync:
  - Merge branch 'master' into release/v0.6.0
  - move block allocation into message queue (#140) ([ipfs/go-graphsync#140](https://github.com/ipfs/go-graphsync/pull/140))
  - Response Assembler Refactor (#138) ([ipfs/go-graphsync#138](https://github.com/ipfs/go-graphsync/pull/138))
  - Add error listener on receiver (#136) ([ipfs/go-graphsync#136](https://github.com/ipfs/go-graphsync/pull/136))
  - Run testplan on in CI (#137) ([ipfs/go-graphsync#137](https://github.com/ipfs/go-graphsync/pull/137))
  - fix(responsemanager): fix network error propogation (#133) ([ipfs/go-graphsync#133](https://github.com/ipfs/go-graphsync/pull/133))
  - testground test for graphsync (#132) ([ipfs/go-graphsync#132](https://github.com/ipfs/go-graphsync/pull/132))
  - docs(CHANGELOG): update for v0.5.2 ([ipfs/go-graphsync#130](https://github.com/ipfs/go-graphsync/pull/1

### Contributors

| Contributor | Commits | Lines ± | Files Changed |
|-------------|---------|---------|---------------|
| Alex Cruikshank | 4 | +3269/-1919 | 47 |
| Hannah Howard | 3 | +777/-511 | 25 |
| hannahhoward | 1 | +34/-13 | 3 |

# go-graphsync 0.5.2

Minor release resolves bugs in notification system

### Changelog

- github.com/ipfs/go-graphsync:
  - RegisterNetworkErrorListener should fire when there's an error connecting to the peer (#127) ([ipfs/go-graphsync#127](https://github.com/ipfs/go-graphsync/pull/127))
  - Permit multiple data subscriptions per original topic (#128) ([ipfs/go-graphsync#128](https://github.com/ipfs/go-graphsync/pull/128))
  - release: v0.5.1 (#123) ([ipfs/go-graphsync#123](https://github.com/ipfs/go-graphsync/pull/123))

### Contributors

| Contributor | Commits | Lines ± | Files Changed |
|-------------|---------|---------|---------------|
| dirkmc | 2 | +272/-185 | 10 |
| Alex Cruikshank | 1 | +188/-110 | 12 |
| Hannah Howard | 1 | +23/-6 | 3 |

# go-graphsync 0.5.1

### Changelog

- github.com/ipfs/go-graphsync:
  - feat(responsemanager): allow configuration of max requests (#122) ([ipfs/go-graphsync#122](https://github.com/ipfs/go-graphsync/pull/122))

Contributors

| Contributor | Commits | Lines ± | Files Changed |
|-------------|---------|---------|---------------|
| Hannah Howard | 1 | +23/-6 | 3 |

# go-graphsync 0.4.3

Update libp2p to 0.12. This libp2p release includes a breaking change to the libp2p stream interfaces.

### Changelog

- github.com/ipfs/go-graphsync:
  - feat: use go-libp2p-core 0.7.0 stream interfaces (#116) ([ipfs/go-graphsync#116](https://github.com/ipfs/go-graphsync/pull/116))

Contributors

| Contributor  | Commits | Lines ±  | Files Changed |
|--------------|---------|----------|---------------|
| Steven Allen |       1 | +195/-24 |             3 |

# go-graphsync 0.4.3

Minor fixes and patches

### Changelog

- github.com/ipfs/go-graphsync:
  - chore(benchmarks): remove extra files
  - fix(peerresponsemanager): avoid race condition that could result in NPE in link tracker (#118) ([ipfs/go-graphsync#118](https://github.com/ipfs/go-graphsync/pull/118))
  - docs(CHANGELOG): update for 0.4.2 ([ipfs/go-graphsync#117](https://github.com/ipfs/go-graphsync/pull/117))
  - feat(memory): improve memory usage (#110) ([ipfs/go-graphsync#110](https://github.com/ipfs/go-graphsync/pull/110))

### Contributors

| Contributor | Commits | Lines ± | Files Changed |
|-------------|---------|---------|---------------|
| Hannah Howard | 2 | +49/-6 | 7 |
| hannahhoward | 1 | +0/-0 | 2 |

# go-graphsync 0.4.2

bug fix for 0.4.1

### Changelog

- github.com/ipfs/go-graphsync:
  - fix(notifications): fix lock in close (#115) ([ipfs/go-graphsync#115](https://github.com/ipfs/go-graphsync/pull/115))
  - docs(CHANGELOG): update for v0.4.1 ([ipfs/go-graphsync#114](https://github.com/ipfs/go-graphsync/pull/114))

### Contributors

| Contributor | Commits | Lines ± | Files Changed |
|-------------|---------|---------|---------------|
| Hannah Howard | 1 | +7/-0 | 1 |

# go-graphsync 0.4.1

critical bug fix for 0.4.0

### Changelog

- github.com/ipfs/go-graphsync:
  - fix(allocator): remove peer from peer status list
  - docs(CHANGELOG): update for v0.4.0

### Contributors

| Contributor | Commits | Lines ± | Files Changed |
|-------------|---------|---------|---------------|
| hannahhoward | 2 | +23/-2 | 3 |

# go-graphsync 0.4.0

Feature release - add memory backpressure to responses to minimize extra memory usage

### Changelog

- github.com/ipfs/go-graphsync:
  - docs(CHANGELOG): update for 0.3.1 ([ipfs/go-graphsync#112](https://github.com/ipfs/go-graphsync/pull/112))
  - Update ipld-prime (#111) ([ipfs/go-graphsync#111](https://github.com/ipfs/go-graphsync/pull/111))
  - Add allocator for memory backpressure (#108) ([ipfs/go-graphsync#108](https://github.com/ipfs/go-graphsync/pull/108))
  - Shutdown notifications go routines (#109) ([ipfs/go-graphsync#109](https://github.com/ipfs/go-graphsync/pull/109))

### Contributors

| Contributor | Commits | Lines ± | Files Changed |
|-------------|---------|---------|---------------|
| Hannah Howard | 3 | +724/-83 | 18 |


# go-graphsync 0.3.1

Security fix -- switch to google protobufs

### Changelog

- github.com/ipfs/go-graphsync:
  - Switch to google protobuf generator (#105) ([ipfs/go-graphsync#105](https://github.com/ipfs/go-graphsync/pull/105))
  - feat(CHANGELOG): update for 0.3.0 ([ipfs/go-graphsync#104](https://github.com/ipfs/go-graphsync/pull/104))

### Contributors

| Contributor | Commits | Lines ± | Files Changed |
|-------------|---------|---------|---------------|
| Hannah Howard | 1 | +472/-1553 | 8 |

# go-graphsync 0.3.0

Significant updates allow for:
- completed response hooks run when response is done going over wire (or at least transmitted)
- listening for when blocks are actually sent
- being notified of network send errors on responder

### Changelog

- github.com/ipfs/go-graphsync:
  - docs(CHANGELOG): update for 0.2.1 ([ipfs/go-graphsync#103](https://github.com/ipfs/go-graphsync/pull/103))
  - Track actual network operations in a response (#102) ([ipfs/go-graphsync#102](https://github.com/ipfs/go-graphsync/pull/102))
  - feat(responsecache): prune blocks more intelligently (#101) ([ipfs/go-graphsync#101](https://github.com/ipfs/go-graphsync/pull/101))

### Contributors

| Contributor | Commits | Lines ± | Files Changed |
|-------------|---------|---------|---------------|
| Hannah Howard | 2 | +1983/-927 | 29 |

# go-graphsync 0.2.1

Compatibility fix for 0.2.0

### Changelog

- github.com/ipfs/go-graphsync:
  - fix(metadata): fix cbor-gen (#98) ([ipfs/go-graphsync#98](https://github.com/ipfs/go-graphsync/pull/98))

### Contributors

| Contributor | Commits | Lines ± | Files Changed |
|-------------|---------|---------|---------------|
| Hannah Howard | 1 | +12/-16 | 3 |

# go-graphsync 0.2.0

Update to IPLD prime + several optimizations for performance

### Changelog

- github.com/ipfs/go-graphsync:
  - style(imports): fix imports
  - fix(selectorvalidator): memory optimization (#97) ([ipfs/go-graphsync#97](https://github.com/ipfs/go-graphsync/pull/97))
  - Update go-ipld-prime@v0.5.0 (#92) ([ipfs/go-graphsync#92](https://github.com/ipfs/go-graphsync/pull/92))
  - refactor(metadata): use cbor-gen encoding (#96) ([ipfs/go-graphsync#96](https://github.com/ipfs/go-graphsync/pull/96))
  - Release/v0.1.2 ([ipfs/go-graphsync#95](https://github.com/ipfs/go-graphsync/pull/95))
  - Return Request context cancelled error (#93) ([ipfs/go-graphsync#93](https://github.com/ipfs/go-graphsync/pull/93))
  - feat(benchmarks): add p2p stress test (#91) ([ipfs/go-graphsync#91](https://github.com/ipfs/go-graphsync/pull/91))
- github.com/hannahhoward/cbor-gen-for (null -> v0.0.0-20200817222906-ea96cece81f1):
  - add flag to select map encoding ([hannahhoward/cbor-gen-for#1](https://github.com/hannahhoward/cbor-gen-for/pull/1))

### Contributors

| Contributor | Commits | Lines ± | Files Changed |
|-------------|---------|---------|---------------|
| Eric Myhre | 1 | +2919/-121 | 39 |
| Hannah Howard | 3 | +412/-103 | 15 |
| hannahhoward | 1 | +31/-31 | 7 |
| whyrusleeping | 1 | +31/-18 | 2 |
| Aarsh Shah | 1 | +27/-1 | 3 |

# go-graphsync 0.1.2

Minor release with initial benchmarks

### Changelog

- github.com/ipfs/go-graphsync:
  - Benchmark framework + First memory fixes (#89) ([ipfs/go-graphsync#89](https://github.com/ipfs/go-graphsync/pull/89))
  - docs(CHANGELOG): update for v0.1.1 ([ipfs/go-graphsync#85](https://github.com/ipfs/go-graphsync/pull/85))

### Contributors

| Contributor | Commits | Lines ± | Files Changed |
|-------------|---------|---------|---------------|
| Hannah Howard | 1 | +1055/-39 | 17 |

# go-graphsync 0.1.1

Minor fix for alternate persistence stores and deduplication

### Changelog

- github.com/ipfs/go-graphsync:
  - docs(CHANGELOG): update for v0.1.0 release ([ipfs/go-graphsync#84](https://github.com/ipfs/go-graphsync/pull/84))
  - Dedup by key extension (#83) ([ipfs/go-graphsync#83](https://github.com/ipfs/go-graphsync/pull/83))

### Contributors

| Contributor | Commits | Lines ± | Files Changed |
|-------------|---------|---------|---------------|
| Hannah Howard | 1 | +316/-7 | 10 |

# go-graphsync v0.1.0

Major release (we fell behind on creating tagged releases for a while) -- many augmentations to hooks, authorization, persistence, request execution.

### Changelog

- github.com/ipfs/go-graphsync:
  - style(imports): fix import formatting
  - feat(persistenceoptions): add unregister ability (#80) ([ipfs/go-graphsync#80](https://github.com/ipfs/go-graphsync/pull/80))
  - fix(message): regen protobuf code (#79) ([ipfs/go-graphsync#79](https://github.com/ipfs/go-graphsync/pull/79))
  - feat(requestmanager): run response hooks on completed requests (#77) ([ipfs/go-graphsync#77](https://github.com/ipfs/go-graphsync/pull/77))
  - Revert "add extensions on complete (#76)"
  - add extensions on complete (#76) ([ipfs/go-graphsync#76](https://github.com/ipfs/go-graphsync/pull/76))
  - All changes to date including pause requests & start paused, along with new adds for cleanups and checking of execution (#75) ([ipfs/go-graphsync#75](https://github.com/ipfs/go-graphsync/pull/75))
  - More fine grained response controls (#71) ([ipfs/go-graphsync#71](https://github.com/ipfs/go-graphsync/pull/71))
  - Refactor request execution and use IPLD SkipMe functionality for proper partial results on a request (#70) ([ipfs/go-graphsync#70](https://github.com/ipfs/go-graphsync/pull/70))
  - feat(graphsync): implement do-no-send-cids extension (#69) ([ipfs/go-graphsync#69](https://github.com/ipfs/go-graphsync/pull/69))
  - Incoming Block Hooks (#68) ([ipfs/go-graphsync#68](https://github.com/ipfs/go-graphsync/pull/68))
  - fix(responsemanager): add nil check (#67) ([ipfs/go-graphsync#67](https://github.com/ipfs/go-graphsync/pull/67))
  - Add autocomment configuration
  - refactor(hooks): use external pubsub (#65) ([ipfs/go-graphsync#65](https://github.com/ipfs/go-graphsync/pull/65))
  - Update of IPLD Prime (#66) ([ipfs/go-graphsync#66](https://github.com/ipfs/go-graphsync/pull/66))
  - Add standard issue template
  - feat(responsemanager): add listener for completed responses (#64) ([ipfs/go-graphsync#64](https://github.com/ipfs/go-graphsync/pull/64))
  - Update Requests (#63) ([ipfs/go-graphsync#63](https://github.com/ipfs/go-graphsync/pull/63))
  - Add pausing and unpausing of requests (#62) ([ipfs/go-graphsync#62](https://github.com/ipfs/go-graphsync/pull/62))
  - ci(circle): remove benchmark task for now
  - ci(circle): update orb
  - Outgoing Request Hooks, swapping persistence layers (#61) ([ipfs/go-graphsync#61](https://github.com/ipfs/go-graphsync/pull/61))
  - Feat/request hook loader chooser (#60) ([ipfs/go-graphsync#60](https://github.com/ipfs/go-graphsync/pull/60))
  - Option to Reject requests by default (#58) ([ipfs/go-graphsync#58](https://github.com/ipfs/go-graphsync/pull/58))
  - Testify refactor (#56) ([ipfs/go-graphsync#56](https://github.com/ipfs/go-graphsync/pull/56))
  - Switch To Circle CI (#57) ([ipfs/go-graphsync#57](https://github.com/ipfs/go-graphsync/pull/57))
  - fix(deps): go mod tidy
  - docs(README): remove ipldbridge reference
  - Tech Debt: Remove IPLD Bridge ([ipfs/go-graphsync#55](https://github.com/ipfs/go-graphsync/pull/55))

### Contributors

| Contributor | Commits | Lines ± | Files Changed |
|-------------|---------|---------|---------------|
| Hannah Howard | 20 | +13273/-7718 | 262 |
| hannahhoward | 13 | +1663/-1906 | 184 |
| Hector Sanjuan | 2 | +95/-0 | 3 |

# go-graphsync v0.0.5

Minor release -- update task queue and add some documentation

### Changelog

- github.com/ipfs/go-graphsync:
  - feat: update the peer task queue ([ipfs/go-graphsync#54](https://github.com/ipfs/go-graphsync/pull/54))
  - docs(readme): document the storeutil package in the readme ([ipfs/go-graphsync#52](https://github.com/ipfs/go-graphsync/pull/52))

### Contributors

| Contributor | Commits | Lines ± | Files Changed |
|-------------|---------|---------|---------------|
| Steven Allen | 2 | +68/-49 | 5 |

# go-graphsync 0.0.4

Initial release to incorporate into go-data-transfer module.

Implements request authorization, request hooks, default valdiation policy, etc

### Changelog

- github.com/ipfs/go-graphsync:
  - Add DAG Protobuf Support ([ipfs/go-graphsync#51](https://github.com/ipfs/go-graphsync/pull/51))
  - Add response hooks ([ipfs/go-graphsync#50](https://github.com/ipfs/go-graphsync/pull/50))
  - Request hooks ([ipfs/go-graphsync#49](https://github.com/ipfs/go-graphsync/pull/49))
  - Add a default validation policy ([ipfs/go-graphsync#48](https://github.com/ipfs/go-graphsync/pull/48))
  - Send user extensions in request ([ipfs/go-graphsync#47](https://github.com/ipfs/go-graphsync/pull/47))
  - Revert "Merge pull request #44 from ipfs/chore/update-peertaskqueue"
  - Update peertaskqueue ([ipfs/go-graphsync#44](https://github.com/ipfs/go-graphsync/pull/44))
  - Refactor file organization ([ipfs/go-graphsync#43](https://github.com/ipfs/go-graphsync/pull/43))
  - feat(graphsync): support extension protocol ([ipfs/go-graphsync#42](https://github.com/ipfs/go-graphsync/pull/42))
  - Bump go-ipld-prime to 092ea9a7696d ([ipfs/go-graphsync#41](https://github.com/ipfs/go-graphsync/pull/41))
  - Fix some typo ([ipfs/go-graphsync#40](https://github.com/ipfs/go-graphsync/pull/40))

### Contributors

| Contributor | Commits | Lines ± | Files Changed |
|-------------|---------|---------|---------------|
| hannahhoward | 12 | +3040/-1516 | 103 |
| Hannah Howard | 2 | +253/-321 | 3 |
| Dirk McCormick | 1 | +47/-33 | 4 |
| Edgar Lee | 1 | +36/-20 | 8 |
| Alexey | 1 | +15/-15 | 1 |

# go-graphsync 0.0.3

Bug fix release. Fix issues issues with message queue.

### Changelog

- github.com/ipfs/go-graphsync:
  - fix(messagequeue): no retry after queue shutdown ([ipfs/go-graphsync#38](https://github.com/ipfs/go-graphsync/pull/38))

### Contributors

| Contributor | Commits | Lines ± | Files Changed |
|-------------|---------|---------|---------------|
| hannahhoward | 1 | +70/-1 | 2 |

# go-graphsync 0.0.2

Bug fix release. Fix message sizes to not overflow limits.

### Changelog

- github.com/ipfs/go-graphsync:
  - Limit Response Size ([ipfs/go-graphsync#37](https://github.com/ipfs/go-graphsync/pull/37))

### Contributors

| Contributor | Commits | Lines ± | Files Changed |
|-------------|---------|---------|---------------|
| hannahhoward | 2 | +295/-52 | 5 |

# go-graphysnc 0.0.1-filecoin

Initial tagged release for early version of filecoin

### Changelog

Initial feature set including parallel requests, selectors, basic architecture,
etc. -- changelog not tracked due to lack of go.mod

### 🙌🏽 Want to contribute?

Would you like to contribute to this repo and don’t know how? Here are a few places you can get started:

- Check out the [Contributing Guidelines](https://github.com/ipfs/go-graphsync/blob/master/CONTRIBUTING.md)
- Look for issues with the `good-first-issue` label in [go-graphsync](https://github.com/ipfs/go-graphsync/issues?utf8=%E2%9C%93&q=is%3Aissue+is%3Aopen+label%3A%22e-good-first-issue%22+)
