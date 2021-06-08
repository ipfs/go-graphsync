# go-data-transfer changelog

# go-data-transfer 1.6.1

Remove performance bottleneck with CID lists.

- github.com/filecoin-project/go-data-transfer:
  - Remove CID lists (#217) ([filecoin-project/go-data-transfer#217](https://github.com/filecoin-project/go-data-transfer/pull/217))
  - feat: use different extension names to fit multiple hooks data in same graphsync message (#204) ([filecoin-project/go-data-transfer#204](https://github.com/filecoin-project/go-data-transfer/pull/204))
  - fix: map race in GS transport (#208) ([filecoin-project/go-data-transfer#208](https://github.com/filecoin-project/go-data-transfer/pull/208))
  - refactor: simplify graphsync transport (#203) ([filecoin-project/go-data-transfer#203](https://github.com/filecoin-project/go-data-transfer/pull/203))

Contributors

| Contributor | Commits | Lines ± | Files Changed |
|-------------|---------|---------|---------------|
| dirkmc | 5 | +1134/-707 | 24 |
| tchardin | 1 | +261/-33 | 4 |
| Dirk McCormick | 13 | +193/-2 | 13 |
| hannahhoward | 1 | +17/-0 | 1 |

# go-data-transfer 1.6.0

- github.com/filecoin-project/go-data-transfer:
  - fix: option to disable accept and complete timeouts
  - fix: disable restart ack timeout

Contributors

| Contributor | Commits | Lines ± | Files Changed |
|-------------|---------|---------|---------------|
| Dirk McCormick | 2 | +41/-105 | 5 |

# go-data-transfer 1.5.0

Support the data transfer being restarted.

- github.com/filecoin-project/go-data-transfer:
  - Add isRestart param to validators (#197) ([filecoin-project/go-data-transfer#197](https://github.com/filecoin-project/go-data-transfer/pull/197))
  - fix: flaky TestChannelMonitorAutoRestart (#198) ([filecoin-project/go-data-transfer#198](https://github.com/filecoin-project/go-data-transfer/pull/198))
  - Channel monitor watches for errors instead of measuring data rate (#190) ([filecoin-project/go-data-transfer#190](https://github.com/filecoin-project/go-data-transfer/pull/190))
  - fix: prevent concurrent restarts for same channel (#195) ([filecoin-project/go-data-transfer#195](https://github.com/filecoin-project/go-data-transfer/pull/195))
  - fix: channel state machine event handling (#194) ([filecoin-project/go-data-transfer#194](https://github.com/filecoin-project/go-data-transfer/pull/194))
  - Dont double count data sent (#185) ([filecoin-project/go-data-transfer#185](https://github.com/filecoin-project/go-data-transfer/pull/185))
- github.com/ipfs/go-graphsync (v0.6.0 -> v0.6.1):
  - feat: fire network error when network disconnects during request (#164) ([ipfs/go-graphsync#164](https://github.com/ipfs/go-graphsync/pull/164))

Contributors

| Contributor | Commits | Lines ± | Files Changed |
|-------------|---------|---------|---------------|
| dirkmc | 8 | +1235/-868 | 37 |
| Dirk McCormick | 1 | +11/-0 | 1 |

# go-data-transfer 1.4.3

- github.com/filecoin-project/go-data-transfer:
  - fix: dont throw error from cancel event if state machine already terminated (#188) ([filecoin-project/go-data-transfer#188](https://github.com/filecoin-project/go-data-transfer/pull/188))

Contributors

| Contributor | Commits | Lines ± | Files Changed |
|-------------|---------|---------|---------------|
| dirkmc | 2 | +23/-1 | 2 |

# go-data-transfer 1.4.2

- github.com/filecoin-project/go-data-transfer:
  - Support no-op error responses  (#186) ([filecoin-project/go-data-transfer#186](https://github.com/filecoin-project/go-data-transfer/pull/186))
  - fix: fail a pull channel when there is a timeout receiving the Complete message (#179) ([filecoin-project/go-data-transfer#179](https://github.com/filecoin-project/go-data-transfer/pull/179))

Contributors

| Contributor | Commits | Lines ± | Files Changed |
|-------------|---------|---------|---------------|
| Aarsh Shah | 1 | +23/-122 | 6 |
| dirkmc | 2 | +65/-21 | 3 |

# go-data-transfer 1.4.1

- github.com/filecoin-project/go-data-transfer:
  - Add ChannelStages to keep track of history of lifecycle of a DataTransfer (#163) ([filecoin-project/go-data-transfer#163](https://github.com/filecoin-project/go-data-transfer/pull/163))

Contributors

| Contributor | Commits | Lines ± | Files Changed |
|-------------|---------|---------|---------------|
| Anton Evangelatov | 1 | +572/-28 | 8 |

# go-data-transfer 1.4.0

- github.com/filecoin-project/go-data-transfer:
  - feat: add config options to enable / disable push or pull monitoring individually (#174) ([filecoin-project/go-data-transfer#174](https://github.com/filecoin-project/go-data-transfer/pull/174))
  - fix: ensure channel monitor shuts down when transfer complete (#171) ([filecoin-project/go-data-transfer#171](https://github.com/filecoin-project/go-data-transfer/pull/171))

Contributors

| Contributor | Commits | Lines ± | Files Changed |
|-------------|---------|---------|---------------|
| dirkmc | 3 | +46/-14 | 5 |

# go-data-transfer 1.3.0

- github.com/filecoin-project/go-data-transfer:
  - feat: use random number instead of incrementing counter for transfer ID (#169) ([filecoin-project/go-data-transfer#169](https://github.com/filecoin-project/go-data-transfer/pull/169))

Contributors

| Contributor | Commits | Lines ± | Files Changed |
|-------------|---------|---------|---------------|
| dirkmc | 2 | +126/-71 | 13 |

# go-data-transfer 1.2.9

- github.com/filecoin-project/go-data-transfer:
  - fix: log line in pull data channel monitor (#165) ([filecoin-project/go-data-transfer#165](https://github.com/filecoin-project/go-data-transfer/pull/165))
  - feat: better reconnect behaviour (#162) ([filecoin-project/go-data-transfer#162](https://github.com/filecoin-project/go-data-transfer/pull/162))
  - Improve push channel to detect when not all data has been received (#157) ([filecoin-project/go-data-transfer#157](https://github.com/filecoin-project/go-data-transfer/pull/157))
  - fix: flaky TestSimulatedRetrievalFlow (#159) ([filecoin-project/go-data-transfer#159](https://github.com/filecoin-project/go-data-transfer/pull/159))
  - feat: better logging (#155) ([filecoin-project/go-data-transfer#155](https://github.com/filecoin-project/go-data-transfer/pull/155))
  - fix: add missing event names (#148) ([filecoin-project/go-data-transfer#148](https://github.com/filecoin-project/go-data-transfer/pull/148))
  - enable codecov. (#146) ([filecoin-project/go-data-transfer#146](https://github.com/filecoin-project/go-data-transfer/pull/146))
  - Better error message on complete (#145) ([filecoin-project/go-data-transfer#145](https://github.com/filecoin-project/go-data-transfer/pull/145))

Contributors

| Contributor | Commits | Lines ± | Files Changed |
|-------------|---------|---------|---------------|
| dirkmc | 8 | +1492/-750 | 34 |
| raulk | 1 | +2/-2 | 1 |

# go-data-transfer 1.2.8

- github.com/filecoin-project/go-data-transfer:
  - test: check total blocks sent when theres a restart (#140) ([filecoin-project/go-data-transfer#140](https://github.com/filecoin-project/go-data-transfer/pull/140))
  - feat(deps): update to go-graphsync v0.6.0 (#139) ([filecoin-project/go-data-transfer#139](https://github.com/filecoin-project/go-data-transfer/pull/139))
- github.com/ipfs/go-graphsync (v0.5.2 -> v0.6.0):
  - move block allocation into message queue (#140) ([ipfs/go-graphsync#140](https://github.com/ipfs/go-graphsync/pull/140))
  - Response Assembler Refactor (#138) ([ipfs/go-graphsync#138](https://github.com/ipfs/go-graphsync/pull/138))
  - Add error listener on receiver (#136) ([ipfs/go-graphsync#136](https://github.com/ipfs/go-graphsync/pull/136))
  - Run testplan on in CI (#137) ([ipfs/go-graphsync#137](https://github.com/ipfs/go-graphsync/pull/137))
  - fix(responsemanager): fix network error propogation (#133) ([ipfs/go-graphsync#133](https://github.com/ipfs/go-graphsync/pull/133))
  - testground test for graphsync (#132) ([ipfs/go-graphsync#132](https://github.com/ipfs/go-graphsync/pull/132))

Contributors

| Contributor | Commits | Lines ± | Files Changed |
|-------------|---------|---------|---------------|
| Alex Cruikshank | 4 | +3135/-1785 | 46 |
| Hannah Howard | 4 | +671/-386 | 28 |
| dirkmc | 2 | +369/-81 | 12 |
| hannahhoward | 2 | +38/-15 | 4 |

# go-data-transfer 1.2.7

- github.com/filecoin-project/go-data-transfer:
  - feat: configurable send message timeouts (#136) ([filecoin-project/go-data-transfer#136](https://github.com/filecoin-project/go-data-transfer/pull/136))
  - log request / response events (#137) ([filecoin-project/go-data-transfer#137](https://github.com/filecoin-project/go-data-transfer/pull/137))
  - fix: dont complete transfer because graphsync request was cancelled (#134) ([filecoin-project/go-data-transfer#134](https://github.com/filecoin-project/go-data-transfer/pull/134))
  - feat: better push channel monitor logging (#133) ([filecoin-project/go-data-transfer#133](https://github.com/filecoin-project/go-data-transfer/pull/133))
  - release: v1.2.6 (#132) ([filecoin-project/go-data-transfer#132](https://github.com/filecoin-project/go-data-transfer/pull/132))

Contributors

| Contributor | Commits | Lines ± | Files Changed |
|-------------|---------|---------|---------------|
| dirkmc | 5 | +172/-67 | 12 |

# go-data-transfer 1.2.6

### Changelog

- github.com/filecoin-project/go-data-transfer:
  - add logging to push channel monitor (#131) ([filecoin-project/go-data-transfer#131](https://github.com/filecoin-project/go-data-transfer/pull/131))

Contributors

| Contributor | Commits | Lines ± | Files Changed |
|-------------|---------|---------|---------------|
| dirkmc | 2 | +27/-2 | 2 |

# go-data-transfer 1.2.5

### Changelog

- github.com/filecoin-project/go-data-transfer:
  - feat: limit consecutive restarts with no data transfer (#129) ([filecoin-project/go-data-transfer#129](https://github.com/filecoin-project/go-data-transfer/pull/129))

Contributors

| Contributor | Commits | Lines ± | Files Changed |
|-------------|---------|---------|---------------|
| dirkmc | 2 | +194/-78 | 5 |

# go-data-transfer 1.2.4

### Changelog

- github.com/filecoin-project/go-data-transfer:
  - Automatically restart push channel (#127) ([filecoin-project/go-data-transfer#127](https://github.com/filecoin-project/go-data-transfer/pull/127))
- github.com/ipfs/go-graphsync (v0.5.0 -> v0.5.2):
  - RegisterNetworkErrorListener should fire when there's an error connecting to the peer (#127) ([ipfs/go-graphsync#127](https://github.com/ipfs/go-graphsync/pull/127))
  - Permit multiple data subscriptions per original topic (#128) ([ipfs/go-graphsync#128](https://github.com/ipfs/go-graphsync/pull/128))

Contributors

| Contributor | Commits | Lines ± | Files Changed |
|-------------|---------|---------|---------------|
| dirkmc | 4 | +1072/-74 | 17 |
| Alex Cruikshank | 1 | +188/-110 | 12 |
| hannahhoward | 1 | +30/-14 | 8 |
| Hannah Howard | 1 | +23/-6 | 3 |

# go-data-transfer 1.2.3

### Changelog

- github.com/filecoin-project/go-data-transfer:
  - Better retry config (#124) ([filecoin-project/go-data-transfer#124](https://github.com/filecoin-project/go-data-transfer/pull/124))
  - feat: expose channel state on Manager interface (#125) ([filecoin-project/go-data-transfer#125](https://github.com/filecoin-project/go-data-transfer/pull/125))
  - Fix typo, wrap correct FSM error (#123) ([filecoin-project/go-data-transfer#123](https://github.com/filecoin-project/go-data-transfer/pull/123))

Contributors

| Contributor | Commits | Lines ± | Files Changed |
|-------------|---------|---------|---------------|
| dirkmc | 3 | +167/-7 | 6 |
| Ingar Shu | 1 | +1/-1 | 1 |

# go-data-transfer 1.2.2

### Changelog

- github.com/filecoin-project/go-data-transfer:
  - fix(graphsync): fix UseStore for restarts (#115) ([filecoin-project/go-data-transfer#115](https://github.com/filecoin-project/go-data-transfer/pull/115))

Contributors

| Contributor | Commits | Lines ± | Files Changed |
|-------------|---------|---------|---------------|
| Hannah Howard | 1 | +6/-2 | 1 |

# go-data-transfer 1.2.1

### Changelog

- github.com/filecoin-project/go-data-transfer:
  - Fire cancel locally even if remote cancel fails (#120) ([filecoin-project/go-data-transfer#120](https://github.com/filecoin-project/go-data-transfer/pull/120))
  - fix: respect context when opening stream (#119) ([filecoin-project/go-data-transfer#119](https://github.com/filecoin-project/go-data-transfer/pull/119))

Contributors

| Contributor | Commits | Lines ± | Files Changed |
|-------------|---------|---------|---------------|
| dirkmc | 2 | +17/-3 | 2 |

# go-data-transfer 1.1.0

This release primarily updates to go-libp2p 0.12 to use the new stream interfaces. Additionally, it pulls in a bug fix release of graphsync.

### Changelog

- github.com/filecoin-project/go-data-transfer:
  - feat: update libp2p (#116) ([filecoin-project/go-data-transfer#116](https://github.com/filecoin-project/go-data-transfer/pull/116))
  - docs(CHANGELOG): update for 1.0.1 release ([filecoin-project/go-data-transfer#114](https://github.com/filecoin-project/go-data-transfer/pull/114))
- github.com/ipfs/go-graphsync (v0.4.2 -> v0.5.0):
  - docs(CHANGELOG): update for 0.5.0
  - feat: use go-libp2p-core 0.7.0 stream interfaces (#116) ([ipfs/go-graphsync#116](https://github.com/ipfs/go-graphsync/pull/116))
  - Merge branch 'release/v0.4.3'
  - chore(benchmarks): remove extra files
  - fix(peerresponsemanager): avoid race condition that could result in NPE in link tracker (#118) ([ipfs/go-graphsync#118](https://github.com/ipfs/go-graphsync/pull/118))
  - docs(CHANGELOG): update for 0.4.2 ([ipfs/go-graphsync#117](https://github.com/ipfs/go-graphsync/pull/117))
  - feat(memory): improve memory usage (#110) ([ipfs/go-graphsync#110](https://github.com/ipfs/go-graphsync/pull/110))

Contributors

| Contributor   | Commits | Lines ±  | Files Changed |
|---------------|---------|----------|---------------|
| Steven Allen  |       3 | +393/-37 |             7 |
| Hannah Howard |       2 | +49/-6   |             7 |
| hannahhoward  |       2 | +19/-0   |             3 |

# go-data-transfer 1.0.1

Bug fix release that fixes channel closing and timeout issues

### Changelog

- github.com/filecoin-project/go-data-transfer:
  - fix(impl): reset timeouts (#113) ([filecoin-project/go-data-transfer#113](https://github.com/filecoin-project/go-data-transfer/pull/113))
  - feat(impl): fix shutdown (#112) ([filecoin-project/go-data-transfer#112](https://github.com/filecoin-project/go-data-transfer/pull/112))
  - Remove link to missing design documentation (#111) ([filecoin-project/go-data-transfer#111](https://github.com/filecoin-project/go-data-transfer/pull/111))
  - fix(channels): add nil check (#94) ([filecoin-project/go-data-transfer#94](https://github.com/filecoin-project/go-data-transfer/pull/94))
  - docs(CHANGELOG): update for v1.0.0 ([filecoin-project/go-data-transfer#110](https://github.com/filecoin-project/go-data-transfer/pull/110))

### Contributors

| Contributor | Commits | Lines ± | Files Changed |
|-------------|---------|---------|---------------|
| Hannah Howard | 4 | +240/-64 | 16 |

# go-data-transfer 1.0.0

Major release brings big graphsync improvements and better measuring of data transferred

### Changelog

- github.com/filecoin-project/go-data-transfer:
  - Update graphsync & fix in-progress request memory leak by consuming responses (#109) ([filecoin-project/go-data-transfer#109](https://github.com/filecoin-project/go-data-transfer/pull/109))
  - Correct data transfer Sent stats (#106) ([filecoin-project/go-data-transfer#106](https://github.com/filecoin-project/go-data-transfer/pull/106))
  - Rudimentary Benchmarking (#108) ([filecoin-project/go-data-transfer#108](https://github.com/filecoin-project/go-data-transfer/pull/108))
  - Create SECURITY.md (#105) ([filecoin-project/go-data-transfer#105](https://github.com/filecoin-project/go-data-transfer/pull/105))
  - fix(impl): don't error when channel missing (#107) ([filecoin-project/go-data-transfer#107](https://github.com/filecoin-project/go-data-transfer/pull/107))
  - docs(CHANGELOG): update for 0.9.0 ([filecoin-project/go-data-transfer#103](https://github.com/filecoin-project/go-data-transfer/pull/103))
- github.com/ipfs/go-graphsync (v0.3.0 -> v0.4.2):
  - docs(CHANGELOG): update for 0.4.2
  - fix(notifications): fix lock in close (#115) ([ipfs/go-graphsync#115](https://github.com/ipfs/go-graphsync/pull/115))
  - docs(CHANGELOG): update for v0.4.1 ([ipfs/go-graphsync#114](https://github.com/ipfs/go-graphsync/pull/114))
  - fix(allocator): remove peer from peer status list
  - docs(CHANGELOG): update for v0.4.0
  - docs(CHANGELOG): update for 0.3.1 ([ipfs/go-graphsync#112](https://github.com/ipfs/go-graphsync/pull/112))
  - Update ipld-prime (#111) ([ipfs/go-graphsync#111](https://github.com/ipfs/go-graphsync/pull/111))
  - Add allocator for memory backpressure (#108) ([ipfs/go-graphsync#108](https://github.com/ipfs/go-graphsync/pull/108))
  - Shutdown notifications go routines (#109) ([ipfs/go-graphsync#109](https://github.com/ipfs/go-graphsync/pull/109))
  - Switch to google protobuf generator (#105) ([ipfs/go-graphsync#105](https://github.com/ipfs/go-graphsync/pull/105))
  - feat(CHANGELOG): update for 0.3.0 ([ipfs/go-graphsync#104](https://github.com/ipfs/go-graphsync/pull/104))

### Contributors

| Contributor | Commits | Lines ± | Files Changed |
|-------------|---------|---------|---------------|
| Hannah Howard | 8 | +1745/-1666 | 40 |
| Aarsh Shah | 1 | +132/-40 | 14 |
| hannahhoward | 5 | +74/-4 | 7 |
| David Dias | 1 | +9/-0 | 1 |

# go-data-transfer 0.9.0

Major release of the 1.1 data transfer protocol, which supports restarts of data transfers.

### Changelog

- github.com/filecoin-project/go-data-transfer:
  - Message compatibility on graphsync (#102) ([filecoin-project/go-data-transfer#102](https://github.com/filecoin-project/go-data-transfer/pull/102))
  - Handle network errors/stalls (#101) ([filecoin-project/go-data-transfer#101](https://github.com/filecoin-project/go-data-transfer/pull/101))
  - Resume Data Transfer (#100) ([filecoin-project/go-data-transfer#100](https://github.com/filecoin-project/go-data-transfer/pull/100))
  - docs(CHANGELOG): update for 0.6.7 release ([filecoin-project/go-data-transfer#98](https://github.com/filecoin-project/go-data-transfer/pull/98))
- github.com/ipfs/go-graphsync (v0.2.1 -> v0.3.0):
  - feat(CHANGELOG): update for 0.3.0
  - docs(CHANGELOG): update for 0.2.1 ([ipfs/go-graphsync#103](https://github.com/ipfs/go-graphsync/pull/103))
  - Track actual network operations in a response (#102) ([ipfs/go-graphsync#102](https://github.com/ipfs/go-graphsync/pull/102))
  - feat(responsecache): prune blocks more intelligently (#101) ([ipfs/go-graphsync#101](https://github.com/ipfs/go-graphsync/pull/101))

### Contributors

| Contributor | Commits | Lines ± | Files Changed |
|-------------|---------|---------|---------------|
| Aarsh Shah | 1 | +9597/-2220 | 67 |
| Hannah Howard | 4 | +2355/-1018 | 51 |
| hannahhoward | 1 | +25/-3 | 4 |

# go-data-transfer 0.6.7

Minor update w/ fixes to support go-fil-markets 0.7.0

### Changelog

- github.com/filecoin-project/go-data-transfer:
  - Feat/cleanup errors (#90) ([filecoin-project/go-data-transfer#90](https://github.com/filecoin-project/go-data-transfer/pull/90))
  - Disambiguate whether a revalidator recognized a request when checking for a need to revalidate (#87) ([filecoin-project/go-data-transfer#87](https://github.com/filecoin-project/go-data-transfer/pull/87))
  - docs(CHANGELOG): update for 0.6.6 ([filecoin-project/go-data-transfer#89](https://github.com/filecoin-project/go-data-transfer/pull/89))

### Contributors

| Contributor | Commits | Lines ± | Files Changed |
|-------------|---------|---------|---------------|
| Hannah Howard | 2 | +167/-30 | 9 |

# go-data-transfer 0.6.6

Dependency update - go graphsync fix

### Changelog

- github.com/filecoin-project/go-data-transfer:
  - feat(deps): update graphsync (#86) ([filecoin-project/go-data-transfer#86](https://github.com/filecoin-project/go-data-transfer/pull/86))
  - docs(CHANGELOG): updates for 0.6.5 ([filecoin-project/go-data-transfer#85](https://github.com/filecoin-project/go-data-transfer/pull/85))
- github.com/ipfs/go-graphsync (v0.2.0 -> v0.2.1):
  - docs(CHANGELOG): update for 0.2.1
  - Release/0.2.0 ([ipfs/go-graphsync#99](https://github.com/ipfs/go-graphsync/pull/99))
  - fix(metadata): fix cbor-gen (#98) ([ipfs/go-graphsync#98](https://github.com/ipfs/go-graphsync/pull/98))

### Contributors

| Contributor | Commits | Lines ± | Files Changed |
|-------------|---------|---------|---------------|
| hannahhoward | 1 | +83/-68 | 1 |
| Hannah Howard | 2 | +15/-19 | 5 |

# go-data-transfer 0.6.5

Dependency update - go-graphsync and go-ipld-prime

### Changelog

- github.com/filecoin-project/go-data-transfer:
  - feat(deps): update graphsync 0.2.0 (#83) ([filecoin-project/go-data-transfer#83](https://github.com/filecoin-project/go-data-transfer/pull/83))
  - docs(CHANGELOG): update for 0.6.4 ([filecoin-project/go-data-transfer#82](https://github.com/filecoin-project/go-data-transfer/pull/82))
- github.com/hannahhoward/cbor-gen-for (v0.0.0-20191218204337-9ab7b1bcc099 -> v0.0.0-20200817222906-ea96cece81f1):
  - add flag to select map encoding ([hannahhoward/cbor-gen-for#1](https://github.com/hannahhoward/cbor-gen-for/pull/1))
  - fix(deps): update cbor-gen-to-latest
- github.com/ipfs/go-graphsync (v0.1.2 -> v0.2.0):
  - docs(CHANGELOG): update for 0.2.0
  - style(imports): fix imports
  - fix(selectorvalidator): memory optimization (#97) ([ipfs/go-graphsync#97](https://github.com/ipfs/go-graphsync/pull/97))
  - Update go-ipld-prime@v0.5.0 (#92) ([ipfs/go-graphsync#92](https://github.com/ipfs/go-graphsync/pull/92))
  - refactor(metadata): use cbor-gen encoding (#96) ([ipfs/go-graphsync#96](https://github.com/ipfs/go-graphsync/pull/96))
  - Release/v0.1.2 ([ipfs/go-graphsync#95](https://github.com/ipfs/go-graphsync/pull/95))
  - Return Request context cancelled error (#93) ([ipfs/go-graphsync#93](https://github.com/ipfs/go-graphsync/pull/93))
  - feat(benchmarks): add p2p stress test (#91) ([ipfs/go-graphsync#91](https://github.com/ipfs/go-graphsync/pull/91))

### Contributors

| Contributor | Commits | Lines ± | Files Changed |
|-------------|---------|---------|---------------|
| Eric Myhre | 1 | +2919/-121 | 39 |
| Hannah Howard | 4 | +453/-143 | 29 |
| hannahhoward | 3 | +83/-63 | 10 |
| whyrusleeping | 1 | +31/-18 | 2 |
| Aarsh Shah | 1 | +27/-1 | 3 |

# go-data-transfer 0.6.4

security fix for messages

### Changelog

- github.com/filecoin-project/go-data-transfer:
  - Ensure valid messages are returned from FromNet() (#74) ([filecoin-project/go-data-transfer#74](https://github.com/filecoin-project/go-data-transfer/pull/74))
  - Release/v0.6.3 ([filecoin-project/go-data-transfer#70](https://github.com/filecoin-project/go-data-transfer/pull/70))

### Contributors

| Contributor | Commits | Lines ± | Files Changed |
|-------------|---------|---------|---------------|
| Ingar Shu | 1 | +20/-1 | 2 |

# go-data-transfer 0.6.3

Primarily a bug fix release-- graphsync performance and some better shutdown
logic

### Changelog

- github.com/filecoin-project/go-data-transfer:
  - fix(deps): update graphsync, small cleanup
  - Stop data transfer correctly and some minor cleanp (#69) ([filecoin-project/go-data-transfer#69](https://github.com/filecoin-project/go-data-transfer/pull/69))
  - docs(CHANGELOG): update for 0.6.2 release ([filecoin-project/go-data-transfer#68](https://github.com/filecoin-project/go-data-transfer/pull/68))
- github.com/ipfs/go-graphsync (v0.1.1 -> v0.1.2):
  - fix(asyncloader): remove send on close channel
  - docs(CHANGELOG): update for 0.1.2 release
  - Benchmark framework + First memory fixes (#89) ([ipfs/go-graphsync#89](https://github.com/ipfs/go-graphsync/pull/89))
  - docs(CHANGELOG): update for v0.1.1 ([ipfs/go-graphsync#85](https://github.com/ipfs/go-graphsync/pull/85))

### Contributors

| Contributor | Commits | Lines ± | Files Changed |
|-------------|---------|---------|---------------|
| Hannah Howard | 1 | +1055/-39 | 17 |
| Aarsh Shah | 1 | +53/-68 | 8 |
| hannahhoward | 3 | +67/-34 | 11 |

# go-data-transfer 0.6.2

Minor bug fix release for request cancelling

### Changelog

- github.com/filecoin-project/go-data-transfer:
  - Fix Pull Request Cancelling (#67) ([filecoin-project/go-data-transfer#67](https://github.com/filecoin-project/go-data-transfer/pull/67))
  - docs(CHANGELOG): update for 0.6.1 ([filecoin-project/go-data-transfer#66](https://github.com/filecoin-project/go-data-transfer/pull/66))

### Contributors

| Contributor | Commits | Lines ± | Files Changed |
|-------------|---------|---------|---------------|
| Hannah Howard | 1 | +265/-9 | 4 |

# go-data-transfer 0.6.1

Update graphsync with critical bug fix for multiple transfers across custom stores

### Changelog

- github.com/filecoin-project/go-data-transfer:
  - Update graphsync 0.1.1 (#65) ([filecoin-project/go-data-transfer#65](https://github.com/filecoin-project/go-data-transfer/pull/65))
- github.com/ipfs/go-graphsync (v0.1.0 -> v0.1.1):
  - docs(CHANGELOG): update for v0.1.1
  - docs(CHANGELOG): update for v0.1.0 release ([ipfs/go-graphsync#84](https://github.com/ipfs/go-graphsync/pull/84))
  - Dedup by key extension (#83) ([ipfs/go-graphsync#83](https://github.com/ipfs/go-graphsync/pull/83))

### Contributors

| Contributor | Commits | Lines ± | Files Changed |
|-------------|---------|---------|---------------|
| Hannah Howard | 2 | +456/-57 | 17 |
| hannahhoward | 1 | +18/-1 | 2 |

# go-data-transfer 0.6.0

Includes two small breaking change updates:

- Update go-ipfs-blockstore to address blocks by-hash instead of by-cid. This brings go-data-transfer in-line with lotus.
- Update cbor-gen for some performance improvements and some API-breaking changes.

### Changelog

- github.com/filecoin-project/go-data-transfer:
  - Update cbor-gen (#63) ([filecoin-project/go-data-transfer#63](https://github.com/filecoin-project/go-data-transfer/pull/63))

### Contributors

| Contributor  | Commits | Lines ± | Files Changed |
|--------------|---------|---------|---------------|
| Steven Allen |       1 | +30/-23 |             5 |

# go-data-transfer 0.5.3

Minor fixes + update to release process

### Changelog

- github.com/filecoin-project/go-data-transfer:
  - fix(deps): update graphsync
  - Release infrastructure (#61) ([filecoin-project/go-data-transfer#61](https://github.com/filecoin-project/go-data-transfer/pull/61))
  - Update cbor-gen (#60) ([filecoin-project/go-data-transfer#60](https://github.com/filecoin-project/go-data-transfer/pull/60))
- github.com/ipfs/go-graphsync (v0.0.6-0.20200731020347-9ff2ade94aa4 -> v0.1.0):
  - docs(CHANGELOG): update for v0.1.0 release
  - Release infrastructure (#81) ([ipfs/go-graphsync#81](https://github.com/ipfs/go-graphsync/pull/81))

### Contributors

| Contributor | Commits | Lines ± | Files Changed |
|-------------|---------|---------|---------------|
| Hannah Howard | 2 | +1202/-223 | 91 |
| Łukasz Magiera | 1 | +176/-176 | 8 |
| hannahhoward | 2 | +48/-3 | 3 |

# go-data-transfer 0.5.2

Security fix release

### Changelog

- github.com/filecoin-project/go-data-transfer:
  - fix(deps): update graphsync
  - fix(message): add error check to FromNet (#59) ([filecoin-project/go-data-transfer#59](https://github.com/filecoin-project/go-data-transfer/pull/59))
- github.com/ipfs/go-graphsync (v0.0.6-0.20200721211002-c376cbe14c0a -> v0.0.6-0.20200731020347-9ff2ade94aa4):
  - feat(persistenceoptions): add unregister ability (#80) ([ipfs/go-graphsync#80](https://github.com/ipfs/go-graphsync/pull/80))
  - fix(message): regen protobuf code (#79) ([ipfs/go-graphsync#79](https://github.com/ipfs/go-graphsync/pull/79))

### Contributors

| Contributor | Commits | Lines ± | Files Changed |
|-------------|---------|---------|---------------|
| Hannah Howard | 3 | +442/-302 | 7 |
| hannahhoward | 1 | +3/-3 | 2 |

# go-data-transfer v0.5.1

Support custom configruation of transports

### Changelog

- github.com/filecoin-project/go-data-transfer:
  - Allow custom configuration of transports (#57) ([filecoin-project/go-data-transfer#57](https://github.com/filecoin-project/go-data-transfer/pull/57))
- github.com/ipfs/go-graphsync (v0.0.6-0.20200715204712-ef06b3d32e83 -> v0.0.6-0.20200721211002-c376cbe14c0a):
  - feat(persistenceoptions): add unregister ability

### Contributors

| Contributor | Commits | Lines ± | Files Changed |
|-------------|---------|---------|---------------|
| Hannah Howard | 1 | +1049/-751 | 35 |
| hannahhoward | 1 | +113/-32 | 5 |

# go-data-transfer 0.5.0

Additional changes to support implementation of retrieval on top of go-data-transfer

### Changelog

- github.com/filecoin-project/go-data-transfer:
  - Minor fixes for retrieval on data transfer (#56) ([filecoin-project/go-data-transfer#56](https://github.com/filecoin-project/go-data-transfer/pull/56))
- github.com/ipfs/go-graphsync (v0.0.6-0.20200708073926-caa872f68b2c -> v0.0.6-0.20200715204712-ef06b3d32e83):
  - feat(requestmanager): run response hooks on completed requests (#77) ([ipfs/go-graphsync#77](https://github.com/ipfs/go-graphsync/pull/77))
  - Revert "add extensions on complete (#76)"
  - add extensions on complete (#76) ([ipfs/go-graphsync#76](https://github.com/ipfs/go-graphsync/pull/76))

### Contributors

| Contributor | Commits | Lines ± | Files Changed |
|-------------|---------|---------|---------------|
| Hannah Howard | 3 | +515/-218 | 26 |
| hannahhoward | 1 | +155/-270 | 9 |

# go-data-transfer 0.4.0

Major rewrite of library -- transports, persisted state, revalidators, etc. To support retrieval

### Changelog

- github.com/filecoin-project/go-data-transfer:
  - The new data transfer (#55) ([filecoin-project/go-data-transfer#55](https://github.com/filecoin-project/go-data-transfer/pull/55))
  - Actually track progress for send/receive (#53) ([filecoin-project/go-data-transfer#53](https://github.com/filecoin-project/go-data-transfer/pull/53))
- github.com/ipfs/go-graphsync (v0.0.6-0.20200504202014-9d5f2c26a103 -> v0.0.6-0.20200708073926-caa872f68b2c):
  - All changes to date including pause requests & start paused, along with new adds for cleanups and checking of execution (#75) ([ipfs/go-graphsync#75](https://github.com/ipfs/go-graphsync/pull/75))
  - More fine grained response controls (#71) ([ipfs/go-graphsync#71](https://github.com/ipfs/go-graphsync/pull/71))
  - Refactor request execution and use IPLD SkipMe functionality for proper partial results on a request (#70) ([ipfs/go-graphsync#70](https://github.com/ipfs/go-graphsync/pull/70))
  - feat(graphsync): implement do-no-send-cids extension (#69) ([ipfs/go-graphsync#69](https://github.com/ipfs/go-graphsync/pull/69))
  - Incoming Block Hooks (#68) ([ipfs/go-graphsync#68](https://github.com/ipfs/go-graphsync/pull/68))

### Contributors

| Contributor | Commits | Lines ± | Files Changed |
|-------------|---------|---------|---------------|
| Hannah Howard | 7 | +12381/-4583 | 133 |

# go-data-transfer 0.3.0

Additional refactors to refactors to registry

### Changelog 
- github.com/filecoin-project/go-data-transfer:
  - feat(graphsyncimpl): fix open/close events (#52) ([filecoin-project/go-data-transfer#52](https://github.com/filecoin-project/go-data-transfer/pull/52))
  - chore(deps): update graphsync ([filecoin-project/go-data-transfer#51](https://github.com/filecoin-project/go-data-transfer/pull/51))
  - Refactor registry and encoding (#50) ([filecoin-project/go-data-transfer#50](https://github.com/filecoin-project/go-data-transfer/pull/50))

### Contributors

| Contributor | Commits | Lines ± | Files Changed |
|-------------|---------|---------|---------------|
| Hannah Howard | 2 | +993/-496 | 30 |

# go-data-transfer 0.2.1

Bug fix release -- critical nil check

### Changelog

- github.com/filecoin-project/go-data-transfer:
  - chore(deps): update graphsync
- github.com/ipfs/go-graphsync (v0.0.6-0.20200428204348-97a8cf76a482 -> v0.0.6-0.20200504202014-9d5f2c26a103):
  - fix(responsemanager): add nil check (#67) ([ipfs/go-graphsync#67](https://github.com/ipfs/go-graphsync/pull/67))
  - Add autocomment configuration

### Contributors

| Contributor | Commits | Lines ± | Files Changed |
|-------------|---------|---------|---------------|
| Hector Sanjuan | 1 | +68/-0 | 1 |
| hannahhoward | 1 | +3/-3 | 2 |
| Hannah Howard | 1 | +4/-0 | 1 |

# go-data-transfer 0.2.0

Initial extracted release for Testnet Phase 2 (v0.1.0 + v0.1.1 were lotus tags prior to extraction)

### Changelog

- github.com/filecoin-project/go-data-transfer:
  - Upgrade graphsync + ipld-prime (#49) ([filecoin-project/go-data-transfer#49](https://github.com/filecoin-project/go-data-transfer/pull/49))
  - Use extracted generic pubsub (#48) ([filecoin-project/go-data-transfer#48](https://github.com/filecoin-project/go-data-transfer/pull/48))
  - Refactor & Cleanup In Preparation For Added Complexity (#47) ([filecoin-project/go-data-transfer#47](https://github.com/filecoin-project/go-data-transfer/pull/47))
  - feat(graphsync): complete notifications for responder (#46) ([filecoin-project/go-data-transfer#46](https://github.com/filecoin-project/go-data-transfer/pull/46))
  - Update graphsync ([filecoin-project/go-data-transfer#45](https://github.com/filecoin-project/go-data-transfer/pull/45))
  - docs(docs): remove outdated docs
  - docs(README): clean up README
  - docs(license): add license + contrib
  - ci(circle): add config
  - build(datatransfer): add go.mod
  - build(cbor-gen): add tools for cbor-gen-for .
  - fix links in datatransfer README (#11) ([filecoin-project/go-data-transfer#11](https://github.com/filecoin-project/go-data-transfer/pull/11))
  - feat(shared): add shared tools and types (#9) ([filecoin-project/go-data-transfer#9](https://github.com/filecoin-project/go-data-transfer/pull/9))
  - Feat/datatransfer readme, contributing, design doc (rename)
  - refactor(datatransfer): move to local module
  - feat(datatransfer): switch to graphsync implementation
  - Don't respond with error in gsReqRcdHook when we can't find the datatransfer extension. (#754) ([filecoin-project/go-data-transfer#754](https://github.com/filecoin-project/go-data-transfer/pull/754))
  - Feat/dt subscribe, file Xfer round trip (#720) ([filecoin-project/go-data-transfer#720](https://github.com/filecoin-project/go-data-transfer/pull/720))
  - Feat/dt gs pullrequests (#693) ([filecoin-project/go-data-transfer#693](https://github.com/filecoin-project/go-data-transfer/pull/693))
  - DTM sends data over graphsync for validated push requests (#665) ([filecoin-project/go-data-transfer#665](https://github.com/filecoin-project/go-data-transfer/pull/665))
  - Techdebt/dt split graphsync impl receiver (#651) ([filecoin-project/go-data-transfer#651](https://github.com/filecoin-project/go-data-transfer/pull/651))
  - Feat/dt initiator cleanup (#645) ([filecoin-project/go-data-transfer#645](https://github.com/filecoin-project/go-data-transfer/pull/645))
  - Feat/dt graphsync pullreqs (#627) ([filecoin-project/go-data-transfer#627](https://github.com/filecoin-project/go-data-transfer/pull/627))
  - fix(datatransfer): fix tests
  - Graphsync response is scheduled when a valid push request is received (#625) ([filecoin-project/go-data-transfer#625](https://github.com/filecoin-project/go-data-transfer/pull/625))
  - responses alert subscribers when request is not accepted (#607) ([filecoin-project/go-data-transfer#607](https://github.com/filecoin-project/go-data-transfer/pull/607))
  - feat(datatransfer): milestone 2 infrastructure
  - other tests passing
  - send data transfer response
  - a better reflection
  - remove unused fmt import in graphsync_test
  - cleanup for PR
  - tests passing
  - Initiate push and pull requests (#536) ([filecoin-project/go-data-transfer#536](https://github.com/filecoin-project/go-data-transfer/pull/536))
  - Fix tests
  - Respond to PR comments: * Make DataTransferRequest/Response be returned in from Net * Regenerate cbor_gen and fix the generator caller so it works better * Please the linters
  - Cleanup for PR, clarifying and additional comments
  - Some cleanup for PR
  - all message tests passing, some others in datatransfer
  - WIP trying out some stuff * Embed request/response in message so all the interfaces work AND the CBOR unmarshaling works: this is more like the spec anyway * get rid of pb stuff
  - * Bring cbor-gen stuff into datatransfer package * make transferRequest private struct * add transferResponse + funcs * Rename VoucherID to VoucherType * more tests passing
  - WIP using CBOR encoding for dataxfermsg
  - feat(datatransfer): setup implementation path
  - Duplicate comment ([filecoin-project/go-data-transfer#619](https://github.com/filecoin-project/go-data-transfer/pull/619))
  - fix typo ([filecoin-project/go-data-transfer#621](https://github.com/filecoin-project/go-data-transfer/pull/621))
  - fix types typo
  - refactor(datatransfer): implement style fixes
  - refactor(deals): move type instantiation to modules
  - refactor(datatransfer): xerrors, cbor-gen, tweaks
  - feat(datatransfer): make dag service dt async
  - refactor(datatransfer): add comments, renames
  - feat(datatransfer): integration w/ simple merkledag

### Contributors

| Contributor | Commits | Lines ± | Files Changed |
|-------------|---------|---------|---------------|
| Shannon Wells | 12 | +4337/-3455 | 53 |
| hannahhoward | 20 | +5090/-1692 | 99 |
| shannonwells | 13 | +1720/-983 | 65 |
| Hannah Howard | 6 | +1393/-1262 | 45 |
| wanghui | 2 | +4/-4 | 2 |
| 郭光华 | 1 | +0/-1 | 1 |

### 🙌🏽 Want to contribute?

Would you like to contribute to this repo and don’t know how? Here are a few places you can get started:

- Check out the [Contributing Guidelines](https://github.com/filecoin-project/go-data-transfer/blob/master/CONTRIBUTING.md)
- Look for issues with the `good-first-issue` label in [go-fil-markets](https://github.com/filecoin-project/go-data-transfer/issues?utf8=%E2%9C%93&q=is%3Aissue+is%3Aopen+label%3A%22e-good-first-issue%22+)
