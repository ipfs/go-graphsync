# go-graphsync changelog

# go-graphysnc 0.0.1-filecoin

Initial tagged release for early version of filecoin

### Changelog

Initial feature set including parallel requests, selectors, basic architecture,
etc. -- changelog not tracked due to lack of go.mod

# go-graphsync 0.0.2

Bug fix release. Fix message sizes to not overflow limits.

### Changelog

- github.com/ipfs/go-graphsync:
  - Limit Response Size ([ipfs/go-graphsync#37](https://github.com/ipfs/go-graphsync/pull/37))

### Contributors

| Contributor | Commits | Lines ± | Files Changed |
|-------------|---------|---------|---------------|
| hannahhoward | 2 | +295/-52 | 5 |

# go-graphsync 0.0.3

Bug fix release. Fix issues issues with message queue.

### Changelog

- github.com/ipfs/go-graphsync:
  - fix(messagequeue): no retry after queue shutdown ([ipfs/go-graphsync#38](https://github.com/ipfs/go-graphsync/pull/38))

### Contributors

| Contributor | Commits | Lines ± | Files Changed |
|-------------|---------|---------|---------------|
| hannahhoward | 1 | +70/-1 | 2 |

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

### 🙌🏽 Want to contribute?

Would you like to contribute to this repo and don’t know how? Here are a few places you can get started:

- Check out the [Contributing Guidelines](https://github.com/ipfs/go-graphsync/blob/master/CONTRIBUTING.md)
- Look for issues with the `good-first-issue` label in [go-graphsync](https://github.com/ipfs/go-graphsync/issues?utf8=%E2%9C%93&q=is%3Aissue+is%3Aopen+label%3A%22e-good-first-issue%22+)
