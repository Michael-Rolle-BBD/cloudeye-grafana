# Changelog

## Unreleased (fork: server-side alerting)

- Implement backend `QueryData` so Grafana alerting and other server-side
  queries can evaluate CloudEye metrics; set `"alerting": true` in plugin.json.
- Track `go.sum` and list indirect module requirements in `go.mod` so the
  backend builds reproducibly from a clean checkout.
- See [docs/ALERTING.md](docs/ALERTING.md) for limitations, build and packaging.

## 1.0.0 (Unreleased)

Initial release.
