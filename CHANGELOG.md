## Change log

### Unreleased

- OpenSearch 2.11/2.15 v2 aggregations can stop at `plugins.query.size_limit`
  (200 by default). Ambiguous full buckets now raise `DataError`, rather than
  silently returning incomplete groups. Exactly 200 remains accepted on
  Open Distro and OpenSearch 1.x, and on newer servers at their default size
  limit. A custom size limit below 1000 is also checked on OpenSearch 2.x/3.x.
- An explicit v2 SELECT `LIMIT` bypasses the query size limit; only the search
  window is checked. Plain SELECTs with a LIMIT above 10000 are replayed without
  LIMIT using SQL cursors, then capped locally; unavailable cursors raise
  `DataError`, including a limited cursor whose next page is empty.
- The v2 engine sorts grouped top-N results **after** its capped composite
  aggregation, so `GROUP BY ... ORDER BY COUNT(*) DESC LIMIT 5` can select the
  wrong groups even though only five rows are returned. The driver now probes
  the underlying group listing and raises `DataError` at its bucket ceiling.
  Narrow the grouping or use `v2=false` for legacy top-N. Safe small listings
  incur one additional SQL request.
- OpenSearch 1.x v2 plain SELECTs stay on the v2 engine to preserve floating
  values and timestamp objects. Version discovery is best effort and cached;
  when unavailable, unpaged v2 semantics are preferred to legacy paging.

### 0.2.13

- fix(OpenSearch): Support removing `default` from query (#121) [Vitor Avila]
- fix(ci): use and fix pinned requirements (#120) [Daniel Vaz Gaspar]

### 0.2.12

- fix: upgrade to elasticsearch-py 7.17.13 and opensearch-py 2.x for urllib3 2.x compatibility (#118) [Daniel Vaz Gaspar]

### 0.2.11

- fix: relax packaging dependency (#109) [Daniel Vaz Gaspar]

### 0.2.10

- fix: OpenDistro dialect quotes properly with backticks now (#99) [Beto Dealmeida]

### 0.2.9

- fix: remove six dependency (#84) [Daniel Vaz Gaspar]

### 0.2.8

- fix: remove show tables column retrieval index based (#81) [Daniel Vaz Gaspar]

### 0.2.7

- fix: unpin packaging dependency (#77) [Daniel Vaz Gaspar]

### 0.2.6

- fix: pin elasticsearch-py bellow 7.14 (#71) [Daniel Vaz Gaspar]
- feat(query): add time_zone param (#69) [aniaan]

### 0.2.5

- fix: Bump packaging [Beto Dealmeida]

### 0.2.4

- fix: Bump urllib3 from 1.25.6 to 1.26.5 (#64) [dependabot]
- fix: missing time type (#60) [maltoze]

### 0.2.3

- fix: missing dependency, packaging [Daniel Vaz Gaspar]

### 0.2.2

- fix: support elasticsearch > 7.10 [Daniel Vaz Gaspar]

### 0.2.1

- feat: support new opendistro SQL engine 1.13 [Daniel Vaz Gaspar]

### 0.2.0

- docs: update changelog [Daniel Vaz Gaspar]
- release: version bump and exception fix (#48)  [Daniel Vaz Gaspar]
- feat(opendistro): implement get view names with ES alias (#47)  [Daniel Vaz Gaspar]
- fix(opendistro): aws auth and discard not supported engine type from meta (#46)  [Daniel Vaz Gaspar]
- feat: support opendistro (#45) [Daniel Vaz Gaspar]

### 0.1.4

- [fix]: crash with empty indexes (#39) [Daniel Vaz Gaspar]
- [docs]: update README with github actions badge (#41) [Daniel Vaz Gaspar]
- [ci]: from travis to github actions (#40) [Daniel Vaz Gaspar]
- [docs]: updated readme opendistro info (#37) [Anirudha (Ani) Jadhav]

### 0.1.3

- [elasticsearch]: feat: `fetch_size` configurable and set default to 10000 (#30) 
- [docs]: fix: Update README.md (#32)

### 0.1.2

- [elasticsearch] fix: newer elasticsearch version were crashing (#23)

### 0.1.1

- [dbapi] fix: enforce list tuple to follow PEP-249 (#17)
- [dbapi] fix: don't do anything in commit method (#16)
- [dbapi] fix: support connection string without trailing slash (#9)
