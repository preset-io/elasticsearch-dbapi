## Change log

### 0.2.14

- feat: OpenSearch 3 support (#124) [Amin Ghadersohi]. The OpenSearch dialect uses `_plugins/_sql` by default (OpenSearch 3 removed `_opendistro/_sql`) and falls back once to `_opendistro/_sql` for Open Distro / Elasticsearch 7.10; single-table columns compile unqualified; aliases are listed as views in v2 mode.
- fix: SQLAlchemy 2 and DB-API correctness fixes found by live testing (#124) [Amin Ghadersohi]
  - follow the SQL cursor across pages on Elasticsearch and OpenSearch, so results larger than `fetch_size` are no longer cut to the first page; Elasticsearch cursor pagination by Evan Rusackas (from #123)
  - never render the dummy `default` schema, including on projected columns
  - reflect byte/short as `SmallInteger`, unsigned_long as `BigInteger` and date_nanos as `DateTime`; unknown result types no longer raise `KeyError`
  - table/view listing and the OpenSearch `SELECT 1` ping work without cluster privileges; `has_table` is true for aliases
  - `server_version_info` from the cluster; SQL compilation cache enabled
  - boolean URL arguments (`v2`, `verify_certs`, ...) accept `true/1/yes/on` and `false/0/no/off`

#### Behaviour changes / upgrade notes

- **`time_zone` on the OpenSearch dialect (`odelasticsearch`)** is logged as ignored and dropped (UTC, `Z` and `+00:00` silently). The SQL plugin always ignored it and returns UTC. *Migration:* none required; remove `time_zone` from the URL to silence the warning and convert to local time in the application.
- **Temporal values are returned as `datetime.datetime` / `datetime.date` / `datetime.time`** instead of strings. Elasticsearch values keep their offset (tz-aware); OpenSearch values are naive UTC. Columns whose values `datetime` cannot hold exactly (nanoseconds) stay strings. *Migration:* code that parsed these strings should use the objects directly (or `str()`/`isoformat()` them).
- **`double`, `float`, `half_float` and `scaled_float` columns are reflected as `Float`** (Python `float`) instead of `Numeric` (`Decimal` rounded to 10 places). *Migration:* code expecting `Decimal` should convert explicitly.
- **Client exceptions are raised as DB-API exceptions** from `es.exceptions`: connection and TLS errors, authentication/authorization errors as `OperationalError`; `RequestError` and `NotFoundError` as `ProgrammingError`; every other `TransportError` as `DatabaseError`. The original `elasticsearch.*` / `opensearchpy.*` exception is chained as `__cause__`. *Migration:* catch `es.exceptions.*` (or the DB-API classes) instead of `elasticsearch.exceptions.*` / `opensearchpy.exceptions.*`, or inspect `__cause__`.
- **The default OpenSearch `sql_path` is `_plugins/_sql`**, with one fallback to `_opendistro/_sql` when the cluster has no `_plugins/_sql` endpoint (`no handler found`, or Elasticsearch 7.10's `invalid_index_name_exception` for `_plugins`). A proxy or IAM policy that only allows `_opendistro/_sql` answers 403, which is not a missing endpoint, so there is no fallback. *Migration:* add `sql_path=_opendistro/_sql` to the URL in that case (an explicit `sql_path` is never replaced).
- **An unpaged OpenSearch result that may have been cut at `plugins.query.size_limit` raises `DataError`** instead of returning partial rows. *Migration:* add a `LIMIT` of at most the size limit, or narrow the query.
- **Full result sets are held in memory.** A DB-API `SELECT` without `LIMIT` used to return at most `fetch_size` (10 000) rows; every page is now fetched. *Migration:* add a `LIMIT` to queries over large indices.
- **Single-table columns compile unqualified on OpenSearch** (`SELECT a FROM t ORDER BY a`), which OpenSearch 3 requires. A column keeps its table qualifier when a label of another expression in the same select has its name. *Migration:* none expected; raw SQL is not affected.
- **In v2 mode, GROUP BY, DISTINCT, aggregates, joins and other statements the SQL plugin cannot page are sent without `fetch_size`**, so the v2 engine returns every bucket instead of the legacy engine's top 200. Plain SELECTs are still paged on OpenSearch (`_plugins/_sql`); on Open Distro (`_opendistro/_sql`), whose v2 engine cannot page, v2 statements are sent unpaged as in 0.2.13, and if OpenSearch refuses a paged request for lack of privileges it is retried once unpaged. `v2=false` from a URL now means false (it used to switch v2 mode on) and keeps the legacy engine; an aggregation that reaches the legacy engine's 200-bucket cap is asked of the v2 engine, which returns every group.
- **Open Distro returns every row of a plain SELECT.** Open Distro 1.13 stops a SELECT without its own LIMIT at `opendistro.query.size_limit` (200) and has SQL cursors off by default; 0.2.13 returned those 200 rows silently. A capped answer is now asked for again with an explicit LIMIT of the 10000-row search window, and beyond that paged with the SQL cursor. A plain SELECT whose own LIMIT is past that window (e.g. SQL Lab's `LIMIT 100001`), which Open Distro refuses, is answered the same way and cut at its LIMIT. *Migration:* results above 10000 rows need `opendistro.sql.cursor.enabled: true` on the cluster, otherwise the query raises `DataError` saying so.

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
