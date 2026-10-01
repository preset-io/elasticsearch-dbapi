## Change log

### 0.3.0

- feat: OpenSearch 3 support (#124) [Amin Ghadersohi]. The OpenSearch dialect uses `_plugins/_sql` by default (OpenSearch 3 removed `_opendistro/_sql`) and falls back once to `_opendistro/_sql` for Open Distro / Elasticsearch 7.10; single-table columns compile unqualified; aliases are listed as views in v2 mode.
- fix: SQLAlchemy 2 and DB-API correctness fixes found by live testing (#124) [Amin Ghadersohi]
  - follow the SQL cursor across pages on Elasticsearch and OpenSearch, so results larger than `fetch_size` are no longer cut to the first page; Elasticsearch cursor pagination by Evan Rusackas (from #123)
  - never render the dummy `default` schema, including on projected columns
  - reflect byte/short as `SmallInteger`, unsigned_long as `BigInteger` and date_nanos as `DateTime`; unknown result types no longer raise `KeyError`
  - table/view listing and the OpenSearch `SELECT 1` ping work without cluster privileges; `has_table` is true for aliases
  - `server_version_info` from the cluster; SQL compilation cache enabled
  - boolean URL arguments (`v2`, `verify_certs`, ...) accept `true/1/yes/on` and `false/0/no/off`
- fix: reflected columns report the field's type (`DOUBLE`, `FLOAT`, `INTEGER`, `LONG`, `BOOLEAN`) instead of `LONG`/`FLOAT` for every numeric and boolean field (#126) [Amin Ghadersohi]. Reflected types retain their generic SQLAlchemy compilation on other dialects, including when copying a table to another database.

#### Behaviour changes / upgrade notes

- **`time_zone` on the OpenSearch dialect (`odelasticsearch`)** is logged as ignored and dropped (UTC, `Z` and `+00:00` silently). The SQL plugin always ignored it and returns UTC. *Migration:* none required; remove `time_zone` from the URL to silence the warning and convert to local time in the application.
- **Supported SQL temporal values are decoded to `datetime.datetime` / `datetime.date` / `datetime.time`** instead of strings. Timezone-aware Elasticsearch values are normalised to UTC to avoid applying one row's DST offset to an entire result column; decoded OpenSearch timestamps remain naive UTC. This is not a guarantee that every date-mapped column returns an object: conversion depends on the SQL engine's result type and value, and nanosecond or unrecognised representations remain strings. In particular, legacy OpenSearch results can label a full timestamp as `date`; previously even `v2=true` queries with LIMIT could take that legacy path. LIMIT queries on a v2 connection now stay on v2, preserving its functions and timestamp result types. *Migration:* accept both temporal objects and preserved strings; use objects directly (or `str()`/`isoformat()` them), and do not discard precision when parsing preserved strings. Convert UTC-aware values to the desired display timezone explicitly.
- **`double`, `float`, `half_float` and `scaled_float` columns are reflected as `Float`** (Python `float`) instead of `Numeric` (`Decimal` rounded to 10 places). *Migration:* code expecting `Decimal` should convert explicitly.
- **Client exceptions are raised as DB-API exceptions** from `es.exceptions`: connection and TLS errors, authentication/authorization errors as `OperationalError`; `RequestError` and `NotFoundError` as `ProgrammingError`; every other `TransportError` as `DatabaseError`. The original `elasticsearch.*` / `opensearchpy.*` exception is chained as `__cause__`. *Migration:* catch `es.exceptions.*` (or the DB-API classes) instead of `elasticsearch.exceptions.*` / `opensearchpy.exceptions.*`, or inspect `__cause__`.
- **The default OpenSearch `sql_path` is `_plugins/_sql`**, with one fallback to `_opendistro/_sql` when the cluster has no `_plugins/_sql` endpoint (`no handler found`, or Elasticsearch 7.10's `invalid_index_name_exception` for `_plugins`, which is `index_not_found_exception` with `action.auto_create_index=false`). A proxy or IAM policy that only allows `_opendistro/_sql` answers 403, which is not a missing endpoint, so there is no fallback. *Migration:* add `sql_path=_opendistro/_sql` to the URL in that case (an explicit `sql_path` is never replaced).
- **An unpaged OpenSearch result that may have been cut at `plugins.query.size_limit` raises `DataError`** instead of returning partial rows. *Migration:* add a `LIMIT` of at most the size limit, or narrow the query.
- **Full result sets are held in memory.** A DB-API `SELECT` without `LIMIT` used to return at most `fetch_size` (10 000) rows; every page is now fetched. *Migration:* add a `LIMIT` to queries over large indices.
- **Single-table columns compile unqualified on OpenSearch** (`SELECT a FROM t ORDER BY a`), which OpenSearch 3 requires. A column keeps its table qualifier when a label of another expression in the same select has its name. *Migration:* none expected; raw SQL is not affected.
- **In v2 mode, GROUP BY, DISTINCT, aggregates, joins and other statements the SQL plugin cannot page are sent without `fetch_size`**, avoiding the legacy engine's default top-200 result. This does **not** mean the v2 engine returns every bucket: unpaged aggregations stop at a backend bucket ceiling and cannot be cursor-paged. That ceiling is 1000 buckets on Open Distro and OpenSearch 1.x; OpenSearch 2.x/3.x also stop at `plugins.query.size_limit` when it is below 1000, which is the default on 2.11–2.15 (200), on either `sql_path`. A result at the applicable ceiling without an explicit LIMIT of at most that ceiling now raises `DataError` instead of silently returning a partial answer, so exactly 200 groups raise on OpenSearch 2.11–2.15 at the default size limit (or when the server version cannot be read), and are accepted where 200 is not a ceiling (Open Distro, OpenSearch 1.x, and 2.19/3.x at their default size limit). Plain SELECTs without LIMIT are still paged on OpenSearch (`_plugins/_sql`); on Open Distro (`_opendistro/_sql`), whose v2 engine cannot page, v2 statements are sent unpaged as in 0.2.13, and if OpenSearch refuses a paged request for lack of privileges it is retried once unpaged. `v2=false` from a URL now means false (it used to switch v2 mode on) and keeps the legacy engine; an aggregation that reaches the legacy engine's 200-bucket cap is asked of the v2 engine, rather than assuming those 200 groups are complete. *Migration:* narrow high-cardinality aggregations or use an explicit bounded LIMIT; do not assume an unpaged aggregate is complete merely because v2 is enabled.
- **Statements over subqueries return correct results** (e.g. Superset virtual datasets). A subquery without a LIMIT used to stop at the size limit (200 rows on Open Distro) and silently drop rows from the outer result; the legacy engine (v2 off) ignored the outer WHERE; and Open Distro dropped a row when an outer WHERE and LIMIT met without ORDER BY. Subqueries now get an explicit LIMIT of one search window, run on the v2 engine, and on Open Distro the outer LIMIT is applied to the rows.
- **A Superset virtual dataset (or any statement over a subquery) whose subquery returns more than 10000 rows raises `DataError`** on OpenSearch and Open Distro, e.g. a virtual dataset of `SELECT * FROM idx` over an index with 10k+ documents. 0.2.13 answered these with silently wrong results (a filtered `COUNT` of 0 where the true count was 9). The SQL plugin cannot page a subquery, so no complete answer exists. On Open Distro, which refuses to count past the window, a subquery of exactly 10000 rows raises too. *Migration:* narrow the subquery (the virtual dataset's SQL) with a WHERE, an aggregation or a LIMIT of at most 10000, or use a physical dataset.
- **Reflected column types render differently.** `double`, `float`, `integer`, `long` and `boolean` fields report `DOUBLE`, `FLOAT`, `INTEGER`, `LONG` and `BOOLEAN`; 0.2.13 reported `LONG` for `double`, `integer`, `long` and `boolean`. `byte` and `short` report `INTEGER`, `half_float` `FLOAT`, `scaled_float` `DOUBLE` and `unsigned_long` `LONG` (0.2.13 reported `STRING` for all but `half_float`): the SQL plugins cannot `CAST` to their own names, and Superset's default `column_type_mappings` do not know them. Superset recognises every rendered name, so these numeric fields are now numeric and boolean fields are boolean rather than numeric. *Migration:* none for Superset; code comparing the rendered type strings should expect the new names.
- **An explicit `LIMIT` on a v2 plain SELECT bypasses `plugins.query.size_limit`**, so only the 10000-row search window is checked (an exactly-200-row answer to `LIMIT 1001` is complete, not truncated). A plain SELECT whose LIMIT is above 10000 is replayed without the LIMIT through SQL cursors and cut at its LIMIT locally; if cursors are unavailable (including a limited cursor whose next page is empty) it raises `DataError`. *Migration:* none; results above 10000 rows need SQL cursors on the cluster.
- **Grouped top-N on the v2 engine is validated.** The v2 engine sorts `GROUP BY ... ORDER BY COUNT(*) DESC LIMIT 5` only after its capped composite aggregation, so it can return the wrong five groups. The driver probes the unsorted group listing (one additional SQL request) and raises `DataError` when that listing reaches its bucket ceiling. Ordering only by grouped fields (`GROUP BY k ORDER BY k DESC LIMIT 3`) is sorted inside the aggregation, so it is not probed. *Migration:* narrow the grouping, or use `v2=false` for legacy top-N.
- **OFFSET is an exception to the grouped-field ordering shortcut:** `SELECT k, COUNT(*) FROM t GROUP BY k ORDER BY k LIMIT 3 OFFSET 999` is probed and raises `DataError` at the bucket ceiling instead of silently returning one row from 5000 groups.
- **OpenSearch 1.x v2 plain SELECTs stay on the v2 engine** (paging them would hand them to the legacy engine), preserving floating values and timestamp objects. The server version is discovered once per connection, best effort; when it cannot be read, unpaged v2 semantics are preferred to legacy paging.
- **The legacy engine no longer confuses a table-qualified column with a select-list alias of the same name** (`ORDER BY grp.k` next to `v AS k` sorted by `v`): such aliases are renamed in the statement sent and restored in the result.
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
