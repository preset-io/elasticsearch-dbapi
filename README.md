# ElasticSearch DBAPI


[![Build Status](https://github.com/preset-io/elasticsearch-dbapi/workflows/Python/badge.svg)](https://github.com/preset-io/elasticsearch-dbapi/actions)
[![PyPI version](https://badge.fury.io/py/elasticsearch-dbapi.svg)](https://badge.fury.io/py/elasticsearch-dbapi)
[![Coverage Status](https://codecov.io/github/preset-io/elasticsearch-dbapi/coverage.svg?branch=master)](https://codecov.io/github/preset-io/elasticsearch-dbapi)


`elasticsearch-dbapi` Implements a DBAPI (PEP-249) and SQLAlchemy dialect,
that enables SQL access on elasticsearch clusters for query only access.

On Elastic Elasticsearch:
Uses Elastic X-Pack [SQL API](https://www.elastic.co/guide/en/elasticsearch/reference/current/xpack-sql.html)

On AWS ES, opendistro Elasticsearch:
[Open Distro SQL](https://opendistro.github.io/for-elasticsearch-docs/docs/sql/)

This library supports Elasticsearch 7.X versions.

### Installation

```bash
$ pip install elasticsearch-dbapi
```

To install support for AWS Elasticsearch Service / [Open Distro](https://opendistro.github.io/for-elasticsearch/features/SQL%20Support.html):

```bash
$ pip install elasticsearch-dbapi[opendistro]
```

### Usage:

#### Using DBAPI:

```python
from es.elastic.api import connect

conn = connect(host='localhost')
curs = conn.cursor()
curs.execute(
    "select * from flights LIMIT 10"
)
print([row for row in curs])
```

#### Using SQLAlchemy execute:

```python
from sqlalchemy.engine import create_engine

engine = create_engine("elasticsearch+http://localhost:9200/")
rows = engine.connect().execute(
    "select * from flights LIMIT 10"
)
print([row for row in rows])
```

#### Using SQLAlchemy:

```python
from sqlalchemy import func, select
from sqlalchemy.engine import create_engine
from sqlalchemy.schema import MetaData, Table


engine = create_engine("elasticsearch+http://localhost:9200/")
logs = Table("flights", MetaData(bind=engine), autoload=True)
count = select([func.count("*")], from_obj=logs).scalar()
print(f"COUNT: {count}")
```

#### Using SQLAlchemy reflection:

```python

from sqlalchemy.engine import create_engine
from sqlalchemy.schema import Table, MetaData

engine = create_engine("elasticsearch+http://localhost:9200/")
logs = Table("flights", MetaData(bind=engine), autoload=True)
print(engine.table_names())

metadata = MetaData()
metadata.reflect(bind=engine)
print([table for table in metadata.sorted_tables])
print(logs.columns)
```

#### Connection Parameters:

[elasticsearch-py](https://elasticsearch-py.readthedocs.io/en/master/index.html)
is used to establish connections and transport, this is the official
elastic python library. `Elasticsearch` constructor accepts multiple optional parameters
that can be used to properly configure your connection on aspects like security, performance
and high availability. These optional parameters can be set at the connection string, for
example:

 ```bash
    elasticsearch+http://localhost:9200/?http_compress=True&timeout=100
```
will set transport to use gzip (http_compress) and timeout to 10 seconds.

For more information on configuration options, look at `elasticsearch-py`’s documentation:
- [Transport Options](https://elasticsearch-py.readthedocs.io/en/master/connection.html#transport)
- [HTTP tranport](https://elasticsearch-py.readthedocs.io/en/master/transports.html#urllib3httpconnection)

The connection string follows RFC-1738, to support multiple nodes you should use `sniff_*` parameters

#### Fetch size

By default the maximum number of rows which get fetched by a single query
is limited to 10000. This can be adapted through the `fetch_size`
parameter:

```python
from es.elastic.api import connect

conn = connect(host="localhost", fetch_size=1000)
curs = conn.cursor()
```

If more than 10000 rows should get fetched then
[max_result_window](https://www.elastic.co/guide/en/elasticsearch/reference/7.x/index-modules.html#dynamic-index-settings)
has to be adapted as well.

#### Time zone

By default, elasticsearch query time zone defaults to `Z` (UTC). This can be adapted through the `time_zone`
parameter:

```python
from es.elastic.api import connect

conn = connect(host="localhost", time_zone="Asia/Shanghai")
curs = conn.cursor()
```

### Tests

To run unittest launch elasticsearch and kibana (kibana is really not required but is a nice to have)

```bash
$ docker-compose up -d
$ nosetests -v
```

### Special case for sql opendistro endpoint (AWS ES)

AWS ES exposes the opendistro SQL plugin, and it follows a different SQL dialect.
Using the `odelasticsearch` driver:

```python
from sqlalchemy.engine import create_engine

engine = create_engine(
    "odelasticsearch+https://search-SOME-CLUSTER.us-west-2.es.amazonaws.com:443/"
)
rows = engine.connect().execute(
    "select count(*), Carrier from flights GROUP BY Carrier"
)
print([row for row in rows])
```

Or using DBAPI:
```python
from es.opendistro.api import connect

conn = connect(host='localhost',port=9200,path="", scheme="http")

curs = conn.cursor().execute(
    "select * from flights LIMIT 10"
)

print([row for row in curs])
```

### Opendistro (AWS ES) Basic authentication

Basic authentication is configured as expected on the <username>,<password> fields of the URI

```python
from sqlalchemy.engine import create_engine

engine = create_engine(
    "odelasticsearch+https://my_user:my_password@search-SOME-CLUSTER.us-west-2.es.amazonaws.com:443/"
)
```

IAM AWS Authentication keys are passed on the URI basic auth location, and by setting `aws_keys`

Query string keys are:

- aws_keys
- aws_region

```python
from sqlalchemy.engine import create_engine

engine = create_engine(
    "odelasticsearch+https://<AWS_ACCESS_KEY>:<AWS_SECRET_KEY>@search-SOME-CLUSTER.us-west-2.es.amazonaws.com:443/?aws_keys=1&&aws_region=<AWS_REGION>"
)
```

IAM AWS profile is configured has a query parameter name `aws_profile` on the URI. The value for the key provides the AWS region

```python
from sqlalchemy.engine import create_engine

engine = create_engine(
    "odelasticsearch+https://search-SOME-CLUSTER.us-west-2.es.amazonaws.com:443/?aws_profile=us-west-2"
)
```

Using the new SQL engine:

Opendistro 1.13.0 brings (enabled by default) a new SQL engine, with lots of improvements and fixes.
Take a look at the [release notes](https://github.com/opendistro-for-elasticsearch/sql/blob/develop/docs/dev/NewSQLEngine.md)

This DBAPI has to behave slightly different for SQL v1 and SQL v2, by default we comply with v1,
to enable v2 support, pass `v2=true` has a query parameter.

```
odelasticsearch+https://search-SOME-CLUSTER.us-west-2.es.amazonaws.com:443/?aws_profile=us-west-2&v2=true
```

To connect to the provided Opendistro ES on `docker-compose` use the following URI:
`odelasticsearch+https://admin:admin@localhost:9400/?verify_certs=False`

### Known limitations

This library does not yet support the following features:

- Array type columns are not supported. Elaticsearch SQL does not support them either.
SQLAlchemy `get_columns` will exclude them.
- `object` and `nested` column types are not well supported and are converted to strings
- Indexes that whose name start with `.`
- GEO points are not currently well-supported and are converted to strings

- AWS ES (opendistro elascticsearch) is supported (still beta), known limitations are:
  * You are only able to `GROUP BY` keyword fields (new [experimental](https://github.com/opendistro-for-elasticsearch/sql#experimental)
 opendistro SQL already supports it)
  * Indices with dots are not supported (indices like 'audit_log.2021.01.20'),
  on these cases we recommend the use of aliases
