# ElasticSearch DBAPI

[![Build Status](https://travis-ci.org/preset-io/elasticsearch-dbapi.svg?branch=master)](https://travis-ci.org/preset-io/elasticsearch-dbapi)
[![PyPI version](https://badge.fury.io/py/elasticsearch-dbapi.svg)](https://badge.fury.io/py/elasticsearch-dbapi)
[![Coverage Status](https://codecov.io/github/preset-io/elasticsearch-dbapi/coverage.svg?branch=master)](https://codecov.io/github/preset-io/elasticsearch-dbapi)


`elasticsearch-dbapi` Implements a DBAPI (PEP-249) and SQLAlchemy dialect, 
that enables SQL access on elasticsearch clusters for query only access. 
Uses Elastic X-Pack [SQL API](https://www.elastic.co/guide/en/elasticsearch/reference/current/xpack-sql.html)

We are currently building support for `opendistro/_sql` API for AWS ES

This library supports Elasticsearch 7.X versions.

### Installation

```bash
$ pip install elasticsearch-dbapi
```  

To install support for AWS ES:

```bash
$ pip install elasticsearch-dbapi[aws]
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

### Tests

To run unittest launch elasticsearch and kibana (kibana is really not required but is a nice to have)

```bash
$ docker-compose up -d
$ nosetests -v
```

### Special case for sql opendistro endpoint (AWS ES)

AWS ES exposes the opendistro SQL plugin, and it follows a different SQL dialect. 
Because of dialect and API response differences, we provide limited support for opendistro SQL 
on this package using the `odelasticsearch` driver:

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

### Known limitations

This library does not yet support the following features:

- Array type columns are not supported. Elaticsearch SQL does not support them either. 
SQLAlchemy `get_columns` will exclude them.
- `object` and `nested` column types are not well supported and are converted to strings
- Indexes that whose name start with `.`
- GEO points are not currently well-supported and are converted to strings
- Very limited support for AWS ES, no AWS Auth yet for example
