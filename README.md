# ElasticSearch DBAPI

[![Build Status](https://travis-ci.org/apache/incubator-superset.svg?branch=master)](https://travis-ci.org/dpgaspar/es-dbapi)
[![PyPI version](https://badge.fury.io/py/es-dbapi.svg)](https://badge.fury.io/py/es-dbapi)
[![Coverage Status](https://codecov.io/github/preset-io/es-dbapi/coverage.svg?branch=master)](https://codecov.io/github/preset-io/es-dbapi)


`es-dbapi` Implements a DBAPI (PEP-249) and SQLAlchemy dialect, 
that enables SQL access to elasticsearch clusters for query only access. 
Uses Elastic X-Pack [SQL API](https://www.elastic.co/guide/en/elasticsearch/reference/current/xpack-sql.html)

We are currently building support for `opendistro/_sql` API for AWS ES

##### Elasticsearch version > 7 (may work with > 6.5)

### Install

```bash
$ pip install es-dbapi
```  

To install support for AWS ES:

```bash
$ pip install es-dbapi[aws]
```  

### Usage:

Using DBAPI:

```python
from es.elastic.api import connect

conn = connect(host='localhost')
curs = conn.cursor()
curs.execute(
    "select * from flights LIMIT 10"
)
print([row for row in curs])
```

Using SQLAlchemy execute:

```python
from sqlalchemy.engine import create_engine

engine = create_engine("es+http://localhost:9200/")
rows = engine.connect().execute(
    "select * from flights LIMIT 10"
)
print([row for row in rows])
```

Using SQLAlchemy:

```python
from sqlalchemy import func, select
from sqlalchemy.engine import create_engine
from sqlalchemy.schema import MetaData, Table


engine = create_engine("es+http://localhost:9200/")
logs = Table("flights", MetaData(bind=engine), autoload=True)
count = select([func.count("*")], from_obj=logs).scalar()
print(f"COUNT: {count}")
```

Using SQLAlchemy reflection:

```python

from sqlalchemy.engine import create_engine
from sqlalchemy.schema import Table, MetaData

engine = create_engine("es+http://localhost:9200/")
logs = Table("flights", MetaData(bind=engine), autoload=True)
print(engine.table_names())

metadata = MetaData()
metadata.reflect(bind=engine)
print([table for table in metadata.sorted_tables])
print(logs.columns)
```

### Tests

To run unittest launch elasticsearch and kibana (kibana is really not required but is a nice to have)

```bash
$ docker-compose up -d
$ nosetests -v
```

### Special case for sql opendistro endpoint (AWS ES)

AWS ES exposes opendistro SQL plugin, and it follows a different SQL dialect. 
Because of the dialect differences and API response, `opendistro SQL` is supported
on this package using a different driver `esaws`:

```python
from sqlalchemy.engine import create_engine

engine = create_engine(
    "esaws+https://search-SOME-CLUSTER.us-west-2.es.amazonaws.com:443/"
)
rows = engine.connect().execute(
    "select count(*), Carrier from flights GROUP BY Carrier"
)
print([row for row in rows])
```

### Known limitations

This library does not yet support the following features:

- Array type columns, Elaticsearch SQL does not support it either 
(lib get_columns will exclude these columns)
- Proper support for GEO points
- Very limited support for AWS ES, no auth yet for example
