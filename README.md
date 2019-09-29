ElasticSearch DBAPI
-------------------

elasticsearch-dbapi is a simple DBAPI (PEP-249), that enables SQL access to elasticsearch
clusters for query only access. Also implements bindings for SQLAlchemy. 

Usage:

```python
from es.api import connect

conn = connect(host='localhost')
curs = conn.cursor()
curs.execute("select agent, clientip, machine.ram from kibana_sample_data_logs LIMIT 10")
for row in curs:
    print(row)
```

Using SQLAlchemy execute:

```python
from sqlalchemy.engine import create_engine

engine = create_engine("es+http://localhost:9200/_sql")
rows = engine.connect().execute("select agent, clientip, machine.ram from kibana_sample_data_logs LIMIT 10")
for row in rows:
    print(row)

```

Using SQLAlchemy reflection:

```python

from sqlalchemy.engine import create_engine
from sqlalchemy.schema import Table, MetaData

engine = create_engine("es+http://localhost:9200/_sql")
logs = Table("kibana_sample_data_logs", MetaData(bind=engine), autoload=True)
print(engine.table_names())

metadata = MetaData()
metadata.reflect(bind=engine)
for table in metadata.sorted_tables:
    print(table)
print(logs.columns)
```
