from es.api import connect
import sqlalchemy
from sqlalchemy.engine import create_engine
from sqlalchemy.schema import *

conn = connect(host='localhost')
curs = conn.cursor()
curs.execute("select 'agent', clientip, machine.ram from kibana_sample_data_logs LIMIT 10")
curs.execute("SHOW COLUMNS FROM kibana_sample_data_logs")
for row in curs:
    print(row)

curs.execute("SHOW TABLES")
for row in curs:
    print(row)

engine = create_engine("es+http://localhost:9200/_sql")
rows = engine.connect().execute("select agent, clientip, machine.ram from kibana_sample_data_logs LIMIT 10")
for row in rows:
    print(row)

logs = Table("kibana_sample_data_logs", MetaData(bind=engine), autoload=True)
print(engine.table_names())

metadata = MetaData()
metadata.reflect(bind=engine)
for table in metadata.sorted_tables:
    print(table)
print(logs.columns)
