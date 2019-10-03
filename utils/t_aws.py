from es.aws import connect
from sqlalchemy.engine import create_engine
from sqlalchemy.schema import Table, MetaData

host = "search-test-uuid.us-west-2.es.amazonaws.com"
conn = connect(
    host=host
)
curs = conn.cursor()
curs.execute("SHOW TABLES")
for row in curs:
    print(row)
curs.execute("SHOW COLUMNS FROM flights")
for row in curs:
    print(row)


engine = create_engine(f"esaws+https://{host}:443/")
rows = engine.connect().execute("select count(*), Carrier from flights GROUP BY Carrier")
for row in rows:
     print(row)

logs = Table("flights", MetaData(bind=engine), autoload=True)
print(engine.table_names())

metadata = MetaData()
metadata.reflect(bind=engine)
for table in metadata.sorted_tables:
    print(table)
    print(logs.columns)
