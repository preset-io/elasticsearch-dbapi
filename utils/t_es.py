from es.elastic import connect
from sqlalchemy.engine import create_engine
from sqlalchemy.schema import Table, MetaData

conn = connect("http://user:password@localhost:9200/")
curs = conn.cursor()
# curs.execute("select agent from kibana_sample_data_logs LIMIT 10")
# curs.execute("SHOW COLUMNS FROM kibana_sample_data_logs")
# for row in curs:
#     print(row)
#
engine = create_engine("es+http://user:password@localhost:9200/")
rows = engine.connect()
#rows = engine.connect().execute("select * from flights")
# rows = engine.connect().execute("select agent, clientip, machine.ram from kibana_sample_data_logs LIMIT 10")
# for row in rows:
#      print(row)
#
# logs = Table("kibana_sample_data_flights", MetaData(bind=engine), autoload=True)
# print(engine.table_names())
# #
# metadata = MetaData()
# metadata.reflect(bind=engine)
# for table in metadata.sorted_tables:
#     print(table)
#     print(logs.columns)
