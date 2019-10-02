from es.elastic import connect
from sqlalchemy.engine import create_engine

conn = connect(host='localhost')
curs = conn.cursor()
curs.execute("select agent from kibana_sample_data_logs LIMIT 10")
curs.execute("SHOW COLUMNS FROM kibana_sample_data_logs")
for row in curs:
    print(row)
#
# curs.execute("SHOW TABLES")
# for row in curs:
#     print(row)
#
engine = create_engine("es+http://localhost:9200/")
rows = engine.connect().execute("select * from flights")
rows = engine.connect().execute("select agent, clientip, machine.ram from kibana_sample_data_logs LIMIT 10")
for row in rows:
     print(row)

#logs = Table("kibana_sample_data_logs", MetaData(bind=engine), autoload=True)
# print(engine.table_names())
#
#metadata = MetaData()
#metadata.reflect(bind=engine)
#for table in metadata.sorted_tables:
#    print(table)
#    print(logs.columns)
