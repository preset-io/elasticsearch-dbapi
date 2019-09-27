from es.db import connect
from sqlalchemy.engine import create_engine
from sqlalchemy.schema import *

conn = connect(host='localhost')
curs = conn.cursor()
curs.execute("select agent, clientip from kibana_sample_data_logs LIMIT 10")
for row in curs:
    print(row)

curs.execute("SHOW TABLES")
for row in curs:
    print(row)

engine = create_engine("es://localhost:9200/?server=http://localhost:9200/")
places = Table("kibana_sample_data_logs", MetaData(bind=engine), autoload=True)
#print(select([func.count('*')], from_obj=places).scalar())
