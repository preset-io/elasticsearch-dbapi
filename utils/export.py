# Simple util to export indexes from Elasticsearch

import json
import requests

index = "kibana_sample_data_flights"
headers = {"Content-Type": "application/json"}
base_url = "http://localhost:9200"
url = f"{base_url}/{index}/_search?size=100"
file_path = "kibana_sample_data_flights.json"
r = requests.get(url, headers=headers)
data = [doc["_source"] for doc in r.json()["hits"]["hits"]]

fd = open(file_path, "w")
json.dump(data, fd)
fd.close()
