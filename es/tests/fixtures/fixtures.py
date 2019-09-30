import json

import requests


def import_file_to_es(base_url, file_path, index_name):
    headers = {"Content-Type": "application/json"}
    url = f"{base_url}/{index_name}/_doc"
    fd = open(file_path, "r")
    data = json.load(fd)
    fd.close()

    for doc in data:
        r = requests.post(url, headers=headers, json=doc)
        if r.status_code != 201:
            print(f"Error {r.status_code}")
