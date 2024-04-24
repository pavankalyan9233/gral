#!/usr/bin/python3

import requests, time

# Authentication:

DATABASE = "d"
USERNAME = "root"
PASSWORD = "xyz"

# Set URLs:

DEPLOYMENT_URL = "https://632512532532.arangodb.cloud"
ARANGODB_URL = DEPLOYMENT_URL + ":8529"
ENGINE_URL = DEPLOYMENT_URL + ":8829/graph-analytics/engines/qgexbcxmktkxadqxd9pn"

# Get auth token:
jwttoken = requests.post(ARANGODB_URL + "/_open/auth", json = {"username": USERNAME, "password": PASSWORD}).json()["jwt"]
authheader = {"Authorization": "Bearer " + jwttoken}

def post(path, body):
    req = requests.post(ENGINE_URL + path, json = body, headers = authheader)
    if req.status_code < 200 or req.status_code > 299:
        raise Exception("Post error: ", req)
    return req.json()

def delete(path, id):
    req = requests.delete(ENGINE_URL + path + "/" + str(id), headers = authheader)
    if req.status_code < 200 or req.status_code > 299:
        raise Exception("Post error: ", req)
    return req.json()

def wait_job_complete(id):
    while True:
        r = requests.get(ENGINE_URL + "/v1/jobs/" + str(id), headers = authheader)
        if r.status_code < 200 or r.status_code > 299:
            raise Exception("Banana")
        j = r.json()
        if j["progress"] >= j["total"]:
            return
        print("Job ", id, " still running, progress is ", j["progress"], " out of ", j["total"], " ...")
        time.sleep(0.5)

# Load data:
what = { "database": DATABASE, \
         "vertex_collections": ["healthcare_Field"], \
         "vertex_attributes": ["_id"], \
         "edge_collections": ["helper_similarity_fields_3"], \
         "parallelism": 10, \
         "batch_size": 4000000 }
graph_id = post("/v1/loaddata", what)
load_job_id = int(graph_id["job_id"])
graph_id = int(graph_id["graph_id"])

wait_job_complete(load_job_id)

# Run algorithm:
body = {"graph_id": graph_id}
wcc_job_id = int(post("/v1/wcc", body)["job_id"])

wait_job_complete(wcc_job_id)

# Write result back to another collection:
body = { "job_ids": [wcc_job_id], \
  "attribute_names": ["wcc"], \
  "vertex_collections": {}, \
  "database": DATABASE, \
  "target_collection": "helper_wcc_3", \
  "parallelism": 4, \
  "batch_size": 10000 }
store_job_id = int(post("/v1/storeresults", body)["job_id"])

wait_job_complete(store_job_id)

# Cleanup:

delete("/v1/jobs", load_job_id)
delete("/v1/jobs", wcc_job_id)
delete("/v1/jobs", store_job_id)
delete("/v1/graphs", graph_id)


