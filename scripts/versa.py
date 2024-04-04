#!/usr/bin/python3

import requests, time

# Authentication:

DATABASE = "versa-amazon"
USERNAME = "root"
PASSWORD = "xyz"

# Set URLs:

DEPLOYMENT_URL = "https://394fdb0cff5d.arangodb.cloud"
ARANGODB_URL = DEPLOYMENT_URL + ":8529"
ENGINE_URL = DEPLOYMENT_URL + ":8829/graph-analytics/engines/pyszq5imvj6lnypxfgme"

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
        if not "progress" in j:
            j["progress"] = 0
        print("Job ", id, " still running, progress is ", j["progress"], " out of ", j["total"], " ...")
        if j["progress"] >= j["total"]:
            return
        time.sleep(0.5)

# Load data:
what = { "database": DATABASE, \
         "vertex_collections": ["APPLIANCE", "APPLICATION", "DESTADDRESS", "LOCATION", "TENANT", "URL", "USER", "GATEWAY"], \
         "vertex_attributes": ["_id", "@collectionname"], \
         "edge_collections": ["APPLICATION_FROM_URL", "URL_ACCESSES_DESTADDRESS", "USER_ACCESSES_APPLICATION", "USER_ACCESSES_DESTADDRESS", "USER_ACCESSES_URL", "USER_ACCESSESFROM_LOCATION", "USER_BELONGSTO_TENANT", "USER_CONNECTSTO_APPLIANCE", "USER_CONNECTSTO_GATEWAY", "TENANT_ON_APPLIANCE"], \
         "parallelism": 10, \
         "batchSize": 4000000 }
graph_id = post("/v1/loaddata", what)
load_job_id = int(graph_id["jobId"])
graph_id = int(graph_id["graphId"])

wait_job_complete(load_job_id)

# Run algorithms:
body_wcc = {"graphId": graph_id}
wcc_job_id = int(post("/v1/wcc", body_wcc)["jobId"])

body_irank = {"graphId": graph_id, "damping_factor": 0.85, "maximum_supersteps": 64}
irank_job_id = int(post("/v1/irank", body_irank)["jobId"])

body_labelprop = {"graphId": graph_id, "start_label_attribute": "_id", "synchronous": False, "random_tiebreak": False}
labelprop_job_id = int(post("/v1/labelpropagation", body_labelprop)["jobId"])

wait_job_complete(wcc_job_id)
wait_job_complete(irank_job_id)
wait_job_complete(labelprop_job_id)

# Write result back to another collection:
body = { "jobIds": [wcc_job_id, irank_job_id, labelprop_job_id], \
  "attributeNames": ["wcc", "irank", "lab"], \
  "vertexCollections": {}, \
  "database": DATABASE, \
  "targetCollection": "results", \
  "parallelism": 4, \
  "batchSize": 10000 }
store_job_id = int(post("/v1/storeresults", body)["jobId"])

wait_job_complete(store_job_id)

# Cleanup:

delete("/v1/jobs", load_job_id)
delete("/v1/jobs", wcc_job_id)
delete("/v1/jobs", irank_job_id)
delete("/v1/jobs", labelprop_job_id)
delete("/v1/jobs", store_job_id)
delete("/v1/graphs", graph_id)


