use crate::api::graphanalyticsengine::{
    GraphAnalyticsEngineLoadDataRequest, GraphAnalyticsEngineStoreResultsRequest,
};
use crate::args::GralArgs;
use crate::auth::create_jwt_token;
use crate::computations::{Computation, LoadComputation, StoreComputation};
use crate::graph_store::graph::{Edge, Graph};
use byteorder::WriteBytesExt;
use bytes::Bytes;
use log::{debug, error, info};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, RwLock, RwLockReadGuard};
use std::thread::JoinHandle;
use std::time::SystemTime;
use tokio::task::JoinSet;
use warp::http::StatusCode;

#[derive(Debug, Serialize, Deserialize)]
struct ShardLocation {
    leader: String,
    followers: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct CollectionDistribution {
    plan: HashMap<String, ShardLocation>,
    current: HashMap<String, ShardLocation>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ShardDistribution {
    error: bool,
    code: i32,
    results: HashMap<String, CollectionDistribution>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ArangoDBError {
    error: bool,
    error_num: i32,
    error_message: String,
    code: i32,
}

fn build_client(use_tls: bool, ca: &[u8]) -> Result<reqwest::Client, String> {
    let builder = reqwest::Client::builder();
    if use_tls {
        let client = if ca.is_empty() {
            builder
                .use_rustls_tls()
                .min_tls_version(reqwest::tls::Version::TLS_1_2)
                .danger_accept_invalid_certs(true)
                .https_only(true)
                .build()
        } else {
            let cert = reqwest::Certificate::from_pem(ca);
            if let Err(err) = cert {
                return Err(format!(
                    "Could not parse CA certificate for ArangoDB: {:?}",
                    err
                ));
            }
            builder
                .use_rustls_tls()
                .min_tls_version(reqwest::tls::Version::TLS_1_2)
                .add_root_certificate(cert.unwrap())
                .https_only(true)
                .build()
        };
        if let Err(err) = client {
            return Err(format!("Error message from request builder: {:?}", err));
        }
        Ok(client.unwrap())
    } else {
        let client = builder
            //.connection_verbose(true)
            //.http2_prior_knowledge()
            .build();
        if let Err(err) = client {
            return Err(format!("Error message from request builder: {:?}", err));
        }
        Ok(client.unwrap())
    }
}

// This function handles an HTTP response from ArangoDB, including
// connection errors, bad status codes and body parsing. The template
// type is the type of the expected body in the good case.
async fn handle_arangodb_response_with_parsed_body<T>(
    resp: reqwest::Result<reqwest::Response>,
    expected_code: reqwest::StatusCode,
) -> Result<T, String>
where
    T: serde::de::DeserializeOwned,
{
    if let Err(err) = resp {
        return Err(err.to_string());
    }
    let resp = resp.unwrap();
    let status = resp.status();
    if status != expected_code {
        let err = resp.json::<ArangoDBError>().await;
        match err {
            Err(e) => {
                return Err(format!(
                    "Could not parse error body, error: {}, status code: {:?}",
                    e, status,
                ));
            }
            Ok(e) => {
                return Err(format!(
                    "Error code: {}, message: {}, HTTP code: {}",
                    e.error_num, e.error_message, e.code
                ));
            }
        }
    }
    let body = resp.json::<T>().await;
    body.map_err(|e| format!("Could not parse response body, error: {}", e))
}

// This function handles an empty HTTP response from ArangoDB, including
// connection errors and bad status codes.
async fn handle_arangodb_response(
    resp: reqwest::Result<reqwest::Response>,
    code_test: fn(code: reqwest::StatusCode) -> bool,
) -> Result<reqwest::Response, String> {
    if let Err(err) = resp {
        return Err(err.to_string());
    }
    let resp = resp.unwrap();
    let status = resp.status();
    if !code_test(status) {
        let err = resp.json::<ArangoDBError>().await;
        match err {
            Err(e) => {
                return Err(format!(
                    "Could not parse error body, error: {}, status code: {:?}",
                    e, status,
                ));
            }
            Ok(e) => {
                return Err(format!(
                    "Error code: {}, message: {}, HTTP code: {}",
                    e.error_num, e.error_message, e.code
                ));
            }
        }
    }
    Ok(resp)
}

// A ShardMap maps dbserver names to lists of shards for which these dbservers
// are leaders. We will have one for the vertices and one for the edges.

type ShardMap = HashMap<String, Vec<String>>;

fn compute_shard_map(sd: &ShardDistribution, coll_list: &[String]) -> Result<ShardMap, String> {
    let mut result: ShardMap = HashMap::new();
    for c in coll_list.iter() {
        // Handle the case of a smart edge collection. If c is
        // one, then we also find a collection called `_to_`+c.
        // In this case, we must not get those shards, because their
        // data is already contained in `_from_`+c, just sharded
        // differently.
        let mut ignore: HashSet<String> = HashSet::new();
        let smart_name = "_to_".to_owned() + c;
        match sd.results.get(&smart_name) {
            None => (),
            Some(coll_dist) => {
                // Keys of coll_dist are the shards, value has leader:
                for shard in coll_dist.plan.keys() {
                    ignore.insert(shard.clone());
                }
            }
        }
        match sd.results.get(c) {
            None => {
                return Err(format!("collection {} not found in shard distribution", c));
            }
            Some(coll_dist) => {
                // Keys of coll_dist are the shards, value has leader:
                for (shard, location) in &(coll_dist.plan) {
                    if !ignore.contains(shard) {
                        let leader = &(location.leader);
                        match result.get_mut(leader) {
                            None => {
                                result.insert(leader.clone(), vec![shard.clone()]);
                            }
                            Some(list) => {
                                list.push(shard.clone());
                            }
                        }
                    }
                }
            }
        }
    }
    Ok(result)
}

#[derive(Debug, Clone)]
struct DBServerInfo {
    dbserver: String,
    dump_id: String,
}

async fn get_all_shard_data(
    req: &GraphAnalyticsEngineLoadDataRequest,
    endpoints: &[String],
    jwt_token: &String,
    ca_cert: &[u8],
    shard_map: &ShardMap,
    result_channels: Vec<tokio::sync::mpsc::Sender<Bytes>>,
) -> Result<(), String> {
    let begin = SystemTime::now();

    let use_tls = endpoints[0].starts_with("https://");
    let client = build_client(use_tls, ca_cert)?;

    let make_url = |path: &str| -> String { endpoints[0].clone() + "/_db/" + &req.database + path };

    // Start a single dump context on all involved dbservers, we can do
    // this sequentially, since it is not performance critical, we can
    // also use the same HTTP client and the same first endpoint:
    let mut dbservers: Vec<DBServerInfo> = vec![];
    let mut error_happened = false;
    let mut error: String = "".into();
    for (server, shard_list) in shard_map.iter() {
        let url = make_url(&format!("/_api/dump/start?dbserver={}", server));
        let body = DumpStartBody {
            batch_size: req.batch_size,
            prefetch_count: 5,
            parallelism: req.parallelism,
            shards: shard_list.clone(),
        };
        let body_v =
            serde_json::to_vec::<DumpStartBody>(&body).expect("could not serialize DumpStartBody");
        let resp = client
            .post(url)
            .bearer_auth(jwt_token)
            .body(body_v)
            .send()
            .await;
        let r = handle_arangodb_response(resp, |c| {
            c == StatusCode::NO_CONTENT || c == StatusCode::OK || c == StatusCode::CREATED
        })
        .await;
        if let Err(rr) = r {
            error = rr;
            error_happened = true;
            break;
        }
        let r = r.unwrap();
        let headers = r.headers();
        if let Some(id) = headers.get("X-Arango-Dump-Id") {
            if let Ok(id) = id.to_str() {
                dbservers.push(DBServerInfo {
                    dbserver: server.clone(),
                    dump_id: id.to_owned(),
                });
            }
        }
        debug!("Started dbserver {}", server);
    }

    let client_clone_for_cleanup = client.clone();
    let jwt_token_clone = jwt_token.clone();
    let cleanup = |dbservers: Vec<DBServerInfo>| async move {
        debug!("Doing cleanup...");
        for dbserver in dbservers.iter() {
            let url = make_url(&format!(
                "/_api/dump/{}?dbserver={}",
                dbserver.dump_id, dbserver.dbserver
            ));
            let resp = client_clone_for_cleanup
                .delete(url)
                .bearer_auth(&jwt_token_clone)
                .send()
                .await;
            let r =
                handle_arangodb_response(resp, |c| c == StatusCode::OK || c == StatusCode::CREATED)
                    .await;
            if let Err(rr) = r {
                eprintln!(
                    "An error in cancelling a dump context occurred, dbserver: {}, error: {}",
                    dbserver.dbserver, rr
                );
                // Otherwise ignore the error, this is just a cleanup!
            }
        }
    };

    if error_happened {
        // We need to cancel all dump contexts which we did get successfully:
        cleanup(dbservers).await;
        return Err(error);
    }

    // We want to start the same number of tasks for each dbserver, each of
    // them will send next requests until no more data arrives

    #[derive(Debug)]
    struct TaskInfo {
        dbserver: DBServerInfo,
        current_batch_id: u64,
        last_batch_id: Option<u64>,
        id: u64,
    }

    if dbservers.is_empty() {
        // This actually happened writing integration tests, we cannot divide by zero
        error!("No dbserver found. List is empty.");
        return Err("No dbserver found".to_string());
    }

    let par_per_dbserver = (req.parallelism as usize + dbservers.len() - 1) / dbservers.len();
    let mut task_set = JoinSet::new();
    let mut endpoints_round_robin: usize = 0;
    let mut consumers_round_robin: usize = 0;
    for i in 0..par_per_dbserver {
        for dbserver in &dbservers {
            let mut task_info = TaskInfo {
                dbserver: dbserver.clone(),
                current_batch_id: i as u64,
                last_batch_id: None,
                id: i as u64,
            };
            //let client_clone = client.clone(); // the clones will share
            //                                   // the connection pool
            let client_clone = build_client(use_tls, ca_cert)?;
            let endpoint_clone = endpoints[endpoints_round_robin].clone();
            let jwt_token_clone = jwt_token.clone();
            endpoints_round_robin += 1;
            if endpoints_round_robin >= endpoints.len() {
                endpoints_round_robin = 0;
            }
            let database_clone = req.database.clone();
            let result_channel_clone = result_channels[consumers_round_robin].clone();
            consumers_round_robin += 1;
            if consumers_round_robin >= result_channels.len() {
                consumers_round_robin = 0;
            }
            task_set.spawn(async move {
                loop {
                    let mut url = format!(
                        "{}/_db/{}/_api/dump/next/{}?dbserver={}&batchId={}",
                        endpoint_clone,
                        database_clone,
                        task_info.dbserver.dump_id,
                        task_info.dbserver.dbserver,
                        task_info.current_batch_id
                    );
                    if let Some(last) = task_info.last_batch_id {
                        url.push_str(&format!("&lastBatch={}", last));
                    }
                    let start = SystemTime::now();
                    debug!(
                        "{:?} Sending post request... {} {} {}",
                        start.duration_since(begin).unwrap(),
                        task_info.id,
                        task_info.dbserver.dbserver,
                        task_info.current_batch_id
                    );
                    let resp = client_clone
                        .post(url)
                        .bearer_auth(&jwt_token_clone)
                        .send()
                        .await;
                    let resp = handle_arangodb_response(resp, |c| {
                        c == StatusCode::OK || c == StatusCode::NO_CONTENT
                    })
                    .await?;
                    let end = SystemTime::now();
                    let dur = end.duration_since(start).unwrap();
                    if resp.status() == StatusCode::NO_CONTENT {
                        // Done, cleanup will be done later
                        debug!(
                            "{:?} Received final post response... {} {} {} {:?}",
                            end.duration_since(begin).unwrap(),
                            task_info.id,
                            task_info.dbserver.dbserver,
                            task_info.current_batch_id,
                            dur
                        );
                        return Ok::<(), String>(());
                    }
                    // Now the result was OK and the body is JSONL
                    task_info.last_batch_id = Some(task_info.current_batch_id);
                    task_info.current_batch_id += par_per_dbserver as u64;
                    let body = resp
                        .bytes()
                        .await
                        .map_err(|e| format!("Error in body: {:?}", e))?;
                    result_channel_clone
                        .send(body)
                        .await
                        .expect("Could not send to channel!");
                }
            });
        }
    }
    while let Some(res) = task_set.join_next().await {
        let r = res.unwrap();
        match r {
            Ok(_x) => {
                debug!("Got OK result!");
            }
            Err(msg) => {
                debug!("Got error result: {}", msg);
            }
        }
    }
    cleanup(dbservers).await;
    debug!("Done cleanup and channel is closed!");
    Ok(())
    // We drop the result_channel when we leave the function.
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct DumpStartBody {
    batch_size: u64,
    prefetch_count: u32,
    parallelism: u32,
    shards: Vec<String>,
}

fn collection_name_from_id(id: &str) -> String {
    let pos = id.find('/');
    match pos {
        None => "".to_string(),
        Some(p) => id[0..p].to_string(),
    }
}

async fn fetch_edge_and_vertex_collections_by_graph(
    client: reqwest::Client,
    jwt_token: String,
    url: String,
) -> Result<(Vec<String>, Vec<String>), String> {
    let mut edge_collection_names = vec![];
    let mut vertex_collection_names = vec![];

    let resp = client.get(url).bearer_auth(jwt_token).send().await;
    let parsed_response =
        handle_arangodb_response_with_parsed_body::<serde_json::Value>(resp, StatusCode::OK)
            .await?;
    let graph = parsed_response["graph"]
        .as_object()
        .ok_or("graph is not an object")?;
    let edge_definitions = graph
        .get("edgeDefinitions")
        .ok_or("no edgeDefinitions")?
        .as_array()
        .ok_or("edgeDefinitions is not an array")?;

    for edge_definition in edge_definitions {
        let mut non_unique_vertex_collection_names = vec![];
        let edge_collection_name = edge_definition["collection"]
            .as_str()
            .ok_or("collection is not a string")?;
        edge_collection_names.push(edge_collection_name.to_string());

        let from = edge_definition["from"]
            .as_array()
            .ok_or("from is not an array")?;
        for vertex in from {
            let vertex_collection_name =
                vertex.as_str().ok_or("from collection is not a string")?;
            non_unique_vertex_collection_names.push(vertex_collection_name.to_string());
        }

        let to = edge_definition["to"]
            .as_array()
            .ok_or("to is not an array")?;
        for vertex in to {
            let vertex_collection_name = vertex.as_str().ok_or("to collection is not a string")?;
            non_unique_vertex_collection_names.push(vertex_collection_name.to_string());
        }

        non_unique_vertex_collection_names.sort();
        non_unique_vertex_collection_names.dedup();
        vertex_collection_names.append(&mut non_unique_vertex_collection_names);
    }

    Ok((vertex_collection_names, edge_collection_names))
}

async fn get_collections(
    url: String,
    graph_name: String,
    mut vertex_collections: Vec<String>,
    mut edge_collections: Vec<String>,
    client: reqwest::Client,
    jwt_token: String,
    begin: SystemTime,
) -> Result<(Vec<String>, Vec<String>), String> {
    if !graph_name.is_empty() {
        if !vertex_collections.is_empty() || !edge_collections.is_empty() {
            let error_message =
                "Either specify the graph_name or ensure that vertex_collections and edge_collections are not empty.";
            error!("{:?}", error_message);
            return Err(error_message.to_string());
        }

        // in case a graph name has been give, we need to fetch the vertex and edge collections from ArangoDB
        let graph_name = graph_name.clone();
        let (vertices, edges) =
            fetch_edge_and_vertex_collections_by_graph(client, jwt_token, url).await?;
        info!(
            "{:?} Got vertex collections: {:?}, edge collections: {:?} from graph definition for: {:?}.",
            SystemTime::now().duration_since(begin).unwrap(),
            vertices, edges, graph_name
        );
        vertex_collections.extend(vertices);
        edge_collections.extend(edges);
    } else if vertex_collections.is_empty() || edge_collections.is_empty() {
        let error_message =
            "Either specify the graph_name or ensure that vertex_collections and edge_collections are not empty.";
        error!("{:?}", error_message);
        return Err(error_message.to_string());
    }
    Ok((vertex_collections, edge_collections))
}

pub async fn fetch_graph_from_arangodb(
    user: String,
    req: GraphAnalyticsEngineLoadDataRequest,
    args: Arc<Mutex<GralArgs>>,
    graph_arc: Arc<RwLock<Graph>>,
    comp_arc: Arc<RwLock<LoadComputation>>,
) -> Result<(), String> {
    // Graph object must be new and empty!
    let endpoints: Vec<String>;
    let jwt_token: String;
    let ca_cert: Vec<u8>;
    {
        let guard = args.lock().unwrap();
        endpoints = guard
            .arangodb_endpoints
            .split(',')
            .map(|s| s.to_owned())
            .collect();
        jwt_token = create_jwt_token(&guard, &user, 60 * 60 * 2 /* seconds */);
        ca_cert = guard.arangodb_cacert.clone();
    }
    if endpoints.is_empty() {
        return Err("no endpoints given".to_string());
    }
    let begin = std::time::SystemTime::now();

    info!(
        "{:?} Fetching graph from ArangoDB...",
        std::time::SystemTime::now().duration_since(begin).unwrap()
    );

    let use_tls = endpoints[0].starts_with("https://");
    let client = build_client(use_tls, &ca_cert)?;

    let make_url = |path: &str| -> String { endpoints[0].clone() + "/_db/" + &req.database + path };

    // First ask for the shard distribution:
    let url = make_url("/_admin/cluster/shardDistribution");
    let resp = client.get(url).bearer_auth(&jwt_token).send().await;
    let shard_dist =
        handle_arangodb_response_with_parsed_body::<ShardDistribution>(resp, StatusCode::OK)
            .await?;

    let vertex_coll_list;
    let edge_coll_list;
    let param_url = format!("/_api/gharial/{}", req.graph_name);
    let url = make_url(&param_url);
    match get_collections(
        url,
        req.graph_name.clone(),
        req.vertex_collections.clone(),
        req.edge_collections.clone(),
        client,
        jwt_token.clone(),
        begin,
    )
    .await
    {
        Ok((vertex_collections, edge_collections)) => {
            vertex_coll_list = vertex_collections;
            edge_coll_list = edge_collections;
        }
        Err(err) => {
            return Err(err);
        }
    }

    // Compute which shard we must get from which dbserver, we do vertices
    // and edges right away to be able to error out early:
    let vertex_map = compute_shard_map(&shard_dist, &vertex_coll_list)?;
    let edge_map = compute_shard_map(&shard_dist, &edge_coll_list)?;

    if vertex_map.is_empty() {
        error!("No vertex shards found!");
        return Err("No vertex shards found!".to_string());
    }
    if edge_map.is_empty() {
        error!("No edge shards found!");
        return Err("No edge shards found!".to_string());
    }

    // TODO: also opt out in case zero shards have been found
    info!(
        "{:?} Need to fetch data from {} vertex shards and {} edge shards...",
        std::time::SystemTime::now().duration_since(begin).unwrap(),
        vertex_map.values().map(|v| v.len()).sum::<usize>(),
        edge_map.values().map(|v| v.len()).sum::<usize>()
    );

    // Let's first get the vertices:
    {
        // We use multiple threads to receive the data in batches:
        let mut senders: Vec<tokio::sync::mpsc::Sender<Bytes>> = vec![];
        let mut consumers: Vec<JoinHandle<Result<(), String>>> = vec![];
        let prog_reported = Arc::new(Mutex::new(0_u64));
        for _i in 0..req.parallelism {
            let (sender, mut receiver) = tokio::sync::mpsc::channel::<Bytes>(10);
            senders.push(sender);
            let graph_clone = graph_arc.clone();
            let prog_reported_clone = prog_reported.clone();
            let fields = req.vertex_attributes.clone();
            let consumer = std::thread::spawn(move || -> Result<(), String> {
                let begin = std::time::SystemTime::now();
                while let Some(resp) = receiver.blocking_recv() {
                    let body = std::str::from_utf8(resp.as_ref())
                        .map_err(|e| format!("UTF8 error when parsing body: {:?}", e))?;
                    debug!(
                        "{:?} Received post response, body size: {}",
                        std::time::SystemTime::now().duration_since(begin),
                        body.len()
                    );
                    let mut vertex_keys: Vec<Vec<u8>> = Vec::with_capacity(400000);
                    let mut vertex_json: Vec<Vec<Value>> = vec![];
                    for line in body.lines() {
                        let v: Value = match serde_json::from_str(line) {
                            Err(err) => {
                                return Err(format!(
                                    "Error parsing document for line:\n{}\n{:?}",
                                    line, err
                                ));
                            }
                            Ok(val) => val,
                        };
                        let id = &v["_id"];
                        let idstr: &String = match id {
                            Value::String(i) => {
                                let mut buf = vec![];
                                buf.extend_from_slice(i[..].as_bytes());
                                vertex_keys.push(buf);
                                i
                            }
                            _ => {
                                return Err(format!(
                                    "JSON is no object with a string _id attribute:\n{}",
                                    line
                                ));
                            }
                        };
                        // If we get here, we have to extract the field
                        // values in `fields` from the json and store it
                        // to vertex_json:
                        let get_value = |v: &Value, field: &str| -> Value {
                            if field == "@collection_name" {
                                Value::String(collection_name_from_id(idstr))
                            } else {
                                v[field].clone()
                            }
                        };

                        let mut cols: Vec<Value> = Vec::with_capacity(fields.len());
                        for f in fields.iter() {
                            let j = get_value(&v, f);
                            cols.push(j);
                        }
                        vertex_json.push(cols);
                    }
                    let nr_vertices: u64;
                    {
                        let mut graph = graph_clone.write().unwrap();
                        for i in 0..vertex_keys.len() {
                            let k = &vertex_keys[i];
                            let mut cols: Vec<Value> = vec![];
                            std::mem::swap(&mut cols, &mut vertex_json[i]);
                            graph.insert_vertex(k.clone(), cols);
                        }
                        nr_vertices = graph.number_of_vertices();
                    }
                    let mut prog = prog_reported_clone.lock().unwrap();
                    if nr_vertices > *prog + 1000000_u64 {
                        *prog = nr_vertices;
                        info!(
                            "{:?} Have imported {} vertices.",
                            std::time::SystemTime::now().duration_since(begin).unwrap(),
                            *prog
                        );
                    }
                }
                Ok(())
            });
            consumers.push(consumer);
        }
        get_all_shard_data(&req, &endpoints, &jwt_token, &ca_cert, &vertex_map, senders).await?;
        info!(
            "{:?} Got all data, processing...",
            std::time::SystemTime::now().duration_since(begin).unwrap()
        );
        for c in consumers {
            let _guck = c.join();
        }
        let mut graph = graph_arc.write().unwrap();
        graph.seal_vertices();
    }
    {
        let mut comp = comp_arc.write().unwrap();
        comp.progress = 1;
    }

    // And now the edges:
    {
        let mut senders: Vec<tokio::sync::mpsc::Sender<Bytes>> = vec![];
        let mut consumers: Vec<JoinHandle<Result<(), String>>> = vec![];
        let prog_reported = Arc::new(Mutex::new(0_u64));
        for _i in 0..req.parallelism {
            let (sender, mut receiver) = tokio::sync::mpsc::channel::<Bytes>(10);
            senders.push(sender);
            let graph_clone = graph_arc.clone();
            let prog_reported_clone = prog_reported.clone();
            let consumer = std::thread::spawn(move || -> Result<(), String> {
                while let Some(resp) = receiver.blocking_recv() {
                    let body = std::str::from_utf8(resp.as_ref())
                        .map_err(|e| format!("UTF8 error when parsing body: {:?}", e))?;
                    let mut froms: Vec<Vec<u8>> = Vec::with_capacity(1000000);
                    let mut tos: Vec<Vec<u8>> = Vec::with_capacity(1000000);
                    for line in body.lines() {
                        let v: Value = match serde_json::from_str(line) {
                            Err(err) => {
                                return Err(format!(
                                    "Error parsing document for line:\n{}\n{:?}",
                                    line, err
                                ));
                            }
                            Ok(val) => val,
                        };
                        let from = &v["_from"];
                        match from {
                            Value::String(i) => {
                                let mut buf = vec![];
                                buf.extend_from_slice(i[..].as_bytes());
                                froms.push(buf);
                            }
                            _ => {
                                return Err(format!(
                                    "JSON is no object with a string _from attribute:\n{}",
                                    line
                                ));
                            }
                        }
                        let to = &v["_to"];
                        match to {
                            Value::String(i) => {
                                let mut buf = vec![];
                                buf.extend_from_slice(i[..].as_bytes());
                                tos.push(buf);
                            }
                            _ => {
                                return Err(format!(
                                    "JSON is no object with a string _from attribute:\n{}",
                                    line
                                ));
                            }
                        }
                    }

                    let mut edges: Vec<Edge> = Vec::with_capacity(froms.len());
                    {
                        // First translate keys to indexes by reading
                        // the graph object:
                        let graph = graph_clone.read().unwrap();
                        assert!(froms.len() == tos.len());
                        for i in 0..froms.len() {
                            match graph.get_new_edge_between_vertices(&froms[i], &tos[i]) {
                                Ok(edge) => edges.push(edge),
                                Err(message) => eprintln!("{}", message),
                            }
                        }
                    }
                    let nr_edges: u64;
                    {
                        // Now actually insert edges by writing the graph
                        // object:
                        let mut graph = graph_clone.write().unwrap();
                        edges
                            .into_iter()
                            .for_each(|e| graph.insert_edge_unchecked(e));
                        nr_edges = graph.number_of_edges();
                    }

                    let mut prog = prog_reported_clone.lock().unwrap();
                    if nr_edges > *prog + 1000000_u64 {
                        *prog = nr_edges;
                        info!(
                            "{:?} Have imported {} edges.",
                            std::time::SystemTime::now().duration_since(begin).unwrap(),
                            *prog
                        );
                    }
                }
                Ok(())
            });
            consumers.push(consumer);
        }
        get_all_shard_data(&req, &endpoints, &jwt_token, &ca_cert, &edge_map, senders).await?;
        info!(
            "{:?} Got all data, processing...",
            std::time::SystemTime::now().duration_since(begin).unwrap()
        );
        for c in consumers {
            let _guck = c.join();
        }

        let mut graph = graph_arc.write().unwrap();
        graph.seal_edges();
        info!(
            "{:?} Graph loaded.",
            std::time::SystemTime::now().duration_since(begin).unwrap()
        );
    }
    let mut comp = comp_arc.write().unwrap();
    comp.progress = 2; // done!
    Ok(())
}

#[derive(Debug, Clone)]
struct Batch {
    body: Bytes,
    collection: String,
}

async fn batch_sender(
    mut receiver: tokio::sync::mpsc::Receiver<Batch>,
    endpoint: String,
    use_tls: bool,
    jwt_token: String,
    ca_cert: Vec<u8>,
    database: String,
) -> Result<(), String> {
    let begin = std::time::SystemTime::now();
    let client = build_client(use_tls, &ca_cert)?;
    while let Some(batch) = receiver.recv().await {
        let batch_clone = batch.clone();
        debug!(
            "{:?} Sending off batch, body size: {}",
            std::time::SystemTime::now().duration_since(begin),
            batch.body.len()
        );
        let url = format!(
            "{}/_db/{}/_api/document/{}?overwriteMode=update&silent=false",
            endpoint, database, batch.collection
        );
        let resp = client
            .post(url)
            .bearer_auth(&jwt_token)
            .body(batch_clone.body)
            .send()
            .await;
        let _resp = handle_arangodb_response(resp, |c| {
            c == StatusCode::OK || c == StatusCode::CREATED || c == StatusCode::ACCEPTED
        })
        .await?;
    }
    Ok(())
}

pub async fn write_result_to_arangodb(
    user: String,
    req: GraphAnalyticsEngineStoreResultsRequest,
    args: Arc<Mutex<GralArgs>>,
    result_comp_arcs: Vec<Arc<RwLock<dyn Computation + Send + Sync>>>,
    attribute_names: Vec<String>,
    comp_arc: Arc<RwLock<StoreComputation>>,
) -> Result<(), String> {
    assert_eq!(result_comp_arcs.len(), attribute_names.len());
    let endpoints: Vec<String>;
    let jwt_token: String;
    let ca_cert: Vec<u8>;
    {
        let guard = args.lock().unwrap();
        endpoints = guard
            .arangodb_endpoints
            .split(',')
            .map(|s| s.to_owned())
            .collect();
        jwt_token = create_jwt_token(&guard, &user, 60 * 60 * 2 /* seconds */);
        ca_cert = guard.arangodb_cacert.clone();
    }
    if endpoints.is_empty() {
        return Err("no endpoints given".to_string());
    }
    if result_comp_arcs.is_empty() {
        return Err("no result computations given".to_string());
    }
    let begin = std::time::SystemTime::now();

    let use_tls = endpoints[0].starts_with("https://");

    info!(
        "{:?} Writing result back to ArangoDB...",
        std::time::SystemTime::now().duration_since(begin).unwrap()
    );

    let mut senders: Vec<tokio::sync::mpsc::Sender<Batch>> = vec![];
    let mut task_set = JoinSet::new();
    let mut endpoints_round_robin: usize = 0;
    for _i in 0..req.parallelism {
        let (sender, receiver) = tokio::sync::mpsc::channel::<Batch>(10);
        senders.push(sender);
        let endpoint_clone = endpoints[endpoints_round_robin].clone();
        let jwt_token_clone = jwt_token.clone();
        endpoints_round_robin += 1;
        if endpoints_round_robin >= endpoints.len() {
            endpoints_round_robin = 0;
        }
        let database_clone = req.database.clone();
        let ca_cert_clone = ca_cert.clone();
        task_set.spawn(batch_sender(
            receiver,
            endpoint_clone,
            use_tls,
            jwt_token_clone,
            ca_cert_clone,
            database_clone,
        ));
    }

    // Spawn producer thread which partitions the data:
    let producer = std::thread::spawn(move || -> Result<(), String> {
        // Lock all computations for reading:
        let nr_results = result_comp_arcs.len();
        let mut results: Vec<RwLockReadGuard<'_, dyn Computation + Send + Sync>> =
            Vec::with_capacity(nr_results);
        // In the following, we must do a trick to please the compiler as
        // well as the linter: We must use result_comp_arcs[i] to get the
        // lock guard, otherwise the borrow checker complains that _r does
        // not live long enough. But just running i over a range is not
        // ok with clippy:
        for (i, _r) in result_comp_arcs.iter().enumerate() {
            results.push(result_comp_arcs[i].read().unwrap());
        }
        // Now ask all computations for their number of items and look for
        // the minimum:
        let mut nr_items = u64::MAX;
        for (i, r) in results.iter().enumerate() {
            let items = r.nr_results();
            info!("Found {} result items in computation {}.", items, i);
            if items < nr_items {
                nr_items = items;
            }
        }

        let new_batch = |l: usize| -> Vec<u8> {
            let mut res = Vec::with_capacity(200 * l); // heuristics
            res.write_u8(b'[').expect("Assumed to be able to write");
            res
        };
        let mut cur_batch: Vec<u8> = new_batch(req.batch_size as usize);

        let mut first = true;
        let mut count: u64 = 0;
        let mut batch_count: u64 = 0;
        let mut sender_round_robin = 0;
        for i in 0..nr_items {
            if !first {
                cur_batch.extend_from_slice(b",");
            } else {
                first = false;
            }
            cur_batch
                .write_u8(b'{')
                .expect("Assumed to be able to write");
            for j in 0..results.len() {
                let (key, value) = results[j].get_result(i);
                if j == 0 {
                    cur_batch.extend_from_slice(b"\"id\":\"");
                    cur_batch.extend_from_slice(key.as_bytes());
                    cur_batch.extend_from_slice(b"\"");
                }
                cur_batch.extend_from_slice(b",\"");
                cur_batch.extend_from_slice(attribute_names[j].as_bytes());
                cur_batch.extend_from_slice(b"\":");
                cur_batch.extend_from_slice(&serde_json::to_vec(&value).unwrap());
                cur_batch.extend_from_slice(b"");
            }
            cur_batch.extend_from_slice(b"}");
            count += 1;
            if count >= req.batch_size {
                cur_batch
                    .write_u8(b']')
                    .expect("Assumed to be able to write");
                if let Err(e) = senders[sender_round_robin].blocking_send(Batch {
                    body: cur_batch.into(),
                    collection: req.target_collection.clone(),
                }) {
                    return Err(format!("Could not send batch through channel: {:?}", e));
                }

                sender_round_robin += 1;
                if sender_round_robin >= senders.len() {
                    sender_round_robin = 0;
                }
                cur_batch = new_batch(req.batch_size as usize);
                first = true;
                count = 0;
                batch_count += 1;
                if batch_count % 1000 == 0 {
                    info!(
                        "{:?} Have written {} components out of {}.",
                        std::time::SystemTime::now().duration_since(begin).unwrap(),
                        batch_count * req.batch_size,
                        nr_items
                    );
                }
            }
        }
        if count > 0 {
            cur_batch
                .write_u8(b']')
                .expect("Assumed to be able to write");
            if let Err(e) = senders[sender_round_robin].blocking_send(Batch {
                body: cur_batch.into(),
                collection: req.target_collection.clone(),
            }) {
                return Err(format!("Could not send batch through channel: {:?}", e));
            }
        }
        Ok(())
    });

    let mut error_msg: String = "".to_string();
    let res = producer.join().unwrap();
    if let Err(e) = res {
        error_msg.push_str(&e[..]);
        error_msg.push(' ');
    }

    // Join async workers:
    while let Some(res) = task_set.join_next().await {
        let r = res.unwrap();
        match r {
            Ok(_) => {
                info!("Got OK result!");
            }
            Err(msg) => {
                info!("Got error result: {}", msg);
                error_msg.push_str(&msg[..]);
                error_msg.push(' ');
            }
        }
    }

    // Report completion:
    let mut comp = comp_arc.write().unwrap();
    comp.progress = 1;

    info!(
        "{:?} Write results done.",
        std::time::SystemTime::now().duration_since(begin).unwrap()
    );
    if error_msg.is_empty() {
        Ok(())
    } else {
        Err(error_msg)
    }
}
