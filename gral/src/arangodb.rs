use crate::api::GetArangoDBGraphRequest;
use crate::graphs::{Graph, VertexHash};
use bytes::Bytes;
use log::{debug, info};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::thread::JoinHandle;
use std::time::SystemTime;
use tokio::task::JoinSet;
use warp::http::StatusCode;
use xxhash_rust::xxh3::xxh3_64_with_seed;

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

fn build_client(use_tls: bool) -> Result<reqwest::Client, String> {
    let builder = reqwest::Client::builder();
    if use_tls {
        let client = builder
            .use_rustls_tls()
            .min_tls_version(reqwest::tls::Version::TLS_1_2)
            .danger_accept_invalid_certs(true)
            .https_only(true)
            .build();
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
                    e.to_string(),
                    status,
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
    body.map_err(|e| format!("Could not parse response body, error: {}", e.to_string()))
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
                    e.to_string(),
                    status,
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

fn compute_shard_map(sd: &ShardDistribution, coll_list: &Vec<String>) -> Result<ShardMap, String> {
    let mut result: ShardMap = HashMap::new();
    for c in coll_list.into_iter() {
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
                for (shard, _) in &(coll_dist.plan) {
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
                    if ignore.get(shard).is_none() {
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
    req: &GetArangoDBGraphRequest,
    shard_map: &ShardMap,
    result_channels: Vec<std::sync::mpsc::Sender<Bytes>>,
) -> Result<(), String> {
    let begin = SystemTime::now();

    let client = build_client(req.use_tls)?;

    let make_url =
        |path: &str| -> String { req.endpoints[0].clone() + "/_db/" + &req.database + path };

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
            prefetch_count: req.prefetch_count,
            parallelism: req.dbserver_parallelism,
            shards: shard_list.clone(),
        };
        let body_v =
            serde_json::to_vec::<DumpStartBody>(&body).expect("could not serialize DumpStartBody");
        let resp = client.post(url).body(body_v).send().await;
        let r = handle_arangodb_response(resp, |c| c == StatusCode::NO_CONTENT).await;
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
    let cleanup = |dbservers: Vec<DBServerInfo>| async move {
        debug!("Doing cleanup...");
        for dbserver in dbservers.iter() {
            let url = make_url(&format!(
                "/_api/dump/{}?dbserver={}",
                dbserver.dump_id, dbserver.dbserver
            ));
            let resp = client_clone_for_cleanup.delete(url).send().await;
            let r = handle_arangodb_response(resp, |c| c == StatusCode::OK).await;
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
            let client_clone = build_client(req.use_tls)?;
            let endpoint_clone = req.endpoints[endpoints_round_robin].clone();
            endpoints_round_robin += 1;
            if endpoints_round_robin >= req.endpoints.len() {
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
                        start.duration_since(begin),
                        task_info.id,
                        task_info.dbserver.dbserver,
                        task_info.current_batch_id
                    );
                    let resp = client_clone.post(url).send().await;
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
                            end.duration_since(begin),
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
    batch_size: u32,
    prefetch_count: u32,
    parallelism: u32,
    shards: Vec<String>,
}

pub async fn fetch_graph_from_arangodb(
    req: &GetArangoDBGraphRequest,
) -> Result<Arc<RwLock<Graph>>, String> {
    let begin = std::time::SystemTime::now();

    info!(
        "{:?} Fetching graph from ArangoDB...",
        std::time::SystemTime::now().duration_since(begin).unwrap()
    );

    let client = build_client(req.use_tls)?;

    let make_url =
        |path: &str| -> String { req.endpoints[0].clone() + "/_db/" + &req.database + path };

    // First ask for the shard distribution:
    assert!(!req.endpoints.is_empty()); // checked outside!
    let url = make_url("/_admin/cluster/shardDistribution");
    let resp = client.get(url).send().await;
    let shard_dist =
        handle_arangodb_response_with_parsed_body::<ShardDistribution>(resp, StatusCode::OK)
            .await?;

    // Compute which shard we must get from which dbserver, we do vertices
    // and edges right away to be able to error out early:
    let vertex_coll_list = req
        .vertex_collections
        .iter()
        .map(|ci| -> String { ci.name.clone() })
        .collect();
    let vertex_map = compute_shard_map(&shard_dist, &vertex_coll_list)?;
    let edge_coll_list = req
        .edge_collections
        .iter()
        .map(|ci| -> String { ci.name.clone() })
        .collect();
    let edge_map = compute_shard_map(&shard_dist, &edge_coll_list)?;

    info!(
        "{:?} Need to fetch data from {} vertex shards and {} edge shards...",
        std::time::SystemTime::now().duration_since(begin).unwrap(),
        vertex_map.values().map(|v| v.len()).sum::<usize>(),
        edge_map.values().map(|v| v.len()).sum::<usize>()
    );

    // Generate a graph object:
    let graph_arc = Graph::new(true, 64);

    // Let's first get the vertices:
    {
        // We use multiple threads to receive the data in batches:
        let mut senders: Vec<std::sync::mpsc::Sender<Bytes>> = vec![];
        let mut consumers: Vec<JoinHandle<Result<(), String>>> = vec![];
        for _i in 0..req.nr_threads {
            let (sender, receiver) = std::sync::mpsc::channel::<Bytes>();
            senders.push(sender);
            let graph_clone = graph_arc.clone();
            let consumer = std::thread::spawn(move || -> Result<(), String> {
                let begin = std::time::SystemTime::now();
                while let Ok(resp) = receiver.recv() {
                    let body = std::str::from_utf8(resp.as_ref())
                        .map_err(|e| format!("UTF8 error when parsing body: {:?}", e))?;
                    debug!(
                        "{:?} Received post response, body size: {}",
                        std::time::SystemTime::now().duration_since(begin),
                        body.len()
                    );
                    let mut vertex_keys: Vec<Vec<u8>> = vec![];
                    vertex_keys.reserve(400000);
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
                        match id {
                            Value::String(i) => {
                                let mut buf = vec![];
                                buf.extend_from_slice((&i[..]).as_bytes());
                                vertex_keys.push(buf);
                            }
                            _ => {
                                return Err(format!(
                                    "JSON is no object with a string _id attribute:\n{}",
                                    line
                                ));
                            }
                        }
                    }
                    let mut graph = graph_clone.write().unwrap();
                    let mut i: u32 = 0;
                    let mut exceptional: Vec<(u32, VertexHash)> = vec![];
                    let mut exceptional_keys: Vec<Vec<u8>> = vec![];
                    for k in &vertex_keys {
                        let hash = VertexHash::new(xxh3_64_with_seed(k, 0xdeadbeefdeadbeef));
                        graph.insert_vertex(
                            i,
                            hash,
                            k.clone(),
                            vec![],
                            &mut exceptional,
                            &mut exceptional_keys,
                        );
                        i += 1;
                    }
                }
                Ok(())
            });
            consumers.push(consumer);
        }
        get_all_shard_data(req, &vertex_map, senders).await?;
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

    // And now the edges:
    {
        let mut senders: Vec<std::sync::mpsc::Sender<Bytes>> = vec![];
        let mut consumers: Vec<JoinHandle<Result<(), String>>> = vec![];
        for _i in 0..req.nr_threads {
            let (sender, receiver) = std::sync::mpsc::channel::<Bytes>();
            senders.push(sender);
            let graph_clone = graph_arc.clone();
            let consumer = std::thread::spawn(move || -> Result<(), String> {
                while let Ok(resp) = receiver.recv() {
                    let body = std::str::from_utf8(resp.as_ref())
                        .map_err(|e| format!("UTF8 error when parsing body: {:?}", e))?;
                    let mut froms: Vec<Vec<u8>> = vec![];
                    froms.reserve(1000000);
                    let mut tos: Vec<Vec<u8>> = vec![];
                    tos.reserve(1000000);
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
                                buf.extend_from_slice((&i[..]).as_bytes());
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
                                buf.extend_from_slice((&i[..]).as_bytes());
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
                    let mut graph = graph_clone.write().unwrap();
                    assert!(froms.len() == tos.len());
                    for i in 0..froms.len() {
                        let from_key = &froms[i];
                        let from_opt = graph.index_from_vertex_key(from_key);
                        let to_key = &tos[i];
                        let to_opt = graph.index_from_vertex_key(to_key);
                        if from_opt.is_some() && to_opt.is_some() {
                            graph.insert_edge(from_opt.unwrap(), to_opt.unwrap(), vec![]);
                        } else {
                            eprintln!("Did not find _from or _to key in vertices!");
                        }
                    }
                }
                Ok(())
            });
            consumers.push(consumer);
        }
        get_all_shard_data(req, &edge_map, senders).await?;
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
    Ok(graph_arc)
}
