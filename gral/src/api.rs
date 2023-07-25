use crate::aggregation::aggregate_over_components;
use crate::arangodb::{fetch_graph_from_arangodb, write_result_to_arangodb};
use crate::args::{with_args, GralArgs};
use crate::computations::{
    with_computations, AggregationComputation, ComponentsComputation, Computation, Computations,
    LoadComputation, StoreComputation,
};
use crate::conncomp::{strongly_connected_components, weakly_connected_components};
use crate::graphs::{decode_id, encode_id, with_graphs, Graph, Graphs};
use crate::VERSION;

use bytes::Bytes;
use graphanalyticsengine::*;
use http::Error;
use log::info;
use std::ops::Deref;
use std::sync::{Arc, Mutex, RwLock};
use warp::{http::Response, http::StatusCode, Filter, Rejection};

pub mod graphanalyticsengine {
    include!(concat!(
        env!("OUT_DIR"),
        "/arangodb.cloud.internal.graphanalytics.v1.rs"
    ));
    include!(concat!(
        env!("OUT_DIR"),
        "/arangodb.cloud.internal.graphanalytics.v1.serde.rs"
    ));
}

/// The following function puts together the filters for the API.
/// To this end, it relies on the following async functions below.
pub fn api_filter(
    graphs: Arc<Mutex<Graphs>>,
    computations: Arc<Mutex<Computations>>,
    args: Arc<Mutex<GralArgs>>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    let version = warp::path!("v2" / "version")
        .and(warp::get())
        .map(version_json);
    let get_job = warp::get()
        .and(warp::path!("v2" / "jobs" / String))
        .and(with_computations(computations.clone()))
        .and_then(api_get_job);
    let drop_job = warp::delete()
        .and(warp::path!("v2" / "jobs" / String))
        .and(with_computations(computations.clone()))
        .and_then(api_drop_job);
    let compute = warp::path!("v2" / "process")
        .and(warp::post())
        .and(with_graphs(graphs.clone()))
        .and(with_computations(computations.clone()))
        .and(warp::body::bytes())
        .and_then(api_compute);
    let get_arangodb_graph = warp::path!("v2" / "loaddata")
        .and(warp::post())
        .and(with_graphs(graphs.clone()))
        .and(with_computations(computations.clone()))
        .and(with_args(args.clone()))
        .and(warp::body::bytes())
        .and_then(api_get_arangodb_graph);
    let write_result_back_arangodb = warp::path!("v2" / "storeresults")
        .and(warp::post())
        .and(with_computations(computations.clone()))
        .and(with_args(args.clone()))
        .and(warp::body::bytes())
        .and_then(api_write_result_back_arangodb);
    let get_arangodb_graph_aql = warp::path!("v2" / "loaddataaql")
        .and(warp::post())
        .and(with_graphs(graphs.clone()))
        .and(with_computations(computations.clone()))
        .and(warp::body::bytes())
        .and_then(api_get_arangodb_graph_aql);
    let get_graph = warp::path!("v2" / "graphs" / String)
        .and(warp::get())
        .and(with_graphs(graphs.clone()))
        .and_then(api_get_graph);
    let drop_graph = warp::path!("v2" / "graphs" / String)
        .and(warp::delete())
        .and(with_graphs(graphs.clone()))
        .and_then(api_drop_graph);
    let list_graphs = warp::path!("api" / "graphanalytics" / "v1" / "engines" / String / "graphs")
        .and(warp::get())
        .and(with_graphs(graphs.clone()))
        .and_then(api_list_graphs);
    let list_jobs = warp::path!("v2" / "jobs")
        .and(warp::get())
        .and(with_computations(computations.clone()))
        .and_then(api_list_jobs);

    version
        .or(drop_job)
        .or(get_job)
        .or(compute)
        .or(get_arangodb_graph)
        .or(write_result_back_arangodb)
        .or(get_arangodb_graph_aql)
        .or(drop_graph)
        .or(get_graph)
        .or(list_graphs)
        .or(list_jobs)
}

fn version_json() -> Result<Response<Vec<u8>>, Error> {
    let version_str = format!(
        "{}.{}.{}",
        VERSION >> 16,
        (VERSION >> 8) & 0xff,
        VERSION & 0xff
    );
    let body = serde_json::json!({
        "version": version_str,
        "apiMinVersion": 1,
        "apiMaxVersion": 2
    });
    let v = serde_json::to_vec(&body).expect("Should be serializable");
    Response::builder()
        .header("Content-Type", "application/json")
        .body(v)
}

fn check_graph(graph: &Graph, graph_id: u64, edges_must_be_sealed: bool) -> Result<(), String> {
    if !graph.vertices_sealed {
        return Err(format!(
            "Graph vertices not sealed: {}",
            encode_id(graph_id)
        ));
    }
    if edges_must_be_sealed {
        if !graph.edges_sealed {
            return Err(format!("Graph edges not sealed: {}", encode_id(graph_id)));
        }
    } else {
        if graph.edges_sealed {
            return Err(format!(
                "Graph edges must not be sealed: {}",
                encode_id(graph_id)
            ));
        }
    }
    Ok(())
}

/// This function triggers a computation:
async fn api_compute(
    graphs: Arc<Mutex<Graphs>>,
    computations: Arc<Mutex<Computations>>,
    bytes: Bytes,
) -> Result<warp::reply::WithStatus<Vec<u8>>, Rejection> {
    let err_bad_req = |e: String, c: StatusCode| {
        warp::reply::with_status(
            serde_json::to_vec(&GraphAnalyticsEngineProcessResponse {
                job_id: "".to_string(),
                client_id: "".to_string(),
                error: true,
                error_code: 400,
                error_message: e,
            })
            .expect("Could not serialize"),
            c,
        )
    };
    // Parse body and extract integers:
    let parsed: serde_json::Result<GraphAnalyticsEngineProcessRequest> =
        serde_json::from_slice(&bytes[..]);
    if let Err(e) = parsed {
        return Ok(err_bad_req(
            format!("Cannot parse JSON body of request: {}", e.to_string()),
            StatusCode::BAD_REQUEST,
        ));
    }
    let body = parsed.unwrap();

    let client_id = decode_id(&body.client_id);
    if let Err(e) = client_id {
        return Ok(err_bad_req(
            format!(
                "Could not decode clientId {}: {}",
                body.client_id,
                e.to_string()
            ),
            StatusCode::BAD_REQUEST,
        ));
    }
    let _client_id = client_id.unwrap();
    let graph_id = decode_id(&body.graph_id);
    if let Err(e) = graph_id {
        return Ok(err_bad_req(e, StatusCode::BAD_REQUEST));
    }
    let graph_id = graph_id.unwrap();
    let graph_arc: Arc<RwLock<Graph>>;
    {
        let graphs = graphs.lock().unwrap();
        let g = graphs.list.get(&graph_id);
        if g.is_none() {
            return Ok(err_bad_req(
                format!("Graph with id {} not found.", &body.graph_id),
                StatusCode::NOT_FOUND,
            ));
        }
        graph_arc = g.unwrap().clone();
    }

    // Computation ID is optional:
    let mut prev_comp: Option<Arc<RwLock<dyn Computation + Send + Sync>>> = None;
    if !body.job_id.is_empty() {
        let comp_id = decode_id(&body.job_id);
        if let Err(e) = comp_id {
            return Ok(err_bad_req(
                format!("Could not decode job id {}: {}.", &body.job_id, e),
                StatusCode::BAD_REQUEST,
            ));
        }
        let comps = computations.lock().unwrap();
        let comp = comps.list.get(&comp_id.unwrap());
        if comp.is_none() {
            return Ok(err_bad_req(
                format!("Could not find previous job id {}.", &body.job_id),
                StatusCode::BAD_REQUEST,
            ));
        }
        prev_comp = Some(comp.unwrap().clone());
    }

    {
        // Check graph:
        let graph = graph_arc.read().unwrap();
        let r = check_graph(graph.deref(), graph_id, true);
        if let Err(e) = r {
            return Ok(err_bad_req(e, StatusCode::BAD_REQUEST));
        }
    }

    let algorithm: u32 = match body.algorithm.as_ref() {
        "wcc" => 1,
        "scc" => 2,
        "aggregate_components" => 3,
        _ => 0,
    };

    let generic_comp_arc: Arc<RwLock<dyn Computation + Send + Sync>>;
    match algorithm {
        1 | 2 => {
            let comp_arc = Arc::new(RwLock::new(ComponentsComputation {
                algorithm,
                graph: graph_arc.clone(),
                components: None,
                next_in_component: None,
                shall_stop: false,
                number: None,
            }));
            generic_comp_arc = comp_arc.clone();
            std::thread::spawn(move || {
                let (nr, components, next) = match algorithm {
                    1 => {
                        let graph = graph_arc.read().unwrap();
                        weakly_connected_components(&graph)
                    }
                    2 => {
                        {
                            // Make sure we have an edge index:
                            let mut graph = graph_arc.write().unwrap();
                            if !graph.edges_indexed_from {
                                info!("Indexing edges by from...");
                                graph.index_edges(true, false);
                            }
                        }
                        let graph = graph_arc.read().unwrap();
                        strongly_connected_components(&graph)
                    }
                    _ => std::unreachable!(),
                };
                info!("Found {} connected components.", nr);
                let mut comp = comp_arc.write().unwrap();
                comp.components = Some(components);
                comp.next_in_component = Some(next);
                comp.number = Some(nr);
            });
        }
        3 => {
            if prev_comp.is_none() {
                return Ok(err_bad_req(
                    "Aggregation algorithm needs previous computation as absis to work".to_string(),
                    StatusCode::BAD_REQUEST,
                ));
            }
            let prev_comp = prev_comp.unwrap();
            let guard = prev_comp.read().unwrap();
            let downcast = guard.as_any().downcast_ref::<ComponentsComputation>();
            if downcast.is_none() {
                // Wrong actual type!
                return Ok(err_bad_req(
                    "Aggregation algorithm needs previous component computation as basis to
                        work"
                        .to_string(),
                    StatusCode::BAD_REQUEST,
                ));
            }
            let attr = body.custom_fields.get("aggregationAttribute");
            let attr = match attr {
                Some(s) => s.clone(),
                None => "value".to_string(),
            };
            let comp_arc = Arc::new(RwLock::new(AggregationComputation {
                graph: graph_arc.clone(),
                compcomp: prev_comp.clone(),
                aggregation_attribute: attr.to_string(),
                shall_stop: false,
                total: 1,
                progress: 0,
                error_code: 0,
                error_message: "".to_string(),
                result: vec![],
            }));
            generic_comp_arc = comp_arc.clone();
            let prev_comp_clone = prev_comp.clone();
            std::thread::spawn(move || {
                // Lock first the computation, then the graph!
                let guard = prev_comp_clone.read().unwrap();
                let compcomp = guard
                    .as_any()
                    .downcast_ref::<ComponentsComputation>()
                    .unwrap();
                // already checked outside!

                let res = aggregate_over_components(compcomp, attr);
                info!("Aggregated over {} connected components.", res.len());
                let mut comp = comp_arc.write().unwrap();
                comp.result = res;
            });
        }
        _ => {
            return Ok(err_bad_req(
                format!("Unknown algorithm: {}", body.algorithm),
                StatusCode::BAD_REQUEST,
            ));
        }
    };

    let comp_id: u64;
    {
        let mut comps = computations.lock().unwrap();
        comp_id = comps.register(generic_comp_arc.clone());
    }
    let response = GraphAnalyticsEngineProcessResponse {
        job_id: encode_id(comp_id),
        client_id: body.client_id,
        error: false,
        error_code: 0,
        error_message: "".to_string(),
    };

    // Write response:
    let v = serde_json::to_vec(&response).expect("Should be serializable!");
    Ok(warp::reply::with_status(v, StatusCode::OK))
}

/// This function writes a computation result back to ArangoDB:
async fn api_write_result_back_arangodb(
    computations: Arc<Mutex<Computations>>,
    args: Arc<Mutex<GralArgs>>,
    bytes: Bytes,
) -> Result<warp::reply::WithStatus<Vec<u8>>, Rejection> {
    let err_bad_req = |e: String, sc: StatusCode| {
        warp::reply::with_status(
            serde_json::to_vec(&GraphAnalyticsEngineStoreResultsResponse {
                job_id: "".to_string(),
                client_id: "".to_string(),
                error: true,
                error_code: sc.as_u16() as i32,
                error_message: e,
            })
            .expect("Could not serialize"),
            StatusCode::BAD_REQUEST,
        )
    };
    // Parse body:
    let parsed: serde_json::Result<GraphAnalyticsEngineStoreResultsRequest> =
        serde_json::from_slice(&bytes[..]);
    if let Err(e) = parsed {
        return Ok(err_bad_req(
            format!("Could not parse JSON body: {}", e.to_string()),
            StatusCode::BAD_REQUEST,
        ));
    }
    let mut body = parsed.unwrap();

    let client_id = decode_id(&body.client_id);
    if let Err(e) = client_id {
        return Ok(err_bad_req(
            format!(
                "Could not decode clientId {}: {}",
                body.client_id,
                e.to_string()
            ),
            StatusCode::BAD_REQUEST,
        ));
    }
    let client_id = client_id.unwrap();

    let job_id = decode_id(&body.job_id);
    if let Err(e) = job_id {
        return Ok(err_bad_req(
            format!("Could not decode jobId {}: {}", body.job_id, e.to_string()),
            StatusCode::BAD_REQUEST,
        ));
    }
    let job_id = job_id.unwrap();

    let result_comp: Arc<RwLock<dyn Computation + Send + Sync>>;
    {
        let comps = computations.lock().unwrap();
        let compfound = comps.list.get(&job_id);
        if compfound.is_none() {
            return Ok(err_bad_req(
                format!("Job {} not found.", job_id),
                StatusCode::NOT_FOUND,
            ));
        }
        result_comp = compfound.unwrap().clone();
    }

    // Set a few sensible defaults:
    if body.batch_size == 0 {
        body.batch_size = 400000;
    }
    if body.parallelism == 0 {
        body.parallelism = 5;
    }
    if body.database.is_empty() {
        body.database = "_system".to_string();
    }
    if body.target_collection.is_empty() {
        body.target_collection = encode_id(job_id);
    }

    // Now create a job object:
    let comp_arc = Arc::new(RwLock::new(StoreComputation {
        comp: result_comp.clone(),
        shall_stop: false,
        total: 2, // will eventually be overwritten in background thread
        progress: 0,
        error_code: 0,
        error_message: "".to_string(),
    }));
    let comp_id: u64;
    {
        let mut comps = computations.lock().unwrap();
        comp_id = comps.register(comp_arc.clone());
    }

    // Write to ArangoDB in a background thread:
    std::thread::spawn(move || {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let res =
                    write_result_to_arangodb(body, args, result_comp.clone(), comp_arc.clone())
                        .await;
                let mut comp = comp_arc.write().unwrap();
                match res {
                    Ok(()) => {
                        comp.error_code = 0;
                        comp.error_message = "".to_string();
                    }
                    Err(e) => {
                        comp.error_code = 1;
                        comp.error_message = e;
                    }
                }
            });
    });

    let response = GraphAnalyticsEngineStoreResultsResponse {
        job_id: encode_id(comp_id),
        client_id: encode_id(client_id),
        error: false,
        error_code: 0,
        error_message: "".to_string(),
    };

    // Write response:
    let v = serde_json::to_vec(&response).expect("Should be serializable!");
    Ok(warp::reply::with_status(v, StatusCode::OK))
}

/// This function writes a computation result back to ArangoDB:
async fn api_get_arangodb_graph_aql(
    _graphs: Arc<Mutex<Graphs>>,
    _computations: Arc<Mutex<Computations>>,
    bytes: Bytes,
) -> Result<warp::reply::WithStatus<Vec<u8>>, Rejection> {
    let err_bad_req = |e: String| {
        warp::reply::with_status(
            serde_json::to_vec(&GraphAnalyticsEngineLoadDataResponse {
                job_id: "".to_string(),
                client_id: "".to_string(),
                graph_id: "".to_string(),
                error: true,
                error_code: 400,
                error_message: e,
            })
            .expect("Could not serialize"),
            StatusCode::BAD_REQUEST,
        )
    };
    // Parse body:
    let parsed: serde_json::Result<GraphAnalyticsEngineLoadDataAqlRequest> =
        serde_json::from_slice(&bytes[..]);
    if let Err(e) = parsed {
        return Ok(err_bad_req(format!(
            "Could not parse JSON body: {}",
            e.to_string()
        )));
    }
    let body = parsed.unwrap();

    let client_id = decode_id(&body.client_id);
    if let Err(e) = client_id {
        return Ok(err_bad_req(format!(
            "Could not decode clientId {}: {}",
            body.client_id,
            e.to_string()
        )));
    }
    let _client_id = client_id.unwrap();
    let job_id = decode_id(&body.job_id);
    if let Err(e) = job_id {
        return Ok(err_bad_req(format!(
            "Could not decode jobId {}: {}",
            body.job_id,
            e.to_string()
        )));
    }
    let job_id = job_id.unwrap();

    // TO BE IMPLEMENTED

    let response = GraphAnalyticsEngineLoadDataResponse {
        job_id: format!("{:08x}", job_id),
        client_id: body.client_id,
        graph_id: "bla".to_string(),
        error: true,
        error_code: 1,
        error_message: "NOT_YET_IMPLEMENTED".to_string(),
    };

    // Write response:
    let v = serde_json::to_vec(&response).expect("Should be serializable!");
    Ok(warp::reply::with_status(v, StatusCode::OK))
}

fn id_to_type(id: u32) -> String {
    match id {
        0 => "loaddata",
        1 => "wcc",
        2 => "scc",
        3 => "aggregation",
        4 => "storedata",
        _ => "",
    }
    .to_string()
}

/// This function gets progress of a computation.
async fn api_get_job(
    job_id: String,
    computations: Arc<Mutex<Computations>>,
) -> Result<warp::reply::WithStatus<Vec<u8>>, Rejection> {
    let not_found_err = |j: String| {
        warp::reply::with_status(
            serde_json::to_vec(&GraphAnalyticsEngineJob {
                job_id: j.clone(),
                graph_id: "".into(),
                total: 0,
                progress: 0,
                source_job: "".into(),
                error: true,
                error_code: 404,
                error_message: j,
                comp_type: "".to_string(),
            })
            .unwrap(),
            StatusCode::NOT_FOUND,
        )
    };
    let comp_id = decode_id(&job_id);
    if let Err(e) = comp_id {
        return Ok(not_found_err(format!(
            "Could not decode jobId {}: {}",
            job_id, e
        )));
    }
    let comp_id = comp_id.unwrap();

    let comps = computations.lock().unwrap();
    let comp_arc = comps.list.get(&comp_id);
    match comp_arc {
        None => {
            return Ok(not_found_err(format!("Could not find jobId {}", job_id)));
        }
        Some(comp_arc) => {
            let comp = comp_arc.read().unwrap();
            let graph_arc = comp.get_graph();
            let graph = graph_arc.read().unwrap();

            // Write response:
            let (error_code, error_message) = comp.get_error();
            let response = GraphAnalyticsEngineJob {
                job_id,
                graph_id: encode_id(graph.graph_id),
                total: comp.get_total(),
                progress: comp.get_progress(),
                error: error_code != 0,
                error_code,
                error_message,
                source_job: "".to_string(),
                comp_type: id_to_type(comp.algorithm_id()),
            };
            Ok(warp::reply::with_status(
                serde_json::to_vec(&response).expect("Should be serializable"),
                StatusCode::OK,
            ))
        }
    }
}

/// This function deletes a job.
async fn api_drop_job(
    job_id: String,
    computations: Arc<Mutex<Computations>>,
) -> Result<warp::reply::WithStatus<Vec<u8>>, Rejection> {
    let not_found_err = |j: String| {
        warp::reply::with_status(
            serde_json::to_vec(&GraphAnalyticsEngineDeleteJobResponse {
                job_id: j.clone(),
                error: true,
                error_code: 404,
                error_message: j,
            })
            .unwrap(),
            StatusCode::NOT_FOUND,
        )
    };
    let comp_id = decode_id(&job_id);
    if let Err(e) = comp_id {
        return Ok(not_found_err(format!(
            "Could not decode job id {}: {}",
            job_id, e
        )));
    }
    let comp_id = comp_id.unwrap();

    let mut comps = computations.lock().unwrap();
    let comp_arc = comps.list.get(&comp_id);
    match comp_arc {
        None => {
            return Ok(not_found_err(format!("Could not find job {}", job_id)));
        }
        Some(comp_arc) => {
            {
                let mut comp = comp_arc.write().unwrap();
                comp.cancel();
            }
            comps.list.remove(&comp_id);

            // Write response:
            let response = GraphAnalyticsEngineDeleteJobResponse {
                job_id,
                error: false,
                error_code: 0,
                error_message: "".to_string(),
            };
            Ok(warp::reply::with_status(
                serde_json::to_vec(&response).expect("Should be serializable"),
                StatusCode::OK,
            ))
        }
    }
}

/// This function gets information about a graph:
async fn api_get_graph(
    graph_id: String,
    graphs: Arc<Mutex<Graphs>>,
) -> Result<warp::reply::WithStatus<Vec<u8>>, Rejection> {
    let not_found_err = |j: String| {
        warp::reply::with_status(
            serde_json::to_vec(&GraphAnalyticsEngineGetGraphResponse {
                error: true,
                error_code: 404,
                error_message: j.clone(),
                graph: None,
            })
            .expect("Could not serialize"),
            StatusCode::NOT_FOUND,
        )
    };
    let graph_id_decoded = decode_id(&graph_id);
    if let Err(e) = graph_id_decoded {
        return Ok(not_found_err(e));
    }
    let graph_id_decoded = graph_id_decoded.unwrap();

    let graphs = graphs.lock().unwrap();
    let graph_arc = graphs.list.get(&graph_id_decoded);
    if graph_arc.is_none() {
        return Ok(not_found_err(format!("Graph {} not found!", graph_id)));
    }
    let graph_arc = graph_arc.unwrap().clone();
    let graph = graph_arc.read().unwrap();

    // Write response:
    let response = GraphAnalyticsEngineGetGraphResponse {
        error: false,
        error_code: 0,
        error_message: "".to_string(),
        graph: Some(GraphAnalyticsEngineGraph {
            graph_id: encode_id(graph_id_decoded),
            number_of_vertices: graph.number_of_vertices(),
            number_of_edges: graph.number_of_edges(),
        }),
    };
    Ok(warp::reply::with_status(
        serde_json::to_vec(&response).expect("Should be serializable"),
        StatusCode::OK,
    ))
}

/// This function lists graphs:
async fn api_list_graphs(
    _engine_id: String,
    graphs: Arc<Mutex<Graphs>>,
) -> Result<Vec<u8>, Rejection> {
    let graphs = graphs.lock().unwrap();
    let mut response = vec![];
    for (_id, graph_arc) in graphs.list.iter() {
        let graph = graph_arc.read().unwrap();

        // Write response:
        let g = GraphAnalyticsEngineGraph {
            graph_id: encode_id(graph.graph_id),
            number_of_vertices: graph.number_of_vertices(),
            number_of_edges: graph.number_of_edges(),
        };
        response.push(g);
    }
    Ok(serde_json::to_vec(&response).expect("Should be serializable"))
}

async fn api_list_jobs(computations: Arc<Mutex<Computations>>) -> Result<Vec<u8>, Rejection> {
    let comps = computations.lock().unwrap();
    let mut response: Vec<GraphAnalyticsEngineJob> = vec![];
    for (job_id, comp_arc) in comps.list.iter() {
        let comp = comp_arc.read().unwrap();
        let graph_arc = comp.get_graph();
        let graph = graph_arc.read().unwrap();

        // Write response:
        let (error_code, error_message) = comp.get_error();
        let j = GraphAnalyticsEngineJob {
            job_id: encode_id(*job_id),
            graph_id: encode_id(graph.graph_id),
            total: 1,
            progress: if comp.is_ready() { 1 } else { 0 },
            error: error_code != 0,
            error_code,
            error_message,
            source_job: "".to_string(),
            comp_type: id_to_type(comp.algorithm_id()),
        };
        response.push(j);
    }
    Ok(serde_json::to_vec(&response).expect("Should be serializable"))
}

/// This function drops a graph:
async fn api_drop_graph(
    graph_id: String,
    graphs: Arc<Mutex<Graphs>>,
) -> Result<warp::reply::WithStatus<Vec<u8>>, Rejection> {
    let not_found_err = |j: String| {
        warp::reply::with_status(
            serde_json::to_vec(&GraphAnalyticsEngineGetGraphResponse {
                error: true,
                error_code: 404,
                error_message: j.clone(),
                graph: None,
            })
            .expect("Could not serialize"),
            StatusCode::NOT_FOUND,
        )
    };
    let graph_id_decoded = decode_id(&graph_id);
    if let Err(e) = graph_id_decoded {
        return Ok(not_found_err(e));
    }
    let graph_id_decoded = graph_id_decoded.unwrap();

    let mut graphs = graphs.lock().unwrap();
    let graph_arc = graphs.list.get(&graph_id_decoded);
    if graph_arc.is_none() {
        return Ok(not_found_err(format!("Graph {} not found!", graph_id)));
    }

    // The following will automatically free graph if no longer used by
    // a computation:
    graphs.list.remove(&graph_id_decoded);
    info!("Have dropped graph {}!", graph_id);

    // Write response:
    let response = GraphAnalyticsEngineDeleteGraphResponse {
        graph_id: encode_id(graph_id_decoded),
        error: false,
        error_code: 0,
        error_message: "".to_string(),
    };
    Ok(warp::reply::with_status(
        serde_json::to_vec(&response).expect("Should be serializable"),
        StatusCode::OK,
    ))
}

async fn api_get_arangodb_graph(
    graphs: Arc<Mutex<Graphs>>,
    comps: Arc<Mutex<Computations>>,
    args: Arc<Mutex<GralArgs>>,
    bytes: Bytes,
) -> Result<warp::reply::WithStatus<Vec<u8>>, Rejection> {
    let parsed: serde_json::Result<GraphAnalyticsEngineLoadDataRequest> =
        serde_json::from_slice(&bytes[..]);
    if let Err(e) = parsed {
        let response = GraphAnalyticsEngineLoadDataResponse {
            job_id: "".to_string(),
            client_id: "".to_string(),
            graph_id: "".to_string(),
            error: true,
            error_code: 400,
            error_message: format!("Could not parse JSON body: {}", e.to_string()),
        };
        return Ok(warp::reply::with_status(
            serde_json::to_vec(&response).expect("Could not serialize"),
            StatusCode::BAD_REQUEST,
        ));
    }
    let mut body = parsed.unwrap();
    // Set a few sensible defaults:
    if body.batch_size == 0 {
        body.batch_size = 400000;
    }
    if body.parallelism == 0 {
        body.parallelism = 5;
    }

    let graph = Graph::new(true, 64, 0);
    let graph_clone = graph.clone(); // for background thread

    let client_id = body.client_id.clone();

    // And store it amongst the graphs:
    let mut graphs = graphs.lock().unwrap();
    let graph_id = graphs.register(graph_clone.clone());

    info!("Have created graph with id {}!", encode_id(graph_id));

    // Now create a job object:
    let comp_arc = Arc::new(RwLock::new(LoadComputation {
        graph: graph_clone.clone(),
        shall_stop: false,
        total: 2, // will eventually be overwritten in background thread
        progress: 0,
        error_code: 0,
        error_message: "".to_string(),
    }));
    let comp_id: u64;
    {
        let mut comps = comps.lock().unwrap();
        comp_id = comps.register(comp_arc.clone());
    }

    // Fetch from ArangoDB in a background thread:
    std::thread::spawn(move || {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let res =
                    fetch_graph_from_arangodb(body, args, graph_clone, comp_arc.clone()).await;
                let mut comp = comp_arc.write().unwrap();
                match res {
                    Ok(()) => {
                        comp.error_code = 0;
                        comp.error_message = "".to_string();
                    }
                    Err(e) => {
                        comp.error_code = 1;
                        comp.error_message = e;
                    }
                }
            });
    });

    // Write response:
    let response = GraphAnalyticsEngineLoadDataResponse {
        job_id: encode_id(comp_id),
        client_id,
        graph_id: encode_id(graph_id),
        error: false,
        error_code: 0,
        error_message: "".to_string(),
    };
    Ok(warp::reply::with_status(
        serde_json::to_vec(&response).expect("Could not serialize"),
        StatusCode::OK,
    ))
}
