use crate::algorithms;
use crate::algorithms::aggregation::aggregate_over_components;
use crate::algorithms::attributepropagation::AttributePropagationComputation;
use crate::arangodb::{fetch_graph_from_arangodb, write_result_to_arangodb};
use crate::args::{with_args, GralArgs};
use crate::auth::{with_auth, Unauthorized};
use crate::computations::{
    with_computations, AggregationComputation, ComponentsComputation, Computation, Computations,
    JobRuntime, LabelPropagationComputation, LoadComputation, PageRankComputation,
    StoreComputation,
};
use crate::constants;
use crate::graph_store::graph::Graph;
use crate::graph_store::graphs::{with_graphs, Graphs};
use crate::python::executor;
use crate::python::pythoncomputation::PythonComputation;
use bytes::Bytes;
use graphanalyticsengine::*;
use http::Error;
use log::{error, info};
use std::convert::Infallible;
use std::ops::Deref;
use std::sync::{Arc, Mutex, RwLock};
use warp::{http::Response, http::StatusCode, reply::WithStatus, Filter, Rejection};

pub mod graphanalyticsengine {
    include!(concat!(
        env!("OUT_DIR"),
        "/arangodb.cloud.internal.graphanalytics.v1.rs"
    ));
}

/// The following function puts together the filters for the API.
/// To this end, it relies on the following async functions below.
pub fn api_filter(
    graphs: Arc<Mutex<Graphs>>,
    computations: Arc<Mutex<Computations>>,
    args: Arc<Mutex<GralArgs>>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    let version = warp::path!("v1" / "version")
        .and(warp::get())
        .and(with_auth(args.clone()))
        .map(version_json);
    let get_job = warp::path!("v1" / "jobs" / u64)
        .and(warp::get())
        .and(with_auth(args.clone()))
        .and(with_computations(computations.clone()))
        .and_then(api_get_job);
    let drop_job = warp::path!("v1" / "jobs" / u64)
        .and(warp::delete())
        .and(with_auth(args.clone()))
        .and(with_computations(computations.clone()))
        .and_then(api_drop_job);
    let wcc = warp::path!("v1" / "wcc")
        .and(warp::post())
        .and(with_auth(args.clone()))
        .and(with_graphs(graphs.clone()))
        .and(with_computations(computations.clone()))
        .and(warp::body::bytes())
        .and_then(api_wcc);
    let scc = warp::path!("v1" / "scc")
        .and(warp::post())
        .and(with_auth(args.clone()))
        .and(with_graphs(graphs.clone()))
        .and(with_computations(computations.clone()))
        .and(warp::body::bytes())
        .and_then(api_scc);
    let aggregation_components = warp::path!("v1" / "aggregatecomponents")
        .and(warp::post())
        .and(with_auth(args.clone()))
        .and(with_graphs(graphs.clone()))
        .and(with_computations(computations.clone()))
        .and(warp::body::bytes())
        .and_then(api_aggregate_components);
    let pagerank = warp::path!("v1" / "pagerank")
        .and(warp::post())
        .and(with_auth(args.clone()))
        .and(with_graphs(graphs.clone()))
        .and(with_computations(computations.clone()))
        .and(warp::body::bytes())
        .and_then(api_pagerank);
    let python = warp::path!("v1" / "python")
        .and(warp::post())
        .and(with_auth(args.clone()))
        .and(with_graphs(graphs.clone()))
        .and(with_computations(computations.clone()))
        .and(warp::body::bytes())
        .and_then(api_python);
    let irank = warp::path!("v1" / "irank")
        .and(warp::post())
        .and(with_auth(args.clone()))
        .and(with_graphs(graphs.clone()))
        .and(with_computations(computations.clone()))
        .and(warp::body::bytes())
        .and_then(api_irank);
    let label_prop = warp::path!("v1" / "labelpropagation")
        .and(warp::post())
        .and(with_auth(args.clone()))
        .and(with_graphs(graphs.clone()))
        .and(with_computations(computations.clone()))
        .and(warp::body::bytes())
        .and_then(api_label_propagation);
    let attr_prop = warp::path!("v1" / "attributepropagation")
        .and(warp::post())
        .and(with_auth(args.clone()))
        .and(with_graphs(graphs.clone()))
        .and(with_computations(computations.clone()))
        .and(warp::body::bytes())
        .and_then(api_attribute_propagation);
    let get_arangodb_graph = warp::path!("v1" / "loaddata")
        .and(warp::post())
        .and(with_auth(args.clone()))
        .and(with_graphs(graphs.clone()))
        .and(with_computations(computations.clone()))
        .and(with_args(args.clone()))
        .and(warp::body::bytes())
        .and_then(api_get_arangodb_graph);
    let write_result_back_arangodb = warp::path!("v1" / "storeresults")
        .and(warp::post())
        .and(with_auth(args.clone()))
        .and(with_computations(computations.clone()))
        .and(with_args(args.clone()))
        .and(warp::body::bytes())
        .and_then(api_write_result_back_arangodb);
    let get_arangodb_graph_aql = warp::path!("v1" / "loaddataaql")
        .and(warp::post())
        .and(with_auth(args.clone()))
        .and(with_graphs(graphs.clone()))
        .and(with_computations(computations.clone()))
        .and(warp::body::bytes())
        .and_then(api_get_arangodb_graph_aql);
    let get_graph = warp::path!("v1" / "graphs" / u64)
        .and(warp::get())
        .and(with_auth(args.clone()))
        .and(with_graphs(graphs.clone()))
        .and_then(api_get_graph);
    let dump_graph = warp::path!("v1" / "dumpgraph" / u64)
        .and(warp::put())
        .and(with_auth(args.clone()))
        .and(with_graphs(graphs.clone()))
        .and_then(api_dump_graph);
    let drop_graph = warp::path!("v1" / "graphs" / u64)
        .and(warp::delete())
        .and(with_auth(args.clone()))
        .and(with_graphs(graphs.clone()))
        .and_then(api_drop_graph);
    let list_graphs = warp::path!("v1" / "graphs")
        .and(warp::get())
        .and(with_auth(args.clone()))
        .and(with_graphs(graphs.clone()))
        .and_then(api_list_graphs);
    let list_jobs = warp::path!("v1" / "jobs")
        .and(warp::get())
        .and(with_auth(args.clone()))
        .and(with_computations(computations.clone()))
        .and_then(api_list_jobs);

    version
        .or(get_job)
        .or(drop_job)
        .or(wcc)
        .or(scc)
        .or(aggregation_components)
        .or(pagerank)
        .or(python)
        .or(irank)
        .or(label_prop)
        .or(attr_prop)
        .or(get_arangodb_graph)
        .or(write_result_back_arangodb)
        .or(get_arangodb_graph_aql)
        .or(get_graph)
        .or(dump_graph)
        .or(drop_graph)
        .or(list_graphs)
        .or(list_jobs)
}

fn version_json(_user: String) -> Result<Response<Vec<u8>>, Error> {
    let version_str = format!(
        "{}.{}.{}",
        constants::VERSION >> 16,
        (constants::VERSION >> 8) & 0xff,
        constants::VERSION & 0xff
    );
    let body = serde_json::json!({
        "version": version_str,
        "apiMinVersion": 1,
        "apiMaxVersion": 1
    });
    let v = serde_json::to_vec(&body).expect("Should be serializable");
    Response::builder()
        .header("Content-Type", "application/json")
        .body(v)
}

fn check_graph(graph: &Graph, graph_id: u64, edges_must_be_sealed: bool) -> Result<(), String> {
    if !graph.vertices_sealed {
        return Err(format!("Graph vertices not sealed: {}", graph_id));
    }
    if edges_must_be_sealed {
        if !graph.edges_sealed {
            return Err(format!("Graph edges not sealed: {}", graph_id));
        }
    } else if graph.edges_sealed {
        return Err(format!("Graph edges must not be sealed: {}", graph_id,));
    }
    Ok(())
}

fn err_bad_req_process(e: String, ec: i32, c: StatusCode) -> WithStatus<Vec<u8>> {
    warp::reply::with_status(
        serde_json::to_vec(&GraphAnalyticsEngineProcessResponse {
            job_id: 0,
            error_code: ec,
            error_message: e,
        })
        .expect("Could not serialize"),
        c,
    )
}

fn get_and_check_graph(
    graphs: &Arc<Mutex<Graphs>>,
    graph_id: u64,
) -> Result<Arc<RwLock<Graph>>, WithStatus<Vec<u8>>> {
    let graph_arc: Arc<RwLock<Graph>>;
    {
        let graphs = graphs.lock().unwrap();
        let g = graphs.list.get(&graph_id);
        if g.is_none() {
            return Err(err_bad_req_process(
                format!("Graph with id {} not found.", &graph_id),
                404,
                StatusCode::NOT_FOUND,
            ));
        }
        graph_arc = g.unwrap().clone();
    }

    {
        // Check graph:
        let graph = graph_arc.read().unwrap();
        let r = check_graph(graph.deref(), graph_id, true);
        if let Err(e) = r {
            return Err(err_bad_req_process(e, 400, StatusCode::BAD_REQUEST));
        }
    }
    Ok(graph_arc)
}

/// This function triggers a WCC computation:
async fn api_wcc(
    _user: String,
    graphs: Arc<Mutex<Graphs>>,
    computations: Arc<Mutex<Computations>>,
    bytes: Bytes,
) -> Result<warp::reply::WithStatus<Vec<u8>>, Rejection> {
    // Parse body and extract integers:
    let parsed: serde_json::Result<GraphAnalyticsEngineWccSccRequest> =
        serde_json::from_slice(&bytes[..]);
    if let Err(e) = parsed {
        return Ok(err_bad_req_process(
            format!("Cannot parse JSON body of request: {}", e),
            400,
            StatusCode::BAD_REQUEST,
        ));
    }
    let body = parsed.unwrap();

    let graph_arc: Arc<RwLock<Graph>> = match get_and_check_graph(&graphs, body.graph_id) {
        Err(r) => {
            return Ok(r);
        }
        Ok(g) => g,
    };

    let comp_arc = Arc::new(RwLock::new(ComponentsComputation {
        algorithm: "WCC".to_string(),
        graph: graph_arc.clone(),
        components: None,
        next_in_component: None,
        shall_stop: false,
        number: None,
        error_code: 0,
        error_message: "".to_string(),
        runtime: JobRuntime::start(),
    }));
    let generic_comp_arc: Arc<RwLock<dyn Computation + Send + Sync>> = comp_arc.clone();
    std::thread::spawn(move || {
        let graph = graph_arc.read().unwrap();
        let res = algorithms::wcc::weakly_connected_components(&graph);
        let mut comp = comp_arc.write().unwrap();
        match res {
            Ok((nr, components, next)) => {
                info!("Found {} connected components.", nr);
                comp.components = Some(components);
                comp.next_in_component = Some(next);
                comp.number = Some(nr);
                comp.error_code = 0;
            }
            Err(e) => {
                comp.error_message = e;
                comp.error_code = 1;
            }
        }
        comp.runtime = comp.runtime.stop();
    });

    let comp_id: u64;
    {
        let mut comps = computations.lock().unwrap();
        comp_id = comps.register(generic_comp_arc.clone());
    }
    let response = GraphAnalyticsEngineProcessResponse {
        job_id: comp_id,
        error_code: 0,
        error_message: "".to_string(),
    };

    // Write response:
    let v = serde_json::to_vec(&response).expect("Should be serializable!");
    Ok(warp::reply::with_status(v, StatusCode::OK))
}

/// This function triggers a WCC computation:
async fn api_scc(
    _user: String,
    graphs: Arc<Mutex<Graphs>>,
    computations: Arc<Mutex<Computations>>,
    bytes: Bytes,
) -> Result<warp::reply::WithStatus<Vec<u8>>, Rejection> {
    // Parse body and extract integers:
    let parsed: serde_json::Result<GraphAnalyticsEngineWccSccRequest> =
        serde_json::from_slice(&bytes[..]);
    if let Err(e) = parsed {
        return Ok(err_bad_req_process(
            format!("Cannot parse JSON body of request: {}", e),
            400,
            StatusCode::BAD_REQUEST,
        ));
    }
    let body = parsed.unwrap();

    let graph_arc: Arc<RwLock<Graph>> = match get_and_check_graph(&graphs, body.graph_id) {
        Err(r) => {
            return Ok(r);
        }
        Ok(g) => g,
    };

    {
        // Make sure we have an edge index:
        let mut graph = graph_arc.write().unwrap();
        graph.index_edges(true, false);
    }

    let comp_arc = Arc::new(RwLock::new(ComponentsComputation {
        algorithm: "SCC".to_string(),
        graph: graph_arc.clone(),
        components: None,
        next_in_component: None,
        shall_stop: false,
        number: None,
        error_code: 0,
        error_message: "".to_string(),
        runtime: JobRuntime::start(),
    }));
    let generic_comp_arc: Arc<RwLock<dyn Computation + Send + Sync>> = comp_arc.clone();
    std::thread::spawn(move || {
        {
            // Make sure we have an edge index:
            let mut graph = graph_arc.write().unwrap();
            graph.index_edges(true, false);
        }
        let graph = graph_arc.read().unwrap();
        let res = algorithms::scc::strongly_connected_components(&graph);
        let mut comp = comp_arc.write().unwrap();
        match res {
            Ok((nr, components, next)) => {
                info!("Found {} connected components.", nr);
                comp.components = Some(components);
                comp.next_in_component = Some(next);
                comp.number = Some(nr);
                comp.error_code = 0;
            }
            Err(e) => {
                comp.error_message = e;
                comp.error_code = 1;
            }
        }
        comp.runtime = comp.runtime.stop();
    });

    let comp_id: u64;
    {
        let mut comps = computations.lock().unwrap();
        comp_id = comps.register(generic_comp_arc.clone());
    }
    let response = GraphAnalyticsEngineProcessResponse {
        job_id: comp_id,
        error_code: 0,
        error_message: "".to_string(),
    };

    // Write response:
    let v = serde_json::to_vec(&response).expect("Should be serializable!");
    Ok(warp::reply::with_status(v, StatusCode::OK))
}

/// This function triggers an aggregation computation over components:
async fn api_aggregate_components(
    _user: String,
    graphs: Arc<Mutex<Graphs>>,
    computations: Arc<Mutex<Computations>>,
    bytes: Bytes,
) -> Result<warp::reply::WithStatus<Vec<u8>>, Rejection> {
    // Parse body and extract integers:
    let parsed: serde_json::Result<GraphAnalyticsEngineAggregateComponentsRequest> =
        serde_json::from_slice(&bytes[..]);
    if let Err(e) = parsed {
        return Ok(err_bad_req_process(
            format!("Cannot parse JSON body of request: {}", e),
            400,
            StatusCode::BAD_REQUEST,
        ));
    }
    let body = parsed.unwrap();

    let graph_arc: Arc<RwLock<Graph>> = match get_and_check_graph(&graphs, body.graph_id) {
        Err(r) => {
            return Ok(r);
        }
        Ok(g) => g,
    };

    // Computation ID is optional:
    let mut prev_comp: Option<Arc<RwLock<dyn Computation + Send + Sync>>> = None;
    if body.job_id != 0 {
        let comps = computations.lock().unwrap();
        let comp = comps.list.get(&body.job_id);
        if comp.is_none() {
            return Ok(err_bad_req_process(
                format!("Could not find previous job id {}.", &body.job_id),
                400,
                StatusCode::BAD_REQUEST,
            ));
        }
        prev_comp = Some(comp.unwrap().clone());
    }

    let generic_comp_arc: Arc<RwLock<dyn Computation + Send + Sync>>;
    if prev_comp.is_none() {
        return Ok(err_bad_req_process(
            "Aggregation algorithm needs previous computation as absis to work".to_string(),
            400,
            StatusCode::BAD_REQUEST,
        ));
    }
    let prev_comp = prev_comp.unwrap();
    let guard = prev_comp.read().unwrap();
    let downcast = guard.as_any().downcast_ref::<ComponentsComputation>();
    if downcast.is_none() {
        // Wrong actual type!
        return Ok(err_bad_req_process(
            "Aggregation algorithm needs previous component computation as basis to
                work"
                .to_string(),
            400,
            StatusCode::BAD_REQUEST,
        ));
    }
    let attr = body.aggregation_attribute.clone();
    let comp_arc = Arc::new(RwLock::new(AggregationComputation {
        graph: graph_arc.clone(),
        compcomp: prev_comp.clone(),
        aggregation_attribute: attr.clone(),
        shall_stop: false,
        total: 1,
        progress: 0,
        error_code: 0,
        error_message: "".to_string(),
        result: vec![],
        runtime: JobRuntime::start(),
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
        comp.progress = 1;
        comp.runtime = comp.runtime.stop();
    });

    let comp_id: u64;
    {
        let mut comps = computations.lock().unwrap();
        comp_id = comps.register(generic_comp_arc.clone());
    }
    let response = GraphAnalyticsEngineProcessResponse {
        job_id: comp_id,
        error_code: 0,
        error_message: "".to_string(),
    };

    // Write response:
    let v = serde_json::to_vec(&response).expect("Should be serializable!");
    Ok(warp::reply::with_status(v, StatusCode::OK))
}

/// This function triggers a python based computation:
async fn api_python(
    _user: String,
    graphs: Arc<Mutex<Graphs>>,
    computations: Arc<Mutex<Computations>>,
    bytes: Bytes,
) -> Result<warp::reply::WithStatus<Vec<u8>>, Rejection> {
    // Parse body and extract integers:
    let parsed: serde_json::Result<GraphAnalyticsEnginePythonFunctionRequest> =
        serde_json::from_slice(&bytes[..]);
    if let Err(e) = parsed {
        return Ok(err_bad_req_process(
            format!("Cannot parse JSON body of request: {}", e),
            400,
            StatusCode::BAD_REQUEST,
        ));
    }
    let body = parsed.unwrap();

    let graph_arc: Arc<RwLock<Graph>> = match get_and_check_graph(&graphs, body.graph_id) {
        Err(r) => {
            return Ok(r);
        }
        Ok(g) => g,
    };

    let comp_arc = Arc::new(RwLock::new(PythonComputation {
        graph: graph_arc.clone(),
        algorithm: "Custom".to_string(),
        total: 3, // 1. Write graph to disk, 2. Execute & write computation to disk, 3. Read back
        progress: 0,
        error_code: 0,
        error_message: "".to_string(),
        result: Default::default(),
        runtime: JobRuntime::start(),
    }));
    let generic_comp_arc: Arc<RwLock<dyn Computation + Send + Sync>> = comp_arc.clone();

    std::thread::spawn(move || {
        let res = executor::execute_python_script_on_graph(
            comp_arc.clone(),
            graph_arc.clone(),
            body.function,
            body.use_cugraph,
        );

        let mut comp = comp_arc.write().unwrap();
        match res {
            Ok(()) => {
                info!("Finished python computation!");
                comp.error_code = 0;
            }
            Err(e) => {
                error!("Error during python computation: {}", e);
                comp.error_message = e;
                comp.error_code = 1;
            }
        }
        comp.runtime = comp.runtime.stop();
    });

    let comp_id: u64;
    {
        let mut comps = computations.lock().unwrap();
        comp_id = comps.register(generic_comp_arc);
    }
    let response = GraphAnalyticsEngineProcessResponse {
        job_id: comp_id,
        error_code: 0,
        error_message: "".to_string(),
    };

    // Write response:
    let v = serde_json::to_vec(&response).expect("Should be serializable!");
    Ok(warp::reply::with_status(v, StatusCode::OK))
}

/// This function triggers a pagerank computation:
async fn api_pagerank(
    _user: String,
    graphs: Arc<Mutex<Graphs>>,
    computations: Arc<Mutex<Computations>>,
    bytes: Bytes,
) -> Result<warp::reply::WithStatus<Vec<u8>>, Rejection> {
    // Parse body and extract integers:
    let parsed: serde_json::Result<GraphAnalyticsEnginePageRankRequest> =
        serde_json::from_slice(&bytes[..]);
    if let Err(e) = parsed {
        return Ok(err_bad_req_process(
            format!("Cannot parse JSON body of request: {}", e),
            400,
            StatusCode::BAD_REQUEST,
        ));
    }
    let body = parsed.unwrap();

    let graph_arc: Arc<RwLock<Graph>> = match get_and_check_graph(&graphs, body.graph_id) {
        Err(r) => {
            return Ok(r);
        }
        Ok(g) => g,
    };

    {
        // Make sure we have an edge index:
        let mut graph = graph_arc.write().unwrap();
        graph.index_edges(true, false);
    }
    let comp_arc = Arc::new(RwLock::new(PageRankComputation {
        graph: graph_arc.clone(),
        algorithm: "pagerank".to_string(),
        shall_stop: false,
        total: 100,
        progress: 0,
        error_code: 0,
        error_message: "".to_string(),
        steps: 0,
        rank: vec![],
        result_position: 0,
        runtime: JobRuntime::start(),
    }));
    let generic_comp_arc: Arc<RwLock<dyn Computation + Send + Sync>> = comp_arc.clone();
    std::thread::spawn(move || {
        let graph = graph_arc.read().unwrap();
        let res =
            algorithms::pagerank::page_rank(&graph, body.maximum_supersteps, body.damping_factor);
        info!("Finished pagerank computation!");
        let mut comp = comp_arc.write().unwrap();
        match res {
            Ok((rank, steps)) => {
                comp.rank = rank;
                comp.steps = steps;
                comp.error_code = 0;
            }
            Err(e) => {
                comp.error_message = e;
                comp.error_code = 1;
            }
        }
        comp.progress = 100;
        comp.runtime = comp.runtime.stop();
    });

    let comp_id: u64;
    {
        let mut comps = computations.lock().unwrap();
        comp_id = comps.register(generic_comp_arc);
    }
    let response = GraphAnalyticsEngineProcessResponse {
        job_id: comp_id,
        error_code: 0,
        error_message: "".to_string(),
    };

    // Write response:
    let v = serde_json::to_vec(&response).expect("Should be serializable!");
    Ok(warp::reply::with_status(v, StatusCode::OK))
}

/// This function triggers an irank computation:
async fn api_irank(
    _user: String,
    graphs: Arc<Mutex<Graphs>>,
    computations: Arc<Mutex<Computations>>,
    bytes: Bytes,
) -> Result<warp::reply::WithStatus<Vec<u8>>, Rejection> {
    // Parse body and extract integers:
    let parsed: serde_json::Result<GraphAnalyticsEnginePageRankRequest> =
        serde_json::from_slice(&bytes[..]);
    if let Err(e) = parsed {
        return Ok(err_bad_req_process(
            format!("Cannot parse JSON body of request: {}", e),
            400,
            StatusCode::BAD_REQUEST,
        ));
    }
    let body = parsed.unwrap();

    let graph_arc: Arc<RwLock<Graph>> = match get_and_check_graph(&graphs, body.graph_id) {
        Err(r) => {
            return Ok(r);
        }
        Ok(g) => g,
    };

    {
        // Make sure we have an edge index:
        let mut graph = graph_arc.write().unwrap();
        graph.index_edges(true, false);
    }
    let comp_arc = Arc::new(RwLock::new(PageRankComputation {
        graph: graph_arc.clone(),
        algorithm: "irank".to_string(),
        shall_stop: false,
        total: 100,
        progress: 0,
        error_code: 0,
        error_message: "".to_string(),
        steps: 0,
        rank: vec![],
        result_position: 0,
        runtime: JobRuntime::start(),
    }));
    let generic_comp_arc: Arc<RwLock<dyn Computation + Send + Sync>> = comp_arc.clone();
    std::thread::spawn(move || {
        let graph = graph_arc.read().unwrap();
        let res = algorithms::irank::i_rank(&graph, body.maximum_supersteps, body.damping_factor);
        info!("Finished irank computation!");
        let mut comp = comp_arc.write().unwrap();
        match res {
            Ok((rank, steps)) => {
                comp.rank = rank;
                comp.steps = steps;
                comp.error_code = 0;
            }
            Err(e) => {
                comp.error_message = e;
                comp.error_code = 1;
            }
        }
        comp.progress = 100;
        comp.runtime = comp.runtime.stop();
    });

    let comp_id: u64;
    {
        let mut comps = computations.lock().unwrap();
        comp_id = comps.register(generic_comp_arc.clone());
    }
    let response = GraphAnalyticsEngineProcessResponse {
        job_id: comp_id,
        error_code: 0,
        error_message: "".to_string(),
    };

    // Write response:
    let v = serde_json::to_vec(&response).expect("Should be serializable!");
    Ok(warp::reply::with_status(v, StatusCode::OK))
}

/// This function triggers a label propagation computation:
async fn api_label_propagation(
    _user: String,
    graphs: Arc<Mutex<Graphs>>,
    computations: Arc<Mutex<Computations>>,
    bytes: Bytes,
) -> Result<warp::reply::WithStatus<Vec<u8>>, Rejection> {
    // Parse body and extract integers:
    let parsed: serde_json::Result<GraphAnalyticsEngineLabelPropagationRequest> =
        serde_json::from_slice(&bytes[..]);
    if let Err(e) = parsed {
        return Ok(err_bad_req_process(
            format!("Cannot parse JSON body of request: {}", e),
            400,
            StatusCode::BAD_REQUEST,
        ));
    }
    let mut body = parsed.unwrap();

    let graph_arc: Arc<RwLock<Graph>> = match get_and_check_graph(&graphs, body.graph_id) {
        Err(r) => {
            return Ok(r);
        }
        Ok(g) => g,
    };

    {
        // Make sure we have an edge index:
        let mut graph = graph_arc.write().unwrap();
        graph.index_edges(true, true);
    }

    if body.maximum_supersteps == 0 {
        body.maximum_supersteps = 64;
    }

    let comp_arc = Arc::new(RwLock::new(LabelPropagationComputation {
        graph: graph_arc.clone(),
        sync: body.synchronous,
        shall_stop: false,
        total: 100,
        progress: 0,
        error_code: 0,
        error_message: "".to_string(),
        label: vec![],
        result_position: 0,
        label_size_sum: 0,
        runtime: JobRuntime::start(),
    }));
    let generic_comp_arc: Arc<RwLock<dyn Computation + Send + Sync>> = comp_arc.clone();
    let startlabel = body.start_label_attribute.clone();
    std::thread::spawn(move || {
        let graph = graph_arc.read().unwrap();
        let res = if body.synchronous {
            algorithms::labelpropagation::labelpropagation_sync(
                &graph,
                body.maximum_supersteps,
                &startlabel,
                body.random_tiebreak,
            )
        } else {
            algorithms::labelpropagation::labelpropagation_async(
                &graph,
                body.maximum_supersteps,
                &startlabel,
                body.random_tiebreak,
            )
        };
        info!("Finished label propagation computation!");
        let mut comp = comp_arc.write().unwrap();
        match res {
            Ok((label, label_size, _steps)) => {
                comp.label = label;
                comp.label_size_sum = label_size;
                comp.error_code = 0;
            }
            Err(e) => {
                comp.error_message = e;
                comp.error_code = 1;
            }
        }
        comp.progress = 100;
        comp.runtime = comp.runtime.stop();
    });

    let comp_id: u64;
    {
        let mut comps = computations.lock().unwrap();
        comp_id = comps.register(generic_comp_arc.clone());
    }
    let response = GraphAnalyticsEngineProcessResponse {
        job_id: comp_id,
        error_code: 0,
        error_message: "".to_string(),
    };

    // Write response:
    let v = serde_json::to_vec(&response).expect("Should be serializable!");
    Ok(warp::reply::with_status(v, StatusCode::OK))
}

/// This function triggers an attribute propagation computation:
async fn api_attribute_propagation(
    _user: String,
    graphs: Arc<Mutex<Graphs>>,
    computations: Arc<Mutex<Computations>>,
    bytes: Bytes,
) -> Result<warp::reply::WithStatus<Vec<u8>>, Rejection> {
    // Parse body and extract integers:
    let parsed: serde_json::Result<GraphAnalyticsEngineAttributePropagationRequest> =
        serde_json::from_slice(&bytes[..]);
    if let Err(e) = parsed {
        return Ok(err_bad_req_process(
            format!("Cannot parse JSON body of request: {}", e),
            400,
            StatusCode::BAD_REQUEST,
        ));
    }
    let mut body = parsed.unwrap();

    // Check arguments or set defaults:
    if body.graph_id == 0 {
        return Ok(err_bad_req_process(
            "Attribute 'graph_id' must be non-empty".to_string(),
            400,
            StatusCode::BAD_REQUEST,
        ));
    }
    if body.start_label_attribute.is_empty() {
        return Ok(err_bad_req_process(
            "Attribute 'start_label_attribute' must be non-empty".to_string(),
            400,
            StatusCode::BAD_REQUEST,
        ));
    }
    if body.maximum_supersteps == 0 {
        body.maximum_supersteps = 64;
    }

    let graph_arc: Arc<RwLock<Graph>> = match get_and_check_graph(&graphs, body.graph_id) {
        Err(r) => {
            return Ok(r);
        }
        Ok(g) => g,
    };

    {
        // Make sure we have an edge index:
        let mut graph = graph_arc.write().unwrap();
        graph.index_edges(false, true);
    }

    let comp_arc = Arc::new(RwLock::new(AttributePropagationComputation {
        graph: graph_arc.clone(),
        sync: body.synchronous,
        backwards: body.backwards,
        shall_stop: false,
        total: 100,
        progress: 0,
        error_code: 0,
        error_message: "".to_string(),
        label: vec![],
        result_position: 0,
        label_size_sum: 0,
        runtime: JobRuntime::start(),
    }));
    let generic_comp_arc: Arc<RwLock<dyn Computation + Send + Sync>> = comp_arc.clone();
    let startlabel = body.start_label_attribute.clone();
    std::thread::spawn(move || {
        let graph = graph_arc.read().unwrap();
        let res = if body.synchronous {
            algorithms::attributepropagation::attribute_propagation_async(
                &graph,
                body.maximum_supersteps,
                &startlabel,
                body.backwards,
            )
        } else {
            algorithms::attributepropagation::attribute_propagation_sync(
                &graph,
                body.maximum_supersteps,
                &startlabel,
                body.backwards,
            )
        };
        info!("Finished attribute propagation computation!");
        let mut comp = comp_arc.write().unwrap();
        match res {
            Ok((label, label_size, _steps)) => {
                comp.label = label;
                comp.label_size_sum = label_size;
                comp.error_code = 0;
            }
            Err(e) => {
                comp.error_message = e;
                comp.error_code = 1;
            }
        }
        comp.progress = 100;
        comp.runtime = comp.runtime.stop();
    });

    let comp_id: u64;
    {
        let mut comps = computations.lock().unwrap();
        comp_id = comps.register(generic_comp_arc.clone());
    }
    let response = GraphAnalyticsEngineProcessResponse {
        job_id: comp_id,
        error_code: 0,
        error_message: "".to_string(),
    };

    // Write response:
    let v = serde_json::to_vec(&response).expect("Should be serializable!");
    Ok(warp::reply::with_status(v, StatusCode::OK))
}

/// This function writes a computation result back to ArangoDB:
async fn api_write_result_back_arangodb(
    user: String,
    computations: Arc<Mutex<Computations>>,
    args: Arc<Mutex<GralArgs>>,
    bytes: Bytes,
) -> Result<warp::reply::WithStatus<Vec<u8>>, Rejection> {
    let err_bad_req = |e: String, sc: StatusCode| {
        warp::reply::with_status(
            serde_json::to_vec(&GraphAnalyticsEngineStoreResultsResponse {
                job_id: 0,
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
            format!("Could not parse JSON body: {}", e),
            StatusCode::BAD_REQUEST,
        ));
    }
    let mut body = parsed.unwrap();

    let mut result_comps: Vec<Arc<RwLock<dyn Computation + Send + Sync>>> = vec![];
    {
        let comps = computations.lock().unwrap();
        for id in &body.job_ids {
            let compfound = comps.list.get(id);
            if compfound.is_none() {
                return Ok(err_bad_req(
                    format!("Job {} not found.", id),
                    StatusCode::NOT_FOUND,
                ));
            }
            result_comps.push(compfound.unwrap().clone());
        }
    }

    if result_comps.len() != body.attribute_names.len() {
        return Ok(err_bad_req(
            format!("Number of computations ({}) must be the same as the number of attribute names ({})",
                    result_comps.len(), body.attribute_names.len()),
            StatusCode::BAD_REQUEST));
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
        body.target_collection = "targetCollection".to_string();
    }

    // Now create a job object:
    let comp_arc = Arc::new(RwLock::new(StoreComputation {
        comp: result_comps.clone(),
        shall_stop: false,
        total: 1, // will eventually be overwritten in background thread
        progress: 0,
        error_code: 0,
        error_message: "".to_string(),
        runtime: JobRuntime::start(),
    }));
    let comp_id: u64;
    {
        let mut comps = computations.lock().unwrap();
        comp_id = comps.register(comp_arc.clone());
    }

    // Write to ArangoDB in a background thread:
    let user_clone = user.clone();
    let attribute_names_clone = body.attribute_names.clone();
    std::thread::spawn(move || {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let res = write_result_to_arangodb(
                    user_clone,
                    body,
                    args,
                    result_comps.clone(),
                    attribute_names_clone,
                    comp_arc.clone(),
                )
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
                comp.runtime = comp.runtime.stop();
            });
    });

    let response = GraphAnalyticsEngineStoreResultsResponse {
        job_id: comp_id,
        error_code: 0,
        error_message: "".to_string(),
    };

    // Write response:
    let v = serde_json::to_vec(&response).expect("Should be serializable!");
    Ok(warp::reply::with_status(v, StatusCode::OK))
}

/// This function writes a computation result back to ArangoDB:
async fn api_get_arangodb_graph_aql(
    _user: String,
    _graphs: Arc<Mutex<Graphs>>,
    _computations: Arc<Mutex<Computations>>,
    bytes: Bytes,
) -> Result<warp::reply::WithStatus<Vec<u8>>, Rejection> {
    let err_bad_req = |e: String| {
        warp::reply::with_status(
            serde_json::to_vec(&GraphAnalyticsEngineLoadDataResponse {
                job_id: 0,
                graph_id: 0,
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
        return Ok(err_bad_req(format!("Could not parse JSON body: {}", e)));
    }
    let _body = parsed.unwrap();

    // TO BE IMPLEMENTED

    let response = GraphAnalyticsEngineLoadDataResponse {
        job_id: 0,
        graph_id: 0,
        error_code: 1,
        error_message: "NOT_YET_IMPLEMENTED".to_string(),
    };

    // Write response:
    let v = serde_json::to_vec(&response).expect("Should be serializable!");
    Ok(warp::reply::with_status(v, StatusCode::OK))
}

/// This function gets progress of a computation.
async fn api_get_job(
    job_id: u64,
    _user: String,
    computations: Arc<Mutex<Computations>>,
) -> Result<warp::reply::WithStatus<Vec<u8>>, Rejection> {
    let not_found_err = |j: String| {
        warp::reply::with_status(
            serde_json::to_vec(&GraphAnalyticsEngineJob {
                job_id: 0,
                graph_id: 0,
                total: 0,
                progress: 0,
                source_job: "".into(),
                error: true,
                error_code: 404,
                error_message: j,
                comp_type: "".to_string(),
                memory_usage: 0,
                runtime_in_microseconds: 0,
            })
            .unwrap(),
            StatusCode::NOT_FOUND,
        )
    };

    let comps = computations.lock().unwrap();
    let comp_arc = comps.list.get(&job_id);
    match comp_arc {
        None => Ok(not_found_err(format!("Could not find jobId {}", job_id))),
        Some(comp_arc) => {
            let comp = comp_arc.read().unwrap();
            let response = comp.job_info(job_id);
            Ok(warp::reply::with_status(
                serde_json::to_vec(&response).expect("Should be serializable"),
                StatusCode::OK,
            ))
        }
    }
}

/// This function deletes a job.
async fn api_drop_job(
    job_id: u64,
    _user: String,
    computations: Arc<Mutex<Computations>>,
) -> Result<warp::reply::WithStatus<Vec<u8>>, Rejection> {
    let not_found_err = |j: String| {
        warp::reply::with_status(
            serde_json::to_vec(&GraphAnalyticsEngineDeleteJobResponse {
                job_id: 0,
                error: true,
                error_code: 404,
                error_message: j,
            })
            .unwrap(),
            StatusCode::NOT_FOUND,
        )
    };

    let mut comps = computations.lock().unwrap();
    let comp_arc = comps.list.get(&job_id);
    match comp_arc {
        None => Ok(not_found_err(format!("Could not find job {}", job_id))),
        Some(comp_arc) => {
            {
                let mut comp = comp_arc.write().unwrap();
                comp.cancel();
            }
            comps.remove(job_id);

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
    graph_id: u64,
    _user: String,
    graphs: Arc<Mutex<Graphs>>,
) -> Result<warp::reply::WithStatus<Vec<u8>>, Rejection> {
    let not_found_err = |j: String| {
        warp::reply::with_status(
            serde_json::to_vec(&GraphAnalyticsEngineGetGraphResponse {
                error_code: 404,
                error_message: j.clone(),
                graph: None,
            })
            .expect("Could not serialize"),
            StatusCode::NOT_FOUND,
        )
    };

    let graphs = graphs.lock().unwrap();
    let graph_arc = graphs.list.get(&graph_id);
    if graph_arc.is_none() {
        return Ok(not_found_err(format!("Graph {} not found!", graph_id)));
    }
    let graph_arc = graph_arc.unwrap().clone();
    let graph = graph_arc.read().unwrap();

    // Write response:
    let mem_usage = graph.memory_usage();
    let response = GraphAnalyticsEngineGetGraphResponse {
        error_code: 0,
        error_message: "".to_string(),
        graph: Some(GraphAnalyticsEngineGraph {
            graph_id,
            number_of_vertices: graph.number_of_vertices(),
            number_of_edges: graph.number_of_edges(),
            memory_usage: mem_usage.bytes_total as u64,
            memory_per_vertex: mem_usage.bytes_per_vertex as u64,
            memory_per_edge: mem_usage.bytes_per_edge as u64,
        }),
    };
    Ok(warp::reply::with_status(
        serde_json::to_vec(&response).expect("Should be serializable"),
        StatusCode::OK,
    ))
}

/// This function dumps a graph to stdout:
async fn api_dump_graph(
    graph_id: u64,
    _user: String,
    graphs: Arc<Mutex<Graphs>>,
) -> Result<warp::reply::WithStatus<Vec<u8>>, Rejection> {
    let not_found_err = |j: String| {
        warp::reply::with_status(
            serde_json::to_vec(&GraphAnalyticsEngineGetGraphResponse {
                error_code: 404,
                error_message: j.clone(),
                graph: None,
            })
            .expect("Could not serialize"),
            StatusCode::NOT_FOUND,
        )
    };

    let graph_arc;
    {
        let graphs = graphs.lock().unwrap();
        let graph_arc_opt = graphs.list.get(&graph_id);
        if graph_arc_opt.is_none() {
            return Ok(not_found_err(format!("Graph {} not found!", graph_id)));
        }
        graph_arc = graph_arc_opt.unwrap().clone();
    }
    let graph = graph_arc.read().unwrap();
    graph.dump();

    // Write response:
    let mem_usage = graph.memory_usage();
    let response = GraphAnalyticsEngineGetGraphResponse {
        error_code: 0,
        error_message: "".to_string(),
        graph: Some(GraphAnalyticsEngineGraph {
            graph_id,
            number_of_vertices: graph.number_of_vertices(),
            number_of_edges: graph.number_of_edges(),
            memory_usage: mem_usage.bytes_total as u64,
            memory_per_vertex: mem_usage.bytes_per_vertex as u64,
            memory_per_edge: mem_usage.bytes_per_edge as u64,
        }),
    };
    Ok(warp::reply::with_status(
        serde_json::to_vec(&response).expect("Should be serializable"),
        StatusCode::OK,
    ))
}

/// This function lists graphs:
async fn api_list_graphs(_user: String, graphs: Arc<Mutex<Graphs>>) -> Result<Vec<u8>, Rejection> {
    let graphs = graphs.lock().unwrap();
    let mut response = vec![];
    for (_id, graph_arc) in graphs.list.iter() {
        let graph = graph_arc.read().unwrap();

        // Write response:
        let mem_usage = graph.memory_usage();
        let g = GraphAnalyticsEngineGraph {
            graph_id: graph.graph_id,
            number_of_vertices: graph.number_of_vertices(),
            number_of_edges: graph.number_of_edges(),
            memory_usage: mem_usage.bytes_total as u64,
            memory_per_vertex: mem_usage.bytes_per_vertex as u64,
            memory_per_edge: mem_usage.bytes_per_edge as u64,
        };
        response.push(g);
    }
    Ok(serde_json::to_vec(&response).expect("Should be serializable"))
}

async fn api_list_jobs(
    _user: String,
    computations: Arc<Mutex<Computations>>,
) -> Result<Vec<u8>, Rejection> {
    let comps = computations.lock().unwrap();
    let mut response: Vec<GraphAnalyticsEngineJob> = vec![];
    for (job_id, comp_arc) in comps.list.iter() {
        let comp = comp_arc.read().unwrap();
        response.push(comp.job_info(*job_id));
    }
    Ok(serde_json::to_vec(&response).expect("Should be serializable"))
}

/// This function drops a graph:
async fn api_drop_graph(
    graph_id: u64,
    _user: String,
    graphs: Arc<Mutex<Graphs>>,
) -> Result<warp::reply::WithStatus<Vec<u8>>, Rejection> {
    let not_found_err = |j: String| {
        warp::reply::with_status(
            serde_json::to_vec(&GraphAnalyticsEngineGetGraphResponse {
                error_code: 404,
                error_message: j.clone(),
                graph: None,
            })
            .expect("Could not serialize"),
            StatusCode::NOT_FOUND,
        )
    };

    let mut graphs = graphs.lock().unwrap();
    let graph_arc = graphs.list.get(&graph_id);
    if graph_arc.is_none() {
        return Ok(not_found_err(format!("Graph {} not found!", graph_id)));
    }

    // The following will automatically free graph if no longer used by
    // a computation:
    graphs.remove(graph_id);
    info!("Have dropped graph {}!", graph_id);

    // Write response:
    let response = GraphAnalyticsEngineDeleteGraphResponse {
        graph_id,
        error_code: 0,
        error_message: "".to_string(),
    };
    Ok(warp::reply::with_status(
        serde_json::to_vec(&response).expect("Should be serializable"),
        StatusCode::OK,
    ))
}

async fn api_get_arangodb_graph(
    user: String,
    graphs: Arc<Mutex<Graphs>>,
    comps: Arc<Mutex<Computations>>,
    args: Arc<Mutex<GralArgs>>,
    bytes: Bytes,
) -> Result<warp::reply::WithStatus<Vec<u8>>, Rejection> {
    let parsed: serde_json::Result<GraphAnalyticsEngineLoadDataRequest> =
        serde_json::from_slice(&bytes[..]);
    if let Err(e) = parsed {
        let response = GraphAnalyticsEngineLoadDataResponse {
            job_id: 0,
            graph_id: 0,
            error_code: 400,
            error_message: format!("Could not parse JSON body: {}", e),
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

    let graphsclone = graphs.clone(); // for later removal in subthread
    let graph = Arc::new(RwLock::new(Graph::new(
        true,
        body.vertex_attributes.clone(),
    )));

    // And store it amongst the graphs:
    let mut graphs = graphs.lock().unwrap();
    let graph_id = graphs.register(graph.clone());

    info!("Have created graph with id {}!", graph_id);

    // Now create a job object:
    let comp_arc = Arc::new(RwLock::new(LoadComputation {
        graph_id,
        shall_stop: false,
        total: 2, // will eventually be overwritten in background thread
        progress: 0,
        error_code: 0,
        error_message: "".to_string(),
        runtime: JobRuntime::start(),
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
                    fetch_graph_from_arangodb(user, body, args, graph.clone(), comp_arc.clone())
                        .await;
                let mut comp = comp_arc.write().unwrap();
                match res {
                    Ok(()) => {
                        comp.error_code = 0;
                        comp.error_message = "".to_string();
                    }
                    Err(e) => {
                        info!("Could not load graph successfully: {}", e);
                        comp.error_code = 1;
                        comp.error_message = e;
                        // Get rid of graph again:
                        let graph_id;
                        {
                            let graphguard = graph.read().unwrap();
                            graph_id = graphguard.graph_id;
                        }
                        let mut graphsguard = graphsclone.lock().unwrap();
                        graphsguard.remove(graph_id);
                        // Note that the graph will still be attached to
                        // the computation! Once the computation is
                        // deleted, the graph will be freed!
                        comp.runtime = comp.runtime.stop();
                    }
                }
            });
    });

    // Write response:
    let response = GraphAnalyticsEngineLoadDataResponse {
        job_id: comp_id,
        graph_id,
        error_code: 0,
        error_message: "".to_string(),
    };
    Ok(warp::reply::with_status(
        serde_json::to_vec(&response).expect("Could not serialize"),
        StatusCode::OK,
    ))
}

// This function receives a `Rejection` and is responsible to convert
// this into a proper HTTP error with a body as designed.
pub async fn handle_errors(err: Rejection) -> Result<impl warp::Reply, Infallible> {
    let code;
    let message: String;

    if err.is_not_found() {
        code = StatusCode::NOT_FOUND;
        message = "NOT_FOUND".to_string();
    } else if err.find::<warp::reject::MethodNotAllowed>().is_some() {
        // We can handle a specific error, here METHOD_NOT_ALLOWED,
        // and render it however we want
        code = StatusCode::METHOD_NOT_ALLOWED;
        message = "METHOD_NOT_ALLOWED".to_string();
    } else if let Some(wrong) = err.find::<Unauthorized>() {
        code = StatusCode::UNAUTHORIZED;
        message = wrong.msg.clone();
    } else {
        code = StatusCode::INTERNAL_SERVER_ERROR;
        message = "Unknown error happened".to_string();
    }

    Ok(warp::reply::with_status(
        serde_json::to_vec(&GraphAnalyticsEngineErrorResponse {
            error_code: code.as_u16() as i32,
            error_message: message,
        })
        .unwrap(),
        code,
    ))
}
