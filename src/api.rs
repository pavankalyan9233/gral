use crate::graphs::{with_graphs, Graph, Graphs};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use bytes::Bytes;
use std::convert::Infallible;
use std::io::Cursor;
use std::sync::{Arc, Mutex};
use warp::{http::StatusCode, reject, Filter, Rejection};

pub fn api_filter(
    graphs: Arc<Mutex<Graphs>>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    let create = warp::path!("v1" / "create")
        .and(warp::post())
        .and(with_graphs(graphs.clone()))
        .and(warp::body::bytes())
        .and_then(api_create);
    let drop = warp::path!("v1" / "drop")
        .and(warp::put())
        .and(with_graphs(graphs.clone()))
        .and(warp::body::bytes())
        .and_then(api_drop);
    create.or(drop)
}

#[derive(Debug)]
struct WrongBodyLength {
    pub found: usize,
    pub expected: usize,
}
impl reject::Reject for WrongBodyLength {}

#[derive(Debug)]
struct GraphNotFound;
impl reject::Reject for GraphNotFound {}

async fn api_create(graphs: Arc<Mutex<Graphs>>, bytes: Bytes) -> Result<Vec<u8>, Rejection> {
    // Handle wrong length:
    if bytes.len() != 26 {
        return Err(warp::reject::custom(WrongBodyLength {
            found: bytes.len(),
            expected: 26,
        }));
    }

    // Parse body and extract integers:
    let mut reader = Cursor::new(bytes.to_vec());
    let client_id: u64 = reader.read_u64::<BigEndian>().unwrap();
    // let max_number_vertices: u64 =
    reader.read_u64::<BigEndian>().unwrap();
    // let max_number_edges: u64 =
    reader.read_u64::<BigEndian>().unwrap();
    // let mut bits_for_hash: u8 =
    reader.read_u8().unwrap(); // ignored for now!
    let store_keys: u8 = reader.read_u8().unwrap();

    let bits_for_hash = 64; // any other value ignored for now!

    // Lock list of graphs via their mutex:
    let mut graphs = graphs.lock().unwrap();

    // First try to find an empty spot:
    let mut index: u32 = 0;
    let mut found: bool = false;
    for g in graphs.list.iter_mut() {
        // Lock graph mutex:
        let dropped: bool;
        {
            let gg = g.lock().unwrap();
            dropped = gg.dropped;
        }
        if dropped {
            *g = Graph::new(store_keys != 0, 64);
            found = true;
            break;
        }
        index += 1;
    }
    // or else append to the end:
    if !found {
        index = graphs.list.len() as u32;
        graphs.list.push(Graph::new(store_keys != 0, 64));
    }
    // By now, index is always set to some sensible value!

    println!("Have created graph with number {}!", index);

    // Write response:
    let mut v = Vec::new();
    v.write_u64::<BigEndian>(client_id).unwrap();
    v.write_u32::<BigEndian>(index).unwrap();
    v.write_u8(bits_for_hash).unwrap();
    Ok(v)
}

async fn api_drop(graphs: Arc<Mutex<Graphs>>, bytes: Bytes) -> Result<Vec<u8>, Rejection> {
    // Handle wrong length:
    if bytes.len() != 12 {
        return Err(warp::reject::custom(WrongBodyLength {
            found: bytes.len(),
            expected: 12,
        }));
    }

    // Parse body and extract integers:
    let mut reader = Cursor::new(bytes.to_vec());
    let client_id = reader.read_u64::<BigEndian>().unwrap();
    let graph_number = reader.read_u32::<BigEndian>().unwrap() as usize;

    println!("Dropping graph with number {}!", graph_number);

    let graph_arc: Arc<Mutex<Graph>>;

    {
        // Lock list of graphs via their mutex:
        let graphs = graphs.lock().unwrap();
        if graph_number as usize >= graphs.list.len() {
            // TODO: handle out of bounds
        }
        graph_arc = graphs.list[graph_number].clone();
    }

    // Lock graph:
    let mut graph = graph_arc.lock().unwrap();

    // TODO: Handle already dropped!

    graph.clear();
    graph.dropped = true;

    println!("Have dropped graph with number {}!", graph_number);

    // Write response:
    let mut v = Vec::new();
    v.write_u64::<BigEndian>(client_id).unwrap();
    v.write_u32::<BigEndian>(graph_number as u32).unwrap();
    Ok(v)
}

// This function receives a `Rejection` and is responsible to convert
// this into a proper HTTP error with a body as designed.
pub async fn handle_errors(err: Rejection) -> Result<impl warp::Reply, Infallible> {
    let code;
    let message: String;

    if err.is_not_found() {
        code = StatusCode::NOT_FOUND;
        message = "NOT_FOUND".to_string();
    } else if let Some(_) = err.find::<warp::reject::MethodNotAllowed>() {
        // We can handle a specific error, here METHOD_NOT_ALLOWED,
        // and render it however we want
        code = StatusCode::METHOD_NOT_ALLOWED;
        message = "METHOD_NOT_ALLOWED".to_string();
    } else if let Some(wrong) = err.find::<WrongBodyLength>() {
        code = StatusCode::BAD_REQUEST;
        message = format!(
            "Expected body size {} but found {}",
            wrong.expected, wrong.found
        );
    } else {
        // We should have expected this... Just log and say its a 500
        eprintln!("unhandled rejection: {:?}", err);
        code = StatusCode::INTERNAL_SERVER_ERROR;
        message = "UNHANDLED_REJECTION".to_string();
    }

    let mut v = Vec::new();
    v.write_u32::<BigEndian>(code.as_u16() as u32).unwrap();
    if message.len() < 128 {
        v.write_u8(message.len() as u8).unwrap();
    } else {
        v.write_u32::<BigEndian>((message.len() as u32) | 0x80000000)
            .unwrap();
    }
    v.reserve(message.len());
    for x in message.bytes() {
        v.push(x);
    }
    Ok(warp::reply::with_status(v, code))
}
