use crate::graphs::{with_graphs, Graph, Graphs};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use bytes::Bytes;
use std::io::Cursor;
use std::sync::{Arc, Mutex};
use warp::{reject, Filter, Rejection};

pub fn api_filter(
    graphs: Arc<Mutex<Graphs>>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    let create = warp::path!("v1" / "create")
        .and(warp::post())
        .and(with_graphs(graphs.clone()))
        .and(warp::body::bytes())
        .and_then(api_create);
    create
}

#[derive(Debug)]
struct WrongBodyLength;
impl reject::Reject for WrongBodyLength {}

async fn api_create(graphs: Arc<Mutex<Graphs>>, bytes: Bytes) -> Result<Vec<u8>, Rejection> {
    // Handle wrong length:
    if bytes.len() != 26 {
        return Err(warp::reject::custom(WrongBodyLength));
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
