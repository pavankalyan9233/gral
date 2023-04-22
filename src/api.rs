use crate::{with_graphs, Graphs};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use bytes::Bytes;
use std::io::Cursor;
use std::sync::Arc;
use warp::Filter;

pub fn api_filter(
    graphs: Arc<Graphs>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    let create = warp::path!("v1" / "create")
        .and(warp::post())
        .and(with_graphs(graphs.clone()))
        .and(warp::body::bytes())
        .and_then(api_create);
    create
}

async fn api_create(graphs: Arc<Graphs>, bytes: Bytes) -> Result<Vec<u8>, warp::Rejection> {
    // Handle wrong length:
    // ...
    // Parse body and extract integers:
    let mut reader = Cursor::new(bytes.to_vec());
    let client_id: u64 = reader.read_u64::<BigEndian>().unwrap();
    // ...
    let mut v: Vec<u8> = Vec::new();
    // Write response:
    v.write_u64::<BigEndian>(client_id).unwrap();
    Ok(v)
}
