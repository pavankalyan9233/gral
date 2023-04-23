use byteorder::{BigEndian, WriteBytesExt};
use std::{sync::Arc, sync::Mutex};
use warp::{http::Response, Filter};

mod api;
mod graphs;

use crate::api::api_filter;
use crate::graphs::Graphs;

const VERSION: u32 = 0x00100;

#[tokio::main]
async fn main() {
    // Setup version handler directly here:
    let version = warp::path!("v1" / "version").and(warp::get()).map(|| {
        let mut v = Vec::new();
        v.write_u32::<BigEndian>(VERSION as u32).unwrap();
        v.write_u32::<BigEndian>(1 as u32).unwrap();
        v.write_u32::<BigEndian>(1 as u32).unwrap();

        Response::builder()
            .header("Content-Type", "x-application-gral")
            .body(v)
    });
    let the_graphs = Arc::new(Mutex::new(Graphs { list: vec![] }));
    let apifilters = version.or(api_filter(the_graphs.clone()));
    warp::serve(apifilters)
        //.tls()
        //.cert_path("tls/cert.pem")
        //.key_path("tls/key.pem")
        //.client_auth_required_path("tls/authca.pem")
        .run(([0, 0, 0, 0], 9999))
        .await;
}
