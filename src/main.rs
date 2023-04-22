use byteorder::{BigEndian, WriteBytesExt};
use std::{convert::Infallible, sync::Arc};
use warp::{http::Response, Filter};

mod api;
use crate::api::api_filter;

const VERSION: u32 = 0x00100;

pub struct Graphs {
    pub number: u32,
}

pub fn with_graphs(
    graphs: Arc<Graphs>,
) -> impl Filter<Extract = (Arc<Graphs>,), Error = Infallible> + Clone {
    warp::any().map(move || graphs.clone())
}

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
    let the_graphs = Arc::new(Graphs { number: 17 });
    let apifilters = version.or(api_filter(the_graphs.clone()));
    warp::serve(apifilters)
        //.tls()
        //.cert_path("tls/cert.pem")
        //.key_path("tls/key.pem")
        //.client_auth_required_path("tls/authca.pem")
        .run(([0, 0, 0, 0], 9999))
        .await;
}
