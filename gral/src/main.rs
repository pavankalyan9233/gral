use byteorder::{BigEndian, WriteBytesExt};
use std::net::IpAddr;
use std::sync::{Arc, Mutex};
use warp::{http::Response, Filter};

mod api;
mod args;
mod computations;
mod conncomp;
mod graphs;

use crate::api::{api_filter, handle_errors};
use crate::args::parse_args;
use crate::computations::Computations;
use crate::graphs::Graphs;

const VERSION: u32 = 0x00100;

#[tokio::main]
async fn main() {
    let args = match parse_args() {
        Ok(v) => v,
        Err(e) => {
            eprintln!("Error: {}.", e);
            std::process::exit(1);
        }
    };

    println!("{:#?}", args);

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
    let the_computations = Arc::new(Mutex::new(Computations::new()));

    let apifilters = version
        .or(api_filter(the_graphs.clone(), the_computations.clone()))
        .recover(handle_errors);
    let ip_addr: IpAddr = args
        .bind_addr
        .parse()
        .expect(format!("Could not parse bind address: {}", args.bind_addr).as_str());
    if args.use_tls {
        warp::serve(apifilters)
            .tls()
            .cert_path("tls/cert.pem")
            .key_path("tls/key.pem")
            .client_auth_required_path("tls/authca.pem")
            .run((ip_addr, args.port))
            .await;
    } else {
        warp::serve(apifilters).run((ip_addr, args.port)).await;
    }
}
