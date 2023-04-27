use crate::GruploadArgs;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
//use bytes::Bytes;
use rand::Rng;
use reqwest;
use std::io::Cursor;

pub fn create(args: &GruploadArgs) {
    println!("Creating graph... {:?}", args);
    let client = reqwest::blocking::Client::new();
    let mut v: Vec<u8> = vec![];
    let mut rng = rand::thread_rng();
    let client_id = rng.gen::<u64>();
    v.write_u64::<BigEndian>(client_id).unwrap();
    v.write_u64::<BigEndian>(args.max_vertices).unwrap();
    v.write_u64::<BigEndian>(args.max_edges).unwrap();
    v.write_u8(64).unwrap();
    v.write_u8(if args.store_keys { 1 } else { 0 }).unwrap();
    let mut url = args.endpoint.clone();
    url.push_str("/v1/create");
    let resp = match client.post(url).body(v).send() {
        Ok(resp) => resp,
        Err(err) => panic!("Error: {}", err),
    };
    let status_code = resp.status();
    let body = resp.bytes().unwrap();
    let mut cursor = Cursor::new(&body);
    let _client_id_back = cursor.read_u64::<BigEndian>().unwrap();
    let graph_number = cursor.read_u32::<BigEndian>().unwrap();
    let bits_per_hash = cursor.read_u8().unwrap();

    println!(
        "Code: {}, graph number: {}, bits per hash: {}",
        status_code, graph_number, bits_per_hash
    );
}
