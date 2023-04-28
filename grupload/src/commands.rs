use crate::GruploadArgs;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use bytes::Bytes;
use rand::Rng;
use reqwest::{blocking::Response, StatusCode};
use std::io::{Cursor, Read};
use std::str;

pub fn status(c: u16) -> StatusCode {
    StatusCode::from_u16(c).unwrap()
}

/// A varlen is a length marker which can either be
///  - 0 to indicate something special (zero length or something else)
///  - between 1 and 0x7f to indicate this length in one byte
///  - be a u32 BigEndian with high bit set (so that the first byte is
///    in the range 0x80..0xff and then indicates the length.
/// This function extracts a varlen from the cursor c.
///
fn get_varlen(c: &mut Cursor<&Bytes>) -> Result<u32, std::io::Error> {
    let mut b = c.read_u8()?;
    match b {
        0 => Ok(0),
        1..=0x7f => Ok(b as u32),
        _ => {
            let mut r = (b & 0x7f) as u32;
            for _i in 1..=3 {
                b = c.read_u8()?;
                r = (r << 8) | (b as u32);
            }
            Ok(r)
        }
    }
}

pub fn handle_error(resp: &Response, ok: StatusCode) -> Result<(), String> {
    if resp.status() == ok {
        return Ok(());
    }
    let body = resp.bytes().unwrap();
    if body.len() < 5 {
        return Err("Too short body, no error code found.".to_string());
    }
    let mut cursor = Cursor::new(&body);
    let code = cursor.read_u32::<BigEndian>().unwrap();
    let len = match get_varlen(&mut cursor) {
        Err(err) => return Err("abc".to_string()),
        Ok(v) => v,
    };
    let mut buf = vec![0u8; len as usize];
    match cursor.read_exact(&mut buf) {
        Err(err) => return Err(format!("Could not read error response,  code: {}", code)),
        _ => (),
    }
    let msg = match str::from_utf8(&buf) {
        Ok(v) => v,
        Err(e) => return Err(format!("Error message is no UTF-8, code: {}", code)),
    };
    Err(format!("Error: code={}, {}", code, msg))
}

pub fn create(args: &GruploadArgs) -> Result<(), String> {
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
    handle_error(&resp, status(200))?;

    let body = resp.bytes().unwrap();
    let mut cursor = Cursor::new(&body);
    let _client_id_back = cursor.read_u64::<BigEndian>().unwrap();
    let graph_number = cursor.read_u32::<BigEndian>().unwrap();
    let bits_per_hash = cursor.read_u8().unwrap();

    println!(
        "Graph number: {}, bits per hash: {}",
        graph_number, bits_per_hash
    );
    return Ok(());
}

pub fn seal_vertices(args: &GruploadArgs) -> Result<(), String> {
    println!("Sealing vertices... {:?}", args);
    let client = reqwest::blocking::Client::new();
    let mut v: Vec<u8> = vec![];
    let mut rng = rand::thread_rng();
    let client_id = rng.gen::<u64>();
    v.write_u64::<BigEndian>(client_id).unwrap();
    v.write_u32::<BigEndian>(args.graph_number).unwrap();

    let mut url = args.endpoint.clone();
    url.push_str("/v1/sealVertices");
    let resp = match client.post(url).body(v).send() {
        Ok(resp) => resp,
        Err(err) => panic!("Error: {}", err),
    };
    handle_error(&resp, status(200))?;

    let body = resp.bytes().unwrap();
    let mut cursor = Cursor::new(&body);
    let _client_id_back = cursor.read_u64::<BigEndian>().unwrap();
    let graph_number = cursor.read_u32::<BigEndian>().unwrap();
    let number_of_vertices = cursor.read_u64::<BigEndian>().unwrap();

    println!(
        "Graph number: {}, number of vertices: {}",
        graph_number, number_of_vertices
    );
    Ok(())
}

pub fn seal_edges(args: &GruploadArgs) -> Result<(), String> {
    println!("Sealing edges... {:?}", args);
    let client = reqwest::blocking::Client::new();
    let mut v: Vec<u8> = vec![];
    let mut rng = rand::thread_rng();
    let client_id = rng.gen::<u64>();
    v.write_u64::<BigEndian>(client_id).unwrap();
    v.write_u32::<BigEndian>(args.graph_number).unwrap();

    let mut url = args.endpoint.clone();
    url.push_str("/v1/sealEdges");
    let resp = match client.post(url).body(v).send() {
        Ok(resp) => resp,
        Err(err) => panic!("Error: {}", err),
    };
    handle_error(&resp, status(200));

    let body = resp.bytes().unwrap();
    let mut cursor = Cursor::new(&body);
    let _client_id_back = cursor.read_u64::<BigEndian>().unwrap();
    let graph_number = cursor.read_u32::<BigEndian>().unwrap();
    let number_of_vertices = cursor.read_u64::<BigEndian>().unwrap();
    let number_of_edges = cursor.read_u64::<BigEndian>().unwrap();

    println!(
        "Graph number: {}, number of vertices: {}, number of edges: {}",
        graph_number, number_of_vertices, number_of_edges
    );
    Ok(())
}
