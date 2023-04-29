use crate::GruploadArgs;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use rand::Rng;
use reqwest::{blocking::Response, StatusCode};
//use serde_json::Value::String;
use serde_json::Value;
use std::fs::File;
use std::io::{BufRead, BufReader, Cursor, Read};
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
fn get_varlen(c: &mut impl Read) -> Result<u32, std::io::Error> {
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

fn put_varlen(v: &mut Vec<u8>, l: u32) {
    if l <= 0x7f {
        v.write_u8(l as u8).unwrap();
    } else {
        v.write_u32::<BigEndian>(l | 0x80000000).unwrap();
    };
}

pub fn handle_error(resp: &mut Response, ok: StatusCode) -> Result<(), String> {
    if resp.status() == ok {
        return Ok(());
    }
    let code = resp.read_u32::<BigEndian>().unwrap();
    let len = match get_varlen(resp) {
        Err(err) => {
            return Err(format!(
                "Cannot read error message, code: {}, error: {:?}",
                code, err
            ))
        }
        Ok(v) => v,
    };
    let mut buf = vec![0u8; len as usize];
    match resp.read_exact(&mut buf) {
        Err(err) => {
            return Err(format!(
                "Could not read error response,  code: {}, error: {:?}",
                code, err
            ))
        }
        _ => (),
    }
    let msg = match str::from_utf8(&buf) {
        Ok(v) => v,
        Err(err) => {
            return Err(format!(
                "Error message is no UTF-8, code: {}, error: {:?}",
                code, err
            ))
        }
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
    let mut resp = match client.post(url).body(v).send() {
        Ok(resp) => resp,
        Err(err) => panic!("Error: {}", err),
    };
    handle_error(&mut resp, status(200))?;

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

pub fn vertices(args: &GruploadArgs) -> Result<(), String> {
    println!("Loading vertices... {:?}", args);
    let client = reqwest::blocking::Client::new();

    let file = File::open(&args.vertex_file);
    if let Err(err) = file {
        return Err(format!(
            "Error reading file {}: {:?}",
            args.vertex_file.to_string_lossy(),
            err
        ));
    }
    let mut rng = rand::thread_rng();
    let file = file.unwrap();
    let iter = BufReader::new(file).lines();
    let mut buf: Vec<u8> = vec![];
    buf.reserve(1000000);
    let mut client_id: u64 = 0;

    let mut write_header = |buf: &mut Vec<u8>, client_id: &mut u64| {
        buf.clear();
        *client_id = rng.gen::<u64>();
        buf.write_u64::<BigEndian>(*client_id).unwrap();
        buf.write_u32::<BigEndian>(args.graph_number).unwrap();
        buf.write_u32::<BigEndian>(0).unwrap();
    };

    let send_off = |buf: &mut Vec<u8>, count: u32| -> Result<(), String> {
        let mut tmp = count;
        for i in 1..=4 {
            buf[16 - i] = (tmp & 0xff) as u8;
            tmp >>= 8;
        }
        let mut url = args.endpoint.clone();
        url.push_str("/v1/vertices");
        let mut resp = match client.post(url).body(buf.clone()).send() {
            Ok(resp) => resp,
            Err(err) => return Err(format!("Could not send off batch: {:?}", err)),
        };
        handle_error(&mut resp, status(200))?;

        let mut body: Vec<u8> = vec![];
        let _size = resp.read_to_end(&mut body).unwrap();
        let mut cursor = Cursor::new(&body);
        let _client_id_back = cursor.read_u64::<BigEndian>().unwrap();
        let nr_rejected = cursor.read_u32::<BigEndian>().unwrap();
        let nr_exceptional = cursor.read_u32::<BigEndian>().unwrap();
        for _i in 0..nr_rejected {
            let index = cursor.read_u32::<BigEndian>().unwrap();
            println!("Index of rejected vertex: {}", index);
        }
        for _i in 0..nr_exceptional {
            let index = cursor.read_u32::<BigEndian>().unwrap();
            let hash = cursor.read_u64::<BigEndian>().unwrap();
            println!("Index of exceptional hash: {}, hash: {:x}", index, hash);
        }
        Ok(())
    };

    write_header(&mut buf, &mut client_id);
    let mut count: u32 = 0;
    let mut overall: u64 = 0;
    for line in iter {
        let l = line.unwrap();
        let v: Value = match serde_json::from_str(&l) {
            Err(err) => return Err(format!("Cannot parse JSON: {:?}", err)),
            Ok(val) => val,
        };
        let id = &v["_id"];
        match id {
            Value::String(i) => {
                put_varlen(&mut buf, i.len() as u32);
                for x in i.bytes() {
                    buf.push(x);
                }
                buf.push(0); // no data for now
            }
            _ => {
                return Err(format!(
                    "JSON is no object with a string _id attribute:\n{}",
                    l
                ));
            }
        }

        count += 1;
        if count >= 65536 || buf.len() > 900000 {
            send_off(&mut buf, count)?;
            write_header(&mut buf, &mut client_id);
            overall += count as u64;
            count = 0;
        }
    }
    if count > 0 {
        send_off(&mut buf, count)?;
        overall += count as u64;
    }

    println!(
        "Vertices uploaded, graph number: {}, number of vertices: {}",
        args.graph_number, overall
    );
    Ok(())
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
    let mut resp = match client.post(url).body(v).send() {
        Ok(resp) => resp,
        Err(err) => panic!("Error: {}", err),
    };
    handle_error(&mut resp, status(200))?;

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
    let mut resp = match client.post(url).body(v).send() {
        Ok(resp) => resp,
        Err(err) => panic!("Error: {}", err),
    };
    handle_error(&mut resp, status(200))?;

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
