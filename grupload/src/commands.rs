use crate::GruploadArgs;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use rand::Rng;
use reqwest::{blocking::Response, StatusCode};
//use serde_json::Value::String;
use serde_json::Value;
use sha256::digest;
use std::fs::{metadata, File};
use std::io::prelude::*;
use std::io::{BufRead, BufReader, BufWriter, Cursor, Read, SeekFrom, Write};
use std::str;
use std::thread::{spawn, JoinHandle};
use std::time::Duration;

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

fn read_bytes_or_fail(reader: &mut Cursor<Vec<u8>>, l: u32) -> Result<&[u8], String> {
    let v = reader.get_ref();
    if (v.len() as u64) - reader.position() < l as u64 {
        return Err("input too short".to_string());
    }
    Ok(&v[(reader.position() as usize)..((reader.position() + l as u64) as usize)])
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

pub fn create(args: &mut GruploadArgs) -> Result<(), String> {
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
    args.graph_number = graph_number; // Return number of graph
    return Ok(());
}

fn vertices_one_thread(
    file_name: String,
    graph_number: u32,
    endpoint: String,
    start: u64,
    finish: u64,
) -> Result<(), String> {
    // Some preparations:
    let client = reqwest::blocking::Client::new();
    let mut rng = rand::thread_rng();
    let mut buf: Vec<u8> = vec![];
    buf.reserve(1000000);
    let mut client_id: u64 = 0;

    // Two closures to do some work:

    let mut write_header = |buf: &mut Vec<u8>, client_id: &mut u64| {
        buf.clear();
        *client_id = rng.gen::<u64>();
        buf.write_u64::<BigEndian>(*client_id).unwrap();
        buf.write_u32::<BigEndian>(graph_number).unwrap();
        buf.write_u32::<BigEndian>(0).unwrap();
    };

    let send_off = |buf: &mut Vec<u8>, count: u32| -> Result<(), String> {
        let mut tmp = count;
        for i in 1..=4 {
            buf[16 - i] = (tmp & 0xff) as u8;
            tmp >>= 8;
        }
        let mut url = endpoint.clone();
        url.push_str("/v1/vertices");
        let mut resp = match client.post(url).body(buf.clone()).send() {
            Ok(resp) => resp,
            Err(err) => return Err(format!("Could not send off batch: {:?}", err)),
        };
        handle_error(&mut resp, status(200))?;

        let mut body: Vec<u8> = vec![];
        let _size = resp.read_to_end(&mut body).unwrap();
        let mut cursor = Cursor::new(body);
        // TODO: error handling if input is too short!
        let _client_id_back = cursor.read_u64::<BigEndian>().unwrap();
        let nr_exceptional = cursor.read_u32::<BigEndian>().unwrap();
        for _i in 0..nr_exceptional {
            let index = cursor.read_u32::<BigEndian>().unwrap();
            let hash = cursor.read_u64::<BigEndian>().unwrap();
            let l = get_varlen(&mut cursor).unwrap();
            let k = read_bytes_or_fail(&mut cursor, l).unwrap();
            let kk = str::from_utf8(k).unwrap();
            println!(
                "Key of exceptional hash: {}, index: {}, hash: {:x}",
                kk, index, hash
            );
        }
        Ok(())
    };

    let file = File::open(&file_name);
    if let Err(err) = file {
        return Err(format!("Error reading file {}: {:?}", file_name, err));
    }
    let file = file.unwrap();
    let mut reader = BufReader::new(file);
    let mut file_pos: u64 = 0;
    if start > 0 {
        let seek_res = reader.seek(SeekFrom::Start(start));
        if let Err(err) = seek_res {
            return Err(format!(
                "Error seeking to start position {} in file {}: {:?}",
                start, file_name, err
            ));
        }
        file_pos = seek_res.unwrap();
        // This thread is responsible for all lines after the first line
        // which has its line end character at or behind position `start`.
        // Therefore, we skip bytes until we see a line end. Note that this
        // has the additional benefit of skipping over incomplete UTF-8
        // code points!
        loop {
            let mut byte_buf: Vec<u8> = vec![0];
            let b = reader.read_exact(&mut byte_buf);
            if let Err(err) = b {
                return Err(format!(
                    "Error reading single bytes at start position {} in file {}: {:?}",
                    start, file_name, err
                ));
            }
            file_pos += 1;
            if byte_buf[0] == '\n' as u8 {
                break;
            }
        }
    }

    write_header(&mut buf, &mut client_id);
    let mut count: u32 = 0;
    let mut overall: u64 = 0;
    while file_pos <= finish {
        // Note that we are supposed to read up to (and including) the
        // first line whose line end character is at or behind position
        // `finish` in the file.
        let mut line = String::with_capacity(256);
        let r = reader.read_line(&mut line);
        match r {
            Err(err) => {
                return Err(format!(
                    "Error reading lines from file {}: {:?}",
                    file_name, err
                ));
            }
            Ok(size) => {
                if size == 0 {
                    // EOF
                    break;
                }
                file_pos += size as u64;
            }
        }
        let l = line.trim_end();
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
        "Vertices uploaded range from {} to {}, graph number: {}, number of vertices: {}",
        start, finish, graph_number, overall
    );
    Ok(())
}

pub fn vertices(args: &GruploadArgs) -> Result<(), String> {
    println!("Loading vertices... {:?}", args);

    let meta = metadata(&args.vertex_file);
    if let Err(err) = meta {
        return Err(format!("Could not find vertex file: {:?}", err));
    }
    let total_size = meta.unwrap().len();
    let chunk_size = total_size / (args.nr_threads as u64);
    if chunk_size < 4096 {
        // take care of very small files:
        return vertices_one_thread(
            args.vertex_file.to_str().unwrap().to_owned(),
            args.graph_number,
            args.endpoint.clone(),
            0,
            total_size,
        );
    }
    let mut join: Vec<JoinHandle<Result<(), String>>> = vec![];
    let mut s: u64 = 0;
    for i in 0..(args.nr_threads) {
        let start: u64 = s;
        s += chunk_size;
        let mut finish: u64 = s;
        if i == args.nr_threads - 1 {
            finish = total_size;
        }
        let file_name = args.vertex_file.to_str().unwrap().to_owned();
        let graph_number = args.graph_number;
        let endpoint = args.endpoint.clone();
        join.push(spawn(move || -> Result<(), String> {
            vertices_one_thread(file_name, graph_number, endpoint, start, finish)
        }));
    }
    let mut errors = String::new();
    for jh in join.into_iter() {
        let r = jh.join().unwrap();
        if let Err(msg) = r {
            errors.push_str(&msg);
            errors.push('.');
        }
    }

    if !errors.is_empty() {
        Err(errors)
    } else {
        Ok(())
    }
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

pub fn edges_one_thread(
    file_name: String,
    graph_number: u32,
    endpoint: String,
    start: u64,
    finish: u64,
) -> Result<(), String> {
    let client = reqwest::blocking::Client::new();
    let mut rng = rand::thread_rng();
    let mut buf: Vec<u8> = vec![];
    buf.reserve(1000000);
    let mut client_id: u64 = 0;

    // Two closures to do some work:

    let mut write_header = |buf: &mut Vec<u8>, client_id: &mut u64| {
        buf.clear();
        *client_id = rng.gen::<u64>();
        buf.write_u64::<BigEndian>(*client_id).unwrap();
        buf.write_u32::<BigEndian>(graph_number).unwrap();
        buf.write_u32::<BigEndian>(0).unwrap();
    };

    let send_off = |buf: &mut Vec<u8>, count: u32| -> Result<(), String> {
        let mut tmp = count;
        for i in 1..=4 {
            buf[16 - i] = (tmp & 0xff) as u8;
            tmp >>= 8;
        }
        let mut url = endpoint.clone();
        url.push_str("/v1/edges");
        let mut resp = match client.post(url).body(buf.clone()).send() {
            Ok(resp) => resp,
            Err(err) => return Err(format!("Could not send off batch: {:?}", err)),
        };
        handle_error(&mut resp, status(200))?;

        let mut body: Vec<u8> = vec![];
        let _size = resp.read_to_end(&mut body).unwrap();
        let mut cursor = Cursor::new(body);
        // TODO: error handling if input is too short!
        let _client_id_back = cursor.read_u64::<BigEndian>().unwrap();
        let nr_rejected = cursor.read_u32::<BigEndian>().unwrap();
        for _i in 0..nr_rejected {
            let index = cursor.read_u32::<BigEndian>().unwrap();
            let code = cursor.read_u32::<BigEndian>().unwrap();
            let l = get_varlen(&mut cursor).unwrap();
            let k = read_bytes_or_fail(&mut cursor, l).unwrap();
            let kk = str::from_utf8(k).unwrap();
            println!(
                "Index of rejected vertex: {}, code: {}, data: {:?}",
                index, code, kk
            );
        }
        Ok(())
    };

    let file = File::open(&file_name);
    if let Err(err) = file {
        return Err(format!("Error reading file {}: {:?}", file_name, err));
    }
    let file = file.unwrap();
    let mut reader = BufReader::new(file);
    let mut file_pos: u64 = 0;
    if start > 0 {
        let seek_res = reader.seek(SeekFrom::Start(start));
        if let Err(err) = seek_res {
            return Err(format!(
                "Error seeking to start position {} in file {}: {:?}",
                start, file_name, err
            ));
        }
        file_pos = seek_res.unwrap();
        // This thread is responsible for all lines after the first line
        // which has its line end character at or behind position `start`.
        // Therefore, we skip bytes until we see a line end. Note that this
        // has the additional benefit of skipping over incomplete UTF-8
        // code points!
        loop {
            let mut byte_buf: Vec<u8> = vec![0];
            let b = reader.read_exact(&mut byte_buf);
            if let Err(err) = b {
                return Err(format!(
                    "Error reading single bytes at start position {} in file {}: {:?}",
                    start, file_name, err
                ));
            }
            file_pos += 1;
            if byte_buf[0] == '\n' as u8 {
                break;
            }
        }
    }

    write_header(&mut buf, &mut client_id);
    let mut count: u32 = 0;
    let mut overall: u64 = 0;
    while file_pos <= finish {
        // Note that we are supposed to read up to (and including) the
        // first line whose line end character is at or behind position
        // `finish` in the file.
        let mut line = String::with_capacity(256);
        let r = reader.read_line(&mut line);
        match r {
            Err(err) => {
                return Err(format!(
                    "Error reading lines from file {}: {:?}",
                    file_name, err
                ));
            }
            Ok(size) => {
                if size == 0 {
                    // EOF
                    break;
                }
                file_pos += size as u64;
            }
        }
        let l = line.trim_end();

        let v: Value = match serde_json::from_str(&l) {
            Err(err) => return Err(format!("Cannot parse JSON: {:?}", err)),
            Ok(val) => val,
        };
        let from = &v["_from"];
        match from {
            Value::String(f) => {
                let to = &v["_to"];
                match to {
                    Value::String(t) => {
                        put_varlen(&mut buf, f.len() as u32);
                        for x in f.bytes() {
                            buf.push(x);
                        }
                        put_varlen(&mut buf, t.len() as u32);
                        for x in t.bytes() {
                            buf.push(x);
                        }
                        buf.push(0); // no data for now
                    }
                    _ => {
                        return Err(format!("JSON has no string as _to attribute:\n{}", l));
                    }
                }
            }
            _ => {
                return Err(format!(
                    "JSON is no object with a string _from attribute:\n{}",
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
        "Edges uploaded range from {} to {}, graph number: {}, number of edges: {}",
        start, finish, graph_number, overall
    );
    Ok(())
}

pub fn edges(args: &GruploadArgs) -> Result<(), String> {
    println!("Loading edges... {:?}", args);

    let meta = metadata(&args.edge_file);
    if let Err(err) = meta {
        return Err(format!("Could not find edge file: {:?}", err));
    }
    let total_size = meta.unwrap().len();
    let chunk_size = total_size / (args.nr_threads as u64);
    if chunk_size < 4096 {
        // take care of very small files:
        return edges_one_thread(
            args.edge_file.to_str().unwrap().to_owned(),
            args.graph_number,
            args.endpoint.clone(),
            0,
            total_size,
        );
    }
    let mut join: Vec<JoinHandle<Result<(), String>>> = vec![];
    let mut s: u64 = 0;
    for i in 0..(args.nr_threads) {
        let start: u64 = s;
        s += chunk_size;
        let mut finish: u64 = s;
        if i == args.nr_threads - 1 {
            finish = total_size;
        }
        let file_name = args.edge_file.to_str().unwrap().to_owned();
        let graph_number = args.graph_number;
        let endpoint = args.endpoint.clone();
        join.push(spawn(move || -> Result<(), String> {
            edges_one_thread(file_name, graph_number, endpoint, start, finish)
        }));
    }
    let mut errors = String::new();
    for jh in join.into_iter() {
        let r = jh.join().unwrap();
        if let Err(msg) = r {
            errors.push_str(&msg);
            errors.push('.');
        }
    }
    if !errors.is_empty() {
        Err(errors)
    } else {
        Ok(())
    }
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
    let mut resp = match client
        .post(url)
        .body(v)
        .timeout(Duration::new(3600, 0))
        .send()
    {
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

pub fn drop_graph(args: &GruploadArgs) -> Result<(), String> {
    println!("Dropping graph {}... {:?}", args.graph_number, args);
    let client = reqwest::blocking::Client::new();
    let mut v: Vec<u8> = vec![];
    let mut rng = rand::thread_rng();
    let client_id = rng.gen::<u64>();
    v.write_u64::<BigEndian>(client_id).unwrap();
    v.write_u32::<BigEndian>(args.graph_number).unwrap();

    let mut url = args.endpoint.clone();
    url.push_str("/v1/dropGraph");
    let mut resp = match client.put(url).body(v).send() {
        Ok(resp) => resp,
        Err(err) => panic!("Error: {}", err),
    };
    handle_error(&mut resp, status(200))?;

    let body = resp.bytes().unwrap();
    let mut cursor = Cursor::new(&body);
    let _client_id_back = cursor.read_u64::<BigEndian>().unwrap();
    let graph_number = cursor.read_u32::<BigEndian>().unwrap();

    println!("Graph number: {} dropped.", graph_number,);
    Ok(())
}

pub fn randomize(args: &GruploadArgs) -> Result<(), String> {
    let mut rng = rand::thread_rng();
    //let client_id = rng.gen::<u64>();

    let cap = args.vertex_coll_name.len() + 1 + args.key_size as usize;

    let make_id = |key: &str| -> String {
        let mut id = String::with_capacity(cap);
        id.push_str(&args.vertex_coll_name);
        id.push('/');
        id.push_str(key);
        id
    };

    // First create the vertices file:
    let file = File::create(&args.vertex_file).expect("Cannot create vertex file.");
    let mut out = BufWriter::with_capacity(8 * 1024, file);
    for i in 0..args.max_vertices {
        let dig = digest(i.to_string());
        let key = &dig[0..args.key_size as usize];
        let id = make_id(key);
        let r = write!(out, "{{\"_key\":\"{}\",\"_id\":\"{}\"}}\n", key, id);
        if let Err(rr) = r {
            return Err(format!("Error during vertex write: {:?}", rr));
        }
    }
    let e = out.flush();
    if let Err(ee) = e {
        return Err(format!("Error during flush: {:?}", ee));
    };
    drop(out);

    // And now create the edges file:
    let file = File::create(&args.edge_file).expect("Cannot create edge file.");
    let mut out = BufWriter::with_capacity(1024 * 1024, file);
    for _i in 0..args.max_edges {
        let f = rng.gen::<u64>() % args.max_vertices;
        let digf = digest(f.to_string());
        let keyf = make_id(&digf[0..args.key_size as usize]);

        let t = rng.gen::<u64>() % args.max_vertices;
        let digt = digest(t.to_string());
        let keyt = make_id(&digt[0..args.key_size as usize]);
        let r = write!(out, "{{\"_from\":\"{}\",\"_to\":\"{}\"}}\n", keyf, keyt);
        if let Err(rr) = r {
            return Err(format!("Error during edge write: {:?}", rr));
        }
    }
    let e = out.flush();
    if let Err(ee) = e {
        Err(format!("Error during flush: {:?}", ee))
    } else {
        Ok(())
    }
}

pub fn compute(args: &GruploadArgs) -> Result<(), String> {
    println!(
        "Triggering computation {} for graph {}... {:?}",
        args.algorithm, args.graph_number, args
    );
    let client = reqwest::blocking::Client::new();
    let mut v: Vec<u8> = vec![];
    let mut rng = rand::thread_rng();
    let client_id = rng.gen::<u64>();
    v.write_u64::<BigEndian>(client_id).unwrap();
    v.write_u32::<BigEndian>(args.graph_number).unwrap();

    let mut url = args.endpoint.clone();
    match args.algorithm.as_str() {
        "wcc" => {
            url.push_str("/v1/weaklyConnectedComponents");
        }
        _ => {
            return Err(format!("Unknown algorithm {} triggered.", args.algorithm));
        }
    }
    let mut resp = match client.put(url).body(v).send() {
        Ok(resp) => resp,
        Err(err) => panic!("Error: {}", err),
    };
    handle_error(&mut resp, status(200))?;

    let body = resp.bytes().unwrap();
    let mut cursor = Cursor::new(&body);
    let _client_id_back = cursor.read_u64::<BigEndian>().unwrap();
    let graph_number = cursor.read_u32::<BigEndian>().unwrap();
    let comp_id = cursor.read_u64::<BigEndian>().unwrap();

    println!(
        "{}: graph number: {}, computation number: {}.",
        args.algorithm, graph_number, comp_id
    );
    Ok(())
}
