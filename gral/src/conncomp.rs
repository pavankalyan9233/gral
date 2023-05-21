use crate::graphs::Graph;
use std::time::Instant;

pub fn weakly_connected_components(g: &Graph) -> (u64, Vec<u64>) {
    // Returns the number of weakly connected components and a vector
    // of as many numbers as there are vertices, which contains for each
    // index the id of the weakly connected component of the vertex.
    // The id is the smallest index of a vertex in the same weakly connected
    // component.
    let start = Instant::now();
    let nr_v = g.number_of_vertices();
    let nr_e = g.number_of_edges();
    println!(
        "{:?} Weakly connected components: Have graph with {} vertices and {} edges.",
        start.elapsed(),
        nr_v,
        nr_e
    );
    println!("{:?} Creating mini...", start.elapsed());
    let mut mini: Vec<u64> = vec![];
    mini.reserve(nr_v as usize);
    for i in 0..nr_v {
        mini.push(i);
    }
    println!("{:?} Creating next...", start.elapsed());
    let mut next: Vec<i64> = vec![];
    next.reserve(nr_v as usize);
    for _ in 0..nr_v {
        next.push(-1);
    }

    let mut nr_components = nr_v;

    println!(
        "{:?} Computing weakly connected components...",
        start.elapsed()
    );
    let mut counter: u64 = 0;
    for e in g.edges.iter() {
        if counter % 1000000 == 0 {
            println!(
                "{:?} Have currently {} connected components with {} of {} edges processed.",
                start.elapsed(),
                nr_components,
                counter,
                nr_e
            );
        }
        counter += 1;
        let a = e.from.to_u64();
        let b = e.to.to_u64();
        let mut c = mini[b as usize];
        let mut rep = mini[a as usize];
        if c == rep {
            continue;
        }
        if c < rep {
            (c, rep) = (rep, c);
        }
        // Now c = mini[b] and rep = mini[a] and rep < c
        let first = c;
        loop {
            mini[c as usize] = rep;
            let d = next[c as usize];
            if d == -1 {
                break;
            }
            c = d as u64;
        }
        let second = next[rep as usize]; // can be -1!
        next[rep as usize] = first as i64;
        next[c as usize] = second;
        nr_components -= 1;
    }
    println!(
        "{:?} Finished, found {} weakly connected components.",
        start.elapsed(),
        nr_components
    );
    (nr_components, mini)
}

// We use the terminology as in Knuth:
// https://www-cs-faculty.stanford.edu/~knuth/fasc12a+.pdf

pub fn strongly_connected_components(g: &Graph) -> (u64, Vec<u64>) {
    // Returns the number of strongly connected components and a vector
    // of as many numbers as there are vertices, which contains for each
    // index the id of the strongly connected component of the vertex.
    // The id is the smallest index of a vertex in the same strongly connected
    // component.

    let start = Instant::now();

    let nr_v = g.number_of_vertices();
    let lambda = u64::MAX; // Lambda in Knuth
    let sent = nr_v; // SENT in Knuth

    // Working data, all number of vertices sized:
    println!("{:?} Computing strongly connected components,\nnumber of vertices: {}, number of edges: {}",
             start.elapsed(), nr_v, g.number_of_edges());
    println!("{:?} Allocating data...", start.elapsed());
    let mut parent: Vec<u64> = vec![];
    let mut arc: Vec<u64> = vec![];
    let mut link: Vec<u64> = vec![];
    let mut rep: Vec<u64> = vec![];

    // T1
    // Initialize parent vector:
    parent.resize(nr_v as usize, lambda);
    arc.resize(nr_v as usize, lambda);
    link.resize(nr_v as usize, lambda);
    rep.resize(nr_v as usize + 1, lambda);
    rep[nr_v as usize] = 0; // exception to simplify conditions

    let mut w: u64 = sent;
    let mut p: u64 = 0;
    let mut sink: u64 = sent;
    let mut root: u64;
    let mut count: u64 = 0; // number of connected components
    println!("{:?} Starting depth first search...", start.elapsed());
    while w > 0 {
        w -= 1;
        if parent[w as usize] != lambda {
            continue; // Already done, next one
        }
        // Start exploring from w:
        let mut v = w;
        parent[v as usize] = sent; // root of a spanning tree
        root = v;

        // Prepare exploration from v:
        'T3: loop {
            // This is the outer main loop for the depth first search. We
            // return to this place whenever we start exploring from a new
            // vertex v.
            let mut a = g.edge_index_by_from[v as usize];
            p += 1;
            rep[v as usize] = p;
            link[v as usize] = sent;

            'T4: loop {
                // This is the inner main loop for the depth first
                // search. We return to this place whenever we want to
                // move to a new edge going out of the current vertex.
                // When we get here, the variables v (current vertex)
                // and a (current arc) must be set correctly.

                // First the case of doing another arc from here:
                let u: u64; // the vertex we move to
                if a < g.edge_index_by_from[v as usize + 1] {
                    // T5
                    u = g.edges_by_from[a as usize].to_u64();
                    a += 1;
                    // T6
                    if parent[u as usize] == lambda {
                        // a new vertex, move there
                        parent[u as usize] = v; // u discovered from v
                        arc[v as usize] = a; // for backtracking
                        v = u;
                        continue 'T3;
                    }
                    // Is u our root and we are in the last component?
                    if root == u && p == nr_v {
                        while v != root {
                            link[v as usize] = sink;
                            sink = v;
                            v = parent[v as usize];
                        }
                        // u = sent;   // ineffective, since we break T3
                        // T8
                        while rep[sink as usize] >= rep[v as usize] {
                            rep[sink as usize] = sent + v;
                            sink = link[sink as usize];
                        }
                        rep[v as usize] = sent + v;
                        count += 1;
                        break 'T3;
                    }
                    if rep[u as usize] < rep[v as usize] {
                        rep[v as usize] = rep[u as usize];
                        link[v as usize] = lambda;
                    }
                    continue 'T4;
                }
                // T7, finish with v:
                u = parent[v as usize];
                if link[v as usize] == sent {
                    // T8, new connected component
                    while rep[sink as usize] >= rep[v as usize] {
                        rep[sink as usize] = sent + v;
                        sink = link[sink as usize];
                    }
                    rep[v as usize] = sent + v;
                    count += 1;
                    if count % 100000 == 0 {
                        println!(
                            "{:?} Have found {} connected components",
                            start.elapsed(),
                            count
                        );
                    }
                    // fall through to T9
                } else {
                    if rep[v as usize] < rep[u as usize] {
                        rep[u as usize] = rep[v as usize];
                        link[u as usize] = lambda;
                    }
                    link[v as usize] = sink;
                    sink = v;
                    // fall through to T9
                }
                // T9, tree done?
                if u == sent {
                    break 'T3;
                }
                // Backtrack:
                v = u;
                a = arc[v as usize];
            }
        }
    }
    rep.pop(); // remove unneeded 0
    println!("{:?} Translating result...", start.elapsed());
    // Translate rep array:

    for i in 0..nr_v {
        rep[i as usize] -= sent;
    }
    println!(
        "{:?} Finished. Found {} strongly connected components.",
        start.elapsed(),
        count
    );
    return (count, rep);
}
