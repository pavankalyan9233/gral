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
