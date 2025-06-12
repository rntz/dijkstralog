use dijkstralog::search::{
    //gallop_search as search
    recursive_gallop as search,
};

fn main() {
    let xs: &[u32] = &[];
    let x = search(xs, |x| *x <= 3);
    assert!(x == 0);

    let mut failures: Vec<(u32, u32)> = Vec::new();

    for n in 0..=5 {
        let vec: Vec<u32> = (0..n).collect();
        let xs: &[u32] = vec.as_slice();
        for i in 0..=n {
            let expect = xs.partition_point(|x| *x <= i);
            let actual = search(xs, |x| *x <= i);
            if actual != expect {
                println!("Error! While searching {xs:?} for {i}, got index {actual}; expected {expect}.");
                failures.push((i, n));
            } else {
                println!("Success on ({i}, {n})");
            }
        }
    }

    if !failures.is_empty() {
        println!("FAILURES: {failures:?}");
        panic!("test case(s) failed");
    }

    // let xs: &[u32] = &[1, 2, 3, 4, 5];
    // for i in 0..6 {
    //     let x = search(xs, |x| *x <= i);
    //     let y = xs.partition_point(|x| *x <= i);
    //     if x != y {
    //         println!("***** ERROR! got {x}, expect {y}");
    //         panic!("***** ERROR! got {x}, expect {y}");
    //     }
    // }
}
