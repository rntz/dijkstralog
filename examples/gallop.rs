use dijkstralog::search::{
    //gallop_search as search
    careful_gallop as search,
    // gallop2 as search2,
};

fn main() {
    let xs: &[u32] = &[];
    let x = search(xs, |x| *x <= 3);
    assert!(x == 0);

    let mut failures: Vec<(u32, u32)> = Vec::new();

    for n in 1..=7 {
        let vec: Vec<u32> = (1..n).collect();
        let xs: &[u32] = vec.as_slice();
        for i in 0..=n {
            let expect = xs.partition_point(|x| *x <= i);
            let actual = search(xs, |x| *x <= i);
            if actual != expect {
                println!("Error! While searching {xs:?} for {i}, got index {actual}; expected {expect}.");
                failures.push((n, i));
            } else {
                println!("Success on ({n}, {i}): got {expect} looking for x > {i} in  {vec:?}");
            }

            // for j in i..=n {
            //     let (actual1, actual2) = search2(xs, |x| *x <= i, |x| *x <= j);
            //     let (expect1, expect2) = (expect, xs.partition_point(|x| *x <= j));
            //     if (actual1, actual2) != (expect1, expect2) {
            //         println!("ERROR!");
            //         failures.push((n, i));
            //     } else {
            //         println!("    and on ({n}, {i}, {j})");
            //     }
            // }

        }
    }

    if !failures.is_empty() {
        println!("FAILURES: {failures:?}");
        panic!("test case(s) failed");
    }
}
