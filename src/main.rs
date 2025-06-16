use dijkstralog;
fn main() {
    let xs: &[(u32, &str)] = &[(1, "one"), (1, "wun")];
    assert!(xs.is_sorted());
    let _r = dijkstralog::relationize!(xs, u32, &str);
    // let _q = dijkstralog::iter::Seek::map(dijkstralog::iter::ranges(xs, |x| x.0), |v| v);
    println!("hello, world");
}
