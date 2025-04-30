#![allow(dead_code, unused_variables)]

mod utils;
mod lftj;

fn main() {
    println!("Hello, world!");
    let xs: Vec<usize> = vec![1, 2, 3];
    let it = lftj::VectorSeek::new(xs.as_slice());
    
}
