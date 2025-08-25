#![allow(unused_imports, unused_variables)]

use dijkstralog::{
    relationize,
};
use dijkstralog::macros::{
    query,
    rule,
};

fn main() {
    let xs: &[(u32, &str)] = &[(1, "one"), (1, "wun")];
    assert!(xs.is_sorted());
    let _r = relationize!(xs, u32, &str);

    let foo = "foo";
    let bar = "bar";
    let hey = "hey";
    rule! {
        hey() <-
          foo("foo", 17),
          bar("bar", xs)
    };
}
