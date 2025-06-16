#![allow(unused_imports, dead_code, unused_variables)]

extern crate proc_macro;
use derive_syn_parse::Parse;
use proc_macro2::TokenStream;
use quote::quote;
use syn::{
    parse::{Parse, ParseStream},
    punctuated::Punctuated,
    Result, Ident, Token, Type,
    parse_macro_input, parenthesized, token,
};


// Terms
enum Term {
    Var(Ident),
    Const(syn::ExprLit),
}

impl Parse for Term {
    fn parse(input: ParseStream) -> Result<Term> {
        Ok(if input.peek(syn::Ident) {
            Term::Var(input.parse()?)
        } else {
            Term::Const(input.parse()?)
        })
    }
}

impl quote::ToTokens for Term {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        match self {
            Term::Var(id) => id.to_tokens(tokens),
            Term::Const(lit) => lit.to_tokens(tokens),
        }
    }
}


// Atoms
#[derive(Parse)]
struct Atom {
    pred: Ident,
    #[paren]
    _paren_token: token::Paren,
    #[inside(_paren_token)]
    #[call(Punctuated::parse_terminated)]
    args: Punctuated<Term, Token![,]>,
}

#[proc_macro]
pub fn example(tokens: proc_macro::TokenStream) -> proc_macro::TokenStream {
    println!("Hello, world! from dijkstralog-macros/src/lib.rs");
    let atom: Atom = parse_macro_input!(tokens as Atom);
    let id = atom.pred;
    let args = atom.args.iter();
    quote! {
        println!("hello, {}!", #id);
        #(println!("and you, {}", #args);)*
    }.into()
}
