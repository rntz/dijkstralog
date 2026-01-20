#![allow(unused_imports, dead_code, unused_variables)]

extern crate proc_macro;
use derive_syn_parse::Parse;
use proc_macro2::TokenStream;
use quote::{quote, ToTokens};
use syn::{
    parse::{Parse, ParseStream},
    punctuated::Punctuated,
    Result, Ident, Token, Type,
    parse_macro_input, parenthesized, token,
};

mod kw {
    syn::custom_keyword!(relation);
}


// ----- TERMS -----
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

impl ToTokens for Term {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        match self {
            Term::Var(id) => id.to_tokens(tokens),
            Term::Const(lit) => lit.to_tokens(tokens),
        }
    }
}


// ----- ATOMS, QUERIES, RULES -----
#[derive(Parse)]
struct Atom {
    pred: Ident,
    #[paren]
    _paren_token: token::Paren,
    #[inside(_paren_token)]
    #[call(Punctuated::parse_terminated)]
    args: Punctuated<Term, Token![,]>,
}

#[derive(Parse)]
struct Query {
    #[call(Punctuated::parse_separated_nonempty)]
    atoms: Punctuated<Atom, Token![,]>,
}

#[derive(Parse)]
struct Rule {
    head: Atom,
    _then: Token![<-],
    body: Query,
}


// ----- PROGRAMS -----
#[derive(Parse)]
struct Program {
    #[call(Punctuated::parse_terminated)]
    rules: Punctuated<Rule, Token![.]>,
}

#[derive(Parse)]
struct Sig {
    _sig_token: kw::relation,
    name: Ident,
    #[paren]
    _paren_token: token::Paren,
    #[inside(_paren_token)]
    #[call(Punctuated::parse_terminated)]
    columns: Punctuated<Type, Token![,]>,
}

enum Decl {
    Sig(Sig),
    Rule(Rule),
}

impl Parse for Decl {
    fn parse(input: ParseStream) -> Result<Decl> {
        if input.peek(kw::relation) {
            input.parse().map(Decl::Sig)
        } else {
            input.parse().map(Decl::Rule)
        }
    }
}


// ----- GENERATING PRINTING CODE -----
fn intersperse<T, U>(xs: T, sep: U) -> TokenStream
where T: Iterator, T::Item: ToTokens, U: ToTokens
{
    let mut result = TokenStream::new();
    let mut first = true;
    for x in xs {
        if !first { sep.to_tokens(&mut result); }
        first = false;
        x.to_tokens(&mut result);
    }
    result
}

impl Atom {
    fn print(&self) -> TokenStream {
        let id = &self.pred;
        let args = intersperse(
            self.args.iter().map(|x| quote! { print!("{:?}", #x); }),
            quote! { print!(", "); }
        );
        quote! {
            print!("{}(", #id);
            #args
            print!(")");
        }
    }
}

impl Query {
    fn print(&self) -> TokenStream {
        let atoms = self.atoms.iter().map(|atom| atom.print());
        let p = intersperse(atoms, quote! { println!(","); });
        quote! { #p println!(); }
    }
}

impl Rule {
    fn print(&self) -> TokenStream {
        let head = self.head.print();
        let body = &self.body.atoms;
        match body.len() {
            0 => quote! { #head println!("."); },
            1 => {
                let atom = &body[0].print();
                quote! { #head print!(" <- "); #atom println!("."); }
            },
            _ => {
                let atoms = body.iter().map(|x| x.print());
                quote! {
                    #head
                    print!(" <- ");
                    let mut first = true;
                    #(
                        if !first { print!(",") }
                        first = false;
                        print!("\n  ");
                        #atoms
                    )*
                    println!(".");
                }
            }
        }
    }
}


// ----- MACROS -----
#[proc_macro]
pub fn query(tokens: proc_macro::TokenStream) -> proc_macro::TokenStream {
    parse_macro_input!(tokens as Query).print().into()
}

#[proc_macro]
pub fn rule(tokens: proc_macro::TokenStream) -> proc_macro::TokenStream {
    parse_macro_input!(tokens as Rule).print().into()
}
