use darling::{FromDeriveInput, FromField};
use darling::ast::{Fields, Data};
use quote::{quote, TokenStreamExt};
use proc_macro2::{Span, TokenStream};

enum CompileError {
  Darling(darling::Error),
  Syn(syn::Error)
}

type Result<T> = std::result::Result<T, CompileError>;

impl CompileError {
  fn into_compile_error(self) -> TokenStream {
    match self {
      CompileError::Darling(e) => e.write_errors(),
      CompileError::Syn(e) => e.to_compile_error(),
    }
  }
}

impl From<darling::Error> for CompileError {
  fn from(err: darling::Error) -> Self { CompileError::Darling(err) }
}

impl From<syn::Error> for CompileError {
  fn from(err: syn::Error) -> Self { CompileError::Syn(err) }
}


trait Argname {
  fn get_argname(&self) -> Option<&String>;
  fn get_ident(&self) -> &syn::Ident;
  fn argname_or_default(&self) -> syn::LitStr {
    let ident = self.get_ident();
    if let Some(argname) = self.get_argname() {
      syn::LitStr::new(argname, ident.span())
    } else {
      syn::LitStr::new(&ident.to_string().replace("_", "-"), ident.span())
    }
  }
}

#[derive(Debug, FromField)]
#[darling(attributes(slurm))]
struct InputField {
  pub ident: Option<syn::Ident>,
  pub ty: syn::Type,
  #[darling(default)]
  pub default: Option<String>,
  #[darling(default)]
  pub argname: Option<String>,
}

impl Argname for InputField {
  fn get_argname(&self) -> Option<&String> { self.argname.as_ref() }
  fn get_ident(&self) -> &syn::Ident { self.ident.as_ref().unwrap() }
}

#[derive(Debug, FromField)]
#[darling(attributes(slurm))]
struct ParamsField {
  pub ident: Option<syn::Ident>,
  pub ty: syn::Type,
  #[darling(default)]
  pub argname: Option<String>,
}


impl Argname for ParamsField {
  fn get_argname(&self) -> Option<&String> { self.argname.as_ref() }
  fn get_ident(&self) -> &syn::Ident { self.ident.as_ref().unwrap() }
}


#[derive(Debug, FromDeriveInput)]
#[darling(supports(struct_named))]
struct InputStruct {
  pub ident: syn::Ident,
  pub data: Data<darling::util::Ignored, InputField>,
}


#[derive(Debug, FromDeriveInput)]
#[darling(supports(struct_named))]
struct ParamsStruct {
  pub ident: syn::Ident,
  pub data: Data<darling::util::Ignored, ParamsField>,
}


#[derive(Debug, FromDeriveInput)]
#[darling(supports(struct_named), attributes(slurm))]
struct DeriveInputLookahead {
  #[darling(default)]
  pub inputs: bool,
  #[darling(default)]
  pub parameters: bool,
}



fn get_add_args_input_impl(ident: &syn::Ident, fields: &Fields<InputField>) -> Result<TokenStream> {
  let mut args = Vec::with_capacity(fields.len());
  for f in fields.iter() {
    let ident = f.ident.as_ref().unwrap();
    let argid = syn::LitStr::new(&ident.to_string(), ident.span());

    let mut arg = quote::quote! {
            clap::Arg::with_name(#argid)
        };

    if let Some(default) = &f.default {
      let default = syn::LitStr::new(default, ident.span());
      arg.append_all(quote! { .default_value(#default) });

      let argname = f.argname_or_default();
      arg.append_all(quote! { .long(#argname) });
    } else {
      arg.append_all(quote! { .required(true) })
    }

    args.push(arg);
  }

  let ts = quote! {
        impl slurm_harray::AddArgs for #ident {
            fn add_args<'a, 'b>(app: clap::App<'a, 'b>) -> clap::App<'a, 'b> {
                app#(.arg(#args))*
            }
        }
    };

  Ok(ts)
}


fn get_add_args_param_impl(ident: &syn::Ident, fields: &Fields<ParamsField>) -> Result<TokenStream> {
  let mut args = Vec::with_capacity(fields.len());
  for f in fields.iter() {
    let ident = f.ident.as_ref().unwrap();
    let argid = syn::LitStr::new(&ident.to_string(), ident.span());
    let argname = f.argname_or_default();
    args.push(quote! {
            clap::Arg::with_name(#argid).long(#argname).takes_value(true)
        });
  }

  let ts = quote! {
        impl slurm_harray::AddArgs for #ident {
            fn add_args<'a, 'b>(app: clap::App<'a, 'b>) -> clap::App<'a, 'b> {
                app#(.arg(#args))*
            }
        }
    };

  Ok(ts)
}

fn get_from_args_input_impl(ident: &syn::Ident, fields: &Fields<InputField>) -> Result<TokenStream> {
  let mut field_defs = Vec::with_capacity(fields.len());
  let mut field_names = Vec::with_capacity(fields.len());
  for f in fields.iter() {
    let ident = f.ident.as_ref().unwrap();
    field_names.push(ident);

    let argid = syn::LitStr::new(&ident.to_string(), ident.span());
    let def = quote::quote! {
            let #ident = args.value_of(#argid).unwrap().parse().context(concat!("parsing `", #argid, "`"))?;
        };

    field_defs.push(def)
  }

  let ts = quote! {
        impl slurm_harray::FromArgs for #ident {
            fn from_args(args: &clap::ArgMatches) -> anyhow::Result<Self> {
                use anyhow::Context;
                #(#field_defs)*
                Ok(#ident{
                    #(#field_names),*
                })
            }
        }
    };

  Ok(ts)
}


fn get_from_args_param_impl(ident: &syn::Ident, fields: &Fields<ParamsField>) -> Result<TokenStream> {
  let mut field_argids = Vec::with_capacity(fields.len());
  let mut field_argnames = Vec::with_capacity(fields.len());
  let mut field_names = Vec::with_capacity(fields.len());

  for f in fields.iter() {
    let ident = f.ident.as_ref().unwrap();
    field_names.push(ident);
    field_argids.push(syn::LitStr::new(&ident.to_string(), ident.span()));
    field_argnames.push(f.argname_or_default())
  }

  let ts = quote! {
        impl slurm_harray::FromArgs for #ident {
            fn from_args(args: &clap::ArgMatches) -> anyhow::Result<Self> {
                use anyhow::Context;
                let mut params = Self::default();

                #(
                    if args.occurrences_of(#field_argids) > 0 {
                        params.#field_names = args.value_of(#field_argids).unwrap().parse().context(concat!("parameter `", #field_argnames, "`"))?;
                    }
                )*
                Ok(params)
            }
        }
    };

  Ok(ts)
}

fn unwrap_fields<F>(data: &Data<darling::util::Ignored, F>) -> &Fields<F> {
  match data {
    Data::Struct(fields) => fields,
    _ => unreachable!()
  }
}

fn target_is_parameter_struct(derive_input: &syn::DeriveInput) -> Result<bool> {
  let lookahead : DeriveInputLookahead = DeriveInputLookahead::from_derive_input(&derive_input)?;
  match (lookahead.inputs, lookahead.parameters) {
    (true, false) => Ok(false),
    (false, true) => Ok(true),
    (true, true) => Err(syn::Error::new_spanned(&derive_input.attrs.first().unwrap(),
                                    "Only one of `inputs` or `parameters` cant be specified.").into()),
    (false, false) => Err(syn::Error::new(Span::call_site(),
                                                  "Must specify struct-level `#[slurm(inputs)]` or `#[slurm(parameters)]`").into())
  }
}

fn get_from_args_impl(derive_input: syn::DeriveInput) -> Result<TokenStream> {
  if target_is_parameter_struct(&derive_input)? {
    let target = ParamsStruct::from_derive_input(&derive_input)?;
    let fields = unwrap_fields(&target.data);
    get_from_args_param_impl(&target.ident, fields)
  } else {
    let target = InputStruct::from_derive_input(&derive_input)?;
    let fields = unwrap_fields(&target.data);
    get_from_args_input_impl(&target.ident, fields)
  }
}

fn get_add_args_impl(derive_input: syn::DeriveInput) -> Result<TokenStream> {
  if target_is_parameter_struct(&derive_input)? {
    let target = ParamsStruct::from_derive_input(&derive_input)?;
    let fields = unwrap_fields(&target.data);
    get_add_args_param_impl(&target.ident, fields)
  } else {
    let target = InputStruct::from_derive_input(&derive_input)?;
    let fields = unwrap_fields(&target.data);
    get_add_args_input_impl(&target.ident, fields)
  }
}

#[proc_macro_derive(FromArgs, attributes(slurm))]
pub fn derive_from_args(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
  let derive_input = syn::parse_macro_input!(input as syn::DeriveInput);
  match get_from_args_impl(derive_input) {
    Ok(ts) => ts.into(),
    Err(e) => e.into_compile_error().into(),
  }
}

#[proc_macro_derive(AddArgs, attributes(slurm))]
pub fn derive_add_args(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
  let derive_input = syn::parse_macro_input!(input as syn::DeriveInput);
  match get_add_args_impl(derive_input) {
    Ok(ts) => ts.into(),
    Err(e) => e.into_compile_error().into(),
  }
}
