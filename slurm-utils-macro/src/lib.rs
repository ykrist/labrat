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


#[derive(Debug, FromField)]
#[darling(attributes(slurm))]
struct InputField {
  pub ident: Option<syn::Ident>,
  pub ty: syn::Type,
  #[darling(default)]
  pub default: Option<String>,
  #[darling(default)]
  pub argname: Option<String>,
  #[darling(default)]
  pub help: Option<String>,
  #[darling(default)]
  pub valname: Option<String>,
  #[darling(default)]
  pub choices: bool,
}


trait FieldShared {
  fn ident(&self) -> &syn::Ident;

  fn ty(&self) -> &syn::Type;

  fn default(&self) -> Option<&str>;

  fn help(&self) -> Option<&str>;

  fn valname(&self) -> Option<&str>;

  fn argname(&self) -> Option<&str>;

  fn argname_or_default(&self) -> syn::LitStr {
    let ident = self.ident();
    if let Some(argname) = self.argname() {
      syn::LitStr::new(argname, ident.span())
    } else {
      syn::LitStr::new(&ident.to_string().replace("_", "-"), ident.span())
    }
  }

  fn choices(&self) -> bool;
}


impl FieldShared for InputField {
  fn ident(&self) -> &syn::Ident { self.ident.as_ref().unwrap() }
  fn ty(&self) -> &syn::Type { &self.ty }
  fn help(&self) -> Option<&str> { self.help.as_ref().map(std::ops::Deref::deref) }
  fn default(&self) -> Option<&str> { self.default.as_ref().map(std::ops::Deref::deref) }
  fn valname(&self) -> Option<&str> { self.valname.as_ref().map(std::ops::Deref::deref) }
  fn argname(&self) -> Option<&str> { self.argname.as_ref().map(std::ops::Deref::deref) }
  fn choices(&self) -> bool { self.choices }
}


#[derive(Debug, FromField)]
#[darling(attributes(slurm))]
struct ParamsField {
  pub ident: Option<syn::Ident>,
  pub ty: syn::Type,
  #[darling(default)]
  pub default: Option<String>,
  #[darling(default)]
  pub argname: Option<String>,
  #[darling(default)]
  pub help: Option<String>,
  #[darling(default)]
  pub valname: Option<String>,
  #[darling(default)]
  pub choices: bool,
}

impl FieldShared for ParamsField {
  fn ident(&self) -> &syn::Ident { self.ident.as_ref().unwrap() }
  fn ty(&self) -> &syn::Type { &self.ty }
  fn help(&self) -> Option<&str> { self.help.as_ref().map(std::ops::Deref::deref) }
  fn default(&self) -> Option<&str> { self.default.as_ref().map(std::ops::Deref::deref) }
  fn valname(&self) -> Option<&str> { self.valname.as_ref().map(std::ops::Deref::deref) }
  fn argname(&self) -> Option<&str> { self.argname.as_ref().map(std::ops::Deref::deref) }
  fn choices(&self) -> bool { self.choices }
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

fn ty_match_ident(ty: &syn::Type, names: &[&str]) -> bool {
  if let syn::Type::Path(p) = ty {
    if let Some(ident) = p.path.get_ident() {
      for ty in names {
        if ident == ty {
          return true;
        }
      }
    }
  }
  false
}

fn ty_is_primitive_int(ty: &syn::Type) -> bool {
  ty_match_ident(ty, &["i8", "i16", "i32", "i64", "i128", "u8", "u16", "u32", "u64", "u128", "usize", "isize"])
}

fn ty_is_primitive_float(ty: &syn::Type) -> bool {
  ty_match_ident(ty, &["f64", "f32"])
}

fn ty_is_primitive_bool(ty: &syn::Type) -> bool {
  ty_match_ident(ty, &["bool"])
}

fn get_switch_argname(f: &impl FieldShared) -> Result<syn::LitStr> {
  let ident = f.ident();
  if let Some(argname) = f.argname() {
    Ok(syn::LitStr::new(argname, ident.span()))
  } else {
    let default_val = f.default()
      .map(|s| s.parse())
      .unwrap_or(Ok(false))
      .map_err(|_| {
        let msg ="default must be `true` or `false` for bool switches";
        CompileError::Syn(syn::Error::new_spanned(f.ident(), msg))
      })?;
    let mut name = String::new();
    if default_val {
      name.push_str("no-");
    }
    name.push_str(&ident.to_string().replace("_", "-"));
    Ok(syn::LitStr::new(&name, ident.span()))
  }
}


fn add_optional_args<F: FieldShared>(arg: &mut TokenStream, f: &F, rename_valname: bool) {
  if let Some(help) = f.help() {
    arg.append_all(quote! { .help(#help)  });
  }

  if let Some(name) = f.valname() {
    arg.append_all(quote! { .value_name(#name)  });
  } else if rename_valname {
    if ty_is_primitive_int(f.ty()) {
      arg.append_all(quote! { .value_name("N")  });
    } else if ty_is_primitive_float(f.ty()) {
      arg.append_all(quote! { .value_name("X")  });
    }
  }

  if f.choices() {
    let t = f.ty();
    arg.append_all( quote! { .possible_values( #t::arg_choices() )} )
  }

}


fn get_add_args_input_impl(ident: &syn::Ident, fields: &Fields<InputField>) -> Result<TokenStream> {
  let mut args = Vec::with_capacity(fields.len());
  for f in fields.iter() {


    let ident = f.ident();
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
      arg.append_all(quote! { .required(true) });
    }

    add_optional_args(&mut arg, f, false);
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

    let mut arg = if ty_is_primitive_bool(f.ty()) {
      let argname = get_switch_argname(f)?;
      quote!{  clap::Arg::with_name(#argid).long(#argname).takes_value(false) }
    } else {
      let argname = f.argname_or_default();
      let mut arg = quote!{  clap::Arg::with_name(#argid).long(#argname).takes_value(true) };

      if let Some(default) = &f.default {
        let default = syn::LitStr::new(default, ident.span());
        arg.append_all(quote! { .default_value(#default) });
      }
      arg
    };

    add_optional_args(&mut arg, f, true);
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

fn get_from_args_input_impl(ident: &syn::Ident, fields: &Fields<InputField>) -> Result<TokenStream> {
  let mut field_defs = Vec::with_capacity(fields.len());
  let mut field_names = Vec::with_capacity(fields.len());
  for f in fields.iter() {
    let ident = f.ident();
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
  // TODO allow user to use Option<T> fields where T: FromStr, and default to None.
  let mut parse_field = Vec::with_capacity(fields.len());

  for f in fields.iter() {
    let ident = f.ident();
    let argid = syn::LitStr::new(&ident.to_string(), ident.span());

    let ts = if ty_is_primitive_bool(f.ty()) {
      quote! {
        if args.is_present(#argid) {
          params.#ident ^= true;
        }
      }
    } else {
      let argname = f.argname_or_default();
      quote! {
        if args.occurrences_of(#argid) > 0 {
          params.#ident = args.value_of(#argid).unwrap().parse().context(concat!("parameter `", #argname, "`"))?;
        }
      }
    };
    parse_field.push(ts);
  }

  let ts = quote! {
        impl slurm_harray::FromArgs for #ident {
            fn from_args(args: &clap::ArgMatches) -> anyhow::Result<Self> {
                use anyhow::Context;
                let mut params = Self::default();
                #(#parse_field)*
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


#[derive(Debug, FromDeriveInput)]
#[darling(supports(struct_named), attributes(slurm))]
struct DeriveInputLookahead {
  #[darling(default)]
  pub inputs: bool,
  #[darling(default)]
  pub parameters: bool,
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
