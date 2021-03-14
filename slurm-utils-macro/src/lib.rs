use darling::{FromDeriveInput, FromMeta, FromField};
use darling::ast::{Fields, Data};
use syn::Result;
use quote::{quote, TokenStreamExt};
use proc_macro2::{Span, TokenStream};

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

#[derive(Debug, FromDeriveInput)]
#[darling(supports(struct_named))]
struct InputStruct {
    pub ident: syn::Ident,
    pub data: Data<darling::util::Ignored, InputField>,
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
struct ParamsStruct {
    pub ident: syn::Ident,
    pub data: Data<darling::util::Ignored, ParamsField>,
}

fn get_add_arg_input_impl(ident: &syn::Ident, fields: &Fields<InputField>) -> Result<TokenStream> {
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
        impl AddArgs for #ident {
            fn add_args<'a, 'b>(app: clap::App<'a, 'b>) -> clap::App<'a, 'b> {
                app#(.arg(#args))*
            }
        }
    };

    Ok(ts)
}


fn get_add_arg_param_impl(ident: &syn::Ident, fields: &Fields<ParamsField>) -> Result<TokenStream> {
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
        impl AddArgs for #ident {
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
        let mut def = quote::quote! {
            let #ident = args.value_of(#argid).unwrap().parse().context(concat!("parsing `", #argid, "`"))?;
        };

        field_defs.push(def)
    }

    let ts = quote! {
        impl FromArgs for #ident {
            fn from_args(args: &clap::ArgMatches) -> anyhow::Result<Self> {
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
        impl FromArgs for #ident {
            fn from_args(args: &clap::ArgMatches) -> anyhow::Result<Self> {
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


fn get_exp_inputs_impl(ident: &syn::Ident, fields: &Fields<InputField>) -> Result<TokenStream> {
    let add_arg_impl = get_add_arg_input_impl(ident, fields)?;
    let from_arg_impl = get_from_args_input_impl(ident, fields)?;
    let ts = quote! {
        #add_arg_impl

        #from_arg_impl

        impl ExpInputs for #ident {}
    };
    Ok(ts)
}


fn get_exp_params_impl(ident: &syn::Ident, fields: &Fields<ParamsField>) -> Result<TokenStream> {
    let add_arg_impl = get_add_arg_param_impl(ident, fields)?;
    let from_arg_impl = get_from_args_param_impl(ident, fields)?;
    let ts = quote! {
        #add_arg_impl

        #from_arg_impl

        impl ExpParameters for #ident {}
    };
    Ok(ts)
}


#[proc_macro_derive(ExpInputs, attributes(slurm))]
pub fn derive_exp_inputs(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = syn::parse_macro_input!(input as syn::DeriveInput);
    let input = InputStruct::from_derive_input(&input).unwrap();
    let fields = match &input.data {
        Data::Struct(fields) => fields,
        _ => unreachable!()
    };

    match get_exp_inputs_impl(&input.ident, fields) {
        Ok(output) => output.into(),
        Err(e) => e.to_compile_error().into(),
    }

}

#[proc_macro_derive(ExpParameters, attributes(slurm))]
pub fn derive_exp_parameters(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = syn::parse_macro_input!(input as syn::DeriveInput);
    let input = ParamsStruct::from_derive_input(&input).unwrap();
    let fields = match &input.data {
        Data::Struct(fields) => fields,
        _ => unreachable!()
    };

    match get_exp_params_impl(&input.ident, fields) {
        Ok(output) => output.into(),
        Err(e) => e.to_compile_error().into(),
    }

}
