use clap;
use anyhow::{Context, Result};

pub struct Experiment<I, P, O> {
    pub inputs: I,
    pub parameters: P,
    pub outputs: O,
}

impl<I, P, O> FromArgs for Experiment<I, P, O>
    where
        I: ExpInputs,
        P: ExpParameters,
        O: ExpOutputs<Inputs=I, Params=P>,
{
    fn from_args(args: &clap::ArgMatches) -> Result<Self> {
        let inputs = I::from_args(&args).context("parsing inputs")?;
        let parameters = P::from_args(&args).context("parsing parameters")?;
        let outputs = O::new(&inputs, &parameters);
        Ok(Experiment{ inputs, parameters, outputs })
    }
}

pub trait FromArgs: Sized {
    fn from_args(args: &clap::ArgMatches) -> Result<Self>;
}

pub trait AddArgs {
    fn add_args<'a, 'b>(app: clap::App<'a, 'b>) -> clap::App<'a, 'b>;
}

pub trait ExpInputs: FromArgs + AddArgs {}

pub trait ExpParameters: FromArgs + AddArgs + Default {}

pub trait ExpOutputs: Sized {
    type Inputs: ExpInputs;
    type Params: ExpParameters;
    fn new(inputs: &Self::Inputs, params: &Self::Params) -> Self;
}

