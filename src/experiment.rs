use clap;
use anyhow::{Context, Result};
use serde::{Serialize};
use std::convert::From;
use sha2::Digest;
use std::path::{PathBuf, Path};

#[derive(Debug, Clone)]
pub struct ExpInner<I, P, O> {
    pub inputs: I,
    pub parameters: P,
    pub outputs: O,
}


impl<I, P, O> ExpInner<I, P, O>
    where
      I: ExpInputs,
      P: ExpParameters,
      O: ExpOutputs<Inputs=I, Params=P>,
{

    pub fn new(inputs: I, parameters: P, outputs: O) -> Self {
        ExpInner {
            inputs,
            parameters,
            outputs
        }
    }
}

pub trait ExperimentAuto {
    type Inputs: ExpInputs;
    type Parameters: ExpParameters;
    type Outputs: Serialize;

    fn inputs(&self) -> &Self::Inputs;
    fn outputs(&self) -> &Self::Outputs;
    fn parameters(&self) -> &Self::Parameters;
}

pub trait Experiment : ExperimentAuto {
    fn log_root_dir() -> PathBuf;

    fn get_output_path(&self, filename: &str) -> PathBuf {
        let mut log_dir = Self::log_root_dir();
        log_dir.push(self.parameters().id_str());
        let mut log_dir = ensure_directory_exists(log_dir).unwrap();
        log_dir.push(filename);
        log_dir
    }

    fn write_index_file(&self) -> Result<()> {
        let p = self.get_output_path(&format!("{}-index.json", self.inputs().id_str()));
        let contents = serde_json::json!({
            "input": self.inputs(),
            "output" : self.outputs(),
        });
        let contents = serde_json::to_string_pretty(&contents)?;
        std::fs::write(p, contents)?;
        Ok(())
    }

    fn write_parameter_file(&self) -> Result<()> {
        let p = self.get_output_path("parameters.json");
        if !p.exists() {
            std::fs::write(p, serde_json::to_string_pretty(self.parameters())?)?;
        }
        Ok(())
    }
}

pub fn id_from_serialised<T: Serialize>(val: &T) -> String {
    let mut hasher = sha2::Sha224::new();
    hasher.update(&serde_json::to_string(val).unwrap());
    base_62::encode(hasher.finalize().as_slice())
}


fn ensure_directory_exists(path: impl AsRef<Path>) -> Result<PathBuf> {
    match std::fs::create_dir_all(path.as_ref()) {
        Ok(()) => {}
        Err(e) => match e.kind() {
            std::io::ErrorKind::AlreadyExists => {},
            _ => return Err(e.into()),
        }
    };
    return Ok(path.as_ref().canonicalize().unwrap())
}




#[macro_export]
macro_rules! define_experiment {
    (struct $t:ident, $I:path, $P:path, $O:path) => {
        $crate::define_experiment!{ () struct $t, $I, $P, $O }
    };

    (pub struct $t:ident, $I:path, $P:path, $O:path) => {
        $crate::define_experiment!{ (pub) struct $t, $I, $P, $O }
    };

    (($($vis:tt)*) struct $t:ident, $I:path, $P:path, $O:path) => {
        #[derive(Debug, Clone)]
        $($vis)* struct $t(slurm_harray::ExpInner<$I, $P, $O>);

        impl $crate::ExperimentAuto for $t {
            type Inputs = $I;
            type Outputs = $O;
            type Parameters = $P;

            fn inputs(&self) -> &Self::Inputs { &self.inputs }
            fn outputs(&self) -> &Self::Outputs { &self.outputs }
            fn parameters(&self) -> &Self::Parameters { &self.parameters }
        }

        impl From<slurm_harray::ExpInner<$I, $P, $O>> for $t {
            fn from(val: slurm_harray::ExpInner<$I, $P, $O>) -> Self {
                $t(val)
            }
        }

        impl std::ops::Deref for $t {
            type Target = slurm_harray::ExpInner<$I, $P, $O>;
            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }
     };
}

impl<I, P, O> FromArgs for ExpInner<I, P, O>
    where
        I: ExpInputs,
        P: ExpParameters,
        O: ExpOutputs<Inputs=I, Params=P>,
{
    fn from_args(args: &clap::ArgMatches) -> Result<Self> {
        let inputs = I::from_args(&args).context("parsing inputs")?;
        let parameters = P::from_args(&args).context("parsing parameters")?;
        let outputs = O::new(&inputs, &parameters);
        Ok(ExpInner::new(inputs, parameters, outputs))
    }
}

pub trait FromArgs: Sized {
    fn from_args(args: &clap::ArgMatches) -> Result<Self>;
}

pub trait AddArgs {
    fn add_args<'a, 'b>(app: clap::App<'a, 'b>) -> clap::App<'a, 'b>;
}

pub trait ExpInputs: FromArgs + AddArgs + Serialize {
    fn id_str(&self) -> String {
        id_from_serialised(self)
    }
}

pub trait ExpParameters: FromArgs + AddArgs + Default + Serialize {
    fn id_str(&self) -> String {
        id_from_serialised(self)
    }
}

pub trait ExpOutputs: Sized + Serialize {
    type Inputs: ExpInputs;
    type Params: ExpParameters;
    fn new(inputs: &Self::Inputs, params: &Self::Params) -> Self;
}
