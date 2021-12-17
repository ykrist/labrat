#![allow(unused_variables)]
#![allow(dead_code)]
use slurm_harray::*;
use std::path::{PathBuf};
use anyhow::{Result};
use std::time::Duration;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, StructOpt, Serialize, Deserialize)]
struct Inputs {
    /// Dataset index
    index: u64,
    /// Time window scale
    #[structopt(default_value="1.0", value_name="S")]
    tw_scale: f64,
}

impl IdStr for Inputs {
    fn id_str(&self) -> String {
        format!("IDX{:03}_TW{:06}", self.index, (self.tw_scale*1000.0).round() as u64)
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
enum Penum {
  Foo,
  Bar,
}

impl_arg_enum!{
    Penum;
    Foo = "foo",
    Bar = "bar"
}

#[derive(StructOpt, Debug, Clone, Serialize, Deserialize)]
struct Params {
    /// Parameter epsilon
    #[structopt(long, default_value="0.0001")]
    epsilon: f64,
    /// Number of threads to use
    #[structopt(long)]
    #[cfg_attr(debug_assertions, structopt(default_value="1"))]
    #[cfg_attr(not(debug_assertions), structopt(default_value="4"))]
    cpus: u16,
    /// Parameter frob
    #[structopt(long)]
    frob: bool,
    /// Parameter baz
    #[structopt(long)]
    baz: bool,
    /// Give parameters a name (otherwise use a hash of the parameter values)
    #[structopt(long)]
    param_name: Option<String>,
    /// Parameter cat
    #[structopt(long, default_value="foo", possible_values=Penum::choices())]
    cat: Penum,
}

impl Default for Params {
    fn default() -> Self {
        Params{ epsilon: 0.0001, cpus: 1,  param_name: None, frob: true, baz: false, cat: Penum::Bar }
    }
}

impl IdStr for Params {
    fn id_str(&self) -> String {
        self.param_name.clone().unwrap_or_else(|| id_from_serialised(self))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Outputs {
    log: PathBuf,
}

impl NewOutput for Outputs {
    type Inputs = Inputs;
    type Params = Params;
    type AuxParams = NoAuxParams;

    fn new(inputs: &Inputs, _params: &Params, _aux: &Self::AuxParams) -> Self {
        let filename = format!("{}-sollog.json", inputs.id_str());
        Outputs {
            log: filename.into()
        }
    }
}

struct MyExperiment {
    profile: SlurmProfile,
    inputs: Inputs,
    params: Params,
    outputs: Outputs
}

impl Experiment for MyExperiment {
    impl_experiment_helper! {
      profile;
      inputs: Inputs;
      params: Params;
      outputs: Outputs;
    }

    fn log_root_dir() -> PathBuf {
        concat!(env!("CARGO_MANIFEST_DIR"), "/logs/").into()
    }
}

impl ResourcePolicy for MyExperiment {
    fn time(&self) -> Duration { Duration::from_secs(300 + 60*(self.inputs.index/10)) }
    fn memory(&self) -> MemoryAmount { MemoryAmount::from_gb(4) }
    fn script(&self) -> String { String::from("#!/bin/bash\n") }
    fn job_name(&self) -> Option<String> { Some(String::from("hello world"))}

    fn exclude(&self) -> Option<String> {
        Some("shitty-node-that-fails".into())
    }
}

fn main() -> Result<()> {
    let exp: MyExperiment = handle_slurm_args()?;
    exp.write_index_file()?;
    exp.write_parameter_file()?;

    println!("Inputs:\n{}", serde_json::to_string_pretty(&exp.inputs).unwrap());
    println!("Parameters:\n{}", serde_json::to_string_pretty(&exp.params).unwrap());
    Ok(())
}