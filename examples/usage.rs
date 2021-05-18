#![allow(unused_variables)]
#![allow(dead_code)]
use slurm_harray::{ExpOutputs, ExpInputs, ExpParameters, AddArgs, FromArgs, ResourcePolicy, MemoryAmount, handle_slurm_args, define_experiment, Experiment, id_from_serialised};
use std::path::{PathBuf};
use anyhow::{Result};
use std::time::Duration;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, FromArgs, AddArgs, Serialize, Deserialize)]
#[slurm(inputs)]
struct Inputs {
    #[slurm(help="Dataset index")]
    index: u64,
    #[slurm(default="1.0", help="Time window scale.", valname="S")]
    tw_scale: f64,
}

impl ExpInputs for Inputs {
    fn id_str(&self) -> String {
        format!("IDX{:03}_TW{:06}", self.index, (self.tw_scale*1000.0).round() as u64)
    }
}

#[derive(Debug, Clone, FromArgs, AddArgs, Serialize, Deserialize)]
#[slurm(parameters)]
struct Params {
    #[slurm(argname="eps", default="0.0001")]
    epsilon: f64,
    cpus: u16,
    #[slurm(default="true", help="disable frobbing")]
    frob: bool,
    #[slurm(help="turn on baz mode")]
    baz: bool,
    #[slurm(valname="name")]
    param_name: String,
}

impl Default for Params {
    fn default() -> Self {
        Params{ epsilon: 0.0001, cpus: 1,  param_name: String::new(), frob: true, baz: false }
    }
}

impl ExpParameters for Params {
    fn id_str(&self) -> String {
        if self.param_name.is_empty() {
            id_from_serialised(self)
        } else {
            self.param_name.clone()
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct Outputs {
    log: PathBuf,
}

impl ExpOutputs for Outputs {
    type Inputs = Inputs;
    type Params = Params;

    fn new(inputs: &Inputs, _params: &Params) -> Self {
        let filename = format!("{}-sollog.json", inputs.id_str());
        Outputs {
            log: filename.into()
        }
    }
}

define_experiment!{ struct MyExperiment, Inputs, Params, Outputs }

impl Experiment for MyExperiment {
    fn log_root_dir() -> PathBuf {
        concat!(env!("CARGO_MANIFEST_DIR"), "/logs/").into()
    }
}

impl ResourcePolicy for MyExperiment {
    fn time(&self) -> Duration { Duration::from_secs(300 + 60*(self.inputs.index/10)) }
    fn memory(&self) -> MemoryAmount { MemoryAmount::from_gb(4) }
    fn script(&self) -> String { String::from("#!/bin/bash\n") }
    fn job_name(&self) -> Option<String> { Some(String::from("hello world"))}
}

fn main() -> Result<()> {
    let exp: MyExperiment = handle_slurm_args()?;
    exp.write_index_file()?;
    exp.write_parameter_file()?;

    println!("Inputs:\n{}", serde_json::to_string_pretty(&exp.inputs).unwrap());
    println!("Parameters:\n{}", serde_json::to_string_pretty(&exp.parameters).unwrap());
    Ok(())
}