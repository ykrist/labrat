#![allow(unused_variables)]
#![allow(dead_code)]
use slurm_harray::{ExpOutputs, ExpInputs, ExpParameters, AddArgs, FromArgs, ResourcePolicy, MemoryAmount, handle_slurm_args};
use std::path::PathBuf;
use anyhow::{Result, Context};
use std::time::Duration;

#[derive(ExpInputs)]
struct Inputs {
    #[slurm(default="1.0")]
    tw_scale: f64,
    index: u64,
}

#[derive(ExpParameters)]
struct Params {
    #[slurm(argname="eps")]
    epsilon: f64,
    cpus: u16,
}

impl Default for Params {
    fn default() -> Self {
        Params{ epsilon: 0.0001, cpus: 1 }
    }
}

struct Outputs {
    log: PathBuf,
}

impl ExpOutputs for Outputs {
    type Inputs = Inputs;
    type Params = Params;

    fn new(inputs: &Inputs, _params: &Params) -> Self {
        let filename = format!("scrap/{}.json", inputs.index);
        Outputs {
            log: filename.into()
        }
    }
}

type MyExperiment = slurm_harray::Experiment<Inputs, Params, Outputs>;
struct Simple;
impl ResourcePolicy<Simple> for MyExperiment {
    fn time(&self) -> Duration { Duration::from_secs(300 + 60*(self.inputs.index/10)) }
    fn memory(&self) -> MemoryAmount { MemoryAmount::from_gb(4) }
    fn script(&self) -> String { String::from("#!/bin/bash\n") }
    fn log_err(&self) -> PathBuf { format!("./logs/{}.out", self.inputs.index).into() }
    fn log_out(&self) -> PathBuf { format!("./logs/{}.err", self.inputs.index).into() }
    fn job_name(&self) -> Option<String> { Some(String::from("hello world"))}
}

fn main() -> Result<()> {
    let _exp: MyExperiment = handle_slurm_args()?;

    Ok(())

}