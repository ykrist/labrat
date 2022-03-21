#![allow(unused_variables)]
#![allow(dead_code)]
use labrat::*;
use std::path::PathBuf;
use anyhow::{Result};
use std::time::Duration;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Args, Serialize, Deserialize)]
struct Inputs {
    /// Dataset index
    index: u64,
    /// Time window scale
    #[clap(default_value_t=1.0, value_name="S")]
    tw_scale: f64,
}

impl IdStr for Inputs {
    fn id_str(&self) -> String {
        format!("IDX{:03}_TW{:06}", self.index, (self.tw_scale*1000.0).round() as u64)
    }
}

#[derive(clap::ArgEnum, Clone, Serialize, Deserialize, Debug)]
enum Penum {
  Foo,
  Bar,
}

#[derive(Args, Debug, Clone, Serialize, Deserialize)]
struct Params {
    /// Parameter epsilon
    #[clap(long, default_value_t=0.0001)]
    epsilon: f64,
    /// Number of threads to use
    #[clap(long, default_value_t=1)]
    cpus: u16,
    /// Switch frob
    #[clap(long)]
    frob: bool,
    /// Parameter baz
    #[clap(long)]
    baz: bool,
    /// Give parameters a name (otherwise use a hash of the parameter values)
    #[clap(long)]
    param_name: Option<String>,
    /// Parameter cat
    #[clap(arg_enum, long, default_value_t=Penum::Foo)]
    cat: Penum,
}

#[derive(Args, Default, Debug, Clone, Serialize, Deserialize)]
struct OutputControl {
    /// Enable additional output. Doesn't affect the experiment, just what is outputted
    #[clap(long="tracelog")]
    trace_log: bool,
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
    log: String,
    #[serde(skip_serializing_if="Option::is_none", default)]
    trace_log: Option<String>
}

struct MyExperiment {
    profile: Profile,
    inputs: Inputs,
    params: Params,
    config: OutputControl,
    outputs: Outputs,
}

impl Experiment for MyExperiment {
    type Parameters = Params;
    type Config = OutputControl;
    type Input = Inputs;
    type Output = Outputs;

    fn parameter(&self) -> &Self::Parameters { &self.params }
    
    fn input(&self) -> &Self::Input { &self.inputs }

    fn output(&self) -> &Self::Output { &self.outputs }

    fn new(
        profile: Profile,
        config: Self::Config,
        inputs: Self::Input,
        params: Self::Parameters,
        outputs: Self::Output,
    ) -> Self {
        MyExperiment { profile, config, inputs, params, outputs }
    }

    fn new_output(inputs: &Inputs, _params: &Params, config: &Self::Config) -> Self::Output {
        Outputs {
            log: format!("{}-sollog.json", inputs.id_str()),
            trace_log: 
                if config.trace_log { Some(format!("{}-tracelog.json", inputs.id_str())) }
                else { None }
            
        }
    }

    fn root_dir() -> PathBuf {
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
    let exp = MyExperiment::from_cl_args_with_slurm()?;
    exp.write_index_file()?;
    exp.write_parameter_file()?;

    println!("Inputs:\n{}", serde_json::to_string_pretty(&exp.inputs).unwrap());
    println!("Parameters:\n{}", serde_json::to_string_pretty(&exp.params).unwrap());
    Ok(())
}