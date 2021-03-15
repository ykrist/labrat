mod experiment;

pub use experiment::*;
use std::path::PathBuf;
use std::time::Duration;
use std::io::{Read};
use std::fs::File;
use std::os::unix::io::{FromRawFd, RawFd};
use anyhow::{Result, Context};
use serde::{Serialize, Deserialize};
use std::process::exit;

pub use slurm_utils_macro::*;


pub struct MemoryAmount(usize);

impl MemoryAmount {
    pub fn from_mb(amount: usize) -> Self { MemoryAmount(amount) }

    pub fn from_gb(amount: usize) -> Self { MemoryAmount(amount * 1000) }

    pub fn from_gb_f64(amount: f64) -> Self {
        MemoryAmount((amount * 1000.0).round() as usize)
    }

    pub fn as_mb(&self) -> usize { self.0 }
}


#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum MailType {
    None,
    Begin,
    End,
    Fail,
    Requeue,
    All,
    InvalidDepend,
    StageOut,
    TimeLimit,
    TimeLimit90,
    TimeLimit80,
    TimeLimit50,
    ArrayTasks,
}

impl ToString for MailType {
    fn to_string(&self) -> String {
        use MailType::*;
        match self {
            None => "NONE",
            Begin => "BEGIN",
            End => "END",
            Fail => "FAIL",
            Requeue => "REQUEUE",
            All => "ALL",
            InvalidDepend => "INVALID_DEPEND",
            StageOut => "STAGE_OUT",
            TimeLimit => "TIME_LIMIT",
            TimeLimit90 => "TIME_LIMIT_90",
            TimeLimit80 => "TIME_LIMIT_80",
            TimeLimit50 => "TIME_LIMIT_50",
            ArrayTasks => "ARRAY_TASKS",
        }.to_owned()
    }
}


#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
struct SlurmResources {
    script: String,
    log_err: PathBuf,
    log_out: PathBuf,
    #[serde(skip_serializing_if="Option::is_none")]
    job_name: Option<String>,
    cpus: usize,
    nodes: usize,
    time: String,
    memory: String,
    #[serde(skip_serializing_if="Option::is_none")]
    mail_user: Option<String>,
    #[serde(skip_serializing_if="Option::is_none")]
    mail_type: Option<String>,
}

fn fmt_as_slurm_time(mut secs: u64) -> String {
    let mut minutes = secs / 60;
    secs -= minutes * 60;
    let mut hrs = minutes / 60;
    minutes -= hrs * 60;
    let days = hrs / 24;
    hrs -= days * 24;
    format!("{}-{}:{:02}:{:02}", days, hrs, minutes, secs)
}

impl SlurmResources {
    pub fn get(exp: &impl ResourcePolicy) -> Self {
        let mail_type = {
            let mt = exp.mail_type();
            if mt.is_empty() {
                None
            } else {
                let mt: Vec<_> = mt.into_iter().map(|m| m.to_string()).collect();
                Some(mt.join(","))
            }
        };

        SlurmResources {
            time: fmt_as_slurm_time(exp.time().as_secs()),
            memory: format!("{}MB", exp.memory().as_mb()),
            script: exp.script(),
            log_err: exp.log_err(),
            log_out: exp.log_out(),
            job_name: exp.job_name(),
            mail_user: exp.mail_user(),
            mail_type,
            cpus: exp.cpus(),
            nodes: exp.nodes(),
        }
    }
}


pub trait ResourcePolicy: Experiment {
    fn script(&self) -> String;
    fn time(&self) -> Duration;
    fn memory(&self) -> MemoryAmount;
    fn cpus(&self) -> usize { 1 }
    fn nodes(&self) -> usize { 1 }
    fn job_name(&self) -> Option<String> {
        Some(self.parameters().id_str())
    }
    fn mail_user(&self) -> Option<String> { None }
    fn mail_type(&self) -> Vec<MailType> { Vec::new() }
    fn log_err(&self) -> PathBuf {
        self.get_output_path(&format!("{}.err", self.inputs().id_str()))
    }
    fn log_out(&self) -> PathBuf {
        self.get_output_path(&format!("{}.out", self.inputs().id_str()))
    }
}



fn run_pipe_server<I, P, O, T>(read_fd: RawFd, write_fd: RawFd, app: clap::App) -> Result<()>
    where
      T: From<ExpInner<I, P, O>> + ResourcePolicy,
      I: ExpInputs,
      P: ExpParameters,
      O: ExpOutputs<Inputs=I, Params=P>,
{
    let mut reader: File = unsafe { File::from_raw_fd(read_fd as RawFd) };
    let writer: File = unsafe { File::from_raw_fd(write_fd as RawFd) };

    let mut cl_args = String::new();
    reader.read_to_string(&mut cl_args)?;

    let mut outputs = Vec::new();

    for cmd in cl_args.lines() {
        let q = "binary-name";
        let args = app.clone().get_matches_from(
            [q].iter().copied().chain(cmd.split_whitespace())
        );
        let exp = T::from(ExpInner::<I, P, O>::from_args(&args)?);
        outputs.push(SlurmResources::get(&exp))
    }

    serde_json::to_writer(writer, &outputs)?;
    Ok(())
}

pub fn handle_slurm_args<'de, I, P, O, T>() -> Result<T>
    where
        T: From<ExpInner<I, P, O>> + ResourcePolicy,
        I: ExpInputs,
        P: ExpParameters + Deserialize<'de>,
        O: ExpOutputs<Inputs=I, Params=P>,
{
    let app = clap::App::new("")
        .arg(clap::Arg::with_name("slurm-pipe")
            .long("slurm-pipe")
            .number_of_values(2)
            .help("Start the Slurm info pipe server with file descriptors R (Reading) and W (Writing)")
            .value_names(&["R", "W"])
        )
        .arg(clap::Arg::with_name("slurm-info")
            .long("slurm-info")
            .help("Print Slurm info as a JSON string and exit.")
        )
        .group(clap::ArgGroup::with_name("slurm-managed")
            .args(&["slurm-pipe", "slurm-info"])
        );
    let app = I::add_args(app);
    let app = P::add_args(app);
    let args = app.clone().get_matches();

    if let Some(mut rawfds) = args.values_of("slurm-pipe") {
        let read_fd: RawFd = rawfds.next().unwrap().parse().context("parsing `R`")?;
        let write_fd: RawFd = rawfds.next().unwrap().parse().context("parsing `W`")?;
        debug_assert_eq!(rawfds.next(), None);
        run_pipe_server::<I, P, O, T>(read_fd, write_fd, app)?;
        exit(0)
    }

    let exp = T::from(ExpInner::from_args(&args)?);

    if args.is_present("slurm-info") {
        println!("{}", serde_json::to_string_pretty(&SlurmResources::get(&exp))?);
        exit(0);
    }
    Ok(exp.into())
}