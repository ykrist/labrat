mod experiment;
mod hash;

pub use experiment::*;
use std::path::PathBuf;
use std::time::Duration;
use std::io::{Read, Write};
use std::process::exit;
use std::ops::Deref;
use json::JsonValue;
use std::fs::File;
use std::os::unix::io::{FromRawFd, RawFd};
use anyhow::{Result, Context};
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


#[derive(Debug, Clone, Eq, PartialEq)]
struct SlurmResources {
    script: String,
    log_err: PathBuf,
    log_out: PathBuf,
    job_name: Option<String>,
    cpus: usize,
    nodes: usize,
    time: String,
    memory: String,
    mail_user: Option<String>,
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
    pub fn get<T>(exp: &impl ResourcePolicy<T>) -> Self {
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

    pub fn json(&self) -> json::JsonValue {
        let mut obj = json::object! {
            time: self.time.clone(),
            memory: self.memory.clone(),
            script: self.script.clone(),
            log_err: logpath_to_json_val(&self.log_err),
            log_out: logpath_to_json_val(&self.log_out),
            cpus: self.cpus,
            nodes: self.nodes,
        };
        match &mut obj {
            json::JsonValue::Object(obj) => {
                if let Some(n) = &self.job_name {
                    obj.insert("job_name", n.deref().into())
                }
                if let Some(u) = &self.mail_user {
                    obj.insert("mail_user", u.deref().into())
                }
                if let Some(t) = &self.mail_type {
                    obj.insert("mail_type", t.deref().into())
                }
            }
            _ => unreachable!(),
        }
        obj
    }
}


pub trait ResourcePolicy<T> {
    fn log_err(&self) -> PathBuf;
    fn log_out(&self) -> PathBuf;
    fn script(&self) -> String;
    fn time(&self) -> Duration;
    fn memory(&self) -> MemoryAmount;
    fn cpus(&self) -> usize { 1 }
    fn nodes(&self) -> usize { 1 }
    fn job_name(&self) -> Option<String> { None }
    fn mail_user(&self) -> Option<String> { None }
    fn mail_type(&self) -> Vec<MailType> { Vec::new() }
}

fn logpath_to_json_val(p: &PathBuf) -> json::JsonValue {
    if let Some(logdir) = p.parent() {
        match std::fs::create_dir_all(logdir) {
            Ok(_) => {}
            Err(e) => match e.kind() {
                std::io::ErrorKind::AlreadyExists => {}
                other => panic!("{}", e)
            }
        }
        let mut logdir = logdir.canonicalize().unwrap();
        logdir.push(p.file_name().unwrap());
        logdir.to_string_lossy()
            .into_owned()
            .into()
    } else {
        p.to_string_lossy()
            .into_owned()
            .into()
    }
}


fn run_pipe_server<I, P, O, T>(read_fd: RawFd, write_fd: RawFd, app: clap::App) -> Result<()>
    where
        I: ExpInputs,
        P: ExpParameters,
        O: ExpOutputs<Inputs=I, Params=P>,
        Experiment<I, P, O>: ResourcePolicy<T>
{
    let mut reader: File = unsafe { File::from_raw_fd(read_fd as RawFd) };
    let mut writer: File = unsafe { File::from_raw_fd(write_fd as RawFd) };

    let mut cl_args = String::new();
    reader.read_to_string(&mut cl_args)?;

    let mut outputs = Vec::new();

    for cmd in cl_args.lines() {
        let q = "binary-name";
        let args = app.clone().get_matches_from(
            [q].iter().copied().chain(cmd.split_whitespace())
        );
        let exp = Experiment::<I, P, O>::from_args(&args)?;
        outputs.push(SlurmResources::get(&exp).json())
    }

    writer.write(JsonValue::from(outputs).dump().as_bytes())?;
    Ok(())
}

pub fn handle_slurm_args<I, P, O, T>() -> Result<Experiment<I, P, O>>
    where
        I: ExpInputs,
        P: ExpParameters,
        O: ExpOutputs<Inputs=I, Params=P>,
        Experiment<I, P, O>: ResourcePolicy<T>,
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
        run_pipe_server::<I, P, O, T>(read_fd, write_fd, app);
        exit(0)
    }

    let exp = Experiment::from_args(&args)?;

    if args.is_present("slurm-info") {
        println!("{}", SlurmResources::get(&exp).json().dump());
        exit(0);
    }
    Ok(exp)
}